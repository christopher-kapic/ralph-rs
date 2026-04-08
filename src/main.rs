mod cli;
mod commands;
mod config;
mod db;
mod executor;
mod export;
mod frac_index;
mod git;
mod harness;
mod import;
mod plan_harness;
mod output;
mod plan;
mod preflight;
mod prompt;
mod runner;
mod signal;
mod storage;
mod test_runner;
mod tui;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{
    AgentsCommand, Cli, Command, PlanCommand, PlanHarnessCommand, StepCommand,
};
use crate::commands::resolve_project;
use crate::runner::RunOptions;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration (creates default if absent).
    let _config = config::load_or_create_config()?;

    // Open (or create) the database and run any pending migrations.
    let conn = db::open()?;

    // Resolve project directory for commands that need it.
    let project = resolve_project(cli.project.as_deref())?;

    match cli.command {
        // -- Init --
        Command::Init { .. } => {
            cmd_init()?;
            Ok(())
        }

        // -- Plan --
        Command::Plan(subcmd) => match subcmd {
            PlanCommand::Create {
                slug,
                description,
                branch,
                harness,
                agent,
                tests,
            } => {
                let h = cli.harness.as_deref().or(harness.as_deref());
                commands::plan_create(
                    &conn,
                    &slug,
                    &project,
                    description.as_deref(),
                    branch.as_deref(),
                    h,
                    agent.as_deref(),
                    &tests,
                )
            }
            PlanCommand::List { all, status } => {
                commands::plan_list(&conn, &project, all, status.as_deref())
            }
            PlanCommand::Show { slug } => commands::plan_show(&conn, &slug, &project),
            PlanCommand::Approve { slug } => commands::plan_approve(&conn, &slug, &project),
            PlanCommand::Delete { slug, force } => {
                commands::plan_delete(&conn, &slug, &project, force)
            }
        },

        // -- Step --
        Command::Step(subcmd) => match subcmd {
            StepCommand::List { plan } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step list");
                }
                commands::step_list(&conn, &slug, &project)
            }
            StepCommand::Add {
                title,
                plan,
                description,
                after,
                agent,
                harness,
            } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step add");
                }
                let h = cli.harness.as_deref().or(harness.as_deref());
                commands::step_add(
                    &conn,
                    &slug,
                    &project,
                    &title,
                    description.as_deref(),
                    after,
                    agent.as_deref(),
                    h,
                )
            }
            StepCommand::Remove { step, plan, force } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step remove");
                }
                commands::step_remove(&conn, &slug, &project, step, force)
            }
            StepCommand::Edit {
                step,
                plan,
                title,
                description,
            } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step edit");
                }
                commands::step_edit(
                    &conn,
                    &slug,
                    &project,
                    step,
                    title.as_deref(),
                    description.as_deref(),
                )
            }
            StepCommand::Reset { step, plan } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step reset");
                }
                commands::step_reset(&conn, &slug, &project, step)
            }
            StepCommand::Move { step, to, plan } => {
                let slug = plan.unwrap_or_default();
                if slug.is_empty() {
                    anyhow::bail!("--plan is required for step move");
                }
                commands::step_move(&conn, &slug, &project, step, to)
            }
        },

        // -- Run --
        Command::Run {
            plan: plan_slug,
            all,
            from,
            to,
            dry_run,
            skip_preflight,
            harness: run_harness,
        } => {
            let slug = plan_slug.unwrap_or_default();
            if slug.is_empty() {
                anyhow::bail!("--plan is required for run");
            }
            let plan = storage::get_plan_by_slug(&conn, &slug, &project)?
                .with_context(|| format!("Plan not found: {slug}"))?;

            let workdir = std::path::Path::new(&project);

            // Preflight checks
            if !skip_preflight && !dry_run {
                eprintln!("Running preflight checks...");
                let preflight_results =
                    preflight::run_preflight_checks(&plan, &_config, workdir)?;
                preflight_results.print_report();

                if !preflight_results.is_ok() {
                    anyhow::bail!(
                        "Preflight checks failed. Use --skip-preflight to bypass."
                    );
                }

                // Auto-stash dirty git state
                if preflight::auto_stash_dirty_state(workdir)? {
                    eprintln!("  Auto-committed dirty state before run.");
                }
            }

            let harness_override = cli.harness.or(run_harness);
            let options = RunOptions {
                all,
                from,
                to,
                current_branch: false,
                harness_override,
                dry_run,
            };

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::run_plan(&conn, &plan, &_config, workdir, &options, abort_rx).await
            })?;

            if result.steps_failed > 0 {
                eprintln!(
                    "Plan '{}' failed: {}/{} steps succeeded",
                    slug, result.steps_succeeded, result.steps_executed
                );
            } else {
                eprintln!(
                    "Plan '{}' complete: {}/{} steps succeeded",
                    slug, result.steps_succeeded, result.steps_executed
                );
            }
            Ok(())
        }

        // -- Resume --
        Command::Resume { plan: plan_slug } => {
            let slug = plan_slug.unwrap_or_default();
            if slug.is_empty() {
                anyhow::bail!("--plan is required for resume");
            }
            let plan = storage::get_plan_by_slug(&conn, &slug, &project)?
                .with_context(|| format!("Plan not found: {slug}"))?;

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::resume_plan(&conn, &plan, &_config, project.as_ref(), abort_rx).await
            })?;

            if result.steps_failed > 0 {
                eprintln!(
                    "Plan '{}' failed: {}/{} steps succeeded",
                    slug, result.steps_succeeded, result.steps_executed
                );
            } else {
                eprintln!(
                    "Plan '{}' resumed: {}/{} steps succeeded",
                    slug, result.steps_succeeded, result.steps_executed
                );
            }
            Ok(())
        }

        // -- Skip --
        Command::Skip {
            plan: plan_slug,
            step: step_num,
            reason,
        } => {
            let slug = plan_slug.unwrap_or_default();
            if slug.is_empty() {
                anyhow::bail!("--plan is required for skip");
            }
            let plan = storage::get_plan_by_slug(&conn, &slug, &project)?
                .with_context(|| format!("Plan not found: {slug}"))?;

            runner::skip_step(&conn, &plan, step_num, reason.as_deref())?;
            Ok(())
        }

        // -- Plan-harness --
        Command::PlanHarness(args) => match args.command {
            Some(PlanHarnessCommand::Set { .. }) => Ok(()),
            Some(PlanHarnessCommand::Show { .. }) => Ok(()),
            None => {
                // Interactive plan-harness mode: spawn a harness to create/update plans.
                let harness_name = args
                    .use_harness
                    .or(cli.harness)
                    .unwrap_or_else(|| _config.default_harness.clone());
                let rt = tokio::runtime::Runtime::new()?;
                let exit_code = rt.block_on(plan_harness::run_plan_harness(
                    &_config,
                    &harness_name,
                    &project,
                    args.description.as_deref(),
                ))?;
                std::process::exit(exit_code);
            }
        },

        // -- Export --
        Command::Export { plan, output } => {
            export::export_plan(&conn, &plan, &project, output.as_deref())
        }

        // -- Import --
        Command::Import {
            file,
            slug,
            branch,
        } => {
            let h = cli.harness.as_deref();
            import::import_plan(
                &conn,
                &file,
                &project,
                slug.as_deref(),
                branch.as_deref(),
                h,
            )
        }

        // -- Status --
        Command::Status { plan, verbose } => {
            cmd_status(&conn, &project, plan.as_deref(), verbose)
        }

        // -- Log --
        Command::Log {
            plan,
            step,
            limit,
            full,
        } => cmd_log(&conn, &project, plan.as_deref(), step, limit, full),

        // -- Agents --
        Command::Agents(subcmd) => match subcmd {
            AgentsCommand::List => cmd_agents_list(),
            AgentsCommand::Show { name } => cmd_agents_show(&name),
            AgentsCommand::Create { name, file } => {
                cmd_agents_create(&name, file.as_deref())
            }
            AgentsCommand::Delete { name } => cmd_agents_delete(&name),
        },

        // -- Doctor --
        Command::Doctor => cmd_doctor(&_config),
    }
}

// ---------------------------------------------------------------------------
// Init command
// ---------------------------------------------------------------------------

fn cmd_init() -> Result<()> {
    use std::fs;

    // Create config dir
    let config_dir = config::config_dir()?;
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create config directory {}", config_dir.display()))?;
    println!("\x1b[32m\u{2714}\x1b[0m Config directory: {}", config_dir.display());

    // Create agents dir
    let agents_dir = config::agents_dir()?;
    fs::create_dir_all(&agents_dir)
        .with_context(|| format!("Failed to create agents directory {}", agents_dir.display()))?;
    println!("\x1b[32m\u{2714}\x1b[0m Agents directory: {}", agents_dir.display());

    // Create default config file if it doesn't exist
    let config_path = config_dir.join("config.json");
    if !config_path.exists() {
        let default_config = config::Config::default();
        let json = serde_json::to_string_pretty(&default_config)?;
        fs::write(&config_path, &json)
            .with_context(|| format!("Failed to write config to {}", config_path.display()))?;
        println!("\x1b[32m\u{2714}\x1b[0m Default config: {}", config_path.display());
    } else {
        println!("\x1b[32m\u{2714}\x1b[0m Config exists: {}", config_path.display());
    }

    // Initialize database
    let _conn = db::open()?;
    let db_path = db::db_path()?;
    println!("\x1b[32m\u{2714}\x1b[0m Database: {}", db_path.display());

    println!();
    println!("ralph-rs initialized successfully.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Doctor command
// ---------------------------------------------------------------------------

fn cmd_doctor(config: &config::Config) -> Result<()> {
    println!("ralph-rs doctor");
    println!();

    let checks = preflight::run_doctor_checks(config);

    let mut has_errors = false;
    for check in &checks {
        let icon = match check.severity {
            preflight::CheckSeverity::Pass => "\x1b[32m\u{2714}\x1b[0m",
            preflight::CheckSeverity::Warning => "\x1b[33m\u{26a0}\x1b[0m",
            preflight::CheckSeverity::Error => {
                has_errors = true;
                "\x1b[31m\u{2718}\x1b[0m"
            }
        };
        println!("  {} {}: {}", icon, check.name, check.message);
    }

    println!();
    if has_errors {
        println!("Some checks failed. Please fix the issues above.");
    } else {
        println!("All checks passed.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status command
// ---------------------------------------------------------------------------

fn cmd_status(
    conn: &rusqlite::Connection,
    project: &str,
    plan_slug: Option<&str>,
    verbose: bool,
) -> Result<()> {
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        // Find the most recent active plan (in_progress, ready, or failed).
        let plans = storage::list_plans(conn, project, false)?;
        let active = plans.into_iter().find(|p| {
            matches!(
                p.status,
                plan::PlanStatus::InProgress
                    | plan::PlanStatus::Ready
                    | plan::PlanStatus::Failed
            )
        });
        match active {
            Some(p) => p,
            None => {
                println!("No active plan found. Use --plan to specify a plan slug.");
                return Ok(());
            }
        }
    };

    let status_str = plan.status.as_str();
    let color = match plan.status {
        plan::PlanStatus::Planning => "\x1b[33m",
        plan::PlanStatus::Ready => "\x1b[36m",
        plan::PlanStatus::InProgress => "\x1b[34m",
        plan::PlanStatus::Complete => "\x1b[32m",
        plan::PlanStatus::Failed => "\x1b[31m",
        plan::PlanStatus::Aborted => "\x1b[31m",
    };

    println!(
        "\x1b[1m{}\x1b[0m  {}{}\x1b[0m",
        plan.slug, color, status_str
    );
    println!("  Branch: {}", plan.branch_name);

    let steps = storage::list_steps(conn, &plan.id)?;
    if steps.is_empty() {
        println!("  No steps.");
        return Ok(());
    }

    let total = steps.len();
    let complete = steps.iter().filter(|s| s.status == plan::StepStatus::Complete).count();
    let failed = steps.iter().filter(|s| s.status == plan::StepStatus::Failed).count();
    let skipped = steps.iter().filter(|s| s.status == plan::StepStatus::Skipped).count();
    let pending = steps.iter().filter(|s| s.status == plan::StepStatus::Pending).count();
    let in_progress = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::InProgress)
        .count();

    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        complete, total, failed, skipped, pending, in_progress
    );

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            let ss = step.status.as_str();
            let icon = match step.status {
                plan::StepStatus::Pending => "\x1b[90m\u{25cb}\x1b[0m",
                plan::StepStatus::InProgress => "\x1b[34m\u{25b6}\x1b[0m",
                plan::StepStatus::Complete => "\x1b[32m\u{2714}\x1b[0m",
                plan::StepStatus::Failed => "\x1b[31m\u{2718}\x1b[0m",
                plan::StepStatus::Skipped => "\x1b[90m\u{2298}\x1b[0m",
                plan::StepStatus::Aborted => "\x1b[31m\u{2298}\x1b[0m",
            };
            println!(
                "  {:>3}. {} {} [{}] (attempts: {})",
                i + 1,
                icon,
                step.title,
                ss,
                step.attempts,
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Log command
// ---------------------------------------------------------------------------

fn cmd_log(
    conn: &rusqlite::Connection,
    project: &str,
    plan_slug: Option<&str>,
    step_num: Option<usize>,
    limit: Option<usize>,
    full: bool,
) -> Result<()> {
    // Resolve plan
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        let plans = storage::list_plans(conn, project, false)?;
        let active = plans.into_iter().find(|p| {
            matches!(
                p.status,
                plan::PlanStatus::InProgress
                    | plan::PlanStatus::Ready
                    | plan::PlanStatus::Failed
                    | plan::PlanStatus::Complete
            )
        });
        match active {
            Some(p) => p,
            None => {
                println!("No plan found. Use --plan to specify a plan slug.");
                return Ok(());
            }
        }
    };

    if let Some(step_idx) = step_num {
        // Show logs for a specific step
        let steps = storage::list_steps(conn, &plan.id)?;
        if step_idx == 0 || step_idx > steps.len() {
            anyhow::bail!(
                "Step {} is out of range (plan has {} steps)",
                step_idx,
                steps.len()
            );
        }
        let step = &steps[step_idx - 1];
        let logs = storage::list_execution_logs_for_step(conn, &step.id)?;

        println!(
            "Logs for step #{} '{}' ({} attempts):",
            step_idx,
            step.title,
            logs.len()
        );
        println!();

        for log in &logs {
            print_log_entry(&step.title, log, full);
        }
    } else {
        // Show all logs for the plan
        let entries = storage::list_execution_logs_for_plan(conn, &plan.id, limit)?;

        if entries.is_empty() {
            println!("No execution logs for plan '{}'.", plan.slug);
            return Ok(());
        }

        println!(
            "Execution logs for plan '{}' ({} entries):",
            plan.slug,
            entries.len()
        );
        println!();

        for (step_title, log) in &entries {
            print_log_entry(step_title, log, full);
        }
    }

    Ok(())
}

fn print_log_entry(step_title: &str, log: &plan::ExecutionLog, full: bool) {
    let status_icon = if log.committed {
        "\x1b[32m\u{2714}\x1b[0m"
    } else if log.rolled_back {
        "\x1b[31m\u{21ba}\x1b[0m"
    } else {
        "\x1b[90m\u{25cb}\x1b[0m"
    };

    let duration_str = log
        .duration_secs
        .map(|d| format!("{:.1}s", d))
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {} [attempt {}] {} ({}) {}",
        status_icon,
        log.attempt,
        step_title,
        duration_str,
        log.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
    );

    if let Some(ref hash) = log.commit_hash {
        println!("    commit: {}", &hash[..hash.len().min(8)]);
    }

    if !log.test_results.is_empty() {
        println!("    tests: {}", log.test_results.join(", "));
    }

    if let Some(cost) = log.cost_usd {
        let tokens = match (log.input_tokens, log.output_tokens) {
            (Some(i), Some(o)) => format!(" ({i} in / {o} out tokens)"),
            _ => String::new(),
        };
        println!("    cost: ${:.4}{}", cost, tokens);
    }

    if full {
        if let Some(ref stdout) = log.harness_stdout
            && !stdout.is_empty()
        {
            println!("    --- stdout ---");
            for line in stdout.lines().take(50) {
                println!("    {line}");
            }
        }
        if let Some(ref stderr) = log.harness_stderr
            && !stderr.is_empty()
        {
            println!("    --- stderr ---");
            for line in stderr.lines().take(50) {
                println!("    {line}");
            }
        }
    }

    println!();
}

// ---------------------------------------------------------------------------
// Agents commands
// ---------------------------------------------------------------------------

fn cmd_agents_list() -> Result<()> {
    let agents_dir = config::agents_dir()?;

    if !agents_dir.exists() {
        println!("Agents directory not found: {}", agents_dir.display());
        println!("Run `ralph-rs init` to create it.");
        return Ok(());
    }

    let mut found = false;
    let mut entries: Vec<_> = std::fs::read_dir(&agents_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "md")
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in &entries {
        let name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".md")
            .to_string();
        let metadata = entry.metadata().ok();
        let size = metadata.map(|m| m.len()).unwrap_or(0);
        println!("  {} ({} bytes)", name, size);
        found = true;
    }

    if !found {
        println!("No agent files found in {}", agents_dir.display());
    }

    Ok(())
}

fn cmd_agents_show(name: &str) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

fn cmd_agents_create(name: &str, file: Option<&std::path::Path>) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    std::fs::create_dir_all(&agents_dir)?;
    let path = agents_dir.join(format!("{name}.md"));

    if path.exists() {
        anyhow::bail!("Agent file already exists: {}", path.display());
    }

    let contents = if let Some(src) = file {
        std::fs::read_to_string(src)
            .with_context(|| format!("Failed to read {}", src.display()))?
    } else {
        format!("# {name}\n\nAgent instructions go here.\n")
    };

    std::fs::write(&path, &contents)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    println!("Created agent file: {}", path.display());
    Ok(())
}

fn cmd_agents_delete(name: &str) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    std::fs::remove_file(&path)
        .with_context(|| format!("Failed to delete {}", path.display()))?;
    println!("Deleted agent file: {name}");
    Ok(())
}
