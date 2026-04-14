mod cli;
mod commands;
mod config;
mod db;
mod executor;
mod export;
mod frac_index;
mod git;
mod harness;
mod hook_library;
mod hooks;
mod import;
mod output;
mod plan;
mod plan_harness;
mod preflight;
mod prompt;
mod runner;
mod signal;
mod storage;
mod test_runner;
#[allow(dead_code)]
mod tui;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{
    AgentsCommand, Cli, Command, HooksCommand, PlanCommand, PlanDependencyCommand,
    PlanHarnessCommand, StepCommand,
};

use crate::commands::resolve_project;
use crate::output::OutputContext;
use crate::plan::Plan;
use crate::runner::RunOptions;

/// Resolve a plan from an optional slug: if provided, look it up; otherwise
/// find the active plan for the project. `include_complete` controls whether
/// completed plans count as "active" (useful for status/log).
fn resolve_plan(
    conn: &rusqlite::Connection,
    slug: Option<String>,
    project: &str,
    include_complete: bool,
) -> Result<Plan> {
    if let Some(s) = slug.filter(|s| !s.is_empty()) {
        storage::get_plan_by_slug(conn, &s, project)?
            .with_context(|| format!("Plan not found: {s}"))
    } else {
        storage::find_active_plan(conn, project, include_complete)?
            .context("No active plan found. Specify a plan slug as a positional argument.")
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration (creates default if absent).
    let _config = config::load_or_create_config()?;

    // Open (or create) the database and run any pending migrations.
    let conn = db::open()?;

    // Resolve project directory for commands that need it.
    let project = resolve_project(cli.project.as_deref())?;

    // Build output context from global CLI flags.
    let out = OutputContext::from_cli(cli.json, cli.quiet, cli.no_color);

    match cli.command {
        // -- Init --
        Command::Init { .. } => {
            commands::cmd_init(&out)?;
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
                depends_on,
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
                    &depends_on,
                    &out,
                )
            }
            PlanCommand::List {
                all,
                status,
                archived,
            } => commands::plan_list(&conn, &project, all, status, archived, &out),
            PlanCommand::Show { slug } => commands::plan_show(&conn, &slug, &project, &out),
            PlanCommand::Approve { slug } => commands::plan_approve(&conn, &slug, &project, &out),
            PlanCommand::Delete { slug, force } => {
                commands::plan_delete(&conn, &slug, &project, force, &out)
            }
            PlanCommand::Archive { slug } => commands::plan_archive(&conn, &slug, &project, &out),
            PlanCommand::Unarchive { slug } => commands::plan_unarchive(&conn, &slug, &project, &out),
            PlanCommand::SetHook {
                slug,
                lifecycle,
                hook,
            } => commands::cmd_plan_set_hook(&conn, &slug, &project, lifecycle, &hook, &out),
            PlanCommand::UnsetHook {
                slug,
                lifecycle,
                hook,
            } => commands::cmd_plan_unset_hook(&conn, &slug, &project, lifecycle, &hook, &out),
            PlanCommand::Hooks { slug } => commands::cmd_plan_hooks(&conn, &slug, &project, &out),
            PlanCommand::Dependency(dep_cmd) => match dep_cmd {
                PlanDependencyCommand::Add { slug, depends_on } => {
                    commands::plan_dependency_add(&conn, &slug, &project, &depends_on, &out)
                }
                PlanDependencyCommand::Remove { slug, depends_on } => {
                    commands::plan_dependency_remove(&conn, &slug, &project, &depends_on, &out)
                }
                PlanDependencyCommand::List { slug } => {
                    commands::plan_dependency_list(&conn, &slug, &project, &out)
                }
            },
            PlanCommand::Harness(harness_cmd) => match harness_cmd {
                PlanHarnessCommand::Set { plan, .. } => {
                    // Resolve (or default) the plan slug for future implementation.
                    let _ = plan;
                    Ok(())
                }
                PlanHarnessCommand::Show { plan } => {
                    let _ = plan;
                    Ok(())
                }
                PlanHarnessCommand::Generate {
                    description,
                    plan,
                    use_harness,
                } => {
                    let _ = plan;
                    let harness_name = use_harness
                        .or(cli.harness)
                        .unwrap_or_else(|| _config.default_harness.clone());
                    let rt = tokio::runtime::Runtime::new()?;
                    let exit_code = rt.block_on(plan_harness::run_plan_harness(
                        &_config,
                        &harness_name,
                        &project,
                        description.as_deref(),
                    ))?;
                    std::process::exit(exit_code);
                }
            },
        },

        // -- Step --
        Command::Step(subcmd) => match subcmd {
            StepCommand::List { plan } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_list(&conn, &p.slug, &project, &out)
            }
            StepCommand::Add {
                title,
                plan,
                description,
                after,
                agent,
                harness,
                criteria,
                max_retries,
                import_json,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                let h = cli.harness.as_deref().or(harness.as_deref());
                if let Some(source) = import_json {
                    commands::step_add_bulk(&conn, &p.slug, &project, &source, &out)
                } else {
                    // clap enforces that `title` is Some when `--import-json`
                    // is absent via `required_unless_present`.
                    let title = title.as_deref().expect("clap guarantees title is present");
                    commands::step_add(
                        &conn,
                        &p.slug,
                        &project,
                        title,
                        description.as_deref(),
                        after,
                        agent.as_deref(),
                        h,
                        &criteria,
                        max_retries,
                        &out,
                    )
                }
            }
            StepCommand::Remove { step, step_id, plan, force } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_remove(&conn, &p.slug, &project, step, step_id.as_deref(), force, &out)
            }
            StepCommand::Edit {
                step,
                step_id,
                plan,
                title,
                description,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_edit(
                    &conn,
                    &p.slug,
                    &project,
                    step,
                    step_id.as_deref(),
                    title.as_deref(),
                    description.as_deref(),
                    &out,
                )
            }
            StepCommand::Reset { step, step_id, plan } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_reset(&conn, &p.slug, &project, step, step_id.as_deref(), &out)
            }
            StepCommand::Move { step, step_id, to, plan } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_move(&conn, &p.slug, &project, step, step_id.as_deref(), to, &out)
            }
            StepCommand::SetHook {
                step,
                step_id,
                plan,
                lifecycle,
                hook,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::cmd_step_set_hook(&conn, &p.slug, &project, step, step_id.as_deref(), lifecycle, &hook, &out)
            }
            StepCommand::UnsetHook {
                step,
                step_id,
                plan,
                lifecycle,
                hook,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::cmd_step_unset_hook(
                    &conn, &p.slug, &project, step, step_id.as_deref(), lifecycle, &hook, &out,
                )
            }
        },

        // -- Run --
        Command::Run {
            plan: plan_slug,
            one,
            all,
            from,
            to,
            dry_run,
            skip_preflight,
            current_branch,
            harness: run_harness,
        } => {
            let workdir = std::path::Path::new(&project);
            let harness_override = cli.harness.or(run_harness);

            let options = RunOptions {
                all_plans: all,
                one,
                from,
                to,
                current_branch,
                harness_override,
                dry_run,
            };

            if all {
                if from.is_some() || to.is_some() {
                    anyhow::bail!(
                        "--from/--to cannot be combined with --all (step numbers are per-plan and not comparable across plans)"
                    );
                }
                if plan_slug.is_some() {
                    eprintln!("Warning: --plan is ignored when --all is set.");
                }

                // Preflight every runnable plan before starting the chain so we
                // fail fast if anything is misconfigured.
                if !skip_preflight && !dry_run {
                    let runnable: Vec<_> = storage::list_plans(&conn, &project, false)?
                        .into_iter()
                        .filter(|p| {
                            matches!(
                                p.status,
                                plan::PlanStatus::Ready
                                    | plan::PlanStatus::InProgress
                                    | plan::PlanStatus::Failed
                            )
                        })
                        .collect();

                    let mut any_errors = false;
                    for p in &runnable {
                        eprintln!("Running preflight checks for '{}'...", p.slug);
                        let results = preflight::run_preflight_checks(p, &_config, workdir)?;
                        results.print_report();
                        if !results.is_ok() {
                            any_errors = true;
                        }
                    }
                    if any_errors {
                        anyhow::bail!(
                            "Preflight checks failed for one or more plans. Use --skip-preflight to bypass."
                        );
                    }

                    // Auto-stash dirty git state once before the whole chain.
                    if preflight::auto_stash_dirty_state(workdir)? {
                        eprintln!("  Auto-committed dirty state before run.");
                    }
                }

                let rt = tokio::runtime::Runtime::new()?;
                let results = rt.block_on(async {
                    let abort_rx = signal::install_and_spawn();
                    runner::run_all_plans(&conn, &project, &_config, workdir, &options, abort_rx, &out)
                        .await
                })?;

                let total = results.len();
                let mut succeeded = 0usize;
                let mut failed = 0usize;
                for r in &results {
                    eprintln!(
                        "  - {}: {} ({}/{} steps succeeded)",
                        r.plan_slug, r.final_status, r.steps_succeeded, r.steps_executed
                    );
                    if r.final_status == plan::PlanStatus::Complete {
                        succeeded += 1;
                    } else {
                        failed += 1;
                    }
                }
                eprintln!(
                    "Ran {} plan(s): {} succeeded, {} failed",
                    total, succeeded, failed
                );
                return Ok(());
            }

            // Single-plan run path.
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;
            let slug = plan.slug.clone();

            // Preflight checks
            if !skip_preflight && !dry_run {
                eprintln!("Running preflight checks...");
                let preflight_results = preflight::run_preflight_checks(&plan, &_config, workdir)?;
                preflight_results.print_report();

                if !preflight_results.is_ok() {
                    anyhow::bail!("Preflight checks failed. Use --skip-preflight to bypass.");
                }

                // Auto-stash dirty git state
                if preflight::auto_stash_dirty_state(workdir)? {
                    eprintln!("  Auto-committed dirty state before run.");
                }
            }

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::run_plan(&conn, &plan, &_config, workdir, &options, abort_rx, &out).await
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
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;
            let slug = plan.slug.clone();

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::resume_plan(&conn, &plan, &_config, project.as_ref(), abort_rx, &out).await
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
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;

            runner::skip_step(&conn, &plan, step_num, reason.as_deref())?;
            Ok(())
        }

        // -- Export --
        Command::Export { plan, output } => {
            export::export_plan(&conn, &plan, &project, output.as_deref())
        }

        // -- Import --
        Command::Import { file, slug, branch } => {
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
            commands::cmd_status(&conn, &project, plan.as_deref(), verbose, &out)
        }

        // -- Log --
        Command::Log {
            plan,
            step,
            limit,
            full,
            lines,
        } => {
            let output_mode = if full {
                commands::LogOutputMode::Full
            } else if let Some(n) = lines {
                commands::LogOutputMode::Truncated(n)
            } else {
                commands::LogOutputMode::Hidden
            };
            commands::cmd_log(&conn, &project, plan.as_deref(), step, limit, &output_mode, &out)
        }

        // -- Agents --
        Command::Agents(subcmd) => match subcmd {
            AgentsCommand::List => commands::cmd_agents_list(&out),
            AgentsCommand::Show { name } => commands::cmd_agents_show(&name, &out),
            AgentsCommand::Create { name, file } => {
                commands::cmd_agents_create(&name, file.as_deref(), &out)
            }
            AgentsCommand::Delete { name } => commands::cmd_agents_delete(&name, &out),
        },

        // -- Hooks --
        Command::Hooks(subcmd) => match subcmd {
            HooksCommand::List { all } => commands::cmd_hooks_list(&project, all, &out),
            HooksCommand::Show { name } => commands::cmd_hooks_show(&name, &out),
            HooksCommand::Add {
                name,
                lifecycle,
                command,
                description,
                scope_paths,
                force,
            } => commands::cmd_hooks_add(
                &name,
                lifecycle,
                &command,
                description.as_deref(),
                &scope_paths,
                force,
                &out,
            ),
            HooksCommand::Remove { name } => commands::cmd_hooks_remove(&name, &out),
            HooksCommand::Export { output, all, path } => commands::cmd_hooks_export(
                &project,
                output.as_deref(),
                all,
                path.as_deref(),
                &out,
            ),
            HooksCommand::Import { file, force } => commands::cmd_hooks_import(&file, force, &out),
        },

        // -- Doctor --
        Command::Doctor => commands::cmd_doctor(&_config, &out),

        // -- Completions --
        Command::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(shell, &mut Cli::command(), "ralph", &mut std::io::stdout());
            Ok(())
        }
    }
}
