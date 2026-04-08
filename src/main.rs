mod cli;
mod commands;
mod config;
mod db;
mod executor;
mod export;
mod frac_index;
mod git;
mod harness;
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
mod tui;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{AgentsCommand, Cli, Command, PlanCommand, PlanHarnessCommand, StepCommand};
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
            commands::cmd_init()?;
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
            PlanCommand::List {
                all,
                status,
                archived,
            } => commands::plan_list(&conn, &project, all, status.as_deref(), archived),
            PlanCommand::Show { slug } => commands::plan_show(&conn, &slug, &project),
            PlanCommand::Approve { slug } => commands::plan_approve(&conn, &slug, &project),
            PlanCommand::Delete { slug, force } => {
                commands::plan_delete(&conn, &slug, &project, force)
            }
            PlanCommand::Archive { slug } => commands::plan_archive(&conn, &slug, &project),
            PlanCommand::Unarchive { slug } => commands::plan_unarchive(&conn, &slug, &project),
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
            commands::cmd_status(&conn, &project, plan.as_deref(), verbose)
        }

        // -- Log --
        Command::Log {
            plan,
            step,
            limit,
            full,
        } => commands::cmd_log(&conn, &project, plan.as_deref(), step, limit, full),

        // -- Agents --
        Command::Agents(subcmd) => match subcmd {
            AgentsCommand::List => commands::cmd_agents_list(),
            AgentsCommand::Show { name } => commands::cmd_agents_show(&name),
            AgentsCommand::Create { name, file } => {
                commands::cmd_agents_create(&name, file.as_deref())
            }
            AgentsCommand::Delete { name } => commands::cmd_agents_delete(&name),
        },

        // -- Doctor --
        Command::Doctor => commands::cmd_doctor(&_config),

        // -- Completions --
        Command::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "ralph-rs",
                &mut std::io::stdout(),
            );
            Ok(())
        }
    }
}
