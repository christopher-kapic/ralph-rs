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
mod io_util;
mod output;
mod plan;
mod plan_harness;
mod preflight;
mod prompt;
mod run_lock;
mod runner;
mod signal;
mod storage;
mod test_runner;
#[allow(dead_code)]
mod tui;
mod validate;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{
    AgentsCommand, Cli, Command, HooksCommand, PlanCommand, PlanDependencyCommand,
    PlanHarnessCommand, PlanPrependCommand, PromptCommand, StepCommand,
};

use crate::commands::resolve_project;
use crate::output::OutputContext;
use crate::plan::Plan;
use crate::runner::RunOptions;

/// Read the body for `ralph plan prepend set` from exactly one of the three
/// accepted input sources. Clap's `conflicts_with_all` guarantees at most
/// one of `text` / `file` / `stdin` is set; this helper enforces the
/// "at least one" half and normalises to a `String`.
fn resolve_prepend_input(
    text: Option<String>,
    file: Option<std::path::PathBuf>,
    stdin: bool,
) -> Result<String> {
    use std::io::Read;
    match (text, file, stdin) {
        (Some(t), None, false) => Ok(t),
        (None, Some(path), false) => std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read prepend source file: {}", path.display())),
        (None, None, true) => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("Failed to read prepend text from stdin")?;
            Ok(buf)
        }
        _ => anyhow::bail!(
            "Exactly one of --text, --file, or --stdin is required for `ralph plan prepend set`"
        ),
    }
}

/// Resolve a plan from an optional slug: if provided, look it up; otherwise
/// find the active plan for the project. `include_complete` controls whether
/// completed plans count as "active" (useful for status/log).
fn resolve_plan(
    conn: &rusqlite::Connection,
    slug: Option<String>,
    project: &str,
    include_complete: bool,
) -> Result<Plan> {
    match slug {
        Some(s) if s.is_empty() => {
            anyhow::bail!(
                "Plan slug cannot be empty. Specify a non-empty slug or omit the argument to use the active plan."
            )
        }
        Some(s) => storage::get_plan_by_slug(conn, &s, project)?
            .with_context(|| format!("Plan not found: {s}")),
        None => storage::find_active_plan(conn, project, include_complete)?
            .context("No active plan found. Specify a plan slug as a positional argument."),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration. For `init`, use an in-memory default so we don't
    // write config.json before cmd_init runs — otherwise its "does the config
    // already exist?" check would always be true on a fresh install, silently
    // skipping the interactive harness prompt.
    let config = if matches!(&cli.command, Command::Init { .. }) {
        config::Config::default()
    } else {
        config::load_or_create_config()?
    };

    // Open (or create) the database and run any pending migrations.
    let conn = db::open()?;

    // Resolve project directory for commands that need it.
    let project = resolve_project(cli.project.as_deref())?;

    // Build output context from global CLI flags.
    let out = OutputContext::from_cli(cli.json, cli.quiet, cli.no_color);

    match cli.command {
        // -- Init --
        Command::Init {
            non_interactive,
            default_harness,
            force,
        } => {
            let opts = commands::InitOptions {
                non_interactive,
                default_harness,
                force,
            };
            commands::cmd_init(&opts, &out)?;
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
                // Precedence: per-subcommand --harness overrides the global
                // --harness, which in turn falls back to the plan/config
                // default downstream.
                let h = harness.as_deref().or(cli.harness.as_deref());
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
            PlanCommand::Unarchive { slug } => {
                commands::plan_unarchive(&conn, &slug, &project, &out)
            }
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
                PlanHarnessCommand::Set { harness, plan } => {
                    let p = resolve_plan(&conn, plan, &project, false)?;
                    commands::plan_harness_set(&conn, &p.slug, &project, &harness, &out)
                }
                PlanHarnessCommand::Show { plan } => {
                    let p = resolve_plan(&conn, plan, &project, true)?;
                    commands::plan_harness_show(&conn, &p, &config, &out)
                }
                PlanHarnessCommand::Generate {
                    description,
                    plan,
                    use_harness,
                } => {
                    // Refuse to start the planner if a `ralph run` is live
                    // on this project. Concurrent planner + run corrupts plan
                    // state (the planner can reorder/delete steps the
                    // executor is about to run).
                    plan_harness::preflight_no_live_run(&conn, &project)?;

                    // When the user names a plan, resolve it so the harness
                    // receives a verified existing slug as its target. A
                    // missing plan is a hard error here rather than a silent
                    // fallthrough to "create something new" — if the user
                    // wanted a new plan, they'd omit the slug.
                    let plan_slug = match plan {
                        Some(slug) => Some(resolve_plan(&conn, Some(slug), &project, true)?.slug),
                        None => None,
                    };
                    let harness_name = use_harness
                        .or(cli.harness)
                        .unwrap_or_else(|| config.default_harness.clone());
                    let rt = tokio::runtime::Runtime::new()?;
                    let exit_code = rt.block_on(plan_harness::run_plan_harness(
                        &config,
                        &harness_name,
                        &project,
                        description.as_deref(),
                        plan_slug.as_deref(),
                    ))?;
                    std::process::exit(exit_code);
                }
            },
            PlanCommand::Prepend(prepend_cmd) => match prepend_cmd {
                PlanPrependCommand::Set {
                    plan,
                    text,
                    file,
                    stdin,
                } => {
                    let p = resolve_plan(&conn, plan, &project, true)?;
                    let body = resolve_prepend_input(text, file, stdin)?;
                    commands::plan_prepend_set(&conn, &p, &body, &out)
                }
                PlanPrependCommand::Show { plan, default } => {
                    let p = resolve_plan(&conn, plan, &project, true)?;
                    commands::plan_prepend_show(&conn, &p, default, &out)
                }
                PlanPrependCommand::Clear { plan } => {
                    let p = resolve_plan(&conn, plan, &project, true)?;
                    commands::plan_prepend_clear(&conn, &p, &out)
                }
            },
        },

        // -- Step --
        Command::Step(subcmd) => match subcmd {
            StepCommand::List { plan, tags } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_list(&conn, &p.slug, &project, &config, &tags, &out)
            }
            StepCommand::Add {
                title,
                plan,
                description,
                after,
                agent,
                harness,
                model,
                criteria,
                max_retries,
                change_policy,
                tags,
                import_json,
            } => {
                // Precedence: per-subcommand --harness overrides the global
                // --harness, which in turn falls back to the plan/config
                // default downstream.
                let h = harness.as_deref().or(cli.harness.as_deref());
                if let Some(source) = import_json {
                    // With --import-json, there is no step title; reinterpret
                    // a single positional as the plan slug. Error if the user
                    // supplied both positionals.
                    let plan_slug = match (title, plan) {
                        (Some(_), Some(_)) => anyhow::bail!(
                            "--import-json takes at most one positional (the plan slug); no title is accepted"
                        ),
                        (Some(t), None) => Some(t),
                        (None, p) => p,
                    };
                    let p = resolve_plan(&conn, plan_slug, &project, false)?;
                    commands::step_add_bulk(&conn, &p.slug, &project, &source, &out)
                } else {
                    let p = resolve_plan(&conn, plan, &project, false)?;
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
                        model.as_deref(),
                        &criteria,
                        max_retries,
                        change_policy,
                        &tags,
                        &out,
                    )
                }
            }
            StepCommand::Remove {
                step,
                step_id,
                plan,
                force,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_remove(
                    &conn,
                    &p.slug,
                    &project,
                    step,
                    step_id.as_deref(),
                    force,
                    &out,
                )
            }
            StepCommand::Edit {
                step,
                step_id,
                plan,
                title,
                description,
                agent,
                harness,
                model,
                criteria,
                max_retries,
                clear_max_retries,
                change_policy,
                tags,
                clear_tags,
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
                    agent.as_deref(),
                    harness.as_deref(),
                    model.as_deref(),
                    &criteria,
                    max_retries,
                    clear_max_retries,
                    change_policy,
                    &tags,
                    clear_tags,
                    &out,
                )
            }
            StepCommand::Reset {
                step,
                step_id,
                plan,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_reset(&conn, &p.slug, &project, step, step_id.as_deref(), &out)
            }
            StepCommand::Move {
                step,
                step_id,
                to,
                plan,
            } => {
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
                commands::cmd_step_set_hook(
                    &conn,
                    &p.slug,
                    &project,
                    step,
                    step_id.as_deref(),
                    lifecycle,
                    &hook,
                    &out,
                )
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
                    &conn,
                    &p.slug,
                    &project,
                    step,
                    step_id.as_deref(),
                    lifecycle,
                    &hook,
                    &out,
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
            no_auto_stash,
            harness: run_harness,
            force,
            verbose,
        } => {
            let workdir = std::path::Path::new(&project);
            // Precedence: `ralph run --harness X` beats `ralph --harness Y run`,
            // which in turn falls back to the plan's own harness and then the
            // config default. The per-subcommand flag is the most specific,
            // so it wins.
            let harness_override = run_harness.or(cli.harness);

            // `auto_stash` is default-on. `--no-auto-stash` forces it off
            // for a single run; `config.auto_stash = false` sets a per-user
            // default of "don't stash". The CLI flag always wins when set.
            let auto_stash = if no_auto_stash {
                false
            } else {
                config.auto_stash
            };
            let options = RunOptions {
                all_plans: all,
                one,
                from,
                to,
                current_branch,
                auto_stash,
                harness_override,
                dry_run,
                verbose,
            };

            if all {
                if from.is_some() || to.is_some() {
                    anyhow::bail!(
                        "--from/--to cannot be combined with --all (step numbers are per-plan and not comparable across plans)"
                    );
                }
                if plan_slug.is_some() {
                    eprintln!("Warning: plan slug argument is ignored when --all is set.");
                }

                // Acquire the per-project run lock so two concurrent `ralph run`
                // invocations can't clobber each other. Dry runs skip the lock
                // since they don't mutate state.
                let _run_lock = if !dry_run {
                    Some(run_lock::acquire(&conn, &project, None, None, force)?)
                } else {
                    None
                };

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
                        let results = preflight::run_preflight_checks(p, &config, workdir)?;
                        results.print_report(&out);
                        if !results.is_ok() {
                            any_errors = true;
                        }
                    }
                    if any_errors {
                        anyhow::bail!(
                            "Preflight checks failed for one or more plans. Use --skip-preflight to bypass."
                        );
                    }
                }

                let rt = tokio::runtime::Runtime::new()?;
                let results = rt.block_on(async {
                    let abort_rx = signal::install_and_spawn();
                    runner::run_all_plans(
                        &conn, &project, &config, workdir, &options, abort_rx, &out,
                    )
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

            // Acquire the per-project run lock before doing any mutating work.
            // Dry runs skip the lock.
            let _run_lock = if !dry_run {
                Some(run_lock::acquire(
                    &conn,
                    &project,
                    Some(&plan.slug),
                    Some(&plan.id),
                    force,
                )?)
            } else {
                None
            };

            // Preflight checks
            if !skip_preflight && !dry_run {
                eprintln!("Running preflight checks...");
                let preflight_results = preflight::run_preflight_checks(&plan, &config, workdir)?;
                preflight_results.print_report(&out);

                if !preflight_results.is_ok() {
                    anyhow::bail!("Preflight checks failed. Use --skip-preflight to bypass.");
                }
            }

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::run_plan(&conn, &plan, &config, workdir, &options, abort_rx, &out).await
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
        Command::Resume {
            plan: plan_slug,
            force,
        } => {
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;
            let slug = plan.slug.clone();

            // Acquire the same per-project run lock that `ralph run` uses, so
            // resume can't race a concurrent run or skip.
            let _run_lock =
                run_lock::acquire(&conn, &project, Some(&plan.slug), Some(&plan.id), force)?;

            let rt = tokio::runtime::Runtime::new()?;
            let result = rt.block_on(async {
                let abort_rx = signal::install_and_spawn();
                runner::resume_plan(&conn, &plan, &config, project.as_ref(), abort_rx, &out).await
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

        // -- Cancel --
        Command::Cancel {
            plan: plan_slug,
            force,
            timeout,
        } => commands::cmd_cancel(
            &conn,
            &project,
            plan_slug.as_deref(),
            force,
            std::time::Duration::from_secs(timeout),
            &out,
        ),

        // -- Skip --
        Command::Skip {
            plan: plan_slug,
            step: step_num,
            reason,
            force,
        } => {
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;

            // Acquire the same per-project run lock that `ralph run` uses, so
            // skip can't race a concurrent run or resume.
            let _run_lock =
                run_lock::acquire(&conn, &project, Some(&plan.slug), Some(&plan.id), force)?;

            runner::skip_step(&conn, &plan, step_num, reason.as_deref())?;
            Ok(())
        }

        // -- Export --
        Command::Export { plan, output } => {
            export::export_plan(&conn, &plan, &project, output.as_deref())
        }

        // -- Import --
        Command::Import {
            file,
            slug,
            branch,
            strict,
        } => {
            let h = cli.harness.as_deref();
            import::import_plan(
                &conn,
                &file,
                &project,
                slug.as_deref(),
                branch.as_deref(),
                h,
                strict,
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
            commands::cmd_log(
                &conn,
                &project,
                plan.as_deref(),
                step,
                limit,
                &output_mode,
                &out,
            )
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
            HooksCommand::Export { output, all, path } => {
                commands::cmd_hooks_export(&project, output.as_deref(), all, path.as_deref(), &out)
            }
            HooksCommand::Import { file, force } => commands::cmd_hooks_import(&file, force, &out),
        },

        // -- Prompt --
        Command::Prompt(subcmd) => {
            let config_path = config::config_dir()?.join("config.json");
            match subcmd {
                PromptCommand::Show {
                    plan,
                    scope,
                    resolved,
                } => commands::cmd_prompt_show(
                    &conn,
                    &config,
                    &project,
                    plan.as_deref(),
                    scope,
                    resolved,
                    &out,
                ),
                PromptCommand::Set {
                    scope,
                    prefix,
                    suffix,
                    plan,
                } => commands::cmd_prompt_set(
                    &conn,
                    &config_path,
                    &project,
                    scope,
                    plan.as_deref(),
                    prefix.as_deref(),
                    suffix.as_deref(),
                    &out,
                ),
                PromptCommand::Clear {
                    scope,
                    prefix,
                    suffix,
                    plan,
                } => commands::cmd_prompt_clear(
                    &conn,
                    &config_path,
                    &project,
                    scope,
                    plan.as_deref(),
                    prefix,
                    suffix,
                    &out,
                ),
            }
        }

        // -- Doctor --
        Command::Doctor => commands::cmd_doctor(&config, std::path::Path::new(&project), &out),

        // -- Config --
        Command::Config(sub) => match sub {
            cli::ConfigCommand::Show => commands::config_cmd::config_show(&out),
            cli::ConfigCommand::SetTimezone { tz } => {
                commands::config_cmd::config_set_timezone(&tz)
            }
        },

        // -- Completions --
        Command::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(shell, &mut Cli::command(), "ralph", &mut std::io::stdout());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_plan_rejects_empty_slug() {
        let conn = db::open_memory().expect("open in-memory db");
        let err = resolve_plan(&conn, Some(String::new()), "/tmp/proj", false)
            .expect_err("empty slug must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("empty"),
            "error should mention empty slug, got: {msg}"
        );
    }
}
