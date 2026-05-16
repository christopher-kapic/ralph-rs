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
    PlanHarnessCommand, PromptCommand, QuestionCommand, QuestionsState, StepCommand,
};

use crate::commands::{resolve_plan, resolve_project};
use crate::output::OutputContext;

/// Read the body for `ralph plan prepend set` from exactly one of the three
/// accepted input sources. Clap's `conflicts_with_all` guarantees at most
/// one of `text` / `file` / `stdin` is set; this helper enforces the
/// "at least one" half and normalises to a `String`.
fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration. For `init`, use an in-memory default so we don't
    // write config.json before cmd_init runs — otherwise its "does the config
    // already exist?" check would always be true on a fresh install, silently
    // skipping the interactive harness prompt.
    let config = if matches!(&cli.command, Some(Command::Init { .. })) {
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

    // Bare `ralph` (no subcommand) routes to the TUI plan-list view when
    // stdout is a TTY and the user hasn't asked for non-interactive output.
    // Anything else (piped stdout, --json, --non-interactive) prints clap
    // help so scripts and ` ralph | cat` still get something useful.
    let command = match cli.command {
        Some(c) => c,
        None => {
            let stdout_is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
            if stdout_is_tty && !cli.json && !cli.non_interactive {
                return commands::run_plan_list_tui(&conn, &config, &project, &out, None);
            }
            use clap::CommandFactory;
            let _ = Cli::command().print_help();
            println!();
            return Ok(());
        }
    };

    match command {
        // -- Init --
        Command::Init {
            non_interactive,
            default_harness,
            force,
            restore_prompts,
        } => {
            let opts = commands::InitOptions {
                non_interactive,
                default_harness,
                force,
                restore_prompts,
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
                    if exit_code == 0 {
                        return Ok(());
                    }
                    // Drop the SQLite connection and tokio runtime explicitly
                    // before process::exit so OS handles close cleanly —
                    // process::exit does not run destructors. We still need
                    // process::exit (rather than a normal return) to preserve
                    // the harness's specific non-zero exit code.
                    drop(rt);
                    drop(conn);
                    std::process::exit(exit_code);
                }
            },
            PlanCommand::Questions { state, slug } => {
                let enabled = matches!(state, QuestionsState::On);
                commands::cmd_plan_questions(&conn, &slug, &project, enabled, &out)
            }
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
                clear_criteria,
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
                    clear_criteria,
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
            let args = commands::RunArgs {
                plan_slug,
                one,
                all,
                from,
                to,
                dry_run,
                skip_preflight,
                current_branch,
                no_auto_stash,
                run_harness,
                force,
                verbose,
                cli_harness: cli.harness,
                non_interactive: cli.non_interactive,
                json: cli.json,
            };

            // TUI-plan.md §2 routing: bare `ralph run` / `ralph run <slug>`
            // from a TTY drops into TUI mode. Every other invocation (any
            // non-default flag, `--non-interactive`, or non-TTY stdout) takes
            // today's runner path unchanged so scripts see no regression.
            let stdout_is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
            if commands::is_default_run_invocation(&args, stdout_is_tty) {
                commands::run_tui_mode(&conn, &config, &project, args, &out)
            } else {
                commands::dispatch_run(&conn, &config, &project, args, &out)
            }
        }

        // -- Resume --
        Command::Resume {
            plan: plan_slug,
            force,
        } => {
            let args = commands::ResumeArgs {
                plan_slug,
                force,
                non_interactive: cli.non_interactive,
                json: cli.json,
                quiet: cli.quiet,
                cli_harness: cli.harness,
            };

            // TUI-plan.md §2 (extended to resume per step 34): bare
            // `ralph resume` / `ralph resume <slug>` from a TTY drops into
            // the same plan-detail TUI that `ralph run` uses, with the
            // streaming subprocess started via `ralph resume` instead of
            // `ralph run`. Every other invocation (--non-interactive,
            // --json, --quiet, --harness, --force, or non-TTY stdout)
            // takes today's CLI runner path unchanged so scripts see no
            // regression.
            let stdout_is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
            if commands::is_default_resume_invocation(&args, stdout_is_tty) {
                commands::run_resume_tui_mode(&conn, &config, &project, args, &out)
            } else {
                commands::dispatch_resume(&conn, &config, &project, args, &out)
            }
        }

        // -- Pause --
        Command::Pause { plan: plan_slug } => {
            commands::cmd_pause(&conn, &project, plan_slug.as_deref(), cli.quiet)
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
            changes,
            force,
        } => {
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;

            // Acquire the same per-project run lock that `ralph run` uses, so
            // skip can't race a concurrent run or resume.
            let _run_lock =
                run_lock::acquire(&conn, &project, Some(&plan.slug), Some(&plan.id), force)?;

            runner::skip_step(&conn, &plan, step_num, reason.as_deref(), changes.into())?;
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

        // -- Question --
        Command::Question(subcmd) => match subcmd {
            QuestionCommand::Ask { question, suggest } => {
                use crate::commands::question::{
                    DISABLED_MESSAGE, NO_ACTIVE_RUN_MESSAGE, QuestionAskOutcome,
                    record_question_ask,
                };
                use std::io::Read;

                // Spec: positional arg if present, else stdin. Trim trailing
                // whitespace (heredoc invocations always include a final
                // newline) but leave embedded whitespace alone.
                let q = match question {
                    Some(t) => t,
                    None => {
                        let mut buf = String::new();
                        std::io::stdin()
                            .read_to_string(&mut buf)
                            .context("Failed to read question text from stdin")?;
                        buf.trim_end().to_string()
                    }
                };

                match record_question_ask(&conn, &project, &q, &suggest)? {
                    QuestionAskOutcome::NoActiveRun => {
                        eprintln!("{NO_ACTIVE_RUN_MESSAGE}");
                        std::process::exit(1);
                    }
                    QuestionAskOutcome::Disabled => {
                        eprintln!("{DISABLED_MESSAGE}");
                        std::process::exit(1);
                    }
                    QuestionAskOutcome::Recorded { .. } => Ok(()),
                }
            }
            QuestionCommand::List { plan } => {
                commands::question::cmd_question_list(&conn, &project, plan.as_deref(), &out)
            }
            QuestionCommand::Answer { num, text } => {
                use std::io::Read;
                let answer = match text {
                    Some(t) => t,
                    None => {
                        let mut buf = String::new();
                        std::io::stdin()
                            .read_to_string(&mut buf)
                            .context("Failed to read answer text from stdin")?;
                        buf.trim_end().to_string()
                    }
                };
                commands::question::cmd_question_answer(&conn, &project, num, &answer, &out)
            }
            QuestionCommand::Show { num } => {
                commands::question::cmd_question_show(&conn, &project, num, &out)
            }
        },

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
                PromptCommand::Show { scope, resolved } => commands::cmd_prompt_show(
                    &conn, &config, &project, scope, resolved, &out,
                ),
                PromptCommand::Set { scope, content } => commands::cmd_prompt_set(
                    &conn,
                    &config_path,
                    &project,
                    scope,
                    &content,
                    &out,
                ),
                PromptCommand::Clear { scope } => {
                    commands::cmd_prompt_clear(&conn, &config_path, &project, scope, &out)
                }
            }
        }

        // -- Doctor --
        Command::Doctor => commands::cmd_doctor(&config, std::path::Path::new(&project), &out),

        // -- Harness (read-only inspection) --
        Command::Harness(sub) => match sub {
            cli::HarnessCommand::List { json } => {
                commands::harness::harness_list(&config, json, &out)
            }
            cli::HarnessCommand::Show { name, json } => {
                commands::harness::harness_show(&config, &name, json, &out)
            }
        },

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
