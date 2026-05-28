mod cli;
mod commands;
mod config;
mod dag_util;
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
mod review;
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
    AgentsCommand, Cli, Command, HooksCommand, InterruptionCommand, OnOffState, PlanCommand,
    PlanDependencyCommand, PlanHarnessCommand, PromptCommand, QuestionCommand, StepCommand,
    StepDependencyCommand,
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
                retry_strategy,
                squash_on_complete,
                max_review_corrections,
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
                    retry_strategy,
                    squash_on_complete,
                    max_review_corrections,
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
                        // Non-fatal DAG sanity check: an authoring harness
                        // that expressed ordering by array/positional order
                        // instead of real edges produces an all-roots,
                        // edge-less plan that "runs" but has none of the
                        // intended gating. `ralph import` validates; `plan
                        // harness generate` had no such guard. Warn (never
                        // fail) and point at how to inspect/fix.
                        plan_harness::warn_if_edgeless_dag(&conn, &project, plan_slug.as_deref());
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
            PlanCommand::Review { state, slug } => {
                let enabled = matches!(state, OnOffState::On);
                commands::cmd_plan_review(&conn, &slug, &project, enabled, &out)
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
                before,
                root,
                agent,
                harness,
                model,
                criteria,
                max_retries,
                change_policy,
                retry_strategy,
                tags,
                depends_on,
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
                        after.as_deref(),
                        before.as_deref(),
                        root,
                        agent.as_deref(),
                        h,
                        model.as_deref(),
                        &criteria,
                        max_retries,
                        change_policy,
                        retry_strategy,
                        &tags,
                        &depends_on,
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
                    step.as_deref(),
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
                retry_strategy,
                clear_retry_strategy,
                review,
                tags,
                clear_tags,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_edit(
                    &conn,
                    &p.slug,
                    &project,
                    step.as_deref(),
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
                    retry_strategy,
                    clear_retry_strategy,
                    review.map(|r| r.to_override()),
                    &tags,
                    clear_tags,
                    &out,
                )
            }
            StepCommand::Reset {
                step,
                step_id,
                plan,
                force,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_reset(
                    &conn,
                    &p.slug,
                    &project,
                    step.as_deref(),
                    step_id.as_deref(),
                    force,
                    &out,
                )
            }
            StepCommand::Move {
                step,
                step_id,
                to,
                plan,
            } => {
                let p = resolve_plan(&conn, plan, &project, false)?;
                commands::step_move(
                    &conn,
                    &p.slug,
                    &project,
                    step.as_deref(),
                    step_id.as_deref(),
                    to,
                    &out,
                )
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
                    step.as_deref(),
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
                    step.as_deref(),
                    step_id.as_deref(),
                    lifecycle,
                    &hook,
                    &out,
                )
            }
            StepCommand::Dependency(dep_cmd) => match dep_cmd {
                StepDependencyCommand::Add {
                    step,
                    plan,
                    depends_on,
                } => {
                    let p = resolve_plan(&conn, plan, &project, false)?;
                    commands::step_dependency_add(
                        &conn,
                        &p.slug,
                        &project,
                        &step,
                        &depends_on,
                        &out,
                    )
                }
                StepDependencyCommand::Remove {
                    step,
                    plan,
                    depends_on,
                } => {
                    let p = resolve_plan(&conn, plan, &project, false)?;
                    commands::step_dependency_remove(
                        &conn,
                        &p.slug,
                        &project,
                        &step,
                        &depends_on,
                        &out,
                    )
                }
                StepDependencyCommand::List { step, plan } => {
                    let p = resolve_plan(&conn, plan, &project, true)?;
                    commands::step_dependency_list(&conn, &p.slug, &project, &step, &out)
                }
            },
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
            force: _force,
        } => {
            let plan = resolve_plan(&conn, plan_slug, &project, false)?;

            // Deliberately NOT gated behind the per-project run lock. A live
            // `ralph run` holds that lock for its entire duration; acquiring
            // it here would make it impossible to skip the *currently
            // running* step — the headline use case. `runner::skip_step`
            // routes an in-flight skip through the cross-process DB bridge
            // (`plans.skip_requested_step_id`), which the running runner
            // polls and consumes mid-attempt; a non-running step is a plain
            // synchronous DB flip. Both are single-row writes safe to race a
            // concurrent run (same lock-free model as `ralph pause`). The
            // legacy `--force` flag is accepted for compatibility but no
            // longer has a lock to steal.
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
            // `--strict` rejects a bundle that would enable review on this
            // machine when no usable review harness is configured
            // (docs/dag-redesign.md §13.3). Inherited global review defaults
            // count too.
            let review_harness_configured = crate::preflight::review_harness_is_usable(&config);
            let global_review_enabled = config.review.enabled.unwrap_or(false);
            import::import_plan(
                &conn,
                &file,
                &project,
                slug.as_deref(),
                branch.as_deref(),
                h,
                strict,
                review_harness_configured,
                global_review_enabled,
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
            QuestionCommand::List { plan } => commands::interruption::cmd_interruption_list(
                &conn,
                &project,
                plan.as_deref(),
                &out,
            ),
            QuestionCommand::Answer { id, text } => {
                commands::interruption::cmd_interruption_resolve(
                    &conn,
                    &project,
                    None,
                    &id,
                    None,
                    Some(&text),
                    None,
                    &out,
                )
            }
            QuestionCommand::Ask {
                question,
                suggest,
                priority,
            } => {
                use crate::commands::question::{
                    NO_ACTIVE_RUN_MESSAGE, QuestionAskOutcome, record_question_ask,
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

                match record_question_ask(&conn, &project, &q, &suggest, &priority, &out)? {
                    QuestionAskOutcome::NoActiveRun => {
                        eprintln!("{NO_ACTIVE_RUN_MESSAGE}");
                        std::process::exit(1);
                    }
                    QuestionAskOutcome::Recorded { .. } => Ok(()),
                }
            }
        },

        // -- Block (raise a blocker interruption) --
        Command::Block { text } => {
            use crate::commands::question::{
                BLOCK_NO_ACTIVE_RUN_MESSAGE, QuestionAskOutcome, record_block,
            };
            use std::io::Read;

            let body = match text {
                Some(t) => t,
                None => {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .context("Failed to read blocker text from stdin")?;
                    buf.trim_end().to_string()
                }
            };

            match record_block(&conn, &project, &body, &out)? {
                QuestionAskOutcome::NoActiveRun => {
                    eprintln!("{BLOCK_NO_ACTIVE_RUN_MESSAGE}");
                    std::process::exit(1);
                }
                QuestionAskOutcome::Recorded { .. } => Ok(()),
            }
        }

        // -- Interruption (human-side list/show/resolve) --
        Command::Interruption(subcmd) => match subcmd {
            InterruptionCommand::List { plan } => commands::interruption::cmd_interruption_list(
                &conn,
                &project,
                plan.as_deref(),
                &out,
            ),
            InterruptionCommand::Show { plan, id } => {
                commands::interruption::cmd_interruption_show(
                    &conn,
                    &project,
                    plan.as_deref(),
                    &id,
                    &out,
                )
            }
            InterruptionCommand::Resolve {
                plan,
                id,
                option,
                answer,
                comment,
            } => commands::interruption::cmd_interruption_resolve(
                &conn,
                &project,
                plan.as_deref(),
                &id,
                option,
                answer.as_deref(),
                comment.as_deref(),
                &out,
            ),
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
                PromptCommand::Show { scope, resolved } => {
                    commands::cmd_prompt_show(&conn, &config, &project, scope, resolved, &out)
                }
                PromptCommand::Set { scope, content } => {
                    commands::cmd_prompt_set(&conn, &config_path, &project, scope, &content, &out)
                }
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
            cli::ConfigCommand::Review(cli::ConfigReviewCommand::Set {
                harness,
                model,
                enabled,
            }) => commands::config_cmd::config_review_set(
                harness.as_deref(),
                model.as_deref(),
                enabled,
            ),
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
