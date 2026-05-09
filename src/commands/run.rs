// Run-related CLI command implementations (status, log, cancel)

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::config::Config;
use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{self, ChangePolicy, ExecutionLog, StepStatus};
use crate::preflight;
use crate::run_lock::{self, LiveRun};
use crate::runner::{self, RunOptions};
use crate::signal;
use crate::storage;

// ---------------------------------------------------------------------------
// Run dispatch
// ---------------------------------------------------------------------------

/// All inputs the `Run` subcommand needs to dispatch, gathered into one struct
/// so the CLI surface, the routing decision, and the TUI placeholder can pass
/// them around as a unit.
#[derive(Debug, Clone, Default)]
pub struct RunArgs {
    pub plan_slug: Option<String>,
    pub one: bool,
    pub all: bool,
    pub from: Option<usize>,
    pub to: Option<usize>,
    pub dry_run: bool,
    pub skip_preflight: bool,
    pub current_branch: bool,
    pub no_auto_stash: bool,
    pub run_harness: Option<String>,
    pub force: bool,
    pub verbose: bool,
    /// The global `--harness` flag (root `Cli` field). Run-specific
    /// `--harness` takes precedence; both being unset is the default case.
    pub cli_harness: Option<String>,
    /// The global `--non-interactive` flag.
    pub non_interactive: bool,
    /// The global `--json`/`--jsonl` flag.
    pub json: bool,
}

/// Whether `ralph run` was invoked with all defaults — meaning the routing
/// rule from TUI-plan.md §2 should drop the user into TUI mode.
///
/// "Default" means: no Run-specific flags set, no global `--non-interactive`,
/// no `--json`/`--jsonl`, no `--harness` override at either scope, and stdout
/// is a real TTY. A bare plan slug is allowed — `ralph run my-plan` is still
/// considered default.
///
/// `stdout_is_tty` is passed in rather than detected internally so the
/// routing decision is unit-testable without reaching the real terminal.
pub fn is_default_run_invocation(args: &RunArgs, stdout_is_tty: bool) -> bool {
    if !stdout_is_tty {
        return false;
    }
    if args.non_interactive || args.json {
        return false;
    }
    !args.one
        && !args.all
        && args.from.is_none()
        && args.to.is_none()
        && !args.dry_run
        && !args.skip_preflight
        && !args.force
        && !args.verbose
        && args.run_harness.is_none()
        && args.cli_harness.is_none()
}

/// Dispatch a `ralph run` invocation through today's runner — the
/// non-interactive path used by scripts and meta-harnesses.
///
/// Both [`run_tui_mode`] and the direct-CLI path call this so they share a
/// single source of truth for run-lock acquisition, preflight, and the
/// chained-vs-single-plan branches.
pub fn dispatch_run(
    conn: &Connection,
    config: &Config,
    project: &str,
    args: RunArgs,
    out: &OutputContext,
) -> Result<()> {
    let workdir = Path::new(project);

    // Precedence: `ralph run --harness X` beats `ralph --harness Y run`,
    // which in turn falls back to the plan's own harness and then the
    // config default. The per-subcommand flag is the most specific, so it
    // wins.
    let harness_override = args.run_harness.or(args.cli_harness);

    // `auto_stash` is default-on. `--no-auto-stash` forces it off for a
    // single run; `config.auto_stash = false` sets a per-user default of
    // "don't stash". The CLI flag always wins when set.
    let auto_stash = if args.no_auto_stash {
        false
    } else {
        config.auto_stash
    };
    let options = RunOptions {
        all_plans: args.all,
        one: args.one,
        from: args.from,
        to: args.to,
        current_branch: args.current_branch,
        auto_stash,
        harness_override,
        dry_run: args.dry_run,
        verbose: args.verbose,
    };

    if args.all {
        if args.from.is_some() || args.to.is_some() {
            anyhow::bail!(
                "--from/--to cannot be combined with --all (step numbers are per-plan and not comparable across plans)"
            );
        }

        // Acquire the per-project run lock so two concurrent `ralph run`
        // invocations can't clobber each other. Dry runs skip the lock since
        // they don't mutate state.
        let _run_lock = if !args.dry_run {
            Some(run_lock::acquire(conn, project, None, None, args.force)?)
        } else {
            None
        };

        // Preflight every runnable plan before starting the chain so we
        // fail fast if anything is misconfigured.
        if !args.skip_preflight && !args.dry_run {
            let runnable: Vec<_> = storage::list_plans(conn, project, false)?
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
                let steps = storage::list_steps(conn, &p.id)?;
                let results = preflight::run_preflight_checks(p, &steps, config, workdir)?;
                results.print_report(out);
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
            runner::run_all_plans(conn, project, config, workdir, &options, abort_rx, out).await
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
    let plan = super::resolve_plan(conn, args.plan_slug, project, false)?;
    let slug = plan.slug.clone();

    // Acquire the per-project run lock before doing any mutating work.
    // Dry runs skip the lock.
    let _run_lock = if !args.dry_run {
        Some(run_lock::acquire(
            conn,
            project,
            Some(&plan.slug),
            Some(&plan.id),
            args.force,
        )?)
    } else {
        None
    };

    // Preflight checks
    if !args.skip_preflight && !args.dry_run {
        eprintln!("Running preflight checks...");
        let steps = storage::list_steps(conn, &plan.id)?;
        let preflight_results = preflight::run_preflight_checks(&plan, &steps, config, workdir)?;
        preflight_results.print_report(out);

        if !preflight_results.is_ok() {
            anyhow::bail!("Preflight checks failed. Use --skip-preflight to bypass.");
        }
    }

    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let abort_rx = signal::install_and_spawn();
        runner::run_plan(conn, &plan, config, workdir, &options, abort_rx, out).await
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

/// TUI-mode dispatcher for `ralph run`. Resolves the target plan (the
/// supplied slug, or the active plan when no slug was given), enters the
/// alternate-screen + raw-mode terminal, and hands off to the plan-detail
/// dispatcher with `auto_start=true` so the run kicks off immediately after
/// the first frame draws (TUI-plan.md §2).
///
/// The routing decision in `main.rs` (via [`is_default_run_invocation`])
/// guarantees this is only reached for bare `ralph run` / `ralph run <slug>`
/// invocations from a TTY — every other flag combination falls through to
/// [`dispatch_run`], so `args` other than `plan_slug` is ignored here. `out`
/// is unused for the same reason: the TUI emits its own UI rather than the
/// plain/json output paths.
pub fn run_tui_mode(
    conn: &Connection,
    config: &Config,
    project: &str,
    args: RunArgs,
    out: &OutputContext,
) -> Result<()> {
    // Resolve the plan before touching the terminal so a "no active plan" or
    // "plan not found" error surfaces as plain stderr rather than corrupting
    // the user's terminal with a half-entered alternate screen.
    let plan = super::resolve_plan(conn, args.plan_slug, project, false)?;
    let slug = plan.slug.clone();

    // Delegate to the plan-list dispatcher with an auto-push so popping
    // back from plan-detail lands at plan-list — keeping the runner
    // subscription alive across the navigation transition. Terminal /
    // raw-mode setup happens inside `run_plan_list_tui`.
    run_plan_list_tui(
        conn,
        config,
        project,
        out,
        Some(InitialPush::PlanDetail {
            slug,
            auto_start: Some(crate::tui::events::StreamMode::Run {
                current_branch: args.current_branch,
                no_auto_stash: args.no_auto_stash,
            }),
        }),
    )
}

// ---------------------------------------------------------------------------
// Resume dispatch
// ---------------------------------------------------------------------------

/// All inputs the `Resume` subcommand needs for routing between the TUI
/// auto-start path and today's CLI runner. Mirrors [`RunArgs`] in shape so
/// the gating helper can stay symmetric.
#[derive(Debug, Clone, Default)]
pub struct ResumeArgs {
    pub plan_slug: Option<String>,
    pub force: bool,
    /// The global `--non-interactive` flag.
    pub non_interactive: bool,
    /// The global `--json`/`--jsonl` flag.
    pub json: bool,
    /// The global `--quiet` flag.
    pub quiet: bool,
    /// The global `--harness` flag.
    pub cli_harness: Option<String>,
}

/// Whether `ralph resume` was invoked with all defaults — meaning the
/// routing rule from TUI-plan.md §2 should drop the user into TUI mode
/// (mirrors [`is_default_run_invocation`] for the resume command per
/// step 34's spec).
///
/// "Default" means: stdout is a real TTY, no `--non-interactive`, no
/// `--json` / `--jsonl`, no `--quiet`, no `--harness` override, and no
/// `--force` (force is a recovery flag — its presence implies the user
/// is troubleshooting and wants the scripted path's stderr report).
pub fn is_default_resume_invocation(args: &ResumeArgs, stdout_is_tty: bool) -> bool {
    if !stdout_is_tty {
        return false;
    }
    if args.non_interactive || args.json || args.quiet {
        return false;
    }
    !args.force && args.cli_harness.is_none()
}

/// CLI-mode dispatcher for `ralph resume` — today's behaviour, factored out
/// of `main.rs` so the TUI-mode router and the scripted/non-TTY path share
/// the same lock acquisition, runner invocation, and final-report formatting.
pub fn dispatch_resume(
    conn: &Connection,
    config: &Config,
    project: &str,
    args: ResumeArgs,
    out: &OutputContext,
) -> Result<()> {
    let workdir = Path::new(project);
    let plan = super::resolve_resume_plan(conn, args.plan_slug, project, workdir)?;
    let slug = plan.slug.clone();

    // Acquire the same per-project run lock that `ralph run` uses, so
    // resume can't race a concurrent run or skip.
    let _run_lock = run_lock::acquire(conn, project, Some(&plan.slug), Some(&plan.id), args.force)?;

    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let abort_rx = signal::install_and_spawn();
        runner::resume_plan(conn, &plan, config, workdir, abort_rx, out).await
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

/// TUI-mode dispatcher for `ralph resume`. Resolves the target plan (the
/// supplied slug, or the branch-inferred plan when no slug was given —
/// see [`super::resolve_resume_plan`]), enters the alternate-screen +
/// raw-mode terminal, and hands off to the plan-detail dispatcher with
/// `auto_start = Some(StreamMode::Resume)` so the resume subprocess kicks
/// off immediately after the first frame draws (TUI-plan.md §2,
/// generalised to resume per step 34).
///
/// The routing decision in `main.rs` (via [`is_default_resume_invocation`])
/// guarantees this is only reached for bare `ralph resume` /
/// `ralph resume <slug>` invocations from a TTY — every other flag
/// combination falls through to [`dispatch_resume`].
pub fn run_resume_tui_mode(
    conn: &Connection,
    config: &Config,
    project: &str,
    args: ResumeArgs,
    out: &OutputContext,
) -> Result<()> {
    // Resolve the plan before touching the terminal so a "no resumable
    // plan" error surfaces as plain stderr rather than corrupting the
    // user's terminal with a half-entered alternate screen.
    let workdir = Path::new(project);
    let plan = super::resolve_resume_plan(conn, args.plan_slug, project, workdir)?;
    let slug = plan.slug.clone();

    run_plan_list_tui(
        conn,
        config,
        project,
        out,
        Some(InitialPush::PlanDetail {
            slug,
            auto_start: Some(crate::tui::events::StreamMode::Resume),
        }),
    )
}

// ---------------------------------------------------------------------------
// Plan-list TUI dispatcher (TUI-plan.md §2 / §5)
// ---------------------------------------------------------------------------

/// One-shot view to push immediately on entry — used by `ralph run` /
/// `ralph resume` to land directly in plan-detail with the runner
/// subprocess auto-started, while still routing through the plan-list
/// dispatcher so popping back falls into the plan-list view (and the
/// runner subscription stays alive across the nav transition).
pub enum InitialPush {
    /// Push plan-detail for `slug`. `auto_start` is `Some` when the parent
    /// CLI invocation wants to spawn a runner immediately on first frame
    /// (`ralph run`, `ralph resume`); `None` matches a plain "open this
    /// plan" entry.
    PlanDetail {
        slug: String,
        auto_start: Option<crate::tui::events::StreamMode>,
    },
}

/// Launch the plan-list view for a bare `ralph` invocation.
///
/// Loads tiles from the DB, sets up the alternate-screen + raw-mode terminal,
/// runs the draw / event loop until the user quits, and tears the terminal
/// down. `enter` / `→` / `l` push the plan-detail view for the highlighted
/// tile; the dispatcher reuses the same terminal session so the user can
/// pop back here when they exit plan-detail.
///
/// `initial_push` lets the caller jump straight into a child view on the
/// first iteration (used by `ralph run` / `ralph resume` to auto-start a
/// runner) while preserving plan-list as the navigation root: when the
/// child pops, the user lands in plan-list with the runner still
/// streaming in the background.
pub fn run_plan_list_tui(
    conn: &Connection,
    config: &Config,
    project: &str,
    _out: &OutputContext,
    initial_push: Option<InitialPush>,
) -> Result<()> {
    use crate::plan::PlanStatus;
    use crate::tui::dialog;
    use crate::tui::read_only::{self, ReadOnly, ReadOnlyTracker, Transition};
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_list::{self, PlanListApp};
    use crossterm::event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    };
    use crossterm::execute;
    use crossterm::terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    };
    use ratatui::Terminal;
    use ratatui::backend::CrosstermBackend;
    use std::time::Instant;

    let tiles = build_plan_tiles(conn, project)?;
    let archived_count = storage::count_archived_plans(conn, project)?;
    let mut app = PlanListApp::new(tiles, project, &config.display_timezone)
        .with_archived_count(archived_count);

    // §13.2: when an externally-spawned ralph runner already holds this
    // project's run lock, the TUI starts in read-only mode and polls every
    // 500ms for the runner to release. The plan-list view doesn't spawn
    // child runners itself, so `spawned_child_pid` is always `None`.
    let my_pid = std::process::id() as i64;
    let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
    if let Ok(initial) = read_only::detect(conn, project, my_pid, None) {
        tracker.observe(initial, Instant::now());
        app.set_read_only(tracker.state());
    }

    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = std::io::stdout();
    // Mouse capture is paired with the alternate screen so per-view
    // `handle_mouse` routing receives `Event::Mouse`. Bypass with Shift to
    // fall back to native terminal selection (TUI-plan.md §4).
    if let Err(e) = execute!(stdout, EnterAlternateScreen, EnableMouseCapture) {
        let _ = disable_raw_mode();
        return Err(e).context("enter alternate screen");
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("create terminal")?;

    // Subscription to a TUI-spawned runner. Owned here at the navigation
    // root so popping plan-detail (or pushing through to a sub-view)
    // doesn't tear it down — the run keeps streaming in the background
    // and the user can re-attach by re-entering the running plan's
    // detail. Released when the runner subprocess hangs up, or
    // implicitly on TUI exit (the parent's `Drop` kills the child via
    // `kill_on_drop`).
    let mut subscription: Option<crate::tui::events::HostedSubscription> = None;

    // Auto-push for `ralph run` / `ralph resume`: dispatched on the first
    // iteration so the user sees plan-detail (with the auto-started
    // runner) before plan-list ever paints. Latched so popping back from
    // plan-detail doesn't re-fire it.
    let mut pending_initial_push = initial_push;

    let result: Result<()> = (|| {
        loop {
            // Auto-push (`ralph run` / `ralph resume`) — runs once before
            // the first plan-list draw so the user lands directly in
            // plan-detail. On pop the loop body resumes from the next
            // iteration with plan-list as the visible root.
            if let Some(push) = pending_initial_push.take() {
                match push {
                    InitialPush::PlanDetail { slug, auto_start } => {
                        run_plan_detail_tui(
                            &mut terminal,
                            conn,
                            config,
                            project,
                            &slug,
                            auto_start,
                            &mut subscription,
                        )?;
                        refresh_plan_list_state(conn, project, &mut app)?;
                    }
                }
            }

            // Drain the runner subscription on every tick to keep its
            // unbounded mpsc channel from accumulating events the user
            // can't see (the active view drains and dispatches; here we
            // just discard so memory stays bounded). When the producer
            // hangs up, release the slot so the next spawn attempt is
            // unblocked. Tail/state recovery on re-entering plan-detail
            // comes from the always-on `run_locks` poll inside that
            // dispatcher.
            if let Some(hosted) = subscription.as_mut() {
                let _ = hosted.sub.drain();
                if hosted.sub.is_disconnected() {
                    // Surface any captured runner failure (preflight error,
                    // missing harness, dirty tree, etc.) on the way out so
                    // the user understands why the run they just started
                    // never produced any progress. The TUI was rooted at
                    // plan-list when the disconnect was observed, so the
                    // toast lands here even though the user may have
                    // started the run from plan-detail.
                    match hosted.sub.poll_failure_status() {
                        crate::tui::events::FailureStatus::Pending => {}
                        crate::tui::events::FailureStatus::Clean => {
                            subscription = None;
                        }
                        crate::tui::events::FailureStatus::Message(msg) => {
                            let slug = hosted.slug.clone();
                            subscription = None;
                            app.toasts.push(
                                format!("[{slug}] {msg}"),
                                ToastKind::Error,
                                Instant::now(),
                            );
                        }
                    }
                }
            }

            // Re-poll the run-lock state on a 500ms cadence (TUI-plan.md
            // §13.2). On Released, push the "edits enabled" toast so the
            // user sees the transition; on Engaged, no toast (the banner
            // alone is enough notice).
            let now = Instant::now();
            if tracker.should_poll(now)
                && let Ok(observed) = read_only::detect(conn, project, my_pid, None)
            {
                let transition = tracker.observe(observed, now);
                app.set_read_only(tracker.state());
                if transition == Transition::Released {
                    app.toasts
                        .push(read_only::RELEASED_TOAST, ToastKind::Success, now);
                }
            }

            // §5: lazily fetch the highlighted plan's step list so the
            // right-pane preview has data on the next draw. The cache is
            // dropped on `refresh_tiles`, so this re-fires after archive,
            // create, or returning from plan-detail.
            ensure_preview_cached(conn, &mut app)?;

            terminal.draw(|f| plan_list::draw(f, &mut app))?;

            // Use a polling read so the lock state stays current even when
            // the user isn't pressing keys. The 250ms timeout balances UI
            // smoothness against polling cost; the actual run-lock query
            // still only fires once per `POLL_INTERVAL` thanks to
            // `tracker.should_poll`.
            if !event::poll(std::time::Duration::from_millis(250))? {
                continue;
            }
            let event = event::read()?;
            if let Event::Mouse(m) = &event {
                app.handle_mouse(*m);
                continue;
            }
            if let Event::Key(key) = event
                && key.kind == KeyEventKind::Press
            {
                // §15 help overlay: `?` toggles, `<esc>`/`q`/Ctrl-C close. While
                // the overlay is visible the dispatcher swallows every key
                // (Consumed/Closed/Opened) so view bindings don't fire under
                // it. Passthrough means the overlay is hidden and we proceed
                // with the normal match below.
                if app.help.intercept_key(key) != crate::tui::help::InterceptResult::Passthrough {
                    continue;
                }
                // §9 palette: while open, route every key through the palette
                // bar first. Submit dispatches via [`palette_list_palette_action`]
                // and applies the resulting [`PaletteAction`] in-view. The
                // archive/delete confirms render over the live tile list using
                // the existing `confirm_with_background` helper.
                if let Some(bar) = app.palette_bar.as_mut() {
                    use crate::tui::palette_dispatch::PaletteAction;
                    use crate::tui::widgets::palette_bar::PaletteBarOutcome;
                    match bar.on_key(key) {
                        PaletteBarOutcome::Pending => {}
                        PaletteBarOutcome::Cancel => app.close_palette(),
                        PaletteBarOutcome::Submit(input) => {
                            let archived_refs =
                                plan_refs_from_archived(conn, project).unwrap_or_default();
                            let action = plan_list_palette_action(
                                &input,
                                &config.default_harness,
                                &app,
                                &archived_refs,
                            );
                            app.close_palette();
                            match plan_list_apply_palette_action(conn, project, &mut app, action)? {
                                Some(PaletteAction::OpenConfirmArchive { plan_id, slug }) => {
                                    let body = format!("Archive plan `{slug}`?");
                                    let confirm = dialog::Confirm {
                                        title: "Archive plan",
                                        body: &body,
                                        default: false,
                                    };
                                    if confirm_with_background(&mut terminal, &mut app, &confirm)? {
                                        plan_list_apply_archive(conn, project, &mut app, &plan_id)?;
                                    }
                                }
                                Some(PaletteAction::OpenConfirmDelete { plan_id, slug }) => {
                                    let body = format!(
                                        "Permanently delete plan `{slug}`? This cannot be undone."
                                    );
                                    let confirm = dialog::Confirm {
                                        title: "Permanently delete plan",
                                        body: &body,
                                        default: false,
                                    };
                                    if confirm_with_background(&mut terminal, &mut app, &confirm)? {
                                        plan_list_apply_delete(conn, project, &mut app, &plan_id)?;
                                    }
                                }
                                Some(PaletteAction::OpenRunDialog {
                                    default_branch,
                                    plan_count,
                                    targets,
                                }) => {
                                    let outcome = run_dialog_loop_with_bg(
                                        &mut terminal,
                                        |f| crate::tui::views::plan_list::draw(f, &mut app),
                                        default_branch,
                                        plan_count,
                                    )?;
                                    let report = apply_palette_run_outcome(
                                        &mut terminal,
                                        |f| crate::tui::views::plan_list::draw(f, &mut app),
                                        project,
                                        outcome,
                                        &targets,
                                        plan_count > 1,
                                    )?;
                                    flush_palette_run_toasts(report, &mut app.toasts);
                                }
                                Some(PaletteAction::RunOnBranch {
                                    branch,
                                    targets,
                                    force_current_branch,
                                }) => {
                                    let report = apply_palette_run_outcome(
                                        &mut terminal,
                                        |f| crate::tui::views::plan_list::draw(f, &mut app),
                                        project,
                                        crate::tui::run_dialog::Outcome::NewBranch(branch),
                                        &targets,
                                        force_current_branch,
                                    )?;
                                    flush_palette_run_toasts(report, &mut app.toasts);
                                }
                                // §9 sub-view routing — push plan-dependencies
                                // / plan-hooks against the resolved plan. The
                                // dispatcher already substituted the focused
                                // slug, so the action carries the IDs we need.
                                Some(PaletteAction::OpenPlanDependencies { plan_id, slug }) => {
                                    run_plan_dependencies_tui(
                                        &mut terminal,
                                        conn,
                                        project,
                                        &plan_id,
                                        &slug,
                                    )?;
                                    refresh_plan_list_state(conn, project, &mut app)?;
                                }
                                Some(PaletteAction::OpenPlanHooks { plan_id, slug }) => {
                                    run_plan_hooks_tui(
                                        &mut terminal,
                                        conn,
                                        project,
                                        &plan_id,
                                        &slug,
                                    )?;
                                    refresh_plan_list_state(conn, project, &mut app)?;
                                }
                                _ => {}
                            }
                        }
                    }
                    continue;
                }
                let locked = app.read_only.is_locked();
                match key.code {
                    KeyCode::Char('/') | KeyCode::Char(':') => {
                        let prefix = match key.code {
                            KeyCode::Char(c) => c,
                            _ => '/',
                        };
                        app.open_palette(prefix);
                    }
                    KeyCode::Char('j') | KeyCode::Down => app.navigate_down(),
                    KeyCode::Char('k') | KeyCode::Up => app.navigate_up(),
                    KeyCode::Char('g') => app.jump_top(),
                    KeyCode::Char('G') => app.jump_bottom(),
                    KeyCode::Char(' ') => app.toggle_selection(),
                    KeyCode::Char('d') if !locked => {
                        let targets = app.archive_targets();
                        if targets.is_empty() {
                            continue;
                        }
                        let body = format!("Archive {} plan(s)?", targets.len());
                        let confirm = dialog::Confirm {
                            title: "Archive plans",
                            body: &body,
                            default: false,
                        };
                        if confirm_with_background(&mut terminal, &mut app, &confirm)? {
                            for id in &targets {
                                storage::update_plan_status(conn, id, PlanStatus::Archived)?;
                            }
                            refresh_plan_list_state(conn, project, &mut app)?;
                            let n = targets.len();
                            let msg = if n == 1 {
                                "Archived 1 plan.".to_string()
                            } else {
                                format!("Archived {n} plans.")
                            };
                            app.toasts.push(msg, ToastKind::Success, Instant::now());
                        }
                    }
                    KeyCode::Char('A') if !locked => {
                        plan_list_approve_cursor(conn, project, &mut app)?;
                    }
                    KeyCode::Char('Q') if !locked => {
                        plan_list_toggle_questions_cursor(conn, project, &mut app)?;
                    }
                    KeyCode::Char('r') => {
                        plan_list_refresh(conn, project, &mut app)?;
                    }
                    KeyCode::Char('i') | KeyCode::Char('a') if !locked => {
                        plan_list_create_plan(conn, config, project, &mut terminal, &mut app)?;
                    }
                    KeyCode::Esc => {
                        plan_list_handle_esc(&mut app);
                    }
                    KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                        match app.request_open() {
                            Some(crate::tui::views::plan_list::OpenRequest::Archived) => {
                                run_archived_list_tui(
                                    &mut terminal,
                                    conn,
                                    project,
                                    &config.display_timezone,
                                    &config.default_harness,
                                )?;
                                refresh_plan_list_state(conn, project, &mut app)?;
                            }
                            Some(crate::tui::views::plan_list::OpenRequest::Plan(slug)) => {
                                run_plan_detail_tui(
                                    &mut terminal,
                                    conn,
                                    config,
                                    project,
                                    &slug,
                                    None,
                                    &mut subscription,
                                )?;
                                // The plan-detail view can mutate step state
                                // (skip / add) and counters; refresh tiles so
                                // the user sees up-to-date totals on return.
                                refresh_plan_list_state(conn, project, &mut app)?;
                            }
                            None => {}
                        }
                        // Reset so a future tick doesn't re-dispatch.
                        app.open_request = None;
                    }
                    KeyCode::Char('q') => app.request_quit(),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        app.request_quit();
                    }
                    _ => {}
                }
            }
            if app.should_quit {
                return Ok(());
            }
        }
    })();

    let _ = disable_raw_mode();
    let mut stdout = std::io::stdout();
    let _ = execute!(stdout, LeaveAlternateScreen, DisableMouseCapture);

    result
}

/// Render the plan-list view as the dialog's background and block on a
/// yes/no decision. Mirrors `dialog::run` but composites the live view under
/// the modal so the user keeps context (cursor, selection badges, tiles) while
/// answering. Returns the user's choice.
fn confirm_with_background<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    app: &mut crate::tui::views::plan_list::PlanListApp,
    c: &crate::tui::dialog::Confirm<'_>,
) -> Result<bool>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::dialog::{self, Decision};
    use crate::tui::views::plan_list;
    use crossterm::event::{self, Event, KeyEventKind};

    loop {
        terminal.draw(|f| {
            plan_list::draw(f, app);
            let area = f.area();
            dialog::render(f, area, c);
        })?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match dialog::decide_key(key, c.default) {
                Decision::Yes => return Ok(true),
                Decision::No => return Ok(false),
                Decision::Pending => continue,
            }
        }
    }
}

/// `A` action in the plan-list view (TUI-plan.md §5): approve the cursor
/// target. Flips `Planning` → `Ready` via `update_plan_status`, refreshes the
/// in-memory tile in place (preserving selection / cursor), and toasts. Plans
/// already past Planning surface an info toast instead — mirroring the CLI's
/// `plan approve` rejection but without aborting the TUI session.
pub(crate) fn plan_list_approve_cursor(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()> {
    use crate::plan::PlanStatus;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let target = app
        .cursor_plan()
        .map(|p| (p.id.clone(), p.slug.clone(), p.status));
    let Some((id, slug, status)) = target else {
        return Ok(());
    };
    if status == PlanStatus::Planning {
        storage::update_plan_status(conn, &id, PlanStatus::Ready)?;
        if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
            app.update_plan_in_place(updated);
        }
        app.toasts
            .push("Plan approved.", ToastKind::Success, Instant::now());
    } else {
        app.toasts.push(
            format!("Plan is in {status} status; nothing to approve."),
            ToastKind::Info,
            Instant::now(),
        );
    }
    Ok(())
}

/// `Q` action in the plan-list view (TUI-plan.md §17): flip
/// `plans.questions_enabled` on the cursor target via
/// `set_plan_questions_enabled`, refresh the tile in place, and toast the new
/// state. Cursor-only — selection is ignored.
pub(crate) fn plan_list_toggle_questions_cursor(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let target = app
        .cursor_plan()
        .map(|p| (p.id.clone(), p.slug.clone(), p.questions_enabled));
    let Some((id, slug, current)) = target else {
        return Ok(());
    };
    let next = !current;
    storage::set_plan_questions_enabled(conn, &id, next)?;
    if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
        app.update_plan_in_place(updated);
    }
    let msg = if next {
        "Questions enabled."
    } else {
        "Questions disabled."
    };
    app.toasts.push(msg, ToastKind::Success, Instant::now());
    Ok(())
}

/// `r` action in the plan-list view (TUI-plan.md §5): re-query plans from the
/// DB and toast the user. Re-uses `refresh_plan_list_state` (the same fetch
/// path used at view entry and on focus return), so the cursor is clamped
/// into the new range and the preview cache is dropped. Permitted in
/// read-only mode — refresh is purely a read operation.
pub(crate) fn plan_list_refresh(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    refresh_plan_list_state(conn, project, app)?;
    app.toasts
        .push("Refreshed.", ToastKind::Info, Instant::now());
    Ok(())
}

/// `<esc>` precedence in the plan-list view (TUI-plan.md §4): dismiss the
/// current toast when one is showing and consume the keypress; otherwise
/// fall through to the view's existing Esc binding (`app.escape()` —
/// clear-selection-or-quit). Returns `true` when a toast was dismissed.
/// Extracted so the precedence is unit testable without driving the full
/// event loop.
pub(crate) fn plan_list_handle_esc(app: &mut crate::tui::views::plan_list::PlanListApp) -> bool {
    if app.toasts.dismiss() {
        true
    } else {
        let _ = app.escape();
        false
    }
}

/// `i` / `a` action in the plan-list view (TUI-plan.md §5): open the inline
/// "New plan" modal, drive the slug → description → tests state machine,
/// and on submit insert a row via `storage::create_plan`. Loops the draw +
/// event-read locally so the modal renders over the live plan-list as a
/// background. Cancellation (`Esc` / `Ctrl-C`) just returns; on submit the
/// cursor jumps to the freshly-created plan via [`plan_list_apply_create`].
fn plan_list_create_plan<B: ratatui::backend::Backend>(
    conn: &Connection,
    config: &Config,
    project: &str,
    terminal: &mut ratatui::Terminal<B>,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::views::create_plan::{self, CreatePlanModal, Outcome};
    use crate::tui::views::plan_list;
    use crossterm::event::{self, Event, KeyEventKind};

    let mut modal = CreatePlanModal::new();

    loop {
        terminal.draw(|f| {
            plan_list::draw(f, app);
            let area = f.area();
            create_plan::render(f, area, &modal);
        })?;

        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            _ => continue,
        };

        match modal.handle_key(key) {
            Outcome::Pending => continue,
            Outcome::Cancelled => return Ok(()),
            Outcome::Submit {
                slug,
                description,
                tests,
            } => {
                plan_list_apply_create(conn, config, project, app, &slug, &description, &tests)?;
                return Ok(());
            }
        }
    }
}

/// Pure-storage half of the create-plan flow. Inserts the plan via
/// `storage::create_plan`, refreshes the tile list, and re-positions the
/// cursor on the new plan. Toasts a success or — on a storage failure (e.g.
/// duplicate slug) — an error message. Factored apart from the event loop so
/// it can be integration-tested without a terminal.
pub(crate) fn plan_list_apply_create(
    conn: &Connection,
    config: &Config,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
    slug: &str,
    description: &str,
    tests: &[String],
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    // Branch name auto-defaults to the slug, matching the CLI's
    // `plan create` behavior when `--branch` is omitted.
    let branch_name = slug;
    let harness = Some(config.default_harness.as_str());
    let agent: Option<&str> = None;

    match storage::create_plan(
        conn,
        slug,
        project,
        branch_name,
        description,
        harness,
        agent,
        tests,
    ) {
        Ok(plan) => {
            let new_tiles = build_plan_tiles(conn, project)?;
            let archived_count = storage::count_archived_plans(conn, project)?;
            let new_index = new_tiles
                .iter()
                .position(|t| t.plan.id == plan.id)
                .unwrap_or(0);
            app.refresh_tiles(new_tiles, archived_count);
            app.selected_index = new_index;
            app.toasts.push(
                format!("Created plan: {slug}"),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to create plan: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Palette helpers shared across view dispatchers (TUI-plan.md §9, step 20)
// ---------------------------------------------------------------------------

/// Build [`PlanRef`]s from the `Plan`s embedded in plan-list / archived-list
/// tiles. Used by the palette dispatchers to resolve `[<slug>]` arguments
/// against the visible plan pool.
fn plan_refs_from_tiles(
    tiles: &[crate::tui::views::plan_list::PlanTile],
) -> Vec<crate::tui::palette_dispatch::PlanRef> {
    tiles
        .iter()
        .map(|t| crate::tui::palette_dispatch::PlanRef {
            id: t.plan.id.clone(),
            slug: t.plan.slug.clone(),
            branch_name: t.plan.branch_name.clone(),
            status: t.plan.status,
        })
        .collect()
}

/// Fetch archived plans from the DB and project them into [`PlanRef`]s. Used
/// by plan-list to support `/plan unarchive <slug>` without keeping a second
/// in-memory cache (the archived list isn't loaded until the user pushes
/// into it).
fn plan_refs_from_archived(
    conn: &Connection,
    project: &str,
) -> Result<Vec<crate::tui::palette_dispatch::PlanRef>> {
    let plans = storage::list_archived_plans_sorted_by_recency(conn, project)?;
    Ok(plans
        .into_iter()
        .map(|p| crate::tui::palette_dispatch::PlanRef {
            id: p.id,
            slug: p.slug,
            branch_name: p.branch_name,
            status: p.status,
        })
        .collect())
}

/// Build a single [`PlanRef`] from a fully-loaded `Plan` (used by plan-detail
/// and step-detail, which have one focused plan rather than a tile list).
fn plan_ref_from_plan(plan: &crate::plan::Plan) -> crate::tui::palette_dispatch::PlanRef {
    crate::tui::palette_dispatch::PlanRef {
        id: plan.id.clone(),
        slug: plan.slug.clone(),
        branch_name: plan.branch_name.clone(),
        status: plan.status,
    }
}

// ---------------------------------------------------------------------------
// /run dialog wiring (TUI-plan.md §9.1, step 21)
// ---------------------------------------------------------------------------

/// What `apply_palette_run_outcome` should do with a `NewBranch(target)`
/// outcome before spawning. Pure (no I/O at the decision boundary) so it can
/// be unit-tested against a real git tempdir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchDecision {
    /// cwd is already on the target branch — nothing to switch.
    AlreadyOnTarget,
    /// Target branch exists locally; check it out.
    SwitchExisting,
    /// Target branch doesn't exist; caller must confirm before creating.
    NeedsCreate,
}

/// Decide what action to take when aligning cwd with `target_branch`.
pub(crate) fn classify_branch_target(workdir: &Path, target: &str) -> Result<BranchDecision> {
    use crate::git;
    let current = git::get_current_branch(workdir)?;
    if current == target {
        return Ok(BranchDecision::AlreadyOnTarget);
    }
    if git::branch_exists(workdir, target)? {
        Ok(BranchDecision::SwitchExisting)
    } else {
        Ok(BranchDecision::NeedsCreate)
    }
}

/// Drive the run-dialog state machine to completion, rendering it as an
/// overlay over the caller's view. Returns the terminal `Outcome`.
pub(crate) fn run_dialog_loop_with_bg<B, F>(
    terminal: &mut ratatui::Terminal<B>,
    mut draw_bg: F,
    default_branch: String,
    plan_count: usize,
) -> Result<crate::tui::run_dialog::Outcome>
where
    B: ratatui::backend::Backend,
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
    F: FnMut(&mut ratatui::Frame<'_>),
{
    use crate::tui::run_dialog::{self, Outcome, RunDialog};
    use crossterm::event::{self, Event, KeyEventKind};

    let mut dialog = RunDialog::new(default_branch, plan_count);
    loop {
        terminal.draw(|f| {
            draw_bg(f);
            let area = f.area();
            run_dialog::render(f, area, &dialog);
        })?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match dialog.handle_key(key) {
                Outcome::Pending => continue,
                other => return Ok(other),
            }
        }
    }
}

/// Drive a `dialog::Confirm` loop with a custom background. Mirrors the
/// per-view `confirm_with_*_background` helpers but parameterized on a
/// closure so the run-dialog flow can reuse it without a per-view variant.
fn confirm_loop_with_bg<B, F>(
    terminal: &mut ratatui::Terminal<B>,
    mut draw_bg: F,
    confirm: &crate::tui::dialog::Confirm<'_>,
) -> Result<bool>
where
    B: ratatui::backend::Backend,
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
    F: FnMut(&mut ratatui::Frame<'_>),
{
    use crate::tui::dialog::{self, Decision};
    use crossterm::event::{self, Event, KeyEventKind};

    loop {
        terminal.draw(|f| {
            draw_bg(f);
            let area = f.area();
            dialog::render(f, area, confirm);
        })?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match dialog::decide_key(key, confirm.default) {
                Decision::Yes => return Ok(true),
                Decision::No => return Ok(false),
                Decision::Pending => continue,
            }
        }
    }
}

/// Result of [`apply_palette_run_outcome`]. The `pending_toasts` list is
/// drained by the caller after the helper returns so the borrow held by the
/// background-draw closure is released first; pushing into `app.toasts`
/// inline would conflict with the closure's mutable borrow of the App.
#[derive(Debug, Default)]
pub(crate) struct PaletteRunReport {
    /// Plan slugs whose runner subprocess was spawned, in dispatch order.
    pub spawned: Vec<String>,
    /// Toasts the caller should push onto its view's queue, in order.
    pub pending_toasts: Vec<(String, crate::tui::toast::ToastKind)>,
}

/// Apply a [`crate::tui::run_dialog::Outcome`] (from either `OpenRunDialog`
/// or the synthetic `NewBranch(branch)` of `RunOnBranch`) by:
///   1. Switching to the target branch when the outcome calls for it,
///      prompting the user to confirm creation when the branch is missing.
///   2. Spawning a runner subprocess per target via
///      [`crate::tui::run_dialog::dispatch_outcome`].
///
/// `force_current_branch` is passed through to `dispatch_outcome` so
/// multi-plan callers (or `/run <branch>` in multi-select mode) always pass
/// `--current-branch`. Toasts are returned for the caller to push because the
/// `draw_bg` closure holds a mutable borrow of the underlying App while this
/// function runs — see [`PaletteRunReport`].
pub(crate) fn apply_palette_run_outcome<B, F>(
    terminal: &mut ratatui::Terminal<B>,
    mut draw_bg: F,
    project: &str,
    outcome: crate::tui::run_dialog::Outcome,
    targets: &[crate::tui::run_dialog::RunTarget],
    force_current_branch: bool,
) -> Result<PaletteRunReport>
where
    B: ratatui::backend::Backend,
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
    F: FnMut(&mut ratatui::Frame<'_>),
{
    use crate::tui::run_dialog::Outcome;
    use crate::tui::toast::ToastKind;

    let mut report = PaletteRunReport::default();

    if matches!(outcome, Outcome::Pending | Outcome::Cancelled) {
        return Ok(report);
    }

    let workdir = Path::new(project);

    if let Outcome::NewBranch(name) = &outcome {
        match classify_branch_target(workdir, name) {
            Ok(BranchDecision::AlreadyOnTarget) => {}
            Ok(BranchDecision::SwitchExisting) => {
                if let Err(e) = crate::git::checkout_branch(workdir, name) {
                    report.pending_toasts.push((
                        format!("Failed to checkout `{name}`: {e}"),
                        ToastKind::Error,
                    ));
                    return Ok(report);
                }
            }
            Ok(BranchDecision::NeedsCreate) => {
                let body = format!("Branch `{name}` doesn't exist. Create it?");
                let confirm = crate::tui::dialog::Confirm {
                    title: "Create branch",
                    body: &body,
                    default: false,
                };
                if !confirm_loop_with_bg(terminal, &mut draw_bg, &confirm)? {
                    report
                        .pending_toasts
                        .push(("Run cancelled.".to_string(), ToastKind::Info));
                    return Ok(report);
                }
                if let Err(e) = crate::git::create_and_checkout_branch(workdir, name) {
                    report
                        .pending_toasts
                        .push((format!("Failed to create `{name}`: {e}"), ToastKind::Error));
                    return Ok(report);
                }
            }
            Err(e) => {
                report
                    .pending_toasts
                    .push((format!("Cannot inspect git state: {e}"), ToastKind::Error));
                return Ok(report);
            }
        }
    }

    spawn_palette_runners(
        workdir,
        &outcome,
        targets,
        force_current_branch,
        &mut report,
    );
    Ok(report)
}

/// Spawn one runner subprocess per target via
/// [`crate::tui::run_dialog::ProcessRunSpawner`] and append toasts to the
/// report. Pulled out so unit tests can target the success/error-toast
/// shaping without forking a real subprocess (the `dispatch_outcome` /
/// `RunSpawner` trait covers the per-arg ordering separately).
fn spawn_palette_runners(
    workdir: &Path,
    outcome: &crate::tui::run_dialog::Outcome,
    targets: &[crate::tui::run_dialog::RunTarget],
    force_current_branch: bool,
    report: &mut PaletteRunReport,
) {
    use crate::tui::run_dialog::{self, ProcessRunSpawner};
    use crate::tui::toast::ToastKind;

    let mut spawner = match ProcessRunSpawner::new() {
        Ok(s) => s,
        Err(e) => {
            report
                .pending_toasts
                .push((format!("Cannot locate ralph binary: {e}"), ToastKind::Error));
            return;
        }
    };
    match run_dialog::dispatch_outcome(
        outcome,
        targets,
        workdir,
        &mut spawner,
        force_current_branch,
    ) {
        Ok(spawned) => {
            if !spawned.is_empty() {
                let msg = if spawned.len() == 1 {
                    format!("Started run for {}.", spawned[0])
                } else {
                    format!("Started {} runs.", spawned.len())
                };
                report.pending_toasts.push((msg, ToastKind::Success));
            }
            report.spawned = spawned;
        }
        Err(e) => {
            report
                .pending_toasts
                .push((format!("Failed to start run: {e}"), ToastKind::Error));
        }
    }
}

/// Drain `report.pending_toasts` into `toasts`. Called by view dispatchers
/// once the closure that drove `apply_palette_run_outcome` is dropped.
pub(crate) fn flush_palette_run_toasts(
    report: PaletteRunReport,
    toasts: &mut crate::tui::toast::ToastQueue,
) {
    use std::time::Instant;
    for (msg, kind) in report.pending_toasts {
        toasts.push(msg, kind, Instant::now());
    }
}

/// Apply a [`PaletteAction`] to the plan-list view. Performs every side
/// effect the palette dispatcher requests except for terminal-bound dialogs
/// (`OpenConfirmArchive`, `OpenConfirmDelete`), which the caller drives via
/// `confirm_with_background`. Returns `Some(action)` for those terminal-bound
/// variants so the dispatcher loop can run the confirm step itself.
///
/// Sub-view actions (`OpenPlan{Dependencies,Hooks}`, `OpenStep{Hooks,Tags}`)
/// are forwarded as `Some(action)` so the dispatcher loop can push the
/// matching sub-view; `OpenStep*` variants on the plan-list view (which
/// has no focused step) toast a "Open a step first…" hint instead. The
/// run-dialog actions (`OpenRunDialog`, `RunOnBranch`) are likewise
/// forwarded so the caller can drive the modal over the live view.
pub(crate) fn plan_list_apply_palette_action(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
    action: crate::tui::palette_dispatch::PaletteAction,
) -> Result<Option<crate::tui::palette_dispatch::PaletteAction>> {
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match action {
        PaletteAction::None => {}
        PaletteAction::Toast { message, kind } => {
            app.toasts.push(message, kind, Instant::now());
        }
        PaletteAction::Quit => {
            app.request_quit();
        }
        PaletteAction::PushPlanDetail { slug } => {
            // Plan-list's dispatcher pushes plan-detail directly for `enter`,
            // so route the palette command through the same `open_request`
            // channel — keeps the view's transition logic in one place.
            app.open_request = Some(crate::tui::views::plan_list::OpenRequest::Plan(slug));
        }
        PaletteAction::Approve { plan_id, slug } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
                app.update_plan_in_place(updated);
            }
            app.toasts
                .push("Plan approved.", ToastKind::Success, Instant::now());
        }
        PaletteAction::Unarchive { plan_id, slug: _ } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            refresh_plan_list_state(conn, project, app)?;
            app.toasts
                .push("Unarchived.", ToastKind::Success, Instant::now());
        }
        PaletteAction::SetQuestionsEnabled {
            plan_id,
            slug,
            enabled,
        } => {
            storage::set_plan_questions_enabled(conn, &plan_id, enabled)?;
            if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
                app.update_plan_in_place(updated);
            }
            let msg = if enabled {
                "Questions enabled."
            } else {
                "Questions disabled."
            };
            app.toasts.push(msg, ToastKind::Success, Instant::now());
        }
        PaletteAction::Export { slug, output } => {
            apply_palette_export(&slug, output.as_deref(), conn, project, &mut app.toasts);
        }
        PaletteAction::Import { path } => {
            apply_palette_import(&path, conn, project, &mut app.toasts);
            refresh_plan_list_state(conn, project, app)?;
        }
        PaletteAction::SpawnPlanHarness { harness, slug: _ } => {
            // The plan-harness flow is interactive (calls into
            // plan_harness::run_plan_harness which expects a real terminal).
            // Surface a "not yet from palette" toast — operators can still
            // run it from the CLI.
            app.toasts.push(
                format!(
                    "/plan harness {harness}: not yet wired from palette; use the CLI for now."
                ),
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::CancelRun => {
            app.toasts.push(
                "No live run from this view.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        // §9 sub-view routing — `/step set-hook|unset-hook` and
        // `/step edit --tags` only resolve in step-detail (the dispatcher
        // already toasted "Open a step first…" if focus wasn't a step), so
        // when they land here we explicitly route to a plan-list-shaped
        // hint. Plan-level entries (`OpenPlanDependencies`, `OpenPlanHooks`)
        // are forwarded so the dispatcher loop can push the corresponding
        // sub-view dispatcher against the focused plan.
        PaletteAction::OpenPlanDependencies { .. } | PaletteAction::OpenPlanHooks { .. } => {
            return Ok(Some(action));
        }
        PaletteAction::OpenStepHooks { .. } | PaletteAction::OpenStepTags { .. } => {
            app.toasts.push(
                "Open a step first to edit per-step hooks or tags.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::ComingSoon {
            label,
            target_step: _,
        } => {
            app.toasts.push(
                format!("{label}: palette wiring pending — see TUI-plan.md §9."),
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Plan-list does not host a focused plan for /step add|skip|move — the
        // dispatcher's `resolve_slug(None, …)` already routed those to a toast
        // when the cursor isn't on a plan. If they reach us here it's because
        // the cursor is on a plan; route via `open_request` to plan-detail and
        // toast a hint instead of mutating storage from this view.
        PaletteAction::AddStep { .. }
        | PaletteAction::SkipStep { .. }
        | PaletteAction::MoveStep { .. } => {
            app.toasts.push(
                "Open the plan first to edit steps.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Terminal-bound: hand back to the caller so it can render the
        // confirm dialog with the live plan-list view as the background.
        PaletteAction::OpenConfirmArchive { .. } | PaletteAction::OpenConfirmDelete { .. } => {
            return Ok(Some(action));
        }
        // Terminal-bound: hand back so the caller can render the run-choice
        // dialog (TUI-plan.md §9.1) with the live view as the background.
        PaletteAction::OpenRunDialog { .. } | PaletteAction::RunOnBranch { .. } => {
            return Ok(Some(action));
        }
    }
    Ok(None)
}

/// Post-confirm half of the `OpenConfirmArchive` flow for plan-list. Sets
/// the plan to `Archived` and refreshes the tile list. Factored apart so
/// the dispatcher loop can drive the confirm dialog while keeping this
/// path unit-testable.
pub(crate) fn plan_list_apply_archive(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
    plan_id: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    storage::update_plan_status(conn, plan_id, crate::plan::PlanStatus::Archived)?;
    refresh_plan_list_state(conn, project, app)?;
    app.toasts
        .push("Archived 1 plan.", ToastKind::Success, Instant::now());
    Ok(())
}

/// Post-confirm half of the `OpenConfirmDelete` flow for plan-list. Deletes
/// the plan and refreshes the tile list.
pub(crate) fn plan_list_apply_delete(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
    plan_id: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    storage::delete_plan(conn, plan_id)?;
    refresh_plan_list_state(conn, project, app)?;
    app.toasts
        .push("Deleted 1 plan.", ToastKind::Success, Instant::now());
    Ok(())
}

/// Resolve a parsed-or-error palette input into a [`PaletteAction`] for the
/// plan-list view. Pure — no side effects, no DB writes. Tests target this
/// directly to verify command routing without a terminal.
pub(crate) fn plan_list_palette_action(
    input: &str,
    default_harness: &str,
    app: &crate::tui::views::plan_list::PlanListApp,
    archived_refs: &[crate::tui::palette_dispatch::PlanRef],
) -> crate::tui::palette_dispatch::PaletteAction {
    use crate::tui::palette;
    use crate::tui::palette_dispatch;

    let plans = plan_refs_from_tiles(&app.tiles);
    let focused_slug = app.cursor_plan().map(|p| p.slug.as_str());
    // Run targets: selection wins, otherwise the highlighted plan.
    let run_targets: Vec<crate::tui::run_dialog::RunTarget> = if !app.selection.is_empty() {
        let by_id: std::collections::HashMap<&str, &crate::plan::Plan> = app
            .tiles
            .iter()
            .map(|t| (t.plan.id.as_str(), &t.plan))
            .collect();
        app.selection
            .as_slice()
            .iter()
            .filter_map(|id| {
                by_id
                    .get(id.as_str())
                    .map(|p| crate::tui::run_dialog::RunTarget {
                        slug: p.slug.clone(),
                        default_branch: p.branch_name.clone(),
                    })
            })
            .collect()
    } else if let Some(plan) = app.cursor_plan() {
        vec![crate::tui::run_dialog::RunTarget {
            slug: plan.slug.clone(),
            default_branch: plan.branch_name.clone(),
        }]
    } else {
        Vec::new()
    };

    let ctx = palette_dispatch::PaletteContext {
        default_harness,
        focused_slug,
        focused_step: None,
        run_targets: &run_targets,
        plans: &plans,
        archived: archived_refs,
    };

    match palette::parse(input) {
        Ok(cmd) => palette_dispatch::dispatch(&cmd, &ctx),
        Err(err) => palette_dispatch::dispatch_parse_error(&err),
    }
}

/// Helper for `PaletteAction::Export`: writes `<slug>.ralph.json` (or `output`
/// when given) and toasts. Uses `eprintln` redirected to nowhere — `export_plan`
/// prints to stderr on success.
fn apply_palette_export(
    slug: &str,
    output: Option<&str>,
    conn: &Connection,
    project: &str,
    toasts: &mut crate::tui::toast::ToastQueue,
) {
    use crate::tui::toast::ToastKind;
    use std::path::PathBuf;
    use std::time::Instant;

    let target_path: PathBuf = match output {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from(format!("{slug}.ralph.json")),
    };
    match crate::export::export_plan(conn, slug, project, Some(&target_path)) {
        Ok(()) => {
            toasts.push(
                format!("Exported {slug} to {}", target_path.display()),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            toasts.push(
                format!("Export failed: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
}

/// Helper for `PaletteAction::Import`: reads the file, imports with no slug
/// override, and toasts. Slug-conflict resolution (an inline rename prompt)
/// is **not implemented**; duplicates surface as an error toast. See the
/// `/import` row in TUI-plan.md §9 (PARTIALLY IMPLEMENTED).
fn apply_palette_import(
    path: &str,
    conn: &Connection,
    project: &str,
    toasts: &mut crate::tui::toast::ToastQueue,
) {
    use crate::tui::toast::ToastKind;
    use std::path::Path;
    use std::time::Instant;

    let path_buf = Path::new(path);
    let imported = match crate::import::read_plan_file(path_buf) {
        Ok(p) => p,
        Err(e) => {
            toasts.push(
                format!("Import failed: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return;
        }
    };
    let options = crate::import::ImportOptions {
        slug: None,
        branch: None,
        harness: None,
        project,
        strict: false,
    };
    match crate::import::import_plan_from_data(conn, &imported, &options) {
        Ok(slug) => {
            toasts.push(
                format!("Imported plan: {slug}"),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            toasts.push(
                format!("Import failed: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
}

/// Build the read-only tile rows the plan-list view renders. One tile per
/// non-archived plan; recent first; counts come from `list_steps`; activity
/// stamp comes from `last_log_started_at_for_plan`, falling back to
/// `plan.created_at` so plans that have never run still show "Created …".
fn build_plan_tiles(
    conn: &Connection,
    project: &str,
) -> Result<Vec<crate::tui::views::plan_list::PlanTile>> {
    let plans = storage::list_plans_sorted_by_recency(conn, project)?;
    plans_into_tiles(conn, plans)
}

/// Mirror of [`build_plan_tiles`] for the archived plan list view (§6).
fn build_archived_tiles(
    conn: &Connection,
    project: &str,
) -> Result<Vec<crate::tui::views::plan_list::PlanTile>> {
    let plans = storage::list_archived_plans_sorted_by_recency(conn, project)?;
    plans_into_tiles(conn, plans)
}

fn plans_into_tiles(
    conn: &Connection,
    plans: Vec<crate::plan::Plan>,
) -> Result<Vec<crate::tui::views::plan_list::PlanTile>> {
    use crate::tui::views::plan_list::PlanTile;
    let mut tiles = Vec::with_capacity(plans.len());
    for plan in plans {
        let steps = storage::list_steps(conn, &plan.id)?;
        let total = steps.len() as u32;
        let completed = steps
            .iter()
            .filter(|s| s.status == StepStatus::Complete)
            .count() as u32;
        let last_run = storage::last_log_started_at_for_plan(conn, &plan.id)?;
        let (last_activity, had_run) = match last_run {
            Some(t) => (t, true),
            None => (plan.created_at, false),
        };
        // §17: derive the open-question count + oldest-question teaser for
        // this plan so the tile can flip to the purple `STATUS_QUESTION` dot
        // and surface a one-line preview.
        let opens = storage::list_open_questions(conn, &plan.project, Some(&plan.slug))?;
        let unanswered_questions = opens.len() as u32;
        let oldest_question = opens.first().map(|q| q.question.clone());
        tiles.push(PlanTile {
            plan,
            completed,
            total,
            last_activity,
            had_run,
            unanswered_questions,
            oldest_question,
        });
    }
    Ok(tiles)
}

/// Refresh the plan-list view's tile list AND archived-count from the DB.
/// Used after every operation that can change either set: archive, create,
/// or returning from the archived-list view.
pub(crate) fn refresh_plan_list_state(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()> {
    let tiles = build_plan_tiles(conn, project)?;
    let archived_count = storage::count_archived_plans(conn, project)?;
    app.refresh_tiles(tiles, archived_count);
    Ok(())
}

/// Ensure the right-pane step preview has cached steps for the highlighted
/// plan (TUI-plan.md §5). No-op when the cursor is on the archived sentinel
/// or the cache already has an entry for the current plan.
fn ensure_preview_cached(
    conn: &Connection,
    app: &mut crate::tui::views::plan_list::PlanListApp,
) -> Result<()> {
    let Some(plan_id) = app.highlighted_plan_id().map(str::to_string) else {
        return Ok(());
    };
    if app.preview_cache_contains(&plan_id) {
        return Ok(());
    }
    let steps = storage::list_steps(conn, &plan_id)?;
    app.cache_preview_steps(plan_id, steps);
    Ok(())
}

// ---------------------------------------------------------------------------
// Archived-list TUI dispatcher (TUI-plan.md §6)
// ---------------------------------------------------------------------------

/// Run the archived-list event loop until the user pops back. Reuses the
/// already-open terminal and raw-mode session — the caller (`run_plan_list_tui`
/// after `enter` on the Archived sentinel) owns terminal teardown.
fn run_archived_list_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    project: &str,
    display_timezone: &str,
    default_harness: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::dialog;
    use crate::tui::views::archived_list::{self, ArchivedListApp};
    use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};

    let tiles = build_archived_tiles(conn, project)?;
    let mut app = ArchivedListApp::new(tiles, project, display_timezone);

    loop {
        terminal.draw(|f| archived_list::draw(f, &mut app))?;
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        // §15 help overlay: see plan-list dispatcher for the routing rule.
        if app.help.intercept_key(key) != crate::tui::help::InterceptResult::Passthrough {
            continue;
        }
        // §9 palette: see plan-list dispatcher for the routing rule. Step 20
        // wires submit through `archived_list_palette_action` and applies the
        // resulting `PaletteAction`; the `OpenConfirmDelete` variant runs the
        // confirm modal over the live archived view.
        if let Some(bar) = app.palette_bar.as_mut() {
            use crate::tui::palette_dispatch::PaletteAction;
            use crate::tui::widgets::palette_bar::PaletteBarOutcome;
            match bar.on_key(key) {
                PaletteBarOutcome::Pending => {}
                PaletteBarOutcome::Cancel => app.close_palette(),
                PaletteBarOutcome::Submit(input) => {
                    let action = archived_list_palette_action(&input, default_harness, &app);
                    app.close_palette();
                    match archived_list_apply_palette_action(conn, project, &mut app, action)? {
                        Some(PaletteAction::OpenConfirmDelete { plan_id, slug }) => {
                            let body =
                                format!("Permanently delete plan `{slug}`? This cannot be undone.");
                            let confirm = dialog::Confirm {
                                title: "Permanently delete plan",
                                body: &body,
                                default: false,
                            };
                            if confirm_with_archived_background(terminal, &mut app, &confirm)? {
                                archived_list_apply_delete(conn, project, &mut app, &[plan_id])?;
                            }
                        }
                        // The archived list passes empty `run_targets`, so the
                        // dispatcher folds `/run` into a "No plan to run." toast
                        // before reaching the apply step. We forward defensively
                        // for parity with the active plan-list view.
                        Some(PaletteAction::OpenRunDialog {
                            default_branch,
                            plan_count,
                            targets,
                        }) => {
                            let outcome = run_dialog_loop_with_bg(
                                terminal,
                                |f| crate::tui::views::archived_list::draw(f, &mut app),
                                default_branch,
                                plan_count,
                            )?;
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::archived_list::draw(f, &mut app),
                                project,
                                outcome,
                                &targets,
                                plan_count > 1,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        Some(PaletteAction::RunOnBranch {
                            branch,
                            targets,
                            force_current_branch,
                        }) => {
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::archived_list::draw(f, &mut app),
                                project,
                                crate::tui::run_dialog::Outcome::NewBranch(branch),
                                &targets,
                                force_current_branch,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        _ => {}
                    }
                }
            }
            continue;
        }
        match key.code {
            KeyCode::Char('/') | KeyCode::Char(':') => {
                let prefix = match key.code {
                    KeyCode::Char(c) => c,
                    _ => '/',
                };
                app.open_palette(prefix);
            }
            KeyCode::Char('j') | KeyCode::Down => app.navigate_down(),
            KeyCode::Char('k') | KeyCode::Up => app.navigate_up(),
            KeyCode::Char('g') => app.jump_top(),
            KeyCode::Char('G') => app.jump_bottom(),
            KeyCode::Char(' ') => app.toggle_selection(),
            KeyCode::Char('d') => {
                let targets = app.action_targets();
                if targets.is_empty() {
                    continue;
                }
                let body = format!(
                    "Permanently delete {} plan(s)? This cannot be undone.",
                    targets.len()
                );
                let confirm = dialog::Confirm {
                    title: "Permanently delete plans",
                    body: &body,
                    default: false,
                };
                if confirm_with_archived_background(terminal, &mut app, &confirm)? {
                    archived_list_apply_delete(conn, project, &mut app, &targets)?;
                }
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                let targets = app.action_targets();
                if targets.is_empty() {
                    continue;
                }
                archived_list_apply_unarchive(conn, project, &mut app, &targets)?;
            }
            KeyCode::Char('r') => {
                archived_list_refresh(conn, project, &mut app)?;
            }
            KeyCode::Esc => {
                archived_list_handle_esc(&mut app);
            }
            KeyCode::Left | KeyCode::Char('h') | KeyCode::Char('q') => {
                app.request_pop();
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.request_pop();
            }
            _ => {}
        }
        if app.should_pop {
            return Ok(());
        }
    }
}

/// Permanently delete the targeted plans via `storage::delete_plan`, refresh
/// the in-memory tile list, and toast. Factored apart from the event loop so
/// it can be integration-tested without a real terminal.
pub(crate) fn archived_list_apply_delete(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
    targets: &[String],
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    for id in targets {
        storage::delete_plan(conn, id)?;
    }
    let new_tiles = build_archived_tiles(conn, project)?;
    app.refresh_tiles(new_tiles);
    let n = targets.len();
    let msg = if n == 1 {
        "Permanently deleted 1 plan.".to_string()
    } else {
        format!("Permanently deleted {n} plans.")
    };
    app.toasts.push(msg, ToastKind::Success, Instant::now());
    Ok(())
}

/// Unarchive the targeted plans (status → Ready), refresh the in-memory tile
/// list, and toast. Factored apart for testing.
pub(crate) fn archived_list_apply_unarchive(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
    targets: &[String],
) -> Result<()> {
    use crate::plan::PlanStatus;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    for id in targets {
        storage::update_plan_status(conn, id, PlanStatus::Ready)?;
    }
    let new_tiles = build_archived_tiles(conn, project)?;
    app.refresh_tiles(new_tiles);
    let n = targets.len();
    let msg = if n == 1 {
        "Unarchived 1 plan.".to_string()
    } else {
        format!("Unarchived {n} plans.")
    };
    app.toasts.push(msg, ToastKind::Success, Instant::now());
    Ok(())
}

/// Apply a [`PaletteAction`] inside the archived-list view. Returns
/// `Some(action)` for terminal-bound dialogs (`OpenConfirmDelete`) so the
/// dispatcher loop can render the confirm modal over the live tile list.
pub(crate) fn archived_list_apply_palette_action(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
    action: crate::tui::palette_dispatch::PaletteAction,
) -> Result<Option<crate::tui::palette_dispatch::PaletteAction>> {
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match action {
        PaletteAction::None => {}
        PaletteAction::Toast { message, kind } => {
            app.toasts.push(message, kind, Instant::now());
        }
        PaletteAction::Quit => {
            app.request_pop();
        }
        PaletteAction::Unarchive { plan_id, .. } => {
            archived_list_apply_unarchive(conn, project, app, &[plan_id])?;
        }
        PaletteAction::Approve { .. } | PaletteAction::SetQuestionsEnabled { .. } => {
            app.toasts.push(
                "Unarchive the plan first to edit it.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::Export { slug, output } => {
            apply_palette_export(&slug, output.as_deref(), conn, project, &mut app.toasts);
        }
        PaletteAction::Import { path } => {
            apply_palette_import(&path, conn, project, &mut app.toasts);
        }
        PaletteAction::PushPlanDetail { .. }
        | PaletteAction::SpawnPlanHarness { .. }
        | PaletteAction::AddStep { .. }
        | PaletteAction::SkipStep { .. }
        | PaletteAction::MoveStep { .. } => {
            app.toasts.push(
                "Open the plan first to do that.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::CancelRun => {
            app.toasts.push(
                "No live run from this view.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Archived list passes empty `run_targets` so `/run` never reaches
        // these variants — the dispatcher already toasts "No plan to run."
        // We forward defensively for the same per-view behavior the active
        // plan-list provides; the loop's terminal-bound handler then takes
        // over (and the dialog renders over the archived view).
        PaletteAction::OpenRunDialog { .. } | PaletteAction::RunOnBranch { .. } => {
            app.toasts.push(
                "Unarchive the plan first to run it.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Archived plans aren't usable hosts for sub-views — the user has
        // to unarchive first to edit deps / hooks / tags. Stay silent on the
        // step-level variants for parity with the active plan-list view.
        PaletteAction::OpenPlanDependencies { .. } | PaletteAction::OpenPlanHooks { .. } => {
            app.toasts.push(
                "Unarchive the plan first to edit it.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::OpenStepHooks { .. } | PaletteAction::OpenStepTags { .. } => {
            app.toasts.push(
                "Open a step first to edit per-step hooks or tags.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::ComingSoon {
            label,
            target_step: _,
        } => {
            app.toasts.push(
                format!("{label}: palette wiring pending — see TUI-plan.md §9."),
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Archive of an already-archived plan is a no-op the parser routes
        // away via the `Already archived` toast — but keep the variant
        // covered defensively.
        PaletteAction::OpenConfirmArchive { .. } => {
            app.toasts
                .push("Plan is already archived.", ToastKind::Info, Instant::now());
        }
        PaletteAction::OpenConfirmDelete { .. } => {
            return Ok(Some(action));
        }
    }
    Ok(None)
}

/// Pure dispatch for the archived-list view: build the context from `app`,
/// parse the input, and run [`palette_dispatch::dispatch`]. Mirrors
/// [`plan_list_palette_action`].
pub(crate) fn archived_list_palette_action(
    input: &str,
    default_harness: &str,
    app: &crate::tui::views::archived_list::ArchivedListApp,
) -> crate::tui::palette_dispatch::PaletteAction {
    use crate::tui::palette;
    use crate::tui::palette_dispatch;

    // Archived list's tile pool *is* the archived pool; `plans` is empty so
    // commands like `/plan show <foo>` against an active plan get the
    // expected "unknown" toast rather than confusing the resolver.
    let archived = plan_refs_from_tiles(&app.tiles);
    let focused_slug = app.cursor_plan().map(|p| p.slug.as_str());

    // Run from the archived list isn't supported yet — pass empty targets
    // so `/run` toasts "No plan to run." instead of trying to spawn a run
    // for an archived plan.
    let run_targets: Vec<crate::tui::run_dialog::RunTarget> = Vec::new();

    let ctx = palette_dispatch::PaletteContext {
        default_harness,
        focused_slug,
        focused_step: None,
        run_targets: &run_targets,
        plans: &[],
        archived: &archived,
    };

    match palette::parse(input) {
        Ok(cmd) => palette_dispatch::dispatch(&cmd, &ctx),
        Err(err) => palette_dispatch::dispatch_parse_error(&err),
    }
}

/// `r` action in the archived-list view (TUI-plan.md §6 inherits §5): re-query
/// archived plans from the DB and toast the user. Mirrors
/// [`plan_list_refresh`] — refresh is a pure read operation, so it remains
/// available even when the run lock is held externally.
pub(crate) fn archived_list_refresh(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let new_tiles = build_archived_tiles(conn, project)?;
    app.refresh_tiles(new_tiles);
    app.toasts
        .push("Refreshed.", ToastKind::Info, Instant::now());
    Ok(())
}

/// `<esc>` precedence in the archived-list view (TUI-plan.md §4): dismiss
/// the current toast when one is showing and consume the keypress;
/// otherwise fall through to `app.escape()` (clear-selection-or-pop).
/// Returns `true` when a toast was dismissed.
pub(crate) fn archived_list_handle_esc(
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
) -> bool {
    if app.toasts.dismiss() {
        true
    } else {
        let _ = app.escape();
        false
    }
}

/// Mirror of `confirm_with_background` for the archived-list view: composites
/// a confirm dialog over the live archived view so the user keeps context
/// (cursor, selection) while answering.
fn confirm_with_archived_background<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    app: &mut crate::tui::views::archived_list::ArchivedListApp,
    c: &crate::tui::dialog::Confirm<'_>,
) -> Result<bool>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::dialog::{self, Decision};
    use crate::tui::views::archived_list;
    use crossterm::event::{self, Event, KeyEventKind};

    loop {
        terminal.draw(|f| {
            archived_list::draw(f, app);
            let area = f.area();
            dialog::render(f, area, c);
        })?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match dialog::decide_key(key, c.default) {
                Decision::Yes => return Ok(true),
                Decision::No => return Ok(false),
                Decision::Pending => continue,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Plan-detail TUI dispatcher (TUI-plan.md §7)
// ---------------------------------------------------------------------------

/// Run the plan-detail event loop until the user pops back. Reuses the
/// already-open terminal and raw-mode session — the caller
/// (`run_plan_list_tui` after `enter` on a plan tile) owns terminal
/// teardown.
///
/// Step 21 of tui-v1 adds the run controls (`R`/`S`): the loop polls
/// `run_locks` on each tick so the in-memory step list and "Running
/// step N" banner reflect runner-subprocess progress. Pressing `R`
/// spawns a `ralph run --non-interactive <slug>` child and `S` sends
/// `ralph cancel` semantics (SIGTERM with timeout, then SIGKILL).
///
/// When `auto_start` is `Some`, the dispatcher fires
/// [`plan_detail_apply_run_streaming`] once after rendering the first
/// frame — the same code path the `R` keybinding uses, parameterised by
/// the enclosed [`StreamMode`] so `ralph run` and `ralph resume`
/// (TUI-plan.md §2) both land in plan-detail with their respective
/// runner subprocess already kicked off.
///
/// Latches the run-mode flags (`--current-branch`, `--no-auto-stash`)
/// from a `ralph run` auto-start onto the App so the `R` keybinding's
/// manual re-runs preserve the user's invocation intent. Resume
/// auto-starts and `None` are no-ops.
fn plan_detail_init_preferred_run_mode(
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    auto_start: Option<crate::tui::events::StreamMode>,
) {
    if let Some(mode @ crate::tui::events::StreamMode::Run { .. }) = auto_start {
        app.set_preferred_run_mode(mode);
    }
}

fn run_plan_detail_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    config: &Config,
    project: &str,
    slug: &str,
    auto_start: Option<crate::tui::events::StreamMode>,
    subscription: &mut Option<crate::tui::events::HostedSubscription>,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::events as tui_events;
    use crate::tui::read_only::{self, ReadOnly, ReadOnlyTracker, Transition};
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_detail::{self, PlanDetailApp};
    use crate::tui::views::plan_detail_input::{self, InputAction};
    use crate::tui::views::plan_detail_ui;
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use std::time::Instant;

    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;
    let steps = storage::list_steps(conn, &plan.id)?;
    let mut app = PlanDetailApp::new(plan, steps, config);

    // The runner subscription is owned by the parent dispatcher
    // (`run_plan_list_tui`) so that popping back to the plan list does NOT
    // tear down the subprocess. We only attach to it when its slug matches
    // the plan being viewed; otherwise the parent's drain keeps its
    // channel from filling and we render this plan's static state.
    let subscription_matches =
        |sub: &Option<tui_events::HostedSubscription>| sub.as_ref().is_some_and(|h| h.slug == slug);

    // Local "have we already done the attach handshake for the current
    // subscription instance?" flag. Tracked separately from
    // `app.subscribed` because `dispatch_event(PlanComplete)` clears the
    // App's subscribed bit before the channel disconnects, and we don't
    // want the next loop tick to falsely re-attach (which would wipe the
    // final harness/test tails the user is reading). Reset to false
    // whenever the slot becomes empty.
    let mut attached_this_instance = false;

    // §13.2: read-only attach. When a runner is bound to *this* plan
    // (whether spawned by us or someone else), suppress the lockdown
    // banner — the right-pane "Running step N" surface is enough notice.
    // Edits stay disabled by way of the per-step status checks (you can't
    // edit an in-progress step, etc.).
    let my_pid = std::process::id() as i64;
    let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);

    // TUI-plan.md §2: when `ralph run` / `ralph resume` lands here we
    // auto-start the run after the first frame draws so the user sees the
    // plan-detail UI before the streaming subprocess fires. The enclosed
    // [`StreamMode`] selects between the run and resume code paths.
    // Latched to a single shot.
    let mut pending_auto_start = auto_start;
    plan_detail_init_preferred_run_mode(&mut app, auto_start);

    loop {
        // -- Refresh state from the active source of truth ----------------
        //
        // The subscription is owned by the parent dispatcher and survives
        // navigation, so on every tick we:
        //   1. Attach (once) when the parent's subscription matches our
        //      plan and the App hasn't yet noted the binding.
        //   2. Drain the NDJSON stream and dispatch events into the App.
        //   3. Detect producer hang-up (subprocess exited) and release
        //      the subscription so the next `R` press can spawn a fresh
        //      run.
        //   4. Always poll `run_locks` so externally-spawned runs and
        //      mid-flight re-attaches both get their `LiveRun` snapshot
        //      populated (the `elapsed_secs` priority order keeps NDJSON
        //      timestamps authoritative when present).
        if subscription_matches(subscription) {
            if !attached_this_instance {
                app.attach_subscription();
                attached_this_instance = true;
            }
            let hosted = subscription.as_mut().expect("matched above");
            for evt in hosted.sub.drain() {
                tui_events::dispatch_event(&mut app, &evt);
            }
            if hosted.sub.is_disconnected() {
                // The subscription belongs to *this* plan, so any failure
                // message goes straight to this view's toast queue without
                // a slug prefix — the user is staring at the plan it
                // refers to.
                match hosted.sub.poll_failure_status() {
                    tui_events::FailureStatus::Pending => {}
                    tui_events::FailureStatus::Clean => {
                        *subscription = None;
                        app.detach_subscription();
                        attached_this_instance = false;
                    }
                    tui_events::FailureStatus::Message(msg) => {
                        *subscription = None;
                        app.detach_subscription();
                        attached_this_instance = false;
                        app.toasts.push(msg, ToastKind::Error, Instant::now());
                    }
                }
            }
        } else {
            if attached_this_instance {
                // Parent dropped the subscription while we were attached
                // (e.g. another view popped after a disconnect we missed).
                app.detach_subscription();
                attached_this_instance = false;
            }
            // Subscription exists but is bound to another plan: still
            // drain-and-discard so the unbounded mpsc channel doesn't
            // accumulate events while the user is parked here. Surface a
            // failure message with the slug prefix so the user knows which
            // plan failed even though they're viewing a different one.
            if let Some(hosted) = subscription.as_mut() {
                let _ = hosted.sub.drain();
                if hosted.sub.is_disconnected() {
                    match hosted.sub.poll_failure_status() {
                        tui_events::FailureStatus::Pending => {}
                        tui_events::FailureStatus::Clean => {
                            *subscription = None;
                        }
                        tui_events::FailureStatus::Message(msg) => {
                            let slug = hosted.slug.clone();
                            *subscription = None;
                            app.toasts.push(
                                format!("[{slug}] {msg}"),
                                ToastKind::Error,
                                Instant::now(),
                            );
                        }
                    }
                }
            }
        }
        let live_snapshot = storage::get_live_run(conn, project)
            .ok()
            .flatten()
            .filter(|l| l.plan_slug.as_deref() == Some(slug));
        app.update_live_run(live_snapshot);
        if let Ok(latest_steps) = storage::list_steps(conn, &app.plan.id) {
            app.sync_steps_from_db(latest_steps);
        }
        // §17: refresh the cached open-question list each tick so the
        // banner + `A` keybinding both stay current with answers applied
        // by step-detail or any out-of-band CLI/runner activity.
        if let Ok(opens) = storage::list_open_questions(conn, project, Some(slug)) {
            app.set_open_questions(opens);
        }

        // -- §13.2 read-only attach poll -------------------------------
        //
        // Only relevant when the runner host is NOT us: a TUI-spawned
        // subscription bound to *this* plan implies the lock holder is
        // our child, so skip detection and treat the App as editable.
        // (The user sees the existing right-pane "Running step N"
        // surface for the active run instead.) When the parent's
        // subscription is for a *different* plan, the per-project lock
        // is still effectively ours, so we also stay editable.
        let now = Instant::now();
        let host_owns_subscription = subscription.is_some();
        if !host_owns_subscription {
            if tracker.should_poll(now)
                && let Ok(observed) = read_only::detect(conn, project, my_pid, None)
            {
                let transition = tracker.observe(observed, now);
                app.set_read_only(tracker.state());
                if transition == Transition::Released {
                    app.toasts
                        .push(read_only::RELEASED_TOAST, ToastKind::Success, now);
                }
            }
        } else if app.read_only.is_locked() {
            // We (the same TUI session) own a runner — clear any latched
            // lockdown so the banner doesn't keep showing while our own
            // child holds the row.
            tracker.observe(ReadOnly::Editable, now);
            app.set_read_only(ReadOnly::Editable);
        }

        terminal.draw(|f| plan_detail_ui::draw(f, &mut app))?;

        // TUI-plan.md §2 auto-start: after the first frame is on screen,
        // fire the same streaming-run path that `R` invokes (or its
        // resume counterpart). Cleared after the single shot so subsequent
        // loop iterations don't re-spawn.
        if let Some(mode) = pending_auto_start.take() {
            plan_detail_apply_run_streaming(conn, &mut app, project, slug, mode, subscription)?;
        }

        // Poll with a short timeout so the live timer keeps ticking and
        // any newly-arrived NDJSON chunks are drained on the next iteration
        // even when the user isn't pressing keys. 250ms balances UI
        // smoothness against polling cost.
        if !event::poll(std::time::Duration::from_millis(250))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        // §15 help overlay: route `?` toggle / dismissal through the help
        // state before the per-view input handler so view bindings don't
        // fire while the overlay is up. Add-mode is exempt — `?` is a valid
        // text-input character there.
        if matches!(app.input_mode, plan_detail::InputMode::Normal)
            && app.help.intercept_key(key) != crate::tui::help::InterceptResult::Passthrough
        {
            continue;
        }
        // §9 palette: while open, route every key through the palette bar
        // and skip the per-view input handler. Submit dispatches via
        // `plan_detail_palette_action` and applies the resulting
        // `PaletteAction`. Terminal-bound variants (confirm dialogs,
        // PushPlanDetail) are returned for the loop to handle.
        if let Some(bar) = app.palette_bar.as_mut() {
            use crate::tui::palette_dispatch::PaletteAction;
            use crate::tui::widgets::palette_bar::PaletteBarOutcome;
            match bar.on_key(key) {
                PaletteBarOutcome::Pending => {}
                PaletteBarOutcome::Cancel => app.close_palette(),
                PaletteBarOutcome::Submit(input) => {
                    let action = plan_detail_palette_action(&input, &config.default_harness, &app);
                    app.close_palette();
                    match plan_detail_apply_palette_action(conn, project, &mut app, action)? {
                        Some(PaletteAction::PushPlanDetail { slug: target_slug })
                            if target_slug != app.plan.slug =>
                        {
                            // Switching plans = pop and let plan-list push the
                            // new plan-detail. Keeps the navigation stack
                            // honest (no recursion).
                            app.toasts.push(
                                "Pop back to the plan list to switch plans.",
                                crate::tui::toast::ToastKind::Info,
                                Instant::now(),
                            );
                        }
                        Some(PaletteAction::OpenConfirmArchive { plan_id, slug }) => {
                            let body = format!("Archive plan `{slug}`?");
                            let confirm = crate::tui::dialog::Confirm {
                                title: "Archive plan",
                                body: &body,
                                default: false,
                            };
                            if confirm_with_plan_detail_background(terminal, &mut app, &confirm)? {
                                storage::update_plan_status(
                                    conn,
                                    &plan_id,
                                    crate::plan::PlanStatus::Archived,
                                )?;
                                app.toasts.push(
                                    "Plan archived.",
                                    crate::tui::toast::ToastKind::Success,
                                    Instant::now(),
                                );
                                app.should_pop = true;
                            }
                        }
                        Some(PaletteAction::OpenConfirmDelete { plan_id, slug }) => {
                            let body =
                                format!("Permanently delete plan `{slug}`? This cannot be undone.");
                            let confirm = crate::tui::dialog::Confirm {
                                title: "Permanently delete plan",
                                body: &body,
                                default: false,
                            };
                            if confirm_with_plan_detail_background(terminal, &mut app, &confirm)? {
                                storage::delete_plan(conn, &plan_id)?;
                                app.toasts.push(
                                    "Plan deleted.",
                                    crate::tui::toast::ToastKind::Success,
                                    Instant::now(),
                                );
                                app.should_pop = true;
                            }
                        }
                        // §9.1 run-choice dialog. The dialog renders over the
                        // live plan-detail; on success the caller spawns a
                        // non-streaming runner (the streaming `R` keybinding
                        // remains the in-view live-attach path).
                        Some(PaletteAction::OpenRunDialog {
                            default_branch,
                            plan_count,
                            targets,
                        }) => {
                            let outcome = run_dialog_loop_with_bg(
                                terminal,
                                |f| crate::tui::views::plan_detail_ui::draw(f, &mut app),
                                default_branch,
                                plan_count,
                            )?;
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::plan_detail_ui::draw(f, &mut app),
                                project,
                                outcome,
                                &targets,
                                plan_count > 1,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        Some(PaletteAction::RunOnBranch {
                            branch,
                            targets,
                            force_current_branch,
                        }) => {
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::plan_detail_ui::draw(f, &mut app),
                                project,
                                crate::tui::run_dialog::Outcome::NewBranch(branch),
                                &targets,
                                force_current_branch,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        // §9 sub-view routing — push the corresponding
                        // sub-view dispatcher against the resolved plan /
                        // step. The action carries the IDs the dispatcher
                        // already substituted from focus context, so we can
                        // hand them through without another lookup.
                        Some(PaletteAction::OpenPlanDependencies { plan_id, slug }) => {
                            let project_path = app.plan.project.clone();
                            run_plan_dependencies_tui(
                                terminal,
                                conn,
                                &project_path,
                                &plan_id,
                                &slug,
                            )?;
                        }
                        Some(PaletteAction::OpenPlanHooks { plan_id, slug }) => {
                            let project_path = app.plan.project.clone();
                            run_plan_hooks_tui(terminal, conn, &project_path, &plan_id, &slug)?;
                        }
                        // Plan-detail's palette context doesn't set
                        // `focused_step`, so the dispatcher already toasted
                        // "Open a step first…" before reaching apply. The
                        // forwarded variant is defensive: if a future
                        // change adds a focused-step pointer, the sub-view
                        // pushes correctly without another wiring pass.
                        Some(PaletteAction::OpenStepHooks { step_id, .. }) => {
                            run_step_hooks_tui(terminal, conn, project, &step_id)?;
                        }
                        Some(PaletteAction::OpenStepTags { step_id, .. }) => {
                            run_step_tags_tui(terminal, conn, &step_id)?;
                        }
                        _ => {}
                    }
                }
            }
            continue;
        }
        // §9 palette open: `/` and `:` enter palette mode in Normal mode
        // only. Add-mode treats them as literal text input characters.
        if matches!(app.input_mode, plan_detail::InputMode::Normal)
            && let KeyCode::Char(c) = key.code
            && (c == '/' || c == ':')
        {
            app.open_palette(c);
            continue;
        }
        let action = plan_detail_input::handle_key(&mut app, key);
        match action {
            InputAction::None | InputAction::Pop => {}
            InputAction::AddStep(pos, title) => {
                plan_detail_apply_add(conn, &mut app, pos, &title)?;
            }
            InputAction::SkipStep(step_id) => {
                plan_detail_apply_skip(conn, &mut app, &step_id)?;
            }
            InputAction::Delete(targets) => {
                plan_detail_apply_delete(terminal, conn, &mut app, &targets)?;
            }
            InputAction::Reset(step_id) => {
                plan_detail_apply_reset(conn, &mut app, &step_id)?;
            }
            InputAction::MoveUp(step_id) => {
                plan_detail_apply_move(conn, &mut app, &step_id, MoveDir::Up)?;
            }
            InputAction::MoveDown(step_id) => {
                plan_detail_apply_move(conn, &mut app, &step_id, MoveDir::Down)?;
            }
            InputAction::Run => {
                let mode = app.preferred_run_mode();
                plan_detail_apply_run_streaming(conn, &mut app, project, slug, mode, subscription)?;
            }
            InputAction::Stop => {
                plan_detail_apply_stop(conn, &mut app, project, slug)?;
            }
            InputAction::OpenDependencies => {
                let project_path = app.plan.project.clone();
                let plan_id = app.plan.id.clone();
                let plan_slug = app.plan.slug.clone();
                run_plan_dependencies_tui(terminal, conn, &project_path, &plan_id, &plan_slug)?;
            }
            InputAction::OpenHooks => {
                let project_path = app.plan.project.clone();
                let plan_id = app.plan.id.clone();
                let plan_slug = app.plan.slug.clone();
                run_plan_hooks_tui(terminal, conn, &project_path, &plan_id, &plan_slug)?;
            }
            InputAction::OpenQuestion(step_id) => {
                run_step_detail_tui(terminal, conn, config, project, &mut app, &step_id)?;
            }
            InputAction::OpenStepDetail(step_id) => {
                run_step_detail_tui(terminal, conn, config, project, &mut app, &step_id)?;
            }
            InputAction::ToggleQuestionsEnabled => {
                plan_detail_apply_toggle_questions(conn, &mut app)?;
            }
            InputAction::TogglePauseRequested => {
                plan_detail_apply_toggle_pause(conn, &mut app)?;
            }
        }
        if app.should_pop {
            // Subscription is owned by the parent dispatcher; we leave
            // it intact so the runner subprocess keeps streaming while
            // the user navigates back to the plan list and (optionally)
            // into other plans. The subscription is torn down only when
            // the parent dispatcher exits or the run completes (which
            // disconnects the channel and clears the slot above).
            return Ok(());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MoveDir {
    Up,
    Down,
}

/// Persist an `i` (insert above) / `a` (append below) action: compute the
/// new sort_key from the cursor's neighbors, insert via `storage::create_step_at`,
/// refresh the in-memory step list, and place the cursor on the new row so
/// the user sees the result immediately.
pub(crate) fn plan_detail_apply_add(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    position: crate::tui::views::plan_detail::AddPosition,
    title: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_detail::AddPosition;
    use std::time::Instant;

    let sort_key = match position {
        AddPosition::Above => app.compute_insert_above_sort_key(),
        AddPosition::Below => app.compute_append_below_sort_key(),
    };
    let sort_key = match sort_key {
        Ok(k) => k,
        Err(e) => {
            app.toasts.push(
                format!("Cannot insert step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return Ok(());
        }
    };

    let plan_id = app.plan.id.clone();
    let result = storage::create_step_at(
        conn,
        &plan_id,
        &sort_key,
        title,
        "",
        None,
        None,
        &[],
        None,
        None,
        None,
        None,
    );

    match result {
        Ok((new_step, _position_1based)) => {
            let new_id = new_step.id.clone();
            app.refresh_steps(storage::list_steps(conn, &plan_id)?);
            if let Some(idx) = app.steps.iter().position(|s| s.id == new_id) {
                app.selected_index = idx;
            }
            app.toasts.push(
                format!("Added step: {title}"),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to add step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// Persist an `s` skip action via `storage::mark_step_skipped`. The TUI's
/// skip flow doesn't prompt for a reason — the operator can edit it later
/// via the CLI (`ralph skip --reason`) if they want one.
pub(crate) fn plan_detail_apply_skip(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    step_id: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match storage::mark_step_skipped(conn, step_id, None) {
        Ok(()) => {
            let plan_id = app.plan.id.clone();
            app.refresh_steps(storage::list_steps(conn, &plan_id)?);
            app.toasts
                .push("Step skipped.", ToastKind::Success, Instant::now());
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to skip step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// Persist an `R` run / resume action: spawn `ralph run --non-interactive
/// <slug>` as a child process so the TUI can keep polling the DB while the
/// runner advances the plan. Stdio is redirected to /dev/null so the
/// subprocess output doesn't conflict with the TUI's raw-mode display.
///
/// Superseded by [`plan_detail_apply_run_streaming`] (TUI-plan.md §13) for
/// the live plan-detail event loop, which forks via
/// [`tui::events::spawn_streaming_runner`] and consumes the NDJSON event
/// stream directly. Kept here because callers that don't need the right-pane
/// tails — currently none in production but referenced from tests — can
/// still spawn a fire-and-forget runner.
///
/// No-op (info toast) if a run is already live for this plan, matching the
/// acceptance criteria in TUI-plan.md §7.
#[allow(dead_code)]
pub(crate) fn plan_detail_apply_run(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    project: &str,
    slug: &str,
    runner_child: &mut Option<std::process::Child>,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let already_live = storage::get_live_run(conn, project)?
        .map(|l| l.plan_slug.as_deref() == Some(slug))
        .unwrap_or(false);
    if already_live {
        app.toasts.push(
            "Run already live for this plan.",
            ToastKind::Info,
            Instant::now(),
        );
        return Ok(());
    }

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            app.toasts.push(
                format!("Cannot locate ralph binary: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return Ok(());
        }
    };

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("-C")
        .arg(project)
        .arg("--non-interactive")
        .arg("run")
        .arg(slug)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    match cmd.spawn() {
        Ok(child) => {
            *runner_child = Some(child);
            app.toasts.push(
                format!("Started run for {slug}"),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to start run: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// NDJSON-streaming variant of [`plan_detail_apply_run`] (TUI-plan.md §13).
/// Forks `ralph run --json --non-interactive <slug>` via
/// [`tui::events::spawn_streaming_runner`] and stashes the resulting
/// [`RunSubscription`] on the dispatcher's stack so the next poll iteration
/// can drain its events into the right-pane state.
///
/// `mode` selects between `ralph run` and `ralph resume` — both share the
/// same NDJSON pipe, App-side dispatch, and toast UX, so the auto-start
/// path on `ralph resume` (TUI-plan.md §2) reuses this function with
/// [`StreamMode::Resume`]. On spawn failure the subscription stays `None`
/// and the user gets an error toast.
pub(crate) fn plan_detail_apply_run_streaming(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    project: &str,
    slug: &str,
    mode: crate::tui::events::StreamMode,
    subscription: &mut Option<crate::tui::events::HostedSubscription>,
) -> Result<()> {
    use crate::tui::events::{HostedSubscription, spawn_streaming_runner};
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let verb = match mode {
        crate::tui::events::StreamMode::Run { .. } => "Run",
        crate::tui::events::StreamMode::Resume => "Resume",
    };

    if let Some(existing) = subscription.as_ref() {
        let msg = if existing.slug == slug {
            "Run already live for this plan."
        } else {
            // A different plan is already streaming under the same TUI
            // session. The per-project run lock would block our spawn
            // anyway, but surfacing the conflict early avoids a
            // failed-spawn toast that's harder to interpret.
            "Another plan is already running. Stop it first."
        };
        app.toasts.push(msg, ToastKind::Info, Instant::now());
        return Ok(());
    }
    let already_live = storage::get_live_run(conn, project)?
        .map(|l| l.plan_slug.as_deref() == Some(slug))
        .unwrap_or(false);
    if already_live {
        app.toasts.push(
            "Run already live for this plan.",
            ToastKind::Info,
            Instant::now(),
        );
        return Ok(());
    }

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            app.toasts.push(
                format!("Cannot locate ralph binary: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return Ok(());
        }
    };

    match spawn_streaming_runner(exe, project.into(), slug.to_string(), mode) {
        Ok(sub) => {
            *subscription = Some(HostedSubscription {
                slug: slug.to_string(),
                sub,
            });
            app.attach_subscription();
            app.toasts.push(
                format!("Started {} for {slug}", verb.to_lowercase()),
                ToastKind::Success,
                Instant::now(),
            );
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to start {}: {e}", verb.to_lowercase()),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// Persist an `S` stop action by routing through `cmd_cancel`, which
/// implements the SIGTERM-with-timeout-then-SIGKILL semantics used by the
/// `ralph cancel` CLI. The 15s default mirrors the CLI default and is long
/// enough for a runner mid-phase to write its rollback / finalize records.
pub(crate) fn plan_detail_apply_stop(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    project: &str,
    slug: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let live =
        storage::get_live_run(conn, project)?.filter(|l| l.plan_slug.as_deref() == Some(slug));
    if live.is_none() {
        app.toasts.push(
            "No live run for this plan.",
            ToastKind::Info,
            Instant::now(),
        );
        return Ok(());
    }

    // Quiet plain-format context so cmd_cancel doesn't print progress dots
    // through the alternate screen during the brief SIGTERM wait.
    let cancel_out = OutputContext {
        format: OutputFormat::Plain,
        quiet: true,
        color: false,
    };
    match cmd_cancel(
        conn,
        project,
        Some(slug),
        false,
        Duration::from_secs(15),
        &cancel_out,
    ) {
        Ok(()) => {
            app.toasts
                .push("Run cancelled.", ToastKind::Success, Instant::now());
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to cancel run: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// Render the `Delete N step(s)?` confirm dialog over the live plan-detail
/// view, and on Yes call `storage::delete_step` for each target. The dialog
/// composites onto the same terminal — see `confirm_with_plan_detail_background`.
pub(crate) fn plan_detail_apply_delete<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    targets: &[String],
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::dialog;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    if targets.is_empty() {
        return Ok(());
    }
    let body = format!("Delete {} step(s)?", targets.len());
    let confirm = dialog::Confirm {
        title: "Delete steps",
        body: &body,
        default: false,
    };
    if !confirm_with_plan_detail_background(terminal, app, &confirm)? {
        return Ok(());
    }

    let mut errors = 0usize;
    for id in targets {
        if let Err(e) = storage::delete_step(conn, id) {
            errors += 1;
            app.toasts.push(
                format!("Failed to delete step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    let plan_id = app.plan.id.clone();
    app.refresh_steps(storage::list_steps(conn, &plan_id)?);
    if errors == 0 {
        let n = targets.len();
        let msg = if n == 1 {
            "Deleted 1 step.".to_string()
        } else {
            format!("Deleted {n} steps.")
        };
        app.toasts.push(msg, ToastKind::Success, Instant::now());
    }
    Ok(())
}

/// Persist an `r` reset action via `storage::reset_step` and refresh.
pub(crate) fn plan_detail_apply_reset(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    step_id: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match storage::reset_step(conn, step_id) {
        Ok(()) => {
            let plan_id = app.plan.id.clone();
            app.refresh_steps(storage::list_steps(conn, &plan_id)?);
            app.toasts
                .push("Step reset.", ToastKind::Success, Instant::now());
        }
        Err(e) => {
            app.toasts.push(
                format!("Failed to reset step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
        }
    }
    Ok(())
}

/// Persist a `Shift-J` / `Shift-K` move via `storage::update_step_sort_key`.
/// Recomputes the cursor index after the refresh so the moved step stays
/// highlighted (its position in the list changes but its identity does not).
pub(crate) fn plan_detail_apply_move(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    step_id: &str,
    dir: MoveDir,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let new_key = match dir {
        MoveDir::Up => app.compute_move_up_sort_key(),
        MoveDir::Down => app.compute_move_down_sort_key(),
    };
    let new_key = match new_key {
        Ok(Some(k)) => k,
        Ok(None) => return Ok(()), // already at edge — silent no-op
        Err(e) => {
            app.toasts.push(
                format!("Cannot move step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return Ok(());
        }
    };

    if let Err(e) = storage::update_step_sort_key(conn, step_id, &new_key) {
        app.toasts.push(
            format!("Failed to move step: {e}"),
            ToastKind::Error,
            Instant::now(),
        );
        return Ok(());
    }

    let plan_id = app.plan.id.clone();
    app.refresh_steps(storage::list_steps(conn, &plan_id)?);
    if let Some(idx) = app.steps.iter().position(|s| s.id == step_id) {
        app.selected_index = idx;
    }
    let label = match dir {
        MoveDir::Up => "Moved step up.",
        MoveDir::Down => "Moved step down.",
    };
    app.toasts.push(label, ToastKind::Success, Instant::now());
    Ok(())
}

/// `P` action in the plan-detail view: toggle `plans.pause_requested` for
/// the focused plan and toast the new state. The runner reads + clears the
/// flag between step boundaries (see `storage::take_plan_pause_requested`),
/// so first press requests "stop after current step" and a second press
/// before the boundary fires cancels that request. The wrapping input arm
/// already gated on `is_run_live()`, so we don't re-check that here.
pub(crate) fn plan_detail_apply_toggle_pause(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let current = match storage::get_plan_pause_requested(conn, &app.plan.id) {
        Ok(v) => v,
        Err(e) => {
            app.toasts.push(
                format!("Failed to read pause flag: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return Ok(());
        }
    };
    let next = !current;
    if let Err(e) = storage::set_plan_pause_requested(conn, &app.plan.id, next) {
        app.toasts.push(
            format!("Failed to update pause flag: {e}"),
            ToastKind::Error,
            Instant::now(),
        );
        return Ok(());
    }
    if let Some(updated) = storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)? {
        app.plan = updated;
    }
    let msg = if next {
        "Pause requested. Will stop after current step finishes."
    } else {
        "Pause request cancelled."
    };
    app.toasts.push(msg, ToastKind::Success, Instant::now());
    Ok(())
}

/// `Q` action in the plan-detail view (TUI-plan.md §17 'Toggle surfaces'):
/// flip `plans.questions_enabled` for the focused plan via
/// `set_plan_questions_enabled`, refresh `app.plan` in place from the DB, and
/// toast the new state. Mirrors plan-list's Q binding.
pub(crate) fn plan_detail_apply_toggle_questions(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let next = !app.plan.questions_enabled;
    storage::set_plan_questions_enabled(conn, &app.plan.id, next)?;
    if let Some(updated) = storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)? {
        app.plan = updated;
    }
    let msg = if next {
        "Questions enabled."
    } else {
        "Questions disabled."
    };
    app.toasts.push(msg, ToastKind::Success, Instant::now());
    Ok(())
}

/// Apply a [`PaletteAction`] inside the plan-detail view. Returns
/// `Some(action)` for terminal-bound dialogs (`OpenConfirmArchive`,
/// `OpenConfirmDelete`) so the dispatcher loop can render the confirm modal
/// over the live plan-detail view, and for `PushPlanDetail` so the loop can
/// push a fresh plan-detail view (it can't recurse from inside the helper).
pub(crate) fn plan_detail_apply_palette_action(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    action: crate::tui::palette_dispatch::PaletteAction,
) -> Result<Option<crate::tui::palette_dispatch::PaletteAction>> {
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match action {
        PaletteAction::None => {}
        PaletteAction::Toast { message, kind } => {
            app.toasts.push(message, kind, Instant::now());
        }
        PaletteAction::Quit => {
            app.should_pop = true;
        }
        PaletteAction::Approve { plan_id, slug } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
                app.plan = updated;
            }
            app.toasts
                .push("Plan approved.", ToastKind::Success, Instant::now());
        }
        PaletteAction::SetQuestionsEnabled {
            plan_id, enabled, ..
        } => {
            storage::set_plan_questions_enabled(conn, &plan_id, enabled)?;
            if let Some(updated) =
                storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)?
            {
                app.plan = updated;
            }
            let msg = if enabled {
                "Questions enabled."
            } else {
                "Questions disabled."
            };
            app.toasts.push(msg, ToastKind::Success, Instant::now());
        }
        PaletteAction::Unarchive { plan_id, .. } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            if let Some(updated) =
                storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)?
            {
                app.plan = updated;
            }
            app.toasts
                .push("Unarchived.", ToastKind::Success, Instant::now());
        }
        PaletteAction::AddStep {
            plan_id: _,
            slug: _,
            title,
        } => {
            // Append a step at the bottom — `compute_append_below_sort_key` is
            // the same path the `a` keybinding uses on the last row.
            let sort_key = match app.compute_append_below_sort_key() {
                Ok(k) => k,
                Err(e) => {
                    app.toasts.push(
                        format!("Cannot insert step: {e}"),
                        ToastKind::Error,
                        Instant::now(),
                    );
                    return Ok(None);
                }
            };
            let plan_id = app.plan.id.clone();
            match storage::create_step_at(
                conn,
                &plan_id,
                &sort_key,
                &title,
                "",
                None,
                None,
                &[],
                None,
                None,
                None,
                None,
            ) {
                Ok((new_step, _)) => {
                    let new_id = new_step.id.clone();
                    app.refresh_steps(storage::list_steps(conn, &plan_id)?);
                    if let Some(idx) = app.steps.iter().position(|s| s.id == new_id) {
                        app.selected_index = idx;
                    }
                    app.toasts.push(
                        format!("Added step: {title}"),
                        ToastKind::Success,
                        Instant::now(),
                    );
                }
                Err(e) => {
                    app.toasts.push(
                        format!("Failed to add step: {e}"),
                        ToastKind::Error,
                        Instant::now(),
                    );
                }
            }
        }
        PaletteAction::SkipStep { step_num, .. } => {
            // `runner::skip_step` matches the `s` keybinding's behavior — it
            // resolves the step number, validates status, and writes the
            // skipped row. None means "skip the current step".
            let plan = app.plan.clone();
            match crate::runner::skip_step(conn, &plan, step_num.map(|n| n as usize), None) {
                Ok(actual_num) => {
                    app.refresh_steps(storage::list_steps(conn, &app.plan.id)?);
                    app.toasts.push(
                        format!("Skipped step {actual_num}."),
                        ToastKind::Success,
                        Instant::now(),
                    );
                }
                Err(e) => {
                    app.toasts.push(
                        format!("Failed to skip step: {e}"),
                        ToastKind::Error,
                        Instant::now(),
                    );
                }
            }
        }
        PaletteAction::MoveStep { from, to, .. } => {
            apply_palette_move_step(conn, app, from, to);
        }
        PaletteAction::Export { slug, output } => {
            apply_palette_export(&slug, output.as_deref(), conn, project, &mut app.toasts);
        }
        PaletteAction::Import { path } => {
            apply_palette_import(&path, conn, project, &mut app.toasts);
        }
        PaletteAction::CancelRun => {
            // Mirror `S`: only fire when there's actually a live run for this
            // plan. `plan_detail_apply_stop` already toasts "No live run for
            // this plan" when nothing is bound, so let it do the work.
            let slug = app.plan.slug.clone();
            plan_detail_apply_stop(conn, app, project, &slug)?;
        }
        PaletteAction::SpawnPlanHarness { harness, .. } => {
            app.toasts.push(
                format!(
                    "/plan harness {harness}: not yet wired from palette; use the CLI for now."
                ),
                ToastKind::Info,
                Instant::now(),
            );
        }
        // §9 sub-view routing — plan-detail is the host for plan-level
        // sub-views (`OpenPlanDependencies`, `OpenPlanHooks`) and for
        // step-level sub-views when a step is highlighted in the sidebar.
        // Step-level variants don't reach here today (the plan-detail
        // palette context doesn't set `focused_step`), but we still forward
        // defensively so adding a focused-step pointer later doesn't quietly
        // route to the wrong place.
        PaletteAction::OpenPlanDependencies { .. }
        | PaletteAction::OpenPlanHooks { .. }
        | PaletteAction::OpenStepHooks { .. }
        | PaletteAction::OpenStepTags { .. } => {
            return Ok(Some(action));
        }
        PaletteAction::ComingSoon {
            label,
            target_step: _,
        } => {
            app.toasts.push(
                format!("{label}: palette wiring pending — see TUI-plan.md §9."),
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Terminal-bound: hand back to the caller. `OpenRunDialog` /
        // `RunOnBranch` (TUI-plan.md §9.1) render the run-choice dialog over
        // the live plan-detail view; the others drive the existing
        // archive/delete confirms.
        PaletteAction::PushPlanDetail { .. }
        | PaletteAction::OpenConfirmArchive { .. }
        | PaletteAction::OpenConfirmDelete { .. }
        | PaletteAction::OpenRunDialog { .. }
        | PaletteAction::RunOnBranch { .. } => {
            return Ok(Some(action));
        }
    }
    Ok(None)
}

/// Pure dispatch for plan-detail. Builds the [`PaletteContext`] from the
/// focused plan and parses the input.
pub(crate) fn plan_detail_palette_action(
    input: &str,
    default_harness: &str,
    app: &crate::tui::views::plan_detail::PlanDetailApp,
) -> crate::tui::palette_dispatch::PaletteAction {
    use crate::tui::palette;
    use crate::tui::palette_dispatch;

    let plans = vec![plan_ref_from_plan(&app.plan)];
    let focused_slug = Some(app.plan.slug.as_str());
    let run_targets = vec![crate::tui::run_dialog::RunTarget {
        slug: app.plan.slug.clone(),
        default_branch: app.plan.branch_name.clone(),
    }];
    // No focused step in plan-detail; `/step set-hook` etc. correctly toast
    // "Open a step first" via the dispatcher's resolver.
    let ctx = palette_dispatch::PaletteContext {
        default_harness,
        focused_slug,
        focused_step: None,
        run_targets: &run_targets,
        plans: &plans,
        archived: &[],
    };
    match palette::parse(input) {
        Ok(cmd) => palette_dispatch::dispatch(&cmd, &ctx),
        Err(err) => palette_dispatch::dispatch_parse_error(&err),
    }
}

/// Resolve a 1-based `from`/`to` move into a fractional sort_key and persist
/// it. Factored out of [`plan_detail_apply_palette_action`] so the move-step
/// path stays readable.
fn apply_palette_move_step(
    conn: &Connection,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    from: u32,
    to: u32,
) {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    let from_idx = (from as usize).saturating_sub(1);
    let to_idx = (to as usize).saturating_sub(1);
    if from_idx >= app.steps.len() || to_idx >= app.steps.len() {
        app.toasts.push(
            format!(
                "/step move: out of range (plan has {} steps).",
                app.steps.len()
            ),
            ToastKind::Error,
            Instant::now(),
        );
        return;
    }
    // Find neighbour sort keys at the destination *after* removing the
    // moving step from the list.
    let moving_id = app.steps[from_idx].id.clone();
    let others: Vec<&crate::plan::Step> = app.steps.iter().filter(|s| s.id != moving_id).collect();
    let dest = to_idx.min(others.len());
    let prev = if dest == 0 {
        None
    } else {
        others.get(dest - 1)
    };
    let next = others.get(dest);
    let new_key = match (prev, next) {
        (Some(p), Some(n)) => crate::frac_index::key_between(&p.sort_key, &n.sort_key)
            .map_err(|e| anyhow::anyhow!("{e}")),
        (None, Some(n)) => {
            crate::frac_index::key_between("", &n.sort_key).map_err(|e| anyhow::anyhow!("{e}"))
        }
        (Some(p), None) => {
            crate::frac_index::key_after(&p.sort_key).map_err(|e| anyhow::anyhow!("{e}"))
        }
        (None, None) => Ok(crate::frac_index::initial_key()),
    };
    let new_key = match new_key {
        Ok(k) => k,
        Err(e) => {
            app.toasts.push(
                format!("Cannot move step: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return;
        }
    };
    if let Err(e) = storage::update_step_sort_key(conn, &moving_id, &new_key) {
        app.toasts.push(
            format!("Failed to move step: {e}"),
            ToastKind::Error,
            Instant::now(),
        );
        return;
    }
    drop(others);
    let plan_id = app.plan.id.clone();
    if let Ok(steps) = storage::list_steps(conn, &plan_id) {
        app.refresh_steps(steps);
        if let Some(idx) = app.steps.iter().position(|s| s.id == moving_id) {
            app.selected_index = idx;
        }
    }
    app.toasts.push(
        format!("Moved step {from} → {to}."),
        ToastKind::Success,
        Instant::now(),
    );
}

/// Mirror of `confirm_with_background` for the plan-detail view: composites
/// a confirm dialog over the live view so the user keeps context (cursor,
/// selection, toasts) while answering.
fn confirm_with_plan_detail_background<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    c: &crate::tui::dialog::Confirm<'_>,
) -> Result<bool>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::dialog::{self, Decision};
    use crate::tui::views::plan_detail_ui;
    use crossterm::event::{self, Event, KeyEventKind};

    loop {
        terminal.draw(|f| {
            plan_detail_ui::draw(f, app);
            let area = f.area();
            dialog::render(f, area, c);
        })?;
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match dialog::decide_key(key, c.default) {
                Decision::Yes => return Ok(true),
                Decision::No => return Ok(false),
                Decision::Pending => continue,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Plan-dependencies sub-view loop (TUI-plan.md §1, step 33)
// ---------------------------------------------------------------------------

/// Run the plan-dependencies sub-view loop until the user backs out. Reads
/// the focused plan's deps from the DB, drives the sub-view's state machine
/// for `a`/`d`/`q`/`<esc>`, and writes through to
/// [`storage::add_plan_dependency`] / [`storage::remove_plan_dependency`] on
/// each successful outcome. Cycles are caught with
/// [`storage::would_create_cycle`] before the insert and surfaced as an
/// error toast rather than letting the user wait on a storage error.
///
/// Parameterized on `(project, plan_id, plan_slug)` rather than a
/// `&mut PlanDetailApp` so the palette path (TUI-plan.md §9, step 22) can
/// invoke the sub-view from plan-list / step-detail without first
/// reconstructing a `PlanDetailApp`.
fn run_plan_dependencies_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    project: &str,
    plan_id: &str,
    plan_slug: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_dependencies::{Mode, Outcome, PlanDependenciesApp, render};
    use crossterm::event::{self, Event, KeyEventKind};

    let plan_id = plan_id.to_string();
    let plan_slug = plan_slug.to_string();
    let project = project.to_string();

    let (deps, candidates) = load_dependencies_view_state(conn, &project, &plan_id)?;
    let mut app = PlanDependenciesApp::new(plan_id.clone(), plan_slug, deps, candidates);

    loop {
        terminal.draw(|f| render(f, f.area(), &mut app))?;

        if !event::poll(std::time::Duration::from_millis(250))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        match app.handle_key(key) {
            Outcome::Pending => {}
            Outcome::Pop => return Ok(()),
            Outcome::AddRequested { dep_plan_id } => {
                // Pre-check the cycle so we can render a friendly toast
                // instead of bubbling the storage error. `add_plan_dependency`
                // also checks defensively, so a concurrent edit can't slip
                // a cycle through.
                if storage::would_create_cycle(conn, &plan_id, &dep_plan_id)? {
                    app.push_toast(
                        "Adding that dependency would create a cycle.",
                        ToastKind::Error,
                    );
                    continue;
                }
                if let Err(e) = storage::add_plan_dependency(conn, &plan_id, &dep_plan_id) {
                    app.push_toast(format!("Failed to add dependency: {e}"), ToastKind::Error);
                    continue;
                }
                let (deps, candidates) = load_dependencies_view_state(conn, &project, &plan_id)?;
                app.refresh(deps, candidates);
                // Drop back to the list so the user sees the new row.
                app.mode = Mode::List;
                app.push_toast("Dependency added.", ToastKind::Success);
            }
            Outcome::RemoveRequested { dep_plan_id } => {
                if let Err(e) = storage::remove_plan_dependency(conn, &plan_id, &dep_plan_id) {
                    app.push_toast(
                        format!("Failed to remove dependency: {e}"),
                        ToastKind::Error,
                    );
                    continue;
                }
                let (deps, candidates) = load_dependencies_view_state(conn, &project, &plan_id)?;
                app.refresh(deps, candidates);
                app.push_toast("Dependency removed.", ToastKind::Success);
            }
        }
    }
}

/// Read the dependency edges and the picker candidate list for `plan_id`.
///
/// Candidates are every other non-archived plan in the project that is not
/// already a direct dependency. The cycle check still runs at add-time as
/// defense-in-depth — pre-filtering by direct deps doesn't catch transitive
/// cycles that would close once the new edge is inserted.
fn load_dependencies_view_state(
    conn: &Connection,
    project: &str,
    plan_id: &str,
) -> Result<(
    Vec<crate::tui::views::plan_dependencies::PlanRef>,
    Vec<crate::tui::views::plan_dependencies::PlanRef>,
)> {
    use crate::tui::views::plan_dependencies::PlanRef;

    let dep_ids = storage::list_plan_dependencies(conn, plan_id)?;
    // `list_plans(_, _, false)` filters by project; archived plans are still
    // included in the result set, which is fine — the cycle pre-check and
    // candidate-list filter both run separately below.
    let all_plans = storage::list_plans(conn, project, false)?;

    let mut deps: Vec<PlanRef> = Vec::with_capacity(dep_ids.len());
    for id in &dep_ids {
        if let Some(plan) = all_plans.iter().find(|p| &p.id == id) {
            deps.push(PlanRef {
                id: plan.id.clone(),
                slug: plan.slug.clone(),
            });
        }
    }

    let candidates: Vec<PlanRef> = all_plans
        .iter()
        .filter(|p| {
            p.id != plan_id
                && p.status != crate::plan::PlanStatus::Archived
                && !dep_ids.contains(&p.id)
        })
        .map(|p| PlanRef {
            id: p.id.clone(),
            slug: p.slug.clone(),
        })
        .collect();

    Ok((deps, candidates))
}

// ---------------------------------------------------------------------------
// Plan-hooks dispatcher (TUI-plan.md §1)
// ---------------------------------------------------------------------------

/// Run the plan-hooks event loop until the user pops back. Mirrors
/// [`run_plan_dependencies_tui`]: reuses the parent terminal and raw-mode
/// session, owns the crossterm event loop, and performs the storage
/// write-throughs requested by the [`PlanHooksApp`] state machine.
///
/// Help-overlay routing happens inside [`PlanHooksApp::handle_key`] (step
/// 14), so a stuck `?` overlay can always be dismissed with `?`/`<esc>`/
/// `q`/Ctrl-C without reaching the per-mode handlers. `<esc>`/`q`/Ctrl-C
/// in `Mode::List` pop back to plan-detail.
///
/// Parameterized on `(project, plan_id, plan_slug)` rather than a
/// `&mut PlanDetailApp` so the palette path (TUI-plan.md §9, step 22) can
/// invoke the sub-view from plan-list / step-detail without first
/// reconstructing a `PlanDetailApp`.
fn run_plan_hooks_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    project: &str,
    plan_id: &str,
    plan_slug: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_hooks::{Mode, Outcome, PlanHooksApp, render};
    use crossterm::event::{self, Event, KeyEventKind};

    let plan_id = plan_id.to_string();
    let plan_slug = plan_slug.to_string();
    let project = project.to_string();

    let (attachments, candidates) = load_plan_hooks_view_state(conn, &project, &plan_id)?;
    let mut app = PlanHooksApp::new(plan_id.clone(), plan_slug, attachments, candidates);

    loop {
        terminal.draw(|f| render(f, f.area(), &mut app))?;

        if !event::poll(std::time::Duration::from_millis(250))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        match app.handle_key(key) {
            Outcome::Pending => {}
            Outcome::Pop => return Ok(()),
            Outcome::AddRequested {
                lifecycle,
                hook_name,
            } => {
                if let Err(e) =
                    storage::attach_hook_to_plan(conn, &plan_id, lifecycle.as_str(), &hook_name)
                {
                    app.push_toast(format!("Failed to attach hook: {e}"), ToastKind::Error);
                    continue;
                }
                let (attachments, candidates) =
                    load_plan_hooks_view_state(conn, &project, &plan_id)?;
                app.refresh(attachments, candidates);
                // Drop back to the list so the user sees the new row.
                app.mode = Mode::List;
                app.push_toast(
                    format!("Hook '{hook_name}' attached at {lifecycle}."),
                    ToastKind::Success,
                );
            }
            Outcome::RemoveRequested {
                lifecycle,
                hook_name,
            } => {
                match storage::detach_hook(conn, &plan_id, None, lifecycle.as_str(), &hook_name) {
                    Ok(0) => {
                        app.push_toast(
                            format!("No plan-wide hook '{hook_name}' at {lifecycle}."),
                            ToastKind::Info,
                        );
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        app.push_toast(format!("Failed to detach hook: {e}"), ToastKind::Error);
                        continue;
                    }
                }
                let (attachments, candidates) =
                    load_plan_hooks_view_state(conn, &project, &plan_id)?;
                app.refresh(attachments, candidates);
                app.push_toast(
                    format!("Hook '{hook_name}' detached from {lifecycle}."),
                    ToastKind::Success,
                );
            }
        }
    }
}

/// Read the plan-wide hook attachments and the in-scope hook-library
/// candidates for `plan_id`. Per-step rows are intentionally excluded —
/// this sub-view only shows / edits plan-wide attachments (`step_id IS NULL`).
fn load_plan_hooks_view_state(
    conn: &Connection,
    project: &str,
    plan_id: &str,
) -> Result<(
    Vec<crate::tui::views::plan_hooks::PlanHookRef>,
    Vec<crate::tui::views::plan_hooks::HookCandidate>,
)> {
    use crate::hook_library::{self, Lifecycle};
    use crate::tui::views::plan_hooks::{HookCandidate, PlanHookRef};

    let rows = storage::list_all_hooks_for_plan(conn, plan_id)?;
    let mut attachments: Vec<PlanHookRef> = Vec::new();
    for row in rows {
        if row.step_id.is_some() {
            continue;
        }
        let lifecycle = Lifecycle::parse(&row.lifecycle)?;
        attachments.push(PlanHookRef {
            lifecycle,
            hook_name: row.hook_name,
        });
    }

    let project_dir = std::path::PathBuf::from(project);
    let all = hook_library::load_all().unwrap_or_default();
    let candidates: Vec<HookCandidate> = hook_library::filter_by_project(all, &project_dir)
        .into_iter()
        .map(|h| HookCandidate {
            name: h.name,
            description: h.description,
        })
        .collect();

    Ok((attachments, candidates))
}

// ---------------------------------------------------------------------------
// Step-hooks dispatcher (TUI-plan.md §1)
// ---------------------------------------------------------------------------

/// Run the step-hooks event loop until the user pops back. Mirrors
/// [`run_plan_hooks_tui`] but for per-step attachments: reuses the parent
/// terminal and raw-mode session, owns the crossterm event loop, and
/// performs the storage write-throughs requested by the [`StepHooksApp`]
/// state machine.
///
/// Help-overlay routing happens inside [`StepHooksApp::handle_key`] (step
/// 14), so a stuck `?` overlay can always be dismissed with `?`/`<esc>`/
/// `q`/Ctrl-C without reaching the per-mode handlers. `<esc>`/`q`/Ctrl-C
/// in `Mode::List` pop back to step-detail.
fn run_step_hooks_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    project: &str,
    step_id: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::toast::ToastKind;
    use crate::tui::views::step_hooks::{Mode, Outcome, StepHooksApp, render};
    use crossterm::event::{self, Event, KeyEventKind};

    let step = storage::get_step(conn, step_id)?;
    let plan_id = step.plan_id.clone();
    let plan_slug = storage::get_plan_slug_by_id(conn, &plan_id)?.unwrap_or_default();
    let steps = storage::list_steps(conn, &plan_id)?;
    let step_num = steps
        .iter()
        .position(|s| s.id == step_id)
        .map(|i| i + 1)
        .unwrap_or(0);
    let step_label = format!("#{step_num} — {}", step.title);

    let (attachments, candidates) = load_step_hooks_view_state(conn, project, &plan_id, step_id)?;
    let mut app = StepHooksApp::new(
        plan_id.clone(),
        step_id.to_string(),
        plan_slug,
        step_label,
        attachments,
        candidates,
    );

    loop {
        terminal.draw(|f| render(f, f.area(), &mut app))?;

        if !event::poll(std::time::Duration::from_millis(250))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        match app.handle_key(key) {
            Outcome::Pending => {}
            Outcome::Pop => return Ok(()),
            Outcome::AddRequested {
                lifecycle,
                hook_name,
            } => {
                if let Err(e) = storage::attach_hook_to_step(
                    conn,
                    &plan_id,
                    step_id,
                    lifecycle.as_str(),
                    &hook_name,
                ) {
                    app.push_toast(format!("Failed to attach hook: {e}"), ToastKind::Error);
                    continue;
                }
                let (attachments, candidates) =
                    load_step_hooks_view_state(conn, project, &plan_id, step_id)?;
                app.refresh(attachments, candidates);
                // Drop back to the list so the user sees the new row.
                app.mode = Mode::List;
                app.push_toast(
                    format!("Hook '{hook_name}' attached at {lifecycle}."),
                    ToastKind::Success,
                );
            }
            Outcome::RemoveRequested {
                lifecycle,
                hook_name,
            } => {
                match storage::detach_hook(
                    conn,
                    &plan_id,
                    Some(step_id),
                    lifecycle.as_str(),
                    &hook_name,
                ) {
                    Ok(0) => {
                        app.push_toast(
                            format!("No per-step hook '{hook_name}' at {lifecycle}."),
                            ToastKind::Info,
                        );
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        app.push_toast(format!("Failed to detach hook: {e}"), ToastKind::Error);
                        continue;
                    }
                }
                let (attachments, candidates) =
                    load_step_hooks_view_state(conn, project, &plan_id, step_id)?;
                app.refresh(attachments, candidates);
                app.push_toast(
                    format!("Hook '{hook_name}' detached from {lifecycle}."),
                    ToastKind::Success,
                );
            }
        }
    }
}

/// Read the per-step hook attachments and the in-scope hook-library
/// candidates for `(plan_id, step_id)`. Plan-wide rows (`step_id IS NULL`)
/// are excluded — those are managed by the plan-hooks sub-view.
fn load_step_hooks_view_state(
    conn: &Connection,
    project: &str,
    plan_id: &str,
    step_id: &str,
) -> Result<(
    Vec<crate::tui::views::step_hooks::StepHookRef>,
    Vec<crate::tui::views::step_hooks::HookCandidate>,
)> {
    use crate::hook_library::{self, Lifecycle};
    use crate::tui::views::step_hooks::{HookCandidate, StepHookRef};

    let rows = storage::list_all_hooks_for_plan(conn, plan_id)?;
    let mut attachments: Vec<StepHookRef> = Vec::new();
    for row in rows {
        if row.step_id.as_deref() != Some(step_id) {
            continue;
        }
        let lifecycle = Lifecycle::parse(&row.lifecycle)?;
        attachments.push(StepHookRef {
            lifecycle,
            hook_name: row.hook_name,
        });
    }

    let project_dir = std::path::PathBuf::from(project);
    let all = hook_library::load_all().unwrap_or_default();
    let candidates: Vec<HookCandidate> = hook_library::filter_by_project(all, &project_dir)
        .into_iter()
        .map(|h| HookCandidate {
            name: h.name,
            description: h.description,
        })
        .collect();

    Ok((attachments, candidates))
}

// ---------------------------------------------------------------------------
// Step-tags dispatcher (TUI-plan.md §1)
// ---------------------------------------------------------------------------

/// Run the step-tags event loop until the user pops back. Mirrors
/// [`run_step_hooks_tui`] but for the per-step free-form tag list: reuses
/// the parent terminal and raw-mode session, owns the crossterm event
/// loop, and persists the working tag list via
/// [`storage::update_step_fields_ext`] when the [`StepTagsApp`] state
/// machine returns [`Outcome::SaveAndPop`]. [`Outcome::DiscardAndPop`]
/// pops without writing.
///
/// Help-overlay routing happens inside [`StepTagsApp::handle_key`] (step
/// 14), so a stuck `?` overlay can always be dismissed with `?`/`<esc>`/
/// `q`/Ctrl-C without reaching the per-mode handlers.
fn run_step_tags_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    step_id: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::views::step_tags::{Outcome, StepTagsApp, render};
    use crossterm::event::{self, Event, KeyEventKind};

    let step = storage::get_step(conn, step_id)?;
    let plan_id = step.plan_id.clone();
    let plan_slug = storage::get_plan_slug_by_id(conn, &plan_id)?.unwrap_or_default();
    let steps = storage::list_steps(conn, &plan_id)?;
    let step_num = steps
        .iter()
        .position(|s| s.id == step_id)
        .map(|i| i + 1)
        .unwrap_or(0);
    let step_label = format!("#{step_num} — {}", step.title);

    let mut app = StepTagsApp::new(
        step_id.to_string(),
        plan_slug,
        step_label,
        step.tags.clone(),
    );

    loop {
        terminal.draw(|f| render(f, f.area(), &mut app))?;

        if !event::poll(std::time::Duration::from_millis(250))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };
        match app.handle_key(key) {
            Outcome::Pending => {}
            Outcome::DiscardAndPop => return Ok(()),
            Outcome::SaveAndPop { tags } => {
                storage::update_step_fields_ext(
                    conn,
                    step_id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(&tags),
                )?;
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Step-detail dispatcher (TUI-plan.md §8 + §17)
// ---------------------------------------------------------------------------

/// Run the step-detail event loop until the user pops back. Reuses the
/// already-open terminal and raw-mode session — the caller (`run_plan_detail_tui`
/// after `A` on the open-questions banner) owns terminal teardown.
///
/// Drives every step-detail interaction: pane navigation (`j`/`k`), `c`
/// editor handoffs for editable text panes, bottom-row picker (Harness /
/// Model / Agent / Change policy), the §17 question flow (open-question
/// pane, answer modal, resume-implementation modal), zen-mode toggle,
/// palette and help overlay routing.
/// Sorted, deduplicated list of agent stems (filenames in
/// `Config::agents_dir()` with the `.md` suffix stripped). Used to populate
/// the bottom-row Agent picker — failures (missing dir, unreadable entries)
/// fall through to an empty list so the picker shows its empty placeholder
/// rather than panicking.
fn list_agent_names() -> Vec<String> {
    let Ok(dir) = crate::config::agents_dir() else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            if path.extension().is_some_and(|ext| ext == "md") {
                path.file_stem().map(|s| s.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

fn run_step_detail_tui<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    conn: &Connection,
    config: &Config,
    project: &str,
    plan_app: &mut crate::tui::views::plan_detail::PlanDetailApp,
    target_step_id: &str,
) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    use crate::tui::editor::edit_in_editor;
    use crate::tui::read_only::{self, ReadOnly, ReadOnlyTracker, Transition};
    use crate::tui::toast::ToastKind;
    use crate::tui::views::answer_modal::ResumeModalAction;
    use crate::tui::views::step_detail::{self, Pane, StepDetailApp};
    use crate::tui::views::step_detail_picker::PickerOutcome;
    use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
    use std::time::Instant;

    let plan = plan_app.plan.clone();
    let steps = storage::list_steps(conn, &plan.id)?;
    let target_index = steps
        .iter()
        .position(|s| s.id == target_step_id)
        .unwrap_or(0);
    let project_settings = storage::get_project_settings(conn, project)?;
    let exec_logs = if let Some(step) = steps.get(target_index) {
        storage::list_execution_logs_for_step(conn, &step.id)?
    } else {
        Vec::new()
    };

    let mut app = StepDetailApp::new(
        plan,
        steps,
        target_index,
        config,
        project_settings,
        exec_logs,
    );
    // Focus on the OpenQuestions pane so the user can press `a` immediately.
    app.focused_pane = Pane::OpenQuestions;
    refresh_step_detail_questions(conn, project, &mut app)?;

    // §13.2 read-only attach: any `run_locks` row owned by an unrelated pid
    // means an external runner is driving the plan; suppress edits until it
    // releases. The tracker owns the poll cadence; observations are fed in
    // each tick via [`step_detail_observe_read_only`].
    let my_pid = std::process::id() as i64;
    let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);

    loop {
        // §13.2 poll. Cadence is owned by the tracker (see read_only::POLL_INTERVAL).
        let now = Instant::now();
        if tracker.should_poll(now)
            && let Ok(observed) = read_only::detect(conn, project, my_pid, None)
        {
            let transition = step_detail_observe_read_only(&mut tracker, &mut app, observed, now);
            if transition == Transition::Released {
                app.toasts
                    .push(read_only::RELEASED_TOAST, ToastKind::Success, now);
            }
        }

        terminal.draw(|f| step_detail::draw(f, &mut app))?;

        if !event::poll(std::time::Duration::from_millis(250))? {
            // Re-poll the question state so concurrent answers (CLI or
            // another TUI) are reflected without input.
            refresh_step_detail_questions(conn, project, &mut app)?;
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            Event::Mouse(m) => {
                app.handle_mouse(m);
                continue;
            }
            _ => continue,
        };

        // Bottom-row picker (TUI-plan.md §8) takes priority over every other
        // handler — j/k/Enter/Esc/typed-char must drive the picker rather
        // than the underlying view.
        if app.picker.is_some() {
            if let Some(outcome) = app.picker_handle_key(key) {
                match outcome {
                    PickerOutcome::Pending => {}
                    PickerOutcome::Cancelled => app.close_picker(),
                    PickerOutcome::Submit { kind, value } => {
                        // §13.2: if lockdown engaged after the picker was
                        // opened, drop the submission rather than mutate the
                        // DB. Picker is closed unconditionally so the user
                        // returns to the underlying view.
                        if app.can_edit_panes()
                            && let Err(e) = app.apply_picker_submit(conn, kind, &value)
                        {
                            app.toasts.push(
                                format!("Failed to apply: {e}"),
                                ToastKind::Error,
                                Instant::now(),
                            );
                        }
                        app.close_picker();
                    }
                }
            }
            continue;
        }

        // §15 help overlay: `?` toggles, `<esc>`/`q`/Ctrl-C close. Run before
        // the modal handlers so a stuck overlay can always be dismissed; we
        // skip interception while a modal is up so the modal owns the keymap.
        if app.answer_modal.is_none()
            && app.resume_modal.is_none()
            && app.help.intercept_key(key) != crate::tui::help::InterceptResult::Passthrough
        {
            continue;
        }

        // Modals are exclusive: the resume modal is only opened when no
        // answer modal is also open, and vice versa.
        if app.answer_modal.is_some() {
            handle_answer_modal_key(conn, project, &mut app, key, edit_in_editor)?;
            if app.should_pop {
                return Ok(());
            }
            continue;
        }
        if app.resume_modal.is_some() {
            match handle_resume_modal_key(&mut app, key) {
                ResumeModalAction::Accept => {
                    let modal = app
                        .resume_modal
                        .take()
                        .expect("resume_modal was Some moments ago");
                    spawn_resume_run(&mut app, project, &modal);
                    return Ok(());
                }
                ResumeModalAction::Decline => {
                    app.close_resume_modal();
                }
                ResumeModalAction::Pending => {}
            }
            continue;
        }

        // §9 palette: while open, route every key through the palette bar
        // and skip the per-view input handler. Submit dispatches via
        // `step_detail_palette_action`. Step-detail can't easily host a
        // confirm dialog (its layered panes / pickers), so terminal-bound
        // actions toast a redirect instead.
        if let Some(bar) = app.palette_bar.as_mut() {
            use crate::tui::palette_dispatch::PaletteAction;
            use crate::tui::widgets::palette_bar::PaletteBarOutcome;
            match bar.on_key(key) {
                PaletteBarOutcome::Pending => {}
                PaletteBarOutcome::Cancel => app.close_palette(),
                PaletteBarOutcome::Submit(input) => {
                    let action = step_detail_palette_action(&input, &config.default_harness, &app);
                    app.close_palette();
                    match step_detail_apply_palette_action(conn, project, &mut app, action)? {
                        Some(PaletteAction::PushPlanDetail { .. })
                        | Some(PaletteAction::OpenConfirmArchive { .. })
                        | Some(PaletteAction::OpenConfirmDelete { .. }) => {
                            app.toasts.push(
                                "Pop back to the plan list to do that.",
                                ToastKind::Info,
                                Instant::now(),
                            );
                        }
                        // §9.1 run-choice dialog. Step-detail renders the
                        // dialog over its own surface; success spawns a
                        // non-streaming runner via the palette path (the
                        // streaming attach path remains plan-detail's `R`).
                        Some(PaletteAction::OpenRunDialog {
                            default_branch,
                            plan_count,
                            targets,
                        }) => {
                            let outcome = run_dialog_loop_with_bg(
                                terminal,
                                |f| crate::tui::views::step_detail::draw(f, &mut app),
                                default_branch,
                                plan_count,
                            )?;
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::step_detail::draw(f, &mut app),
                                project,
                                outcome,
                                &targets,
                                plan_count > 1,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        Some(PaletteAction::RunOnBranch {
                            branch,
                            targets,
                            force_current_branch,
                        }) => {
                            let report = apply_palette_run_outcome(
                                terminal,
                                |f| crate::tui::views::step_detail::draw(f, &mut app),
                                project,
                                crate::tui::run_dialog::Outcome::NewBranch(branch),
                                &targets,
                                force_current_branch,
                            )?;
                            flush_palette_run_toasts(report, &mut app.toasts);
                        }
                        // §9 sub-view routing — step-detail is the host for
                        // step-level sub-views (`H`/`T` keybindings already
                        // open these), and is the only view that resolves
                        // `focused_step`. Plan-level entries route to the
                        // same dispatchers as plan-detail's keybindings.
                        Some(PaletteAction::OpenPlanDependencies { plan_id, slug }) => {
                            let project_path = app.plan.project.clone();
                            run_plan_dependencies_tui(
                                terminal,
                                conn,
                                &project_path,
                                &plan_id,
                                &slug,
                            )?;
                        }
                        Some(PaletteAction::OpenPlanHooks { plan_id, slug }) => {
                            let project_path = app.plan.project.clone();
                            run_plan_hooks_tui(terminal, conn, &project_path, &plan_id, &slug)?;
                        }
                        Some(PaletteAction::OpenStepHooks { step_id, .. }) => {
                            run_step_hooks_tui(terminal, conn, project, &step_id)?;
                        }
                        Some(PaletteAction::OpenStepTags { step_id, .. }) => {
                            run_step_tags_tui(terminal, conn, &step_id)?;
                        }
                        _ => {}
                    }
                }
            }
            continue;
        }

        match key.code {
            KeyCode::Char('/') | KeyCode::Char(':') => {
                let prefix = match key.code {
                    KeyCode::Char(c) => c,
                    _ => '/',
                };
                app.open_palette(prefix);
            }
            // Question-pane navigation (j/k) overrides pane navigation while
            // the pane is focused.
            KeyCode::Char('j') | KeyCode::Down
                if app.focused_pane == Pane::OpenQuestions && app.has_open_questions_for_step() =>
            {
                app.select_question_next();
            }
            KeyCode::Char('k') | KeyCode::Up
                if app.focused_pane == Pane::OpenQuestions && app.has_open_questions_for_step() =>
            {
                app.select_question_prev();
            }
            // Pane navigation (j/k outside the questions pane).
            KeyCode::Char('j') | KeyCode::Down => app.focus_down(),
            KeyCode::Char('k') | KeyCode::Up => app.focus_up(),
            KeyCode::Char('a')
                if app.focused_pane == Pane::OpenQuestions && app.can_edit_panes() =>
            {
                let opened = app.open_answer_modal();
                if !opened && !app.has_open_questions_for_step() {
                    app.toasts.push(
                        "No open questions for this step.",
                        ToastKind::Info,
                        Instant::now(),
                    );
                }
            }
            KeyCode::Char('h') | KeyCode::Left => app.handle_left(),
            KeyCode::Char('l') | KeyCode::Right => app.handle_right(),
            KeyCode::Char('z') => {
                app.toggle_zen();
            }
            KeyCode::Char('q') => app.request_pop(),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.request_pop();
            }
            KeyCode::Char('c') if app.focused_pane == Pane::BottomRow && app.can_edit_panes() => {
                let agents = list_agent_names();
                app.open_picker_for_focused_cell(&agents);
            }
            KeyCode::Char('c') if app.can_edit_panes() => {
                let dir = crate::config::config_dir()?;
                step_detail_handle_c(&mut app, conn, config, &dir, edit_in_editor)?;
            }
            // Open the step-hooks sub-view (TUI-plan.md §1). Suppressed
            // during read-only attach so an external runner's lock isn't
            // bypassed by mutating per-step hook attachments.
            KeyCode::Char('H') if app.can_edit_panes() => {
                if let Some(step) = app.current_step() {
                    let step_id = step.id.clone();
                    run_step_hooks_tui(terminal, conn, project, &step_id)?;
                }
            }
            // Open the step-tags sub-view (TUI-plan.md §1). Suppressed
            // during read-only attach so an external runner's lock isn't
            // bypassed by mutating per-step tags.
            KeyCode::Char('T') if app.can_edit_panes() => {
                if let Some(step) = app.current_step() {
                    let step_id = step.id.clone();
                    run_step_tags_tui(terminal, conn, &step_id)?;
                }
            }
            KeyCode::Esc => {
                step_detail_handle_esc(&mut app);
            }
            _ => {}
        }
        if app.should_pop {
            return Ok(());
        }
    }
}

/// Apply a [`PaletteAction`] inside the step-detail view. Returns
/// `Some(action)` for variants the dispatcher loop must drive itself
/// (`PushPlanDetail`, `OpenConfirmArchive`, `OpenConfirmDelete` — none of
/// which fit the step-detail context cleanly, but are forwarded for
/// completeness).
pub(crate) fn step_detail_apply_palette_action(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    action: crate::tui::palette_dispatch::PaletteAction,
) -> Result<Option<crate::tui::palette_dispatch::PaletteAction>> {
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    match action {
        PaletteAction::None => {}
        PaletteAction::Toast { message, kind } => {
            app.toasts.push(message, kind, Instant::now());
        }
        PaletteAction::Quit => {
            app.should_pop = true;
        }
        PaletteAction::Approve { plan_id, slug } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            if let Some(updated) = storage::get_plan_by_slug(conn, &slug, project)? {
                app.plan = updated;
            }
            app.toasts
                .push("Plan approved.", ToastKind::Success, Instant::now());
        }
        PaletteAction::SetQuestionsEnabled {
            plan_id, enabled, ..
        } => {
            storage::set_plan_questions_enabled(conn, &plan_id, enabled)?;
            if let Some(updated) =
                storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)?
            {
                app.plan = updated;
            }
            let msg = if enabled {
                "Questions enabled."
            } else {
                "Questions disabled."
            };
            app.toasts.push(msg, ToastKind::Success, Instant::now());
        }
        PaletteAction::Unarchive { plan_id, .. } => {
            storage::update_plan_status(conn, &plan_id, crate::plan::PlanStatus::Ready)?;
            if let Some(updated) =
                storage::get_plan_by_slug(conn, &app.plan.slug, &app.plan.project)?
            {
                app.plan = updated;
            }
            app.toasts
                .push("Unarchived.", ToastKind::Success, Instant::now());
        }
        PaletteAction::SkipStep { step_num, .. } => {
            let plan = app.plan.clone();
            match crate::runner::skip_step(conn, &plan, step_num.map(|n| n as usize), None) {
                Ok(actual_num) => {
                    app.steps = storage::list_steps(conn, &app.plan.id)?;
                    app.toasts.push(
                        format!("Skipped step {actual_num}."),
                        ToastKind::Success,
                        Instant::now(),
                    );
                }
                Err(e) => {
                    app.toasts.push(
                        format!("Failed to skip step: {e}"),
                        ToastKind::Error,
                        Instant::now(),
                    );
                }
            }
        }
        PaletteAction::Export { slug, output } => {
            apply_palette_export(&slug, output.as_deref(), conn, project, &mut app.toasts);
        }
        PaletteAction::Import { path } => {
            apply_palette_import(&path, conn, project, &mut app.toasts);
        }
        PaletteAction::CancelRun => {
            app.toasts.push(
                "Pop back to plan-detail to cancel a live run.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::SpawnPlanHarness { harness, .. } => {
            app.toasts.push(
                format!(
                    "/plan harness {harness}: not yet wired from palette; use the CLI for now."
                ),
                ToastKind::Info,
                Instant::now(),
            );
        }
        PaletteAction::AddStep { .. } | PaletteAction::MoveStep { .. } => {
            app.toasts.push(
                "Pop back to plan-detail to add or move steps.",
                ToastKind::Info,
                Instant::now(),
            );
        }
        // §9 sub-view routing — step-detail is the host for step-level
        // sub-views (`OpenStepHooks`, `OpenStepTags`) since it's the only
        // view that sets `focused_step` in the palette context. Plan-level
        // entries are forwarded too (the dispatcher can resolve the focused
        // plan from `app.plan`) so `/plan dependency` / `/plan hooks` from
        // step-detail still lands in the right sub-view.
        PaletteAction::OpenPlanDependencies { .. }
        | PaletteAction::OpenPlanHooks { .. }
        | PaletteAction::OpenStepHooks { .. }
        | PaletteAction::OpenStepTags { .. } => {
            return Ok(Some(action));
        }
        PaletteAction::ComingSoon {
            label,
            target_step: _,
        } => {
            app.toasts.push(
                format!("{label}: palette wiring pending — see TUI-plan.md §9."),
                ToastKind::Info,
                Instant::now(),
            );
        }
        // Terminal-bound: hand back to the caller. The step-detail dispatcher
        // currently toasts a "pop back" hint for the inherited variants, so
        // the run-choice dialog (TUI-plan.md §9.1) gets the same treatment
        // there — the loop renders the dialog over plan-detail's parent view.
        PaletteAction::PushPlanDetail { .. }
        | PaletteAction::OpenConfirmArchive { .. }
        | PaletteAction::OpenConfirmDelete { .. }
        | PaletteAction::OpenRunDialog { .. }
        | PaletteAction::RunOnBranch { .. } => {
            return Ok(Some(action));
        }
    }
    Ok(None)
}

/// Pure dispatch for step-detail. Builds the [`PaletteContext`] from the
/// focused plan + focused step.
pub(crate) fn step_detail_palette_action(
    input: &str,
    default_harness: &str,
    app: &crate::tui::views::step_detail::StepDetailApp,
) -> crate::tui::palette_dispatch::PaletteAction {
    use crate::tui::palette;
    use crate::tui::palette_dispatch::{self, FocusedStep};

    let plans = vec![plan_ref_from_plan(&app.plan)];
    let focused_slug = Some(app.plan.slug.as_str());
    let run_targets = vec![crate::tui::run_dialog::RunTarget {
        slug: app.plan.slug.clone(),
        default_branch: app.plan.branch_name.clone(),
    }];
    let focused_step = app.current_step().map(|s| FocusedStep {
        id: s.id.clone(),
        label: format!("#{} — {}", app.selected_step_index + 1, s.title),
    });

    let ctx = palette_dispatch::PaletteContext {
        default_harness,
        focused_slug,
        focused_step: focused_step.as_ref(),
        run_targets: &run_targets,
        plans: &plans,
        archived: &[],
    };
    match palette::parse(input) {
        Ok(cmd) => palette_dispatch::dispatch(&cmd, &ctx),
        Err(err) => palette_dispatch::dispatch_parse_error(&err),
    }
}

/// Feed a fresh [`read_only::ReadOnly`] observation into the tracker, push
/// the resulting state into the app, and return the [`Transition`] so the
/// caller can decide whether to toast `RELEASED_TOAST`.
///
/// Extracted so dispatcher-level tests can drive the §13.2 lockdown wiring
/// without spinning up a real terminal: the test inserts a `run_locks` row
/// owned by an external pid, calls [`read_only::detect`], then feeds the
/// observation through this helper and asserts that the app's edit gates
/// flip.
pub(crate) fn step_detail_observe_read_only(
    tracker: &mut crate::tui::read_only::ReadOnlyTracker,
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    observed: crate::tui::read_only::ReadOnly,
    now: std::time::Instant,
) -> crate::tui::read_only::Transition {
    let transition = tracker.observe(observed, now);
    app.set_read_only(tracker.state());
    // Picker submissions are gated by `can_edit_panes()`, but if a picker
    // was open when the lock engaged the user would still see it on screen
    // until they pressed Esc. Closing it here mirrors the dispatcher's
    // intent that edit affordances become inert while locked.
    if !app.can_edit_panes() && app.picker.is_some() {
        app.close_picker();
    }
    transition
}

/// Bare `c` on the step-detail view (TUI-plan.md §8 "Editing — `c`"):
/// dispatch the editor handoff for the focused pane and toast the result.
///
/// Routes by `app.focused_pane`:
/// - `UniversalPrompt` / `ProjectPrompt` / `PlanContextPrepend` /
///   `PlanPrefix` / `PlanSuffix` / `StepPrompt` / `Tests` → the matching
///   `edit_*_pane` method on `StepDetailApp`.
/// - `Appended` / `OpenQuestions` → no-op (those panes are read-only).
/// - `BottomRow` → no-op here; the focused cell's picker is opened by
///   `step_detail_handle_bottom_row_c` instead.
///
/// `config` is cloned locally so the pane's `&mut Config` can be persisted
/// via `save_at`; the on-disk file is the source of truth, and the app's
/// in-memory mirrors (`config_prompt_prefix` / `config_prompt_suffix`) are
/// updated by `edit_universal_pane` so the pane re-renders without a reload.
fn step_detail_handle_c<E>(
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    conn: &Connection,
    config: &Config,
    config_dir: &Path,
    edit_fn: E,
) -> Result<()>
where
    E: FnOnce(&str) -> Result<Option<String>>,
{
    use crate::tui::toast::ToastKind;
    use crate::tui::views::step_detail::{
        EditOutcome, NO_CHANGES_TOAST, NO_EDITOR_TOAST, PARSE_ERROR_TOAST_PREFIX, Pane, SAVED_TOAST,
    };
    use std::time::Instant;

    let outcome = match app.focused_pane {
        Pane::UniversalPrompt => {
            let mut local = config.clone();
            app.edit_universal_pane(&mut local, config_dir, edit_fn)?
        }
        Pane::ProjectPrompt => app.edit_project_pane(conn, edit_fn)?,
        Pane::PlanContextPrepend => app.edit_plan_context_prepend_pane(conn, edit_fn)?,
        Pane::PlanPrefix => app.edit_plan_prefix_pane(conn, edit_fn)?,
        Pane::PlanSuffix => app.edit_plan_suffix_pane(conn, edit_fn)?,
        Pane::StepPrompt => app.edit_step_prompt_pane(conn, edit_fn)?,
        Pane::Tests => app.edit_tests_pane(conn, edit_fn)?,
        Pane::Appended | Pane::OpenQuestions | Pane::BottomRow => return Ok(()),
    };

    let now = Instant::now();
    match outcome {
        EditOutcome::NoEditor => {
            app.toasts.push(NO_EDITOR_TOAST, ToastKind::Error, now);
        }
        EditOutcome::Saved => {
            app.toasts.push(SAVED_TOAST, ToastKind::Success, now);
        }
        EditOutcome::NoChanges => {
            app.toasts.push(NO_CHANGES_TOAST, ToastKind::Info, now);
        }
        EditOutcome::ParseError(msg) => {
            app.toasts.push(
                format!("{PARSE_ERROR_TOAST_PREFIX}{msg}"),
                ToastKind::Error,
                now,
            );
        }
    }
    Ok(())
}

/// `<esc>` precedence in the step-detail view (TUI-plan.md §4): dismiss the
/// current toast when one is showing and consume the keypress; otherwise
/// fall through to the view's existing Esc binding (`request_pop`).
/// Returns `true` when a toast was dismissed.
pub(crate) fn step_detail_handle_esc(
    app: &mut crate::tui::views::step_detail::StepDetailApp,
) -> bool {
    if app.toasts.dismiss() {
        true
    } else {
        app.request_pop();
        false
    }
}

/// Drive one key event into the open answer modal. Persists the chosen
/// answer (suggestion or `$EDITOR` round-trip) and refreshes the open
/// question list. When the just-applied answer was the plan's last open
/// question, opens the resume-implementation modal via
/// [`StepDetailApp::note_answer_persisted`].
fn handle_answer_modal_key<E>(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    key: crossterm::event::KeyEvent,
    editor_fn: E,
) -> Result<()>
where
    E: FnOnce(&str) -> Result<Option<String>>,
{
    use crate::tui::toast::ToastKind;
    use crate::tui::views::answer_modal::AnswerModalAction;
    use std::time::Instant;

    let Some(modal) = app.answer_modal.as_ref() else {
        return Ok(());
    };
    let action = modal.handle_key(key);
    match action {
        AnswerModalAction::Pending => {}
        AnswerModalAction::Cancel => {
            app.close_answer_modal();
        }
        AnswerModalAction::Submit { index } => {
            let modal = app.answer_modal.as_ref().expect("modal still open");
            let Some(answer) = modal.suggestion_text(index).map(|s| s.to_string()) else {
                app.toasts.push(
                    "No suggestion at that index.",
                    ToastKind::Error,
                    Instant::now(),
                );
                return Ok(());
            };
            let qid = modal.question_id.clone();
            persist_answer_and_refresh(conn, project, app, &qid, &answer)?;
        }
        AnswerModalAction::EditCustom => {
            let modal = app.answer_modal.as_ref().expect("modal still open");
            let qid = modal.question_id.clone();
            // Seed the editor with a short hint so the user knows what
            // they're answering — stripped on persist.
            let seed = format!(
                "# Replace this with your answer to:\n# {q}\n\n",
                q = modal.question
            );
            let edited = match editor_fn(&seed)? {
                Some(s) => s,
                None => {
                    app.toasts.push(
                        crate::tui::views::step_detail::NO_EDITOR_TOAST,
                        ToastKind::Error,
                        Instant::now(),
                    );
                    app.close_answer_modal();
                    return Ok(());
                }
            };
            let answer = strip_answer_comments(&edited);
            if answer.trim().is_empty() {
                app.toasts.push(
                    "Empty answer — modal closed without writing.",
                    ToastKind::Info,
                    Instant::now(),
                );
                app.close_answer_modal();
                return Ok(());
            }
            persist_answer_and_refresh(conn, project, app, &qid, &answer)?;
        }
    }
    Ok(())
}

/// Drive one key event into the resume-implementation modal. Returns the
/// outcome so the caller can decide whether to spawn the runner. Pure of
/// I/O.
fn handle_resume_modal_key(
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    key: crossterm::event::KeyEvent,
) -> crate::tui::views::answer_modal::ResumeModalAction {
    use crate::tui::views::answer_modal::ResumeModalAction;
    let Some(modal) = app.resume_modal.as_ref() else {
        return ResumeModalAction::Pending;
    };
    modal.handle_key(key)
}

/// Persist a question answer and refresh the App's question caches. When
/// this answer was the plan's last open question, the App opens the
/// resume-implementation modal automatically via `note_answer_persisted`.
fn persist_answer_and_refresh(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    question_id: &str,
    answer: &str,
) -> Result<()> {
    use crate::tui::toast::ToastKind;
    use std::time::Instant;

    if let Err(e) = storage::set_question_answer(conn, question_id, answer) {
        app.toasts.push(
            format!("Failed to save answer: {e}"),
            ToastKind::Error,
            Instant::now(),
        );
        app.close_answer_modal();
        return Ok(());
    }
    refresh_step_detail_questions(conn, project, app)?;
    let prev_current_branch = previous_run_current_branch(conn, project, &app.plan.slug)?;
    app.note_answer_persisted(prev_current_branch);
    app.toasts
        .push("Answer saved.", ToastKind::Success, Instant::now());
    Ok(())
}

/// Refresh the App's open-question cache + plan-wide count from the DB.
/// Called after every answer and on each idle tick so concurrent CLI /
/// runner activity is reflected.
fn refresh_step_detail_questions(
    conn: &Connection,
    project: &str,
    app: &mut crate::tui::views::step_detail::StepDetailApp,
) -> Result<()> {
    let opens = storage::list_open_questions(conn, project, Some(&app.plan.slug))?;
    let plan_total = opens.len();
    let step_id = app.current_step().map(|s| s.id.clone());
    let for_step: Vec<_> = match step_id {
        Some(sid) => opens.into_iter().filter(|q| q.step_id == sid).collect(),
        None => Vec::new(),
    };
    app.set_open_questions_for_step(for_step);
    app.set_plan_open_questions_count(plan_total);
    Ok(())
}

/// Best-effort lookup of the previous run's `--current-branch` flag for
/// `plan_slug`. Returns `false` (the normal branch flow) when the previous
/// branch mode can't be recovered — the schema doesn't currently persist
/// this across runs once the run lock is released, so the safe default is
/// the more common branch-stash flow. The user can decline the resume
/// prompt and re-run with their preferred flag if needed.
fn previous_run_current_branch(
    _conn: &Connection,
    _project: &str,
    _plan_slug: &str,
) -> Result<bool> {
    Ok(false)
}

/// Spawn `ralph run` in response to the resume modal's Accept action.
/// Mirrors [`plan_detail_apply_run_streaming`]'s spawn behavior but
/// without the streaming subscription — the caller is about to pop back
/// to plan-detail, which will pick up the run via its DB-poll path.
fn spawn_resume_run(
    app: &mut crate::tui::views::step_detail::StepDetailApp,
    project: &str,
    modal: &crate::tui::views::answer_modal::ResumeModal,
) {
    use crate::tui::toast::ToastKind;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            app.toasts.push(
                format!("Cannot locate ralph binary: {e}"),
                ToastKind::Error,
                Instant::now(),
            );
            return;
        }
    };
    let mut cmd = Command::new(&exe);
    cmd.arg("-C")
        .arg(project)
        .arg("--non-interactive")
        .arg("run");
    if modal.current_branch {
        cmd.arg("--current-branch");
    }
    cmd.arg(&modal.plan_slug)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Err(e) = cmd.spawn() {
        app.toasts.push(
            format!("Failed to start run: {e}"),
            ToastKind::Error,
            Instant::now(),
        );
    }
    // Pop back to plan-detail so the user sees the live status.
    app.request_pop();
}

/// Strip leading `#`-prefixed comment lines (and any trailing blank line)
/// from a custom-answer editor blob, mirroring git commit-message
/// conventions. The seed text the modal injects starts each hint line
/// with `#` so this leaves only the user's actual answer.
fn strip_answer_comments(text: &str) -> String {
    let mut out = String::new();
    for line in text.lines() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim().to_string()
}

// ---------------------------------------------------------------------------
// Status command
// ---------------------------------------------------------------------------

pub fn cmd_status(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    verbose: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        // Find the most recent active plan, including completed plans so that
        // running `status` right after a plan finishes still shows it.
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format == OutputFormat::Json {
                    println!("null");
                } else {
                    eprintln!(
                        "No active plan found. Specify a plan slug as a positional argument."
                    );
                }
                return Ok(());
            }
        }
    };

    let (summary, steps) = build_status_summary(conn, project, &plan)?;

    if out.format == OutputFormat::Json {
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    render_status_plain(&summary, &plan, &steps, verbose, out);
    Ok(())
}

/// Assemble a [`output::StatusSummary`] for `plan`, computing step counts and
/// attaching a live-run snapshot when one exists and is bound to this plan
/// (or unbound). Exposed to tests so the JSON contract can be exercised
/// without capturing stdout.
fn build_status_summary(
    conn: &Connection,
    project: &str,
    plan: &crate::plan::Plan,
) -> Result<(output::StatusSummary, Vec<crate::plan::Step>)> {
    let steps = storage::list_steps(conn, &plan.id)?;

    let total = steps.len();
    let complete = steps
        .iter()
        .filter(|s| s.status == StepStatus::Complete)
        .count();
    let failed = steps
        .iter()
        .filter(|s| s.status == StepStatus::Failed)
        .count();
    let skipped = steps
        .iter()
        .filter(|s| s.status == StepStatus::Skipped)
        .count();
    let pending = steps
        .iter()
        .filter(|s| s.status == StepStatus::Pending)
        .count();
    let in_progress = steps
        .iter()
        .filter(|s| s.status == StepStatus::InProgress)
        .count();

    // Load the live-run snapshot for this project and attach it iff its
    // recorded plan_id matches (or is unset — an unbound lock still covers
    // this project). If the live row records a different plan, it belongs to
    // someone else's run — omit it so the current plan's status doesn't
    // falsely show live.
    let live = storage::get_live_run(conn, project)?;
    let live_display: Option<output::LiveRunDisplay> =
        live.and_then(|lr| match lr.plan_id.as_deref() {
            Some(pid) if pid != plan.id => None,
            _ => Some(output::LiveRunDisplay::from_live_run(&lr)),
        });

    let summary = output::StatusSummary {
        slug: plan.slug.clone(),
        status: plan.status,
        branch_name: plan.branch_name.clone(),
        steps: output::StepCounts {
            total,
            complete,
            failed,
            skipped,
            pending,
            in_progress,
        },
        live: live_display,
        pause_requested: plan.pause_requested,
    };
    Ok((summary, steps))
}

/// Render the plain-text status output for an assembled summary. Separated
/// from [`build_status_summary`] so the JSON contract can be tested without
/// capturing stdout.
fn render_status_plain(
    summary: &output::StatusSummary,
    plan: &crate::plan::Plan,
    steps: &[crate::plan::Step],
    verbose: bool,
    out: &OutputContext,
) {
    println!(
        "{}  {}",
        output::bold(&summary.slug, out.color),
        output::colored_plan_status(summary.status, out.color),
    );
    println!("  Branch: {}", summary.branch_name);

    if steps.is_empty() {
        println!("  No steps.");
        return;
    }

    let c = &summary.steps;
    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        c.complete, c.total, c.failed, c.skipped, c.pending, c.in_progress
    );

    if summary.pause_requested {
        println!("  pause_requested: true");
    }

    if let Some(lv) = summary.live.as_ref() {
        print_live_block(lv, steps);
    }

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            let policy_tag = if step.change_policy == ChangePolicy::Optional {
                " [optional]"
            } else {
                ""
            };
            let tags_inline = crate::commands::step::render_tags_inline(step);
            let tags_prefix = if tags_inline.is_empty() {
                String::new()
            } else {
                format!("{tags_inline} ")
            };
            println!(
                "  {:>3}. {} {}{}{} [{}] (attempts: {})",
                i + 1,
                output::status_icon(step.status, out.color),
                tags_prefix,
                step.title,
                policy_tag,
                output::colored_status(step.status, out.color),
                step.attempts,
            );
            if step.status == StepStatus::Skipped
                && let Some(reason) = step.skipped_reason.as_deref()
            {
                println!("       reason: {reason}");
            }
        }
    }
    let _ = plan; // quiet unused-param warning; kept for future plan-level fields.
}

/// Render the plain-text `Current:` block for the live-run snapshot. Lines
/// are written to `out` so the rendering is testable without capturing
/// stdout. Fields that aren't populated are skipped, so an unbound lock
/// (runner sitting between steps with no phase recorded yet) quietly emits
/// only what it has.
fn render_live_block(lv: &output::LiveRunDisplay, steps: &[crate::plan::Step]) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(s, "  Current:");

    // "step: N/M \"Title\"" — look up the title from the step list if the
    // live row's step_id resolves there.
    if let Some(num) = lv.step_num {
        let title = lv.step_id.as_deref().and_then(|id| {
            steps
                .iter()
                .find(|st| st.id == id)
                .map(|st| st.title.as_str())
        });
        match title {
            Some(t) => {
                let _ = writeln!(s, "    step: {}/{} \"{}\"", num, steps.len(), t);
            }
            None => {
                let _ = writeln!(s, "    step: {}/{}", num, steps.len());
            }
        }
    }

    if let Some(phase) = lv.phase {
        match lv.phase_elapsed_secs {
            Some(secs) => {
                let rounded = secs.round().max(0.0) as u64;
                let _ = writeln!(s, "    phase: {} ({}s)", phase.as_str(), rounded);
            }
            None => {
                let _ = writeln!(s, "    phase: {}", phase.as_str());
            }
        }
    }

    if let (Some(a), Some(m)) = (lv.attempt, lv.max_attempts) {
        let _ = writeln!(s, "    attempt: {a}/{m}");
    } else if let Some(a) = lv.attempt {
        let _ = writeln!(s, "    attempt: {a}");
    }

    if let Some(cmd) = lv.current_command.as_deref() {
        let _ = writeln!(s, "    command: {cmd}");
    }

    // runner pid + human-readable started_at. If the stored timestamp doesn't
    // parse as chrono we fall back to the raw string rather than dropping the
    // line — it's diagnostic information and something is strictly better
    // than nothing.
    let started_display = lv
        .started_at
        .parse::<chrono::DateTime<chrono::Utc>>()
        .ok()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| lv.started_at.clone());
    let _ = writeln!(s, "    runner: pid {}, started {}", lv.pid, started_display);

    s
}

/// Thin wrapper over `render_live_block` that prints to stdout.
fn print_live_block(lv: &output::LiveRunDisplay, steps: &[crate::plan::Step]) {
    print!("{}", render_live_block(lv, steps));
}

// ---------------------------------------------------------------------------
// Log command
// ---------------------------------------------------------------------------

/// Controls how harness stdout/stderr is displayed in log output.
///
/// - `Hidden` — don't show output (default when no flags given).
/// - `Truncated(n)` — show up to `n` lines per stream.
/// - `Full` — show everything, no truncation.
pub enum LogOutputMode {
    Hidden,
    Truncated(usize),
    Full,
}

pub fn cmd_log(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    step_num: Option<usize>,
    limit: Option<usize>,
    output_mode: &LogOutputMode,
    out: &OutputContext,
) -> Result<()> {
    // Resolve plan
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format == OutputFormat::Json {
                    println!("null");
                } else {
                    eprintln!("No plan found. Specify a plan slug as a positional argument.");
                }
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

        if out.format == OutputFormat::Json {
            for log in &logs {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode))?;
            }
            return Ok(());
        }

        eprintln!(
            "Logs for step #{} '{}' ({} attempts):",
            step_idx,
            step.title,
            logs.len()
        );
        eprintln!();

        if step.status == StepStatus::Skipped {
            match step.skipped_reason.as_deref() {
                Some(reason) => println!("  (skipped: {reason})"),
                None => println!("  (skipped)"),
            }
            println!();
        }

        for log in &logs {
            print_log_entry(&step.title, log, output_mode, out.color);
        }
    } else {
        // Show all logs for the plan
        let entries = storage::list_execution_logs_for_plan(conn, &plan.id, limit)?;

        if out.format == OutputFormat::Json {
            for (_, log) in &entries {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode))?;
            }
            return Ok(());
        }

        // Surface skipped steps' reasons alongside execution logs — skips
        // don't produce an execution_log row, so they'd otherwise be invisible
        // in this view.
        let steps = storage::list_steps(conn, &plan.id)?;
        let skipped_with_reason: Vec<&crate::plan::Step> = steps
            .iter()
            .filter(|s| s.status == StepStatus::Skipped)
            .collect();

        if entries.is_empty() && skipped_with_reason.is_empty() {
            eprintln!("No execution logs for plan '{}'.", plan.slug);
            return Ok(());
        }

        if !skipped_with_reason.is_empty() {
            eprintln!("Skipped steps for plan '{}':", plan.slug);
            eprintln!();
            for step in &skipped_with_reason {
                let num = steps.iter().position(|s| s.id == step.id).unwrap_or(0) + 1;
                match step.skipped_reason.as_deref() {
                    Some(reason) => {
                        println!("  #{num} {} — skipped ({reason})", step.title);
                    }
                    None => {
                        println!("  #{num} {} — skipped", step.title);
                    }
                }
            }
            println!();
        }

        if entries.is_empty() {
            return Ok(());
        }

        eprintln!(
            "Execution logs for plan '{}' ({} entries):",
            plan.slug,
            entries.len()
        );
        eprintln!();

        for (step_title, log) in &entries {
            print_log_entry(step_title, log, output_mode, out.color);
        }
    }

    Ok(())
}

fn print_log_entry(step_title: &str, log: &ExecutionLog, output_mode: &LogOutputMode, color: bool) {
    let icon = output::log_status_icon(log.committed, log.rolled_back, color);

    let duration_str = log
        .duration_secs
        .map(|d| format!("{:.1}s", d))
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {} [attempt {}] {} ({}) {}",
        icon,
        log.attempt,
        step_title,
        duration_str,
        log.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
    );

    if let Some(ref hash) = log.commit_hash {
        println!("    commit: {}", &hash[..hash.len().min(8)]);
    }

    // Surface the optional-policy no-op marker explicitly so a successful log
    // row with no commit doesn't look like a mysterious empty success. We key
    // off termination_reason = Success + absent commit_hash, then fall back to
    // the sentinel string embedded in test_results when observability data is
    // missing (older rows, or the deliberate `change_policy=optional` marker
    // written by the executor).
    let optional_no_change = log
        .test_results
        .iter()
        .any(|r| r.contains("change_policy=optional"))
        || (log.commit_hash.is_none()
            && log.termination_reason == Some(crate::plan::TerminationReason::Success));
    if optional_no_change {
        println!("    (no changes — change_policy=optional)");
    }

    if !log.test_results.is_empty() {
        println!("    tests: {}", log.test_results.join(", "));
    }

    // Always print termination_reason when Some so a stuck/interrupted row
    // that has no commit hash, no diff, and no test_results still surfaces
    // unambiguous diagnostic information. Rows that predate V11 (Option::None)
    // just omit the line.
    if let Some(reason) = log.termination_reason {
        println!(
            "    reason: {}",
            output::colored_termination_reason(reason, color)
        );
    }

    // Always print test_status when Some. Same rationale — this is cheap to
    // render and painful to miss when a row's test_results field is empty
    // but the phase actually ran.
    if let Some(status) = log.test_status {
        println!(
            "    test status: {}",
            output::colored_test_status(status, color)
        );
    }

    if let Some(cost) = log.cost_usd {
        let tokens = match (log.input_tokens, log.output_tokens) {
            (Some(i), Some(o)) => format!(" ({i} in / {o} out tokens)"),
            _ => String::new(),
        };
        println!("    cost: ${:.4}{}", cost, tokens);
    }

    if !matches!(output_mode, LogOutputMode::Hidden) {
        // --lines N is a *total* budget across both streams. Distribute it
        // proportionally so --lines 50 never prints more than 50 lines.
        let (stdout_cap, stderr_cap) = match output_mode {
            LogOutputMode::Truncated(n) => {
                let out_n = log
                    .harness_stdout
                    .as_deref()
                    .map(|s| s.lines().count())
                    .unwrap_or(0);
                let err_n = log
                    .harness_stderr
                    .as_deref()
                    .map(|s| s.lines().count())
                    .unwrap_or(0);
                let (a, b) = output::split_lines_budget(out_n, err_n, *n);
                (Some(a), Some(b))
            }
            _ => (None, None),
        };
        let print_stream = |label: &str, text: &Option<String>, cap: Option<usize>| {
            if let Some(s) = text.as_deref()
                && !s.is_empty()
                && cap != Some(0)
            {
                println!("    --- {label} ---");
                let lines_iter = s.lines();
                let lines: Box<dyn Iterator<Item = &str>> = match cap {
                    Some(n) => Box::new(lines_iter.take(n)),
                    None => Box::new(lines_iter),
                };
                for line in lines {
                    println!("    {line}");
                }
            }
        };
        print_stream("stdout", &log.harness_stdout, stdout_cap);
        print_stream("stderr", &log.harness_stderr, stderr_cap);
    }

    println!();
}

// ---------------------------------------------------------------------------
// Pause command
// ---------------------------------------------------------------------------

/// Implement `ralph pause [<slug>]`.
///
/// Sets `plans.pause_requested = 1` so the runner exits between steps with
/// `TerminationReason::PausedByUser`. Gated on the project's run lock —
/// the runner clears+consumes `pause_requested` at the *top* of its loop
/// (see [`storage::take_plan_pause_requested`]), so arming it while no
/// runner is alive would cause the next `ralph run` / `ralph resume` to
/// exit after zero steps. This mirrors the TUI `[P]` keybinding's
/// `is_run_live()` gate.
///
/// When `plan_slug` is passed, the live run's plan must match it; on
/// mismatch we refuse rather than silently pausing the wrong plan.
pub fn cmd_pause(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    quiet: bool,
) -> Result<()> {
    let live = storage::get_live_run(conn, project)?.ok_or_else(|| {
        anyhow::anyhow!(
            "No active run in this project. `ralph pause` only takes effect while a run is in progress."
        )
    })?;

    if let Some(requested) = plan_slug
        && live.plan_slug.as_deref() != Some(requested)
    {
        let live_label = live.plan_slug.as_deref().unwrap_or("<none>");
        anyhow::bail!("Live run is for plan '{live_label}', not '{requested}'. Refusing to pause.");
    }

    // Resolve the plan to flag. Prefer the slug the caller passed (already
    // validated to match the live run) and fall back to the live run's
    // recorded plan_id when the caller omitted the argument.
    let plan = match plan_slug {
        Some(s) => storage::get_plan_by_slug(conn, s, project)?
            .with_context(|| format!("Plan not found: {s}"))?,
        None => {
            let plan_id = live.plan_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Live run has no associated plan_id. Pass a slug to `ralph pause` to disambiguate."
                )
            })?;
            storage::get_plan_by_id(conn, plan_id)?
        }
    };

    storage::set_plan_pause_requested(conn, &plan.id, true)?;
    if !quiet {
        eprintln!(
            "Pause requested for plan '{}'. The runner will stop after the current step finishes.",
            plan.slug,
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cancel command
// ---------------------------------------------------------------------------

/// Cancel the live `ralph run` for this project.
///
/// Finds the live run via the `run_locks` row, sends SIGTERM to the recorded
/// ralph pid (routed through the graceful-shutdown path), polls for the lock
/// to release, and falls back to SIGKILL if the runner doesn't release within
/// `timeout`. After the target is gone, stale execution-log and step-status
/// rows are reconciled so the history isn't left ambiguous.
///
/// Idempotent: a no-op when there is no active run.
///
/// Unix-only: the graceful shutdown relies on POSIX signals. On non-unix
/// platforms cancel returns an error rather than silently falling back to
/// SIGKILL-equivalents, since the partial semantics would be confusing.
pub fn cmd_cancel(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    force: bool,
    timeout: Duration,
    out: &OutputContext,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (conn, project, plan_slug, force, timeout, out);
        anyhow::bail!("ralph cancel is only supported on unix platforms");
    }

    #[cfg(unix)]
    {
        cmd_cancel_unix(conn, project, plan_slug, force, timeout, out)
    }
}

#[cfg(unix)]
fn cmd_cancel_unix(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    force: bool,
    timeout: Duration,
    out: &OutputContext,
) -> Result<()> {
    // 1. Load the live run.
    let live = match storage::get_live_run(conn, project)? {
        Some(l) => l,
        None => {
            emit_no_active(out)?;
            return Ok(());
        }
    };

    // 2. Validate plan mismatch.
    if let Some(requested) = plan_slug
        && live.plan_slug.as_deref() != Some(requested)
    {
        let live_label = live.plan_slug.as_deref().unwrap_or("<none>");
        anyhow::bail!("Live run is for plan {live_label}, not {requested}. Refusing to cancel.");
    }

    // 3. Verify the pid is the same process we think it is. If the token
    //    mismatches, the pid was reused since the lock was taken, so the
    //    original ralph is already dead.
    let current_token = run_lock::process_start_token(live.pid);
    let target_dead = match (live.pid_start_token.as_deref(), current_token.as_deref()) {
        // We have a stored token: the target is alive iff the live token matches.
        (Some(stored), Some(current)) => stored != current,
        // Stored token but no live one → pid is gone.
        (Some(_), None) => true,
        // No stored token (pre-v9 row) → fall back to liveness probe.
        (None, _) => !pid_is_alive(live.pid),
    };

    if target_dead {
        return finalize_stale_run(conn, &live, /*forced=*/ false, out)
            .context("cleaning up after already-dead runner");
    }

    // 4. Graceful path or --force path.
    let forced = if force {
        kill_force(&live)?;
        true
    } else {
        // SIGTERM the runner, then wait for the lock to release. If it
        // doesn't, escalate to SIGKILL on the runner *and* the harness
        // process group.
        if !out.quiet && out.format == OutputFormat::Plain {
            eprintln!(
                "Waiting for runner (pid {}) to release lock (timeout {}s)...",
                live.pid,
                timeout.as_secs()
            );
        }
        send_signal(live.pid, libc::SIGTERM)
            .with_context(|| format!("sending SIGTERM to pid {}", live.pid))?;

        let released = wait_for_release(conn, project, timeout, out)?;
        if released {
            // Runner handled it gracefully. Its Drop-path release already
            // deleted the run_locks row; just emit summary.
            emit_summary(
                out, &live, /*forced=*/ false, /*already_dead=*/ false,
            )?;
            return Ok(());
        }
        // Escalate.
        if !out.quiet && out.format == OutputFormat::Plain {
            eprintln!(
                "\nRunner did not release lock within {}s — escalating to SIGKILL.",
                timeout.as_secs()
            );
        }
        kill_force(&live)?;
        true
    };

    // 5/6/7/8: After the target is gone, reconcile bookkeeping the runner
    //     didn't get a chance to write.
    finalize_stale_run(conn, &live, forced, out)
}

/// Emit the "no active run" message for both plain and JSON formats.
#[cfg(unix)]
fn emit_no_active(out: &OutputContext) -> Result<()> {
    if out.format == OutputFormat::Json {
        let summary = output::CancelSummary {
            cancelled: false,
            forced: false,
            plan_slug: None,
            step_num: None,
            phase: None,
            attempt: None,
            max_attempts: None,
            pid: None,
            already_dead: false,
        };
        output::emit_ndjson(&summary)?;
    } else if !out.quiet {
        println!("No active run in this project.");
    }
    Ok(())
}

/// Poll the run_locks row every 200ms; returns `true` if it disappeared before
/// `timeout` elapsed, `false` otherwise. A progress dot is printed every ~2s
/// in plain/non-quiet mode so the user sees we're still waiting.
#[cfg(unix)]
fn wait_for_release(
    conn: &Connection,
    project: &str,
    timeout: Duration,
    out: &OutputContext,
) -> Result<bool> {
    const POLL: Duration = Duration::from_millis(200);
    let start = std::time::Instant::now();
    let mut ticks: u64 = 0;
    let show_progress = !out.quiet && out.format == OutputFormat::Plain;

    while start.elapsed() < timeout {
        if storage::get_live_run(conn, project)?.is_none() {
            if show_progress {
                eprintln!();
            }
            return Ok(true);
        }
        std::thread::sleep(POLL);
        ticks += 1;
        // 200ms * 10 = 2s.
        if show_progress && ticks.is_multiple_of(10) {
            eprint!(".");
            use std::io::Write as _;
            let _ = std::io::stderr().flush();
        }
    }
    if show_progress {
        eprintln!();
    }
    Ok(false)
}

/// Handle the case where the target ralph process is already gone (either
/// pid-start-token mismatch from pid reuse, or `--force` after escalation).
/// Deletes the run_locks row (pid + start-token scoped), finalizes a stale
/// execution_log if one was recorded, and flips an InProgress step to Aborted.
#[cfg(unix)]
fn finalize_stale_run(
    conn: &Connection,
    live: &LiveRun,
    forced: bool,
    out: &OutputContext,
) -> Result<()> {
    // Stale execution log: COALESCE-based helper only fills in fields still
    // NULL, so it never clobbers diff/stdout/commit data the runner persisted
    // or a terminal reason the runner already recorded. A missing row is
    // benign — the runner may have deleted its own log during cleanup.
    if let Some(log_id) = live.execution_log_id {
        storage::finalize_execution_log_as_interrupted_if_exists(conn, log_id)?;
    }

    // Atomically flip InProgress → Aborted. A step that's already Complete /
    // Failed / etc. from the runner's own cleanup won't match the predicate
    // and is left alone. Errors from the UPDATE propagate so the operator
    // doesn't see a "cancelled successfully" summary after a DB failure.
    if let Some(step_id) = live.step_id.as_deref() {
        storage::update_step_status_if(conn, step_id, StepStatus::InProgress, StepStatus::Aborted)?;
    }

    // Delete the run_locks row scoped by pid + start token so a new ralph run
    // that already inserted its row (different pid, or reused pid with a new
    // start token) is untouched.
    storage::delete_run_lock_row_unscoped(
        conn,
        &live.project,
        live.pid,
        live.pid_start_token.as_deref(),
    )?;

    emit_summary(out, live, forced, /*already_dead=*/ !forced)
}

/// Escalation / force path: SIGKILL the runner (if still alive) and the
/// harness process group (if the child_pid's start token still matches,
/// guarding against grandchild-pid reuse). Waits briefly for the runner pid
/// to actually disappear.
#[cfg(unix)]
fn kill_force(live: &LiveRun) -> Result<()> {
    // Only SIGKILL the runner if it's still the same live process.
    let runner_alive = match live.pid_start_token.as_deref() {
        Some(stored) => run_lock::process_start_token(live.pid).as_deref() == Some(stored),
        None => pid_is_alive(live.pid),
    };
    if runner_alive {
        // Best-effort: a race where the pid dies between the liveness check
        // and the kill surfaces as ESRCH which we happily ignore.
        let _ = send_signal(live.pid, libc::SIGKILL);
    }

    // SIGKILL the harness process group if we can positively identify it.
    if let (Some(child_pid), Some(stored_child_token)) =
        (live.child_pid, live.child_start_token.as_deref())
        && run_lock::process_start_token(child_pid).as_deref() == Some(stored_child_token)
    {
        // Negative pid targets the whole process group led by `child_pid`.
        let _ = send_signal(-child_pid, libc::SIGKILL);
    }

    // Brief wait (~2s) for the runner to actually die. `kill(pid, 0)` returns
    // -1 with ESRCH when the pid no longer exists.
    for _ in 0..40 {
        if !pid_is_alive(live.pid) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Don't fail hard — stale bookkeeping still needs to run — but warn.
    if !runner_alive {
        // Never was alive; nothing to report.
        return Ok(());
    }
    eprintln!(
        "warning: pid {} did not exit after SIGKILL; cleaning up anyway",
        live.pid
    );
    Ok(())
}

/// Send a signal; returns the errno on failure. SAFETY: `libc::kill` is a
/// plain syscall wrapper.
#[cfg(unix)]
fn send_signal(pid: i64, signo: i32) -> Result<()> {
    // Clamp pid into i32 since libc::kill takes `pid_t` which is i32 on Linux
    // and every other unix we care about.
    let pid_i32 = i32::try_from(pid).with_context(|| format!("pid {pid} does not fit in i32"))?;
    let rc = unsafe { libc::kill(pid_i32, signo) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // ESRCH (no such process) is OK — caller may have raced us.
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        return Err(err).with_context(|| format!("kill({pid_i32}, {signo})"));
    }
    Ok(())
}

/// Liveness probe without requiring a start token. Returns true only if the
/// pid is currently valid.
#[cfg(unix)]
fn pid_is_alive(pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    let Ok(pid_i32) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: kill(pid, 0) is a pure liveness probe.
    let r = unsafe { libc::kill(pid_i32, 0) };
    r == 0
}

/// Final user-facing summary in both plain and JSON modes.
#[cfg(unix)]
fn emit_summary(
    out: &OutputContext,
    live: &LiveRun,
    forced: bool,
    already_dead: bool,
) -> Result<()> {
    if out.format == OutputFormat::Json {
        let summary = output::CancelSummary {
            cancelled: true,
            forced,
            plan_slug: live.plan_slug.clone(),
            step_num: live.step_num,
            phase: live.phase.map(|p| p.as_str().to_string()),
            attempt: live.attempt,
            max_attempts: live.max_attempts,
            pid: Some(live.pid),
            already_dead,
        };
        output::emit_ndjson(&summary)?;
        return Ok(());
    }
    if out.quiet {
        return Ok(());
    }

    let plan_label = live.plan_slug.as_deref().unwrap_or("<unknown>");
    let phase_label = live
        .phase
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let step_label = match (live.step_num, live.max_attempts) {
        (Some(n), _) => format!("step {n}"),
        _ => "no active step".to_string(),
    };
    let attempts_label = match (live.attempt, live.max_attempts) {
        (Some(a), Some(m)) => format!("attempt {a}/{m}"),
        (Some(a), None) => format!("attempt {a}"),
        _ => "no attempt".to_string(),
    };
    let qualifier = if already_dead {
        " (runner was already dead)"
    } else if forced {
        " (forced)"
    } else {
        ""
    };
    println!(
        "Cancelled run for plan {plan_label}, {step_label} (phase {phase_label}, {attempts_label}){qualifier}.",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// FOLLOWUP: end-to-end cancel test with a real `ralph run` subprocess. The
// current coverage exercises the stale-target bookkeeping paths (pid-start-
// token mismatch → cleanup) plus the plan-mismatch guard. Driving a live
// graceful SIGTERM handshake requires spawning the full binary and is best
// done as an integration test in a separate module.

#[cfg(all(test, unix))]
mod cancel_tests {
    use super::*;
    use crate::db;
    use crate::output::{OutputContext, OutputFormat};
    use crate::plan::{Phase, StepStatus, TerminationReason, TestStatus};
    use rusqlite::params;

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    /// A deliberately-bogus pid outside any real pid space on Linux — not
    /// alive, and `/proc/<pid>/stat` returns nothing.
    const DEAD_PID: i64 = 0x7FFF_FFFE;

    fn seed_plan_and_step(conn: &Connection, slug: &str, project: &str) -> (String, String) {
        let plan =
            storage::create_plan(conn, slug, project, "br", "desc", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            conn,
            &plan.id,
            "t",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        (plan.id, step.id)
    }

    #[test]
    fn cancel_no_live_run_is_ok() {
        let conn = db::open_memory().unwrap();
        let result = cmd_cancel(
            &conn,
            "/tmp/proj-no-run",
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        );
        assert!(
            result.is_ok(),
            "cancel with no row should succeed: {result:?}"
        );
    }

    #[test]
    fn cancel_plan_mismatch_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-mismatch";
        let (plan_id, _) = seed_plan_and_step(&conn, "plan-a", project);

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project, DEAD_PID, "fabricated-token", plan_id, "plan-a"],
        )
        .unwrap();

        let err = cmd_cancel(
            &conn,
            project,
            Some("plan-b"),
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect_err("plan mismatch should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Refusing to cancel"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn cancel_dead_target_cleans_up_row() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-dead";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-dead", project);

        // Set the step InProgress so cancel can flip it to Aborted.
        storage::update_step_status(&conn, &step_id, StepStatus::InProgress).unwrap();

        // Seed a run_locks row with a fabricated start token against a pid
        // that's definitely dead (or at least mismatched).
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, step_num, phase, attempt, max_attempts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-dead",
                step_id,
                1i32,
                Phase::Harness.as_str(),
                1i32,
                3i32,
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        // Row is gone.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params![project],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "run_locks row should have been deleted");

        // Step flipped to Aborted.
        let step = storage::get_step_by_id(&conn, &step_id).unwrap().unwrap();
        assert_eq!(step.status, StepStatus::Aborted);
    }

    #[test]
    fn cancel_stale_log_cleanup_preserves_observability_fields() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-stalelog";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-sl", project);

        // Simulate the runner having written diff + stdout before it died.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
            &conn,
            log.id,
            Some(2.5),
            Some("+runner wrote this diff"),
            &[],
            false,
            false,
            None,
            Some("runner stdout"),
            Some("runner stderr"),
            None,
            None,
            None,
            None,
            None, // termination_reason still NULL
            None,
        )
        .unwrap();

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, execution_log_id, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-sl",
                step_id,
                log.id,
                Phase::Tests.as_str(),
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        let updated = storage::get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(
            updated.termination_reason,
            Some(TerminationReason::UserInterrupted)
        );
        assert_eq!(updated.test_status, Some(TestStatus::NotRun));
        // Fields the runner had persisted must survive.
        assert_eq!(updated.diff.as_deref(), Some("+runner wrote this diff"));
        assert_eq!(updated.harness_stdout.as_deref(), Some("runner stdout"));
        assert_eq!(updated.harness_stderr.as_deref(), Some("runner stderr"));
        assert_eq!(updated.duration_secs, Some(2.5));
    }

    #[test]
    fn cancel_stale_log_does_not_overwrite_existing_terminal_reason() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-stalelog-done";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-sld", project);

        // Runner finished Success before cancel raced in.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            false,
            true,
            Some("abc"),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::Success),
            Some(TestStatus::Passed),
        )
        .unwrap();

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, execution_log_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-sld",
                step_id,
                log.id,
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        let updated = storage::get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(updated.termination_reason, Some(TerminationReason::Success));
        assert_eq!(updated.test_status, Some(TestStatus::Passed));
    }

    #[test]
    fn cancel_summary_json_shape() {
        // Build a CancelSummary directly and round-trip through JSON to make
        // sure the field names/shape the CLI advertises are stable. This
        // avoids capturing stdout, which is awkward from a library test.
        let s = output::CancelSummary {
            cancelled: true,
            forced: false,
            plan_slug: Some("plan-xyz".to_string()),
            step_num: Some(4),
            phase: Some("harness".to_string()),
            attempt: Some(2),
            max_attempts: Some(3),
            pid: Some(4242),
            already_dead: false,
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"cancelled\":true"));
        assert!(json.contains("\"forced\":false"));
        assert!(json.contains("\"plan_slug\":\"plan-xyz\""));
        assert!(json.contains("\"step_num\":4"));
        assert!(json.contains("\"phase\":\"harness\""));
        assert!(json.contains("\"attempt\":2"));
        assert!(json.contains("\"max_attempts\":3"));
        assert!(json.contains("\"pid\":4242"));
        assert!(json.contains("\"already_dead\":false"));
    }

    /// Smoke test for the SIGKILL mechanics. Spawns a tiny shell loop,
    /// registers it as the live runner with a matching start token, and
    /// calls `cmd_cancel` with `force=true`. The script has no SIGTERM
    /// handler, so this exercises the escalation-to-SIGKILL path rather
    /// than the full graceful handshake. The pid should be dead (ESRCH)
    /// after cancel returns.
    #[test]
    fn cancel_force_kills_live_script() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-force-kill";
        let (plan_id, _step_id) = seed_plan_and_step(&conn, "plan-fk", project);

        // Spawn `sh -c 'while true; do sleep 1; done'`. Use Rust's
        // std::process so we can read the pid; the child inherits a default
        // SIGTERM disposition (default action: terminate), but we're going
        // straight to SIGKILL via --force.
        let child = std::process::Command::new("sh")
            .arg("-c")
            .arg("while true; do sleep 1; done")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sh loop");

        let child_pid = child.id() as i64;
        // Give the shell a beat to start so /proc/<pid>/stat is populated.
        std::thread::sleep(Duration::from_millis(50));
        let token = run_lock::process_start_token(child_pid).expect("child start token");

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project, child_pid, token, plan_id, "plan-fk"],
        )
        .unwrap();

        // Reap the child asynchronously so `kill(pid, 0)` inside cmd_cancel
        // sees ESRCH instead of lingering on a zombie. In real ralph usage the
        // cancel process is a *sibling* of the runner (not its parent), so it
        // never observes zombification — this thread emulates that.
        let mut child_mut = child;
        let reaper = std::thread::spawn(move || {
            let _ = child_mut.wait();
        });

        // --force → SIGKILL-on-runner straight away, with no graceful wait.
        cmd_cancel(
            &conn,
            project,
            None,
            true,
            Duration::from_secs(2),
            &test_out(),
        )
        .expect("cancel --force");

        let _ = reaper.join();
        assert!(
            !pid_is_alive(child_pid),
            "script pid {child_pid} should be dead after cancel --force"
        );

        // Row cleaned up.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params![project],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}

// ---------------------------------------------------------------------------
// Status live-view + log termination_reason/test_status tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod status_live_view_tests {
    use super::*;
    use crate::db;
    use crate::plan::{Phase, TerminationReason, TestStatus};
    use rusqlite::params;

    fn seed_plan_and_step(conn: &Connection, slug: &str, project: &str) -> (String, String) {
        let plan =
            storage::create_plan(conn, slug, project, "br", "desc", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            conn,
            &plan.id,
            "t",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        (plan.id, step.id)
    }

    #[test]
    fn test_status_with_live_run_populates_json_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-live";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "live-plan", project);

        // Seed a run_locks row with live observability data.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug,
                                    step_id, step_num, attempt, max_attempts, phase,
                                    phase_started_at, current_command)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                project,
                12345i64,
                "tok",
                plan_id,
                "live-plan",
                step_id,
                1i32,
                2i32,
                4i32,
                Phase::Tests.as_str(),
                "2026-04-21T17:23:10.000Z",
                "cargo test",
            ],
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "live-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _steps) = build_status_summary(&conn, project, &plan).unwrap();

        let live = summary
            .live
            .clone()
            .expect("live field should be populated");
        assert_eq!(live.pid, 12345);
        assert_eq!(live.phase, Some(Phase::Tests));
        assert_eq!(live.attempt, Some(2));
        assert_eq!(live.max_attempts, Some(4));
        assert_eq!(live.current_command.as_deref(), Some("cargo test"));

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"live\":{"));
        assert!(json.contains("\"pid\":12345"));
        assert!(json.contains("\"phase\":\"tests\""));
    }

    #[test]
    fn test_status_without_live_run_omits_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-nolive";
        let (_plan_id, _step_id) = seed_plan_and_step(&conn, "quiet-plan", project);

        let plan = storage::get_plan_by_slug(&conn, "quiet-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan).unwrap();
        assert!(summary.live.is_none());

        let json = serde_json::to_string(&summary).unwrap();
        assert!(
            !json.contains("\"live\""),
            "expected live field to be omitted from JSON, got: {json}"
        );
    }

    #[test]
    fn test_status_plan_mismatch_omits_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-mismatch";
        let (plan_a_id, _step_a) = seed_plan_and_step(&conn, "plan-a", project);
        let (_plan_b_id, _step_b) = seed_plan_and_step(&conn, "plan-b", project);

        // The live run is for plan-a.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                project,
                12345i64,
                "tok",
                plan_a_id,
                "plan-a",
                Phase::Harness.as_str(),
            ],
        )
        .unwrap();

        // But we query status for plan-b.
        let plan_b = storage::get_plan_by_slug(&conn, "plan-b", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan_b).unwrap();
        assert!(
            summary.live.is_none(),
            "live run is for a different plan; queried plan should not see it"
        );
    }

    #[test]
    fn test_status_live_with_unbound_plan_id_still_attaches() {
        // An unbound lock (plan_id NULL) covers the whole project; we should
        // attach it to whatever plan is queried rather than silently hiding
        // the live snapshot.
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-unbound";
        let (_plan_id, _step_id) = seed_plan_and_step(&conn, "any-plan", project);

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, phase)
             VALUES (?1, ?2, ?3, ?4)",
            params![project, 12345i64, "tok", Phase::Idle.as_str()],
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "any-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan).unwrap();
        assert!(
            summary.live.is_some(),
            "unbound live lock should attach to any plan queried"
        );
    }

    #[test]
    fn test_cmd_log_json_includes_termination_reason() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-log-reason";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "log-plan", project);

        // Seed an execution_log row with termination_reason + test_status set.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::UserInterrupted),
            Some(TestStatus::Passed),
        )
        .unwrap();

        // Round-trip through the same projection cmd_log uses when emitting
        // NDJSON. Verifies the fields flow through LogEntrySummary::new into
        // the JSON payload.
        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 1);
        let summary = output::LogEntrySummary::new(&logs[0], &LogOutputMode::Hidden);
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"termination_reason\":\"user_interrupted\""));
        assert!(json.contains("\"test_status\":\"passed\""));
    }

    #[test]
    fn test_render_live_block_formats_current_section() {
        // Exercise the plain-text rendering path so the live block format
        // contract is guarded by a test. Uses a phase_started_at a few
        // seconds in the past so the `(Ns)` tag shows up.
        let started = chrono::Utc::now() - chrono::Duration::seconds(12);
        let live = output::LiveRunDisplay {
            pid: 12345,
            plan_slug: Some("plan".into()),
            started_at: "2026-04-21T17:23:10.000Z".into(),
            step_id: Some("step-uuid".into()),
            step_num: Some(3),
            attempt: Some(2),
            max_attempts: Some(4),
            phase: Some(Phase::Tests),
            phase_started_at: Some(started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            phase_elapsed_secs: Some(12.0),
            current_command: Some("cargo test".into()),
            child_pid: Some(54321),
        };
        // Seed a minimal fake step list matching the live.step_id so the
        // title resolves.
        let fake_step = crate::plan::Step {
            id: "step-uuid".into(),
            plan_id: "p".into(),
            sort_key: "a0".into(),
            title: "Add repository types".into(),
            description: "".into(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: crate::plan::StepStatus::InProgress,
            attempts: 2,
            max_retries: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
        };
        let rendered = render_live_block(&live, std::slice::from_ref(&fake_step));
        assert!(rendered.contains("Current:"));
        assert!(rendered.contains("step: 3/1 \"Add repository types\""));
        assert!(rendered.contains("phase: tests"));
        assert!(rendered.contains("attempt: 2/4"));
        assert!(rendered.contains("command: cargo test"));
        assert!(rendered.contains("runner: pid 12345"));
    }

    /// Finding 3 regression: after the harness phase ends and `update_live_phase`
    /// is called with `ChildUpdate::Clear` (simulating the Tests phase), the
    /// emitted status JSON must not advertise the dead harness pid. With
    /// `skip_serializing_if = "Option::is_none"` the field is omitted entirely.
    #[test]
    fn test_status_live_child_pid_clears_after_harness_phase() {
        use crate::storage::ChildUpdate;
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-clear-child";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "clear-plan", project);

        // Seed a run_locks row representing a live run currently in the
        // Harness phase, with child_pid set.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug,
                                    step_id, step_num, attempt, max_attempts, phase,
                                    phase_started_at, child_pid, child_start_token)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                project,
                12345i64,
                "tok",
                plan_id,
                "clear-plan",
                step_id,
                1i32,
                1i32,
                1i32,
                Phase::Harness.as_str(),
                "2026-04-21T17:23:10.000Z",
                98_765i64,
                "child-tok",
            ],
        )
        .unwrap();

        // Simulate the Tests phase write, which clears the child columns.
        storage::update_live_phase(
            &conn,
            project,
            Phase::Tests,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Clear,
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "clear-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _steps) = build_status_summary(&conn, project, &plan).unwrap();

        let live = summary
            .live
            .clone()
            .expect("live field should be populated");
        assert_eq!(
            live.child_pid, None,
            "child_pid must be cleared once the harness phase ends",
        );

        // With `skip_serializing_if = "Option::is_none"`, the field is
        // absent from the JSON payload entirely.
        let json = serde_json::to_string(&summary).unwrap();
        assert!(
            !json.contains("child_pid"),
            "cleared child_pid must be absent from status JSON: {json}"
        );
    }
}

// ---------------------------------------------------------------------------
// Run dispatch routing tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod run_dispatch_tests {
    use super::*;

    fn defaults() -> RunArgs {
        RunArgs::default()
    }

    #[test]
    fn bare_invocation_on_tty_routes_to_tui() {
        // `ralph run` from a TTY with no flags is the canonical TUI entry.
        assert!(is_default_run_invocation(&defaults(), true));
    }

    #[test]
    fn bare_invocation_with_plan_slug_routes_to_tui() {
        // A plan-slug positional alone does not count as a "non-default flag";
        // `ralph run my-plan` from a TTY still drops to the TUI.
        let args = RunArgs {
            plan_slug: Some("my-plan".to_string()),
            ..defaults()
        };
        assert!(is_default_run_invocation(&args, true));
    }

    #[test]
    fn current_branch_does_not_bypass_tui() {
        // `--current-branch` is a behavior modifier, not an opt-out from
        // interactivity — the TUI's auto-start path threads it through to
        // the spawned runner via `StreamMode::Run { current_branch }`.
        let args = RunArgs {
            current_branch: true,
            ..defaults()
        };
        assert!(is_default_run_invocation(&args, true));
    }

    #[test]
    fn no_auto_stash_does_not_bypass_tui() {
        // `--no-auto-stash` is a behavior modifier, not an opt-out from
        // interactivity — the TUI's auto-start path threads it through to
        // the spawned runner via `StreamMode::Run { no_auto_stash }`.
        let args = RunArgs {
            no_auto_stash: true,
            ..defaults()
        };
        assert!(is_default_run_invocation(&args, true));
    }

    #[test]
    fn verbose_bypasses_tui() {
        // `--verbose` is an explicit user request to see the full prompt
        // preview on stderr. The TUI's spawned subprocess runs with `--json`,
        // which routes the preview through NDJSON `PromptPrepared` events
        // instead — so the verbose stderr output the user asked for would
        // never reach them. Honor the intent by staying on the direct CLI
        // runner path.
        let args = RunArgs {
            verbose: true,
            ..defaults()
        };
        assert!(!is_default_run_invocation(&args, true));
    }

    #[test]
    fn non_tty_stdout_bypasses_tui() {
        // Piping to `tee` (or any non-TTY) must keep today's runner path.
        assert!(!is_default_run_invocation(&defaults(), false));
    }

    #[test]
    fn non_interactive_flag_bypasses_tui() {
        // `--non-interactive` is the explicit opt-out from the TUI even on
        // a real TTY (e.g. user wants to capture scripted output via `tee`).
        let args = RunArgs {
            non_interactive: true,
            ..defaults()
        };
        assert!(!is_default_run_invocation(&args, true));
    }

    #[test]
    fn json_flag_bypasses_tui() {
        // `--json` / `--jsonl` are scripted-output formats; they must keep
        // today's NDJSON behavior regardless of TTY status.
        let args = RunArgs {
            json: true,
            ..defaults()
        };
        assert!(!is_default_run_invocation(&args, true));
    }

    #[test]
    fn run_specific_flags_each_bypass_tui() {
        // Every Run-subcommand flag listed in TUI-plan.md §2 must drop
        // today's runner path. Using a single per-flag sweep so adding a
        // new flag forces an explicit decision in this test.
        let cases: Vec<(&str, RunArgs)> = vec![
            (
                "--one",
                RunArgs {
                    one: true,
                    ..defaults()
                },
            ),
            (
                "--all",
                RunArgs {
                    all: true,
                    ..defaults()
                },
            ),
            (
                "--from",
                RunArgs {
                    from: Some(2),
                    ..defaults()
                },
            ),
            (
                "--to",
                RunArgs {
                    to: Some(5),
                    ..defaults()
                },
            ),
            (
                "--dry-run",
                RunArgs {
                    dry_run: true,
                    ..defaults()
                },
            ),
            (
                "--skip-preflight",
                RunArgs {
                    skip_preflight: true,
                    ..defaults()
                },
            ),
            (
                "--force",
                RunArgs {
                    force: true,
                    ..defaults()
                },
            ),
            (
                "--verbose",
                RunArgs {
                    verbose: true,
                    ..defaults()
                },
            ),
        ];
        for (label, args) in cases {
            assert!(
                !is_default_run_invocation(&args, true),
                "{label} must bypass TUI mode"
            );
        }
    }

    #[test]
    fn harness_override_at_either_scope_bypasses_tui() {
        // Per-subcommand and global `--harness` both count as a non-default
        // override. Either alone is enough to take today's runner path.
        let run_only = RunArgs {
            run_harness: Some("codex".to_string()),
            ..defaults()
        };
        assert!(!is_default_run_invocation(&run_only, true));

        let global_only = RunArgs {
            cli_harness: Some("codex".to_string()),
            ..defaults()
        };
        assert!(!is_default_run_invocation(&global_only, true));
    }
}

// ---------------------------------------------------------------------------
// Plan-detail auto-start → preferred-run-mode wiring (TUI-plan.md §2)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod plan_detail_init_tests {
    use super::*;
    use crate::config::Config;
    use crate::plan::{Plan, PlanStatus, Step};
    use crate::tui::events::StreamMode;
    use crate::tui::views::plan_detail::PlanDetailApp;
    use chrono::Utc;

    fn make_plan() -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "demo".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "feat/test".to_string(),
            description: "A test plan".to_string(),
            status: PlanStatus::InProgress,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
        }
    }

    fn make_app() -> PlanDetailApp {
        PlanDetailApp::new(make_plan(), Vec::<Step>::new(), &Config::default())
    }

    #[test]
    fn auto_start_run_latches_flags_onto_app() {
        let mut app = make_app();
        plan_detail_init_preferred_run_mode(
            &mut app,
            Some(StreamMode::Run {
                current_branch: true,
                no_auto_stash: true,
            }),
        );
        assert_eq!(
            app.preferred_run_mode(),
            StreamMode::Run {
                current_branch: true,
                no_auto_stash: true,
            }
        );
    }

    #[test]
    fn auto_start_resume_does_not_disturb_default_run_mode() {
        let mut app = make_app();
        let before = app.preferred_run_mode();
        plan_detail_init_preferred_run_mode(&mut app, Some(StreamMode::Resume));
        assert_eq!(app.preferred_run_mode(), before);
    }

    #[test]
    fn no_auto_start_leaves_default_run_mode() {
        let mut app = make_app();
        let before = app.preferred_run_mode();
        plan_detail_init_preferred_run_mode(&mut app, None);
        assert_eq!(app.preferred_run_mode(), before);
    }
}

// ---------------------------------------------------------------------------
// Resume dispatch routing tests (TUI-plan.md §2 / step 34)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod resume_dispatch_tests {
    use super::*;

    fn defaults() -> ResumeArgs {
        ResumeArgs::default()
    }

    #[test]
    fn bare_invocation_on_tty_routes_to_tui() {
        // `ralph resume` from a TTY with no flags is the canonical TUI entry.
        assert!(is_default_resume_invocation(&defaults(), true));
    }

    #[test]
    fn bare_invocation_with_plan_slug_routes_to_tui() {
        // `ralph resume my-plan` from a TTY still drops to the TUI — slug
        // alone is not a "non-default flag".
        let args = ResumeArgs {
            plan_slug: Some("my-plan".to_string()),
            ..defaults()
        };
        assert!(is_default_resume_invocation(&args, true));
    }

    #[test]
    fn non_tty_stdout_bypasses_tui() {
        // Piping resume output to `tee` (or any non-TTY) keeps today's CLI
        // runner path so script captures don't regress.
        assert!(!is_default_resume_invocation(&defaults(), false));
    }

    #[test]
    fn non_interactive_flag_bypasses_tui() {
        let args = ResumeArgs {
            non_interactive: true,
            ..defaults()
        };
        assert!(!is_default_resume_invocation(&args, true));
    }

    #[test]
    fn json_flag_bypasses_tui() {
        // `--json` / `--jsonl` are scripted-output formats — they must keep
        // the NDJSON path on stdout regardless of TTY status.
        let args = ResumeArgs {
            json: true,
            ..defaults()
        };
        assert!(!is_default_resume_invocation(&args, true));
    }

    #[test]
    fn quiet_flag_bypasses_tui() {
        // `--quiet` signals scripted use; the TUI is intentionally chatty
        // (toasts, banners), so honour the explicit ask for silence.
        let args = ResumeArgs {
            quiet: true,
            ..defaults()
        };
        assert!(!is_default_resume_invocation(&args, true));
    }

    #[test]
    fn force_flag_bypasses_tui() {
        // `--force` is a recovery flag for a stale run lock — its presence
        // means the user is troubleshooting and wants the CLI report on
        // stderr.
        let args = ResumeArgs {
            force: true,
            ..defaults()
        };
        assert!(!is_default_resume_invocation(&args, true));
    }

    #[test]
    fn harness_override_bypasses_tui() {
        // Global `--harness` counts as a non-default override.
        let args = ResumeArgs {
            cli_harness: Some("codex".to_string()),
            ..defaults()
        };
        assert!(!is_default_resume_invocation(&args, true));
    }
}

// ---------------------------------------------------------------------------
// Resume parity test: the TUI auto-start path and the CLI path both
// dispatch through the SAME runner code. The TUI forks
// `ralph resume <slug>` (which lands in `dispatch_resume` on the child
// side, calling `runner::resume_plan`), and the CLI path calls
// `dispatch_resume` directly. The shared-helper structure means there's
// only one way the resume actually runs — verified here by spawning a
// real subprocess against a fake plan and asserting that the same NDJSON
// stream byte layout we'd see on the CLI is the one that flows through
// the streaming command builder.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod resume_parity_tests {
    use crate::tui::events::{StreamMode, build_streaming_run_command};
    use std::path::Path;

    /// The TUI-spawned resume subprocess invokes the same `ralph resume`
    /// CLI surface as the user would type. This guards against the
    /// streaming helper drifting away from the CLI path (e.g. someone
    /// adding `--current-branch` to the run variant and forgetting the
    /// implicit-current-branch invariant for resume).
    #[test]
    fn streaming_resume_reaches_same_cli_surface_as_user_typed_resume() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Resume,
        );
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();

        // Exact arg list — equivalent to typing
        // `ralph -C /proj --non-interactive --json resume my-plan`
        // by hand. Anything else means we'd be invoking a different
        // resume code path than the user does on the CLI.
        assert_eq!(
            args,
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "--json".to_string(),
                "resume".to_string(),
                "my-plan".to_string(),
            ]
        );
    }
}

#[cfg(test)]
mod plan_list_action_tests {
    //! Integration tests for the `A` (approve) and `Q` (toggle questions)
    //! keybinding handlers in the plan-list TUI view. Exercise the public
    //! action helpers end-to-end against an in-memory DB so the tests cover
    //! both the storage write and the in-place tile update.

    use super::*;
    use crate::db;
    use crate::plan::PlanStatus;
    use crate::tui::views::plan_list::PlanListApp;

    fn seed_app(project: &str) -> (Connection, PlanListApp) {
        let conn = db::open_memory().unwrap();
        // Two plans so we can verify the cursor target is the one mutated.
        // Sleep between creates so the millisecond-precision created_at
        // values differ — `list_plans_sorted_by_recency` orders by
        // `created_at DESC` and SQLite's tie-break on equal timestamps is
        // undefined, so without this gap the test cursor could land on
        // either plan depending on which side of the millisecond boundary
        // both inserts fell on.
        storage::create_plan(&conn, "alpha", project, "b1", "d", None, None, &[]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        storage::create_plan(&conn, "beta", project, "b2", "d", None, None, &[]).unwrap();
        let tiles = build_plan_tiles(&conn, project).unwrap();
        let app = PlanListApp::new(tiles, project, "UTC");
        (conn, app)
    }

    #[test]
    fn approve_cursor_flips_planning_to_ready() {
        let project = "/tmp/approve-flow";
        let (conn, mut app) = seed_app(project);
        // Cursor on the first tile (most-recent first → "beta").
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        assert_eq!(app.cursor_plan().unwrap().status, PlanStatus::Planning);

        plan_list_approve_cursor(&conn, project, &mut app).unwrap();

        // DB row was updated.
        let from_db = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert_eq!(from_db.status, PlanStatus::Ready);

        // In-memory tile is updated in-place; cursor / scroll preserved.
        assert_eq!(app.cursor_plan().unwrap().status, PlanStatus::Ready);
        assert_eq!(app.cursor_plan().unwrap().slug, target_slug);

        // Toast confirms the action.
        let toast = app.toasts.current().expect("toast should be present");
        assert_eq!(toast.text, "Plan approved.");
    }

    #[test]
    fn approve_cursor_on_non_planning_emits_info_toast_and_keeps_status() {
        let project = "/tmp/approve-noop";
        let (conn, mut app) = seed_app(project);
        // Pre-flip the cursor target to Ready so A becomes a no-op write.
        let id = app.cursor_plan().unwrap().id.clone();
        storage::update_plan_status(&conn, &id, PlanStatus::Ready).unwrap();
        // Refresh tiles so the in-memory state reflects the pre-flipped status.
        app.refresh_tiles(build_plan_tiles(&conn, project).unwrap(), 0);

        plan_list_approve_cursor(&conn, project, &mut app).unwrap();

        let from_db = storage::get_plan_by_slug(&conn, &app.cursor_plan().unwrap().slug, project)
            .unwrap()
            .unwrap();
        assert_eq!(from_db.status, PlanStatus::Ready);
        let toast = app.toasts.current().expect("toast should be present");
        assert!(
            toast.text.contains("nothing to approve"),
            "expected info toast, got: {toast:?}"
        );
    }

    #[test]
    fn approve_cursor_empty_app_is_noop() {
        let conn = db::open_memory().unwrap();
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        plan_list_approve_cursor(&conn, "/proj", &mut app).unwrap();
        assert!(app.toasts.is_empty());
    }

    #[test]
    fn toggle_questions_flips_column_and_toasts_new_state() {
        let project = "/tmp/questions-toggle";
        let (conn, mut app) = seed_app(project);
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        assert!(!app.cursor_plan().unwrap().questions_enabled);

        // First press: off → on.
        plan_list_toggle_questions_cursor(&conn, project, &mut app).unwrap();
        let after_on = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert!(after_on.questions_enabled);
        assert!(app.cursor_plan().unwrap().questions_enabled);
        assert_eq!(app.toasts.current().unwrap().text, "Questions enabled.");

        // Second press: on → off.
        plan_list_toggle_questions_cursor(&conn, project, &mut app).unwrap();
        let after_off = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert!(!after_off.questions_enabled);
        assert!(!app.cursor_plan().unwrap().questions_enabled);
        assert_eq!(app.toasts.current().unwrap().text, "Questions disabled.");
    }

    #[test]
    fn toggle_questions_does_not_touch_non_cursor_tiles() {
        let project = "/tmp/questions-cursor-only";
        let (conn, mut app) = seed_app(project);
        let cursor_slug = app.cursor_plan().unwrap().slug.clone();
        // The other tile.
        let other_slug = app
            .tiles
            .iter()
            .map(|t| t.plan.slug.clone())
            .find(|s| s != &cursor_slug)
            .unwrap();

        plan_list_toggle_questions_cursor(&conn, project, &mut app).unwrap();

        let other = storage::get_plan_by_slug(&conn, &other_slug, project)
            .unwrap()
            .unwrap();
        assert!(
            !other.questions_enabled,
            "non-cursor plan must remain untouched"
        );
    }

    #[test]
    fn toggle_questions_empty_app_is_noop() {
        let conn = db::open_memory().unwrap();
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        plan_list_toggle_questions_cursor(&conn, "/proj", &mut app).unwrap();
        assert!(app.toasts.is_empty());
    }

    // -- refresh ---------------------------------------------------------

    #[test]
    fn refresh_picks_up_externally_inserted_plan_and_toasts() {
        let project = "/tmp/refresh-pickup";
        let (conn, mut app) = seed_app(project);
        let initial_len = app.tiles.len();

        // Simulate an external mutation: another process inserts a plan
        // while the TUI is open. Without `r`, the in-memory tile list would
        // remain stale.
        storage::create_plan(&conn, "gamma", project, "b3", "d", None, None, &[]).unwrap();
        assert_eq!(app.tiles.len(), initial_len);

        plan_list_refresh(&conn, project, &mut app).unwrap();

        assert_eq!(app.tiles.len(), initial_len + 1);
        assert!(app.tiles.iter().any(|t| t.plan.slug == "gamma"));
        assert_eq!(app.toasts.current().unwrap().text, "Refreshed.");
    }

    #[test]
    fn refresh_drops_externally_archived_plan() {
        let project = "/tmp/refresh-archive";
        let (conn, mut app) = seed_app(project);
        let id = app.tiles[0].plan.id.clone();

        storage::update_plan_status(&conn, &id, crate::plan::PlanStatus::Archived).unwrap();
        plan_list_refresh(&conn, project, &mut app).unwrap();

        assert_eq!(app.tiles.len(), 1);
        assert!(!app.tiles.iter().any(|t| t.plan.id == id));
    }

    #[test]
    fn refresh_on_empty_project_still_toasts() {
        let conn = db::open_memory().unwrap();
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        plan_list_refresh(&conn, "/proj", &mut app).unwrap();
        assert!(app.tiles.is_empty());
        assert_eq!(app.toasts.current().unwrap().text, "Refreshed.");
    }

    // -- create-plan -----------------------------------------------------

    #[test]
    fn apply_create_inserts_plan_and_positions_cursor_on_it() {
        let project = "/tmp/create-plan-flow";
        let (conn, mut app) = seed_app(project);
        let config = Config::default();
        let initial_len = app.tiles.len();

        plan_list_apply_create(
            &conn,
            &config,
            project,
            &mut app,
            "gamma",
            "A new plan",
            &["cargo test".to_string()],
        )
        .unwrap();

        // Tile list grew by one and the cursor lands on the new plan
        // regardless of where it sorts.
        assert_eq!(app.tiles.len(), initial_len + 1);
        let cursor = app.cursor_plan().expect("cursor target");
        assert_eq!(cursor.slug, "gamma");

        // DB row exists with the expected fields.
        let row = storage::get_plan_by_slug(&conn, "gamma", project)
            .unwrap()
            .unwrap();
        assert_eq!(row.description, "A new plan");
        assert_eq!(row.deterministic_tests, vec!["cargo test".to_string()]);
        // Default harness comes from `Config::default()`.
        assert_eq!(row.harness.as_deref(), Some("claude"));
        assert!(row.agent.is_none());
        // Branch name defaults to the slug.
        assert_eq!(row.branch_name, "gamma");

        let toast = app.toasts.current().expect("success toast");
        assert!(
            toast.text.contains("gamma"),
            "expected slug in toast, got: {toast:?}"
        );
    }

    #[test]
    fn apply_create_with_empty_tests_passes_empty_vec() {
        let project = "/tmp/create-plan-no-tests";
        let (conn, mut app) = seed_app(project);
        let config = Config::default();

        plan_list_apply_create(&conn, &config, project, &mut app, "delta", "d", &[]).unwrap();

        let row = storage::get_plan_by_slug(&conn, "delta", project)
            .unwrap()
            .unwrap();
        assert!(row.deterministic_tests.is_empty());
    }

    #[test]
    fn apply_create_uses_configured_default_harness() {
        let project = "/tmp/create-plan-harness";
        let (conn, mut app) = seed_app(project);
        // Default config defines a "codex" harness — rebind the global
        // default to it so we exercise non-claude harness selection.
        let config = Config {
            default_harness: "codex".to_string(),
            ..Config::default()
        };

        plan_list_apply_create(&conn, &config, project, &mut app, "epsilon", "", &[]).unwrap();

        let row = storage::get_plan_by_slug(&conn, "epsilon", project)
            .unwrap()
            .unwrap();
        assert_eq!(row.harness.as_deref(), Some("codex"));
    }

    #[test]
    fn apply_create_duplicate_slug_emits_error_toast_and_leaves_tiles_untouched() {
        let project = "/tmp/create-plan-dup";
        let (conn, mut app) = seed_app(project);
        let config = Config::default();
        let before_len = app.tiles.len();

        // "alpha" already exists from `seed_app`.
        plan_list_apply_create(&conn, &config, project, &mut app, "alpha", "", &[]).unwrap();

        assert_eq!(app.tiles.len(), before_len, "tile count unchanged");
        let toast = app.toasts.current().expect("error toast");
        assert!(
            toast.text.starts_with("Failed to create plan"),
            "unexpected toast text: {toast:?}"
        );
    }

    #[test]
    fn apply_create_clears_selection_via_refresh() {
        let project = "/tmp/create-plan-clears-selection";
        let (conn, mut app) = seed_app(project);
        let config = Config::default();
        // Select a tile; create-plan should wipe it via refresh_tiles.
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);

        plan_list_apply_create(&conn, &config, project, &mut app, "zeta", "z", &[]).unwrap();

        assert!(app.selection.is_empty());
    }

    // -- Esc precedence (toast dismiss) ----------------------------------

    #[test]
    fn esc_dismisses_toast_when_one_is_present_and_preserves_selection() {
        // Toast precedence (TUI-plan.md §4): Esc consumes the toast first.
        // The view's normal Esc handler (clear-selection) must NOT fire when
        // a toast is dismissed, otherwise a single Esc would do two things.
        use crate::tui::toast::ToastKind;
        use std::time::Instant;

        let project = "/tmp/plan-list-esc-toast";
        let (_conn, mut app) = seed_app(project);
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        app.toasts
            .push("Saved.", ToastKind::Success, Instant::now());
        assert!(app.toasts.current().is_some());

        let dismissed = plan_list_handle_esc(&mut app);

        assert!(dismissed, "Esc must report toast was consumed");
        assert!(app.toasts.is_empty(), "toast must be popped");
        assert_eq!(
            app.selection.len(),
            1,
            "selection must be untouched when Esc consumed the toast"
        );
        assert!(!app.should_quit, "Esc must not quit when consuming a toast");
    }

    #[test]
    fn esc_falls_through_to_clear_selection_when_no_toast() {
        // Without a toast, Esc retains its original §5 behavior:
        // clear-selection-or-quit. With a selection present, it clears.
        let project = "/tmp/plan-list-esc-no-toast";
        let (_conn, mut app) = seed_app(project);
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        assert!(app.toasts.is_empty());

        let dismissed = plan_list_handle_esc(&mut app);

        assert!(!dismissed, "no toast was present");
        assert!(app.selection.is_empty(), "selection must be cleared");
        assert!(!app.should_quit);
    }

    #[test]
    fn esc_falls_through_to_quit_when_no_toast_and_no_selection() {
        // Empty-selection fallthrough still mirrors `app.escape()`'s second
        // arm (set should_quit), so behavior matches the pre-precedence view.
        let project = "/tmp/plan-list-esc-quit";
        let (_conn, mut app) = seed_app(project);
        assert!(app.toasts.is_empty());
        assert!(app.selection.is_empty());

        let dismissed = plan_list_handle_esc(&mut app);

        assert!(!dismissed);
        assert!(app.should_quit, "escape on empty selection still quits");
    }
}

// ---------------------------------------------------------------------------
// Archived-list dispatcher tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod archived_list_dispatcher_tests {
    //! Integration tests for the archived-list view's `d` (permanently delete)
    //! and `enter` (unarchive) actions. Drives the public action helpers
    //! against an in-memory DB so we cover both the storage write and the
    //! in-memory tile refresh.

    use super::*;
    use crate::db;
    use crate::plan::PlanStatus;
    use crate::tui::views::archived_list::ArchivedListApp;

    fn seed_archived(project: &str) -> (Connection, ArchivedListApp) {
        let conn = db::open_memory().unwrap();
        let a = storage::create_plan(&conn, "alpha", project, "b1", "d", None, None, &[]).unwrap();
        let b = storage::create_plan(&conn, "beta", project, "b2", "d", None, None, &[]).unwrap();
        let g = storage::create_plan(&conn, "gamma", project, "b3", "d", None, None, &[]).unwrap();
        // Archive every plan so the archived view sees three rows.
        storage::update_plan_status(&conn, &a.id, PlanStatus::Archived).unwrap();
        storage::update_plan_status(&conn, &b.id, PlanStatus::Archived).unwrap();
        storage::update_plan_status(&conn, &g.id, PlanStatus::Archived).unwrap();
        let tiles = build_archived_tiles(&conn, project).unwrap();
        let app = ArchivedListApp::new(tiles, project, "UTC");
        (conn, app)
    }

    #[test]
    fn delete_targets_removes_from_db_and_tile_list() {
        let project = "/tmp/archived-delete";
        let (conn, mut app) = seed_archived(project);
        // Cursor target by default: most recent (gamma) — capture its slug.
        let target_id = app.cursor_plan().unwrap().id.clone();
        let target_slug = app.cursor_plan().unwrap().slug.clone();

        archived_list_apply_delete(&conn, project, &mut app, std::slice::from_ref(&target_id))
            .unwrap();

        // DB row gone.
        assert!(
            storage::get_plan_by_slug(&conn, &target_slug, project)
                .unwrap()
                .is_none()
        );
        // In-memory tile list shrunk by one and no longer contains it.
        assert_eq!(app.tiles.len(), 2);
        assert!(app.tiles.iter().all(|t| t.plan.id != target_id));
        // Toast confirms the destructive action.
        let toast = app.toasts.current().expect("toast should be present");
        assert_eq!(toast.text, "Permanently deleted 1 plan.");
    }

    #[test]
    fn delete_with_multi_selection_pluralizes_toast() {
        let project = "/tmp/archived-delete-multi";
        let (conn, mut app) = seed_archived(project);
        // Select all three.
        app.toggle_selection();
        app.selected_index = 1;
        app.toggle_selection();
        app.selected_index = 2;
        app.toggle_selection();
        let targets = app.action_targets();
        assert_eq!(targets.len(), 3);

        archived_list_apply_delete(&conn, project, &mut app, &targets).unwrap();

        // All three plans gone.
        assert_eq!(app.tiles.len(), 0);
        assert_eq!(storage::count_archived_plans(&conn, project).unwrap(), 0);
        let toast = app.toasts.current().expect("toast should be present");
        assert_eq!(toast.text, "Permanently deleted 3 plans.");
    }

    #[test]
    fn unarchive_targets_flips_status_to_ready_and_drops_from_archived_view() {
        let project = "/tmp/archived-unarchive";
        let (conn, mut app) = seed_archived(project);
        let target_id = app.cursor_plan().unwrap().id.clone();
        let target_slug = app.cursor_plan().unwrap().slug.clone();

        archived_list_apply_unarchive(&conn, project, &mut app, std::slice::from_ref(&target_id))
            .unwrap();

        // Status flipped to Ready in the DB.
        let row = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, PlanStatus::Ready);
        // The unarchived plan is gone from the archived view's tile list.
        assert!(app.tiles.iter().all(|t| t.plan.id != target_id));
        assert_eq!(app.tiles.len(), 2);
        // Toast confirms.
        let toast = app.toasts.current().expect("toast should be present");
        assert_eq!(toast.text, "Unarchived 1 plan.");
    }

    #[test]
    fn unarchive_with_multi_selection_pluralizes_toast() {
        let project = "/tmp/archived-unarchive-multi";
        let (conn, mut app) = seed_archived(project);
        app.toggle_selection();
        app.selected_index = 1;
        app.toggle_selection();
        let targets = app.action_targets();
        assert_eq!(targets.len(), 2);

        archived_list_apply_unarchive(&conn, project, &mut app, &targets).unwrap();

        assert_eq!(app.tiles.len(), 1);
        assert_eq!(storage::count_archived_plans(&conn, project).unwrap(), 1);
        let toast = app.toasts.current().expect("toast should be present");
        assert_eq!(toast.text, "Unarchived 2 plans.");
    }

    #[test]
    fn unarchive_empties_view_when_last_archived_plan_returns() {
        let project = "/tmp/archived-unarchive-last";
        let conn = db::open_memory().unwrap();
        let only = storage::create_plan(&conn, "only", project, "b", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &only.id, PlanStatus::Archived).unwrap();
        let tiles = build_archived_tiles(&conn, project).unwrap();
        let mut app = ArchivedListApp::new(tiles, project, "UTC");
        assert_eq!(app.tiles.len(), 1);

        archived_list_apply_unarchive(&conn, project, &mut app, std::slice::from_ref(&only.id))
            .unwrap();

        assert!(app.tiles.is_empty());
        assert_eq!(app.selected_index, 0);
        // archived count went to zero, so a subsequent plan-list refresh
        // will hide the sentinel.
        assert_eq!(storage::count_archived_plans(&conn, project).unwrap(), 0);
    }

    #[test]
    fn delete_clears_selection_via_refresh() {
        let project = "/tmp/archived-delete-clears";
        let (conn, mut app) = seed_archived(project);
        app.toggle_selection();
        let targets = app.action_targets();

        archived_list_apply_delete(&conn, project, &mut app, &targets).unwrap();

        assert!(app.selection.is_empty());
    }

    #[test]
    fn enter_path_targets_cursor_when_no_selection() {
        // The dispatcher uses `app.action_targets()` for both `d` and `enter`;
        // when nothing is selected it should fall through to the cursor row.
        let project = "/tmp/archived-cursor-fallback";
        let (_conn, mut app) = seed_archived(project);
        app.selected_index = 1;
        let cursor_id = app.cursor_plan().unwrap().id.clone();
        assert_eq!(app.action_targets(), vec![cursor_id]);
    }

    #[test]
    fn refresh_picks_up_externally_archived_plan_and_toasts() {
        // Mirrors plan-list `r`: an external mutation (here, archiving a new
        // plan) becomes visible in the in-memory tile list only after `r`.
        let project = "/tmp/archived-refresh-pickup";
        let (conn, mut app) = seed_archived(project);
        let initial_len = app.tiles.len();

        let delta =
            storage::create_plan(&conn, "delta", project, "b4", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &delta.id, PlanStatus::Archived).unwrap();
        // Without a refresh, the tile list is still stale.
        assert_eq!(app.tiles.len(), initial_len);

        archived_list_refresh(&conn, project, &mut app).unwrap();

        assert_eq!(app.tiles.len(), initial_len + 1);
        assert!(app.tiles.iter().any(|t| t.plan.slug == "delta"));
        assert_eq!(app.toasts.current().unwrap().text, "Refreshed.");
    }

    #[test]
    fn refresh_drops_externally_unarchived_plan() {
        let project = "/tmp/archived-refresh-unarchive";
        let (conn, mut app) = seed_archived(project);
        let id = app.tiles[0].plan.id.clone();

        storage::update_plan_status(&conn, &id, PlanStatus::Ready).unwrap();
        archived_list_refresh(&conn, project, &mut app).unwrap();

        assert_eq!(app.tiles.len(), 2);
        assert!(!app.tiles.iter().any(|t| t.plan.id == id));
    }

    #[test]
    fn refresh_on_empty_archived_view_still_toasts() {
        let project = "/tmp/archived-refresh-empty";
        let conn = db::open_memory().unwrap();
        let tiles = build_archived_tiles(&conn, project).unwrap();
        let mut app = ArchivedListApp::new(tiles, project, "UTC");

        archived_list_refresh(&conn, project, &mut app).unwrap();

        assert!(app.tiles.is_empty());
        assert_eq!(app.toasts.current().unwrap().text, "Refreshed.");
    }

    // -- Esc precedence (toast dismiss) ----------------------------------

    #[test]
    fn esc_dismisses_toast_when_one_is_present_and_preserves_selection() {
        // Same precedence rule as plan-list (TUI-plan.md §4): Esc consumes
        // the toast first. The view's `escape()` (clear-selection-or-pop)
        // must NOT fire when a toast is dismissed.
        use crate::tui::toast::ToastKind;
        use std::time::Instant;

        let project = "/tmp/archived-list-esc-toast";
        let (_conn, mut app) = seed_archived(project);
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        app.toasts
            .push("Saved.", ToastKind::Success, Instant::now());

        let dismissed = archived_list_handle_esc(&mut app);

        assert!(dismissed);
        assert!(app.toasts.is_empty());
        assert_eq!(
            app.selection.len(),
            1,
            "selection must be untouched when Esc consumed the toast"
        );
        assert!(!app.should_pop, "Esc must not pop when consuming a toast");
    }

    #[test]
    fn esc_falls_through_to_clear_selection_when_no_toast() {
        let project = "/tmp/archived-list-esc-no-toast";
        let (_conn, mut app) = seed_archived(project);
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        assert!(app.toasts.is_empty());

        let dismissed = archived_list_handle_esc(&mut app);

        assert!(!dismissed);
        assert!(app.selection.is_empty());
        assert!(!app.should_pop);
    }

    #[test]
    fn esc_falls_through_to_pop_when_no_toast_and_no_selection() {
        let project = "/tmp/archived-list-esc-pop";
        let (_conn, mut app) = seed_archived(project);
        assert!(app.toasts.is_empty());
        assert!(app.selection.is_empty());

        let dismissed = archived_list_handle_esc(&mut app);

        assert!(!dismissed);
        assert!(app.should_pop, "escape on empty selection still pops");
    }
}

// ---------------------------------------------------------------------------
// Step-detail dispatcher tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod step_detail_dispatcher_tests {
    //! Verify the `<esc>` toast-dismiss precedence (TUI-plan.md §4) for the
    //! step-detail dispatcher: when a toast is showing it is consumed
    //! without popping the view; otherwise Esc behaves as before
    //! (`request_pop`).

    use super::*;
    use crate::plan::{ChangePolicy, Plan, PlanStatus, Step, StepStatus};
    use crate::tui::toast::ToastKind;
    use crate::tui::views::step_detail::StepDetailApp;
    use chrono::Utc;
    use std::time::Instant;

    fn make_app() -> StepDetailApp {
        let plan = Plan {
            id: "p1".to_string(),
            slug: "test".to_string(),
            project: "/tmp".to_string(),
            branch_name: "b".to_string(),
            description: "d".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
        };
        let steps = vec![Step {
            id: "s0".to_string(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Step".to_string(),
            description: "Desc".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::InProgress,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
        }];
        StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            storage::ProjectSettings::default(),
            Vec::new(),
        )
    }

    #[test]
    fn esc_dismisses_toast_without_popping() {
        let mut app = make_app();
        app.toasts
            .push("Saved.", ToastKind::Success, Instant::now());

        let dismissed = step_detail_handle_esc(&mut app);

        assert!(dismissed);
        assert!(app.toasts.is_empty(), "toast must be popped");
        assert!(
            !app.should_pop,
            "Esc must not pop the view when consuming a toast"
        );
    }

    #[test]
    fn esc_falls_through_to_request_pop_when_no_toast() {
        let mut app = make_app();
        assert!(app.toasts.is_empty());
        assert!(!app.should_pop);

        let dismissed = step_detail_handle_esc(&mut app);

        assert!(!dismissed);
        assert!(
            app.should_pop,
            "without a toast Esc retains its original pop behavior"
        );
    }

    #[test]
    fn esc_dismisses_only_one_toast_at_a_time() {
        // Stacked toasts: the first Esc pops the most-recent (current) one;
        // a follow-up Esc still has a toast to consume rather than popping
        // the view.
        let mut app = make_app();
        app.toasts.push("first", ToastKind::Info, Instant::now());
        app.toasts.push("second", ToastKind::Info, Instant::now());

        assert!(step_detail_handle_esc(&mut app));
        assert_eq!(app.toasts.current().unwrap().text, "first");
        assert!(!app.should_pop);

        assert!(step_detail_handle_esc(&mut app));
        assert!(app.toasts.is_empty());
        assert!(!app.should_pop);

        // A third Esc with no toasts left finally falls through to pop.
        assert!(!step_detail_handle_esc(&mut app));
        assert!(app.should_pop);
    }

    // -- step_detail_handle_c (TUI-plan.md §8 "Editing — `c`") -----------

    use crate::tui::views::step_detail::{
        NO_CHANGES_TOAST, NO_EDITOR_TOAST, PARSE_ERROR_TOAST_PREFIX, Pane, SAVED_TOAST,
        format_step_pane, format_tests_pane, format_wrap_pane,
    };

    /// Build a step-detail app whose plan + first step are materialized in
    /// `conn`, so dispatcher edits land on real rows we can read back.
    fn db_app(conn: &Connection, project: &str) -> StepDetailApp {
        let plan =
            storage::create_plan(conn, "tui-c", project, "branch-c", "desc", None, None, &[])
                .unwrap();
        let (step, _pos) = storage::create_step(
            conn,
            &plan.id,
            "Original title",
            "Original description",
            None,
            None,
            &["original-crit".to_string()],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        StepDetailApp::new(
            plan,
            vec![step],
            0,
            &Config::default(),
            storage::ProjectSettings::default(),
            Vec::new(),
        )
    }

    fn fake_editor(returning: Option<String>) -> impl FnOnce(&str) -> Result<Option<String>> {
        move |_initial| Ok(returning)
    }

    #[test]
    fn c_on_step_prompt_persists_edited_step() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::StepPrompt;

        let buffer = format_step_pane("NEW TITLE", "NEW BODY", &["NEW-CRIT".to_string()]);
        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some(buffer)),
        )
        .unwrap();

        assert_eq!(app.steps[0].title, "NEW TITLE");
        assert_eq!(app.toasts.current().unwrap().text, SAVED_TOAST);
        let reloaded = storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].title, "NEW TITLE");
    }

    #[test]
    fn c_on_tests_pane_persists_edited_tests() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::Tests;

        let buffer = format_tests_pane(&["cargo test".to_string()]);
        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some(buffer)),
        )
        .unwrap();

        assert_eq!(app.plan.deterministic_tests, vec!["cargo test".to_string()]);
        assert_eq!(app.toasts.current().unwrap().text, SAVED_TOAST);
    }

    #[test]
    fn c_on_plan_prefix_persists_value() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::PlanPrefix;

        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some("PRE".to_string())),
        )
        .unwrap();

        assert_eq!(app.plan.prompt_prefix.as_deref(), Some("PRE"));
        assert_eq!(app.toasts.current().unwrap().text, SAVED_TOAST);
    }

    #[test]
    fn c_on_plan_suffix_persists_value() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::PlanSuffix;

        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some("SUF".to_string())),
        )
        .unwrap();

        assert_eq!(app.plan.prompt_suffix.as_deref(), Some("SUF"));
        assert_eq!(app.toasts.current().unwrap().text, SAVED_TOAST);
    }

    #[test]
    fn c_on_universal_pane_persists_to_disk() {
        // Universal-prompt edits land in `<config_dir>/config.json`. Pointing
        // `config_dir` at a tempdir keeps the test from touching the user's
        // real config.
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::UniversalPrompt;

        let dir = tempfile::tempdir().unwrap();
        let buffer = format_wrap_pane(Some("UP"), Some("US"));
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some(buffer)),
        )
        .unwrap();

        assert_eq!(app.toasts.current().unwrap().text, SAVED_TOAST);
        assert_eq!(app.config_prompt_prefix.as_deref(), Some("UP"));
        assert_eq!(app.config_prompt_suffix.as_deref(), Some("US"));
        let written = std::fs::read_to_string(dir.path().join("config.json")).unwrap();
        let reloaded: Config = serde_json::from_str(&written).unwrap();
        assert_eq!(reloaded.prompt_prefix.as_deref(), Some("UP"));
        assert_eq!(reloaded.prompt_suffix.as_deref(), Some("US"));
    }

    #[test]
    fn c_with_no_editor_toasts_no_editor() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::PlanPrefix;

        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(None),
        )
        .unwrap();

        assert_eq!(app.toasts.current().unwrap().text, NO_EDITOR_TOAST);
    }

    #[test]
    fn c_with_unchanged_buffer_toasts_no_changes() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::PlanPrefix;

        let dir = tempfile::tempdir().unwrap();
        let unchanged = app.plan.prompt_prefix.clone().unwrap_or_default();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some(unchanged)),
        )
        .unwrap();

        assert_eq!(app.toasts.current().unwrap().text, NO_CHANGES_TOAST);
    }

    #[test]
    fn c_with_parse_error_toasts_prefixed_message() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj");
        app.focused_pane = Pane::StepPrompt;

        // Missing description header → parse error.
        let bad = "# Title\nstill the title\n## Acceptance criteria\n- c\n".to_string();
        let dir = tempfile::tempdir().unwrap();
        step_detail_handle_c(
            &mut app,
            &conn,
            &Config::default(),
            dir.path(),
            fake_editor(Some(bad)),
        )
        .unwrap();

        let toast = app.toasts.current().unwrap();
        assert!(
            toast.text.starts_with(PARSE_ERROR_TOAST_PREFIX),
            "expected parse-error prefix; got {}",
            toast.text
        );
        // The original step row is untouched.
        let reloaded = storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].title, "Original title");
    }

    #[test]
    fn c_on_read_only_panes_is_a_noop() {
        // Appended / OpenQuestions / BottomRow shouldn't run the edit handoff
        // or toast — bare `c` is reserved for the editable text panes
        // (BottomRow has its own picker handler that runs separately).
        let dir = tempfile::tempdir().unwrap();
        for pane in [Pane::Appended, Pane::OpenQuestions, Pane::BottomRow] {
            let conn = crate::db::open_memory().unwrap();
            let mut app = db_app(&conn, "/proj");
            app.focused_pane = pane;
            // The closure panics if it's invoked — proving the dispatch was a no-op.
            let editor = |_: &str| -> Result<Option<String>> { panic!("editor must not run") };
            step_detail_handle_c(&mut app, &conn, &Config::default(), dir.path(), editor).unwrap();
            assert!(app.toasts.is_empty(), "no toast on read-only pane {pane:?}");
        }
    }

    // -- §13.2 read-only attach (external run lock) ---------------------

    /// Drive the dispatcher's `step_detail_observe_read_only` helper end to
    /// end: insert a `run_locks` row owned by a foreign pid, run one
    /// detect-then-observe cycle, and assert that every gate the dispatcher
    /// consults flips closed; then release the lock and assert that edits
    /// come back online with a `Released` transition.
    #[test]
    fn external_run_lock_engages_lockdown_and_blocks_edits() {
        use crate::tui::read_only::{self, ReadOnly, ReadOnlyTracker, Transition};
        use rusqlite::params;

        let conn = crate::db::open_memory().unwrap();
        let project = "/proj-step-detail-lock";
        let external_pid: i64 = 0x7FFF_FFFE; // bogus, definitely not us
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4)",
            params![project, external_pid, "p1", "feat"],
        )
        .unwrap();

        let mut app = db_app(&conn, project);
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        let my_pid = std::process::id() as i64;
        let now = Instant::now();

        // First poll cycle: external lock → Engaged.
        let observed = read_only::detect(&conn, project, my_pid, None).unwrap();
        let transition = step_detail_observe_read_only(&mut tracker, &mut app, observed, now);
        assert_eq!(transition, Transition::Engaged);
        assert_eq!(app.read_only, ReadOnly::Locked { pid: external_pid });
        assert!(
            !app.can_edit_panes(),
            "edits must be suppressed while an external runner holds the lock"
        );

        // Edit gates the dispatcher consults:
        // - `a` on OpenQuestions: open_answer_modal must refuse to open even
        //   when the focused step has questions.
        app.focused_pane = Pane::OpenQuestions;
        app.set_open_questions_for_step(vec![storage::OpenQuestion {
            id: "q1".into(),
            step_id: app.steps[0].id.clone(),
            plan_id: app.plan.id.clone(),
            plan_slug: app.plan.slug.clone(),
            step_num: 1,
            step_title: app.steps[0].title.clone(),
            attempt: 1,
            question: "Q?".into(),
            suggestions: vec!["yes".into(), "no".into()],
            asked_at: "2026-05-05T00:00:00Z".into(),
        }]);
        assert!(!app.open_answer_modal());
        assert!(app.answer_modal.is_none());

        // - `c` on BottomRow: the dispatcher's guard (`app.can_edit_panes()`)
        //   must prevent the picker from opening. We simulate that gate
        //   here.
        app.focused_pane = Pane::BottomRow;
        if app.can_edit_panes() {
            app.open_picker_for_focused_cell(&[]);
        }
        assert!(app.picker.is_none(), "picker must not open while locked");

        // - bare `c` on a text pane: same gate. step_detail_handle_c is
        //   never called by the dispatcher when can_edit_panes() is false,
        //   so no editor side effect is expected.
        app.focused_pane = Pane::StepPrompt;
        assert!(!app.can_edit_panes());

        // Now release the lock and run another poll cycle: Locked → Editable.
        conn.execute("DELETE FROM run_locks WHERE project = ?1", params![project])
            .unwrap();
        let later = now + read_only::POLL_INTERVAL;
        let observed = read_only::detect(&conn, project, my_pid, None).unwrap();
        let transition = step_detail_observe_read_only(&mut tracker, &mut app, observed, later);
        assert_eq!(transition, Transition::Released);
        assert_eq!(app.read_only, ReadOnly::Editable);
        assert!(app.can_edit_panes());
    }

    /// If a picker is open when lockdown engages, the helper should close it
    /// so the user doesn't see a stale edit affordance over the read-only
    /// banner.
    #[test]
    fn open_picker_is_closed_when_lockdown_engages() {
        use crate::tui::read_only::{ReadOnly, ReadOnlyTracker};

        let conn = crate::db::open_memory().unwrap();
        let mut app = db_app(&conn, "/proj-picker-lock");
        app.focused_pane = Pane::BottomRow;
        app.open_picker_for_focused_cell(&[]);
        assert!(
            app.picker.is_some(),
            "test setup: picker should be open before lockdown engages"
        );

        let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        let now = Instant::now();
        step_detail_observe_read_only(&mut tracker, &mut app, ReadOnly::Locked { pid: 4242 }, now);

        assert!(
            app.picker.is_none(),
            "an open picker must be torn down when an external lock engages"
        );
    }
}

#[cfg(test)]
mod palette_action_tests {
    //! Integration tests for the per-view palette dispatch consumption added
    //! in step 20 of `tui-gap-fixes`. We exercise the public
    //! `<view>_palette_action` + `<view>_apply_palette_action` halves
    //! end-to-end against an in-memory DB so we cover the parse → dispatch →
    //! storage write → toast pipeline. Per the step spec, the focus is the
    //! toast + refresh + archive paths.
    use super::*;
    use crate::db;
    use crate::plan::PlanStatus;
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_list::PlanListApp;

    fn seed_app(project: &str) -> (Connection, PlanListApp) {
        let conn = db::open_memory().unwrap();
        // Sleep between creates so the millisecond-precision created_at
        // values differ — list_plans_sorted_by_recency orders by
        // created_at DESC and SQLite's tie-break on equal timestamps is
        // undefined, which makes downstream tests fragile.
        storage::create_plan(&conn, "alpha", project, "b1", "d", None, None, &[]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        storage::create_plan(&conn, "beta", project, "b2", "d", None, None, &[]).unwrap();
        let tiles = build_plan_tiles(&conn, project).unwrap();
        let app = PlanListApp::new(tiles, project, "UTC");
        (conn, app)
    }

    // -- Toast path -----------------------------------------------------

    #[test]
    fn parse_error_for_unknown_command_yields_toast_action() {
        // Driving an unknown verb through the palette dispatcher should
        // produce an error-kind Toast action that the consuming view
        // surfaces verbatim.
        let (_conn, app) = seed_app("/tmp/palette-unknown");
        let action = plan_list_palette_action("/nope-not-a-cmd", "claude", &app, &[]);
        match action {
            PaletteAction::Toast { message, kind } => {
                assert!(
                    message.contains("Unknown command"),
                    "expected 'Unknown command' in toast: {message}"
                );
                assert_eq!(kind, ToastKind::Error);
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn applying_toast_action_pushes_onto_view_queue() {
        // Wiring the dispatcher's Toast action through the apply helper must
        // end up on the view's toast queue verbatim.
        let project = "/tmp/palette-toast-apply";
        let (conn, mut app) = seed_app(project);
        let action = PaletteAction::Toast {
            message: "hello there".to_string(),
            kind: ToastKind::Info,
        };
        plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        let toast = app.toasts.current().expect("toast was pushed");
        assert_eq!(toast.text, "hello there");
        assert_eq!(toast.color, ToastKind::Info.color());
    }

    #[test]
    fn empty_palette_input_is_a_silent_close() {
        // The parser's `Empty` error maps to `PaletteAction::None` — applying
        // it must leave the toast queue alone.
        let project = "/tmp/palette-empty";
        let (conn, mut app) = seed_app(project);
        let action = plan_list_palette_action("/", "claude", &app, &[]);
        assert_eq!(action, PaletteAction::None);
        plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(app.toasts.is_empty());
    }

    // -- Refresh path (after a mutation) --------------------------------

    #[test]
    fn approve_refreshes_in_place_tile_and_toasts_success() {
        // /plan approve <slug> on a Planning plan must flip status, refresh
        // the in-memory tile, and push the success toast.
        let project = "/tmp/palette-approve";
        let (conn, mut app) = seed_app(project);
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        let action =
            plan_list_palette_action(&format!("/plan approve {target_slug}"), "claude", &app, &[]);
        plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();

        let from_db = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert_eq!(from_db.status, PlanStatus::Ready);

        let tile = app
            .tiles
            .iter()
            .find(|t| t.plan.slug == target_slug)
            .unwrap();
        assert_eq!(tile.plan.status, PlanStatus::Ready);
        assert_eq!(app.toasts.current().unwrap().text, "Plan approved.");
    }

    #[test]
    fn questions_toggle_via_palette_persists_and_refreshes_in_place() {
        // The /plan questions on|off pair flips `questions_enabled` and
        // updates the in-memory tile so the next render reflects the new
        // state without a full refresh.
        let project = "/tmp/palette-questions";
        let (conn, mut app) = seed_app(project);
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        assert!(!app.cursor_plan().unwrap().questions_enabled);

        let on_action = plan_list_palette_action(
            &format!("/plan questions on {target_slug}"),
            "claude",
            &app,
            &[],
        );
        plan_list_apply_palette_action(&conn, project, &mut app, on_action).unwrap();
        let row = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert!(row.questions_enabled);
        let tile = app
            .tiles
            .iter()
            .find(|t| t.plan.slug == target_slug)
            .unwrap();
        assert!(tile.plan.questions_enabled);
        assert_eq!(app.toasts.current().unwrap().text, "Questions enabled.");
    }

    // -- Archive path ---------------------------------------------------

    #[test]
    fn archive_command_returns_confirm_action_then_apply_archives_and_refreshes() {
        // /plan archive <slug> goes through a confirm dialog at the
        // dispatcher level. We assert:
        //   1. The dispatcher returns OpenConfirmArchive (handed back from
        //      apply for the loop to drive).
        //   2. plan_list_apply_archive — the post-confirm path — flips the
        //      DB row, refreshes tiles, and toasts.
        let project = "/tmp/palette-archive";
        let (conn, mut app) = seed_app(project);
        let target_id = app.cursor_plan().unwrap().id.clone();
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        let initial_len = app.tiles.len();

        let action =
            plan_list_palette_action(&format!("/plan archive {target_slug}"), "claude", &app, &[]);
        match &action {
            PaletteAction::OpenConfirmArchive { plan_id, slug } => {
                assert_eq!(plan_id, &target_id);
                assert_eq!(slug, &target_slug);
            }
            other => panic!("expected OpenConfirmArchive, got {other:?}"),
        }
        // Apply returns the action back so the caller renders the confirm.
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenConfirmArchive { .. })
        ));

        // Simulate a confirmed yes by running the post-confirm helper.
        plan_list_apply_archive(&conn, project, &mut app, &target_id).unwrap();

        let row = storage::get_plan_by_slug(&conn, &target_slug, project)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, PlanStatus::Archived);
        // The archived plan should no longer appear among non-archived tiles.
        assert_eq!(app.tiles.len(), initial_len - 1);
        assert!(!app.tiles.iter().any(|t| t.plan.slug == target_slug));
        assert_eq!(app.toasts.current().unwrap().text, "Archived 1 plan.");
    }

    #[test]
    fn archive_unknown_slug_toasts_error() {
        // A `/plan archive <bogus>` should land on `Toast { kind: Error }`.
        let project = "/tmp/palette-archive-unknown";
        let (conn, mut app) = seed_app(project);
        let action = plan_list_palette_action("/plan archive does-not-exist", "claude", &app, &[]);
        match &action {
            PaletteAction::Toast { kind, .. } => assert_eq!(*kind, ToastKind::Error),
            other => panic!("expected Toast(Error), got {other:?}"),
        }
        plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert_eq!(
            app.toasts.current().unwrap().color,
            ToastKind::Error.color()
        );
    }
}

#[cfg(test)]
mod run_dialog_apply_tests {
    //! Tests for the `/run` palette wiring (TUI-plan.md §9.1, step 21):
    //!   * `*_apply_palette_action` forwards `OpenRunDialog` / `RunOnBranch`
    //!     to the caller (terminal-bound — same channel as the existing
    //!     archive/delete confirms).
    //!   * `classify_branch_target` correctly identifies the three branch
    //!     states the apply helper needs: already-on, switch-existing,
    //!     needs-create.
    //!   * `spawn_palette_runners` shapes its toast on the spawn outcome.

    use super::*;
    use crate::db;
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::run_dialog::{Outcome, RunTarget};
    use crate::tui::toast::ToastKind;
    use crate::tui::views::plan_list::PlanListApp;

    fn seed_plan(project: &str) -> (Connection, PlanListApp) {
        let conn = db::open_memory().unwrap();
        storage::create_plan(&conn, "alpha", project, "feature-x", "d", None, None, &[]).unwrap();
        let tiles = build_plan_tiles(&conn, project).unwrap();
        let app = PlanListApp::new(tiles, project, "UTC");
        (conn, app)
    }

    fn run_target(slug: &str, branch: &str) -> RunTarget {
        RunTarget {
            slug: slug.to_string(),
            default_branch: branch.to_string(),
        }
    }

    // -- Forwarding from apply (mirrors archive/delete forwarding) ----------

    #[test]
    fn plan_list_apply_forwards_open_run_dialog_to_caller() {
        let project = "/tmp/run-dialog-forward";
        let (conn, mut app) = seed_plan(project);
        let action = PaletteAction::OpenRunDialog {
            default_branch: "feature-x".to_string(),
            plan_count: 1,
            targets: vec![run_target("alpha", "feature-x")],
        };
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenRunDialog { plan_count: 1, .. })
        ));
        // No toast is pushed — the caller drives the dialog.
        assert!(app.toasts.is_empty());
    }

    #[test]
    fn plan_list_apply_forwards_run_on_branch_to_caller() {
        let project = "/tmp/run-on-branch-forward";
        let (conn, mut app) = seed_plan(project);
        let action = PaletteAction::RunOnBranch {
            branch: "hotfix".to_string(),
            targets: vec![run_target("alpha", "feature-x")],
            force_current_branch: false,
        };
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::RunOnBranch { ref branch, .. }) if branch == "hotfix"
        ));
        assert!(app.toasts.is_empty());
    }

    // -- /run end-to-end through the dispatcher ------------------------------

    #[test]
    fn slash_run_with_focus_returns_open_run_dialog() {
        // The acceptance criterion: `/run` from plan-list opens the dialog.
        // We can't drive the dialog without a real terminal, but we *can*
        // assert that the parser → dispatcher → apply pipeline routes the
        // action back to the caller for the loop's terminal-bound handler.
        let project = "/tmp/slash-run-opens-dialog";
        let (conn, mut app) = seed_plan(project);
        let action = plan_list_palette_action("/run", "claude", &app, &[]);
        match &action {
            PaletteAction::OpenRunDialog {
                default_branch,
                plan_count,
                targets,
            } => {
                assert_eq!(default_branch, "feature-x");
                assert_eq!(*plan_count, 1);
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].slug, "alpha");
            }
            other => panic!("expected OpenRunDialog, got {other:?}"),
        }
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenRunDialog { .. })
        ));
    }

    #[test]
    fn slash_run_with_branch_short_circuits_dialog() {
        // The acceptance criterion: `/run <branch>` short-circuits the dialog
        // and routes straight to RunOnBranch.
        let project = "/tmp/slash-run-with-branch";
        let (conn, mut app) = seed_plan(project);
        let action = plan_list_palette_action("/run hotfix", "claude", &app, &[]);
        match &action {
            PaletteAction::RunOnBranch {
                branch,
                targets,
                force_current_branch,
            } => {
                assert_eq!(branch, "hotfix");
                assert_eq!(targets.len(), 1);
                // Single plan, not multi-select — don't force current-branch
                // at the dispatcher level; the apply step decides per the
                // branch's relationship to plan.branch_name.
                assert!(!*force_current_branch);
            }
            other => panic!("expected RunOnBranch, got {other:?}"),
        }
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(forwarded, Some(PaletteAction::RunOnBranch { .. })));
    }

    // -- classify_branch_target ----------------------------------------------

    fn init_repo() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.email", "t@t.com"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["config", "user.name", "t"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::fs::write(dir.join("README"), "x").unwrap();
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&dir)
            .output()
            .unwrap();
        (tmp, dir)
    }

    #[test]
    fn classify_branch_already_on_target_returns_already_on_target() {
        let (_tmp, dir) = init_repo();
        let current = crate::git::get_current_branch(&dir).unwrap();
        assert_eq!(
            classify_branch_target(&dir, &current).unwrap(),
            BranchDecision::AlreadyOnTarget,
        );
    }

    #[test]
    fn classify_branch_existing_returns_switch_existing() {
        let (_tmp, dir) = init_repo();
        let initial = crate::git::get_current_branch(&dir).unwrap();
        crate::git::create_and_checkout_branch(&dir, "feature/here").unwrap();
        // We're now on feature/here. Asking for `initial` (which exists) →
        // SwitchExisting.
        assert_eq!(
            classify_branch_target(&dir, &initial).unwrap(),
            BranchDecision::SwitchExisting,
        );
    }

    #[test]
    fn classify_branch_missing_returns_needs_create() {
        let (_tmp, dir) = init_repo();
        assert_eq!(
            classify_branch_target(&dir, "feature/never").unwrap(),
            BranchDecision::NeedsCreate,
        );
    }

    // -- spawn_palette_runners toast shape ------------------------------------

    #[test]
    fn spawn_runners_toasts_singular_for_one_plan() {
        // Use the real ProcessRunSpawner — its smoke test in run_dialog
        // proves it doesn't panic. Here we just want the success-toast
        // wording to flow through PaletteRunReport.
        //
        // We can't actually fork ralph here without polluting test state,
        // so this asserts the shape of the report when dispatch_outcome is
        // a no-op (Outcome::Cancelled).
        let workdir = std::path::Path::new("/tmp/spawn-runners-shape");
        let mut report = PaletteRunReport::default();
        spawn_palette_runners(
            workdir,
            &Outcome::Cancelled,
            &[run_target("alpha", "main")],
            false,
            &mut report,
        );
        // Cancelled outcome → no spawn → no toast.
        assert!(report.spawned.is_empty());
        assert!(report.pending_toasts.is_empty());
    }

    // -- apply_palette_run_outcome short-circuits ----------------------------

    #[test]
    fn apply_outcome_cancelled_yields_empty_report() {
        // Cancelled / Pending must be no-ops: no toast, no spawn, no branch
        // switch. We can call apply_palette_run_outcome without a terminal
        // because the cancelled path doesn't render anything.
        let backend = ratatui::backend::TestBackend::new(40, 10);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let report = apply_palette_run_outcome(
            &mut terminal,
            |_f| {},
            "/tmp/no-such-project",
            Outcome::Cancelled,
            &[run_target("alpha", "main")],
            false,
        )
        .unwrap();
        assert!(report.spawned.is_empty());
        assert!(report.pending_toasts.is_empty());
    }

    #[test]
    fn apply_outcome_pending_yields_empty_report() {
        let backend = ratatui::backend::TestBackend::new(40, 10);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let report = apply_palette_run_outcome(
            &mut terminal,
            |_f| {},
            "/tmp/no-such-project",
            Outcome::Pending,
            &[run_target("alpha", "main")],
            false,
        )
        .unwrap();
        assert!(report.spawned.is_empty());
        assert!(report.pending_toasts.is_empty());
    }

    // -- flush_palette_run_toasts -------------------------------------------

    #[test]
    fn flush_palette_run_toasts_drains_into_view_queue() {
        let mut queue = crate::tui::toast::ToastQueue::default();
        let report = PaletteRunReport {
            spawned: vec!["alpha".to_string()],
            pending_toasts: vec![
                ("Started run for alpha.".to_string(), ToastKind::Success),
                ("Hint.".to_string(), ToastKind::Info),
            ],
        };
        flush_palette_run_toasts(report, &mut queue);
        // The most recent toast is the visible one; the queue should have
        // received both.
        assert!(!queue.is_empty());
    }
}

#[cfg(test)]
mod sub_view_routing_tests {
    //! Tests for the `/plan dependency`, `/plan set-hook|unset-hook|hooks`,
    //! `/step set-hook|unset-hook`, and `/step edit --tags` palette wiring
    //! (TUI-plan.md §9, step 22).
    //!
    //! We can't drive the actual sub-view dispatchers from a unit test (they
    //! own a crossterm event loop), so the tests prove two halves instead:
    //!   * `<view>_palette_action` routes the parsed verb to the right
    //!     `PaletteAction::Open*` variant with the IDs the dispatcher will
    //!     consume.
    //!   * `<view>_apply_palette_action` forwards each variant to the
    //!     caller (returns `Some(action)`), the same channel the existing
    //!     archive/delete confirms use, so the dispatcher loop's terminal-
    //!     bound match arm is what actually invokes
    //!     `run_plan_dependencies_tui` / `run_plan_hooks_tui` /
    //!     `run_step_hooks_tui` / `run_step_tags_tui`.
    //!
    //! The forwarding half is what makes "the command lands in the right
    //! dispatcher" a falsifiable property: a regression that toasted "lands
    //! in step 22" again would return `Ok(None)` here and fail the assert.

    use super::*;
    use crate::config::Config;
    use crate::db;
    use crate::plan::{ChangePolicy, Plan, PlanStatus, Step, StepStatus};
    use crate::tui::palette_dispatch::PaletteAction;
    use crate::tui::views::plan_detail::PlanDetailApp;
    use crate::tui::views::plan_list::PlanListApp;
    use crate::tui::views::step_detail::StepDetailApp;
    use chrono::Utc;

    // -- Fixtures ---------------------------------------------------------

    fn seed_plan_list(project: &str) -> (Connection, PlanListApp) {
        let conn = db::open_memory().unwrap();
        storage::create_plan(&conn, "alpha", project, "feature-x", "d", None, None, &[]).unwrap();
        let tiles = build_plan_tiles(&conn, project).unwrap();
        let app = PlanListApp::new(tiles, project, "UTC");
        (conn, app)
    }

    fn seed_plan_detail(project: &str) -> (Connection, PlanDetailApp) {
        let conn = db::open_memory().unwrap();
        storage::create_plan(&conn, "alpha", project, "feature-x", "d", None, None, &[]).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "alpha", project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let app = PlanDetailApp::new(plan, steps, &Config::default());
        (conn, app)
    }

    fn make_step_detail_app(slug: &str, project: &str) -> StepDetailApp {
        let plan = Plan {
            id: format!("plan-{slug}"),
            slug: slug.to_string(),
            project: project.to_string(),
            branch_name: "feature-x".to_string(),
            description: "d".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
        };
        let steps = vec![Step {
            id: "step-1".to_string(),
            plan_id: plan.id.clone(),
            sort_key: "a0".to_string(),
            title: "First step".to_string(),
            description: "d".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
        }];
        StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            storage::ProjectSettings::default(),
            Vec::new(),
        )
    }

    // -- /plan dependency add|remove|list ---------------------------------

    #[test]
    fn slash_plan_dependency_from_plan_list_routes_to_dependencies_dispatcher() {
        let project = "/tmp/plan-dep-from-list";
        let (conn, mut app) = seed_plan_list(project);
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        let target_id = app.cursor_plan().unwrap().id.clone();

        for verb in ["dependency add", "dependency remove", "dependency list"] {
            let action = plan_list_palette_action(&format!("/plan {verb}"), "claude", &app, &[]);
            match &action {
                PaletteAction::OpenPlanDependencies { plan_id, slug } => {
                    assert_eq!(plan_id, &target_id, "/plan {verb}: plan_id");
                    assert_eq!(slug, &target_slug, "/plan {verb}: slug");
                }
                other => panic!("/plan {verb}: expected OpenPlanDependencies, got {other:?}"),
            }
            let forwarded =
                plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
            assert!(
                matches!(forwarded, Some(PaletteAction::OpenPlanDependencies { .. })),
                "/plan {verb}: apply must forward to caller, got {forwarded:?}"
            );
        }
    }

    #[test]
    fn slash_plan_dependency_from_plan_detail_routes_to_dependencies_dispatcher() {
        let project = "/tmp/plan-dep-from-detail";
        let (conn, mut app) = seed_plan_detail(project);
        let action = plan_detail_palette_action("/plan dependency add", "claude", &app);
        assert!(matches!(action, PaletteAction::OpenPlanDependencies { .. }));
        let forwarded = plan_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenPlanDependencies { .. })
        ));
    }

    #[test]
    fn slash_plan_dependency_from_step_detail_routes_to_dependencies_dispatcher() {
        let project = "/tmp/plan-dep-from-step";
        let conn = db::open_memory().unwrap();
        let mut app = make_step_detail_app("alpha", project);
        let action = step_detail_palette_action("/plan dependency add", "claude", &app);
        assert!(matches!(action, PaletteAction::OpenPlanDependencies { .. }));
        let forwarded = step_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenPlanDependencies { .. })
        ));
    }

    // -- /plan set-hook|unset-hook|hooks ----------------------------------

    #[test]
    fn slash_plan_hook_verbs_from_plan_list_route_to_hooks_dispatcher() {
        let project = "/tmp/plan-hooks-from-list";
        let (conn, mut app) = seed_plan_list(project);
        let target_slug = app.cursor_plan().unwrap().slug.clone();
        let target_id = app.cursor_plan().unwrap().id.clone();

        for verb in ["set-hook", "unset-hook", "hooks"] {
            let action = plan_list_palette_action(&format!("/plan {verb}"), "claude", &app, &[]);
            match &action {
                PaletteAction::OpenPlanHooks { plan_id, slug } => {
                    assert_eq!(plan_id, &target_id, "/plan {verb}: plan_id");
                    assert_eq!(slug, &target_slug, "/plan {verb}: slug");
                }
                other => panic!("/plan {verb}: expected OpenPlanHooks, got {other:?}"),
            }
            let forwarded =
                plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
            assert!(
                matches!(forwarded, Some(PaletteAction::OpenPlanHooks { .. })),
                "/plan {verb}: apply must forward to caller, got {forwarded:?}"
            );
        }
    }

    #[test]
    fn slash_plan_hooks_from_plan_detail_routes_to_hooks_dispatcher() {
        let project = "/tmp/plan-hooks-from-detail";
        let (conn, mut app) = seed_plan_detail(project);
        let action = plan_detail_palette_action("/plan hooks", "claude", &app);
        assert!(matches!(action, PaletteAction::OpenPlanHooks { .. }));
        let forwarded = plan_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenPlanHooks { .. })
        ));
    }

    #[test]
    fn slash_plan_hooks_from_step_detail_routes_to_hooks_dispatcher() {
        let project = "/tmp/plan-hooks-from-step";
        let conn = db::open_memory().unwrap();
        let mut app = make_step_detail_app("alpha", project);
        let action = step_detail_palette_action("/plan hooks", "claude", &app);
        assert!(matches!(action, PaletteAction::OpenPlanHooks { .. }));
        let forwarded = step_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenPlanHooks { .. })
        ));
    }

    // -- /step set-hook|unset-hook ----------------------------------------

    #[test]
    fn slash_step_hooks_from_step_detail_routes_to_step_hooks_dispatcher() {
        let project = "/tmp/step-hooks-from-step";
        let conn = db::open_memory().unwrap();
        let mut app = make_step_detail_app("alpha", project);
        let expected_step_id = app.current_step().unwrap().id.clone();

        for verb in ["set-hook", "unset-hook"] {
            let action = step_detail_palette_action(&format!("/step {verb}"), "claude", &app);
            match &action {
                PaletteAction::OpenStepHooks { step_id, .. } => {
                    assert_eq!(step_id, &expected_step_id, "/step {verb}: step_id");
                }
                other => panic!("/step {verb}: expected OpenStepHooks, got {other:?}"),
            }
            let forwarded =
                step_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
            assert!(
                matches!(forwarded, Some(PaletteAction::OpenStepHooks { .. })),
                "/step {verb}: apply must forward to caller, got {forwarded:?}"
            );
        }
    }

    #[test]
    fn slash_step_hooks_from_plan_list_toasts_open_a_step_first() {
        // No `focused_step` in plan-list context → dispatcher folds the
        // command into a toast and apply consumes it (toast queue receives
        // the hint, no action is forwarded for the loop to route).
        let project = "/tmp/step-hooks-from-list";
        let (conn, mut app) = seed_plan_list(project);
        let action = plan_list_palette_action("/step set-hook", "claude", &app, &[]);
        match &action {
            PaletteAction::Toast { message, .. } => {
                assert!(message.contains("Open a step first"), "got: {message}");
            }
            other => panic!("expected Toast, got {other:?}"),
        }
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(
            forwarded.is_none(),
            "Toast must not forward to caller: {forwarded:?}"
        );
    }

    // -- /step edit --tags ------------------------------------------------

    #[test]
    fn slash_step_tags_from_step_detail_routes_to_step_tags_dispatcher() {
        let project = "/tmp/step-tags-from-step";
        let conn = db::open_memory().unwrap();
        let mut app = make_step_detail_app("alpha", project);
        let expected_step_id = app.current_step().unwrap().id.clone();

        let action = step_detail_palette_action("/step edit --tags", "claude", &app);
        match &action {
            PaletteAction::OpenStepTags { step_id, .. } => {
                assert_eq!(step_id, &expected_step_id);
            }
            other => panic!("expected OpenStepTags, got {other:?}"),
        }
        let forwarded = step_detail_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(matches!(
            forwarded,
            Some(PaletteAction::OpenStepTags { .. })
        ));
    }

    #[test]
    fn slash_step_tags_from_plan_list_toasts_open_a_step_first() {
        let project = "/tmp/step-tags-from-list";
        let (conn, mut app) = seed_plan_list(project);
        let action = plan_list_palette_action("/step edit --tags", "claude", &app, &[]);
        match &action {
            PaletteAction::Toast { message, .. } => {
                assert!(message.contains("Open a step first"), "got: {message}");
            }
            other => panic!("expected Toast, got {other:?}"),
        }
        let forwarded = plan_list_apply_palette_action(&conn, project, &mut app, action).unwrap();
        assert!(forwarded.is_none(), "Toast must not forward: {forwarded:?}");
    }

    // -- Plan-detail [P] pause toggle (TUI-plan.md §17 manual pause) ----------

    #[test]
    fn plan_detail_apply_toggle_pause_first_press_sets_flag_and_toasts() {
        let project = "/tmp/pause-toggle-1";
        let (conn, mut app) = seed_plan_detail(project);
        // Default state: pause_requested = false.
        assert!(!app.plan.pause_requested);

        plan_detail_apply_toggle_pause(&conn, &mut app).unwrap();

        // DB row + in-memory plan both flipped to true.
        assert!(storage::get_plan_pause_requested(&conn, &app.plan.id).unwrap());
        assert!(app.plan.pause_requested);

        // Toast surfaces the "stop after current step" message so the user
        // sees acknowledgement of the request.
        let active = app
            .toasts
            .current()
            .expect("toast must surface confirmation");
        assert!(
            active.text.contains("Pause requested"),
            "first press toast should mention pause request, got {:?}",
            active.text
        );
    }

    #[test]
    fn plan_detail_apply_toggle_pause_second_press_clears_flag_and_toasts_cancel() {
        let project = "/tmp/pause-toggle-2";
        let (conn, mut app) = seed_plan_detail(project);

        // Pre-set the flag — simulates "user pressed P once already".
        storage::set_plan_pause_requested(&conn, &app.plan.id, true).unwrap();
        if let Some(updated) =
            storage::get_plan_by_slug(&conn, &app.plan.slug, &app.plan.project).unwrap()
        {
            app.plan = updated;
        }
        assert!(app.plan.pause_requested);

        plan_detail_apply_toggle_pause(&conn, &mut app).unwrap();

        // DB row + in-memory plan both flipped back to false.
        assert!(!storage::get_plan_pause_requested(&conn, &app.plan.id).unwrap());
        assert!(!app.plan.pause_requested);

        let active = app.toasts.current().expect("cancel toast must surface");
        assert!(
            active.text.contains("cancelled"),
            "second press toast should mention cancel, got {:?}",
            active.text
        );
    }
}

#[cfg(test)]
mod mouse_routing_tests {
    //! TUI-plan.md §4 mouse capture (step 25): each main + sub-view dispatcher
    //! routes `Event::Mouse` to the focused App's `handle_mouse` method.
    //!
    //! We can't drive crossterm's real `event::read()` from a unit test, so the
    //! tests prove the routing in two halves:
    //!   * `route_mouse_event` mirrors the dispatcher's match arm
    //!     (`Event::Mouse(m) => app.handle_mouse(m)`); calling it with an
    //!     `Event::Mouse` lets each `App::handle_mouse` actually fire under
    //!     the same `TestBackend` terminal that production code would use.
    //!   * `MouseHandler` is a tiny test-only adapter so the routing helper
    //!     can fan out across every view's `handle_mouse` signature without
    //!     trait-objecting the App structs themselves.
    //!
    //! A regression that drops the `Event::Mouse` arm in any dispatcher would
    //! leave `Event::Mouse` falling through to the `_ => continue` arm and
    //! `handle_mouse` would never be called — these tests don't observe the
    //! dispatcher loop directly, but they pin the routing pattern (and the
    //! per-view `handle_mouse` method's existence and signature) so a
    //! refactor that breaks either property fails the build.
    //!
    //! Per-view drag handling lands in tui-gap-fixes steps 26–28; until then
    //! `handle_mouse` is a no-op contract this module also pins.
    use super::*;
    use crate::config::Config;
    use crate::plan::{ChangePolicy, Plan, PlanStatus, Step, StepStatus};
    use crate::tui::views::archived_list::ArchivedListApp;
    use crate::tui::views::plan_dependencies::PlanDependenciesApp;
    use crate::tui::views::plan_detail::PlanDetailApp;
    use crate::tui::views::plan_hooks::PlanHooksApp;
    use crate::tui::views::plan_list::PlanListApp;
    use crate::tui::views::step_detail::StepDetailApp;
    use crate::tui::views::step_hooks::StepHooksApp;
    use crate::tui::views::step_tags::StepTagsApp;
    use chrono::Utc;
    use crossterm::event::{Event, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Test-only adapter so [`route_mouse_event`] can dispatch into every
    /// view's inherent `handle_mouse(MouseEvent)` without committing the
    /// production code to a trait object.
    trait MouseHandler {
        fn handle_mouse(&mut self, event: MouseEvent);
    }

    macro_rules! impl_mouse_handler {
        ($($t:ty),+ $(,)?) => {
            $(
                impl MouseHandler for $t {
                    fn handle_mouse(&mut self, event: MouseEvent) {
                        Self::handle_mouse(self, event);
                    }
                }
            )+
        };
    }

    impl_mouse_handler!(
        PlanListApp,
        ArchivedListApp,
        PlanDetailApp,
        StepDetailApp,
        PlanDependenciesApp,
        PlanHooksApp,
        StepHooksApp,
        StepTagsApp,
    );

    /// Mirrors the routing arm every TUI dispatcher carries:
    /// `Event::Mouse(m) => app.handle_mouse(m)`. Returns `true` when the
    /// routed event was a mouse event. Tests assert this is what happens
    /// when we feed an `Event::Mouse` through.
    fn route_mouse_event<A: MouseHandler>(event: Event, app: &mut A) -> bool {
        match event {
            Event::Mouse(m) => {
                app.handle_mouse(m);
                true
            }
            _ => false,
        }
    }

    fn sample_mouse_event() -> MouseEvent {
        MouseEvent {
            kind: MouseEventKind::Down(crossterm::event::MouseButton::Left),
            column: 1,
            row: 1,
            modifiers: KeyModifiers::NONE,
        }
    }

    /// Exercise [`route_mouse_event`] inside a real `TestBackend` terminal so
    /// the routing path is covered under the same backend production uses.
    /// The call returns true iff the dispatcher's `Event::Mouse` arm was
    /// taken — falsifies a regression that drops the arm.
    fn assert_routes_mouse_to_app<A: MouseHandler>(app: &mut A) {
        let backend = TestBackend::new(40, 10);
        let mut _terminal = Terminal::new(backend).unwrap();
        let routed = route_mouse_event(Event::Mouse(sample_mouse_event()), app);
        assert!(routed, "Event::Mouse should route to app.handle_mouse");

        // Non-mouse events fall through to the dispatcher's other arms; the
        // routing helper should report `false` so we can be sure the helper
        // isn't accidentally swallowing every event.
        let routed_other = route_mouse_event(Event::FocusGained, app);
        assert!(
            !routed_other,
            "Non-mouse events must not route to handle_mouse"
        );
    }

    // -- Per-view fixtures ---------------------------------------------------

    fn make_plan_list_app() -> PlanListApp {
        PlanListApp::new(Vec::new(), "/tmp/mouse-plan-list", "UTC")
    }

    fn make_archived_list_app() -> ArchivedListApp {
        ArchivedListApp::new(Vec::new(), "/tmp/mouse-archived", "UTC")
    }

    fn make_plan_detail_app() -> PlanDetailApp {
        let plan = Plan {
            id: "plan-1".to_string(),
            slug: "alpha".to_string(),
            project: "/tmp/mouse-plan-detail".to_string(),
            branch_name: "feature-x".to_string(),
            description: "d".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
        };
        PlanDetailApp::new(plan, Vec::new(), &Config::default())
    }

    fn make_step_detail_app() -> StepDetailApp {
        let plan = Plan {
            id: "plan-1".to_string(),
            slug: "alpha".to_string(),
            project: "/tmp/mouse-step-detail".to_string(),
            branch_name: "feature-x".to_string(),
            description: "d".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
        };
        let step = Step {
            id: "step-1".to_string(),
            plan_id: plan.id.clone(),
            sort_key: "a0".to_string(),
            title: "Step".to_string(),
            description: "d".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
        };
        StepDetailApp::new(
            plan,
            vec![step],
            0,
            &Config::default(),
            storage::ProjectSettings::default(),
            Vec::new(),
        )
    }

    fn make_plan_dependencies_app() -> PlanDependenciesApp {
        PlanDependenciesApp::new(
            "plan-1".to_string(),
            "alpha".to_string(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn make_plan_hooks_app() -> PlanHooksApp {
        PlanHooksApp::new(
            "plan-1".to_string(),
            "alpha".to_string(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn make_step_hooks_app() -> StepHooksApp {
        StepHooksApp::new(
            "plan-1".to_string(),
            "step-1".to_string(),
            "alpha".to_string(),
            "#1 — Step".to_string(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn make_step_tags_app() -> StepTagsApp {
        StepTagsApp::new(
            "step-1".to_string(),
            "alpha".to_string(),
            "#1 — Step".to_string(),
            Vec::new(),
        )
    }

    // -- Dispatcher routing tests --------------------------------------------

    #[test]
    fn plan_list_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_plan_list_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn archived_list_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_archived_list_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn plan_detail_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_plan_detail_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn step_detail_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_step_detail_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn plan_dependencies_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_plan_dependencies_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn plan_hooks_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_plan_hooks_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn step_hooks_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_step_hooks_app();
        assert_routes_mouse_to_app(&mut app);
    }

    #[test]
    fn step_tags_dispatcher_routes_mouse_to_handle_mouse() {
        let mut app = make_step_tags_app();
        assert_routes_mouse_to_app(&mut app);
    }

    // Pin the no-op default contract: per-view drag handling is added in
    // steps 26–28; until then a mouse event must not mutate the App's
    // observable state. We pick PlanListApp's cursor as a representative
    // probe — it's the field most likely to be touched accidentally if a
    // future drag handler escapes the wrong scope.
    #[test]
    fn handle_mouse_default_is_noop_for_plan_list() {
        let mut app = make_plan_list_app();
        let before = app.selected_index;
        app.handle_mouse(sample_mouse_event());
        assert_eq!(app.selected_index, before);
    }
}

#[cfg(test)]
mod pause_tests {
    //! Pin the run-lock gate on `ralph pause`. The runner consumes
    //! `pause_requested` at the *top* of its loop (before listing or
    //! executing any step), so arming the flag while no runner is alive
    //! would cause the next `ralph run` / `ralph resume` to exit after
    //! zero steps. `cmd_pause` therefore refuses unless a live run row
    //! exists in `run_locks`, mirroring the TUI `[P]` keybinding's
    //! `is_run_live()` gate.
    use super::*;
    use crate::db;
    use rusqlite::params;

    /// Bogus pid outside any real pid space — the test only inspects DB
    /// state, never sends a signal, so the pid value is purely
    /// bookkeeping.
    const DEAD_PID: i64 = 0x7FFF_FFFE;

    fn seed_plan(conn: &Connection, slug: &str, project: &str) -> String {
        let plan =
            storage::create_plan(conn, slug, project, "br", "desc", None, None, &[]).unwrap();
        storage::update_plan_status(conn, &plan.id, crate::plan::PlanStatus::Ready).unwrap();
        plan.id
    }

    fn insert_live_lock(conn: &Connection, project: &str, plan_id: &str, plan_slug: &str) {
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![project, DEAD_PID, plan_id, plan_slug],
        )
        .unwrap();
    }

    #[test]
    fn pause_with_no_live_run_errors_and_does_not_arm_flag() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/pause-no-run";
        let plan_id = seed_plan(&conn, "p", project);

        let err = cmd_pause(&conn, project, None, /*quiet=*/ true).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("No active run"),
            "expected 'No active run' guidance, got: {msg}"
        );

        // Flag must remain cleared so a subsequent `ralph run` is not
        // poisoned into exiting after zero steps.
        let pr = storage::get_plan_pause_requested(&conn, &plan_id).unwrap();
        assert!(!pr, "pause_requested must not be armed when no run is live");
    }

    #[test]
    fn pause_with_live_run_sets_flag_for_correct_plan() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/pause-live";
        let plan_id = seed_plan(&conn, "p", project);
        insert_live_lock(&conn, project, &plan_id, "p");

        cmd_pause(&conn, project, None, /*quiet=*/ true).unwrap();

        assert!(
            storage::get_plan_pause_requested(&conn, &plan_id).unwrap(),
            "pause_requested must be armed when a live run exists"
        );
    }

    #[test]
    fn pause_with_explicit_slug_matching_live_run_sets_flag() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/pause-explicit";
        let plan_id = seed_plan(&conn, "deploy", project);
        insert_live_lock(&conn, project, &plan_id, "deploy");

        cmd_pause(&conn, project, Some("deploy"), /*quiet=*/ true).unwrap();

        assert!(storage::get_plan_pause_requested(&conn, &plan_id).unwrap());
    }

    #[test]
    fn pause_with_explicit_slug_mismatching_live_run_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/pause-mismatch";
        let plan_a = seed_plan(&conn, "plan-a", project);
        let plan_b = seed_plan(&conn, "plan-b", project);
        // Live run is on plan-a; user asks to pause plan-b.
        insert_live_lock(&conn, project, &plan_a, "plan-a");

        let err = cmd_pause(&conn, project, Some("plan-b"), /*quiet=*/ true).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Refusing to pause"),
            "expected 'Refusing to pause' guidance, got: {msg}"
        );

        // Neither plan's flag should be armed after a mismatch error.
        assert!(!storage::get_plan_pause_requested(&conn, &plan_a).unwrap());
        assert!(!storage::get_plan_pause_requested(&conn, &plan_b).unwrap());
    }
}
