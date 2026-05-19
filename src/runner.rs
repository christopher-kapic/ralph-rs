// Plan runner / orchestrator
//
// Implements the plan-level execution loop: loading a plan, iterating through
// steps in sort_key order, executing each via the single-step executor, and
// managing plan-level status transitions.

use std::collections::{HashMap, HashSet};
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::executor::{self, StepOutcome, StepResult};
use crate::git::{self, StashPopOutcome, StashRef};
use crate::harness;
use crate::hooks::HookContext;
use crate::output::{self, OutputContext, OutputFormat, RunEvent};
use crate::plan::{Plan, PlanStatus, Step, StepStatus};
use crate::run_lock;
use crate::signal::CancelState;
use crate::storage;

// ---------------------------------------------------------------------------
// RunOptions
// ---------------------------------------------------------------------------

/// Options controlling a plan run.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct RunOptions {
    /// Run all plans in dependency order, chaining branches between plans.
    /// Plan slug is ignored when set.
    pub all_plans: bool,
    /// Run only the next pending step instead of all remaining steps.
    pub one: bool,
    /// Start from a specific step number (1-based).
    pub from: Option<usize>,
    /// Stop after a specific step number (1-based).
    pub to: Option<usize>,
    /// Skip branch creation and use the current branch.
    pub current_branch: bool,
    /// Stash any dirty working-tree state (tracked + untracked) before
    /// switching to the plan branch via `git stash push
    /// --include-untracked`, and pop it back at run end. Default-on;
    /// `--no-auto-stash` forces it off, in which case a dirty tree causes
    /// the run to bail with a clear error.
    pub auto_stash: bool,
    /// Override the harness for this run.
    pub harness_override: Option<String>,
    /// Optional short_id of the resume target step (populated by
    /// `resume_plan` from `find_resume_point` + steps list). When present,
    /// `stash_if_dirty` uses it with `git::has_crash_residue_overlap_for_step`
    /// (via `iteration_commits_for_step` on the plan branch) for the
    /// conservative crash-residue detection before offering the medium
    /// interactive reconcile UX.
    pub resume_target_short_id: Option<String>,
    /// Dry-run mode: print what would happen without executing.
    pub dry_run: bool,
    /// Print the full per-attempt prompt to stderr instead of the
    /// 512-char preview. Threaded into [`executor::execute_step`].
    pub verbose: bool,
}

// ---------------------------------------------------------------------------
// PlanRunResult
// ---------------------------------------------------------------------------

/// Summary of a plan run.
#[derive(Debug)]
pub struct PlanRunResult {
    pub plan_slug: String,
    pub steps_executed: usize,
    pub steps_succeeded: usize,
    pub steps_failed: usize,
    pub steps_skipped: usize,
    pub final_status: PlanStatus,
    pub step_results: Vec<StepResult>,
}

// ---------------------------------------------------------------------------
// Plan runner
// ---------------------------------------------------------------------------

/// Run a plan from start to finish (or a subset of steps).
///
/// Flow:
/// 1. Load plan and validate status
/// 2. Optionally create and checkout branch
/// 3. Mark plan as in_progress
/// 4. Iterate through steps in sort_key order
/// 5. For each pending step: execute via [`executor::execute_step`]
/// 6. On step failure: mark plan as failed and stop
/// 7. On all steps complete: mark plan as complete
/// 8. Check abort signal between steps
pub async fn run_plan(
    conn: &Connection,
    plan: &Plan,
    config: &Config,
    workdir: &Path,
    options: &RunOptions,
    abort_rx: watch::Receiver<CancelState>,
    out: &OutputContext,
) -> Result<PlanRunResult> {
    // 1. Validate plan status.
    validate_plan_status(plan)?;

    // Apply harness override if provided.
    let mut effective_plan = plan.clone();
    if let Some(ref h) = options.harness_override {
        effective_plan.harness = Some(h.clone());
    }

    // Sweep stale in_progress step rows left behind by a crashed prior run.
    // The caller holds the per-project run lock, so any InProgress row we see
    // here is definitively orphaned. Skip in dry-run mode (it mutates state).
    if !options.dry_run {
        sweep_and_log_stale_in_progress(conn, &effective_plan, out)?;
        // Best-effort: reap review worktrees stranded by a force-quit/SIGKILL
        // of a prior run (RAII Drop is skipped on exit(130)/SIGKILL). Acts
        // only on >6h-old dirs, so it is safe under a concurrent run.
        crate::git::sweep_stale_review_worktrees(workdir);
    }

    // 2. Stash dirty tree + (optionally) create branch.
    //
    // We stash a dirty tree whenever `auto_stash` is true (the default;
    // togglable via `config.auto_stash = false` or the `--no-auto-stash`
    // CLI flag), regardless of `current_branch`. With `auto_stash = false`,
    // `stash_if_dirty` returns an error on a dirty tree, causing the run
    // to bail before any branch switch. Two reasons:
    //   - Even when not switching branches, the per-step commit logic in
    //     the executor will sweep up any uncommitted changes the user had
    //     in tracked files into the next step's commit. Stashing first
    //     gives the agent a clean baseline.
    //   - The user's request: ralph should "just work" with a dirty tree.
    //
    // The orchestrator (`run_all_plans`) handles stash/setup itself and
    // forces `current_branch: true` on the inner `run_plan` call, so only
    // top-level single-plan runs take this path; `dry_run` is a no-op.
    let teardown = if !options.dry_run {
        let source_branch = {
            let workdir_owned = workdir.to_path_buf();
            blocking_git(move || git::get_current_branch(&workdir_owned))
                .await
                .context("Failed to get current git branch")?
        };
        let stashed = stash_if_dirty(
            workdir,
            &effective_plan.slug,
            Some(&effective_plan.branch_name),
            options.resume_target_short_id.as_deref(),
            Some(conn),
            Some(&effective_plan.id),
            options.auto_stash,
        )
        .await?;
        // Record source_branch + stash_sha on the run_lock row so resume /
        // diagnostics can see what we'll try to restore. Best-effort — if
        // the row isn't there (tests), swallow the error.
        let _ = run_lock::record_source_branch_and_stash(
            conn,
            workdir.to_string_lossy().as_ref(),
            &source_branch,
            stashed.as_ref().map(|s| s.stash_ref.as_str()),
        );
        // Construct teardown state BEFORE setup_branch so that a failure in
        // branch creation/checkout still triggers stash restoration. Without
        // this, a bad branch name or checkout conflict would leave the user's
        // uncommitted work stranded on the stash stack.
        let td = TeardownState {
            workdir: workdir.to_path_buf(),
            stashed,
        };
        // Only switch branches when the caller didn't say `--current-branch`.
        // (When current_branch is true, we still stash a dirty tree above —
        // the stash gives the agent a clean baseline and gets popped on the
        // way out.)
        if !options.current_branch
            && let Err(setup_err) = setup_branch(workdir, &effective_plan, None).await
        {
            if let Err(te) = restore_working_tree(&td.workdir, td.stashed.as_ref()).await {
                eprintln!("Warning: teardown after failed branch setup: {te}");
            }
            return Err(setup_err);
        }
        Some(td)
    } else {
        None
    };

    // Execute the plan body. On any exit path (success, error, abort),
    // restore_working_tree must fire. We use a manual `finalize` pattern
    // rather than Drop because `stash pop` can fail and we need to
    // surface that to the caller.
    let outcome = run_plan_inner(
        conn,
        &effective_plan,
        config,
        workdir,
        options,
        abort_rx,
        out,
    )
    .await;

    if let Some(td) = teardown {
        match &outcome {
            Ok(_) => {
                // Don't mask a teardown error with a success.
                restore_working_tree(&td.workdir, td.stashed.as_ref()).await?;
            }
            Err(_) => {
                // Run already failed; log teardown errors but don't mask
                // the original failure.
                if let Err(te) = restore_working_tree(&td.workdir, td.stashed.as_ref()).await {
                    eprintln!("Warning: teardown after failed run: {te}");
                }
            }
        }
    }

    outcome
}

/// State captured by the top-level `run_plan` before the plan body runs.
/// Handed to `restore_working_tree` during teardown. `stashed` is `None` on
/// a clean tree (nothing to restore).
struct TeardownState {
    workdir: std::path::PathBuf,
    stashed: Option<StashedState>,
}

async fn run_plan_inner(
    conn: &Connection,
    effective_plan: &Plan,
    config: &Config,
    workdir: &Path,
    options: &RunOptions,
    abort_rx: watch::Receiver<CancelState>,
    out: &OutputContext,
) -> Result<PlanRunResult> {
    let effective_plan = effective_plan.clone();

    // Load steps (post-sweep snapshot used to resolve --from/--to and for the
    // initial "known step IDs" baseline).
    let initial_steps = storage::list_steps(conn, &effective_plan.id)?;
    if initial_steps.is_empty() {
        bail!("Plan '{}' has no steps", effective_plan.slug);
    }

    // Resolve the run window to sort_key bounds ONCE. Positions shift if new
    // steps are inserted mid-run, but sort_keys are stable. We still accept
    // `--from`/`--to` as 1-based step numbers for the CLI, but we immediately
    // translate them to sort_key bounds so later filtering tolerates inserts.
    let window = resolve_window(&initial_steps, options)?;

    // Steps that fall inside the run window. Used to bail early when the
    // window is empty. If the window contains NO steps at all (e.g. a bogus
    // `--from`/`--to` range), bail — but if the window contains steps that
    // just happen to all be Complete/Skipped, fall through and let the final
    // status computation report Complete. That mirrors the pre-fix behavior
    // of a user re-running an already-finished plan.
    let window_steps: Vec<&Step> = initial_steps
        .iter()
        .filter(|s| window.contains_key(&s.sort_key))
        .collect();
    if window_steps.is_empty() {
        bail!("No pending steps to run in plan '{}'", effective_plan.slug);
    }

    // Dry-run mode: just print what would happen.
    if options.dry_run {
        let steps_to_run: Vec<Step> = window_steps.iter().map(|s| (*s).clone()).collect();
        return dry_run_report(&effective_plan, &initial_steps, &steps_to_run);
    }

    // Record the branch this run is physically executing on. This is the
    // anchor `ralph resume` (no slug) uses to map the current git branch
    // back to a paused/failed plan. We capture it AFTER any branch switch
    // (`setup_branch` runs in `run_plan` above) and regardless of
    // `--current-branch`, so the value always reflects the workdir's
    // checked-out HEAD at the moment the runner began iterating steps.
    // Best-effort: if `git rev-parse` fails (detached HEAD edge cases,
    // missing git, …) we log and continue rather than aborting the run —
    // resume falls back to slug/active-plan resolution when no row matches.
    match git::get_current_branch(workdir) {
        Ok(branch) => {
            if let Err(e) = storage::set_plan_last_run_branch(conn, &effective_plan.id, &branch) {
                eprintln!("Warning: failed to record last_run_branch: {e}");
            }
        }
        Err(e) => {
            eprintln!("Warning: could not resolve current branch for last_run_branch: {e}");
        }
    }

    // 3. Mark plan as in_progress.
    if effective_plan.status != PlanStatus::InProgress {
        storage::update_plan_status(conn, &effective_plan.id, PlanStatus::InProgress)?;
    }

    // Discard any leftover cross-process skip request before iterating.
    // `request_skip` is only ever written while a run is genuinely live for
    // this plan (see `runner::skip_step`), and its sole consume/clear site is
    // the executor's `Completed` arm — so a request that times out, is
    // aborted, or whose run ends before the targeted step runs would persist
    // with a stable step UUID and silently skip that same step (with the
    // stale `--changes`) on *this* fresh run. Clearing here at run start is
    // safe (no legitimate pre-run skip request exists) and closes that gap.
    storage::clear_skip_request(conn, &effective_plan.id)?;

    // Anchor the elapsed-timer for NDJSON consumers (the TUI's in-process
    // attach path, log shippers): emit `run_started` with the wall-clock
    // start instant before the first step's phase transitions begin
    // landing. Subscribers that drove their elapsed timer off this event
    // alone still see something useful in the gap before the first
    // `phase_changed`.
    if out.format == OutputFormat::Json {
        output::emit_ndjson(&RunEvent::RunStarted {
            plan_slug: effective_plan.slug.clone(),
            started_at: chrono::Utc::now(),
        })?;
    }

    // Load the hook library once for this run, filtered by project scope.
    let hook_ctx = HookContext::load(workdir, config.hook_timeout_secs)?;

    // 4. Iterate through steps. Each iteration re-queries the step list so
    //    steps inserted mid-run by the running agent (via `ralph step add`)
    //    are picked up. `known_step_ids` tracks steps observed at or before
    //    this iteration so we can report any that appeared since the last
    //    loop pass.
    let mut result = PlanRunResult {
        plan_slug: effective_plan.slug.clone(),
        steps_executed: 0,
        steps_succeeded: 0,
        steps_failed: 0,
        steps_skipped: 0,
        final_status: PlanStatus::InProgress,
        step_results: Vec::new(),
    };

    let mut known_step_ids: HashSet<String> = initial_steps.iter().map(|s| s.id.clone()).collect();
    let mut executed_step_ids: HashSet<String> = HashSet::new();

    // Monotonic per-run counter for `HarnessChunk` / `TestChunk` event
    // `seq` fields. Created once here so the same `Arc<AtomicU64>` is
    // threaded through every `execute_step` call — `seq` stays unique
    // across step boundaries within a single `ralph run`. Per
    // TUI-plan §13.1.
    let chunk_seq: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    // ONE implementation slot (docs/dag-redesign.md §9 invariant 1 / §3.5
    // item 2). A `Semaphore` with exactly **1** permit guards the
    // implement+test+commit phase: at most one step may be in that phase at
    // a time — this *is* "implementation steps don't run in parallel". The
    // loop is already serial, so under no review this changes no behavior
    // (a linear plan is byte-identical); the explicit semaphore makes the
    // invariant a structural, test-observable guarantee and is what the
    // read-only review is deliberately allowed to run *outside* of (it
    // never acquires this permit — §9-inv-2 / §3.5 item 3), so the
    // scheduler is free to pick the next *unrelated* implementation while a
    // review is outstanding. A step's *direct dependents* are still gated
    // (they require the reviewed step `Complete`, which only happens after
    // the review returns — §3.1/§3.3), so concurrency never starts work on
    // un-reviewed output.
    let impl_slot = Arc::new(tokio::sync::Semaphore::new(1));

    // In-flight read-only reviews (docs/dag-redesign.md §3.5 item 3 / §9).
    // Each entry is a DETACHED `tokio::spawn`ed `run_review_subprocess`
    // future — `Send`, holds NO `rusqlite::Connection` and NO implementation
    // permit — so it runs CONCURRENTLY with whatever the scheduler picks
    // next. The orchestrator (this loop = the SOLE DB writer, §9-inv-3)
    // drains finished tasks at every scheduler tick via
    // `drain_finished_reviews`, where ALL DB / git-note side effects happen
    // serialized. A linear / no-review plan never spawns into this set (the
    // executor returns `needs_review: None`), so `reviews.is_empty()` is
    // always true on that path and the loop is byte-identical to before.
    let mut reviews: tokio::task::JoinSet<crate::review::SpawnedReview> =
        tokio::task::JoinSet::new();

    // For `--one`, we need to stop after the first step actually executed;
    // capture its ID at the start (the step the topological scheduler would
    // pick first) and exit after it completes. Positions can shift due to
    // inserts, but the ID is stable. If `--one` is requested but nothing is
    // runnable, bail — mirrors the pre-fix behavior of `select_steps`
    // returning an empty slice in that case. Computing the target via
    // `pick_next_step` (not `initial_actionable.first()`) makes `--one`
    // honor dependencies under the DAG: a step whose prerequisite is not
    // yet `Complete` is not the first pick even if it has the lowest
    // sort_key. With no edges this is identical to the old behavior.
    let one_target_id: Option<String> = if options.one {
        let deps_of = storage::list_step_dependency_edges(conn, &effective_plan.id)?;
        let depths = compute_step_depths(&initial_steps, &deps_of);
        let blocked = blocked_step_ids(conn, &effective_plan.id)?;
        match pick_next_step(
            &initial_steps,
            &deps_of,
            &depths,
            &window,
            &HashSet::new(),
            &blocked,
        ) {
            Some(s) => Some(s.id.clone()),
            None => bail!("No pending steps to run in plan '{}'", effective_plan.slug),
        }
    } else {
        None
    };

    loop {
        // Check the cancel signal between steps. Only `Aborted` (Ctrl+C)
        // terminates the whole run here; a `Skipped` reason only ever
        // targets the in-flight step (consumed inside the executor) and
        // must NOT end the run — fall through and pick the next step.
        if matches!(
            *abort_rx.borrow(),
            Some(crate::signal::CancelReason::Aborted)
        ) {
            eprintln!("Aborted");
            storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
            result.final_status = PlanStatus::Aborted;
            return Ok(result);
        }

        // Check the operator's graceful-pause flag between steps. The read +
        // clear is atomic so a subsequent `ralph resume` doesn't immediately
        // re-pause. We leave `plans.status` as-is (InProgress) — pause is a
        // transient runner-control signal, not a status transition — so
        // resume's normal "find earliest non-complete step" path Just Works.
        if storage::take_plan_pause_requested(conn, &effective_plan.id)? {
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::PausedByUser {
                    plan_slug: effective_plan.slug.clone(),
                })?;
            } else {
                eprintln!(
                    "> Paused by user request after {} step(s). Use `ralph resume` to continue.",
                    result.steps_executed,
                );
            }
            result.final_status = PlanStatus::InProgress;
            return Ok(result);
        }

        // Re-fetch the step list. This is the core of the mid-run-insert fix.
        let all_steps = storage::list_steps(conn, &effective_plan.id)?;

        // Detect and report new inserts.
        let new_inserts: Vec<Step> = all_steps
            .iter()
            .filter(|s| !known_step_ids.contains(&s.id))
            .filter(|s| window.contains_key(&s.sort_key))
            .cloned()
            .collect();
        if !new_inserts.is_empty() {
            report_plan_grew(&new_inserts, &all_steps, out)?;
        }
        for s in &all_steps {
            known_step_ids.insert(s.id.clone());
        }

        let total_now = all_steps.len();

        // Topological scheduler tick (docs/dag-redesign.md §3.5). Re-read
        // the dependency edges every iteration so a step inserted mid-run
        // with `--depends-on` is scheduled correctly. `pick_next_step`
        // returns the runnable step with the smallest
        // `(topological depth, sort_key, short_id)`; with no edges every
        // depth is 0 and this is exactly the old "first actionable step by
        // sort_key" behavior.
        let deps_of = storage::list_step_dependency_edges(conn, &effective_plan.id)?;
        let depths = compute_step_depths(&all_steps, &deps_of);
        // Recompute the blocked set every tick: a cross-process resolution
        // (§9 invariant 4) drops a step out of it here, re-queuing the step
        // on the very next iteration with the resolution injected (the
        // injection is already done by prompt.rs via the bounded
        // resolved-interruption section). An empty set ⇒ pre-interruption
        // behavior, so a linear plan is byte-identical.
        let blocked = blocked_step_ids(conn, &effective_plan.id)?;
        let next = pick_next_step(
            &all_steps,
            &deps_of,
            &depths,
            &window,
            &executed_step_ids,
            &blocked,
        )
        .cloned();

        let current_step = match next {
            Some(s) => s,
            None => {
                // Nothing is runnable *right now*. If a read-only review is
                // still in flight, the runnable set is not final: when that
                // review returns, the orchestrator either promotes the
                // reviewed step to `Complete` (un-gating its dependents) or
                // inserts a corrective step `A′` (a fresh runnable step) —
                // either of which re-populates the runnable set. So instead
                // of declaring the plan done, BLOCK on the next finished
                // review, drain it (sole DB writer), then loop and re-tick.
                // This is what makes the plan not complete with a review
                // still pending/undrained (§3.3). With no in-flight reviews
                // (the linear / no-review path always) this is an immediate
                // `break`, byte-identical to before.
                if reviews.is_empty() {
                    break;
                }
                if let Some(fs) = drain_finished_reviews(
                    conn,
                    &effective_plan,
                    &mut reviews,
                    workdir,
                    out,
                    true, // block: wait for at least one review to finish
                )
                .await?
                {
                    result.final_status = fs;
                    return Ok(result);
                }
                continue;
            }
        };

        // `--one`: once we've executed the captured target, stop. We also
        // refuse to pivot to a later step if the original target has moved
        // out of the actionable set (e.g. it was skipped out-of-band).
        if let Some(ref target) = one_target_id
            && current_step.id != *target
        {
            break;
        }

        // Skip already-completed or skipped steps that happen to fall in
        // the window but weren't filtered out above (defensive: the
        // is_actionable filter already excludes these).
        if current_step.status == StepStatus::Complete || current_step.status == StepStatus::Skipped
        {
            if current_step.status == StepStatus::Skipped {
                result.steps_skipped += 1;
            }
            executed_step_ids.insert(current_step.id.clone());
            continue;
        }

        // Print progress header / emit step_started event.
        let step_num = step_number_in_plan(&all_steps, &current_step);
        if out.format == OutputFormat::Json {
            output::emit_ndjson(&RunEvent::StepStarted {
                step_id: current_step.id.clone(),
                step_title: current_step.title.clone(),
                step_num,
                step_total: total_now,
            })?;
        } else {
            // Resolve the step-level harness label (hooks into the executor's
            // per-attempt sub-header below). Per-step override falls back to
            // the plan-level harness, then to config default.
            let (harness_name, harness_config) =
                harness::resolve_harness(&current_step, &effective_plan, config)?;
            let harness_label = output::format_harness_label_with_override(
                harness_name,
                harness_config.color.as_deref(),
                out.color,
            );
            eprintln!(
                "[{}/{}] > Step {} \"{}\" ({})",
                step_num, total_now, step_num, current_step.title, harness_label
            );
        }

        let started = Instant::now();

        // Mark this step in-flight for the duration of `execute_step` so a
        // same-process `ralph skip` (CLI or TUI) routes through the cancel
        // ladder instead of just flipping the DB status. The guard clears
        // the flag on drop — covering the `?`-early-return path too.
        let _in_flight = crate::signal::StepInFlightGuard::enter();

        // Acquire the single implementation slot (§9-inv-1 / §3.5 item 2).
        // Held ONLY for implement+test+commit; explicitly released before any
        // read-only review so a review never occupies the implementation
        // slot (§9-inv-2). `acquire_owned` cannot fail here — the semaphore
        // is never closed — but we surface it rather than `unwrap` to keep
        // the runner panic-free.
        let impl_permit = impl_slot
            .clone()
            .acquire_owned()
            .await
            .context("implementation semaphore closed unexpectedly")?;

        // Execute the step.
        let step_result = executor::execute_step(
            conn,
            &effective_plan,
            &current_step,
            config,
            workdir,
            &hook_ctx,
            abort_rx.clone(),
            executor::ExecuteOptions {
                verbose: options.verbose,
                step_num_in_plan: step_num,
                step_total: total_now,
                json_output: out.format == OutputFormat::Json,
                color: out.color,
                chunk_seq: Some(chunk_seq.clone()),
                chunk_max_bytes: config.harness_chunk_max_bytes,
            },
        )
        .await?;
        drop(_in_flight);

        // Release the implementation slot BEFORE any review (§9-inv-2 /
        // §3.5 item 3): a read-only review must never hold the
        // implementation permit, so the scheduler is free to take it for the
        // next *unrelated* implementation while this step's review is
        // outstanding. Direct dependents of `current_step` are NOT in the
        // runnable set yet — `execute_step` left a review-gated step
        // `InProgress` (not `Complete`), and `deps_satisfied` requires
        // `Complete` — so concurrency never starts work on un-reviewed
        // output.
        drop(impl_permit);

        // Built-in review pipeline (docs/dag-redesign.md §3.2-§3.3 / §3.5
        // item 3 / §9 / §10). When the step's success carries `needs_review`,
        // the executor deliberately left it `InProgress` with
        // `review_status = Pending`. We DO NOT review inline — that would
        // serialize the scheduler behind the reviewer and defeat the entire
        // §2-Decision-3 / §3.5-item-3 promise that *unrelated branches run
        // concurrently with a review*. Instead we:
        //
        //  1. mark the step `review_status = InFlight` and emit
        //     `ReviewStarted` HERE (the orchestrator is the SOLE DB writer —
        //     §9-inv-3 — so this status write must NOT happen in the task);
        //  2. `tokio::spawn` the read-only `run_review_subprocess` future
        //     (it is `Send`, holds NO `Connection` and NO impl permit, and
        //     runs `git show <fixed sha>` only — §9-inv-2) into the
        //     `reviews` JoinSet;
        //  3. CONTINUE the scheduler loop. The impl permit is already
        //     dropped, so the next *unrelated* runnable step implements
        //     concurrently with this outstanding review. `current_step`'s
        //     direct dependents stay non-runnable (it is `InProgress`, not
        //     `Complete`; `deps_satisfied` requires `Complete`), so
        //     concurrency never starts work on un-reviewed output.
        //
        // The orchestrator drains finished reviews at every tick via
        // `drain_finished_reviews` (below) — the ONLY place
        // `review_status` Passed/Failed, the git-note verdict, the V29
        // bridge row, and the §10 insert + re-parent are written, all
        // serialized on this single loop. A linear / no-review plan never
        // sets `needs_review`, so nothing is ever spawned and behavior is
        // byte-identical to before.
        if let StepResult {
            outcome: StepOutcome::Success,
            needs_review: Some((ref commit_sha, iteration)),
            ..
        } = step_result
        {
            let commit_sha = commit_sha.clone();
            // Orchestrator-side DB write (sole writer): Pending -> InFlight.
            storage::update_step_review_status(
                conn,
                &current_step.id,
                crate::plan::ReviewStatus::InFlight,
            )?;
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewStarted {
                    step_id: current_step.id.clone(),
                    step_num,
                    commit_sha: commit_sha.clone(),
                    iteration,
                })?;
            }
            // Owned clones so the detached task is `'static` and `Send`.
            let plan_for_review = effective_plan.clone();
            let step_for_review = current_step.clone();
            let config_for_review = config.clone();
            let workdir_for_review = workdir.to_path_buf();
            let review_step_id = step_for_review.id.clone();
            reviews.spawn(async move {
                let result = crate::review::run_review_subprocess(
                    &plan_for_review,
                    &step_for_review,
                    &config_for_review,
                    &workdir_for_review,
                    &commit_sha,
                    iteration,
                    step_num,
                )
                .await;
                // Carry the step identity back even on `Err` so the
                // sole-writer drain can surface a review *error* cleanly
                // (reset review_status + raise a blocker) instead of letting
                // the next run's stale-InProgress sweep re-implement an
                // implementation-complete step.
                crate::review::SpawnedReview {
                    step_id: review_step_id,
                    iteration,
                    result,
                }
            });
        }

        // Drain any reviews that finished while this step implemented
        // (non-blocking). This is the SOLE DAG-writer point for review
        // verdicts (§9-inv-3): finalize verdict + git-note, promote a passed
        // step to `Complete`, and consume corrective requests (§10). On a
        // review error the run fails exactly like a hard step failure.
        if let Some(fs) =
            drain_finished_reviews(conn, &effective_plan, &mut reviews, workdir, out, false).await?
        {
            result.final_status = fs;
            result.step_results.push(step_result);
            return Ok(result);
        }

        let elapsed = started.elapsed();
        result.steps_executed += 1;
        executed_step_ids.insert(current_step.id.clone());

        // Print result / emit step_finished event.
        let outcome_str = match step_result.outcome {
            StepOutcome::Success => "success",
            StepOutcome::Failed => "failed",
            StepOutcome::Aborted => "aborted",
            StepOutcome::Skipped => "skipped",
            StepOutcome::Timeout => "timeout",
            StepOutcome::PausedForQuestion => "paused_for_question",
        };

        let emit_finished = |outcome: &str| -> Result<()> {
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::StepFinished {
                    step_id: current_step.id.clone(),
                    step_title: current_step.title.clone(),
                    step_num,
                    step_total: total_now,
                    outcome: outcome.to_string(),
                    attempts: step_result.attempts_used,
                    duration_secs: elapsed.as_secs_f64(),
                })?;
            }
            Ok(())
        };

        match step_result.outcome {
            StepOutcome::Success => {
                result.steps_succeeded += 1;
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... OK ({:.0}s)",
                        step_num,
                        total_now,
                        current_step.title,
                        elapsed.as_secs_f64()
                    );
                }
            }
            StepOutcome::Failed => {
                result.steps_failed += 1;
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... FAILED (after {} attempts, {:.0}s)",
                        step_num,
                        total_now,
                        current_step.title,
                        step_result.attempts_used,
                        elapsed.as_secs_f64()
                    );
                }
                // Mark plan as failed and stop.
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Failed)?;
                result.final_status = PlanStatus::Failed;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::Aborted => {
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... ABORTED",
                        step_num, total_now, current_step.title
                    );
                }
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
                result.final_status = PlanStatus::Aborted;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::Skipped => {
                // `ralph skip` killed the in-flight harness for THIS step
                // only. Unlike Aborted, the run does NOT end — count the
                // skip and fall through so the loop advances to the next
                // actionable step.
                result.steps_skipped += 1;
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... SKIPPED",
                        step_num, total_now, current_step.title
                    );
                }
            }
            StepOutcome::Timeout => {
                result.steps_failed += 1;
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... TIMEOUT",
                        step_num, total_now, current_step.title
                    );
                }
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Failed)?;
                result.final_status = PlanStatus::Failed;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::PausedForQuestion => {
                emit_finished(outcome_str)?;
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... BLOCKED (open interruption — resolve to resume)",
                        step_num, total_now, current_step.title
                    );
                }
                // DAG scheduler change (docs/dag-redesign.md §1/§3.4/§3.5):
                // a blocked branch must NOT pause the whole plan. While the
                // interruption is open the step is in the recomputed
                // `blocked` set, so the next scheduler tick excludes it and
                // picks another runnable branch instead of stalling — the §1
                // payoff. We do NOT `return` here and do NOT write
                // `Interrupted` to plans.status (it's a *derived* status).
                // The plan only reports `Interrupted` once the runnable set
                // is exhausted (handled after the loop), so a *linear* plan —
                // whose one blocked step starves all dependents — still
                // pauses exactly as before (§1: linear plans get zero benefit
                // and must not regress).
                //
                // Crucially, a paused step must NOT stay in
                // `executed_step_ids`: that set permanently excludes a step
                // from `pick_next_step` for the rest of this run, so leaving
                // it there means a *same-run* resolution (a human answers
                // while the loop keeps ticking on another branch — the exact
                // §1 scenario, or the §9-invariant-4 cross-process bridge)
                // would drop the step out of `blocked` but it would still
                // never be re-picked until a fresh `ralph resume` process.
                // Drop it back out so the next tick after the interruption is
                // resolved re-queues it (the resolution is injected by
                // prompt.rs via the bounded resolved-interruption section);
                // while the interruption stays open `blocked` keeps it
                // excluded, so this does not busy-spin.
                executed_step_ids.remove(&current_step.id);
                result.step_results.push(step_result);
                continue;
            }
        }

        result.step_results.push(step_result);

        // `--one`: stop after executing the captured target.
        if one_target_id.is_some() {
            break;
        }
    }

    // The scheduler loop exited because the runnable set is empty. Three
    // terminal shapes (docs/dag-redesign.md §3.4/§3.5):
    //
    //  1. Every step Complete/Skipped         → plan Complete.
    //  2. Some step has an open interruption   → plan Interrupted (derived;
    //     never written to plans.status — it un-shadows automatically when
    //     the human resolves the last open interruption, possibly from a
    //     different process via the §9-invariant-4 bridge). For a *linear*
    //     plan this is reached exactly when its one blocked step starves
    //     all dependents — i.e. it still pauses the whole plan just like
    //     before (§1: no regression). For a *wide* DAG it is reached only
    //     after every independent branch has run to a stop (the payoff).
    //  3. Otherwise (e.g. `--from/--to` window, a Failed step)
    //     → InProgress, exactly as before.
    //
    // The loop only reaches here once the runnable set is empty AND every
    // in-flight review has been drained (the `None`-branch blocks on
    // `drain_finished_reviews` and re-ticks while `!reviews.is_empty()`, so
    // a corrective step a failed review inserts is itself picked up before
    // the loop can exit). Therefore a plan is never declared `Complete`
    // with a review pending or undrained (docs/dag-redesign.md §3.3). The
    // ONE exception is `--one` (it `break`s after its single target): drain
    // that step's outstanding review here so its status is finalized before
    // we compute terminal status / return.
    while !reviews.is_empty() {
        if let Some(fs) =
            drain_finished_reviews(conn, &effective_plan, &mut reviews, workdir, out, true).await?
        {
            result.final_status = fs;
            return Ok(result);
        }
    }
    debug_assert!(
        reviews.is_empty(),
        "the plan must not be finalized with a review still in flight (§3.3)"
    );
    let final_steps = storage::list_steps(conn, &effective_plan.id)?;
    let all_done = final_steps
        .iter()
        .all(|s| s.status == StepStatus::Complete || s.status == StepStatus::Skipped);
    let any_open_interruption =
        !storage::list_open_interruptions_for_plan(conn, &effective_plan.id)?.is_empty();

    // Invariant: a plan can only be `all_done` (every step Complete/Skipped)
    // with NO open interruption. A Complete step can't carry an open
    // interruption (the scheduler parks a derived-`Blocked` step before it
    // reaches Complete), and skipping a step now resolves its open
    // interruptions (`storage::mark_step_skipped` /
    // `resolve_open_interruptions_for_step`) — closing the prior hole where
    // `ralph skip` on a derived-`Blocked` step left an unresolved
    // interruption behind a `Complete` plan. The `all_done` arm is kept
    // first deliberately: a fully Complete/Skipped plan must reach
    // `Complete`, and preferring `Interrupted` here would instead strand it
    // forever on a stale interruption the operator already moved past. The
    // assert catches any future path that reintroduces the inconsistency.
    debug_assert!(
        !(all_done && any_open_interruption),
        "plan {} finalized all-done with an open interruption — a skip path \
         failed to resolve it (see resolve_open_interruptions_for_step)",
        effective_plan.slug
    );

    if all_done {
        storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Complete)?;
        result.final_status = PlanStatus::Complete;
    } else if any_open_interruption {
        result.final_status = PlanStatus::Interrupted;
    } else {
        result.final_status = PlanStatus::InProgress;
    }

    // Emit plan_complete event in NDJSON mode.
    if out.format == OutputFormat::Json {
        output::emit_ndjson(&RunEvent::PlanComplete {
            plan_slug: result.plan_slug.clone(),
            final_status: result.final_status,
            steps_executed: result.steps_executed,
            steps_succeeded: result.steps_succeeded,
            steps_failed: result.steps_failed,
        })?;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Multi-plan orchestration (run_all_plans)
// ---------------------------------------------------------------------------

/// For a plan being run as part of `run_all_plans`, the branch-setup decision
/// that the orchestrator made for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanBranchPlan {
    /// Plan ID (primary key).
    pub plan_id: String,
    /// SHA to branch off of. `None` means "no explicit parent; the caller
    /// will stay on the current HEAD" — this happens when
    /// `current_branch: true`.
    pub parent_sha: Option<String>,
    /// Additional SHAs to merge in after branch creation (for plans with
    /// multiple dependencies). Entries correspond to deps OTHER than the
    /// one whose SHA became the parent.
    pub merge_shas: Vec<String>,
}

/// Pure helper: given a topo-sorted plan order, the deps edges, the
/// run's starting SHA, and the tip SHA recorded after each plan finished,
/// compute the branching decision for the plan at position `index`.
///
/// This is factored out of `run_all_plans` so it can be unit-tested
/// without spinning up a real harness.
fn compute_branch_plan(
    topo_order: &[String],
    index: usize,
    deps_of: &HashMap<String, Vec<String>>,
    tip_sha_map: &HashMap<String, String>,
    run_start_sha: &str,
    current_branch: bool,
) -> PlanBranchPlan {
    let plan_id = topo_order[index].clone();

    if current_branch {
        return PlanBranchPlan {
            plan_id,
            parent_sha: None,
            merge_shas: Vec::new(),
        };
    }

    // Filter deps to those that appear in the topo list (same rule as topo_sort).
    let in_scope: Vec<String> = deps_of
        .get(&plan_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|d| topo_order.iter().any(|p| p == d))
        .collect();

    if in_scope.is_empty() {
        return PlanBranchPlan {
            plan_id,
            parent_sha: Some(run_start_sha.to_string()),
            merge_shas: Vec::new(),
        };
    }

    // Pick the most-recently-completed dep (highest topo index) as the parent.
    // All other deps' SHAs will be merged in afterward.
    let mut parent_dep: Option<(usize, String)> = None;
    let mut others: Vec<String> = Vec::new();
    for d in &in_scope {
        let idx = topo_order.iter().position(|p| p == d).unwrap_or(0);
        match &parent_dep {
            None => parent_dep = Some((idx, d.clone())),
            Some((cur_idx, _)) if idx > *cur_idx => {
                // Demote the previous parent to the "others" list.
                if let Some((_, prev)) = parent_dep.take() {
                    others.push(prev);
                }
                parent_dep = Some((idx, d.clone()));
            }
            Some(_) => others.push(d.clone()),
        }
    }

    let parent_sha = parent_dep
        .and_then(|(_, id)| tip_sha_map.get(&id).cloned())
        .unwrap_or_else(|| run_start_sha.to_string());
    let merge_shas: Vec<String> = others
        .into_iter()
        .filter_map(|id| tip_sha_map.get(&id).cloned())
        .collect();

    PlanBranchPlan {
        plan_id,
        parent_sha: Some(parent_sha),
        merge_shas,
    }
}

/// Run all plans in a project in dependency order.
///
/// Loads runnable plans, topologically sorts them, then runs each plan via
/// [`run_plan`] while chaining branches based on the dependency graph:
///
/// - Plans with no in-scope dependencies branch off the run's starting HEAD
///   (captured once at the start of the run).
/// - Plans with one in-scope dependency branch off that dep's captured tip
///   SHA.
/// - Plans with multiple in-scope dependencies branch off the
///   most-recently-completed dep (highest position in topo order) and then
///   merge the remaining deps' tip SHAs via `git merge --no-ff`. Merge
///   conflicts abort the run and require manual resolution.
///
/// If `options.current_branch` is true, the orchestrator stays on the
/// current branch for every plan and does not set up any branches itself.
///
/// Plans in `Planning`, `Complete`, `Aborted`, or `Archived` state are
/// skipped (only `Ready`, `InProgress`, and `Failed` are considered
/// runnable).
pub async fn run_all_plans(
    conn: &Connection,
    project: &str,
    config: &Config,
    workdir: &Path,
    options: &RunOptions,
    abort_rx: watch::Receiver<CancelState>,
    out: &OutputContext,
) -> Result<Vec<PlanRunResult>> {
    // 1. Load runnable plans.
    let all = storage::list_plans(conn, project, false)?;
    let runnable: Vec<Plan> = all
        .into_iter()
        .filter(|p| {
            matches!(
                p.status,
                PlanStatus::Ready | PlanStatus::InProgress | PlanStatus::Failed
            )
        })
        .collect();

    if runnable.is_empty() {
        eprintln!("No runnable plans found for project '{project}'.");
        return Ok(Vec::new());
    }

    // 2. Topo-sort them.
    let plan_ids: Vec<String> = runnable.iter().map(|p| p.id.clone()).collect();
    let topo_order = storage::topo_sort_plans(conn, &plan_ids)?;

    // Index for quick lookup.
    let plan_by_id: HashMap<String, Plan> =
        runnable.into_iter().map(|p| (p.id.clone(), p)).collect();

    // 3. Capture the run's starting SHA (used for plans with no deps) and
    // stash any dirty tree + record the source branch for teardown. We stash
    // even with `--current-branch` so the per-plan executor sees a clean
    // baseline (mirrors the single-plan path).
    let teardown = if !options.dry_run {
        let source_branch = {
            let workdir_owned = workdir.to_path_buf();
            blocking_git(move || git::get_current_branch(&workdir_owned))
                .await
                .context("Failed to get current git branch")?
        };
        let stashed =
            stash_if_dirty(workdir, "all", None, None, None, None, options.auto_stash).await?;
        let _ = run_lock::record_source_branch_and_stash(
            conn,
            project,
            &source_branch,
            stashed.as_ref().map(|s| s.stash_ref.as_str()),
        );
        Some(TeardownState {
            workdir: workdir.to_path_buf(),
            stashed,
        })
    } else {
        None
    };

    let run_start_sha = if options.current_branch || options.dry_run {
        String::new()
    } else {
        let workdir_owned = workdir.to_path_buf();
        blocking_git(move || git::get_commit_hash(&workdir_owned))
            .await
            .context("could not capture starting HEAD SHA")?
    };

    let inner = run_all_plans_inner(
        conn,
        project,
        config,
        workdir,
        options,
        abort_rx,
        out,
        topo_order,
        plan_by_id,
        run_start_sha,
    )
    .await;

    if let Some(td) = teardown {
        match &inner {
            Ok(_) => {
                restore_working_tree(&td.workdir, td.stashed.as_ref()).await?;
            }
            Err(_) => {
                if let Err(te) = restore_working_tree(&td.workdir, td.stashed.as_ref()).await {
                    eprintln!("Warning: teardown after failed --all run: {te}");
                }
            }
        }
    }

    inner
}

#[allow(clippy::too_many_arguments)]
async fn run_all_plans_inner(
    conn: &Connection,
    project: &str,
    config: &Config,
    workdir: &Path,
    options: &RunOptions,
    abort_rx: watch::Receiver<CancelState>,
    out: &OutputContext,
    topo_order: Vec<String>,
    plan_by_id: HashMap<String, Plan>,
    run_start_sha: String,
) -> Result<Vec<PlanRunResult>> {
    // 4. Build deps_of map for the in-scope plan set.
    let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
    for pid in &topo_order {
        deps_of.insert(pid.clone(), storage::list_plan_dependencies(conn, pid)?);
    }

    // Reverse adjacency: for each plan, which plans directly depend on it
    // (within the in-scope set). Used to block transitive dependents when
    // an upstream plan ends incomplete.
    let mut dependents_of: HashMap<String, Vec<String>> = HashMap::new();
    for (pid, deps) in &deps_of {
        for d in deps {
            dependents_of
                .entry(d.clone())
                .or_default()
                .push(pid.clone());
        }
    }

    // 5. Iterate through plans in topo order.
    let mut tip_sha_map: HashMap<String, String> = HashMap::new();
    let mut results: Vec<PlanRunResult> = Vec::new();
    // Plans whose upstream deps ended incomplete — skip them but continue.
    let mut blocked: HashSet<String> = HashSet::new();
    // Slugs of plans that ended with an incomplete (InProgress) final status.
    let mut incomplete_slugs: Vec<String> = Vec::new();
    let total = topo_order.len();

    for (i, plan_id) in topo_order.iter().enumerate() {
        // Abort check between plans. Only Ctrl+C (`Aborted`) tears the
        // multi-plan run down; a leftover `Skipped` reason is step-scoped.
        if matches!(
            *abort_rx.borrow(),
            Some(crate::signal::CancelReason::Aborted)
        ) {
            eprintln!("Aborted before plan {}/{}", i + 1, total);
            return Ok(results);
        }

        let plan = plan_by_id
            .get(plan_id)
            .with_context(|| format!("internal: missing plan {plan_id}"))?;

        // If an upstream dep of this plan ended incomplete, skip it —
        // its branch can't be set up from an incomplete parent tip.
        if blocked.contains(plan_id) {
            // Still rebind the live-run row so `ralph status` doesn't keep
            // advertising the previously executed plan as active while the
            // orchestrator walks past blocked plans.
            if !options.dry_run {
                storage::bind_live_run_to_plan(conn, project, &plan.id, &plan.slug)?;
            }
            eprintln!(
                "=== Plan {}/{}: {} (skipped — upstream dependency ended incomplete) ===",
                i + 1,
                total,
                plan.slug
            );
            continue;
        }

        let branch_plan = compute_branch_plan(
            &topo_order,
            i,
            &deps_of,
            &tip_sha_map,
            &run_start_sha,
            options.current_branch,
        );

        if !options.dry_run {
            storage::bind_live_run_to_plan(conn, project, &plan.id, &plan.slug)?;
        }

        // Print header.
        eprintln!("=== Plan {}/{}: {} ===", i + 1, total, plan.slug);
        match (&branch_plan.parent_sha, options.current_branch) {
            (_, true) => {
                eprintln!("  Using current branch (no branch setup)");
            }
            (Some(sha), false) => {
                let short = sha.chars().take(10).collect::<String>();
                eprintln!("  Branch '{}' from parent SHA {}", plan.branch_name, short);
                if !branch_plan.merge_shas.is_empty() {
                    eprintln!(
                        "  Will merge {} additional dep SHA(s) into '{}'",
                        branch_plan.merge_shas.len(),
                        plan.branch_name
                    );
                }
            }
            (None, false) => {
                eprintln!("  Branch '{}' from current HEAD", plan.branch_name);
            }
        }

        // Set up the branch ourselves (unless the user wants current-branch mode).
        if !options.current_branch && !options.dry_run {
            setup_branch(workdir, plan, branch_plan.parent_sha.as_deref()).await?;

            // Merge any additional deps' SHAs for multi-parent plans.
            for other_sha in &branch_plan.merge_shas {
                let workdir_owned = workdir.to_path_buf();
                let sha = other_sha.clone();
                let merge_result = blocking_git(move || git::merge_sha(&workdir_owned, &sha)).await;
                if let Err(e) = merge_result {
                    // Try to find a human-readable slug for the conflicting SHA.
                    let other_slug = tip_sha_map
                        .iter()
                        .find(|(_, v)| *v == other_sha)
                        .and_then(|(k, _)| plan_by_id.get(k).map(|p| p.slug.clone()))
                        .unwrap_or_else(|| other_sha.clone());
                    bail!(
                        "Plan '{}' has multiple dependencies whose branches diverge. \
                         Failed to merge {} into {}'s branch. \
                         Resolve manually with: git merge {}\n\
                         Underlying error: {}",
                        plan.slug,
                        other_slug,
                        plan.slug,
                        other_sha,
                        e
                    );
                }
            }
        }

        // Build the inner RunOptions. Force `current_branch: true` so the
        // inner run_plan doesn't try to re-do branch setup — we've already
        // handled it at the orchestrator level. Also force `all_plans: false`
        // to avoid any chance of recursion.
        let inner_options = RunOptions {
            all_plans: false,
            one: options.one,
            from: options.from,
            to: options.to,
            current_branch: true,
            // Branch setup was already handled at the orchestrator level;
            // forward `auto_stash` for completeness even though the inner
            // call won't re-run `setup_branch`.
            auto_stash: options.auto_stash,
            harness_override: options.harness_override.clone(),
            resume_target_short_id: options.resume_target_short_id.clone(),
            dry_run: options.dry_run,
            verbose: options.verbose,
        };

        let result = run_plan(
            conn,
            plan,
            config,
            workdir,
            &inner_options,
            abort_rx.clone(),
            out,
        )
        .await?;

        let final_status = result.final_status;
        results.push(result);

        // Stop on failure or abort.
        match final_status {
            PlanStatus::Complete => {
                // Capture the tip SHA of this plan's branch for downstream deps.
                if !options.current_branch && !options.dry_run {
                    let workdir_owned = workdir.to_path_buf();
                    let sha = blocking_git(move || git::get_commit_hash(&workdir_owned))
                        .await
                        .context("could not capture tip SHA after plan completed")?;
                    tip_sha_map.insert(plan_id.clone(), sha);
                }
            }
            PlanStatus::Failed | PlanStatus::Aborted => {
                eprintln!(
                    "Plan '{}' ended with status {}; stopping multi-plan run.",
                    plan.slug, final_status
                );
                return Ok(results);
            }
            _ => {
                // InProgress — plan stopped cleanly but incomplete. Block
                // its transitive dependents (their branches would root on an
                // incomplete tip) but keep iterating so independent plans
                // still run.
                incomplete_slugs.push(plan.slug.clone());
                let newly_blocked = transitive_dependents(plan_id, &dependents_of);
                if newly_blocked.is_empty() {
                    eprintln!(
                        "Plan '{}' ended incomplete; continuing with independent plans.",
                        plan.slug
                    );
                } else {
                    let blocked_slugs: Vec<String> = newly_blocked
                        .iter()
                        .filter_map(|id| plan_by_id.get(id).map(|p| p.slug.clone()))
                        .collect();
                    eprintln!(
                        "Plan '{}' ended incomplete; skipping {} dependent plan(s): {}",
                        plan.slug,
                        blocked_slugs.len(),
                        blocked_slugs.join(", ")
                    );
                    blocked.extend(newly_blocked);
                }
            }
        }
    }

    if !incomplete_slugs.is_empty() {
        eprintln!(
            "Warning: {} plan(s) ended incomplete: {}",
            incomplete_slugs.len(),
            incomplete_slugs.join(", ")
        );
    }

    Ok(results)
}

/// Collect every plan that transitively depends on `root_id` within the given
/// reverse-adjacency graph. Returns plan IDs (excluding `root_id`).
fn transitive_dependents(
    root_id: &str,
    dependents_of: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = dependents_of.get(root_id).cloned().unwrap_or_default();
    while let Some(node) = stack.pop() {
        if !seen.insert(node.clone()) {
            continue;
        }
        if let Some(next) = dependents_of.get(&node) {
            for n in next {
                if !seen.contains(n) {
                    stack.push(n.clone());
                }
            }
        }
        out.push(node);
    }
    out
}

/// Resume a plan from the last failed or in-progress step.
///
/// Finds the first step that is failed or in_progress, resets it to pending,
/// and runs from there.
pub async fn resume_plan(
    conn: &Connection,
    plan: &Plan,
    config: &Config,
    workdir: &Path,
    abort_rx: watch::Receiver<CancelState>,
    out: &OutputContext,
) -> Result<PlanRunResult> {
    // Validate status early so the sweep never fires on a plan the caller
    // can't resume anyway (e.g. Archived, Complete).
    validate_plan_status(plan)?;

    // Sweep stale InProgress rows BEFORE locating the resume point. The run
    // lock is held by the caller, so any InProgress row is definitively
    // orphaned. This runs the same log path as `run_plan`; `run_plan` below
    // also calls sweep but on a clean plan it's a no-op.
    sweep_and_log_stale_in_progress(conn, plan, out)?;

    // Find the resume point.
    let steps = storage::list_steps(conn, &plan.id)?;
    let resume_idx = find_resume_point(&steps)?;

    // Reset the failed/in-progress step to pending.
    let step = &steps[resume_idx];
    if step.status == StepStatus::Failed
        || step.status == StepStatus::InProgress
        || step.status == StepStatus::Aborted
    {
        storage::reset_step(conn, &step.id)?;
    }

    let step_num = resume_idx + 1; // 1-based
    eprintln!(
        "Resuming plan '{}' at step {}/{} '{}' (earliest non-complete by sort_key)",
        plan.slug,
        step_num,
        steps.len(),
        step.title
    );

    // With the Phase 3 default flip, omitting `step` means "run all remaining
    // steps", so resume only needs the starting step and the current-branch
    // flag (we don't want to create a new branch when resuming).
    let options = RunOptions {
        from: Some(step_num),
        current_branch: true,
        resume_target_short_id: Some(step.short_id.clone()),
        ..Default::default()
    };

    run_plan(conn, plan, config, workdir, &options, abort_rx, out).await
}

/// Skip the current (or specified) step in a plan.
///
/// Marks the step as skipped and returns the step number that was skipped.
/// The optional `reason` is persisted on the step so it appears in
/// `ralph status -v` and `ralph log`.
///
/// `changes` is the user's `--changes` choice. It only matters when the
/// target step is currently running (live harness path via cancel registry
/// or DB bridge) *or* when the step is stale `InProgress` (crashed runner,
/// e.g. after pre-commit hook hard-exit) and `changes == Commit`: in the
/// latter case we directly call `git::park_changes` with `ParkStrategy::Commit`
/// using the step's `id` for the `Ralph-Skipped-Step` trailer and subject
/// "[ralph wip] skipped step N: title (crashed runner residue)", after a
/// conservative `has_uncommitted_changes` safety guard. Only performed if
/// changes exist; on error we fall back to a note advising manual commit.
/// For any other non-running step the strategy is ignored (a one-line note
/// is emitted unless the Stash default).
pub fn skip_step(
    conn: &Connection,
    plan: &Plan,
    step_num: Option<usize>,
    reason: Option<&str>,
    changes: crate::git::ParkStrategyKind,
) -> Result<usize> {
    let steps = storage::list_steps(conn, &plan.id)?;

    let idx = if let Some(num) = step_num {
        if num == 0 || num > steps.len() {
            bail!(
                "Step {} is out of range (plan has {} steps)",
                num,
                steps.len()
            );
        }
        num - 1
    } else {
        // Find the current step: first non-complete, non-skipped step.
        find_current_step(&steps)?
    };

    let step = &steps[idx];
    let actual_num = idx + 1;

    // Only allow skipping pending, failed, or in_progress steps.
    // `Blocked` is a derived overlay never stored on `steps.status` (its
    // underlying state is Pending/InProgress, both skippable), but match it
    // explicitly for exhaustiveness.
    match step.status {
        StepStatus::Pending | StepStatus::Failed | StepStatus::InProgress | StepStatus::Blocked => {
        }
        StepStatus::Complete => bail!("Step {} '{}' is already complete", actual_num, step.title),
        StepStatus::Skipped => bail!("Step {} '{}' is already skipped", actual_num, step.title),
        StepStatus::Aborted => {
            // Allow skipping aborted steps too.
        }
    }

    // If the target is the step currently running, the skip must interrupt
    // the live harness — not just flip a DB status the running runner
    // ignores. There are two transports, tried in order:
    //
    //  1. Same-process fast path (`request_skip_in_flight`): only fires when
    //     the skip and the blocking runner share a process. In production
    //     they never do (the runner is always a separate subprocess from
    //     `ralph skip` / the TUI), so this is effectively a unit-test-only
    //     path — but it must keep working for those tests.
    //
    //  2. Cross-process DB bridge (`storage::request_skip`): the production
    //     path. We write `plans.skip_requested_step_id` + `skip_changes`;
    //     the runner that owns the in-flight harness polls it mid-attempt
    //     (see `executor::poll_cross_process_skip`) and funnels it into the
    //     SAME executor skip handling. Modeled on `plans.pause_requested`.
    //     Crucially this write is NOT gated behind the per-project run lock
    //     (a live run holds that lock for its whole duration), so it always
    //     succeeds even while a run is in progress.
    //
    // On either in-flight path we must NOT flip the status here — the
    // executor owns that on the skip path, and a double write would race the
    // in-flight attempt.
    if step.status == StepStatus::InProgress {
        if crate::signal::request_skip_in_flight(changes) {
            eprintln!(
                "Skipping in-flight step {} '{}' — interrupting the harness…",
                actual_num, step.title
            );
            return Ok(actual_num);
        }

        // Not same-process. If a run is genuinely live for this project,
        // hand the skip off to it via the DB bridge.
        let live = storage::get_live_run(conn, &plan.project)?;
        if live.as_ref().and_then(|r| r.plan_id.as_deref()) == Some(plan.id.as_str()) {
            storage::request_skip(conn, &plan.id, &step.id, changes)?;
            eprintln!(
                "Requested skip of in-flight step {} '{}' — interrupting the harness…",
                actual_num, step.title
            );
            return Ok(actual_num);
        }
        // No live run despite an InProgress status: this is a stale status
        // (e.g. a crashed prior run). Fall through to the synchronous
        // DB-flip below so the user's skip still takes effect.
        //
        // For the Commit strategy we now honor it here: capture any residue
        // left by the dead runner (e.g. post-harness changes that never got
        // committed because of a hook crash) as a WIP commit with trailer.
    }

    // For a stale InProgress step + explicit --changes commit we (attempt to)
    // park the crashed-runner residue (if any) using the step id for the
    // trailer. We use a conservative has_uncommitted_changes guard (no
    // pre_existing available for the crashed run) and fall back to a
    // "commit manually" note on park error (be conservative on attribution
    // and safety). We treat the Commit choice on stale InProgress as handled
    // (suppress the generic "no effect" note) even if the tree was clean.
    let handled_stale_commit =
        step.status == StepStatus::InProgress && changes == crate::git::ParkStrategyKind::Commit;
    if handled_stale_commit {
        let workdir = Path::new(&plan.project);
        let has_changes = git::has_uncommitted_changes(workdir).unwrap_or(false);
        if has_changes {
            let trailer_id = &step.id;
            let subject = format!(
                "[ralph wip] skipped step {}: {} (crashed runner residue)",
                actual_num, step.title
            );
            let strategy = crate::git::ParkStrategy::Commit { subject };
            match git::park_changes(workdir, strategy, &[], trailer_id) {
                Ok(git::ParkOutcome::Committed { sha }) => {
                    eprintln!(
                        "Committed crashed-runner residue for step {} '{}' as {}",
                        actual_num, step.title, sha
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!(
                        "note: --changes commit could not capture residue for step {} '{}': {} — commit manually if needed",
                        actual_num, step.title, e
                    );
                }
            }
        }
        // (no generic note for this case)
    }

    // For genuinely non-running steps (or stale InProgress with non-Commit),
    // the working tree's changes (if any) aren't causally tied to *this*
    // skip, so we deliberately don't touch them. The --changes choice has
    // no effect unless it was the (no-op) default. We skip this note when
    // we just handled a stale-Commit park above.
    if !handled_stale_commit && changes != crate::git::ParkStrategyKind::Stash {
        eprintln!(
            "note: --changes has no effect — step {} is not running, so its \
             working-tree changes are left untouched",
            actual_num
        );
    }

    storage::mark_step_skipped(conn, &step.id, reason)?;
    match reason {
        Some(r) => eprintln!(
            "Skipped step {} '{}' (reason: {})",
            actual_num, step.title, r
        ),
        None => eprintln!("Skipped step {} '{}'", actual_num, step.title),
    }

    Ok(actual_num)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate that the plan is in a runnable status.
fn validate_plan_status(plan: &Plan) -> Result<()> {
    match plan.status {
        // Aborted is runnable: `resume_plan` routes through `run_plan`, and
        // the old rejection (with a "use resume to continue" hint) made
        // resume error out on exactly the plans it was meant to handle.
        PlanStatus::Ready | PlanStatus::InProgress | PlanStatus::Failed | PlanStatus::Aborted => {
            Ok(())
        }
        PlanStatus::Planning => bail!(
            "Plan '{}' is still in planning status. Run `plan approve {}` first.",
            plan.slug,
            plan.slug
        ),
        PlanStatus::Complete => bail!(
            "Plan '{}' is already complete. Reset steps to re-run.",
            plan.slug
        ),
        PlanStatus::Archived => bail!(
            "Plan '{}' is archived. Use `plan unarchive {}` to restore it.",
            plan.slug,
            plan.slug
        ),
        // `Interrupted` is a derived status — `plans.status` is never written
        // to "interrupted" in the DB, so this arm is defensive. If a caller
        // ever materializes a Plan with Interrupted (e.g. a future helper
        // that shadows status when open interruptions exist), refuse to run:
        // the human must resolve the interruption first.
        PlanStatus::Interrupted => bail!(
            "Plan '{}' is paused for an open interruption. Resolve it first.",
            plan.slug
        ),
    }
}

/// What we stashed at run start. Beyond the stash commit SHA we also remember
/// which files were *staged* (in the index) at the time, so the matching
/// teardown can re-stage them after `git stash pop` (which always restores
/// everything as unstaged). Empty `staged_files` means the user had nothing
/// staged — only unstaged or untracked changes — and teardown will leave the
/// post-pop unstaged status as-is.
#[derive(Debug, Clone)]
pub(crate) struct StashedState {
    pub stash_ref: StashRef,
    pub staged_files: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconcileAction {
    CommitWip,
    Stash,
    Discard,
    Cancel,
}

/// Tiny stdio prompt (CLI-friendly, no ratatui) for the four crash-reconcile
/// choices. Used only on TTY after we have detected likely ralph residue.
fn prompt_crash_reconcile(dirty_files: &[String], step_hint: &str) -> Result<ReconcileAction> {
    eprintln!(
        "\nWorking tree has uncommitted changes, and at least one of them \
         overlaps files touched by recent Ralph-* commits for step '{}' \
         (likely residue from a crashed InProgress step for this plan).",
        step_hint
    );
    // Detection is overlap-only: a single overlapping file triggers this, but
    // the dirty set below may also contain UNRELATED work. Commit/Discard act
    // on the WHOLE tree, not just the residue — so list every dirty file and
    // say so explicitly. The user makes an informed choice.
    eprintln!(
        "\nAll {} uncommitted file(s) below — NOT all are necessarily ralph \
         residue:",
        dirty_files.len()
    );
    for f in dirty_files {
        eprintln!("  {}", f);
    }
    eprintln!(
        "\n⚠  Commit and Discard act on ALL of the above, including any \
         unrelated work. Use Stash if unsure (it is fully recoverable)."
    );
    eprintln!("\nReconcile before continuing the run/resume?");
    eprintln!(
        "  [c] Commit as [ralph wip]   — commit ALL listed changes as one [ralph wip] commit"
    );
    eprintln!("  [s] Stash                 — ralph auto-stash ALL changes (restored on teardown)");
    eprintln!(
        "  [d] Discard               — PERMANENTLY delete ALL listed changes (incl. unrelated)"
    );
    eprintln!("  [x] Cancel                — abort without touching the tree");
    for _ in 0..5 {
        eprint!("Choice [c/s/d/x]: ");
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        if std::io::stdin().read_line(&mut line).is_err() {
            return Ok(ReconcileAction::Cancel);
        }
        match line.trim().to_ascii_lowercase().as_str() {
            "c" | "commit" => return Ok(ReconcileAction::CommitWip),
            "s" | "stash" => return Ok(ReconcileAction::Stash),
            "d" | "discard" => return Ok(ReconcileAction::Discard),
            "x" | "cancel" | "q" | "" => return Ok(ReconcileAction::Cancel),
            other => eprintln!("Unrecognized choice {:?}. Enter c, s, d or x.", other),
        }
    }
    Ok(ReconcileAction::Cancel)
}

/// If the working tree is dirty, stash it with a ralph-branded message and
/// return the stash's commit SHA plus the list of files that were staged at
/// stash time. Returns `Ok(None)` on a clean tree. Bails with a user-facing
/// error when the tree is dirty and `auto_stash` is false.
///
/// When `plan_branch` and/or `resume_target_short_id` are supplied and the
/// tree is dirty with `auto_stash=false`, we first attempt the "medium"
/// crash-reconcile UX: if stdout is a TTY and
/// `git::has_crash_residue_overlap_for_step` (or latest Ralph commit when no
/// explicit target) reports that the dirty files overlap files touched by
/// recent Ralph-* commits for the (crashed) step on the plan branch, we pop
/// an interactive stdio choice (Commit as [ralph wip], Stash, Discard, Cancel).
/// On Commit/Discard the tree is cleaned and we proceed (return None); on
/// Stash we perform the stash ourselves and return it; on Cancel we bail.
///
/// If not TTY or detection uncertain, we emit a tailored error mentioning
/// `ralph skip --changes commit` (or the resume step) plus manual options.
///
/// The reconcile "Commit as [ralph wip]" path resolves the candidate
/// `short_id` to its step UUID via `conn`/`plan_id` and parks the changes
/// through [`git::park_changes`] so the commit carries the standard
/// `Ralph-Skipped-Step` trailer — making it discoverable by `ralph log` and
/// revertable by `ralph step reset`, exactly like a `ralph skip --changes
/// commit` WIP commit. If the UUID can't be resolved (no `conn`, or the
/// short_id no longer maps to a step) it falls back to a plain trailerless
/// commit so the reconcile still clears the tree.
///
/// The stash message is `"ralph: auto-stash for plan '<slug>' at
/// <ISO-8601>"` so teardown (or manual recovery) can locate it by subject.
async fn stash_if_dirty(
    workdir: &Path,
    plan_slug: &str,
    plan_branch: Option<&str>,
    resume_target_short_id: Option<&str>,
    conn: Option<&Connection>,
    plan_id: Option<&str>,
    auto_stash: bool,
) -> Result<Option<StashedState>> {
    let workdir_owned = workdir.to_path_buf();
    let dirty = blocking_git(move || git::has_uncommitted_changes(&workdir_owned)).await?;
    if !dirty {
        return Ok(None);
    }

    if !auto_stash {
        let workdir_owned = workdir.to_path_buf();
        let files = blocking_git(move || git::get_all_changed_files(&workdir_owned)).await?;

        // --- crash-reconcile detection + interactive TTY prompt ---
        let mut candidate: Option<String> = resume_target_short_id.map(|s| s.to_string());
        let branch_owned = plan_branch.map(|b| b.to_string());
        let wdir_clone = workdir.to_path_buf();

        if candidate.is_none()
            && let Some(ref br) = branch_owned
        {
            let br2 = br.clone();
            let wd2 = wdir_clone.clone();
            if let Ok(iter_cs) = blocking_git(move || git::list_iteration_commits(&wd2, &br2)).await
                && let Some(l) = iter_cs.first()
            {
                candidate = Some(l.step_short_id.clone());
            }
        }

        let mut reconciled_stash: Option<StashedState> = None;
        let mut did_clean = false;

        if let (Some(br), Some(tgt)) = (&branch_owned, &candidate) {
            let wdir3 = wdir_clone.clone();
            let br3 = br.clone();
            let tgt3 = tgt.clone();
            let is_residue =
                blocking_git(move || git::has_crash_residue_overlap_for_step(&wdir3, &br3, &tgt3))
                    .await
                    .unwrap_or(false);

            if is_residue {
                // The prompt reads from stdin and writes to stderr, so gate
                // on *those* streams — not stdout (which carries NDJSON under
                // `--json` and is piped when a TUI spawns the runner). Keying
                // on stdout would let a stdin-closed/non-interactive invocation
                // enter the prompt, hit EOF, and silently Cancel the run.
                let is_tty = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
                if is_tty {
                    match prompt_crash_reconcile(&files, tgt) {
                        Ok(ReconcileAction::CommitWip) => {
                            let subject =
                                format!("[ralph wip] crashed-residue before step {}", tgt);
                            // Resolve the candidate short_id -> step UUID so the
                            // WIP commit carries the standard Ralph-Skipped-Step
                            // trailer (discoverable by `ralph log`, revertable by
                            // `ralph step reset`). Synchronous DB read, done
                            // before the blocking git call. Falls back to a
                            // plain trailerless commit if unresolvable.
                            let trailer_uuid: Option<String> = match (conn, plan_id) {
                                (Some(c), Some(pid)) => {
                                    storage::list_steps(c, pid).ok().and_then(|steps| {
                                        steps.into_iter().find(|s| s.short_id == *tgt).map(|s| s.id)
                                    })
                                }
                                _ => None,
                            };
                            let wdir_c = workdir.to_path_buf();
                            let commit_res = blocking_git(move || match trailer_uuid {
                                Some(uuid) => git::park_changes(
                                    &wdir_c,
                                    git::ParkStrategy::Commit { subject },
                                    &[],
                                    &uuid,
                                )
                                .map(|_| ()),
                                None => git::commit_changes(&wdir_c, &subject),
                            })
                            .await;
                            match commit_res {
                                Ok(_) => {
                                    eprintln!("Committed residue as [ralph wip] (clean tree).");
                                    did_clean = true;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Warning: [ralph wip] commit failed ({e}); tree may still be dirty."
                                    );
                                }
                            }
                        }
                        Ok(ReconcileAction::Stash) => {
                            // Perform the exact same stash that auto=true path would do.
                            let staged = blocking_git({
                                let wd = workdir.to_path_buf();
                                move || git::list_staged_files(&wd)
                            })
                            .await
                            .unwrap_or_default();
                            let ts = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                            let message = format!(
                                "ralph: auto-stash for plan '{plan_slug}' at {ts} (reconcile)"
                            );
                            let wd_s = workdir.to_path_buf();
                            let msg_s = message.clone();
                            match blocking_git(move || {
                                git::stash_push_with_untracked(&wd_s, &msg_s)
                            })
                            .await
                            {
                                Ok(Some(stash_ref)) => {
                                    reconciled_stash = Some(StashedState {
                                        stash_ref,
                                        staged_files: staged,
                                    });
                                    eprintln!("Stashed (via reconcile choice).");
                                }
                                _ => {
                                    eprintln!("Stash choice could not complete; will error below.");
                                }
                            }
                        }
                        Ok(ReconcileAction::Discard) => {
                            let wdir_d = workdir.to_path_buf();
                            let _ = blocking_git(move || {
                                let _ = git::rollback_except(&wdir_d, &[]);
                                Ok::<_, anyhow::Error>(())
                            })
                            .await;
                            eprintln!("Discarded residue (clean tree).");
                            did_clean = true;
                        }
                        Ok(ReconcileAction::Cancel) | Err(_) => {
                            bail!(
                                "User cancelled at crash-reconcile prompt. \
                                 Working tree left dirty; resolve manually and re-invoke."
                            );
                        }
                    }
                } else {
                    // Non-TTY or uncertain: improved, actionable error (points at skip --changes now that it works)
                    let step_desc = if resume_target_short_id.is_some() {
                        format!("resume target step {}", tgt)
                    } else {
                        format!("step {} (most recent Ralph-* on branch)", tgt)
                    };
                    bail!(
                        "Working tree has uncommitted changes overlapping ralph-owned \
                         files from {} (on plan branch '{}').\n\
                         (stdout is not a TTY — no interactive prompt offered.)\n\
                         Quick fixes:\n\
                         \n  ralph skip --changes commit\n    (parks the residue as a [ralph wip] commit with the step's trailer; \
                         safe for the crashed InProgress case)\n\
                         \n  git stash push --include-untracked\n  git add -A && git commit -m '[ralph wip] manual'\n  git checkout -- . && git clean -fd\n\
                         \nThen retry `ralph resume` (or `ralph run`). Or set `auto_stash: true` in config.",
                        step_desc,
                        plan_branch.unwrap_or("(unknown)")
                    );
                }
            }
        }

        if let Some(st) = reconciled_stash {
            return Ok(Some(st));
        }
        if did_clean {
            // We cleaned via commit or discard; proceed as if tree was clean.
            return Ok(None);
        }

        // Fallthrough: no (or failed) reconcile — original generic refusal.
        let mut msg = format!(
            "Working tree has uncommitted changes; refusing to run with \
             auto_stash disabled and {} file(s) dirty:\n",
            files.len(),
        );
        for f in &files {
            msg.push_str("  ");
            msg.push_str(f);
            msg.push('\n');
        }
        msg.push_str(
            "Set `auto_stash: true` in ~/.config/ralph-rs/config.json (or omit \
             `--no-auto-stash`) to let ralph preserve your changes via \
             `git stash push --include-untracked` and restore them at run end. \
             Or stash/commit the changes manually before re-running.",
        );
        bail!(msg);
    }

    // Capture which files are currently staged BEFORE stashing — `git stash
    // pop` always restores everything as unstaged, so we need this list to
    // re-stage on teardown if we want to preserve the user's staged/unstaged
    // split across the run.
    let workdir_owned = workdir.to_path_buf();
    let staged_files = blocking_git(move || git::list_staged_files(&workdir_owned)).await?;

    // Timestamp for traceability; the pairing of plan slug + timestamp makes
    // every ralph stash line distinct on the stack.
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let message = format!("ralph: auto-stash for plan '{plan_slug}' at {ts}");

    let workdir_owned = workdir.to_path_buf();
    let msg_owned = message.clone();
    let stash = blocking_git(move || git::stash_push_with_untracked(&workdir_owned, &msg_owned))
        .await
        .context("failed to stash dirty working tree before running")?;
    Ok(stash.map(|stash_ref| StashedState {
        stash_ref,
        staged_files,
    }))
}

/// If we stashed at run start, pop the stash on whatever branch the run
/// left us on (typically the plan branch) and re-stage any files that were
/// staged at stash time. Called once at the end of the top-level run
/// regardless of outcome.
///
/// On `stash pop` conflict, we leave the stash on the stack and return a
/// non-zero error — the user pops manually.
async fn restore_working_tree(workdir: &Path, state: Option<&StashedState>) -> Result<()> {
    if let Some(state) = state {
        let workdir_owned = workdir.to_path_buf();
        let stash_owned = state.stash_ref.clone();
        let outcome = blocking_git(move || git::stash_pop(&workdir_owned, &stash_owned)).await?;
        match outcome {
            StashPopOutcome::Clean => {
                // Re-stage any files the user had staged before we stashed.
                // `git stash pop` always restores them as unstaged, so this
                // step is what preserves the staged/unstaged distinction
                // across a run.
                if !state.staged_files.is_empty() {
                    let workdir_owned = workdir.to_path_buf();
                    let staged_owned = state.staged_files.clone();
                    // restage_files is best-effort and never returns an error;
                    // wrap with Ok so blocking_git's signature stays uniform.
                    blocking_git(move || {
                        git::restage_files(&workdir_owned, &staged_owned);
                        Ok(())
                    })
                    .await?;
                }
                if state.staged_files.is_empty() {
                    eprintln!("Restored your uncommitted changes.");
                } else {
                    eprintln!(
                        "Restored your uncommitted changes ({} file(s) re-staged).",
                        state.staged_files.len()
                    );
                }
            }
            StashPopOutcome::Conflicted(stderr) => {
                bail!(
                    "Pop of ralph's stash conflicts with committed work. \
                     Your changes are preserved at {} — resolve manually with \
                     `git stash pop {}`.\n{}",
                    state.stash_ref.as_str(),
                    state.stash_ref.as_str(),
                    stderr,
                );
            }
            StashPopOutcome::NotFound => {
                eprintln!(
                    "Warning: ralph's auto-stash ({}) was no longer on the stack at teardown.",
                    state.stash_ref.as_str(),
                );
            }
        }
    }

    Ok(())
}

/// Set up the git branch for the plan.
///
/// Assumes the working tree is clean at entry — callers must run
/// [`stash_if_dirty`] (or prove cleanliness another way) first.
///
/// If the current branch matches the plan's branch, no action is taken.
/// Otherwise:
/// - If `parent_sha` is `Some`, creates the branch rooted explicitly at that
///   SHA (`git checkout -b <branch> <sha>`). If the branch already exists the
///   parent SHA is ignored and the existing branch is checked out.
/// - If `parent_sha` is `None`, creates the branch from the current HEAD
///   (legacy behavior).
async fn setup_branch(workdir: &Path, plan: &Plan, parent_sha: Option<&str>) -> Result<()> {
    let current = {
        let workdir_owned = workdir.to_path_buf();
        blocking_git(move || git::get_current_branch(&workdir_owned))
            .await
            .context("Failed to get current git branch")?
    };

    if current == plan.branch_name {
        return Ok(());
    }

    // Try to create and checkout the branch. If it already exists,
    // just check it out.
    let create_result = {
        let workdir_owned = workdir.to_path_buf();
        let branch = plan.branch_name.clone();
        let parent = parent_sha.map(|s| s.to_string());
        blocking_git(move || match parent {
            Some(sha) => git::create_branch_from_sha(&workdir_owned, &branch, &sha),
            None => git::create_and_checkout_branch(&workdir_owned, &branch),
        })
        .await
    };

    if create_result.is_err() {
        // Branch might already exist; try a plain checkout.
        checkout_existing_branch(workdir, &plan.branch_name).await?;
    }

    Ok(())
}

/// Checkout an existing branch.
async fn checkout_existing_branch(workdir: &Path, branch: &str) -> Result<()> {
    let output = tokio::process::Command::new("git")
        .args(["checkout", branch])
        .current_dir(workdir)
        .output()
        .await
        .context("Failed to execute git checkout")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to checkout branch '{}': {}", branch, stderr.trim());
    }

    Ok(())
}

/// Run a synchronous `git.rs` operation on the tokio blocking thread pool so
/// that the runtime worker remains free to drive other futures (such as the
/// abort-signal watcher) while the git subprocess runs.
async fn blocking_git<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .context("git worker task panicked")?
}

/// Select which steps to run based on RunOptions.
///
/// Phase 3 defaults:
/// - If `step` is set, return only the next pending/failed/in_progress step.
/// - If `from`/`to` are set, return that inclusive range.
/// - Otherwise, return all remaining steps (the new default).
///
/// `all_plans` is orthogonal to this function and is handled by the
/// multi-plan orchestrator, not the step selector.
///
/// As of the mid-run-insert fix, `run_plan` no longer calls this function
/// directly — it uses sort_key-bound windowing via [`resolve_window`] so that
/// step positions don't drift when new steps are inserted mid-run. The helper
/// is retained under `#[cfg(test)]` because its tests document the intended
/// legacy semantics that the new windowing code must still honor.
#[cfg(test)]
fn select_steps(all_steps: &[Step], options: &RunOptions) -> Result<Vec<Step>> {
    let total = all_steps.len();

    // --one: only run the next actionable step. Aborted is included so
    // that a Ctrl+C'd step is retryable via `--one` without an explicit
    // reset, mirroring how the default (range) path already handles it.
    if options.one {
        let next = all_steps.iter().find(|s| {
            s.status == StepStatus::Pending
                || s.status == StepStatus::Failed
                || s.status == StepStatus::InProgress
                || s.status == StepStatus::Aborted
        });
        return Ok(next.cloned().into_iter().collect());
    }

    // Determine range (1-based, inclusive). When neither `from` nor `to` is
    // provided this yields the full step list (the new "run all" default).
    let from_idx = options.from.unwrap_or(1).saturating_sub(1);
    let to_idx = options.to.unwrap_or(total);

    // Validate range.
    if from_idx >= total {
        bail!(
            "Start step {} is out of range (plan has {} steps)",
            from_idx + 1,
            total
        );
    }

    let end = to_idx.min(total);
    if from_idx >= end {
        return Ok(Vec::new());
    }

    Ok(all_steps[from_idx..end].to_vec())
}

/// Find the 1-based step number of a step within the plan's step list.
fn step_number_in_plan(all_steps: &[Step], step: &Step) -> usize {
    all_steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|i| i + 1)
        .unwrap_or(0)
}

/// Find the resume point: the earliest non-complete step by sort_key order.
///
/// Post-sweep note: `resume_plan` now calls `sweep_stale_in_progress` before
/// invoking this function, so in normal use no `InProgress` rows should ever
/// be visible here. The `InProgress` arm is retained as a belt-and-suspenders
/// guard in case the sweep is ever accidentally bypassed or reordered.
/// Preference order: InProgress > Failed > Aborted > Pending.
fn find_resume_point(steps: &[Step]) -> Result<usize> {
    // Belt-and-suspenders: sweep should have cleared any InProgress, but if
    // something skipped it, still prefer an in_progress step.
    if let Some(idx) = steps
        .iter()
        .position(|s| s.status == StepStatus::InProgress)
    {
        return Ok(idx);
    }

    // Then look for a failed step.
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::Failed) {
        return Ok(idx);
    }

    // Then look for an aborted step (including rows the sweep just wrote).
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::Aborted) {
        return Ok(idx);
    }

    // Check if there are pending steps (plan may not have started yet).
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::Pending) {
        return Ok(idx);
    }

    bail!("No failed, in-progress, or pending steps found to resume from")
}

/// True if a step is in a status that the runner loop will attempt to execute.
/// Pre-existing Complete / Skipped steps are non-actionable.
fn is_actionable(status: StepStatus) -> bool {
    matches!(
        status,
        StepStatus::Pending | StepStatus::Failed | StepStatus::InProgress | StepStatus::Aborted
    )
}

/// Resolved sort_key bounds for a run window.
///
/// `--from`/`--to` are 1-based step numbers from the CLI, but step positions
/// shift when steps are inserted mid-run. We translate the position-based
/// bounds into sort_keys once at run start, then filter each iteration's
/// re-queried step list by sort_key — which is stable across inserts.
#[derive(Debug, Clone)]
struct RunWindow {
    /// Lower-bound sort_key (inclusive). `None` means "from the first step".
    from_key: Option<String>,
    /// Upper-bound sort_key (inclusive). `None` means "to the last step".
    to_key: Option<String>,
}

impl RunWindow {
    /// True if a step's sort_key falls within this window.
    fn contains_key(&self, sort_key: &str) -> bool {
        if let Some(ref from) = self.from_key
            && sort_key < from.as_str()
        {
            return false;
        }
        if let Some(ref to) = self.to_key
            && sort_key > to.as_str()
        {
            return false;
        }
        true
    }
}

/// Resolve the run window (sort_key bounds) from [`RunOptions`] against the
/// plan's current step list.
///
/// Validates that `--from` / `--to` are in range. For `--one`, the window is
/// still the full plan — the `one_target_id` captured at run start is what
/// enforces the single-step semantics, not the window.
fn resolve_window(all_steps: &[Step], options: &RunOptions) -> Result<RunWindow> {
    let total = all_steps.len();

    if let Some(from) = options.from
        && (from == 0 || from > total)
    {
        bail!(
            "Start step {} is out of range (plan has {} steps)",
            from,
            total
        );
    }
    if let Some(to) = options.to
        && (to == 0 || to > total)
    {
        bail!("End step {} is out of range (plan has {} steps)", to, total);
    }
    if let (Some(from), Some(to)) = (options.from, options.to)
        && from > to
    {
        bail!("Start step {} is greater than end step {}", from, to);
    }

    let from_key = options.from.map(|n| all_steps[n - 1].sort_key.clone());
    let to_key = options.to.map(|n| all_steps[n - 1].sort_key.clone());

    Ok(RunWindow { from_key, to_key })
}

// ---------------------------------------------------------------------------
// Topological scheduler (docs/dag-redesign.md §3.5)
// ---------------------------------------------------------------------------
//
// A plan is a DAG of steps (§3.1). The runner no longer walks a flat
// sort_key list; each tick it computes the *runnable set* and picks the
// next step by a deterministic tie-break. Phase 1 implements scheduling
// only — interruptions (Phase 2), reviews/concurrency (Phase 3) layer on
// later. With no dependency edges every step has topological depth 0, so
// the tie-break collapses to sort_key order and a linear plan executes
// byte-identically to today.

/// Topological depth of every step in `steps`.
///
/// `depth(s) = 0` when `s` has no in-scope dependencies; otherwise
/// `1 + max(depth(d))` over the in-scope deps `d`. Edges pointing outside
/// `steps` (e.g. a deleted prerequisite — `ON DELETE CASCADE` prevents
/// this in the DB, but tests and defensive code may hit it) are ignored
/// for depth. Memoized; the DAG-acyclicity hard invariant
/// (`storage::would_create_step_cycle` on every edge mutation, V25
/// backfill is a chain) guarantees termination, but a `visiting` guard
/// still bounds recursion to 0 if acyclicity is ever violated.
pub(crate) fn compute_step_depths(
    steps: &[Step],
    deps_of: &HashMap<String, Vec<String>>,
) -> HashMap<String, usize> {
    fn depth_of(
        id: &str,
        deps_of: &HashMap<String, Vec<String>>,
        in_scope: &HashSet<String>,
        memo: &mut HashMap<String, usize>,
        visiting: &mut HashSet<String>,
    ) -> usize {
        if let Some(d) = memo.get(id) {
            return *d;
        }
        if !visiting.insert(id.to_string()) {
            // Cycle guard: acyclicity is a hard invariant; never recurse
            // forever if it is somehow violated.
            return 0;
        }
        let depth = deps_of
            .get(id)
            .map(|deps| {
                deps.iter()
                    .filter(|d| in_scope.contains(d.as_str()))
                    .map(|d| 1 + depth_of(d, deps_of, in_scope, memo, visiting))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        visiting.remove(id);
        memo.insert(id.to_string(), depth);
        depth
    }

    let in_scope: HashSet<String> = steps.iter().map(|s| s.id.clone()).collect();
    let mut memo: HashMap<String, usize> = HashMap::new();
    let mut visiting: HashSet<String> = HashSet::new();
    for s in steps {
        depth_of(&s.id, deps_of, &in_scope, &mut memo, &mut visiting);
    }
    memo
}

/// The deterministic scheduler tie-break ordering of two steps:
/// `(topological depth, sort_key, short_id)` (docs/dag-redesign.md §3.5
/// item 4). Depth first so a prerequisite always outranks its dependents,
/// then the authored `sort_key`, then `short_id` as a stable final
/// discriminator. With no edges depth is uniformly 0 and this collapses to
/// "sort_key order" — the exact pre-DAG linear behavior.
///
/// This is the SINGLE definition of scheduler order. [`pick_next_step`]
/// (execution) and the Phase-4 outline projection
/// (`crate::tui::outline`, presentation) both call it so the drawn outline
/// is byte-for-byte the execution order — there is no second, divergent
/// sort anywhere in the codebase.
pub(crate) fn step_schedule_cmp(
    a: &Step,
    b: &Step,
    depths: &HashMap<String, usize>,
) -> std::cmp::Ordering {
    let da = depths.get(&a.id).copied().unwrap_or(0);
    let db = depths.get(&b.id).copied().unwrap_or(0);
    da.cmp(&db)
        .then_with(|| a.sort_key.cmp(&b.sort_key))
        .then_with(|| a.short_id.cmp(&b.short_id))
}

/// True when every dependency of `step_id` that is *in the run window* is
/// `Complete`.
///
/// `window_status` holds only steps whose sort_key falls inside the run
/// window. A dependency absent from it is treated as **satisfied** — that
/// covers two cases that must both be non-blocking:
///
///  - The dep is outside the `--from`/`--to` window: the user explicitly
///    bounded the run and is asserting the excluded steps' preconditions
///    hold. This is exactly today's `--from`/`--to` behavior (the old
///    linear loop never checked whether earlier steps were done) and is a
///    hard parity requirement of this step.
///  - The dep is out-of-scope / deleted: the graph must not deadlock on a
///    prerequisite that no longer exists. `ON DELETE CASCADE` makes this
///    unreachable from the DB; it only matters for the pure unit tests and
///    as defense-in-depth.
///
/// (§3.1: runnable ⇔ every dependency `Complete`. Reviews/§10 come in
/// Phase 3 and are not consulted here. A non-`Complete` terminal dep
/// inside the window — e.g. `Skipped` — does *not* satisfy the edge; the
/// skip-with-dependents semantics are the deferred §14 open question and
/// are intentionally not special-cased here.)
fn deps_satisfied(
    step_id: &str,
    deps_of: &HashMap<String, Vec<String>>,
    window_status: &HashMap<&str, StepStatus>,
) -> bool {
    let Some(deps) = deps_of.get(step_id) else {
        return true;
    };
    deps.iter().all(|d| match window_status.get(d.as_str()) {
        Some(status) => *status == StepStatus::Complete,
        None => true,
    })
}

/// The set of step ids in `plan_id` that currently have at least one open
/// interruption — i.e. the steps the derived `Blocked` overlay shadows
/// (docs/dag-redesign.md §3.4). Recomputed every scheduler tick so a
/// cross-process resolution (a CLI/TUI in another process flipping the row
/// to `resolved`) drops the step out of the blocked set and back into the
/// runnable set at the very next tick — the bridge of §9 invariant 4.
fn blocked_step_ids(conn: &Connection, plan_id: &str) -> Result<HashSet<String>> {
    Ok(storage::list_open_interruptions_for_plan(conn, plan_id)?
        .into_iter()
        .map(|i| i.step_id)
        .collect())
}

/// One scheduler tick: pick the next step to execute, or `None` when the
/// runnable set is empty.
///
/// Runnable ⇔ in the run window, an actionable status (not terminal —
/// [`is_actionable`]), **not `Blocked`** (no open interruption shadows it —
/// docs/dag-redesign.md §3.4/§3.5: a blocked branch is excluded from the
/// runnable set so its dependents wait while the scheduler picks another
/// branch), not already executed this invocation, and every dependency
/// `Complete` ([`deps_satisfied`]). Among runnable steps the deterministic
/// tie-break is `(topological depth, sort_key, short_id)` (§3.5 item 4):
/// depth first so a prerequisite always outranks its dependents, then the
/// authored sort_key, then short_id as a stable final discriminator.
/// Concurrency (§3.5 items 2–3) is Phase 3 — this returns a single step.
///
/// `blocked_step_ids` is the set of step ids that currently have an open
/// interruption. It is the *only* place the derived `Blocked` overlay
/// gates scheduling: the stored status after a pause is `Pending`
/// (actionable), so without this exclusion the scheduler would re-pick the
/// blocked step forever. An empty set (no interruptions) reproduces the
/// pre-interruption behavior exactly, so a linear plan is unaffected.
fn pick_next_step<'a>(
    all_steps: &'a [Step],
    deps_of: &HashMap<String, Vec<String>>,
    depths: &HashMap<String, usize>,
    window: &RunWindow,
    executed_step_ids: &HashSet<String>,
    blocked_step_ids: &HashSet<String>,
) -> Option<&'a Step> {
    // Status of every step *inside the run window* only — deps outside the
    // window must not gate (preserves `--from`/`--to`; see `deps_satisfied`).
    let window_status: HashMap<&str, StepStatus> = all_steps
        .iter()
        .filter(|s| window.contains_key(&s.sort_key))
        .map(|s| (s.id.as_str(), s.status))
        .collect();

    all_steps
        .iter()
        .filter(|s| {
            window.contains_key(&s.sort_key)
                && is_actionable(s.status)
                && !blocked_step_ids.contains(&s.id)
                && !executed_step_ids.contains(&s.id)
                && deps_satisfied(&s.id, deps_of, &window_status)
        })
        .min_by(|a, b| step_schedule_cmp(a, b, depths))
}

/// Drain finished concurrent reviews as the SOLE DAG writer
/// (docs/dag-redesign.md §3.5 item 3 / §9-inv-3 / §10).
///
/// Called by the orchestrator (`run_plan`) at every scheduler tick. The
/// review subprocesses run detached in `reviews` (a `JoinSet`) — `Send`,
/// holding no `Connection` and no impl permit, so they overlap whatever the
/// scheduler implements next. This is the *only* place review verdicts hit
/// the DB / git-notes / the §10 DAG mutation, all serialized on the single
/// scheduler loop so the §9-inv-3 single-writer guarantee holds even with a
/// review in flight.
///
/// `block`:
///  - `false` (mid-tick): non-blockingly reap *every already-finished*
///    review (`try_join_next`), then return. Reviews still running are left
///    in the set for a later tick — the scheduler keeps implementing
///    unrelated work meanwhile (the §3.5-item-3 concurrency payoff).
///  - `true` (runnable set empty / post-loop): the runnable set cannot grow
///    without a review returning, so `await` the *next* finished review
///    (`join_next`), drain it (and any other already-finished ones), then
///    return. This is what lets a passed review un-gate dependents / a
///    failed review's corrective step re-enter scheduling, and guarantees
///    the plan is never finalized with a review pending (§3.3).
///
/// For each finished review it calls [`crate::review::finalize_review`] (the
/// sole-writer verdict sink): `Pass` ⇒ promote the reviewed step
/// `InProgress → Complete` (its dependents become runnable next tick);
/// `Fail` ⇒ the V29 bridge row is written, then every open corrective
/// request for the plan is consumed via
/// [`crate::review::consume_corrective_request`] (§10 insert + re-parent +
/// recursion cap), oldest-first for deterministic, reproducible scheduling
/// (§3.5 item 4 / §11). A review **error** (e.g. the §9-inv-2 read-only
/// invariant fired, or a misconfigured review harness) or a task **panic**
/// must never silently pass un-reviewed work: the plan is marked `Failed`
/// and `Some(PlanStatus::Failed)` is returned so the caller stops the run,
/// exactly as a hard step failure does.
///
/// Returns `Ok(None)` on success (drained, or nothing to drain) and
/// `Ok(Some(final_status))` when the run must terminate.
async fn drain_finished_reviews(
    conn: &Connection,
    plan: &Plan,
    reviews: &mut tokio::task::JoinSet<crate::review::SpawnedReview>,
    workdir: &Path,
    out: &OutputContext,
    block: bool,
) -> Result<Option<PlanStatus>> {
    if reviews.is_empty() {
        return Ok(None);
    }

    // If asked to block, wait for the first one; otherwise reap only what is
    // already done. After the (optional) blocking wait, greedily drain every
    // other already-finished task in this same call.
    let mut joined: Vec<std::result::Result<crate::review::SpawnedReview, tokio::task::JoinError>> =
        Vec::new();
    if block {
        match reviews.join_next().await {
            Some(j) => joined.push(j),
            None => return Ok(None), // emptied concurrently — nothing to do
        }
    }
    while let Some(j) = reviews.try_join_next() {
        joined.push(j);
    }

    for j in joined {
        let crate::review::SpawnedReview {
            step_id,
            iteration,
            result,
        } = match j {
            Ok(sr) => sr,
            Err(join_err) => {
                // A panicked / aborted review task: the task died before it
                // could hand back even its step id, so a targeted recovery
                // isn't possible. Fail-safe — never pass un-reviewed work.
                eprintln!("Review task failed to complete: {join_err}");
                storage::update_plan_status(conn, &plan.id, PlanStatus::Failed)?;
                return Ok(Some(PlanStatus::Failed));
            }
        };
        let review = match result {
            Ok(r) => r,
            Err(e) => {
                // The review SUBPROCESS errored (the §9-inv-2 read-only
                // invariant fired, or the review harness is misconfigured) —
                // the reviewer never produced a verdict. But the
                // implementation itself SUCCEEDED and is committed; the step
                // is `InProgress` + `review_status = InFlight`. Surface this
                // cleanly instead of letting the next run's stale-InProgress
                // sweep silently RE-IMPLEMENT an implementation-complete step:
                //
                //   * reset `review_status` InFlight -> Pending so there is no
                //     phantom in-flight reviewer for a resume to trip over;
                //   * raise ONE kind=blocker interruption on the reviewed step
                //     so the failure is visible (`ralph interruption list` /
                //     the TUI inbox) and — crucially — the step renders
                //     derived `Blocked`: that gates every dependent AND keeps
                //     the stale-InProgress sweep from aborting+re-implementing
                //     it (the sweep now skips steps with an open interruption).
                //     Resolving the blocker re-runs the step from a clean
                //     state (re-implement + re-review) — safe and explicit,
                //     never silent.
                //
                // The plan is still marked `Failed` and the run still stops,
                // exactly as a hard step failure does (§9-inv-2 — never pass
                // un-reviewed work). The blocker is what makes the *next* run
                // recover cleanly rather than discard correct committed work.
                eprintln!("Review failed: {e:#}");
                if storage::get_step_by_id(conn, &step_id)?.is_some() {
                    crate::db::with_tx(conn, |conn| {
                        storage::update_step_review_status(
                            conn,
                            &step_id,
                            crate::plan::ReviewStatus::Pending,
                        )?;
                        storage::insert_interruption(
                            conn,
                            &step_id,
                            iteration,
                            crate::plan::InterruptionKind::Blocker,
                            &format!(
                                "review could not run for this step: {e:#}. The \
                                 implementation is committed but UNREVIEWED. Fix the \
                                 review configuration if it is misconfigured, then \
                                 resolve this blocker — ralph will re-run \
                                 (re-implement and re-review) this step from a clean \
                                 state. ralph will not continue on unreviewed work."
                            ),
                            &[],
                        )?;
                        Ok(())
                    })?;
                }
                storage::update_plan_status(conn, &plan.id, PlanStatus::Failed)?;
                return Ok(Some(PlanStatus::Failed));
            }
        };

        // SOLE DB writer: finalize the verdict (status + git-note +, on
        // FAIL, the V29 bridge row).
        match crate::review::finalize_review(conn, workdir, &review, out)? {
            crate::review::ReviewOutcome::Passed => {
                // The reviewed step was held `InProgress` during its
                // concurrent-eligible review window; PASS ⇒ it is genuinely
                // done. Promote to `Complete` so its dependents become
                // runnable on the next tick.
                storage::update_step_status(conn, &review.step_id, StepStatus::Complete)?;
            }
            crate::review::ReviewOutcome::Failed { .. } => {
                // The reviewer only *requested* a corrective step (V29
                // bridge row). Consumed just below as the sole writer.
            }
            crate::review::ReviewOutcome::Discarded => {
                // The reviewed step was removed (CASCADE) while its review
                // was in flight; the verdict is for a step that no longer
                // exists. Nothing to promote, no corrective to request —
                // skip it rather than failing the whole run.
            }
        }
    }

    // Drain corrective-step requests for this plan as the SOLE DAG writer
    // (§9-inv-3 / §10). Oldest-first, deterministic (reproducible
    // scheduling — §3.5 item 4 / §11).
    for req in storage::list_open_corrective_step_requests_for_plan(conn, &plan.id)? {
        crate::review::consume_corrective_request(conn, plan, &req, out)?;
    }
    Ok(None)
}

/// Sweep any stale InProgress step rows and emit a log line if the sweep
/// actually touched anything. Shared between [`run_plan`] and [`resume_plan`].
fn sweep_and_log_stale_in_progress(
    conn: &Connection,
    plan: &Plan,
    out: &OutputContext,
) -> Result<Vec<Step>> {
    let swept = storage::sweep_stale_in_progress(conn, &plan.id)?;
    if swept.is_empty() {
        return Ok(swept);
    }

    // Resolve step numbers from the post-sweep step list (sort_keys are
    // stable, so positions line up).
    let all_steps = storage::list_steps(conn, &plan.id)?;
    if out.format == OutputFormat::Json {
        let events: Vec<output::StaleStep> = swept
            .iter()
            .map(|s| output::StaleStep {
                step_id: s.id.clone(),
                step_num: step_number_in_plan(&all_steps, s),
                title: s.title.clone(),
            })
            .collect();
        output::emit_ndjson(&RunEvent::StaleStepsSwept { steps: events })?;
    } else {
        let summary = format_step_list(&swept, &all_steps);
        eprintln!(
            "> Swept {} stale in_progress step(s) from prior crashed run: {}",
            swept.len(),
            summary
        );
    }
    Ok(swept)
}

/// Report newly-inserted steps (the plan grew between runner iterations).
fn report_plan_grew(new_inserts: &[Step], all_steps: &[Step], out: &OutputContext) -> Result<()> {
    if out.format == OutputFormat::Json {
        let events: Vec<output::StaleStep> = new_inserts
            .iter()
            .map(|s| output::StaleStep {
                step_id: s.id.clone(),
                step_num: step_number_in_plan(all_steps, s),
                title: s.title.clone(),
            })
            .collect();
        output::emit_ndjson(&RunEvent::PlanGrew { steps: events })?;
    } else {
        let summary = format_step_list(new_inserts, all_steps);
        eprintln!(
            "> Plan grew: +{} step(s) ({}) inserted mid-run",
            new_inserts.len(),
            summary
        );
    }
    Ok(())
}

/// Format a list of steps as `#N 'title', #M 'title'` for log messages.
fn format_step_list(steps: &[Step], all_steps: &[Step]) -> String {
    steps
        .iter()
        .map(|s| format!("#{} '{}'", step_number_in_plan(all_steps, s), s.title))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Find the current step: first step that is not complete or skipped.
fn find_current_step(steps: &[Step]) -> Result<usize> {
    steps
        .iter()
        .position(|s| s.status != StepStatus::Complete && s.status != StepStatus::Skipped)
        .context("All steps are already complete or skipped")
}

/// Produce a dry-run report without executing anything.
fn dry_run_report(plan: &Plan, all_steps: &[Step], steps_to_run: &[Step]) -> Result<PlanRunResult> {
    println!("Dry run for plan '{}':", plan.slug);
    println!("  Branch: {}", plan.branch_name);
    if !plan.deterministic_tests.is_empty() {
        println!("  Tests:  {}", plan.deterministic_tests.join(", "));
    }
    println!();

    for (i, step) in steps_to_run.iter().enumerate() {
        let step_num = step_number_in_plan(all_steps, step);
        let status_label = match step.status {
            StepStatus::Pending => "WOULD RUN",
            StepStatus::Complete => "SKIP (complete)",
            StepStatus::Skipped => "SKIP (skipped)",
            StepStatus::Failed => "WOULD RETRY",
            StepStatus::InProgress => "WOULD RESUME",
            StepStatus::Aborted => "WOULD RETRY",
            // Derived overlay; never stored, so a dry run (which reads stored
            // statuses) won't normally see it.
            StepStatus::Blocked => "BLOCKED (open interruption)",
        };
        println!(
            "  [{}/{}] Step {}: {} [{}]",
            i + 1,
            steps_to_run.len(),
            step_num,
            step.title,
            status_label
        );
    }

    Ok(PlanRunResult {
        plan_slug: plan.slug.clone(),
        steps_executed: 0,
        steps_succeeded: 0,
        steps_failed: 0,
        steps_skipped: 0,
        // Dry run does not mutate state; report the projected status assuming
        // every step that would run succeeds.
        final_status: PlanStatus::Complete,
        step_results: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use chrono::Utc;

    fn setup() -> Connection {
        db::open_memory().expect("open_memory")
    }

    fn make_plan(harness: Option<&str>) -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "test-plan".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "feat/test".to_string(),
            description: "A test plan".to_string(),
            status: PlanStatus::Ready,
            harness: harness.map(|s| s.to_string()),
            agent: None,
            deterministic_tests: vec!["cargo test".to_string()],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        }
    }

    // -- validate_plan_status tests --

    #[test]
    fn test_validate_plan_status_ready() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Ready;
        assert!(validate_plan_status(&plan).is_ok());
    }

    #[test]
    fn test_validate_plan_status_in_progress() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::InProgress;
        assert!(validate_plan_status(&plan).is_ok());
    }

    #[test]
    fn test_validate_plan_status_failed_allows_retry() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Failed;
        assert!(validate_plan_status(&plan).is_ok());
    }

    #[test]
    fn test_validate_plan_status_planning_rejected() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Planning;
        let err = validate_plan_status(&plan).unwrap_err();
        assert!(err.to_string().contains("planning"));
    }

    #[test]
    fn test_validate_plan_status_complete_rejected() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Complete;
        let err = validate_plan_status(&plan).unwrap_err();
        assert!(err.to_string().contains("complete"));
    }

    #[test]
    fn test_validate_plan_status_aborted_allows_resume() {
        // Regression: previously this was rejected with a "use `resume`"
        // hint, but `resume_plan` itself routes through `run_plan` →
        // `validate_plan_status`, so the rejection made aborted plans
        // unresumable. Aborted must be a runnable state.
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Aborted;
        assert!(validate_plan_status(&plan).is_ok());
    }

    // -- select_steps tests --

    fn make_steps(n: usize) -> Vec<Step> {
        (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                short_id: String::new(),
                plan_id: "p1".to_string(),
                sort_key: format!("a{i}"),
                title: format!("Step {}", i + 1),
                description: format!("Description {}", i + 1),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                status: StepStatus::Pending,
                attempts: 0,
                max_retries: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                model: None,
                skipped_reason: None,
                change_policy: crate::plan::ChangePolicy::Required,
                tags: vec![],
                retry_strategy: None,
                review_enabled: None,
                review_status: None,
                corrects_step_id: None,
            })
            .collect()
    }

    #[test]
    fn test_select_steps_default_returns_all_remaining() {
        // Phase 3: the default (no flags) now means "all remaining steps".
        let steps = make_steps(3);
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_steps_step_flag_returns_only_next() {
        // Phase 3: `one: true` returns just the next pending step.
        let steps = make_steps(3);
        let options = RunOptions {
            one: true,
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s0");
    }

    #[test]
    fn test_select_steps_from_to() {
        let steps = make_steps(5);
        let options = RunOptions {
            from: Some(2),
            to: Some(4),
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3); // steps 2, 3, 4
        assert_eq!(selected[0].id, "s1");
        assert_eq!(selected[2].id, "s3");
    }

    #[test]
    fn test_select_steps_from_only() {
        let steps = make_steps(5);
        let options = RunOptions {
            from: Some(3),
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3); // steps 3, 4, 5
    }

    #[test]
    fn test_select_steps_step_flag_skips_completed() {
        // `--step` should skip already-complete steps and pick the next pending.
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        let options = RunOptions {
            one: true,
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s1");
    }

    #[test]
    fn test_select_steps_step_flag_none_pending() {
        // When all steps are complete, `--step` returns empty.
        let mut steps = make_steps(2);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Complete;
        let options = RunOptions {
            one: true,
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_steps_default_returns_all_even_completed() {
        // With the new default (no flags), select_steps returns the full slice;
        // it's up to run_plan itself to skip already-completed steps at
        // execution time.
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_steps_out_of_range() {
        let steps = make_steps(3);
        let options = RunOptions {
            from: Some(10),
            ..Default::default()
        };
        let result = select_steps(&steps, &options);
        assert!(result.is_err());
    }

    // -- find_resume_point tests --

    #[test]
    fn test_find_resume_point_in_progress() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::InProgress;
        let idx = find_resume_point(&steps).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn test_find_resume_point_failed() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Failed;
        let idx = find_resume_point(&steps).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn test_find_resume_point_prefers_in_progress_over_failed() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Failed;
        steps[1].status = StepStatus::InProgress;
        let idx = find_resume_point(&steps).unwrap();
        assert_eq!(idx, 1); // in_progress takes priority
    }

    #[test]
    fn test_find_resume_point_pending() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        let idx = find_resume_point(&steps).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn test_find_resume_point_all_complete() {
        let mut steps = make_steps(2);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Complete;
        let result = find_resume_point(&steps);
        assert!(result.is_err());
    }

    #[test]
    fn test_resume_resets_aborted_step_to_pending() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        let (s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s2.id, StepStatus::Aborted).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let resume_idx = find_resume_point(&steps).unwrap();
        assert_eq!(resume_idx, 1);

        let step = &steps[resume_idx];
        assert_eq!(step.status, StepStatus::Aborted);

        // Replicate the reset condition from resume_plan
        if step.status == StepStatus::Failed
            || step.status == StepStatus::InProgress
            || step.status == StepStatus::Aborted
        {
            storage::reset_step(&conn, &step.id).unwrap();
        }

        let refreshed = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(refreshed[resume_idx].status, StepStatus::Pending);
    }

    // -- find_current_step tests --

    #[test]
    fn test_find_current_step() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Skipped;
        let idx = find_current_step(&steps).unwrap();
        assert_eq!(idx, 2);
    }

    #[test]
    fn test_find_current_step_all_done() {
        let mut steps = make_steps(2);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Skipped;
        let result = find_current_step(&steps);
        assert!(result.is_err());
    }

    // -- skip_step tests --

    #[test]
    fn test_skip_step_by_number() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let skipped = skip_step(
            &conn,
            &plan,
            Some(2),
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 2);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Skipped);
    }

    #[test]
    fn test_skip_step_current() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Mark first as complete so current is "Second".
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let skipped = skip_step(
            &conn,
            &plan,
            None,
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 2);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Skipped);
    }

    /// STEP 16: `skip_step` against a step that is in-flight in THIS
    /// process must route through the cancel channel (signalling
    /// `Skipped`, which kicks the kill ladder) rather than flipping the DB
    /// status itself — the executor owns the status/log write on that
    /// path. The step must therefore stay `InProgress` here (no executor
    /// is running in this unit test to consume the signal), and the cancel
    /// channel must receive exactly `Some(CancelReason::Skipped)`.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_skip_step_in_flight_routes_through_cancel_channel() {
        // Holding the std::Mutex guard across .await serializes the
        // process-wide cancel registry / in-flight flag against the
        // signal-module tests that touch the same globals. The
        // current_thread runtime rules out cross-thread guard transfer.
        let _guard = crate::signal::lock_exit_cleanup_test();

        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::InProgress).unwrap();

        // Register a cancel sender for this process (as the signal listener
        // would for a real run) and mark a step in-flight.
        let (_handle, mut rx) = crate::signal::install_and_spawn_with_handle();
        let _in_flight = crate::signal::StepInFlightGuard::enter();

        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        // The cancel channel received the Skipped reason (distinct from
        // the Aborted the SIGINT/SIGTERM listener would send).
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(crate::signal::CancelReason::Skipped));

        // skip_step must NOT have flipped the status — the executor owns
        // that on the in-flight path.
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].status,
            StepStatus::InProgress,
            "in-flight skip must defer the status flip to the executor"
        );
    }

    /// Counterpart: with NO step in-flight (the common `ralph skip` case
    /// where the runner is a different process, or nothing is running),
    /// `skip_step` keeps the original synchronous DB-flip behavior.
    #[test]
    fn test_skip_step_not_in_flight_flips_db() {
        let _guard = crate::signal::lock_exit_cleanup_test();
        crate::signal::set_step_in_flight(false);

        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::InProgress).unwrap();

        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].status,
            StepStatus::Skipped,
            "no in-flight step → plain DB flip"
        );
    }

    #[test]
    fn test_skip_step_stale_in_progress_ignores_other_plan_live_run() {
        let _guard = crate::signal::lock_exit_cleanup_test();
        crate::signal::set_step_in_flight(false);

        let conn = setup();
        let project = "/p";
        let plan =
            storage::create_plan(&conn, "target", project, "b", "d", None, None, &[]).unwrap();
        let other =
            storage::create_plan(&conn, "other", project, "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::InProgress).unwrap();

        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![project, 42_i64, other.id, other.slug],
        )
        .unwrap();

        // Use Commit here so the test exercises the new stale-InProgress +
        // Commit fallthrough (has_uncommitted_changes returns false on the
        // non-repo /p project, so we take the safe no-op arm but still cover
        // the branch and the "no DB request left" assertions).
        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Commit,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].status,
            StepStatus::Skipped,
            "a run lock for a different plan must not turn stale InProgress into a DB skip request"
        );
        assert!(
            storage::peek_skip_request(&conn, &plan.id)
                .unwrap()
                .is_none(),
            "no runner is polling this plan, so skip_step must not leave an orphaned request"
        );
    }

    #[test]
    fn test_skip_step_rejects_complete() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let result = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Stash,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_out_of_range() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let result = skip_step(
            &conn,
            &plan,
            Some(5),
            None,
            crate::git::ParkStrategyKind::Stash,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_persists_reason() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            Some("redundant after H7"),
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Skipped);
        assert_eq!(
            steps[0].skipped_reason.as_deref(),
            Some("redundant after H7")
        );
    }

    #[test]
    fn test_skip_step_no_reason_stores_null() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Skipped);
        assert!(steps[0].skipped_reason.is_none());
    }

    #[test]
    fn test_reset_clears_skipped_reason() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        skip_step(
            &conn,
            &plan,
            Some(1),
            Some("because"),
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        storage::reset_step(&conn, &s1.id).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
        assert!(steps[0].skipped_reason.is_none());
    }

    #[test]
    fn test_skip_step_allows_failed() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Failed).unwrap();

        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Skipped);
    }

    /// STEP 17: skipping a step that is NOT currently running must leave
    /// the working tree completely alone — those changes aren't causally
    /// tied to the skip — even when a non-default `--changes` is passed.
    /// Only the DB status flips.
    #[test]
    fn test_skip_non_running_step_ignores_changes_and_leaves_tree() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();

        // A dirty working tree the user has unrelated to any step.
        fs::write(dir.join("README.md"), "# locally edited").unwrap();
        fs::write(dir.join("scratch.txt"), "user scratch").unwrap();
        assert!(git::has_uncommitted_changes(&dir).unwrap());
        let before = git::get_all_changed_files(&dir).unwrap();

        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "s",
            &dir.to_string_lossy(),
            "b",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        // Pending (NOT in-flight). request_skip_in_flight will be a no-op
        // because no step is in-flight in this process.
        assert_eq!(s1.status, StepStatus::Pending);

        // A non-default --changes: discard. It must have NO effect because
        // the step isn't running.
        let skipped = skip_step(
            &conn,
            &plan,
            Some(1),
            None,
            crate::git::ParkStrategyKind::Discard,
        )
        .unwrap();
        assert_eq!(skipped, 1);

        // DB flipped…
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Skipped);

        // …but the working tree is byte-for-byte untouched: no rollback,
        // no stash, no commit.
        assert!(git::has_uncommitted_changes(&dir).unwrap());
        assert_eq!(git::get_all_changed_files(&dir).unwrap(), before);
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# locally edited"
        );
        assert_eq!(
            fs::read_to_string(dir.join("scratch.txt")).unwrap(),
            "user scratch"
        );
        // No ralph-skip stash was created.
        let stash = std::process::Command::new("git")
            .args(["stash", "list"])
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&stash.stdout).trim().is_empty(),
            "non-running skip must not create a stash"
        );
    }

    // -- step_number_in_plan tests --

    #[test]
    fn test_step_number_in_plan() {
        let steps = make_steps(3);
        assert_eq!(step_number_in_plan(&steps, &steps[0]), 1);
        assert_eq!(step_number_in_plan(&steps, &steps[2]), 3);
    }

    // -- dry_run_report tests --

    #[test]
    fn test_dry_run_report() {
        let plan = make_plan(None);
        let all_steps = make_steps(3);
        let result = dry_run_report(&plan, &all_steps, &all_steps).unwrap();
        assert_eq!(result.steps_executed, 0);
        assert_eq!(result.plan_slug, "test-plan");
        // Projected status is not the plan's current status (Ready); it reflects
        // the outcome of a successful run.
        assert_ne!(result.final_status, PlanStatus::Ready);
        assert_eq!(result.final_status, PlanStatus::Complete);
    }

    // -- RunOptions default --

    #[test]
    fn test_run_options_default() {
        let opts = RunOptions::default();
        assert!(!opts.all_plans);
        assert!(!opts.one);
        assert!(opts.from.is_none());
        assert!(opts.to.is_none());
        assert!(!opts.current_branch);
        assert!(opts.harness_override.is_none());
        assert!(!opts.dry_run);
    }

    // -- Integration test: plan status transitions --

    #[test]
    fn test_plan_status_transitions_in_storage() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        // planning -> ready
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p")
            .unwrap()
            .unwrap();
        assert_eq!(p.status, PlanStatus::Ready);

        // ready -> in_progress
        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p")
            .unwrap()
            .unwrap();
        assert_eq!(p.status, PlanStatus::InProgress);

        // in_progress -> complete
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Complete).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p")
            .unwrap()
            .unwrap();
        assert_eq!(p.status, PlanStatus::Complete);
    }

    #[test]
    fn test_plan_status_failed_transition() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Failed).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p")
            .unwrap()
            .unwrap();
        assert_eq!(p.status, PlanStatus::Failed);
    }

    // -- step status transitions --

    #[test]
    fn test_step_status_transitions() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
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

        // pending -> in_progress
        storage::update_step_status(&conn, &step.id, StepStatus::InProgress).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::InProgress);

        // in_progress -> complete
        storage::update_step_status(&conn, &step.id, StepStatus::Complete).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Complete);
    }

    #[test]
    fn test_step_status_failed_and_skipped() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
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

        storage::update_step_status(&conn, &step.id, StepStatus::Failed).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Failed);

        storage::update_step_status(&conn, &step.id, StepStatus::Skipped).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Skipped);
    }

    // -- select_steps with mixed statuses --

    #[test]
    fn test_select_steps_step_picks_failed_as_next() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Failed;
        let options = RunOptions {
            one: true,
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s1"); // the failed step
    }

    #[test]
    fn test_select_steps_to_only() {
        let steps = make_steps(5);
        let options = RunOptions {
            to: Some(3),
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3);
    }

    // -- compute_branch_plan tests (pure helper for run_all_plans) --

    #[test]
    fn test_compute_branch_plan_no_deps_uses_run_start_sha() {
        let topo = vec!["a".to_string()];
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        deps_of.insert("a".to_string(), vec![]);
        let tip = HashMap::new();
        let plan = compute_branch_plan(&topo, 0, &deps_of, &tip, "SHA_START", false);
        assert_eq!(plan.parent_sha.as_deref(), Some("SHA_START"));
        assert!(plan.merge_shas.is_empty());
    }

    #[test]
    fn test_compute_branch_plan_current_branch_skips_parent() {
        let topo = vec!["a".to_string()];
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        deps_of.insert("a".to_string(), vec![]);
        let tip = HashMap::new();
        let plan = compute_branch_plan(&topo, 0, &deps_of, &tip, "SHA_START", true);
        assert_eq!(plan.parent_sha, None);
        assert!(plan.merge_shas.is_empty());
    }

    #[test]
    fn test_compute_branch_plan_single_dep_uses_dep_tip() {
        // b depends on a; a's tip is captured as SHA_A.
        let topo = vec!["a".to_string(), "b".to_string()];
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        deps_of.insert("a".to_string(), vec![]);
        deps_of.insert("b".to_string(), vec!["a".to_string()]);
        let mut tip = HashMap::new();
        tip.insert("a".to_string(), "SHA_A".to_string());

        let plan = compute_branch_plan(&topo, 1, &deps_of, &tip, "SHA_START", false);
        assert_eq!(plan.parent_sha.as_deref(), Some("SHA_A"));
        assert!(plan.merge_shas.is_empty());
    }

    #[test]
    fn test_compute_branch_plan_multiple_deps_picks_most_recent() {
        // c depends on both a and b; topo is [a, b, c], so b is "more recent" than a.
        // c should branch off b's SHA and merge a's SHA.
        let topo = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        deps_of.insert("a".to_string(), vec![]);
        deps_of.insert("b".to_string(), vec![]);
        deps_of.insert("c".to_string(), vec!["a".to_string(), "b".to_string()]);
        let mut tip = HashMap::new();
        tip.insert("a".to_string(), "SHA_A".to_string());
        tip.insert("b".to_string(), "SHA_B".to_string());

        let plan = compute_branch_plan(&topo, 2, &deps_of, &tip, "SHA_START", false);
        assert_eq!(plan.parent_sha.as_deref(), Some("SHA_B"));
        assert_eq!(plan.merge_shas, vec!["SHA_A".to_string()]);
    }

    #[test]
    fn test_compute_branch_plan_ignores_out_of_scope_deps() {
        // c depends on a and on "ext" (which is NOT in the topo list).
        // Only a should be considered.
        let topo = vec!["a".to_string(), "c".to_string()];
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        deps_of.insert("a".to_string(), vec![]);
        deps_of.insert("c".to_string(), vec!["a".to_string(), "ext".to_string()]);
        let mut tip = HashMap::new();
        tip.insert("a".to_string(), "SHA_A".to_string());

        let plan = compute_branch_plan(&topo, 1, &deps_of, &tip, "SHA_START", false);
        assert_eq!(plan.parent_sha.as_deref(), Some("SHA_A"));
        assert!(plan.merge_shas.is_empty());
    }

    // -- run_all_plans tests --

    #[test]
    fn test_run_all_plans_cycle_detection() {
        // Insert two plans with a direct cycle via raw SQL and verify that
        // run_all_plans (via topo_sort_plans) surfaces a cycle error.
        use tokio::sync::watch;

        let conn = setup();
        let p1 =
            storage::create_plan(&conn, "cyc-a", "/tmp/cyc", "b1", "d1", None, None, &[]).unwrap();
        let p2 =
            storage::create_plan(&conn, "cyc-b", "/tmp/cyc", "b2", "d2", None, None, &[]).unwrap();

        // Mark both as Ready so they're runnable.
        storage::update_plan_status(&conn, &p1.id, PlanStatus::Ready).unwrap();
        storage::update_plan_status(&conn, &p2.id, PlanStatus::Ready).unwrap();

        // Create a cycle directly in the DB, bypassing the cycle check
        // that add_plan_dependency would apply.
        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params![p1.id, p2.id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params![p2.id, p1.id],
        )
        .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let workdir = std::path::Path::new("/tmp");
        let options = RunOptions {
            all_plans: true,
            dry_run: true,
            current_branch: true,
            ..Default::default()
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let out = OutputContext::from_cli(false, false, false);
            run_all_plans(&conn, "/tmp/cyc", &config, workdir, &options, rx, &out).await
        });

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cycle"),
            "expected cycle error, got: {err}"
        );
    }

    #[test]
    fn test_run_all_plans_no_runnable_plans() {
        use tokio::sync::watch;

        let conn = setup();
        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let workdir = std::path::Path::new("/tmp");
        let options = RunOptions {
            all_plans: true,
            current_branch: true,
            ..Default::default()
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let results = rt
            .block_on(async {
                let out = OutputContext::from_cli(false, false, false);
                run_all_plans(&conn, "/tmp/empty", &config, workdir, &options, rx, &out).await
            })
            .unwrap();

        assert!(results.is_empty());
    }

    // -- transitive_dependents (L9 helper) --

    #[test]
    fn test_transitive_dependents_no_edges() {
        // With no edges, no plan is blocked by any other. This is the core
        // of the L9 fix: when B ends incomplete and C is independent, C must
        // not be blocked.
        let dependents_of: HashMap<String, Vec<String>> = HashMap::new();
        let blocked = transitive_dependents("B", &dependents_of);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_transitive_dependents_direct_dependent() {
        // C depends on B → B's incomplete run blocks C.
        let mut dependents_of: HashMap<String, Vec<String>> = HashMap::new();
        dependents_of.insert("B".to_string(), vec!["C".to_string()]);
        let blocked = transitive_dependents("B", &dependents_of);
        assert_eq!(blocked, vec!["C".to_string()]);
    }

    #[test]
    fn test_transitive_dependents_transitive_chain() {
        // B -> C -> D: incomplete B blocks both C and D.
        let mut dependents_of: HashMap<String, Vec<String>> = HashMap::new();
        dependents_of.insert("B".to_string(), vec!["C".to_string()]);
        dependents_of.insert("C".to_string(), vec!["D".to_string()]);
        let mut blocked = transitive_dependents("B", &dependents_of);
        blocked.sort();
        assert_eq!(blocked, vec!["C".to_string(), "D".to_string()]);
    }

    #[test]
    fn test_transitive_dependents_diamond_no_duplicates() {
        // B -> {C, D}; both C and D -> E. E appears once, not twice.
        let mut dependents_of: HashMap<String, Vec<String>> = HashMap::new();
        dependents_of.insert("B".to_string(), vec!["C".to_string(), "D".to_string()]);
        dependents_of.insert("C".to_string(), vec!["E".to_string()]);
        dependents_of.insert("D".to_string(), vec!["E".to_string()]);
        let mut blocked = transitive_dependents("B", &dependents_of);
        blocked.sort();
        assert_eq!(
            blocked,
            vec!["C".to_string(), "D".to_string(), "E".to_string()]
        );
    }

    /// Acceptance test for L9: with [A Ready, B, C Ready] where C is
    /// independent of B, an incomplete run of B must not block C. The
    /// helper encodes that decision.
    #[test]
    fn test_transitive_dependents_independent_plans_not_blocked() {
        // Graph: A, B, C all in scope, no edges between any of them.
        let dependents_of: HashMap<String, Vec<String>> = HashMap::new();
        // B ends incomplete → nothing is blocked.
        let blocked_by_b = transitive_dependents("B", &dependents_of);
        assert!(blocked_by_b.is_empty());
        // Sanity: A and C aren't in a blocked set, so run_all_plans' blocked
        // check `contains(plan_id)` returns false for them and they run.
        let blocked_set: HashSet<String> = blocked_by_b.into_iter().collect();
        assert!(!blocked_set.contains("A"));
        assert!(!blocked_set.contains("C"));
    }

    // -- setup_branch with parent_sha --

    /// Initialize a throwaway git repo with a single commit and return its path.
    fn init_git_repo() -> (tempfile::TempDir, std::path::PathBuf) {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
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
        fs::write(dir.join("README.md"), "# hi").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
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

    /// Seed a `run_locks` row so the executor's `write_phase` has a row to
    /// update (mirrors `run_lock::acquire` in production; the same helper
    /// the executor tests use). Required by any test that drives a real
    /// `execute_step` through `run_plan`.
    #[cfg(test)]
    fn seed_run_lock_row(conn: &Connection, project: &str) {
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![project, 1i64, "p-test", "slug"],
        )
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_setup_branch_with_parent_sha() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        let initial_sha = git::get_commit_hash(&dir).unwrap();

        // Make a second commit.
        fs::write(dir.join("second.txt"), "second").unwrap();
        git::commit_changes(&dir, "second").unwrap();

        let plan = Plan {
            id: "p1".to_string(),
            slug: "test".to_string(),
            project: dir.to_string_lossy().to_string(),
            branch_name: "feat/rooted".to_string(),
            description: String::new(),
            status: PlanStatus::Ready,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        };

        // Should create feat/rooted rooted at initial_sha.
        setup_branch(&dir, &plan, Some(&initial_sha)).await.unwrap();
        assert_eq!(git::get_current_branch(&dir).unwrap(), "feat/rooted");
        assert_eq!(git::get_commit_hash(&dir).unwrap(), initial_sha);
        // The second commit's file should not be visible on the new branch.
        assert!(!dir.join("second.txt").exists());
    }

    /// Confirm `setup_branch` no longer monopolises a single-threaded runtime:
    /// a concurrent tokio task must be able to make progress while the git
    /// subprocesses run.
    #[tokio::test(flavor = "current_thread")]
    async fn test_setup_branch_does_not_block_runtime() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (_tmp, dir) = init_git_repo();

        let plan = Plan {
            id: "p1".to_string(),
            slug: "test".to_string(),
            project: dir.to_string_lossy().to_string(),
            branch_name: "feat/concurrent".to_string(),
            description: String::new(),
            status: PlanStatus::Ready,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        };

        // Concurrent ticker that increments a counter every few ms. On a
        // blocking runtime worker it would not get any cycles while the git
        // subprocesses run serially in `setup_branch`.
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticks_task = ticks.clone();
        let ticker = tokio::spawn(async move {
            for _ in 0..50 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                ticks_task.fetch_add(1, Ordering::SeqCst);
            }
        });

        setup_branch(&dir, &plan, None).await.unwrap();
        ticker.await.unwrap();

        // The ticker's 50 × 1ms sleeps only make progress if the runtime
        // worker was free to poll them. Assert at least a few got through —
        // the exact count depends on git timing, but a fully blocked runtime
        // yields zero.
        assert!(
            ticks.load(Ordering::SeqCst) > 0,
            "ticker made no progress — setup_branch blocked the runtime"
        );
    }

    // Regression for L7: pre-existing Complete steps must not inflate
    // steps_succeeded — only steps this invocation actually executed count.
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_plan_does_not_count_preexisting_complete_as_succeeded() {
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();

        let conn = setup();
        let plan =
            storage::create_plan(&conn, "s", &project, "feat/x", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();

        // Two pre-completed steps from an earlier run.
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        let (s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s2.id, StepStatus::Complete).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "s", &project)
            .unwrap()
            .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            ..Default::default()
        };

        let result = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        // Before the fix, pre-existing Complete steps were counted as
        // succeeded; this invocation executed nothing, so both counters
        // must be zero.
        assert_eq!(result.steps_executed, 0);
        assert_eq!(result.steps_succeeded, 0);
        assert_eq!(result.final_status, PlanStatus::Complete);
    }

    /// Regression: when `plans.pause_requested` is set, the runner's between-
    /// steps check must exit cleanly before executing the next step, clear
    /// the flag in the same transaction so a subsequent `ralph resume`
    /// doesn't immediately re-pause, and report InProgress (not Complete /
    /// Failed) so the live-run summary reflects the pause.
    #[tokio::test(flavor = "current_thread")]
    async fn test_pause_requested_stops_between_steps_and_clears_flag() {
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();

        let conn = setup();
        let plan =
            storage::create_plan(&conn, "p", &project, "feat/p", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();

        // Three steps. Mark step 1 Complete so the runner enters the loop
        // with a real "next step" candidate. The pause check at the top of
        // the loop fires before that candidate ever gets executed, exercising
        // exactly the between-steps boundary the spec describes.
        for (i, title) in ["s1", "s2", "s3"].iter().enumerate() {
            let (s, _) = storage::create_step(
                &conn,
                &plan.id,
                title,
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
            if i == 0 {
                storage::update_step_status(&conn, &s.id, StepStatus::Complete).unwrap();
            }
        }

        // Operator requests pause before the runner ever reaches step 2.
        storage::set_plan_pause_requested(&conn, &plan.id, true).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            ..Default::default()
        };

        let result = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        // No new step should have run — the pause check fires before the
        // executor is invoked.
        assert_eq!(result.steps_executed, 0);
        assert_eq!(result.steps_succeeded, 0);
        // Final status stays InProgress (deliberate pause, not failure).
        assert_eq!(result.final_status, PlanStatus::InProgress);

        // Flag must be cleared so a subsequent run/resume isn't immediately
        // re-paused.
        assert!(
            !storage::get_plan_pause_requested(&conn, &plan.id).unwrap(),
            "pause_requested must be cleared on entry-to-pause",
        );

        // Steps 2 and 3 stayed pending — neither was started or skipped.
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Pending);
        assert_eq!(steps[2].status, StepStatus::Pending);
    }

    /// Regression: after a paused run, `resume_plan` must continue from the
    /// next pending step normally — no additional pause logic, since the
    /// runner cleared the flag on entry-to-pause.
    #[tokio::test(flavor = "current_thread")]
    async fn test_resume_after_pause_does_not_re_pause() {
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();

        let conn = setup();
        let plan =
            storage::create_plan(&conn, "p", &project, "feat/p", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();

        // Two steps — step 1 already complete, step 2 pending.
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "first",
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
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        let (_s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "second",
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

        // Pause-then-resume: set the flag, run (which clears it via the
        // between-steps check), then call resume_plan — the flag stays clear
        // and the runner enters the executor for step 2 (which fails because
        // there's no harness configured, but only AFTER passing the pause
        // check, which is what we're verifying).
        storage::set_plan_pause_requested(&conn, &plan.id, true).unwrap();
        let plan_obj = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            ..Default::default()
        };

        let _paused = run_plan(&conn, &plan_obj, &config, &dir, &options, rx.clone(), &out)
            .await
            .unwrap();
        assert!(!storage::get_plan_pause_requested(&conn, &plan_obj.id).unwrap());

        // Re-running with the flag clear MUST NOT short-circuit the loop —
        // the runner gets to the "find next actionable step" path. We don't
        // assert success here because there's no real harness; what we
        // assert is that pause_requested is still false after the second
        // call (so a future resume keeps progressing).
        let _second = run_plan(&conn, &plan_obj, &config, &dir, &options, rx, &out).await;
        assert!(
            !storage::get_plan_pause_requested(&conn, &plan_obj.id).unwrap(),
            "pause_requested stays clear across the boundary",
        );
    }

    /// The runner must record `plans.last_run_branch` at run-start in
    /// `--current-branch` mode — that's the path resume's branch-based
    /// resolver depends on for plans that ran without their own branch.
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_plan_records_last_run_branch_in_current_branch_mode() {
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();
        let actual_branch = git::get_current_branch(&dir).unwrap();

        let conn = setup();
        // Plan's branch_name is intentionally different from the actual git
        // branch so the assertion below catches a regression where the
        // runner records `branch_name` instead of the workdir's HEAD.
        let plan = storage::create_plan(
            &conn,
            "s",
            &project,
            "would-be-branch",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        // One Complete step so the runner reaches run_plan_inner, writes
        // last_run_branch, and exits cleanly (no executor needed).
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "Done",
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
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "s", &project)
            .unwrap()
            .unwrap();
        assert!(plan.last_run_branch.is_none());

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            ..Default::default()
        };
        let _ = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        let after = storage::get_plan_by_slug(&conn, "s", &project)
            .unwrap()
            .unwrap();
        assert_eq!(
            after.last_run_branch.as_deref(),
            Some(actual_branch.as_str()),
            "runner must record the workdir's HEAD as last_run_branch"
        );
        // The pre-existing branch_name field is left untouched — it's the
        // user's "where I want this plan to run" intent, not the record of
        // where it physically ran.
        assert_eq!(after.branch_name, "would-be-branch");
    }

    /// Default (non-`--current-branch`) mode: setup_branch switches to the
    /// plan's branch first, so last_run_branch must reflect THAT branch,
    /// not the source branch the user kicked the run off from.
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_plan_records_plan_branch_when_not_current_branch_mode() {
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();
        let source_branch = git::get_current_branch(&dir).unwrap();

        let conn = setup();
        let plan =
            storage::create_plan(&conn, "s", &project, "feat/run-here", "d", None, None, &[])
                .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "Done",
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
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "s", &project)
            .unwrap()
            .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: false,
            auto_stash: true,
            ..Default::default()
        };
        let _ = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        let after = storage::get_plan_by_slug(&conn, "s", &project)
            .unwrap()
            .unwrap();
        assert_eq!(
            after.last_run_branch.as_deref(),
            Some("feat/run-here"),
            "must record the branch setup_branch switched into, not the source branch ({source_branch})"
        );
    }

    // -- stash_if_dirty / setup_branch --

    /// With `--no-auto-stash`, a dirty tree must bail cleanly and list the
    /// files that are blocking the switch so the user can stage or
    /// discard them intentionally.
    #[tokio::test(flavor = "current_thread")]
    async fn test_dirty_tree_no_auto_stash_bails_cleanly() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        let err = stash_if_dirty(
            &dir, "demo", None, None, None, None, /*auto_stash=*/ false,
        )
        .await
        .expect_err("dirty tree with auto_stash=false must bail");
        let msg = format!("{err}");
        assert!(
            msg.contains("scratch.txt"),
            "error must list the dirty file, got: {msg}"
        );
        assert!(
            msg.contains("auto_stash"),
            "error must point users at the auto_stash setting, got: {msg}"
        );

        // Nothing was swept; the tree is still dirty.
        assert!(git::has_uncommitted_changes(&dir).unwrap());
    }

    /// `--current-branch` + a dirty tree must still stash and restore.
    /// Earlier behavior gated stashing on `!current_branch`, which left the
    /// agent staring at a dirty tree on its first step. This guards the
    /// fix: stash always happens (when not dry-run), and teardown pops
    /// the stash regardless of whether we switched branches.
    #[tokio::test(flavor = "current_thread")]
    async fn test_current_branch_with_dirty_tree_stashes_and_restores() {
        use std::fs;
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();
        let starting_branch = git::get_current_branch(&dir).unwrap();

        // Dirty the tree before the run.
        fs::write(dir.join("scratch.txt"), "wip\n").unwrap();
        assert!(git::has_uncommitted_changes(&dir).unwrap());

        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "demo",
            &project,
            "would-be-branch",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        // One Complete step so run_plan_inner exits cleanly without an
        // executor; the value-under-test is the stash/teardown wrapper.
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "Done",
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
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "demo", &project)
            .unwrap()
            .unwrap();
        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            auto_stash: true,
            ..Default::default()
        };
        run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        // We never switched branches.
        assert_eq!(git::get_current_branch(&dir).unwrap(), starting_branch);
        // The dirty file came back via stash pop.
        assert_eq!(
            fs::read_to_string(dir.join("scratch.txt")).unwrap(),
            "wip\n",
            "dirty file must be restored after the run",
        );
        assert!(
            git::has_uncommitted_changes(&dir).unwrap(),
            "tree should still be dirty (stash was popped, not discarded)",
        );
        // No leftover ralph stashes on the stack.
        assert!(
            git::find_stash_by_message(&dir, "ralph: auto-stash for plan 'demo'")
                .unwrap()
                .is_none(),
            "ralph's stash entry must be popped at teardown",
        );
    }

    /// Staged files are re-staged after the round trip — `git stash pop`
    /// always returns everything as unstaged, so `restore_working_tree` has
    /// to re-stage the files that were in the index at stash time. Without
    /// this, users who had `git add`-ed a file before kicking off a run
    /// would lose that staged status.
    #[tokio::test(flavor = "current_thread")]
    async fn test_stash_round_trip_preserves_staged_status() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();

        // Two dirty files — one staged, one unstaged.
        fs::write(dir.join("staged.txt"), "i am staged\n").unwrap();
        fs::write(dir.join("unstaged.txt"), "i am unstaged\n").unwrap();
        git::stage_except(&dir, &["unstaged.txt".to_string()]).unwrap();

        // Sanity: only staged.txt is in the index.
        let staged_before = git::list_staged_files(&dir).unwrap();
        assert_eq!(staged_before, vec!["staged.txt".to_string()]);

        let stashed = stash_if_dirty(&dir, "demo", Some("demo"), None, None, None, true)
            .await
            .unwrap()
            .expect("expected a stash");
        assert_eq!(stashed.staged_files, vec!["staged.txt".to_string()]);

        // After teardown the file is back AND staged.
        restore_working_tree(&dir, Some(&stashed)).await.unwrap();
        let staged_after = git::list_staged_files(&dir).unwrap();
        assert_eq!(
            staged_after,
            vec!["staged.txt".to_string()],
            "staged.txt must be re-staged after pop, got: {staged_after:?}"
        );
        // unstaged.txt should still be present in the worktree but not in
        // the index.
        assert!(dir.join("unstaged.txt").exists());
        assert!(!staged_after.contains(&"unstaged.txt".to_string()));
    }

    /// Default (auto_stash=true) stash-push + stash-pop round trip: the
    /// dirty file survives a fake run and reappears with identical
    /// contents once teardown runs.
    #[tokio::test(flavor = "current_thread")]
    async fn test_dirty_tree_default_auto_stash_push_pop_round_trip() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        fs::write(dir.join("scratch.txt"), "wip-contents").unwrap();
        // Also modify a tracked file so we exercise both paths.
        fs::write(dir.join("README.md"), "# modified\n").unwrap();

        let source_branch = git::get_current_branch(&dir).unwrap();

        let stash = stash_if_dirty(
            &dir,
            "demo",
            Some("demo"),
            None,
            None,
            None,
            /*auto_stash=*/ true,
        )
        .await
        .unwrap()
        .expect("expected a stash SHA");

        // Tree is clean; scratch.txt is gone; tracked file is reverted.
        assert!(!git::has_uncommitted_changes(&dir).unwrap());
        assert!(!dir.join("scratch.txt").exists());
        assert_eq!(fs::read_to_string(dir.join("README.md")).unwrap(), "# hi");

        // Set up a branch (simulates a run).
        let plan = Plan {
            id: "p1".to_string(),
            slug: "demo".to_string(),
            project: dir.to_string_lossy().to_string(),
            branch_name: "feat/stash-roundtrip".to_string(),
            description: String::new(),
            status: PlanStatus::Ready,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        };
        setup_branch(&dir, &plan, None).await.unwrap();
        assert_eq!(
            git::get_current_branch(&dir).unwrap(),
            "feat/stash-roundtrip"
        );

        // Teardown: pop stash on the plan branch (no checkout back).
        restore_working_tree(&dir, Some(&stash)).await.unwrap();

        assert_eq!(
            git::get_current_branch(&dir).unwrap(),
            "feat/stash-roundtrip",
            "teardown must leave us on the plan branch, not source",
        );
        // Silence the unused-binding warning; source_branch capture is kept
        // to document intent (run started here) but teardown no longer
        // checks it out.
        let _ = source_branch;
        assert_eq!(
            fs::read_to_string(dir.join("scratch.txt")).unwrap(),
            "wip-contents",
            "untracked file must be restored by the pop"
        );
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# modified\n",
            "tracked modification must be restored by the pop"
        );
    }

    /// A clean tree returns None from `stash_if_dirty` regardless of
    /// `auto_stash`, and teardown with no stash is a no-op (we stay on the
    /// plan branch).
    #[tokio::test(flavor = "current_thread")]
    async fn test_clean_tree_no_stash_needed() {
        let (_tmp, dir) = init_git_repo();

        let result_off = stash_if_dirty(&dir, "demo", Some("feat/clean"), None, None, None, false)
            .await
            .unwrap();
        assert!(result_off.is_none());
        let result_on = stash_if_dirty(&dir, "demo", Some("feat/clean"), None, None, None, true)
            .await
            .unwrap();
        assert!(result_on.is_none());

        // Exercise the new conservative detection helper (no trailers => no residue).
        assert!(
            !git::has_crash_residue_overlap_for_step(&dir, "feat/clean", "no-such-short")
                .unwrap_or(false),
            "detection must be conservative (false) when no matching Ralph commits exist"
        );

        let plan = Plan {
            id: "p1".to_string(),
            slug: "test".to_string(),
            project: dir.to_string_lossy().to_string(),
            branch_name: "feat/clean".to_string(),
            description: String::new(),
            status: PlanStatus::Ready,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        };
        setup_branch(&dir, &plan, None).await.unwrap();
        assert_eq!(git::get_current_branch(&dir).unwrap(), "feat/clean");
        restore_working_tree(&dir, None).await.unwrap();
        assert_eq!(
            git::get_current_branch(&dir).unwrap(),
            "feat/clean",
            "teardown without a stash is a no-op; stay on the plan branch",
        );
    }

    /// If the run produced a commit that CONFLICTS with the stashed
    /// working-tree state, teardown must leave the stash on the stack
    /// and return a non-zero error so the user can recover manually.
    #[tokio::test(flavor = "current_thread")]
    async fn test_stash_pop_conflict_during_teardown_preserves_stash() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();

        // Pre-stash: README has version A queued up.
        fs::write(dir.join("README.md"), "# version A\n").unwrap();
        let stash = stash_if_dirty(&dir, "demo", Some("demo"), None, None, None, true)
            .await
            .unwrap()
            .expect("sha");

        // Simulate a run that commits a divergent README to the source
        // branch. In practice this would be on the plan branch, but the
        // conflict materializes the same way when popping.
        fs::write(dir.join("README.md"), "# version B\n").unwrap();
        git::commit_changes(&dir, "divergent commit").unwrap();

        let err = restore_working_tree(&dir, Some(&stash))
            .await
            .expect_err("pop must surface the conflict");
        let msg = format!("{err}");
        assert!(
            msg.contains(stash.stash_ref.as_str()),
            "error must surface the stash SHA for manual recovery, got: {msg}"
        );

        // The stash is still on the stack.
        let still_there =
            git::find_stash_by_message(&dir, "ralph: auto-stash for plan 'demo'").unwrap();
        assert_eq!(still_there.as_ref(), Some(&stash.stash_ref));
    }

    /// The teardown path must fire even when the inner plan body fails.
    /// Drive `run_plan` against a plan with no steps — this produces an
    /// inner `bail!` AFTER stash_if_dirty + setup_branch have run — and
    /// assert that teardown still popped the stash on the plan branch.
    #[tokio::test(flavor = "current_thread")]
    async fn test_stash_pop_on_failure_still_fires() {
        use std::fs;
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();

        // Seed a dirty tree.
        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        // A plan with zero steps will hit `bail!("Plan ... has no steps")`
        // inside run_plan_inner — i.e. after the stash + branch setup.
        let conn = setup();
        let plan =
            storage::create_plan(&conn, "empty", &project, "feat/empty", "d", None, None, &[])
                .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "empty", &project)
            .unwrap()
            .unwrap();

        // Seed a run_locks row so `record_source_branch_and_stash` inside
        // run_plan has a target. Keeping it minimal — the column defaults
        // cover everything else.
        use rusqlite::params;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![
                project,
                std::process::id() as i64,
                plan.id.as_str(),
                plan.slug.as_str()
            ],
        )
        .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, true, true);
        // current_branch=false so run_plan drives the stash/branch/teardown
        // path; auto_stash=true mirrors the CLI default.
        let options = RunOptions {
            auto_stash: true,
            ..Default::default()
        };

        let err = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .expect_err("no-steps plan must surface an error");
        assert!(
            format!("{err}").contains("has no steps"),
            "unexpected error: {err}"
        );

        // Teardown leaves us on the plan branch (no checkout back) and
        // pops the stash there — scratch.txt reappears.
        assert_eq!(git::get_current_branch(&dir).unwrap(), "feat/empty");
        assert_eq!(fs::read_to_string(dir.join("scratch.txt")).unwrap(), "wip");
        // The stash is gone.
        let remaining = git::find_stash_by_message(&dir, "ralph: auto-stash").unwrap();
        assert!(remaining.is_none(), "stash should have been popped");
    }

    /// Regression: if `setup_branch` fails AFTER `stash_if_dirty` has already
    /// created a stash, the teardown path must still pop the stash. Without
    /// the fix, a bad branch name would leave the user's uncommitted work
    /// stranded on the stash stack.
    #[tokio::test(flavor = "current_thread")]
    async fn test_setup_branch_failure_still_restores_stash() {
        use std::fs;
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();
        let source_branch = git::get_current_branch(&dir).unwrap();

        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        // `..` in a branch name is rejected by git-check-ref-format, so
        // create_and_checkout_branch will fail — exercising the
        // post-stash-pre-teardown failure window.
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "bad",
            &project,
            "feat/bad..branch",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bad", &project)
            .unwrap()
            .unwrap();

        use rusqlite::params;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![
                project,
                std::process::id() as i64,
                plan.id.as_str(),
                plan.slug.as_str()
            ],
        )
        .unwrap();

        let config = Config::default();
        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, true, true);
        let options = RunOptions {
            auto_stash: true,
            ..Default::default()
        };

        let err = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .expect_err("invalid branch name must surface an error");
        // Sanity: the error is the branch-setup error, not a teardown error.
        let msg = format!("{err}");
        assert!(
            !msg.contains("has no steps"),
            "should fail at branch setup, not later: {msg}"
        );

        // Still on the source branch (setup never switched).
        assert_eq!(git::get_current_branch(&dir).unwrap(), source_branch);
        // Stash was popped — scratch.txt is back in the working tree.
        assert_eq!(fs::read_to_string(dir.join("scratch.txt")).unwrap(), "wip");
        // No stash left behind.
        let remaining = git::find_stash_by_message(&dir, "ralph: auto-stash").unwrap();
        assert!(
            remaining.is_none(),
            "stash must be popped after setup_branch failure, found: {remaining:?}"
        );
    }

    /// Simulates a crash: stash_if_dirty creates the stash, then the
    /// process disappears before restore_working_tree runs. The stash
    /// must survive on the stack for manual recovery.
    #[tokio::test(flavor = "current_thread")]
    async fn test_crash_leaves_stash_on_stack() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        let stash = stash_if_dirty(&dir, "crashy", Some("crashy"), None, None, None, true)
            .await
            .unwrap()
            .expect("sha");

        // Simulate the crash: skip the teardown entirely. The stash must
        // still be findable by its message and its SHA must still be on
        // the stack.
        let recovered =
            git::find_stash_by_message(&dir, "ralph: auto-stash for plan 'crashy'").unwrap();
        assert_eq!(recovered.as_ref(), Some(&stash.stash_ref));
    }

    // -- sweep_stale_in_progress / stale-step recovery --

    /// Unit test for the storage helper: an orphaned InProgress row is flipped
    /// to Aborted and returned to the caller. Equivalent to what
    /// `run_plan` / `resume_plan` rely on at startup after a crashed prior run.
    #[test]
    fn test_stale_in_progress_swept_on_run_start() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Seed two InProgress rows — simulates a runner crash mid-execution.
        storage::update_step_status(&conn, &s1.id, StepStatus::InProgress).unwrap();
        storage::update_step_status(&conn, &s2.id, StepStatus::InProgress).unwrap();

        let swept = storage::sweep_stale_in_progress(&conn, &plan.id).unwrap();
        assert_eq!(swept.len(), 2);
        // Returned rows reflect the PRE-update status recorded by RETURNING's
        // semantics, but they're flipped in the DB. What we care about is
        // that the DB side now reads Aborted.
        let after = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(after[0].status, StepStatus::Aborted);
        assert_eq!(after[1].status, StepStatus::Aborted);
    }

    /// Sweep is a no-op when there are no InProgress rows — ensures we don't
    /// clobber Complete/Failed/Pending rows.
    #[test]
    fn test_stale_sweep_noop_without_in_progress() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        storage::update_step_status(&conn, &s2.id, StepStatus::Failed).unwrap();

        let swept = storage::sweep_stale_in_progress(&conn, &plan.id).unwrap();
        assert!(swept.is_empty());

        let after = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(after[0].status, StepStatus::Complete);
        assert_eq!(after[1].status, StepStatus::Failed);
    }

    /// End-to-end test of the sweep + log path invoked from `run_plan` /
    /// `resume_plan`. We can't easily drive the full runner loop without a
    /// real git repo and harness, so drive `sweep_and_log_stale_in_progress`
    /// directly — it's the exact code the runner calls.
    #[test]
    fn test_sweep_and_log_wrapper_flips_and_returns_rows() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::InProgress).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "s", "/p")
            .unwrap()
            .unwrap();
        let out = OutputContext::from_cli(false, true, true);

        let swept = sweep_and_log_stale_in_progress(&conn, &plan, &out).unwrap();
        assert_eq!(swept.len(), 1);

        let after = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(after[0].status, StepStatus::Aborted);
    }

    // -- RunWindow / resolve_window tests --

    #[test]
    fn test_resolve_window_no_bounds_covers_all() {
        let steps = make_steps(5);
        let options = RunOptions::default();
        let window = resolve_window(&steps, &options).unwrap();
        for s in &steps {
            assert!(window.contains_key(&s.sort_key));
        }
    }

    #[test]
    fn test_resolve_window_from_to_bounds() {
        let steps = make_steps(5);
        let options = RunOptions {
            from: Some(2),
            to: Some(4),
            ..Default::default()
        };
        let window = resolve_window(&steps, &options).unwrap();
        assert!(!window.contains_key(&steps[0].sort_key));
        assert!(window.contains_key(&steps[1].sort_key));
        assert!(window.contains_key(&steps[2].sort_key));
        assert!(window.contains_key(&steps[3].sort_key));
        assert!(!window.contains_key(&steps[4].sort_key));
    }

    #[test]
    fn test_resolve_window_tolerates_midrun_insert() {
        // Simulates the mid-run-insert scenario: the window was resolved
        // against a 3-step plan, then a 4th step was inserted with a
        // sort_key BETWEEN the original step 1 and step 2. The new step
        // must be accepted by the window because it falls within the
        // resolved sort_key bounds.
        let mut steps = make_steps(3);
        // Ensure sort keys are lexicographically ordered and leave a gap.
        steps[0].sort_key = "a0".to_string();
        steps[1].sort_key = "a5".to_string();
        steps[2].sort_key = "a9".to_string();

        let options = RunOptions::default(); // full range
        let window = resolve_window(&steps, &options).unwrap();

        // A new step with sort_key "a3" (between a0 and a5) must be in
        // the window.
        assert!(window.contains_key("a3"));
    }

    #[test]
    fn test_resolve_window_out_of_range_errors() {
        let steps = make_steps(3);
        let options = RunOptions {
            from: Some(5),
            ..Default::default()
        };
        assert!(resolve_window(&steps, &options).is_err());
    }

    #[test]
    fn test_resolve_window_from_greater_than_to_errors() {
        let steps = make_steps(5);
        let options = RunOptions {
            from: Some(4),
            to: Some(2),
            ..Default::default()
        };
        assert!(resolve_window(&steps, &options).is_err());
    }

    /// Regression test for the progress-header fix: the header should display
    /// `step_num` / `plan_total`, not `slice_pos` / `slice_len`. Full-loop
    /// integration is hard (needs a real harness), so assert the helper that
    /// computes step_num works for a plan-relative position after mid-run
    /// inserts would have shifted a slice-relative counter.
    #[test]
    fn test_progress_header_uses_plan_relative_numbers() {
        // Plan has 5 steps initially, we imagine a run of steps 3..=5 (so
        // slice-relative numerator would be 1,2,3). `step_number_in_plan`
        // must return 3,4,5 — the plan-relative indices.
        let steps = make_steps(5);
        let slice = &steps[2..];
        assert_eq!(step_number_in_plan(&steps, &slice[0]), 3);
        assert_eq!(step_number_in_plan(&steps, &slice[1]), 4);
        assert_eq!(step_number_in_plan(&steps, &slice[2]), 5);

        // After a mid-run insert, plan size changes. `step_number_in_plan`
        // re-derives from the passed-in list, so the number reflects the
        // step's current position.
        let mut grown = steps.clone();
        let new_step = Step {
            id: "s_new".to_string(),
            short_id: String::new(),
            plan_id: "p1".to_string(),
            sort_key: "a05".to_string(), // between s0=a0 and s1=a1
            title: "Inserted".to_string(),
            description: "d".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        };
        grown.insert(1, new_step.clone());
        // The inserted step becomes step 2; what was step 2 (s1) is now
        // step 3.
        assert_eq!(step_number_in_plan(&grown, &new_step), 2);
        assert_eq!(step_number_in_plan(&grown, &steps[1]), 3);
        // Plan size grew.
        assert_eq!(grown.len(), 6);
    }

    /// Unit test of the re-query logic: once a step is executed, the runner
    /// adds it to `executed_step_ids` and will not re-execute it even if the
    /// re-query returns the full step list again on the next iteration.
    /// Full integration (driving a real runner loop with a mid-run storage
    /// insert) is infeasible without a real harness, so this asserts the
    /// "find next actionable" lookup that the loop uses.
    #[test]
    fn test_mid_run_step_insertion_picked_up() {
        use std::collections::HashSet;

        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Simulate iteration 1: load steps, "execute" s1.
        let initial = storage::list_steps(&conn, &plan.id).unwrap();
        let window = resolve_window(&initial, &RunOptions::default()).unwrap();
        let mut known: HashSet<String> = initial.iter().map(|s| s.id.clone()).collect();
        let mut executed: HashSet<String> = HashSet::new();

        let next = initial
            .iter()
            .find(|s| {
                window.contains_key(&s.sort_key)
                    && is_actionable(s.status)
                    && !executed.contains(&s.id)
            })
            .unwrap();
        assert_eq!(next.id, s1.id);
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        executed.insert(s1.id.clone());

        // Between iterations, the running agent inserts a new step at a
        // sort_key BETWEEN s1 and s2 (simulates `ralph step add` mid-run).
        let mid_key = crate::frac_index::key_between(&s1.sort_key, &s2.sort_key).unwrap();
        let (new_step, _) = storage::create_step_at(
            &conn,
            &plan.id,
            &mid_key,
            "Inserted",
            "dN",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Iteration 2: re-query. Detect new inserts. Find next actionable.
        let refreshed = storage::list_steps(&conn, &plan.id).unwrap();
        let new_inserts: Vec<Step> = refreshed
            .iter()
            .filter(|s| !known.contains(&s.id))
            .filter(|s| window.contains_key(&s.sort_key))
            .cloned()
            .collect();
        assert_eq!(new_inserts.len(), 1);
        assert_eq!(new_inserts[0].id, new_step.id);
        for s in &refreshed {
            known.insert(s.id.clone());
        }

        let next2 = refreshed
            .iter()
            .find(|s| {
                window.contains_key(&s.sort_key)
                    && is_actionable(s.status)
                    && !executed.contains(&s.id)
            })
            .unwrap();
        // The inserted step must now be picked up BEFORE s2, because its
        // sort_key is between s1 and s2.
        assert_eq!(next2.id, new_step.id);
        assert_ne!(next2.id, s2.id);
    }

    // -- is_actionable --

    #[test]
    fn test_is_actionable_statuses() {
        assert!(is_actionable(StepStatus::Pending));
        assert!(is_actionable(StepStatus::Failed));
        assert!(is_actionable(StepStatus::InProgress));
        assert!(is_actionable(StepStatus::Aborted));
        assert!(!is_actionable(StepStatus::Complete));
        assert!(!is_actionable(StepStatus::Skipped));
    }

    // -- topological scheduler (docs/dag-redesign.md §3.5) --

    /// Unbounded run window (no `--from`/`--to`).
    fn full_window() -> RunWindow {
        RunWindow {
            from_key: None,
            to_key: None,
        }
    }

    /// Build `deps_of` from index edges. `(a, b)` ⇒ `steps[a]` depends on
    /// `steps[b]` (matches `add_step_dependency(step, depends_on)`).
    fn deps_map(steps: &[Step], edges: &[(usize, usize)]) -> HashMap<String, Vec<String>> {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();
        for &(a, b) in edges {
            m.entry(steps[a].id.clone())
                .or_default()
                .push(steps[b].id.clone());
        }
        m
    }

    /// Drive the scheduler to quiescence: each tick pick the next step,
    /// mark it `Complete`, and record the order. Mirrors the real runner
    /// loop (re-derives depths every tick, tracks `executed_step_ids`).
    fn schedule_order(
        steps: &mut [Step],
        edges: &[(usize, usize)],
        window: &RunWindow,
    ) -> Vec<String> {
        schedule_order_blocked(steps, edges, window, &HashSet::new())
    }

    /// `schedule_order` with a fixed set of `Blocked` step ids that are
    /// never runnable (simulates open interruptions the scheduler must
    /// route around — docs/dag-redesign.md §3.4/§3.5).
    fn schedule_order_blocked(
        steps: &mut [Step],
        edges: &[(usize, usize)],
        window: &RunWindow,
        blocked: &HashSet<String>,
    ) -> Vec<String> {
        let deps_of = deps_map(steps, edges);
        let mut order: Vec<String> = Vec::new();
        let mut executed: HashSet<String> = HashSet::new();
        loop {
            let depths = compute_step_depths(steps, &deps_of);
            let pick = pick_next_step(steps, &deps_of, &depths, window, &executed, blocked)
                .map(|s| s.id.clone());
            let Some(id) = pick else { break };
            order.push(id.clone());
            for s in steps.iter_mut() {
                if s.id == id {
                    s.status = StepStatus::Complete;
                }
            }
            executed.insert(id);
        }
        order
    }

    #[test]
    fn test_scheduler_linear_chain() {
        // s0 <- s1 <- s2 <- s3 (each depends on the previous).
        let mut steps = make_steps(4);
        let edges = [(1, 0), (2, 1), (3, 2)];
        let deps_of = deps_map(&steps, &edges);

        // Depth grows along the chain.
        let depths = compute_step_depths(&steps, &deps_of);
        assert_eq!(depths["s0"], 0);
        assert_eq!(depths["s1"], 1);
        assert_eq!(depths["s2"], 2);
        assert_eq!(depths["s3"], 3);

        // Only the root is runnable until it completes.
        let executed = HashSet::new();
        let pick = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &executed,
            &HashSet::new(),
        );
        assert_eq!(pick.unwrap().id, "s0");

        // Full walk reproduces the authored order.
        let order = schedule_order(&mut steps, &edges, &full_window());
        assert_eq!(order, vec!["s0", "s1", "s2", "s3"]);
    }

    // ---- STEP 25: scheduler interruption integration (§3.4/§3.5) ----

    #[test]
    fn test_blocked_branch_does_not_starve_an_independent_branch() {
        // Two independent branches off no shared root:
        //   branch A: s0 -> s1   branch B: s2 -> s3
        // s0 is Blocked (open interruption). The §1 payoff: branch B must
        // still run to completion even though branch A is fully stalled
        // (s1 waits on the blocked s0 and is therefore never reached).
        let mut steps = make_steps(4);
        let edges = [(1, 0), (3, 2)]; // s1<-s0, s3<-s2
        let mut blocked = HashSet::new();
        blocked.insert("s0".to_string());

        let order = schedule_order_blocked(&mut steps, &edges, &full_window(), &blocked);

        // Branch B ran fully; branch A produced nothing (s0 blocked, s1
        // gated on the non-Complete s0).
        assert_eq!(
            order,
            vec!["s2".to_string(), "s3".to_string()],
            "the blocked branch must not prevent the independent branch"
        );
        assert!(!order.contains(&"s0".to_string()), "s0 is Blocked");
        assert!(
            !order.contains(&"s1".to_string()),
            "s1 waits on the blocked s0 (its dependency is not Complete)"
        );
    }

    #[test]
    fn test_linear_plan_with_one_blocked_step_pauses_like_before() {
        // §1 no-regression: a *linear* plan gets zero benefit. s0<-s1<-s2;
        // s0 Blocked ⇒ NOTHING is runnable (s1/s2 gated on s0), so the
        // scheduler stalls exactly as the pre-DAG loop did when the head
        // step paused for a question.
        let mut steps = make_steps(3);
        let edges = [(1, 0), (2, 1)];
        let mut blocked = HashSet::new();
        blocked.insert("s0".to_string());

        let order = schedule_order_blocked(&mut steps, &edges, &full_window(), &blocked);
        assert!(
            order.is_empty(),
            "a linear plan whose head step is blocked produces no progress \
             (same whole-plan pause as before — no regression)"
        );

        // And once the interruption is resolved (blocked set empties), the
        // very next scheduler pass runs the whole chain in authored order —
        // the cross-process re-queue (§9 invariant 4).
        let resumed = schedule_order_blocked(&mut steps, &edges, &full_window(), &HashSet::new());
        assert_eq!(resumed, vec!["s0", "s1", "s2"]);
    }

    /// Regression for the same-run re-queue bug: the scheduler loop adds
    /// every picked step to `executed_step_ids` (runner.rs ~695) so it is
    /// never re-picked this run. A step that paused for an interruption must
    /// be dropped back out of that set in the `PausedForQuestion` arm —
    /// otherwise a *same-run* resolution (a human answers while the loop
    /// keeps ticking on another branch — the §1 payoff / §9-inv-4 bridge)
    /// clears `blocked` but the step is still permanently excluded and never
    /// resumes until a fresh process. This models the loop's exact
    /// executed/blocked transitions across a pause→resolve.
    #[test]
    fn test_paused_step_requeues_within_same_run_after_resolution() {
        // Two independent branches so the loop keeps ticking while one
        // branch is paused: A: s0   B: s1->s2. s0 pauses on its first pick
        // (raises an interruption); branch B keeps the loop alive; then the
        // interruption is resolved mid-run and s0 must run *this run*.
        let mut steps = make_steps(3);
        let edges = [(2, 1)]; // s2 <- s1 ; s0 independent
        let deps_of = deps_map(&steps, &edges);

        let mut executed: HashSet<String> = HashSet::new();
        let mut blocked: HashSet<String> = HashSet::new();
        let mut order: Vec<String> = Vec::new();
        let mut s0_paused_once = false;

        // Drive the scheduler exactly as run_plan_inner does: pick, then
        // either "execute" (mark Complete + insert into executed) or, for
        // s0's first pick, take the PausedForQuestion path (insert into
        // executed at ~695 THEN the arm's `executed.remove`, leaving an open
        // interruption in `blocked`).
        let mut ticks = 0;
        loop {
            ticks += 1;
            assert!(ticks < 50, "scheduler must not busy-spin");
            let depths = compute_step_depths(&steps, &deps_of);
            let pick = pick_next_step(
                &steps,
                &deps_of,
                &depths,
                &full_window(),
                &executed,
                &blocked,
            )
            .map(|s| s.id.clone());
            let Some(id) = pick else { break };

            // runner.rs:695 — every picked step enters executed_step_ids.
            executed.insert(id.clone());

            if id == "s0" && !s0_paused_once {
                // PausedForQuestion arm: leaves an open interruption AND
                // (the fix) drops the step back out of executed_step_ids.
                s0_paused_once = true;
                blocked.insert("s0".to_string());
                executed.remove("s0"); // <-- the fix under test
                // Simulate branch B keeping the loop alive, then a human
                // resolving s0's interruption out of band.
                continue;
            }

            order.push(id.clone());
            for s in steps.iter_mut() {
                if s.id == id {
                    s.status = StepStatus::Complete;
                }
            }
            // Resolve s0's interruption once branch B has made progress,
            // proving the resolution lands mid-run (loop still alive).
            if id == "s1" {
                blocked.remove("s0");
            }
        }

        assert!(
            order.contains(&"s0".to_string()),
            "a paused step whose interruption is resolved mid-run MUST run \
             in the same run (got order {order:?})"
        );
        // Deterministic interleave: s0 pauses; s1 runs (branch B); on
        // resolution s0 (depth 0) re-queues ahead of the still-gated depth-1
        // s2; then s2. The point is s0 runs *this run* — not the exact spot.
        assert_eq!(
            order,
            vec!["s1".to_string(), "s0".to_string(), "s2".to_string()],
            "branch B runs while s0 is paused; s0 resumes after resolution"
        );
    }

    #[test]
    fn test_resolved_interruption_requeues_step_at_next_tick() {
        // A blocked root excludes itself and its dependents this pass;
        // clearing the block (a cross-process resolve) makes it runnable
        // again with no other state change — proving the re-queue is purely
        // a function of the recomputed blocked set.
        let steps = make_steps(2);
        let edges = [(1, 0)];
        let deps_of = deps_map(&steps, &edges);
        let depths = compute_step_depths(&steps, &deps_of);

        let mut blocked = HashSet::new();
        blocked.insert("s0".to_string());
        let pick_blocked = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &HashSet::new(),
            &blocked,
        );
        assert!(
            pick_blocked.is_none(),
            "blocked root + gated dependent ⇒ nothing runnable"
        );

        // Resolution: blocked set no longer contains s0.
        let pick_after = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(
            pick_after.unwrap().id,
            "s0",
            "resolving the interruption re-queues the step at the next tick"
        );
    }

    #[test]
    fn test_scheduler_no_edges_is_authored_sort_key_order() {
        // The linear-plan parity claim: with no dependency edges every
        // step has depth 0, so the tie-break collapses to sort_key — a
        // linear plan executes byte-identically to the pre-DAG loop.
        let mut steps = make_steps(5);
        let deps_of = deps_map(&steps, &[]);
        let depths = compute_step_depths(&steps, &deps_of);
        assert!(steps.iter().all(|s| depths[&s.id] == 0));

        let order = schedule_order(&mut steps, &[], &full_window());
        assert_eq!(order, vec!["s0", "s1", "s2", "s3", "s4"]);
    }

    #[test]
    fn test_scheduler_multi_root_dag() {
        // Two roots; s2 depends on s0, s3 on s1, s4 on both s2 and s3.
        //   s0 ──► s2 ─┐
        //   s1 ──► s3 ─┴► s4
        let mut steps = make_steps(5);
        let edges = [(2, 0), (3, 1), (4, 2), (4, 3)];
        let deps_of = deps_map(&steps, &edges);

        let depths = compute_step_depths(&steps, &deps_of);
        assert_eq!(depths["s0"], 0);
        assert_eq!(depths["s1"], 0);
        assert_eq!(depths["s2"], 1);
        assert_eq!(depths["s3"], 1);
        assert_eq!(depths["s4"], 2);

        // Only the two roots are runnable initially (all Pending).
        let win_status: HashMap<&str, StepStatus> =
            steps.iter().map(|s| (s.id.as_str(), s.status)).collect();
        let runnable_now: Vec<&str> = steps
            .iter()
            .filter(|s| deps_satisfied(&s.id, &deps_of, &win_status))
            .map(|s| s.id.as_str())
            .collect();
        assert_eq!(runnable_now, vec!["s0", "s1"]);

        let order = schedule_order(&mut steps, &edges, &full_window());
        // Roots first (depth 0, by sort_key), then depth-1, then the sink.
        assert_eq!(order, vec!["s0", "s1", "s2", "s3", "s4"]);
    }

    #[test]
    fn test_scheduler_diamond() {
        // s0 ─┬► s1 ─┐
        //     └► s2 ─┴► s3
        let mut steps = make_steps(4);
        let edges = [(1, 0), (2, 0), (3, 1), (3, 2)];
        let deps_of = deps_map(&steps, &edges);

        let depths = compute_step_depths(&steps, &deps_of);
        assert_eq!(depths["s0"], 0);
        assert_eq!(depths["s1"], 1);
        assert_eq!(depths["s2"], 1);
        assert_eq!(depths["s3"], 2); // 1 + max(depth(s1), depth(s2))

        // The sink stays blocked until BOTH mid steps are Complete.
        let mut work = steps.clone();
        for s in work.iter_mut() {
            if s.id == "s0" || s.id == "s1" {
                s.status = StepStatus::Complete;
            }
        }
        let win_status: HashMap<&str, StepStatus> =
            work.iter().map(|s| (s.id.as_str(), s.status)).collect();
        assert!(
            !deps_satisfied("s3", &deps_of, &win_status),
            "s3 must wait for s2 even though s1 is Complete"
        );

        let order = schedule_order(&mut steps, &edges, &full_window());
        assert_eq!(order, vec!["s0", "s1", "s2", "s3"]);
    }

    #[test]
    fn test_deps_satisfied_only_complete_unblocks() {
        let steps = make_steps(2);
        let deps_of = deps_map(&steps, &[(1, 0)]); // s1 depends on s0

        for blocking in [
            StepStatus::Pending,
            StepStatus::InProgress,
            StepStatus::Failed,
            StepStatus::Aborted,
            StepStatus::Skipped, // §14 open question: Skipped does NOT unblock
        ] {
            let win_status: HashMap<&str, StepStatus> = [("s0", blocking)].into_iter().collect();
            assert!(
                !deps_satisfied("s1", &deps_of, &win_status),
                "{blocking:?} dep must block its dependent"
            );
        }

        let win_status: HashMap<&str, StepStatus> =
            [("s0", StepStatus::Complete)].into_iter().collect();
        assert!(deps_satisfied("s1", &deps_of, &win_status));

        // A dep absent from window_status (out of window / deleted) does
        // not block — keeps the graph from deadlocking and preserves
        // `--from`/`--to`.
        assert!(deps_satisfied("s1", &deps_of, &HashMap::new()));
    }

    // ---- STEP 38: concurrency model (§9-inv-1/2, §3.5 item 2/3) ----

    // -- honest, scheduler-driven concurrency proofs --
    //
    // These drive the REAL `run_plan` scheduler on a 2-independent-branch
    // DAG with stub implementation + a deliberately slow stub review. Each
    // stub appends a wall-clock-timestamped phase marker to a shared event
    // log; we parse that log to prove (i) real wall-clock overlap of an
    // unrelated implementation with an outstanding review and (ii) that two
    // implementations never overlap (semaphore=1 still holds). They replace
    // two earlier tests that hand-poked a bare `tokio::sync::Semaphore` and
    // never drove `run_plan` (so they could not have caught the inline-
    // review serialization defect this commit fixes).

    /// One parsed phase marker: `(phase, pid, t_ns)`. `phase` is one of
    /// `IMPL_START`/`IMPL_END`/`REV_START`/`REV_END`; `pid` correlates a
    /// START with its matching END (multiple reviews can be in flight at
    /// once — they share no semaphore — so START/END pairs can nest and a
    /// single-slot stack is wrong; the pid disambiguates).
    #[cfg(test)]
    fn parse_event_log(path: &std::path::Path) -> Vec<(String, String, u128)> {
        let raw = std::fs::read_to_string(path).unwrap_or_default();
        let mut ev: Vec<(String, String, u128)> = raw
            .lines()
            .filter_map(|l| {
                let mut it = l.split_whitespace();
                let phase = it.next()?.to_string();
                let pid = it.next()?.to_string();
                let ts: u128 = it.next()?.parse().ok()?;
                Some((phase, pid, ts))
            })
            .collect();
        ev.sort_by_key(|(_, _, t)| *t);
        ev
    }

    /// Reconstruct `(start_ns, end_ns)` intervals for `phase_start` /
    /// `phase_end` markers, correlating by pid so nested/overlapping
    /// intervals of the same phase are reconstructed correctly.
    #[cfg(test)]
    fn intervals(
        ev: &[(String, String, u128)],
        phase_start: &str,
        phase_end: &str,
    ) -> Vec<(u128, u128)> {
        use std::collections::HashMap;
        let mut open: HashMap<String, u128> = HashMap::new();
        let mut out: Vec<(u128, u128)> = Vec::new();
        for (phase, pid, t) in ev {
            if phase == phase_start {
                open.insert(pid.clone(), *t);
            } else if phase == phase_end
                && let Some(s) = open.remove(pid)
            {
                out.push((s, *t));
            }
        }
        out
    }

    /// Build a stub harness `HarnessConfig` that runs `sh <script>` (the
    /// CLAUDE.md ETXTBSY-safe invocation).
    #[cfg(test)]
    fn sh_stub_harness(script: &str) -> crate::config::HarnessConfig {
        crate::config::HarnessConfig {
            command: "sh".to_string(),
            args: vec![script.to_string()],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
            color: None,
        }
    }

    /// HONEST PROOF (§2 Decision 3 / §3.5 item 3 / §9-inv-1/2): driving the
    /// real `run_plan` scheduler on TWO independent root steps with a slow
    /// stub review proves that (i) the unrelated branch's IMPLEMENTATION
    /// actually starts and finishes while the first step's review is still
    /// in flight (true wall-clock overlap), and (ii) the two
    /// implementations never overlap (impl semaphore = 1 still holds).
    ///
    /// What this proves: under the fixed runner, some implementation
    /// interval *intersects* some review interval on the wall clock (and
    /// because a step's own review is spawned only after its own impl ends,
    /// that intersection is necessarily cross-step — an UNRELATED branch
    /// implemented while another step's review was in flight) AND no two
    /// IMPL intervals overlap (impl semaphore=1). Under the pre-fix
    /// inline-review code the scheduler `await`ed the review before picking
    /// the next step, so every impl interval was strictly disjoint from
    /// every review interval and this test would fail — which is exactly
    /// why it is the honest replacement.
    ///
    /// What this does NOT prove: it does not assert a specific scheduler
    /// pick order beyond "both independent steps run", nor anything about
    /// review *correctness* (covered by review.rs unit tests); only the
    /// concurrency *shape*.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_run_plan_overlaps_unrelated_impl_with_in_flight_review() {
        use std::fs;
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();

        // Scripts + the shared event log live OUTSIDE the git workdir so
        // they are never seen as a tracked/untracked "change".
        let ext = tempfile::TempDir::new().unwrap();
        let evlog = ext.path().join("events.log");
        let evlog_s = evlog.to_string_lossy().into_owned();

        // Implementation stub: timestamp IMPL_START, create a UNIQUE file
        // (so every step produces a real change to commit), timestamp
        // IMPL_END, exit 0. `date +%s%N` is nanosecond wall-clock.
        let impl_sh = ext.path().join("impl.sh");
        fs::write(
            &impl_sh,
            format!(
                "#!/bin/sh\n\
                 echo \"IMPL_START $$ $(date +%s%N)\" >> {log}\n\
                 f=\"change_$$_$(date +%s%N).txt\"\n\
                 echo work > \"$f\"\n\
                 sleep 0.15\n\
                 echo \"IMPL_END $$ $(date +%s%N)\" >> {log}\n",
                log = evlog_s
            ),
        )
        .unwrap();

        // Review stub: timestamp REV_START, sleep LONG (so an unrelated
        // impl has ample time to start+finish during it), timestamp
        // REV_END, then PASS.
        let rev_sh = ext.path().join("rev.sh");
        fs::write(
            &rev_sh,
            format!(
                "#!/bin/sh\n\
                 echo \"REV_START $$ $(date +%s%N)\" >> {log}\n\
                 sleep 0.8\n\
                 echo \"REV_END $$ $(date +%s%N)\" >> {log}\n\
                 echo 'REVIEW PASS'\n",
                log = evlog_s
            ),
        )
        .unwrap();

        let conn = setup();
        seed_run_lock_row(&conn, &project);
        let plan = storage::create_plan(
            &conn,
            "concurrent",
            &project,
            "feat/concurrent",
            "d",
            Some("impl"),
            None,
            &[],
        )
        .unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        // Two INDEPENDENT root steps (no edges between them): both runnable
        // from the start, so the scheduler is FREE to pick the second while
        // the first is under review.
        for title in ["Alpha", "Beta"] {
            storage::create_step(
                &conn,
                &plan.id,
                title,
                "d",
                None,
                None,
                &[],
                Some(0),
                None,
                None,
                None,
            )
            .unwrap();
        }

        let plan = storage::get_plan_by_slug(&conn, "concurrent", &project)
            .unwrap()
            .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "impl".to_string(),
            sh_stub_harness(&impl_sh.to_string_lossy()),
        );
        config.harnesses.insert(
            "reviewer".to_string(),
            sh_stub_harness(&rev_sh.to_string_lossy()),
        );
        config.review.enabled = Some(true);
        config.review.harness = "reviewer".to_string();

        let (_tx, rx) = watch::channel(None);
        let out = OutputContext::from_cli(false, false, false);
        let options = RunOptions {
            current_branch: true,
            ..Default::default()
        };

        let result = run_plan(&conn, &plan, &config, &dir, &options, rx, &out)
            .await
            .unwrap();

        // Both steps ran and the plan finished cleanly (both reviews PASS,
        // both promoted to Complete, drained before terminal status).
        assert_eq!(result.steps_succeeded, 2, "both independent steps ran");
        assert_eq!(result.final_status, PlanStatus::Complete);
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(
            steps.iter().all(|s| s.status == StepStatus::Complete
                && s.review_status == Some(crate::plan::ReviewStatus::Passed)),
            "every step Complete + review Passed (no review left pending — §3.3)"
        );

        let ev = parse_event_log(&evlog);
        // Reconstruct interval lists (pid-correlated so overlapping reviews
        // are reconstructed correctly).
        let impls = intervals(&ev, "IMPL_START", "IMPL_END");
        let revs = intervals(&ev, "REV_START", "REV_END");
        assert_eq!(
            impls.len(),
            2,
            "two implementations ran (one per step); events: {ev:?}"
        );
        assert_eq!(
            revs.len(),
            2,
            "two reviews ran (one per step); events: {ev:?}"
        );

        // (i) REAL OVERLAP: some implementation interval intersects some
        // review interval on the wall clock (`is < re && rs < ie`). Because
        // a step's own review is spawned only AFTER its implementation has
        // ended (the executor commits, then the runner spawns the review),
        // an impl∩review intersection is necessarily *cross-step* — i.e. an
        // UNRELATED branch implemented while another step's review was in
        // flight. This is the property the pre-fix inline-review code
        // structurally CANNOT satisfy: it `await`ed `run_review` before
        // picking the next step, so every impl interval was strictly
        // disjoint from every review interval.
        let overlap = impls
            .iter()
            .any(|&(is, ie)| revs.iter().any(|&(rs, re)| is < re && rs < ie));
        assert!(
            overlap,
            "an unrelated implementation MUST overlap an in-flight review \
             on the wall clock (§2 Decision 3 / §3.5 item 3); events: {ev:?}"
        );

        // (ii) SEMAPHORE = 1: no two implementation intervals overlap.
        for (i, &(s1, e1)) in impls.iter().enumerate() {
            for &(s2, e2) in impls.iter().skip(i + 1) {
                assert!(
                    e1 <= s2 || e2 <= s1,
                    "two implementations overlapped — impl semaphore=1 \
                     violated (§9-inv-1); events: {ev:?}"
                );
            }
        }
    }

    /// §3.5 item 3 / §3.1: a step's DIRECT DEPENDENTS are not runnable until
    /// that step is `Complete`. While review keeps the reviewed step
    /// `InProgress` (executor's review gate), `deps_satisfied` (which
    /// requires `Complete`) excludes its dependents from the runnable set —
    /// so concurrency never starts work on un-reviewed output.
    #[test]
    fn test_direct_dependents_gated_until_reviewed_step_complete() {
        let steps = make_steps(3); // s0, s1, s2
        // s1 and s2 both directly depend on s0 (the reviewed step).
        let deps_of = deps_map(&steps, &[(1, 0), (2, 0)]);
        let depths = compute_step_depths(&steps, &deps_of);

        // s0 is `InProgress` — exactly the state the executor leaves a
        // review-gated step in (NOT `Complete`) until its review returns.
        let mut steps_ip = steps.clone();
        steps_ip[0].status = StepStatus::InProgress;
        let win: HashMap<&str, StepStatus> =
            steps_ip.iter().map(|s| (s.id.as_str(), s.status)).collect();
        assert!(
            !deps_satisfied("s1", &deps_of, &win),
            "a direct dependent must NOT be runnable while the reviewed \
             step is still InProgress (review not yet returned)"
        );
        assert!(!deps_satisfied("s2", &deps_of, &win));
        // The scheduler therefore picks neither dependent (only s0 is
        // actionable, and it's already executing/being reviewed).
        let pick = pick_next_step(
            &steps_ip,
            &deps_of,
            &depths,
            &full_window(),
            &["s0".to_string()].into_iter().collect(),
            &HashSet::new(),
        );
        assert!(
            pick.is_none(),
            "no dependent may start on un-reviewed work (§3.5 item 3)"
        );

        // Once review returns and the orchestrator promotes s0 to Complete,
        // its dependents become runnable on the very next tick.
        let mut steps_done = steps;
        steps_done[0].status = StepStatus::Complete;
        let win2: HashMap<&str, StepStatus> = steps_done
            .iter()
            .map(|s| (s.id.as_str(), s.status))
            .collect();
        assert!(deps_satisfied("s1", &deps_of, &win2));
        assert!(deps_satisfied("s2", &deps_of, &win2));
    }

    /// LINEAR-PARITY PROOF (§9-inv-5 / §1): with no review config and no
    /// edges (or a degenerate chain), the scheduler tie-break still
    /// reproduces the authored sort_key order EXACTLY — a linear plan
    /// serializes and behaves exactly as today. (The review pipeline is
    /// `needs_review = None` for every step when review is not
    /// effective-enabled, so the loop never spawns a review at all.)
    #[test]
    fn test_linear_plan_serializes_exactly_as_today_under_review_code() {
        // A degenerate chain s0<-s1<-s2<-s3 (the V25 backfill shape).
        let mut steps = make_steps(4);
        let order = schedule_order(&mut steps, &[(1, 0), (2, 1), (3, 2)], &full_window());
        assert_eq!(
            order,
            vec!["s0", "s1", "s2", "s3"],
            "a linear chain must execute in authored order, byte-identical \
             to the pre-review behavior"
        );
        // And review being OFF by default means a no-review Plan/Step never
        // sets the executor's review gate. Assert the default config.
        let cfg = crate::config::Config::default();
        assert_eq!(
            cfg.review.enabled, None,
            "review is OFF by default — linear/no-config plans never review"
        );
    }

    #[test]
    fn test_scheduler_short_id_breaks_sort_key_ties() {
        // Force a depth + sort_key tie; short_id is the final, stable
        // discriminator (§3.5 item 4).
        let mut steps = make_steps(2);
        steps[0].sort_key = "a0".to_string();
        steps[1].sort_key = "a0".to_string();
        steps[0].short_id = "zzzzzzzz".to_string();
        steps[1].short_id = "aaaaaaaa".to_string();
        let deps_of = deps_map(&steps, &[]);
        let depths = compute_step_depths(&steps, &deps_of);

        let pick = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(pick.unwrap().id, "s1"); // lower short_id wins the tie
    }

    #[test]
    fn test_scheduler_window_excludes_out_of_window_dep() {
        // `--from`-style window on a V25-style linear chain: the excluded
        // prerequisite must NOT gate the first in-window step, so the
        // windowed steps run in chain order exactly as the pre-DAG loop.
        let mut steps = make_steps(4); // s0<-s1<-s2<-s3, sort_keys a0..a3
        let edges = [(1, 0), (2, 1), (3, 2)];
        // Window starts at s2 (sort_key "a2"); s0/s1 are excluded.
        let window = RunWindow {
            from_key: Some("a2".to_string()),
            to_key: None,
        };

        let deps_of = deps_map(&steps, &edges);
        let depths = compute_step_depths(&steps, &deps_of);
        // s2's dep (s1) is out of window ⇒ s2 is the first pick.
        let pick = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &window,
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(pick.unwrap().id, "s2");

        let order = schedule_order(&mut steps, &edges, &window);
        assert_eq!(order, vec!["s2", "s3"]);
    }

    #[test]
    fn test_scheduler_skips_executed_and_is_deterministic() {
        let steps = make_steps(3);
        let deps_of = deps_map(&steps, &[]);
        let depths = compute_step_depths(&steps, &deps_of);

        // Determinism: repeated ticks on identical state pick the same step.
        let a = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &HashSet::new(),
            &HashSet::new(),
        );
        let b = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(a.unwrap().id, "s0");
        assert_eq!(b.unwrap().id, "s0");

        // A step already executed this invocation is not re-picked.
        let mut executed = HashSet::new();
        executed.insert("s0".to_string());
        let pick = pick_next_step(
            &steps,
            &deps_of,
            &depths,
            &full_window(),
            &executed,
            &HashSet::new(),
        );
        assert_eq!(pick.unwrap().id, "s1");

        // Nothing runnable ⇒ None.
        let mut done = make_steps(2);
        for s in done.iter_mut() {
            s.status = StepStatus::Complete;
        }
        let deps_of = deps_map(&done, &[]);
        let depths = compute_step_depths(&done, &deps_of);
        assert!(
            pick_next_step(
                &done,
                &deps_of,
                &depths,
                &full_window(),
                &HashSet::new(),
                &HashSet::new()
            )
            .is_none()
        );
    }

    // -- linear-plan parity regression (docs/dag-redesign.md §1, §3.5
    //    item 4, §13.1) --
    //
    // Phase-1 hard invariant: a plan whose only edges are the V25
    // linear-chain backfill must execute in EXACTLY today's `sort_key`
    // order, and any DAG must execute in EXACTLY the documented
    // `(depth, sort_key, short_id)` order — stable and reproducible
    // given identical inputs. The §3.5 tests above assert specific
    // orders against hand-computed expectations; these instead compare
    // the scheduler's emission against an *independent oracle* (the
    // steps sorted by the documented key) and prove the emission is a
    // pure function of the DAG + tie-break tuple, never of the order
    // the steps happen to arrive in or of how many times the scheduler
    // is run. That is the precise byte-identical-to-pre-DAG claim.

    /// Apply a fixed, deterministic, fixed-point-free permutation (for
    /// `n >= 2`) to a step vec: reverse, then rotate left by one. No
    /// `rand` dependency, so the "reproducible given identical inputs"
    /// assertions stay deterministic. Used to prove the scheduler's
    /// emission order does not depend on input-slice position.
    fn scrambled(steps: &[Step]) -> Vec<Step> {
        let mut v: Vec<Step> = steps.iter().rev().cloned().collect();
        v.rotate_left(1);
        v
    }

    /// Translate id-keyed `(dependent_id, dependency_id)` pairs into the
    /// index-edge form `deps_map`/`schedule_order` expect, resolved
    /// against *this* slice ordering. Lets a test scramble the step vec
    /// while keeping the same logical DAG.
    fn edges_for(steps: &[Step], id_pairs: &[(&str, &str)]) -> Vec<(usize, usize)> {
        let pos = |id: &str| steps.iter().position(|s| s.id == id).unwrap();
        id_pairs.iter().map(|&(a, b)| (pos(a), pos(b))).collect()
    }

    /// The documented deterministic emission order (§3.5 item 4): every
    /// step sorted by `(topological depth, sort_key, short_id)`, using
    /// the same `compute_step_depths` the scheduler uses. For an
    /// all-`Pending`, full-window DAG the scheduler emits exactly this
    /// total order: a dependency always has strictly smaller depth —
    /// hence a strictly smaller tuple — than its dependents, so the
    /// global tuple-minimum unexecuted step is always runnable. This is
    /// the independent oracle, not a re-derivation of the scheduler.
    fn tie_break_order(steps: &[Step], id_pairs: &[(&str, &str)]) -> Vec<String> {
        let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
        for &(a, b) in id_pairs {
            deps_of
                .entry(a.to_string())
                .or_default()
                .push(b.to_string());
        }
        let depths = compute_step_depths(steps, &deps_of);
        let mut ranked: Vec<&Step> = steps.iter().collect();
        ranked.sort_by(|a, b| {
            depths[&a.id]
                .cmp(&depths[&b.id])
                .then_with(|| a.sort_key.cmp(&b.sort_key))
                .then_with(|| a.short_id.cmp(&b.short_id))
        });
        ranked.into_iter().map(|s| s.id.clone()).collect()
    }

    /// Run the scheduler on the original input order and on several
    /// fixed permutations of it, repeatedly, asserting every run emits
    /// `expected`. This is the "stable and reproducible given identical
    /// inputs" guarantee of §3.5 item 4.
    fn assert_reproducible(steps: &[Step], id_pairs: &[(&str, &str)], expected: &[String]) {
        let orderings: Vec<Vec<Step>> = vec![
            steps.to_vec(),
            scrambled(steps),
            scrambled(&scrambled(steps)),
            steps.iter().rev().cloned().collect(),
        ];
        for _ in 0..3 {
            for ord in &orderings {
                let mut s = ord.clone();
                let edges = edges_for(&s, id_pairs);
                let order = schedule_order(&mut s, &edges, &full_window());
                assert_eq!(
                    order, expected,
                    "scheduler emission must be stable & reproducible \
                     regardless of input-slice order or run count"
                );
            }
        }
    }

    #[test]
    fn test_linear_chain_is_byte_identical_to_sort_key_order() {
        // V25 backfill: order a plan's steps by `sort_key`, then chain
        // each onto its sort_key-predecessor. The scheduler must emit
        // them in EXACTLY `sort_key` order — byte-identical to the
        // pre-DAG loop, which iterated `list_steps` (`ORDER BY
        // sort_key`) — no matter what order the rows arrive in.
        let mut steps = make_steps(5);
        // sort_keys deliberately NOT in `s{i}`/input order, so the
        // oracle is a non-trivial permutation and the assertion bites.
        let keys = ["a30", "a10", "a40", "a00", "a20"];
        for (s, k) in steps.iter_mut().zip(keys) {
            s.sort_key = k.to_string();
        }
        // Realistic V25: every step carries a distinct minted short_id.
        for (i, s) in steps.iter_mut().enumerate() {
            s.short_id = format!("h{i}");
        }

        // Independent oracle: the steps sorted by `sort_key` — exactly
        // what `list_steps` yields, hence exactly the pre-DAG order.
        let mut by_key: Vec<&Step> = steps.iter().collect();
        by_key.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
        let expected: Vec<String> = by_key.iter().map(|s| s.id.clone()).collect();
        assert_eq!(
            expected,
            vec!["s3", "s1", "s4", "s0", "s2"],
            "sanity: sort_key order is the intended non-trivial permutation"
        );

        // V25 backfill edges: each step depends on its sort_key
        // predecessor (the linear chain).
        let chain: Vec<&str> = expected.iter().map(|s| s.as_str()).collect();
        let id_pairs: Vec<(&str, &str)> =
            (1..chain.len()).map(|i| (chain[i], chain[i - 1])).collect();

        // The chain's topological depth ranking equals its sort_key
        // ranking, so `(depth, sort_key, short_id)` collapses to
        // sort_key — i.e. the DAG oracle agrees with the parity oracle.
        assert_eq!(tie_break_order(&steps, &id_pairs), expected);

        // Emission equals the sort_key oracle, regardless of input
        // order and repeated runs.
        assert_reproducible(&steps, &id_pairs, &expected);
    }

    #[test]
    fn test_multi_root_dag_order_is_oracle_stable() {
        //   s0 ──► s2 ─┐
        //   s1 ──► s3 ─┴► s4
        // sort_keys interleave the two roots / two mids so the
        // expected order is not the `s{i}` order — proving the
        // `(depth, sort_key, …)` ranking, not authored index, decides.
        let mut steps = make_steps(5);
        let keys = ["a50", "a10", "a40", "a20", "a99"];
        for (s, k) in steps.iter_mut().zip(keys) {
            s.sort_key = k.to_string();
        }
        for (i, s) in steps.iter_mut().enumerate() {
            s.short_id = format!("k{i}");
        }
        let id_pairs = [("s2", "s0"), ("s3", "s1"), ("s4", "s2"), ("s4", "s3")];

        let expected = tie_break_order(&steps, &id_pairs);
        // depth0 {s1@a10, s0@a50} → s1,s0 ; depth1 {s3@a20, s2@a40} →
        // s3,s2 ; depth2 s4.
        assert_eq!(
            expected,
            vec!["s1", "s0", "s3", "s2", "s4"],
            "multi-root order is determined by (depth, sort_key)"
        );
        assert_reproducible(&steps, &id_pairs, &expected);
    }

    #[test]
    fn test_diamond_dag_short_id_is_stable_final_discriminator() {
        // s0 ─┬► s1 ─┐
        //     └► s2 ─┴► s3
        // s1 and s2 share a sort_key (same depth too): the
        // tie-break must fall through to `short_id` and stay stable
        // and reproducible across runs and input orderings.
        let mut steps = make_steps(4);
        let keys = ["a05", "a10", "a10", "a90"]; // s1 / s2 sort_key tie
        for (s, k) in steps.iter_mut().zip(keys) {
            s.sort_key = k.to_string();
        }
        let short_ids = ["m0", "zzz", "aaa", "m3"];
        for (s, sid) in steps.iter_mut().zip(short_ids) {
            s.short_id = sid.to_string();
        }
        let id_pairs = [("s1", "s0"), ("s2", "s0"), ("s3", "s1"), ("s3", "s2")];

        let expected = tie_break_order(&steps, &id_pairs);
        // depth1 tie on sort_key "a10" broken by short_id: "aaa"(s2) <
        // "zzz"(s1) ⇒ s2 before s1.
        assert_eq!(
            expected,
            vec!["s0", "s2", "s1", "s3"],
            "short_id is the stable final discriminator under a sort_key tie"
        );
        assert_reproducible(&steps, &id_pairs, &expected);
    }
}
