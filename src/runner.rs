// Plan runner / orchestrator
//
// Implements the plan-level execution loop: loading a plan, iterating through
// steps in sort_key order, executing each via the single-step executor, and
// managing plan-level status transitions.

use std::collections::{HashMap, HashSet};
use std::path::Path;
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
    abort_rx: watch::Receiver<bool>,
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
    }

    // 2. Stash dirty tree + create branch. The orchestrator
    // (`run_all_plans`) handles stash/setup itself and forces
    // `current_branch: true` on the inner `run_plan` call, so only
    // top-level single-plan runs take this path.
    let teardown = if !options.current_branch && !options.dry_run {
        let source_branch = {
            let workdir_owned = workdir.to_path_buf();
            blocking_git(move || git::get_current_branch(&workdir_owned))
                .await
                .context("Failed to get current git branch")?
        };
        let stash_ref =
            stash_if_dirty(workdir, &effective_plan.slug, options.auto_stash).await?;
        // Record source_branch + stash_sha on the run_lock row so resume /
        // diagnostics can see what we'll try to restore. Best-effort — if
        // the row isn't there (tests), swallow the error.
        let _ = run_lock::record_source_branch_and_stash(
            conn,
            workdir.to_string_lossy().as_ref(),
            &source_branch,
            stash_ref.as_ref().map(|s| s.as_str()),
        );
        // Construct teardown state BEFORE setup_branch so that a failure in
        // branch creation/checkout still triggers stash restoration. Without
        // this, a bad branch name or checkout conflict would leave the user's
        // uncommitted work stranded on the stash stack.
        let td = TeardownState {
            workdir: workdir.to_path_buf(),
            source_branch,
            stash_ref,
        };
        if let Err(setup_err) = setup_branch(workdir, &effective_plan, None).await {
            if let Err(te) =
                restore_working_tree(&td.workdir, &td.source_branch, td.stash_ref.as_ref()).await
            {
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
                restore_working_tree(&td.workdir, &td.source_branch, td.stash_ref.as_ref())
                    .await?;
            }
            Err(_) => {
                // Run already failed; log teardown errors but don't mask
                // the original failure.
                if let Err(te) = restore_working_tree(
                    &td.workdir,
                    &td.source_branch,
                    td.stash_ref.as_ref(),
                )
                .await
                {
                    eprintln!("Warning: teardown after failed run: {te}");
                }
            }
        }
    }

    outcome
}

/// State captured by the top-level `run_plan` before the plan body runs.
/// Handed to `restore_working_tree` during teardown.
struct TeardownState {
    workdir: std::path::PathBuf,
    source_branch: String,
    stash_ref: Option<StashRef>,
}

async fn run_plan_inner(
    conn: &Connection,
    effective_plan: &Plan,
    config: &Config,
    workdir: &Path,
    options: &RunOptions,
    abort_rx: watch::Receiver<bool>,
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

    // Snapshot of currently-actionable steps in the window. Used to capture
    // the `--one` target (earliest actionable step) before we start mutating
    // state. If the window contains NO steps at all (e.g. a bogus
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
    let initial_actionable: Vec<Step> = window_steps
        .iter()
        .filter(|s| is_actionable(s.status))
        .map(|s| (*s).clone())
        .collect();

    // Dry-run mode: just print what would happen.
    if options.dry_run {
        let steps_to_run: Vec<Step> = window_steps.iter().map(|s| (*s).clone()).collect();
        return dry_run_report(&effective_plan, &initial_steps, &steps_to_run);
    }

    // 3. Mark plan as in_progress.
    if effective_plan.status != PlanStatus::InProgress {
        storage::update_plan_status(conn, &effective_plan.id, PlanStatus::InProgress)?;
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

    let mut known_step_ids: HashSet<String> =
        initial_steps.iter().map(|s| s.id.clone()).collect();
    let mut executed_step_ids: HashSet<String> = HashSet::new();

    // For `--one`, we need to stop after the first step actually executed;
    // capture its ID at the start (the earliest actionable step in the
    // window) and exit after it completes. Positions can shift due to
    // inserts, but the ID is stable. If `--one` is requested but nothing
    // is actionable, bail — mirrors the pre-fix behavior of `select_steps`
    // returning an empty slice in that case.
    let one_target_id: Option<String> = if options.one {
        match initial_actionable.first() {
            Some(s) => Some(s.id.clone()),
            None => bail!("No pending steps to run in plan '{}'", effective_plan.slug),
        }
    } else {
        None
    };

    loop {
        // Check abort signal between steps.
        if *abort_rx.borrow() {
            eprintln!("Aborted");
            storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
            result.final_status = PlanStatus::Aborted;
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

        // Find the next step to execute in the window: first actionable step
        // whose ID we haven't already executed in this invocation.
        let next = all_steps
            .iter()
            .find(|s| {
                window.contains_key(&s.sort_key)
                    && is_actionable(s.status)
                    && !executed_step_ids.contains(&s.id)
            })
            .cloned();

        let current_step = match next {
            Some(s) => s,
            None => break, // no more actionable steps in the window
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
        if current_step.status == StepStatus::Complete
            || current_step.status == StepStatus::Skipped
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
            },
        )
        .await?;

        let elapsed = started.elapsed();
        result.steps_executed += 1;
        executed_step_ids.insert(current_step.id.clone());

        // Print result / emit step_finished event.
        let outcome_str = match step_result.outcome {
            StepOutcome::Success => "success",
            StepOutcome::Failed => "failed",
            StepOutcome::Aborted => "aborted",
            StepOutcome::Timeout => "timeout",
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
        }

        result.step_results.push(step_result);

        // `--one`: stop after executing the captured target.
        if one_target_id.is_some() {
            break;
        }
    }

    // All steps completed successfully.
    // Check if *all* steps in the plan are done (not just the subset we ran).
    let final_steps = storage::list_steps(conn, &effective_plan.id)?;
    let all_done = final_steps
        .iter()
        .all(|s| s.status == StepStatus::Complete || s.status == StepStatus::Skipped);

    if all_done {
        storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Complete)?;
        result.final_status = PlanStatus::Complete;
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
    abort_rx: watch::Receiver<bool>,
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

    // 3. Capture the run's starting SHA (used for plans with no deps) and,
    // on the stash-managing path, stash any dirty tree + record the source
    // branch for teardown.
    let teardown = if !options.current_branch && !options.dry_run {
        let source_branch = {
            let workdir_owned = workdir.to_path_buf();
            blocking_git(move || git::get_current_branch(&workdir_owned))
                .await
                .context("Failed to get current git branch")?
        };
        let stash_ref = stash_if_dirty(workdir, "all", options.auto_stash).await?;
        let _ = run_lock::record_source_branch_and_stash(
            conn,
            project,
            &source_branch,
            stash_ref.as_ref().map(|s| s.as_str()),
        );
        Some(TeardownState {
            workdir: workdir.to_path_buf(),
            source_branch,
            stash_ref,
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
                restore_working_tree(&td.workdir, &td.source_branch, td.stash_ref.as_ref())
                    .await?;
            }
            Err(_) => {
                if let Err(te) = restore_working_tree(
                    &td.workdir,
                    &td.source_branch,
                    td.stash_ref.as_ref(),
                )
                .await
                {
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
    abort_rx: watch::Receiver<bool>,
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
        // Abort check between plans.
        if *abort_rx.borrow() {
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
    abort_rx: watch::Receiver<bool>,
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
        ..Default::default()
    };

    run_plan(conn, plan, config, workdir, &options, abort_rx, out).await
}

/// Skip the current (or specified) step in a plan.
///
/// Marks the step as skipped and returns the step number that was skipped.
/// The optional `reason` is persisted on the step so it appears in
/// `ralph status -v` and `ralph log`.
pub fn skip_step(
    conn: &Connection,
    plan: &Plan,
    step_num: Option<usize>,
    reason: Option<&str>,
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
    match step.status {
        StepStatus::Pending | StepStatus::Failed | StepStatus::InProgress => {}
        StepStatus::Complete => bail!("Step {} '{}' is already complete", actual_num, step.title),
        StepStatus::Skipped => bail!("Step {} '{}' is already skipped", actual_num, step.title),
        StepStatus::Aborted => {
            // Allow skipping aborted steps too.
        }
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
    }
}

/// If the working tree is dirty, stash it with a ralph-branded message and
/// return the stash's commit SHA. Returns `Ok(None)` on a clean tree. Bails
/// with a user-facing error when the tree is dirty and `auto_stash` is
/// false.
///
/// The stash message is `"ralph: auto-stash for plan '<slug>' at
/// <ISO-8601>"` so teardown (or manual recovery) can locate it by subject.
async fn stash_if_dirty(
    workdir: &Path,
    plan_slug: &str,
    auto_stash: bool,
) -> Result<Option<StashRef>> {
    let workdir_owned = workdir.to_path_buf();
    let dirty = blocking_git(move || git::has_uncommitted_changes(&workdir_owned)).await?;
    if !dirty {
        return Ok(None);
    }

    if !auto_stash {
        let workdir_owned = workdir.to_path_buf();
        let files = blocking_git(move || git::get_all_changed_files(&workdir_owned)).await?;
        let mut msg = format!(
            "Working tree has uncommitted changes; refusing to switch branches \
             with {} file(s) dirty:\n",
            files.len(),
        );
        for f in &files {
            msg.push_str("  ");
            msg.push_str(f);
            msg.push('\n');
        }
        msg.push_str(
            "Re-run without --no-auto-stash to let ralph preserve your changes \
             via `git stash push --include-untracked`, or stash/commit them manually first.",
        );
        bail!(msg);
    }

    // Timestamp for traceability; the pairing of plan slug + timestamp makes
    // every ralph stash line distinct on the stack.
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let message = format!("ralph: auto-stash for plan '{plan_slug}' at {ts}");

    let workdir_owned = workdir.to_path_buf();
    let msg_owned = message.clone();
    let stash = blocking_git(move || git::stash_push_with_untracked(&workdir_owned, &msg_owned))
        .await
        .context("failed to stash dirty working tree before switching branches")?;
    Ok(stash)
}

/// Switch back to `source_branch` and, if we stashed at run start, pop the
/// stash. Called once at the end of the top-level run regardless of
/// outcome.
///
/// On `checkout <source>` failure (e.g. the plan branch has uncommitted
/// changes from a misbehaving hook), we surface the error and leave the
/// user where they are. On `stash pop` conflict, we leave the stash on the
/// stack and return a non-zero error — the user pops manually.
async fn restore_working_tree(
    workdir: &Path,
    source_branch: &str,
    stash_ref: Option<&StashRef>,
) -> Result<()> {
    // Only checkout if we're not already on the source branch (spares us
    // a spurious "already on 'X'" message and a no-op write).
    let current = {
        let workdir_owned = workdir.to_path_buf();
        blocking_git(move || git::get_current_branch(&workdir_owned)).await?
    };

    if current != source_branch {
        let workdir_owned = workdir.to_path_buf();
        let branch = source_branch.to_string();
        let checkout_result =
            blocking_git(move || git::checkout_branch(&workdir_owned, &branch)).await;
        if let Err(e) = checkout_result {
            bail!(
                "Failed to checkout source branch '{source_branch}' during run teardown: {e}. \
                 Your stash (if any) is still on the stack — run `git stash list` to inspect."
            );
        }
    }

    if let Some(stash) = stash_ref {
        let workdir_owned = workdir.to_path_buf();
        let stash_owned = stash.clone();
        let outcome =
            blocking_git(move || git::stash_pop(&workdir_owned, &stash_owned)).await?;
        match outcome {
            StashPopOutcome::Clean => {
                eprintln!("Restored your uncommitted changes.");
            }
            StashPopOutcome::Conflicted(stderr) => {
                bail!(
                    "Pop of ralph's stash conflicts with committed work. \
                     Your changes are preserved at {} — resolve manually with \
                     `git stash pop {}`.\n{}",
                    stash.as_str(),
                    stash.as_str(),
                    stderr,
                );
            }
            StashPopOutcome::NotFound => {
                eprintln!(
                    "Warning: ralph's auto-stash ({}) was no longer on the stack at teardown.",
                    stash.as_str(),
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
async fn setup_branch(
    workdir: &Path,
    plan: &Plan,
    parent_sha: Option<&str>,
) -> Result<()> {
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
        StepStatus::Pending
            | StepStatus::Failed
            | StepStatus::InProgress
            | StepStatus::Aborted
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
        bail!(
            "End step {} is out of range (plan has {} steps)",
            to,
            total
        );
    }
    if let (Some(from), Some(to)) = (options.from, options.to)
        && from > to
    {
        bail!(
            "Start step {} is greater than end step {}",
            from,
            to
        );
    }

    let from_key = options.from.map(|n| all_steps[n - 1].sort_key.clone());
    let to_key = options.to.map(|n| all_steps[n - 1].sort_key.clone());

    Ok(RunWindow { from_key, to_key })
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
        .map(|s| {
            format!(
                "#{} '{}'",
                step_number_in_plan(all_steps, s),
                s.title
            )
        })
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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
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

        let skipped = skip_step(&conn, &plan, Some(2), None).unwrap();
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

        let skipped = skip_step(&conn, &plan, None, None).unwrap();
        assert_eq!(skipped, 2);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Skipped);
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

        let result = skip_step(&conn, &plan, Some(1), None);
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

        let result = skip_step(&conn, &plan, Some(5), None);
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

        let skipped = skip_step(&conn, &plan, Some(1), Some("redundant after H7")).unwrap();
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

        skip_step(&conn, &plan, Some(1), None).unwrap();

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

        skip_step(&conn, &plan, Some(1), Some("because")).unwrap();
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

        let skipped = skip_step(&conn, &plan, Some(1), None).unwrap();
        assert_eq!(skipped, 1);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Skipped);
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
        let (_tx, rx) = watch::channel(false);
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
        let (_tx, rx) = watch::channel(false);
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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
        };

        // Should create feat/rooted rooted at initial_sha.
        setup_branch(&dir, &plan, Some(&initial_sha))
            .await
            .unwrap();
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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
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
        let (_tx, rx) = watch::channel(false);
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

    // -- stash_if_dirty / setup_branch --

    /// With `--no-auto-stash`, a dirty tree must bail cleanly and list the
    /// files that are blocking the switch so the user can stage or
    /// discard them intentionally.
    #[tokio::test(flavor = "current_thread")]
    async fn test_dirty_tree_no_auto_stash_bails_cleanly() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        let err = stash_if_dirty(&dir, "demo", /*auto_stash=*/ false)
            .await
            .expect_err("dirty tree with auto_stash=false must bail");
        let msg = format!("{err}");
        assert!(
            msg.contains("scratch.txt"),
            "error must list the dirty file, got: {msg}"
        );
        assert!(
            msg.contains("--no-auto-stash"),
            "error must point users at the opt-out flag, got: {msg}"
        );

        // Nothing was swept; the tree is still dirty.
        assert!(git::has_uncommitted_changes(&dir).unwrap());
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

        let stash = stash_if_dirty(&dir, "demo", /*auto_stash=*/ true)
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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
        };
        setup_branch(&dir, &plan, None).await.unwrap();
        assert_eq!(git::get_current_branch(&dir).unwrap(), "feat/stash-roundtrip");

        // Teardown: back to source_branch + pop stash.
        restore_working_tree(&dir, &source_branch, Some(&stash))
            .await
            .unwrap();

        assert_eq!(git::get_current_branch(&dir).unwrap(), source_branch);
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
    /// `auto_stash`, and teardown with no stash is just a branch switch.
    #[tokio::test(flavor = "current_thread")]
    async fn test_clean_tree_no_stash_needed() {
        let (_tmp, dir) = init_git_repo();

        let result_off = stash_if_dirty(&dir, "demo", false).await.unwrap();
        assert!(result_off.is_none());
        let result_on = stash_if_dirty(&dir, "demo", true).await.unwrap();
        assert!(result_on.is_none());

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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
        };
        let source_branch = git::get_current_branch(&dir).unwrap();
        setup_branch(&dir, &plan, None).await.unwrap();
        assert_eq!(git::get_current_branch(&dir).unwrap(), "feat/clean");
        restore_working_tree(&dir, &source_branch, None).await.unwrap();
        assert_eq!(git::get_current_branch(&dir).unwrap(), source_branch);
    }

    /// If the run produced a commit that CONFLICTS with the stashed
    /// working-tree state, teardown must leave the stash on the stack
    /// and return a non-zero error so the user can recover manually.
    #[tokio::test(flavor = "current_thread")]
    async fn test_stash_pop_conflict_during_teardown_preserves_stash() {
        use std::fs;

        let (_tmp, dir) = init_git_repo();
        let source_branch = git::get_current_branch(&dir).unwrap();

        // Pre-stash: README has version A queued up.
        fs::write(dir.join("README.md"), "# version A\n").unwrap();
        let stash = stash_if_dirty(&dir, "demo", true)
            .await
            .unwrap()
            .expect("sha");

        // Simulate a run that commits a divergent README to the source
        // branch. In practice this would be on the plan branch, but the
        // conflict materializes the same way when popping.
        fs::write(dir.join("README.md"), "# version B\n").unwrap();
        git::commit_changes(&dir, "divergent commit").unwrap();

        let err = restore_working_tree(&dir, &source_branch, Some(&stash))
            .await
            .expect_err("pop must surface the conflict");
        let msg = format!("{err}");
        assert!(
            msg.contains(stash.as_str()),
            "error must surface the stash SHA for manual recovery, got: {msg}"
        );

        // The stash is still on the stack.
        let still_there =
            git::find_stash_by_message(&dir, "ralph: auto-stash for plan 'demo'").unwrap();
        assert_eq!(still_there.as_ref(), Some(&stash));
    }

    /// The teardown path must fire even when the inner plan body fails.
    /// Drive `run_plan` against a plan with no steps — this produces an
    /// inner `bail!` AFTER stash_if_dirty + setup_branch have run — and
    /// assert that teardown still switched us back to the source branch
    /// and popped the stash.
    #[tokio::test(flavor = "current_thread")]
    async fn test_stash_pop_on_failure_still_fires() {
        use std::fs;
        use tokio::sync::watch;

        let (_tmp, dir) = init_git_repo();
        let project = dir.to_string_lossy().to_string();
        let source_branch = git::get_current_branch(&dir).unwrap();

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
        let (_tx, rx) = watch::channel(false);
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

        // Teardown must have switched us back to the source branch.
        assert_eq!(git::get_current_branch(&dir).unwrap(), source_branch);
        // And popped the stash — scratch.txt reappears.
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
        let plan =
            storage::create_plan(&conn, "bad", &project, "feat/bad..branch", "d", None, None, &[])
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
        let (_tx, rx) = watch::channel(false);
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

        let stash = stash_if_dirty(&dir, "crashy", true)
            .await
            .unwrap()
            .expect("sha");

        // Simulate the crash: skip the teardown entirely. The stash must
        // still be findable by its message and its SHA must still be on
        // the stack.
        let recovered =
            git::find_stash_by_message(&dir, "ralph: auto-stash for plan 'crashy'").unwrap();
        assert_eq!(recovered.as_ref(), Some(&stash));
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
            &conn, &plan.id, "First", "d1", None, None, &[], None, None, None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn, &plan.id, "Second", "d2", None, None, &[], None, None, None,
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
            &conn, &plan.id, "First", "d1", None, None, &[], None, None, None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn, &plan.id, "Second", "d2", None, None, &[], None, None, None,
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
            &conn, &plan.id, "First", "d1", None, None, &[], None, None, None,
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
            &conn, &plan.id, "First", "d1", None, None, &[], None, None, None,
            None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn, &plan.id, "Second", "d2", None, None, &[], None, None, None,
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
}
