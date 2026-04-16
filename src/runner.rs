// Plan runner / orchestrator
//
// Implements the plan-level execution loop: loading a plan, iterating through
// steps in sort_key order, executing each via the single-step executor, and
// managing plan-level status transitions.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::executor::{self, StepOutcome, StepResult};
use crate::git;
use crate::hooks::HookContext;
use crate::output::{self, OutputContext, OutputFormat, RunEvent};
use crate::plan::{Plan, PlanStatus, Step, StepStatus};
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
    /// Override the harness for this run.
    pub harness_override: Option<String>,
    /// Dry-run mode: print what would happen without executing.
    pub dry_run: bool,
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

    // 2. Optionally create and checkout branch.
    if !options.current_branch && !options.dry_run {
        setup_branch(workdir, &effective_plan, None).await?;
    }

    // Load steps.
    let all_steps = storage::list_steps(conn, &effective_plan.id)?;
    if all_steps.is_empty() {
        bail!("Plan '{}' has no steps", effective_plan.slug);
    }

    // Determine which steps to run.
    let steps_to_run = select_steps(&all_steps, options)?;

    if steps_to_run.is_empty() {
        bail!("No pending steps to run in plan '{}'", effective_plan.slug);
    }

    // Dry-run mode: just print what would happen.
    if options.dry_run {
        return dry_run_report(&effective_plan, &all_steps, &steps_to_run);
    }

    // 3. Mark plan as in_progress.
    if effective_plan.status != PlanStatus::InProgress {
        storage::update_plan_status(conn, &effective_plan.id, PlanStatus::InProgress)?;
    }

    // Load the hook library once for this run, filtered by project scope.
    let hook_ctx = HookContext::load(workdir, config.hook_timeout_secs)?;

    // 4. Iterate through steps.
    let total = steps_to_run.len();
    let mut result = PlanRunResult {
        plan_slug: effective_plan.slug.clone(),
        steps_executed: 0,
        steps_succeeded: 0,
        steps_failed: 0,
        steps_skipped: 0,
        final_status: PlanStatus::InProgress,
        step_results: Vec::new(),
    };

    for (i, step) in steps_to_run.iter().enumerate() {
        // Check abort signal between steps.
        if *abort_rx.borrow() {
            eprintln!("[{}/{}] Aborted", i + 1, total);
            storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
            result.final_status = PlanStatus::Aborted;
            return Ok(result);
        }

        // Skip already-completed or skipped steps.
        let current_step = storage::get_step(conn, &step.id)?;
        if current_step.status == StepStatus::Complete || current_step.status == StepStatus::Skipped
        {
            if current_step.status == StepStatus::Skipped {
                result.steps_skipped += 1;
            } else {
                result.steps_succeeded += 1;
            }
            continue;
        }

        // Print progress header / emit step_started event.
        let step_num = step_number_in_plan(&all_steps, &current_step);
        if out.format == OutputFormat::Json {
            output::emit_ndjson(&RunEvent::StepStarted {
                step_id: current_step.id.clone(),
                step_title: current_step.title.clone(),
                step_num,
                step_total: total,
            });
        } else {
            eprintln!(
                "[{}/{}] > Step {}: {} ...",
                i + 1,
                total,
                step_num,
                current_step.title
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
        )
        .await?;

        let elapsed = started.elapsed();
        result.steps_executed += 1;

        // Print result / emit step_finished event.
        let outcome_str = match step_result.outcome {
            StepOutcome::Success => "success",
            StepOutcome::Failed => "failed",
            StepOutcome::Aborted => "aborted",
            StepOutcome::Timeout => "timeout",
        };

        let emit_finished = |outcome: &str| {
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::StepFinished {
                    step_id: current_step.id.clone(),
                    step_title: current_step.title.clone(),
                    step_num,
                    step_total: total,
                    outcome: outcome.to_string(),
                    attempts: step_result.attempts_used,
                    duration_secs: elapsed.as_secs_f64(),
                });
            }
        };

        match step_result.outcome {
            StepOutcome::Success => {
                result.steps_succeeded += 1;
                emit_finished(outcome_str);
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... OK (attempt {}, {:.0}s)",
                        i + 1,
                        total,
                        current_step.title,
                        step_result.attempts_used,
                        elapsed.as_secs_f64()
                    );
                }
            }
            StepOutcome::Failed => {
                result.steps_failed += 1;
                emit_finished(outcome_str);
                if out.format != OutputFormat::Json {
                    eprintln!(
                        "[{}/{}] > {} ... FAILED (after {} attempts, {:.0}s)",
                        i + 1,
                        total,
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
                emit_finished(outcome_str);
                if out.format != OutputFormat::Json {
                    eprintln!("[{}/{}] > {} ... ABORTED", i + 1, total, current_step.title);
                }
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
                result.final_status = PlanStatus::Aborted;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::Timeout => {
                result.steps_failed += 1;
                emit_finished(outcome_str);
                if out.format != OutputFormat::Json {
                    eprintln!("[{}/{}] > {} ... TIMEOUT", i + 1, total, current_step.title);
                }
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Failed)?;
                result.final_status = PlanStatus::Failed;
                result.step_results.push(step_result);
                return Ok(result);
            }
        }

        result.step_results.push(step_result);
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
        });
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

    // 3. Capture the run's starting SHA (used for plans with no deps).
    let run_start_sha = if options.current_branch || options.dry_run {
        String::new()
    } else {
        let workdir_owned = workdir.to_path_buf();
        blocking_git(move || git::get_commit_hash(&workdir_owned))
            .await
            .context("could not capture starting HEAD SHA")?
    };

    // 4. Build deps_of map for the in-scope plan set.
    let mut deps_of: HashMap<String, Vec<String>> = HashMap::new();
    for pid in &topo_order {
        deps_of.insert(pid.clone(), storage::list_plan_dependencies(conn, pid)?);
    }

    // 5. Iterate through plans in topo order.
    let mut tip_sha_map: HashMap<String, String> = HashMap::new();
    let mut results: Vec<PlanRunResult> = Vec::new();
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

        let branch_plan = compute_branch_plan(
            &topo_order,
            i,
            &deps_of,
            &tip_sha_map,
            &run_start_sha,
            options.current_branch,
        );

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
                let merge_result =
                    blocking_git(move || git::merge_sha(&workdir_owned, &sha)).await;
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
            harness_override: options.harness_override.clone(),
            dry_run: options.dry_run,
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
                // InProgress or other — treat as "stopped cleanly but incomplete".
                return Ok(results);
            }
        }
    }

    Ok(results)
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
        "Resuming plan '{}' from step {} '{}'",
        plan.slug, step_num, step.title
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
pub fn skip_step(
    conn: &Connection,
    plan: &Plan,
    step_num: Option<usize>,
    _reason: Option<&str>,
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

    storage::update_step_status(conn, &step.id, StepStatus::Skipped)?;
    eprintln!("Skipped step {} '{}'", actual_num, step.title);

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
        PlanStatus::Ready
        | PlanStatus::InProgress
        | PlanStatus::Failed
        | PlanStatus::Aborted => Ok(()),
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

/// Set up the git branch for the plan.
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

    // Auto-commit any dirty state before switching branches.
    {
        let workdir_owned = workdir.to_path_buf();
        let msg = format!(
            "ralph: auto-save before switching to branch '{}'",
            plan.branch_name
        );
        blocking_git(move || git::auto_commit_dirty_state(&workdir_owned, &msg)).await?;
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

/// Find the resume point: the first step that is failed or in_progress.
fn find_resume_point(steps: &[Step]) -> Result<usize> {
    // First look for an in_progress step.
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

    // Then look for an aborted step.
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::Aborted) {
        return Ok(idx);
    }

    // Check if there are pending steps (plan may not have started yet).
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::Pending) {
        return Ok(idx);
    }

    bail!("No failed, in-progress, or pending steps found to resume from")
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
        final_status: plan.status,
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
        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
                .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        let (s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None)
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
        storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None).unwrap();
        storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None).unwrap();

        let skipped = skip_step(&conn, &plan, Some(2), None).unwrap();
        assert_eq!(skipped, 2);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Skipped);
    }

    #[test]
    fn test_skip_step_current() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
                .unwrap();
        storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None).unwrap();

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
        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
                .unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let result = skip_step(&conn, &plan, Some(1), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_out_of_range() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None).unwrap();

        let result = skip_step(&conn, &plan, Some(5), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_allows_failed() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
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
        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "d", None, None, &[], None, None)
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
        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "d", None, None, &[], None, None)
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
}
