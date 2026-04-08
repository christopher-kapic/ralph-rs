// Plan runner / orchestrator
//
// Implements the plan-level execution loop: loading a plan, iterating through
// steps in sort_key order, executing each via the single-step executor, and
// managing plan-level status transitions.
#![allow(dead_code)]

use std::path::Path;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::executor::{self, StepOutcome, StepResult};
use crate::git;
use crate::plan::{Plan, PlanStatus, Step, StepStatus};
use crate::storage;

// ---------------------------------------------------------------------------
// RunOptions
// ---------------------------------------------------------------------------

/// Options controlling a plan run.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Run all remaining steps (vs. just the next one).
    pub all: bool,
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
        setup_branch(workdir, &effective_plan)?;
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
        if current_step.status == StepStatus::Complete
            || current_step.status == StepStatus::Skipped
        {
            if current_step.status == StepStatus::Skipped {
                result.steps_skipped += 1;
            } else {
                result.steps_succeeded += 1;
            }
            continue;
        }

        // Print progress header.
        let step_num = step_number_in_plan(&all_steps, &current_step);
        eprintln!(
            "[{}/{}] > Step {}: {} ...",
            i + 1,
            total,
            step_num,
            current_step.title
        );

        let started = Instant::now();

        // Execute the step.
        let step_result = executor::execute_step(
            conn,
            &effective_plan,
            &current_step,
            config,
            workdir,
            abort_rx.clone(),
        )
        .await?;

        let elapsed = started.elapsed();
        result.steps_executed += 1;

        // Print result.
        match step_result.outcome {
            StepOutcome::Success => {
                result.steps_succeeded += 1;
                eprintln!(
                    "[{}/{}] > {} ... OK (attempt {}, {:.0}s)",
                    i + 1,
                    total,
                    current_step.title,
                    step_result.attempts_used,
                    elapsed.as_secs_f64()
                );
            }
            StepOutcome::Failed => {
                result.steps_failed += 1;
                eprintln!(
                    "[{}/{}] > {} ... FAILED (after {} attempts, {:.0}s)",
                    i + 1,
                    total,
                    current_step.title,
                    step_result.attempts_used,
                    elapsed.as_secs_f64()
                );
                // Mark plan as failed and stop.
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Failed)?;
                result.final_status = PlanStatus::Failed;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::Aborted => {
                eprintln!(
                    "[{}/{}] > {} ... ABORTED",
                    i + 1,
                    total,
                    current_step.title
                );
                storage::update_plan_status(conn, &effective_plan.id, PlanStatus::Aborted)?;
                result.final_status = PlanStatus::Aborted;
                result.step_results.push(step_result);
                return Ok(result);
            }
            StepOutcome::Timeout => {
                result.steps_failed += 1;
                eprintln!(
                    "[{}/{}] > {} ... TIMEOUT",
                    i + 1,
                    total,
                    current_step.title
                );
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

    Ok(result)
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
) -> Result<PlanRunResult> {
    // Find the resume point.
    let steps = storage::list_steps(conn, &plan.id)?;
    let resume_idx = find_resume_point(&steps)?;

    // Reset the failed/in-progress step to pending.
    let step = &steps[resume_idx];
    if step.status == StepStatus::Failed || step.status == StepStatus::InProgress {
        storage::reset_step(conn, &step.id)?;
    }

    let step_num = resume_idx + 1; // 1-based
    eprintln!(
        "Resuming plan '{}' from step {} '{}'",
        plan.slug, step_num, step.title
    );

    let options = RunOptions {
        all: true,
        from: Some(step_num),
        to: None,
        current_branch: true, // Don't try to create branch on resume
        harness_override: None,
        dry_run: false,
    };

    run_plan(conn, plan, config, workdir, &options, abort_rx).await
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
    eprintln!(
        "Skipped step {} '{}'",
        actual_num, step.title
    );

    Ok(actual_num)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate that the plan is in a runnable status.
fn validate_plan_status(plan: &Plan) -> Result<()> {
    match plan.status {
        PlanStatus::Ready | PlanStatus::InProgress | PlanStatus::Failed => Ok(()),
        PlanStatus::Planning => bail!(
            "Plan '{}' is still in planning status. Run `plan approve {}` first.",
            plan.slug,
            plan.slug
        ),
        PlanStatus::Complete => bail!(
            "Plan '{}' is already complete. Reset steps to re-run.",
            plan.slug
        ),
        PlanStatus::Aborted => bail!(
            "Plan '{}' was aborted. Use `resume` to continue or reset steps.",
            plan.slug
        ),
    }
}

/// Set up the git branch for the plan.
///
/// If the current branch matches the plan's branch, no action is taken.
/// Otherwise, creates and checks out the branch.
fn setup_branch(workdir: &Path, plan: &Plan) -> Result<()> {
    let current = git::get_current_branch(workdir)
        .context("Failed to get current git branch")?;

    if current == plan.branch_name {
        return Ok(());
    }

    // Auto-commit any dirty state before switching branches.
    git::auto_commit_dirty_state(
        workdir,
        &format!("ralph: auto-save before switching to branch '{}'", plan.branch_name),
    )?;

    // Try to create and checkout the branch. If it already exists,
    // just check it out.
    if git::create_and_checkout_branch(workdir, &plan.branch_name).is_err() {
        // Branch might already exist; try a plain checkout.
        checkout_existing_branch(workdir, &plan.branch_name)?;
    }

    Ok(())
}

/// Checkout an existing branch.
fn checkout_existing_branch(workdir: &Path, branch: &str) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["checkout", branch])
        .current_dir(workdir)
        .output()
        .context("Failed to execute git checkout")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to checkout branch '{}': {}", branch, stderr.trim());
    }

    Ok(())
}

/// Select which steps to run based on RunOptions.
fn select_steps(all_steps: &[Step], options: &RunOptions) -> Result<Vec<Step>> {
    let total = all_steps.len();

    // If neither --all nor --from/--to is specified, just return the next
    // actionable step (first pending/failed/in_progress).
    if !options.all && options.from.is_none() && options.to.is_none() {
        let next = all_steps.iter().find(|s| {
            s.status == StepStatus::Pending
                || s.status == StepStatus::Failed
                || s.status == StepStatus::InProgress
        });
        return Ok(next.cloned().into_iter().collect());
    }

    // Determine range (1-based, inclusive).
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
    if let Some(idx) = steps.iter().position(|s| s.status == StepStatus::InProgress) {
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
fn dry_run_report(
    plan: &Plan,
    all_steps: &[Step],
    steps_to_run: &[Step],
) -> Result<PlanRunResult> {
    eprintln!("Dry run for plan '{}':", plan.slug);
    eprintln!("  Branch: {}", plan.branch_name);
    if !plan.deterministic_tests.is_empty() {
        eprintln!("  Tests:  {}", plan.deterministic_tests.join(", "));
    }
    eprintln!();

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
        eprintln!(
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
    fn test_validate_plan_status_aborted_rejected() {
        let mut plan = make_plan(None);
        plan.status = PlanStatus::Aborted;
        let err = validate_plan_status(&plan).unwrap_err();
        assert!(err.to_string().contains("aborted"));
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
            })
            .collect()
    }

    #[test]
    fn test_select_steps_next_pending() {
        let steps = make_steps(3);
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        // Should select only the first pending step.
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s0");
    }

    #[test]
    fn test_select_steps_all() {
        let steps = make_steps(3);
        let options = RunOptions {
            all: true,
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3);
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
    fn test_select_steps_skips_completed_in_default_mode() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s1");
    }

    #[test]
    fn test_select_steps_none_pending() {
        let mut steps = make_steps(2);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Complete;
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        assert!(selected.is_empty());
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
        storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();
        storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None).unwrap();

        let skipped = skip_step(&conn, &plan, Some(2), None).unwrap();
        assert_eq!(skipped, 2);

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].status, StepStatus::Skipped);
    }

    #[test]
    fn test_skip_step_current() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let s1 =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();
        storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None).unwrap();

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
        let s1 =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let result = skip_step(&conn, &plan, Some(1), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_out_of_range() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();

        let result = skip_step(&conn, &plan, Some(5), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_step_allows_failed() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let s1 =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();
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
        assert!(!opts.all);
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
        let p = storage::get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(p.status, PlanStatus::Ready);

        // ready -> in_progress
        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(p.status, PlanStatus::InProgress);

        // in_progress -> complete
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Complete).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(p.status, PlanStatus::Complete);
    }

    #[test]
    fn test_plan_status_failed_transition() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        storage::update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Failed).unwrap();
        let p = storage::get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(p.status, PlanStatus::Failed);
    }

    // -- step status transitions --

    #[test]
    fn test_step_status_transitions() {
        let conn = setup();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step =
            storage::create_step(&conn, &plan.id, "Step", "d", None, None, &[], None).unwrap();

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
        let step =
            storage::create_step(&conn, &plan.id, "Step", "d", None, None, &[], None).unwrap();

        storage::update_step_status(&conn, &step.id, StepStatus::Failed).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Failed);

        storage::update_step_status(&conn, &step.id, StepStatus::Skipped).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Skipped);
    }

    // -- select_steps with mixed statuses --

    #[test]
    fn test_select_steps_picks_failed_as_next() {
        let mut steps = make_steps(3);
        steps[0].status = StepStatus::Complete;
        steps[1].status = StepStatus::Failed;
        let options = RunOptions::default();
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "s1"); // the failed step
    }

    #[test]
    fn test_select_steps_to_only() {
        let steps = make_steps(5);
        let options = RunOptions {
            all: true,
            to: Some(3),
            ..Default::default()
        };
        let selected = select_steps(&steps, &options).unwrap();
        assert_eq!(selected.len(), 3);
    }
}
