// Step executor
//
// Runs a single step through the full lifecycle:
// resolve harness → build prompt → spawn → wait → test → commit/rollback.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::git;
use crate::harness::{self, HarnessOutput};
use crate::hooks::{self, HookContext};
use crate::io_util;
use crate::plan::{ChangePolicy, Phase, Plan, Step, StepStatus, TerminationReason, TestStatus};
use crate::prompt::{self, PriorStepSummary, PromptWrap, PromptWraps, RetryContext};
use crate::run_lock::process_start_token;
use crate::storage::{self, ChildUpdate};
use crate::test_runner;

/// Per-stream cap for concurrent harness pipe drainers. The parent must drain
/// stdout/stderr *concurrently* with `child.wait()` to avoid deadlocking on a
/// full pipe buffer (see `io_util::drain_bounded` for rationale). 4 MiB is
/// generous for realistic harness output — structured JSON tails are small —
/// while bounding a runaway process.
const HARNESS_OUTPUT_TAIL_BYTES: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// StepResult
// ---------------------------------------------------------------------------

/// Outcome of executing a single step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// Tests passed, changes committed.
    Success,
    /// Tests failed (or harness exited non-zero) after exhausting attempts.
    Failed,
    /// Execution was aborted via signal.
    Aborted,
    /// The harness process exceeded the timeout.
    Timeout,
}

/// Result returned from [`execute_step`].
#[derive(Debug)]
#[allow(dead_code)]
pub struct StepResult {
    pub outcome: StepOutcome,
    pub step_id: String,
    pub attempts_used: i32,
    pub commit_hash: Option<String>,
}

// ---------------------------------------------------------------------------
// Structured JSON output parsing
// ---------------------------------------------------------------------------

/// Structured fields that a harness may emit in JSON output.
#[derive(Debug, Default)]
#[allow(dead_code)]
struct ParsedHarnessOutput {
    cost_usd: Option<f64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    session_id: Option<String>,
}

/// Attempt to extract structured fields from harness stdout.
///
/// Looks for a JSON object containing optional keys:
/// `cost_usd`, `input_tokens`, `output_tokens`, `session_id`.
fn parse_harness_json(stdout: &str) -> ParsedHarnessOutput {
    // Try parsing the entire stdout as JSON first, then fall back to
    // searching for a JSON object on a single line.
    if let Some(parsed) = try_parse_json(stdout) {
        return parsed;
    }

    // Scan lines in reverse (structured output is usually at the end).
    for line in stdout.lines().rev() {
        let trimmed = line.trim();
        if trimmed.starts_with('{')
            && let Some(parsed) = try_parse_json(trimmed)
        {
            return parsed;
        }
    }

    ParsedHarnessOutput::default()
}

fn try_parse_json(text: &str) -> Option<ParsedHarnessOutput> {
    let val: serde_json::Value = serde_json::from_str(text).ok()?;
    let obj = val.as_object()?;

    // Only consider it a match if at least one known key is present.
    let has_known_key = obj.contains_key("cost_usd")
        || obj.contains_key("input_tokens")
        || obj.contains_key("output_tokens")
        || obj.contains_key("session_id");
    if !has_known_key {
        return None;
    }

    Some(ParsedHarnessOutput {
        cost_usd: obj.get("cost_usd").and_then(|v| v.as_f64()),
        input_tokens: obj.get("input_tokens").and_then(|v| v.as_i64()),
        output_tokens: obj.get("output_tokens").and_then(|v| v.as_i64()),
        session_id: obj
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

/// Reason a step execution failed terminally.
#[derive(Debug, Clone, Copy)]
enum FailureReason {
    /// Harness exceeded timeout.
    Timeout,
    /// Execution was aborted via signal.
    Aborted,
    /// Tests failed after exhausting all attempts.
    TestFailed,
    /// Harness produced no changes after exhausting all attempts.
    NoChanges,
    /// Harness exited non-zero (or was killed by a signal) — tests never ran.
    HarnessFailed,
}

impl FailureReason {
    fn to_step_status(self) -> StepStatus {
        match self {
            Self::Aborted => StepStatus::Aborted,
            _ => StepStatus::Failed,
        }
    }

    fn to_outcome(self) -> StepOutcome {
        match self {
            Self::Timeout => StepOutcome::Timeout,
            Self::Aborted => StepOutcome::Aborted,
            _ => StepOutcome::Failed,
        }
    }

    fn hook_label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Aborted => "aborted",
            Self::NoChanges => "no_changes",
            Self::TestFailed => "failed",
            Self::HarnessFailed => "harness_failed",
        }
    }
}

/// Shared references that stay constant for the duration of a step execution.
///
/// `step_num` is the 1-based position of the step within its plan, computed
/// once at the top of [`execute_step`]. `max_attempts` is the step's retry
/// budget (1 + `max_retries`). Both are carried here so
/// [`finalize_failure`] and other phase-writing sites don't need long
/// parameter lists to emit a [`crate::plan::Phase`] update.
struct ExecCtx<'a> {
    conn: &'a Connection,
    plan: &'a Plan,
    step: &'a Step,
    workdir: &'a Path,
    pre_existing_untracked: &'a [String],
    hook_ctx: &'a HookContext,
    step_num: i32,
    max_attempts: i32,
}

/// Write a phase transition to the run_locks row. Thin wrapper over
/// [`storage::update_live_phase`] that plugs in the plan's project and
/// whichever step/attempt bookkeeping the caller wants to update (or coalesce
/// via `None`).
///
/// `child` controls what happens to the `child_pid` / `child_start_token`
/// columns on the row: [`ChildUpdate::Keep`] preserves existing values,
/// [`ChildUpdate::Set`] overwrites with a concrete pid/token (used once per
/// attempt after the harness spawns), and [`ChildUpdate::Clear`] wipes both
/// columns to NULL (used by every post-harness phase so the row stops
/// advertising a dead pid).
#[allow(clippy::too_many_arguments)]
fn write_phase(
    conn: &Connection,
    plan: &Plan,
    step_id: &str,
    step_num: i32,
    attempt: i32,
    max_attempts: i32,
    execution_log_id: Option<i64>,
    phase: Phase,
    current_command: Option<&str>,
    child: ChildUpdate<'_>,
) -> Result<()> {
    storage::update_live_phase(
        conn,
        &plan.project,
        phase,
        Some(step_id),
        Some(step_num),
        Some(attempt),
        Some(max_attempts),
        execution_log_id,
        current_command,
        child,
    )
}

/// Optional harness output fields attached to a terminal failure.
struct FailureOutput<'a> {
    diff: Option<&'a str>,
    test_results: &'a [String],
    stdout: &'a str,
    stderr: &'a str,
    parsed: &'a ParsedHarnessOutput,
    has_changes: bool,
}

/// Handle a terminal step failure: rollback changes, update the execution log,
/// set step status, run post-step hook, and return the appropriate [`StepResult`].
///
/// `termination_reason` and `test_status` are written to the execution log so
/// the terminal outcome is explicit. Callers choose these values because they
/// have more context than [`FailureReason`] alone (e.g. whether the test phase
/// ran at all, was aborted mid-flight, or was never configured).
#[allow(clippy::too_many_arguments)]
async fn finalize_failure(
    ctx: &ExecCtx<'_>,
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    reason: FailureReason,
    output: Option<&FailureOutput<'_>>,
    termination_reason: TerminationReason,
    test_status: TestStatus,
) -> Result<StepResult> {
    // Rollback any uncommitted changes, preserving pre-existing untracked files.
    let rolled_back = if git::has_uncommitted_changes(ctx.workdir)? {
        // Record the rollback phase before invoking git so an external
        // observer sees *why* the runner is touching the tree.
        write_phase(
            ctx.conn,
            ctx.plan,
            &ctx.step.id,
            ctx.step_num,
            attempt,
            ctx.max_attempts,
            Some(exec_log_id),
            Phase::Rollback,
            None,
            ChildUpdate::Clear,
        )?;
        git::rollback_except(ctx.workdir, ctx.pre_existing_untracked)?;
        true
    } else {
        false
    };

    // Update execution log — use harness output fields when available.
    if let Some(o) = output {
        storage::update_execution_log(
            ctx.conn,
            exec_log_id,
            Some(duration_secs),
            o.diff,
            o.test_results,
            o.has_changes,
            false,
            None,
            Some(o.stdout),
            Some(o.stderr),
            o.parsed.cost_usd,
            o.parsed.input_tokens,
            o.parsed.output_tokens,
            o.parsed.session_id.as_deref(),
            Some(termination_reason),
            Some(test_status),
        )?;
    } else {
        storage::update_execution_log(
            ctx.conn,
            exec_log_id,
            Some(duration_secs),
            None,
            &[],
            rolled_back,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(termination_reason),
            Some(test_status),
        )?;
    }

    storage::update_step_status(ctx.conn, &ctx.step.id, reason.to_step_status())?;
    write_phase(
        ctx.conn,
        ctx.plan,
        &ctx.step.id,
        ctx.step_num,
        attempt,
        ctx.max_attempts,
        Some(exec_log_id),
        Phase::PostStepHook,
        None,
        ChildUpdate::Clear,
    )?;
    hooks::run_post_step(
        ctx.conn,
        ctx.hook_ctx,
        ctx.plan,
        ctx.step,
        attempt,
        reason.hook_label(),
        ctx.workdir,
    )
    .await?;

    Ok(StepResult {
        outcome: reason.to_outcome(),
        step_id: ctx.step.id.clone(),
        attempts_used: attempt,
        commit_hash: None,
    })
}

// ---------------------------------------------------------------------------
// Core executor
// ---------------------------------------------------------------------------

/// Execute a single step through the full lifecycle.
///
/// The flow:
/// 1. Resolve harness and agent
/// 2. Build prompt (with retry context if retrying)
/// 3. Spawn harness subprocess
/// 4. Wait for completion (racing against abort signal and timeout)
/// 5. Check for changes via git
/// 6. Run deterministic tests if changes exist
/// 7. If tests pass → git commit with step metadata, log success
/// 8. If tests fail → git rollback, log failure
/// 9. Return [`StepResult`]
pub async fn execute_step(
    conn: &Connection,
    plan: &Plan,
    step: &Step,
    config: &Config,
    workdir: &Path,
    hook_ctx: &HookContext,
    abort_rx: watch::Receiver<bool>,
) -> Result<StepResult> {
    let max_retries = step
        .max_retries
        .unwrap_or(config.max_retries_per_step as i32);
    let max_attempts = max_retries + 1; // first attempt + retries

    // Refuse to run a step that has already exhausted its retry budget.
    // Without this guard, the retry loop would skip its body entirely and
    // silently return Failed with zero new work — the user wouldn't know
    // why nothing happened. Require an explicit reset or wider budget.
    if step.attempts >= max_attempts {
        bail!(
            "Step '{}' has already used all {} attempts — run \
             `ralph step reset --step-id {}` to retry from scratch, \
             or raise --max-retries",
            step.title,
            max_attempts,
            step.id,
        );
    }

    let timeout = config.timeout_secs.map(Duration::from_secs);

    // Resolve harness once (doesn't change between retries).
    let (harness_name, harness_config) = harness::resolve_harness(step, plan, config)?;

    // Resolve agent file path.
    let agent_file_path: Option<PathBuf> = resolve_agent_file(step, plan);

    // Collect prior step summaries for prompt context.
    let prior_steps = build_prior_step_summaries(conn, plan, step)?;

    // 1-based position of `step` within its plan. Computed once up front so
    // every `write_phase` call can pass it without reshuffling the plan's
    // step list each time. Mirrors the index walk in
    // `build_prior_step_summaries`.
    let step_num = resolve_step_num(conn, plan, step)?;

    // Snapshot pre-existing untracked files so we don't accidentally commit them.
    let pre_existing_untracked = git::get_untracked_files(workdir)?;

    // Shared context for failure handling.
    let ctx = ExecCtx {
        conn,
        plan,
        step,
        workdir,
        pre_existing_untracked: &pre_existing_untracked,
        hook_ctx,
        step_num,
        max_attempts,
    };

    // Previous attempt context for retries.
    let mut prev_diff: Option<String> = None;
    let mut prev_test_output: Option<String> = None;
    let mut prev_files_modified: Vec<String> = Vec::new();

    let mut attempt = step.attempts;

    while attempt < max_attempts {
        attempt += 1;

        // Check abort before starting. Persist the bumped attempt count and
        // drop an execution-log row so the DB reflects the same attempt number
        // that StepResult reports and the abort has a visible audit trail.
        if *abort_rx.borrow() {
            set_step_attempts(conn, &step.id, attempt)?;
            let exec_log = storage::create_execution_log(conn, &step.id, attempt, None, None)?;
            storage::update_execution_log(
                conn,
                exec_log.id,
                Some(0.0),
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
                Some(TestStatus::NotRun),
            )?;
            storage::update_step_status(conn, &step.id, StepStatus::Aborted)?;
            return Ok(StepResult {
                outcome: StepOutcome::Aborted,
                step_id: step.id.clone(),
                attempts_used: attempt,
                commit_hash: None,
            });
        }

        // Mark step as in-progress and bump attempts.
        storage::update_step_status(conn, &step.id, StepStatus::InProgress)?;
        set_step_attempts(conn, &step.id, attempt)?;

        // Build retry context if this is not the first attempt.
        let retry_context = if attempt > 1 {
            Some(RetryContext {
                attempt,
                max_attempts,
                previous_diff: prev_diff.clone(),
                previous_test_output: prev_test_output.clone(),
                files_modified: prev_files_modified.clone(),
            })
        } else {
            None
        };

        // Resolve the assigned agent name (used for the pointer section in
        // prompts when the harness can't take an agent file directly).
        let agent_name = step.agent.as_deref().or(plan.agent.as_deref());

        // Collect prompt prefix/suffix layers. Project-scope settings are
        // looked up by project path; a missing row is treated as "no wrap".
        let project_settings = storage::get_project_settings(conn, &plan.project)?;
        let wraps = PromptWraps {
            global: PromptWrap::from_opts(
                config.prompt_prefix.as_ref(),
                config.prompt_suffix.as_ref(),
            ),
            project: PromptWrap::from_opts(
                project_settings.prompt_prefix.as_ref(),
                project_settings.prompt_suffix.as_ref(),
            ),
            plan: PromptWrap::from_opts(plan.prompt_prefix.as_ref(), plan.prompt_suffix.as_ref()),
        };

        // Build prompt.
        let prompt_text = prompt::build_step_prompt(
            plan,
            step,
            &prior_steps,
            agent_name,
            retry_context.as_ref(),
            harness_config.supports_agent_file,
            &wraps,
        );

        // Create execution log entry.
        let exec_log =
            storage::create_execution_log(conn, &step.id, attempt, Some(&prompt_text), None)?;
        let started_at = std::time::Instant::now();

        // Record the step identity + attempt bookkeeping on the run_locks
        // row. Subsequent `write_phase` calls in this attempt can pass
        // `None` for step_id/step_num/attempt/max_attempts and let COALESCE
        // preserve what we set here. `Clear` the child columns in case a
        // previous attempt left them populated — a new attempt means any
        // prior child is long dead.
        write_phase(
            conn,
            plan,
            &step.id,
            step_num,
            attempt,
            max_attempts,
            Some(exec_log.id),
            Phase::PreStepHook,
            None,
            ChildUpdate::Clear,
        )?;

        // Run pre-step hook.
        if let Err(e) = hooks::run_pre_step(conn, hook_ctx, plan, step, attempt, workdir).await {
            eprintln!("Pre-step hook failed: {e}");
            // Treat as a failed attempt — skip harness execution.
            let test_result_strings = vec![format!("pre-step hook failed: {e}")];
            storage::update_execution_log(
                conn,
                exec_log.id,
                Some(started_at.elapsed().as_secs_f64()),
                None,
                &test_result_strings,
                false,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(TerminationReason::HookFailed),
                Some(TestStatus::NotRun),
            )?;
            if attempt >= max_attempts {
                storage::update_step_status(conn, &step.id, StepStatus::Failed)?;
                write_phase(
                    conn,
                    plan,
                    &step.id,
                    step_num,
                    attempt,
                    max_attempts,
                    Some(exec_log.id),
                    Phase::PostStepHook,
                    None,
                    ChildUpdate::Clear,
                )?;
                hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir)
                    .await?;
                return Ok(StepResult {
                    outcome: StepOutcome::Failed,
                    step_id: step.id.clone(),
                    attempts_used: attempt,
                    commit_hash: None,
                });
            }
            prev_test_output = Some(format!("pre-step hook failed: {e}"));
            write_phase(
                conn,
                plan,
                &step.id,
                step_num,
                attempt,
                max_attempts,
                Some(exec_log.id),
                Phase::PostStepHook,
                None,
                ChildUpdate::Clear,
            )?;
            hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir).await?;
            continue;
        }

        // Build harness args and env. `step.model` (if set) overrides the
        // harness's config-level `default_model`; None on both sides means
        // the harness is invoked without any model flag.
        let args = harness::build_harness_args(
            harness_name,
            harness_config,
            &prompt_text,
            agent_file_path.as_deref(),
            step.model.as_deref(),
        );
        let env_vars = harness::build_harness_env(harness_config, agent_file_path.as_deref());

        // Announce the harness phase with the harness name as the current
        // command so external observers (ralph status, TUI) can show what's
        // running before we have a pid to attach. `Keep` here because the
        // next write (post-spawn) will Set the concrete pid — nothing to do
        // until then.
        write_phase(
            conn,
            plan,
            &step.id,
            step_num,
            attempt,
            max_attempts,
            Some(exec_log.id),
            Phase::Harness,
            Some(harness_name),
            ChildUpdate::Keep,
        )?;

        // Spawn harness subprocess.
        let child = harness::spawn_harness(harness_config, &args, &env_vars, workdir).await?;

        // As soon as we have a pid, record it on the run_locks row along
        // with a matching start token so the killpg path can verify it's
        // talking to the same child we spawned. Start token may be None on
        // unsupported platforms — that's explicitly fine.
        let child_pid_i64 = child.id().map(|id| id as i64);
        let child_token = child_pid_i64.and_then(process_start_token);
        write_phase(
            conn,
            plan,
            &step.id,
            step_num,
            attempt,
            max_attempts,
            Some(exec_log.id),
            Phase::Harness,
            Some(harness_name),
            match child_pid_i64 {
                Some(pid) => ChildUpdate::Set {
                    pid,
                    start_token: child_token.as_deref(),
                },
                None => ChildUpdate::Keep,
            },
        )?;

        // Wait with timeout and abort racing.
        let wait_result = wait_with_timeout_and_abort(child, timeout, abort_rx.clone()).await;
        let duration_secs = started_at.elapsed().as_secs_f64();

        match wait_result {
            WaitResult::Completed(output) => {
                let output = output.context("Harness process failed")?;
                let parsed = parse_harness_json(&output.stdout);

                // Check for changes.
                let has_changes = git::has_uncommitted_changes(workdir)?;
                let diff = if has_changes {
                    Some(git::get_diff(workdir)?)
                } else {
                    None
                };
                let changed_files = if has_changes {
                    git::get_all_changed_files(workdir)?
                } else {
                    Vec::new()
                };

                // Harness exited non-zero (or was killed by a signal). Do not
                // run tests — the harness didn't finish its work, so a passing
                // test run is meaningless cover (and under `change_policy =
                // optional` a failing non-test run was previously being
                // marked Success). Roll back any partial diff and either
                // retry or finalize as HarnessFailed.
                if !output.success {
                    let exit_msg = match output.exit_code {
                        Some(c) => format!("harness exited with code {c}"),
                        None => "harness terminated by signal".to_string(),
                    };
                    let test_results = vec![exit_msg];

                    if attempt >= max_attempts {
                        let fail_output = FailureOutput {
                            diff: diff.as_deref(),
                            test_results: &test_results,
                            stdout: &output.stdout,
                            stderr: &output.stderr,
                            parsed: &parsed,
                            has_changes,
                        };
                        return finalize_failure(
                            &ctx,
                            exec_log.id,
                            duration_secs,
                            attempt,
                            FailureReason::HarnessFailed,
                            Some(&fail_output),
                            TerminationReason::HarnessFailed,
                            TestStatus::NotRun,
                        )
                        .await;
                    }

                    // Retry path: roll back partial changes, log the attempt
                    // with HarnessFailed + NotRun, then loop back for the
                    // next attempt. The retry log row carries full
                    // observability (diff, stdout, stderr) rather than a
                    // null-everything placeholder.
                    if has_changes {
                        write_phase(
                            conn,
                            plan,
                            &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            Some(exec_log.id),
                            Phase::Rollback,
                            None,
                            ChildUpdate::Clear,
                        )?;
                        git::rollback_except(workdir, &pre_existing_untracked)?;
                    }
                    storage::update_execution_log(
                        conn,
                        exec_log.id,
                        Some(duration_secs),
                        diff.as_deref(),
                        &test_results,
                        has_changes, // rolled_back iff there were changes
                        false,
                        None,
                        Some(&output.stdout),
                        Some(&output.stderr),
                        parsed.cost_usd,
                        parsed.input_tokens,
                        parsed.output_tokens,
                        parsed.session_id.as_deref(),
                        Some(TerminationReason::HarnessFailed),
                        Some(TestStatus::NotRun),
                    )?;
                    prev_diff = diff;
                    prev_test_output = Some(test_results.join("\n"));
                    prev_files_modified = changed_files;
                    continue;
                }

                // Decide whether to run the test phase.
                //
                // With `change_policy = Required`, tests only run when the
                // harness actually produced changes (the existing behavior).
                // With `change_policy = Optional`, tests still run on a clean
                // no-diff exit — a review step may configure `cargo test` to
                // confirm the tree's invariants even when nothing changed.
                let tests_configured = !plan.deterministic_tests.is_empty();
                let policy_allows_no_change_success =
                    step.change_policy == ChangePolicy::Optional;
                let should_run_tests =
                    (has_changes || policy_allows_no_change_success) && tests_configured;

                let (test_passed, test_result_strings, test_aborted) = if should_run_tests {
                        // Pre-test hook. Harness phase is over — clear the
                        // child columns so `ralph status` stops advertising
                        // the dead harness pid.
                        write_phase(
                            conn,
                            plan,
                            &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            Some(exec_log.id),
                            Phase::PreTestHook,
                            None,
                            ChildUpdate::Clear,
                        )?;
                        if let Err(e) =
                            hooks::run_pre_test(conn, hook_ctx, plan, step, attempt, workdir).await
                        {
                            eprintln!("Pre-test hook failed: {e}");
                        }

                        // Aggregate tests phase. Per-command updates would
                        // require plumbing callbacks into run_tests.
                        write_phase(
                            conn,
                            plan,
                            &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            Some(exec_log.id),
                            Phase::Tests,
                            None,
                            ChildUpdate::Clear,
                        )?;
                        let test_results = test_runner::run_tests(
                            &plan.deterministic_tests,
                            workdir,
                            abort_rx.clone(),
                        )
                        .await;
                        let strings: Vec<String> = test_results
                            .results
                            .iter()
                            .map(|r| {
                                format!("{}: {}", r.command, if r.passed { "pass" } else { "FAIL" })
                            })
                            .collect();

                        // Post-test hook.
                        write_phase(
                            conn,
                            plan,
                            &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            Some(exec_log.id),
                            Phase::PostTestHook,
                            None,
                            ChildUpdate::Clear,
                        )?;
                        hooks::run_post_test(
                            conn,
                            hook_ctx,
                            plan,
                            step,
                            attempt,
                            test_results.all_passed,
                            workdir,
                        )
                        .await?;

                        (test_results.all_passed, strings, test_results.aborted)
                    } else if has_changes {
                        // Changes produced, no tests configured: treat as passing.
                        (true, Vec::new(), false)
                    } else if policy_allows_no_change_success {
                        // Optional policy + no changes + no tests configured:
                        // the step is done. The sentinel string below is
                        // surfaced by `ralph log` so a reader doesn't see a
                        // blank-looking successful row and wonder what
                        // happened.
                        (
                            true,
                            vec!["no changes (change_policy=optional)".to_string()],
                            false,
                        )
                    } else {
                        // Required policy, no changes: harness produced nothing useful.
                        (false, vec!["no changes detected".to_string()], false)
                    };

                // If Ctrl+C landed mid-test, the test runner will have killed
                // its child; surface this as Aborted rather than a retry-worthy
                // test failure. Capture partial test_results so the log row
                // reflects what actually ran before the abort landed.
                if test_aborted {
                    if has_changes {
                        write_phase(
                            conn,
                            plan,
                            &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            Some(exec_log.id),
                            Phase::Rollback,
                            None,
                            ChildUpdate::Clear,
                        )?;
                        git::rollback_except(workdir, &pre_existing_untracked)?;
                    }
                    let fail_output = FailureOutput {
                        diff: diff.as_deref(),
                        test_results: &test_result_strings,
                        stdout: &output.stdout,
                        stderr: &output.stderr,
                        parsed: &parsed,
                        has_changes,
                    };
                    return finalize_failure(
                        &ctx,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        FailureReason::Aborted,
                        Some(&fail_output),
                        TerminationReason::UserInterrupted,
                        TestStatus::Aborted,
                    )
                    .await;
                }

                if test_passed && !has_changes {
                    // Optional-policy success path: tests either ran and
                    // passed or weren't configured, and the harness produced
                    // no diff. Record the attempt as a success with no commit.
                    //
                    // `test_status` distinguishes the sub-cases:
                    //  - tests ran successfully → Passed
                    //  - no tests configured   → NotConfigured
                    let success_test_status = if tests_configured {
                        TestStatus::Passed
                    } else {
                        TestStatus::NotConfigured
                    };

                    storage::update_execution_log(
                        conn,
                        exec_log.id,
                        Some(duration_secs),
                        None, // no diff to record
                        &test_result_strings,
                        false, // not rolled back (nothing to rollback)
                        false, // not committed
                        None,  // no commit hash
                        Some(&output.stdout),
                        Some(&output.stderr),
                        parsed.cost_usd,
                        parsed.input_tokens,
                        parsed.output_tokens,
                        parsed.session_id.as_deref(),
                        Some(TerminationReason::Success),
                        Some(success_test_status),
                    )?;

                    storage::update_step_status(conn, &step.id, StepStatus::Complete)?;

                    write_phase(
                        conn,
                        plan,
                        &step.id,
                        step_num,
                        attempt,
                        max_attempts,
                        Some(exec_log.id),
                        Phase::PostStepHook,
                        None,
                        ChildUpdate::Clear,
                    )?;
                    hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "complete", workdir)
                        .await?;

                    return Ok(StepResult {
                        outcome: StepOutcome::Success,
                        step_id: step.id.clone(),
                        attempts_used: attempt,
                        commit_hash: None,
                    });
                }

                if test_passed && has_changes {
                    // Stage changes, excluding pre-existing untracked files.
                    let commit_msg = format!(
                        "ralph: {} [step:{}, plan:{}, attempt:{}]",
                        step.title, step.id, plan.slug, attempt,
                    );
                    write_phase(
                        conn,
                        plan,
                        &step.id,
                        step_num,
                        attempt,
                        max_attempts,
                        Some(exec_log.id),
                        Phase::Commit,
                        None,
                        ChildUpdate::Clear,
                    )?;
                    git::stage_except(workdir, &pre_existing_untracked)?;
                    git::commit_staged(workdir, &commit_msg)?;
                    let commit_hash = git::get_commit_hash(workdir)?;

                    // When no deterministic tests are configured, we skip the
                    // test phase entirely — record NotConfigured so an observer
                    // can tell a passing run from a skipped-tests run.
                    let success_test_status = if plan.deterministic_tests.is_empty() {
                        TestStatus::NotConfigured
                    } else {
                        TestStatus::Passed
                    };

                    // Update execution log.
                    storage::update_execution_log(
                        conn,
                        exec_log.id,
                        Some(duration_secs),
                        diff.as_deref(),
                        &test_result_strings,
                        false, // not rolled back
                        true,  // committed
                        Some(&commit_hash),
                        Some(&output.stdout),
                        Some(&output.stderr),
                        parsed.cost_usd,
                        parsed.input_tokens,
                        parsed.output_tokens,
                        parsed.session_id.as_deref(),
                        Some(TerminationReason::Success),
                        Some(success_test_status),
                    )?;

                    // Mark step as complete.
                    storage::update_step_status(conn, &step.id, StepStatus::Complete)?;

                    write_phase(
                        conn,
                        plan,
                        &step.id,
                        step_num,
                        attempt,
                        max_attempts,
                        Some(exec_log.id),
                        Phase::PostStepHook,
                        None,
                        ChildUpdate::Clear,
                    )?;
                    hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "complete", workdir)
                        .await?;

                    return Ok(StepResult {
                        outcome: StepOutcome::Success,
                        step_id: step.id.clone(),
                        attempts_used: attempt,
                        commit_hash: Some(commit_hash),
                    });
                }

                // Terminal failure — exhausted all attempts.
                if attempt >= max_attempts {
                    let fail_output = FailureOutput {
                        diff: diff.as_deref(),
                        test_results: &test_result_strings,
                        stdout: &output.stdout,
                        stderr: &output.stderr,
                        parsed: &parsed,
                        has_changes,
                    };
                    // Mapping to failure classification:
                    //  - has_changes     -> tests ran and failed
                    //  - Required + none -> NoChanges (unchanged behavior)
                    //  - Optional + none -> the only way to reach here with
                    //    Optional policy is a failing test run, so we classify
                    //    as TestFailed (tests did run; they just failed).
                    let (reason, term_reason, test_st) = if has_changes {
                        (
                            FailureReason::TestFailed,
                            TerminationReason::TestFailed,
                            TestStatus::Failed,
                        )
                    } else if step.change_policy == ChangePolicy::Required {
                        (
                            FailureReason::NoChanges,
                            TerminationReason::NoChanges,
                            TestStatus::NotRun,
                        )
                    } else {
                        (
                            FailureReason::TestFailed,
                            TerminationReason::TestFailed,
                            TestStatus::Failed,
                        )
                    };
                    return finalize_failure(
                        &ctx,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        reason,
                        Some(&fail_output),
                        term_reason,
                        test_st,
                    )
                    .await;
                }

                // Retry: rollback, log failure, stash context for next attempt.
                if has_changes {
                    write_phase(
                        conn,
                        plan,
                        &step.id,
                        step_num,
                        attempt,
                        max_attempts,
                        Some(exec_log.id),
                        Phase::Rollback,
                        None,
                        ChildUpdate::Clear,
                    )?;
                    git::rollback_except(workdir, &pre_existing_untracked)?;
                }
                let test_output_summary = test_result_strings.join("\n");
                // This row describes *this* attempt's termination even though
                // the step will retry — record why this attempt failed. Same
                // mapping as the terminal case: under Optional policy the only
                // non-success path with no changes is a failed test run.
                let (retry_term, retry_test_status) = if has_changes {
                    (TerminationReason::TestFailed, TestStatus::Failed)
                } else if step.change_policy == ChangePolicy::Required {
                    (TerminationReason::NoChanges, TestStatus::NotRun)
                } else {
                    (TerminationReason::TestFailed, TestStatus::Failed)
                };
                storage::update_execution_log(
                    conn,
                    exec_log.id,
                    Some(duration_secs),
                    diff.as_deref(),
                    &test_result_strings,
                    has_changes, // rolled_back only if there were changes
                    false,       // not committed
                    None,
                    Some(&output.stdout),
                    Some(&output.stderr),
                    parsed.cost_usd,
                    parsed.input_tokens,
                    parsed.output_tokens,
                    parsed.session_id.as_deref(),
                    Some(retry_term),
                    Some(retry_test_status),
                )?;
                prev_diff = diff;
                prev_test_output = Some(test_output_summary);
                prev_files_modified = changed_files;
            }

            WaitResult::Timeout { stdout, stderr } => {
                // Timeouts count as a real attempt — consistent with test
                // failures and hook failures, and avoids reusing an attempt
                // number whose execution_logs row already exists (which
                // would trip the UNIQUE(step_id, attempt) constraint on
                // the next run).
                //
                // Capture any partial changes + parsed JSON so the log
                // row retains diagnostic context (stdout/stderr/diff/
                // cost) rather than being a blank "timeout" marker. We
                // never reached the test phase, so test_results stays
                // empty and test_status is NotRun — the termination_reason
                // of Timeout is what conveys the outcome.
                let parsed = parse_harness_json(&stdout);
                let has_changes = git::has_uncommitted_changes(workdir)?;
                let diff = if has_changes {
                    Some(git::get_diff(workdir)?)
                } else {
                    None
                };
                let timeout_results: Vec<String> = Vec::new();
                let fail_output = FailureOutput {
                    diff: diff.as_deref(),
                    test_results: &timeout_results,
                    stdout: &stdout,
                    stderr: &stderr,
                    parsed: &parsed,
                    has_changes,
                };
                return finalize_failure(
                    &ctx,
                    exec_log.id,
                    duration_secs,
                    attempt,
                    FailureReason::Timeout,
                    Some(&fail_output),
                    TerminationReason::Timeout,
                    TestStatus::NotRun,
                )
                .await;
            }

            WaitResult::Aborted => {
                // Harness was killed before we ever reached the test phase,
                // so test_status is NotRun (the test runner itself was never
                // invoked on this attempt).
                return finalize_failure(
                    &ctx,
                    exec_log.id,
                    duration_secs,
                    attempt,
                    FailureReason::Aborted,
                    None,
                    TerminationReason::UserInterrupted,
                    TestStatus::NotRun,
                )
                .await;
            }
        }
    }

    // Unreachable: the budget guard above rejects steps that enter with
    // `attempts >= max_attempts`, so the while-loop always runs at least
    // once, and every terminal state returns from inside the loop.
    unreachable!("retry loop should always return via one of its inner branches")
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

/// Outcome of waiting for a harness process.
enum WaitResult {
    /// Process completed (may have succeeded or failed).
    Completed(Result<HarnessOutput>),
    /// Process exceeded timeout and was killed. The partial stdout/stderr
    /// captured before the kill are surfaced so the execution log retains
    /// diagnostic context for the failed attempt.
    Timeout { stdout: String, stderr: String },
    /// Abort signal received.
    Aborted,
}

/// Wait for a child process, racing against an optional timeout and an abort signal.
///
/// - When timeout is `None`: the process runs indefinitely (only the abort
///   signal can stop it early).
/// - When timeout is `Some(d)`: the process is killed after `d` if it
///   hasn't completed.
/// - On **abort**: SIGTERM is sent, followed by a 5-second grace period,
///   then SIGKILL if still running.
async fn wait_with_timeout_and_abort(
    mut child: tokio::process::Child,
    timeout: Option<Duration>,
    mut abort_rx: watch::Receiver<bool>,
) -> WaitResult {
    // Take stdout/stderr handles before entering select! so we can still
    // access `child` mutably for kill/wait. Spawn concurrent drain tasks
    // *immediately*: a child that writes more than the pipe buffer
    // (~64 KiB) would otherwise block on write(2) while we block on wait(),
    // deadlocking. Draining concurrently keeps the pipe flowing.
    let stdout_task =
        io_util::drain_bounded(child.stdout.take(), HARNESS_OUTPUT_TAIL_BYTES);
    let stderr_task =
        io_util::drain_bounded(child.stderr.take(), HARNESS_OUTPUT_TAIL_BYTES);

    match timeout {
        Some(dur) => {
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(exit_status) => {
                            // Child has exited; pipes will EOF and the
                            // drain tasks will finish on their own.
                            let stdout = io_util::join_drain_string(stdout_task).await;
                            let stderr = io_util::join_drain_string(stderr_task).await;
                            WaitResult::Completed(Ok(HarnessOutput {
                                stdout,
                                stderr,
                                exit_code: exit_status.code(),
                                success: exit_status.success(),
                            }))
                        }
                        Err(e) => {
                            // Still try to collect whatever the drainers
                            // have captured so we don't lose diagnostics.
                            let _ = io_util::join_drain_string(stdout_task).await;
                            let _ = io_util::join_drain_string(stderr_task).await;
                            WaitResult::Completed(Err(e.into()))
                        }
                    }
                }
                _ = tokio::time::sleep(dur) => {
                    // Fan the kill to the whole process group so any
                    // grandchildren the harness spawned are torn down along
                    // with the leader — matches graceful_shutdown's policy.
                    #[cfg(unix)]
                    {
                        if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                            signal_process_group(pid, libc::SIGKILL);
                        }
                    }
                    let _ = child.kill().await;
                    // Reap the child so it doesn't linger as a zombie on Unix.
                    // After the wait() returns, the pipes are definitively
                    // closed and the drain tasks will exit promptly.
                    let _ = child.wait().await;
                    let stdout = io_util::join_drain_string(stdout_task).await;
                    let stderr = io_util::join_drain_string(stderr_task).await;
                    WaitResult::Timeout { stdout, stderr }
                }
                _ = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
                    // Abort the drain tasks rather than awaiting them.
                    // A harness that spawned a grandchild inheriting
                    // stdout/stderr will leave those pipes open past
                    // SIGKILL (the grandchild is reparented to init),
                    // and the drain loop would block on `read` until it
                    // exits. WaitResult::Aborted doesn't carry output,
                    // so we have nothing to lose by dropping the tasks.
                    stdout_task.abort();
                    stderr_task.abort();
                    WaitResult::Aborted
                }
            }
        }
        None => {
            // No timeout — wait for completion or abort.
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(exit_status) => {
                            let stdout = io_util::join_drain_string(stdout_task).await;
                            let stderr = io_util::join_drain_string(stderr_task).await;
                            WaitResult::Completed(Ok(HarnessOutput {
                                stdout,
                                stderr,
                                exit_code: exit_status.code(),
                                success: exit_status.success(),
                            }))
                        }
                        Err(e) => {
                            let _ = io_util::join_drain_string(stdout_task).await;
                            let _ = io_util::join_drain_string(stderr_task).await;
                            WaitResult::Completed(Err(e.into()))
                        }
                    }
                }
                _ = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
                    // See matching comment in the timeout arm above.
                    stdout_task.abort();
                    stderr_task.abort();
                    WaitResult::Aborted
                }
            }
        }
    }
}

/// Block until the abort watch channel signals `true`.
async fn wait_for_abort(rx: &mut watch::Receiver<bool>) {
    // If already aborted, return immediately.
    if *rx.borrow() {
        return;
    }
    // Wait for a change that sets abort to true.
    loop {
        if rx.changed().await.is_err() {
            // Sender dropped — treat as "never abort" by pending forever.
            std::future::pending::<()>().await;
            return;
        }
        if *rx.borrow() {
            return;
        }
    }
}

/// Send a signal to the process group led by `pid`.
///
/// `libc::kill` treats a negative pid as "send to process group <-pid>"; this
/// is how we fan signals out to grandchildren. The child must have been moved
/// into its own group (as leader, so `pid == pgid`) — today that happens in
/// `harness::spawn_harness` and in `test_runner::run_single_test`.
///
/// Best-effort: a stale or already-dead pgid returns ESRCH and we happily
/// proceed. Callers should not treat a missing group as an error.
#[cfg(unix)]
pub(crate) fn signal_process_group(pid: i32, signo: i32) {
    // SAFETY: `libc::kill` is a plain syscall wrapper with no invariants
    // beyond the pid/signal arguments being valid `i32`s.
    unsafe {
        libc::kill(-pid, signo);
    }
}

/// Send SIGTERM to the child's process group, wait up to 5 seconds, then
/// SIGKILL the whole group if anything is still alive. On non-unix, falls
/// back to `child.kill().await` (SIGKILL-equivalent, leader only — Windows
/// does not have a direct analogue to unix process groups here, and
/// `TerminateProcess` is the best we can do).
async fn graceful_shutdown(child: &mut tokio::process::Child) {
    // Capture the pid once. If the child has already been reaped, `id()`
    // returns None and we have nothing to signal — skip straight to the
    // final `wait()` so the caller gets a consistent reap.
    let pid_i32: Option<i32> = child.id().and_then(|id| i32::try_from(id).ok());

    #[cfg(unix)]
    {
        if let Some(pid) = pid_i32 {
            signal_process_group(pid, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        // Windows path: SIGTERM-equivalent does not exist for child groups,
        // so go straight to TerminateProcess on the leader.
        let _ = child.kill().await;
        let _ = child.wait().await;
        return;
    }

    // Wait up to 5 seconds for graceful exit.
    let grace = tokio::time::sleep(Duration::from_secs(5));
    tokio::select! {
        _ = child.wait() => {
            // Exited within grace period. `child.wait` returning tells us the
            // *leader* exited — grandchildren that trapped or ignored SIGTERM
            // may still be alive. The belt-and-braces SIGKILL below fans out
            // to the whole group so no descendant survives past this fn.
        }
        _ = grace => {
            // Grace period expired — force-kill the whole group, then the
            // leader via tokio (for the OS handle bookkeeping), and finally
            // reap so the child doesn't linger as a zombie. Same
            // belt-and-braces is run unconditionally below so this arm's
            // explicit kill is redundant but harmless.
            #[cfg(unix)]
            {
                if let Some(pid) = pid_i32 {
                    signal_process_group(pid, libc::SIGKILL);
                }
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    // Belt-and-braces: whether the leader exited cleanly within the grace
    // period or we force-killed it after the timeout, fan SIGKILL to the
    // whole process group so any descendant that trapped SIGTERM still
    // dies. ESRCH on an empty group is harmless — libc::kill(-pgid, SIGKILL)
    // where no process remains in that group is a no-op from our
    // perspective. We don't inspect the return value.
    #[cfg(unix)]
    {
        if let Some(pid) = pid_i32 {
            signal_process_group(pid, libc::SIGKILL);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the agent file path from step or plan settings.
fn resolve_agent_file(step: &Step, plan: &Plan) -> Option<PathBuf> {
    let agent_name = step.agent.as_deref().or(plan.agent.as_deref())?;
    let agents_dir = crate::config::agents_dir().ok()?;
    let path = agents_dir.join(agent_name);
    if path.exists() {
        Some(path)
    } else {
        // Try with .md extension.
        let with_ext = agents_dir.join(format!("{agent_name}.md"));
        if with_ext.exists() {
            Some(with_ext)
        } else {
            None
        }
    }
}

/// Build prior step summaries for all steps before the current one.
fn build_prior_step_summaries(
    conn: &Connection,
    plan: &Plan,
    current_step: &Step,
) -> Result<Vec<PriorStepSummary>> {
    let all_steps = storage::list_steps(conn, &plan.id)?;
    let mut summaries = Vec::new();

    for (idx, s) in all_steps.iter().enumerate() {
        if s.sort_key >= current_step.sort_key {
            break;
        }
        // Include any step that has produced an outcome — success, skip, or
        // failure. Failed/Aborted steps give the agent useful "here's what
        // did not work" context. Pending/InProgress are excluded because
        // they have nothing to report yet.
        if matches!(
            s.status,
            StepStatus::Complete | StepStatus::Skipped | StepStatus::Failed | StepStatus::Aborted
        ) {
            // Try to get changed files from the latest execution log.
            let files_changed = if let Ok(Some(log)) = storage::get_latest_log_for_step(conn, &s.id)
            {
                if let Some(diff) = &log.diff {
                    extract_changed_files_from_diff(diff)
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            summaries.push(PriorStepSummary {
                // Real 1-based position in the plan — not the summary slice
                // index, so skipped/pending steps keep their numbering
                // stable when some predecessors are filtered out.
                number: idx + 1,
                title: s.title.clone(),
                status: s.status,
                files_changed,
                description: s.description.clone(),
            });
        }
    }

    Ok(summaries)
}

/// Extract file paths from a unified diff.
///
/// Captures additions and modifications (`+++ b/`), deletions (`--- a/` paired
/// with `+++ /dev/null`), and both sides of renames (`rename from`/`rename to`,
/// or the `--- a/` and `+++ b/` pair when rename detection emits a diff body).
fn extract_changed_files_from_diff(diff: &str) -> Vec<String> {
    let mut files = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for line in diff.lines() {
        let path = line
            .strip_prefix("+++ b/")
            .or_else(|| line.strip_prefix("--- a/"))
            .or_else(|| line.strip_prefix("rename from "))
            .or_else(|| line.strip_prefix("rename to "));
        if let Some(path) = path
            && path != "/dev/null"
            && seen.insert(path.to_string())
        {
            files.push(path.to_string());
        }
    }
    files
}

/// Set the attempt count for a step to an absolute value.
fn set_step_attempts(conn: &Connection, step_id: &str, attempts: i32) -> Result<()> {
    conn.execute(
        "UPDATE steps SET attempts = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        rusqlite::params![attempts, step_id],
    ).context("Failed to update step attempts")?;
    Ok(())
}

/// Resolve the 1-based position of `step` within its plan. Falls back to 1
/// if the step isn't found in the list (which would be a consistency bug, but
/// we'd rather proceed than crash the run over observability bookkeeping).
fn resolve_step_num(conn: &Connection, plan: &Plan, step: &Step) -> Result<i32> {
    let all_steps = storage::list_steps(conn, &plan.id)?;
    let pos = all_steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|p| p as i32 + 1)
        .unwrap_or(1);
    Ok(pos)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_harness_json_full() {
        let json = r#"{"cost_usd": 0.05, "input_tokens": 1000, "output_tokens": 500, "session_id": "sess-1"}"#;
        let parsed = parse_harness_json(json);
        assert_eq!(parsed.cost_usd, Some(0.05));
        assert_eq!(parsed.input_tokens, Some(1000));
        assert_eq!(parsed.output_tokens, Some(500));
        assert_eq!(parsed.session_id.as_deref(), Some("sess-1"));
    }

    #[test]
    fn test_parse_harness_json_partial() {
        let json = r#"{"cost_usd": 0.12}"#;
        let parsed = parse_harness_json(json);
        assert_eq!(parsed.cost_usd, Some(0.12));
        assert!(parsed.input_tokens.is_none());
        assert!(parsed.output_tokens.is_none());
        assert!(parsed.session_id.is_none());
    }

    #[test]
    fn test_parse_harness_json_embedded_in_output() {
        let stdout =
            "Some harness output\nProcessing...\n{\"cost_usd\": 0.03, \"session_id\": \"abc\"}\n";
        let parsed = parse_harness_json(stdout);
        assert_eq!(parsed.cost_usd, Some(0.03));
        assert_eq!(parsed.session_id.as_deref(), Some("abc"));
    }

    #[test]
    fn test_parse_harness_json_no_json() {
        let stdout = "Just plain text output\nNo JSON here";
        let parsed = parse_harness_json(stdout);
        assert!(parsed.cost_usd.is_none());
        assert!(parsed.input_tokens.is_none());
    }

    #[test]
    fn test_parse_harness_json_unknown_keys_only() {
        let json = r#"{"unknown_field": 42}"#;
        let parsed = parse_harness_json(json);
        assert!(parsed.cost_usd.is_none());
    }

    #[test]
    fn test_extract_changed_files_from_diff() {
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1,3 +1,4 @@
 fn main() {}
+// new line
diff --git a/src/lib.rs b/src/lib.rs
--- /dev/null
+++ b/src/lib.rs
@@ -0,0 +1 @@
+pub mod foo;
";
        let files = extract_changed_files_from_diff(diff);
        assert_eq!(files, vec!["src/main.rs", "src/lib.rs"]);
    }

    #[test]
    fn test_extract_changed_files_from_diff_empty() {
        let files = extract_changed_files_from_diff("");
        assert!(files.is_empty());
    }

    #[test]
    fn test_extract_changed_files_from_diff_delete_rename_add() {
        let diff = "\
diff --git a/deleted.txt b/deleted.txt
deleted file mode 100644
--- a/deleted.txt
+++ /dev/null
@@ -1 +0,0 @@
-gone
diff --git a/old_name.rs b/new_name.rs
similarity index 80%
rename from old_name.rs
rename to new_name.rs
--- a/old_name.rs
+++ b/new_name.rs
@@ -1,2 +1,2 @@
-fn old() {}
+fn new() {}
diff --git a/added.rs b/added.rs
new file mode 100644
--- /dev/null
+++ b/added.rs
@@ -0,0 +1 @@
+pub fn x() {}
";
        let files = extract_changed_files_from_diff(diff);
        assert!(files.contains(&"deleted.txt".to_string()));
        assert!(files.contains(&"old_name.rs".to_string()));
        assert!(files.contains(&"new_name.rs".to_string()));
        assert!(files.contains(&"added.rs".to_string()));
        assert_eq!(files.len(), 4);
    }

    #[test]
    fn test_step_outcome_variants() {
        // Ensure all variants are constructible.
        let outcomes = [
            StepOutcome::Success,
            StepOutcome::Failed,
            StepOutcome::Aborted,
            StepOutcome::Timeout,
        ];
        assert_eq!(outcomes.len(), 4);
        assert_eq!(StepOutcome::Success, StepOutcome::Success);
        assert_ne!(StepOutcome::Success, StepOutcome::Failed);
    }

    #[test]
    fn test_failure_reason_mappings() {
        assert_eq!(FailureReason::Timeout.hook_label(), "timeout");
        assert_eq!(FailureReason::Aborted.hook_label(), "aborted");
        assert_eq!(FailureReason::TestFailed.hook_label(), "failed");
        assert_eq!(FailureReason::NoChanges.hook_label(), "no_changes");
        assert_eq!(FailureReason::HarnessFailed.hook_label(), "harness_failed");

        assert_eq!(FailureReason::Aborted.to_step_status(), StepStatus::Aborted);
        assert_eq!(
            FailureReason::NoChanges.to_step_status(),
            StepStatus::Failed
        );
        assert_eq!(
            FailureReason::TestFailed.to_step_status(),
            StepStatus::Failed
        );
        assert_eq!(
            FailureReason::HarnessFailed.to_step_status(),
            StepStatus::Failed
        );

        assert_eq!(FailureReason::NoChanges.to_outcome(), StepOutcome::Failed);
        assert_eq!(FailureReason::TestFailed.to_outcome(), StepOutcome::Failed);
        assert_eq!(
            FailureReason::HarnessFailed.to_outcome(),
            StepOutcome::Failed
        );
    }

    #[test]
    fn test_set_step_attempts() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None, None)
                .unwrap();
        assert_eq!(step.attempts, 0);

        super::set_step_attempts(&conn, &step.id, 3).unwrap();
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.attempts, 3);
    }

    #[test]
    fn test_build_prior_step_summaries() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None, None)
                .unwrap();
        let (s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None, None)
                .unwrap();
        let (s3, _) =
            storage::create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None, None, None)
                .unwrap();

        // Mark first two as complete.
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        storage::update_step_status(&conn, &s2.id, StepStatus::Complete).unwrap();

        let summaries = build_prior_step_summaries(&conn, &plan, &s3).unwrap();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].title, "First");
        assert_eq!(summaries[1].title, "Second");
    }

    #[test]
    fn test_build_prior_step_summaries_skips_non_complete() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None, None)
                .unwrap();
        let (_s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None, None)
                .unwrap();
        let (s3, _) =
            storage::create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None, None, None)
                .unwrap();

        // Only first is complete; second is pending.
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let summaries = build_prior_step_summaries(&conn, &plan, &s3).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].title, "First");
        assert_eq!(summaries[0].number, 1);
    }

    /// Prior-step summaries must carry each step's real 1-based position in
    /// the plan, not the index in the filtered slice. When a pending step
    /// sits between two completed steps, the second summary should be
    /// numbered 3 (its plan position) rather than 2 (its slice index).
    #[test]
    fn test_build_prior_step_summaries_preserves_real_numbers_with_gap() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None, None)
                .unwrap();
        let (_s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None, None)
                .unwrap();
        let (s3, _) =
            storage::create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None, None, None)
                .unwrap();
        let (s4, _) =
            storage::create_step(&conn, &plan.id, "Fourth", "d4", None, None, &[], None, None, None)
                .unwrap();

        // s1 and s3 are complete; s2 is pending (the gap).
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();
        storage::update_step_status(&conn, &s3.id, StepStatus::Complete).unwrap();

        let summaries = build_prior_step_summaries(&conn, &plan, &s4).unwrap();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].title, "First");
        assert_eq!(summaries[0].number, 1);
        assert_eq!(summaries[1].title, "Third");
        // Plan position (3), not slice index (2).
        assert_eq!(summaries[1].number, 3);
    }

    /// Regression: aborting at the pre-log boundary must persist the bumped
    /// attempt count and leave behind an execution_log row so the DB agrees
    /// with `StepResult.attempts_used`.
    #[tokio::test(flavor = "current_thread")]
    async fn test_abort_before_pre_log_persists_attempts_and_log() {
        use std::fs;
        use std::process::Command;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        Command::new("git")
            .args(["init"])
            .current_dir(&dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&dir)
            .output()
            .unwrap();
        fs::write(dir.join("README.md"), "init").unwrap();
        Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&dir)
            .output()
            .unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("claude"),
            None,
            &[],
        )
        .unwrap();
        // Seed the run_locks row that `acquire` would have created. The
        // abort branch bails before any `write_phase` call, but downstream
        // observers still expect the row to exist.
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None, None)
                .unwrap();
        assert_eq!(step.attempts, 0);

        let (tx, rx) = watch::channel(false);
        tx.send(true).unwrap();

        let config = Config::default();
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 120,
        };

        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();

        assert_eq!(result.outcome, StepOutcome::Aborted);
        assert_eq!(result.attempts_used, 1);

        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Aborted);
        assert_eq!(
            updated.attempts, result.attempts_used,
            "DB attempts must match StepResult.attempts_used"
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one execution_log row for the abort");
        assert_eq!(logs[0].attempt, 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserInterrupted),
            "abort-before-harness must record UserInterrupted",
        );
        assert_eq!(
            logs[0].test_status,
            Some(TestStatus::NotRun),
            "no tests ran before the abort, so test_status is NotRun",
        );
        assert!(
            logs[0].test_results.is_empty(),
            "test_results should be empty now that test_status carries the semantic"
        );

        // The abort-before-harness path bails before any `write_phase`
        // call, so the run_locks row is still the one we seeded — but it
        // must still exist (no accidental deletion on the abort path).
        let live = storage::get_live_run(&conn, &dir.to_string_lossy())
            .unwrap()
            .expect("run_locks row should still be present after abort");
        assert!(
            live.phase.is_none(),
            "abort-before-harness shouldn't have written a phase"
        );
    }

    /// Seed a `run_locks` row for `project` so `write_phase` has something
    /// to update when the executor invokes it. Mirrors what
    /// `run_lock::acquire` does in production — tests that previously
    /// relied on `open_memory()` alone now need this, because the phase
    /// writes errr out on a missing row (which is a production-safety
    /// invariant we deliberately don't want to soften).
    #[cfg(test)]
    fn seed_run_lock_row(conn: &Connection, project: &str) {
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![project, 1i64, "p-test", "slug"],
        )
        .unwrap();
    }

    /// Helper for executor integration tests: init a git repo in `dir` with
    /// one committed file so ralph has a branch/HEAD to work from.
    #[cfg(test)]
    fn init_git_repo(dir: &std::path::Path) {
        use std::fs;
        use std::process::Command;
        Command::new("git")
            .args(["init"])
            .current_dir(dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(dir)
            .output()
            .unwrap();
        fs::write(dir.join("README.md"), "init").unwrap();
        Command::new("git")
            .args(["add", "-A"])
            .current_dir(dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(dir)
            .output()
            .unwrap();
    }

    /// Write a fake harness shell script that just exits 0 without making
    /// changes — useful for exercising the NoChanges terminal path.
    /// Written outside the git workdir so it doesn't count as "changes" in
    /// the test, since the executor takes a pre-harness snapshot of untracked
    /// files and would otherwise treat the script itself as pre-existing.
    #[cfg(test)]
    fn write_noop_harness(outside_dir: &std::path::Path) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let path = outside_dir.join("noop-harness.sh");
        fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// A pre-step hook that always fails must terminate the execution log
    /// with HookFailed + NotRun when attempts are exhausted.
    #[tokio::test(flavor = "current_thread")]
    async fn test_pre_step_hook_failure_terminal_reason() {
        use crate::hook_library::{Hook as LibHook, Lifecycle, Scope};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("claude"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 0 so a single hook failure is terminal.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        // Register a pre-step hook that always fails.
        let fail_hook = LibHook {
            name: "failhook".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Global,
            command: "exit 1".to_string(),
        };
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "failhook").unwrap();

        let hook_ctx = HookContext {
            applicable: vec![fail_hook],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };

        let (_tx, rx) = watch::channel(false);

        let config = Config::default();
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();

        assert_eq!(result.outcome, StepOutcome::Failed);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::HookFailed)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
    }

    /// A harness that exits successfully but produces no changes should
    /// terminate the log with NoChanges + NotRun.
    #[tokio::test(flavor = "current_thread")]
    async fn test_no_changes_reason() {
        use crate::config::HarnessConfig;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Put the harness script outside the git workdir so it's not seen
        // as an untracked file (and therefore not treated as a "change").
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_noop_harness(harness_tmp.path());

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("noop"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0), // no retries — single failure is terminal
            None,
            None,
        )
        .unwrap();

        // Build a minimal config with our noop harness registered.
        let mut config = Config::default();
        config.harnesses.insert(
            "noop".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);

        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();

        assert_eq!(result.outcome, StepOutcome::Failed);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::NoChanges),
            "harness with no output should terminate as NoChanges"
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
    }

    /// Write a fake harness shell script that dumps a given number of bytes
    /// to stdout, optionally writes a single file inside the workdir so the
    /// step records a change, and exits 0.
    ///
    /// Written outside the git workdir so the script itself isn't counted
    /// as a pre-existing untracked change.
    #[cfg(test)]
    fn write_large_output_harness(
        outside_dir: &std::path::Path,
        workdir: &std::path::Path,
        bytes: usize,
        produce_changes: bool,
    ) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        // `yes | head -c N` is the standard deadlock reproducer: it writes
        // continuously until exactly N bytes have gone to stdout, then
        // returns 0. Much faster than building a string in shell.
        let touch = if produce_changes {
            format!(
                "touch {}/ralph-test-output.txt\n",
                workdir.to_string_lossy()
            )
        } else {
            String::new()
        };
        let script = format!(
            "#!/bin/sh\nyes | head -c {bytes}\n{touch}exit 0\n",
        );
        let path = outside_dir.join("large-output-harness.sh");
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// Regression: a harness that writes more than the kernel pipe buffer
    /// (~64 KiB) would deadlock before the concurrent-drain fix. 500 KB is
    /// well above the pipe buffer but well below the 4 MiB tail cap.
    /// Assert the step completes successfully, is committed, and
    /// `harness_stdout` contains content.
    #[tokio::test(flavor = "current_thread")]
    async fn test_large_harness_output_does_not_deadlock() {
        use crate::config::HarnessConfig;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path =
            write_large_output_harness(harness_tmp.path(), &dir, 500_000, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("bigout"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "bigout".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);

        // Cap the whole test at 30s so a regression hangs fast rather than
        // stalling the suite forever.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx),
        )
        .await
        .expect("execute_step deadlocked on large harness output")
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "step should succeed: {result:?}",
        );
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].committed, "log row should be marked committed");
        let stdout = logs[0].harness_stdout.as_deref().unwrap_or("");
        assert!(
            stdout.contains('y'),
            "captured stdout should contain the emitted 'y' bytes"
        );
        assert!(
            stdout.len() >= 500_000,
            "captured stdout should contain all 500 KB (got {} bytes)",
            stdout.len(),
        );
    }

    /// Truncation regression: a harness that emits > 4 MiB should have its
    /// captured tail bounded at HARNESS_OUTPUT_TAIL_BYTES plus the
    /// truncation marker.
    #[tokio::test(flavor = "current_thread")]
    async fn test_large_harness_output_truncates_to_cap() {
        use crate::config::HarnessConfig;
        use crate::io_util::TRUNCATION_MARKER_PREFIX;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // 5 MiB, safely over the 4 MiB cap.
        let bytes = 5 * 1024 * 1024;
        let harness_tmp = TempDir::new().unwrap();
        let harness_path =
            write_large_output_harness(harness_tmp.path(), &dir, bytes, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("hugeout"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries=0: we expect this to fail terminally because the
        // harness produces no changes — the point is just that we captured
        // the truncated stdout.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "hugeout".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);

        let _result = tokio::time::timeout(
            Duration::from_secs(60),
            execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx),
        )
        .await
        .expect("execute_step deadlocked on >4 MiB harness output")
        .unwrap();

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        let stdout = logs[0]
            .harness_stdout
            .as_deref()
            .expect("stdout should be captured");
        // Bounded at cap + marker length. The marker is short (<64 bytes),
        // so we allow a small slack above the cap.
        assert!(
            stdout.contains(TRUNCATION_MARKER_PREFIX),
            "truncation marker should be present in captured stdout"
        );
        assert!(
            stdout.len() <= HARNESS_OUTPUT_TAIL_BYTES + 128,
            "captured stdout should be bounded at cap + marker, got {} bytes",
            stdout.len()
        );
        // And the tail should still contain the actual harness output.
        assert!(stdout.contains('y'), "tail should include 'y' content");
    }

    /// End-to-end phase-write coverage: run a successful step through a
    /// real harness binary and assert the final `LiveRun` snapshot reflects
    /// the last phase (`PostStepHook`) and carries the step-identity fields
    /// the observer subcommands need.
    #[tokio::test(flavor = "current_thread")]
    async fn test_execute_step_writes_phase_transitions() {
        use crate::config::HarnessConfig;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Small harness that writes a file so the commit + post-step-hook
        // path is actually exercised. Placed outside the git workdir to
        // avoid being treated as a pre-existing untracked file.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, 1024, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("phases"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Phase Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "phases".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx),
        )
        .await
        .expect("execute_step timed out")
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);

        let live = storage::get_live_run(&conn, &dir.to_string_lossy())
            .unwrap()
            .expect("run_locks row must still exist after the step");

        // Final phase on the success path is the post-step-hook write.
        assert_eq!(
            live.phase,
            Some(crate::plan::Phase::PostStepHook),
            "last phase written by a successful step is PostStepHook"
        );
        assert_eq!(live.step_id.as_deref(), Some(step.id.as_str()));
        assert_eq!(live.step_num, Some(1));
        assert_eq!(live.attempt, Some(1));
        assert_eq!(live.max_attempts, Some(1));
        // Pre-test hook wasn't in play (no deterministic tests), so
        // current_command was last cleared by PostStepHook.
        assert_eq!(live.current_command, None);
        // Post-harness phases explicitly Clear the child columns so the row
        // stops advertising a dead harness pid. The harness did spawn (we
        // got a successful outcome), but by the time PostStepHook writes,
        // `child_pid` has been wiped.
        assert_eq!(
            live.child_pid, None,
            "child_pid must be cleared by post-harness phases"
        );
        assert_eq!(
            live.child_start_token, None,
            "child_start_token must be cleared alongside child_pid"
        );
    }

    /// Regression: when the executor aborts a harness that has spawned
    /// grandchildren, the *entire* descendant tree must be torn down.
    /// Before the process-group fix, the sleep grandchild in this test
    /// would be reparented to init and outlive the abort.
    ///
    /// The harness here writes its own pid and a child `sleep` pid to a
    /// file, then blocks via `wait`. We signal abort, then poll
    /// `kill(pid, 0)` on the grandchild to confirm it's actually dead.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_abort_kills_harness_process_group() {
        use crate::config::HarnessConfig;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // The pids file lives outside the git workdir so writing it doesn't
        // dirty the worktree.
        let shared = TempDir::new().unwrap();
        let pids_path = shared.path().join("pids.txt");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("pgroup-harness.sh");
        let script = format!(
            "#!/bin/sh\nsleep 60 &\nSLEEP_PID=$!\necho \"$$ $SLEEP_PID\" > {pids}\nwait\n",
            pids = pids_path.to_string_lossy(),
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("pgroup"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "pgroup".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (tx, rx) = watch::channel(false);

        // In a concurrent task: wait until the pids file appears (harness
        // has spawned its grandchild), then signal abort. The main task
        // drives `execute_step` to completion so graceful_shutdown actually
        // runs. Returning from this task drops `tx`, but the buffered
        // `true` value stays on the watch channel.
        let pids_path_clone = pids_path.clone();
        let abort_task = tokio::spawn(async move {
            for _ in 0..60 {
                if pids_path_clone.exists()
                    && fs::read_to_string(&pids_path_clone)
                        .map(|s| s.split_whitespace().count() == 2)
                        .unwrap_or(false)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = tx.send(true);
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx),
        )
        .await
        .expect("execute_step did not return within 10s on abort")
        .unwrap();

        abort_task.await.ok();

        assert_eq!(result.outcome, StepOutcome::Aborted);

        // Read back the grandchild's pid.
        let contents = fs::read_to_string(&pids_path).expect("pids file should exist");
        let mut parts = contents.split_whitespace();
        let _leader: i32 = parts.next().unwrap().parse().unwrap();
        let grandchild: i32 = parts.next().unwrap().parse().unwrap();

        // Poll up to ~2s for the grandchild to actually be reaped. kill(pid, 0)
        // returns 0 if alive, -1 if ESRCH/EPERM.
        let mut alive = true;
        for _ in 0..40 {
            // SAFETY: libc::kill with signo=0 is a pure liveness probe.
            let r = unsafe { libc::kill(grandchild, 0) };
            if r != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !alive,
            "grandchild sleep (pid {grandchild}) survived the abort — \
             process-group kill did not fan out",
        );
    }

    /// Complements `test_abort_kills_harness_process_group` with the
    /// specific case of a descendant that traps SIGTERM and refuses to die
    /// on the graceful signal. The belt-and-braces SIGKILL in
    /// `graceful_shutdown` must still tear it down.
    ///
    /// Harness script: backgrounds a subshell that traps SIGTERM to a no-op,
    /// then sleeps 60s. Writes both pids to a file and waits. After abort,
    /// we poll for the grandchild with `kill(pid, 0)` returning ESRCH.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_graceful_shutdown_kills_sigterm_resistant_descendant() {
        use crate::config::HarnessConfig;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let shared = TempDir::new().unwrap();
        let pids_path = shared.path().join("pids.txt");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("trap-harness.sh");
        // Subshell traps SIGTERM to nothing and sleeps. The parent writes
        // pids and waits. Note: the subshell must NOT setsid; it stays in
        // the harness's process group so the belt-and-braces SIGKILL finds
        // it. Using a `trap '' TERM` inside a subshell keeps it in the
        // parent's group (no new session) while making SIGTERM a no-op.
        let script = format!(
            "#!/bin/sh\n\
             ( trap '' TERM; sleep 60 ) &\n\
             SLEEP_PID=$!\n\
             echo \"$$ $SLEEP_PID\" > {pids}\n\
             wait\n",
            pids = pids_path.to_string_lossy(),
        );
        fs::write(&harness_path, &script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("trap"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "trap".to_string(),
            HarnessConfig {
                command: harness_path.to_string_lossy().into_owned(),
                args: vec![],
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
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (tx, rx) = watch::channel(false);

        // Abort once the harness has registered its pids.
        let pids_path_clone = pids_path.clone();
        let abort_task = tokio::spawn(async move {
            for _ in 0..60 {
                if pids_path_clone.exists()
                    && fs::read_to_string(&pids_path_clone)
                        .map(|s| s.split_whitespace().count() == 2)
                        .unwrap_or(false)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = tx.send(true);
        });

        // Whole test capped at 10s — if the grandchild survives we want a
        // quick failure rather than a stalled suite.
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx),
        )
        .await
        .expect("execute_step did not return within 10s on abort")
        .unwrap();

        abort_task.await.ok();

        assert_eq!(result.outcome, StepOutcome::Aborted);

        let contents = fs::read_to_string(&pids_path).expect("pids file should exist");
        let mut parts = contents.split_whitespace();
        let _leader: i32 = parts.next().unwrap().parse().unwrap();
        let grandchild: i32 = parts.next().unwrap().parse().unwrap();

        // The grandchild traps SIGTERM, so the graceful signal alone would
        // leave it alive. The belt-and-braces SIGKILL must fan out and
        // reap it. Allow ~2s for the kernel to deliver the signal.
        let mut alive = true;
        for _ in 0..40 {
            // SAFETY: libc::kill with signo=0 is a pure liveness probe.
            let r = unsafe { libc::kill(grandchild, 0) };
            if r != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !alive,
            "SIGTERM-trapping grandchild (pid {grandchild}) survived abort — \
             belt-and-braces SIGKILL did not fan out",
        );
    }

    // ---- change_policy coverage --------------------------------------------

    /// Build a shared `HarnessConfig` that points at a shell script.
    #[cfg(test)]
    fn harness_config_for_script(path: &std::path::Path) -> crate::config::HarnessConfig {
        crate::config::HarnessConfig {
            command: path.to_string_lossy().into_owned(),
            args: vec![],
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
        }
    }

    /// Write a shell script outside `workdir` that optionally touches a file
    /// inside `workdir` (producing a change), then exits 0.
    #[cfg(test)]
    fn write_simple_harness(
        outside_dir: &std::path::Path,
        workdir: &std::path::Path,
        produce_changes: bool,
    ) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let script = if produce_changes {
            format!(
                "#!/bin/sh\ntouch {}/ralph-policy-test.txt\nexit 0\n",
                workdir.to_string_lossy()
            )
        } else {
            "#!/bin/sh\nexit 0\n".to_string()
        };
        let path = outside_dir.join("policy-harness.sh");
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// Default policy (Required) + no changes + no tests configured → Failed
    /// with NoChanges + NotRun. Baseline guard that the existing behavior is
    /// preserved.
    #[tokio::test(flavor = "current_thread")]
    async fn test_default_step_with_no_changes_still_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("poly"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());

        // Default change_policy = Required.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
        )
        .unwrap();
        assert_eq!(step.change_policy, ChangePolicy::Required);

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Failed);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::NoChanges)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
    }

    /// Optional policy + no tests configured + no changes → Success with
    /// NotConfigured, no commit made.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_no_changes_no_tests_completes() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("poly"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Review",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            Some(ChangePolicy::Optional),
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(
            !logs[0].committed,
            "no-change success must not record a commit"
        );
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::Success)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotConfigured));

        let fresh_step = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(fresh_step.status, StepStatus::Complete);
    }

    /// Optional policy + tests configured and passing + no changes → Success
    /// with Passed, no commit.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_no_changes_passing_tests_completes() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, false);

        let conn = crate::db::open_memory().unwrap();
        // Deterministic test that always passes.
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("poly"),
            None,
            &["true".to_string()],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Review",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            Some(ChangePolicy::Optional),
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(!logs[0].committed);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::Success)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::Passed));
    }

    /// Optional policy + tests configured and failing + no changes → Failed
    /// with TestFailed + Failed. The failure classification is TestFailed
    /// (not NoChanges) because the tests actually ran.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_no_changes_failing_tests_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, false);

        let conn = crate::db::open_memory().unwrap();
        // Deterministic test that always fails.
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("poly"),
            None,
            &["false".to_string()],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Review",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            Some(ChangePolicy::Optional),
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Failed);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::TestFailed),
            "Optional + no changes + failing tests should classify as TestFailed"
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::Failed));
    }

    /// Optional policy + harness produces a diff + passing tests → Success
    /// with Passed AND a commit. Proves the policy doesn't regress the normal
    /// implementation-step path.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_with_changes_commits_normally() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("poly"),
            None,
            &["true".to_string()],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Implement",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            Some(ChangePolicy::Optional),
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(
            result.commit_hash.is_some(),
            "normal path with changes must still commit"
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].committed);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::Success)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::Passed));
    }

    // ---- non-zero harness exit must not false-green -----------------------

    /// Build a harness shell script that exits with the given code. Optionally
    /// writes a file inside `workdir` first to produce a dirty tree, so the
    /// rollback path can be exercised even on a crashing harness.
    #[cfg(test)]
    fn write_exit_harness(
        outside_dir: &std::path::Path,
        workdir: &std::path::Path,
        exit_code: i32,
        touch_file: bool,
    ) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let touch = if touch_file {
            format!("touch {}/ralph-exit-test.txt\n", workdir.to_string_lossy())
        } else {
            String::new()
        };
        let script = format!("#!/bin/sh\n{touch}exit {exit_code}\n");
        let path = outside_dir.join("exit-harness.sh");
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// Required policy + harness exits non-zero + no changes → Failed with
    /// HarnessFailed + NotRun. Tests are never run.
    #[tokio::test(flavor = "current_thread")]
    async fn test_nonzero_exit_required_policy_retries_and_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_exit_harness(harness_tmp.path(), &dir, 1, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("exit1"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0), // no retries
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("exit1".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Failed);
        assert_eq!(result.attempts_used, 1);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one attempt recorded");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::HarnessFailed),
            "non-zero exit must record HarnessFailed",
        );
        assert_eq!(
            logs[0].test_status,
            Some(TestStatus::NotRun),
            "tests must not run when the harness crashes",
        );
        assert!(!logs[0].committed, "no commit on a crashed harness");
    }

    /// Optional policy + harness exits non-zero + no changes + no tests →
    /// Failed (NOT Success). The whole point: optional policy must not
    /// whitewash a crashed harness.
    #[tokio::test(flavor = "current_thread")]
    async fn test_nonzero_exit_optional_policy_does_not_false_green() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_exit_harness(harness_tmp.path(), &dir, 1, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("exit1"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Review",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            Some(ChangePolicy::Optional),
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("exit1".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(
            result.outcome,
            StepOutcome::Failed,
            "optional policy must not false-green a crashed harness",
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::HarnessFailed),
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
    }

    /// Required policy + harness produces a diff + exits non-zero + passing
    /// tests → Failed with HarnessFailed. Passing tests must NOT rescue a
    /// crashed harness; the diff is rolled back.
    #[tokio::test(flavor = "current_thread")]
    async fn test_nonzero_exit_with_diff_and_passing_tests_still_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        // touch_file=true: harness writes a file, THEN exits 1.
        let harness_path = write_exit_harness(harness_tmp.path(), &dir, 1, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("exit1"),
            None,
            // Tests that always pass — they should NOT be run, so this choice
            // is immaterial except to prove that even if someone later changes
            // the code to run them, they couldn't rescue the attempt.
            &["true".to_string()],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(0),
            None,
            None, // Required
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("exit1".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Failed);
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::HarnessFailed),
        );
        assert!(
            !logs[0].committed,
            "crashed harness must not commit even with a diff",
        );

        // The diff must have been rolled back: check that the working tree is
        // clean (no ralph-exit-test.txt left behind).
        assert!(
            !dir.join("ralph-exit-test.txt").exists(),
            "diff must be rolled back after a crashed harness",
        );
    }

    /// Non-zero exit with retry budget: every attempt must log HarnessFailed;
    /// the final step status is Failed. 3 attempts = 1 initial + 2 retries.
    #[tokio::test(flavor = "current_thread")]
    async fn test_nonzero_exit_retries_up_to_budget() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_exit_harness(harness_tmp.path(), &dir, 1, false);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("exit1"),
            None,
            &[],
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            Some(2), // 2 retries = 3 total attempts
            None,
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("exit1".to_string(), harness_config_for_script(&harness_path));

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(false);
        let result = execute_step(&conn, &plan, &step, &config, &dir, &hook_ctx, rx)
            .await
            .unwrap();
        assert_eq!(result.outcome, StepOutcome::Failed);
        assert_eq!(result.attempts_used, 3);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 3, "one log row per attempt");
        for log in &logs {
            assert_eq!(
                log.termination_reason,
                Some(TerminationReason::HarnessFailed),
                "every attempt must record HarnessFailed",
            );
            assert_eq!(log.test_status, Some(TestStatus::NotRun));
        }

        let fresh_step = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(fresh_step.status, StepStatus::Failed);
    }
}
