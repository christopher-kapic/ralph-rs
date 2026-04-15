// Step executor
//
// Runs a single step through the full lifecycle:
// resolve harness → build prompt → spawn → wait → test → commit/rollback.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::git;
use crate::harness::{self, HarnessOutput};
use crate::hooks::{self, HookContext};
use crate::plan::{Plan, Step, StepStatus};
use crate::prompt::{self, PriorStepSummary, RetryContext};
use crate::storage;
use crate::test_runner;

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
    /// Tests failed (or no changes) after exhausting all attempts.
    TestFailed,
    /// Harness produced no changes (reserved for future use).
    #[allow(dead_code)]
    NoChanges,
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
            _ => "failed",
        }
    }
}

/// Shared references that stay constant for the duration of a step execution.
struct ExecCtx<'a> {
    conn: &'a Connection,
    plan: &'a Plan,
    step: &'a Step,
    workdir: &'a Path,
    pre_existing_untracked: &'a [String],
    hook_ctx: &'a HookContext,
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
fn finalize_failure(
    ctx: &ExecCtx<'_>,
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    reason: FailureReason,
    output: Option<&FailureOutput<'_>>,
) -> Result<StepResult> {
    // Rollback any uncommitted changes, preserving pre-existing untracked files.
    let rolled_back = if git::has_uncommitted_changes(ctx.workdir)? {
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
        )?;
    }

    storage::update_step_status(ctx.conn, &ctx.step.id, reason.to_step_status())?;
    hooks::run_post_step(
        ctx.conn,
        ctx.hook_ctx,
        ctx.plan,
        ctx.step,
        attempt,
        reason.hook_label(),
        ctx.workdir,
    );

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
    let timeout = if config.timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(config.timeout_secs))
    };

    // Resolve harness once (doesn't change between retries).
    let (harness_name, harness_config) = harness::resolve_harness(step, plan, config)?;

    // Resolve agent file path.
    let agent_file_path: Option<PathBuf> = resolve_agent_file(step, plan);

    // Collect prior step summaries for prompt context.
    let prior_steps = build_prior_step_summaries(conn, plan, step)?;

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
    };

    // Previous attempt context for retries.
    let mut prev_diff: Option<String> = None;
    let mut prev_test_output: Option<String> = None;
    let mut prev_files_modified: Vec<String> = Vec::new();

    let mut attempt = step.attempts;

    while attempt < max_attempts {
        attempt += 1;

        // Check abort before starting.
        if *abort_rx.borrow() {
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
        increment_step_attempts(conn, &step.id, attempt)?;

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

        // Build prompt.
        let prompt_text = prompt::build_step_prompt(
            plan,
            step,
            &prior_steps,
            agent_name,
            retry_context.as_ref(),
            harness_config.supports_agent_file,
        );

        // Create execution log entry.
        let exec_log =
            storage::create_execution_log(conn, &step.id, attempt, Some(&prompt_text), None)?;
        let started_at = std::time::Instant::now();

        // Run pre-step hook.
        if let Err(e) = hooks::run_pre_step(conn, hook_ctx, plan, step, attempt, workdir) {
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
            )?;
            if attempt >= max_attempts {
                storage::update_step_status(conn, &step.id, StepStatus::Failed)?;
                hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir);
                return Ok(StepResult {
                    outcome: StepOutcome::Failed,
                    step_id: step.id.clone(),
                    attempts_used: attempt,
                    commit_hash: None,
                });
            }
            prev_test_output = Some(format!("pre-step hook failed: {e}"));
            hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir);
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

        // Spawn harness subprocess.
        let child = harness::spawn_harness(harness_config, &args, &env_vars, workdir).await?;

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

                // Run tests if there are changes and tests are defined.
                let (test_passed, test_result_strings) = if has_changes
                    && !plan.deterministic_tests.is_empty()
                {
                    // Pre-test hook.
                    if let Err(e) =
                        hooks::run_pre_test(conn, hook_ctx, plan, step, attempt, workdir)
                    {
                        eprintln!("Pre-test hook failed: {e}");
                    }

                    let test_results = test_runner::run_tests(&plan.deterministic_tests, workdir);
                    let strings: Vec<String> = test_results
                        .results
                        .iter()
                        .map(|r| {
                            format!("{}: {}", r.command, if r.passed { "pass" } else { "FAIL" })
                        })
                        .collect();

                    // Post-test hook.
                    hooks::run_post_test(
                        conn,
                        hook_ctx,
                        plan,
                        step,
                        attempt,
                        test_results.all_passed,
                        workdir,
                    );

                    (test_results.all_passed, strings)
                } else if has_changes {
                    // No tests defined: treat as passing.
                    (true, Vec::new())
                } else {
                    // No changes at all: harness produced nothing useful.
                    (false, vec!["no changes detected".to_string()])
                };

                if test_passed && has_changes {
                    // Stage changes, excluding pre-existing untracked files.
                    let commit_msg = format!(
                        "ralph: {} [step:{}, plan:{}, attempt:{}]",
                        step.title, step.id, plan.slug, attempt,
                    );
                    git::stage_except(workdir, &pre_existing_untracked)?;
                    git::commit_staged(workdir, &commit_msg)?;
                    let commit_hash = git::get_commit_hash(workdir)?;

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
                    )?;

                    // Mark step as complete.
                    storage::update_step_status(conn, &step.id, StepStatus::Complete)?;

                    hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "complete", workdir);

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
                    return finalize_failure(
                        &ctx,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        FailureReason::TestFailed,
                        Some(&fail_output),
                    );
                }

                // Retry: rollback, log failure, stash context for next attempt.
                if has_changes {
                    git::rollback_except(workdir, &pre_existing_untracked)?;
                }
                let test_output_summary = test_result_strings.join("\n");
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
                )?;
                prev_diff = diff;
                prev_test_output = Some(test_output_summary);
                prev_files_modified = changed_files;
            }

            WaitResult::Timeout => {
                // Timeout: don't count as an attempt (revert the increment).
                attempt -= 1;
                increment_step_attempts(conn, &step.id, attempt)?;
                return finalize_failure(
                    &ctx,
                    exec_log.id,
                    duration_secs,
                    attempt,
                    FailureReason::Timeout,
                    None,
                );
            }

            WaitResult::Aborted => {
                return finalize_failure(
                    &ctx,
                    exec_log.id,
                    duration_secs,
                    attempt,
                    FailureReason::Aborted,
                    None,
                );
            }
        }
    }

    // Should not be reachable, but handle gracefully.
    storage::update_step_status(conn, &step.id, StepStatus::Failed)?;
    Ok(StepResult {
        outcome: StepOutcome::Failed,
        step_id: step.id.clone(),
        attempts_used: attempt,
        commit_hash: None,
    })
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

/// Outcome of waiting for a harness process.
enum WaitResult {
    /// Process completed (may have succeeded or failed).
    Completed(Result<HarnessOutput>),
    /// Process exceeded timeout and was killed.
    Timeout,
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
    // access `child` mutably for kill/wait.
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    match timeout {
        Some(dur) => {
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(exit_status) => {
                            let stdout = read_stdout(stdout_handle).await;
                            let stderr = read_stderr(stderr_handle).await;
                            WaitResult::Completed(Ok(HarnessOutput {
                                stdout,
                                stderr,
                                exit_code: exit_status.code(),
                                success: exit_status.success(),
                            }))
                        }
                        Err(e) => WaitResult::Completed(Err(e.into())),
                    }
                }
                _ = tokio::time::sleep(dur) => {
                    let _ = child.kill().await;
                    WaitResult::Timeout
                }
                _ = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
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
                            let stdout = read_stdout(stdout_handle).await;
                            let stderr = read_stderr(stderr_handle).await;
                            WaitResult::Completed(Ok(HarnessOutput {
                                stdout,
                                stderr,
                                exit_code: exit_status.code(),
                                success: exit_status.success(),
                            }))
                        }
                        Err(e) => WaitResult::Completed(Err(e.into())),
                    }
                }
                _ = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
                    WaitResult::Aborted
                }
            }
        }
    }
}

/// Read all bytes from an optional child stdout handle.
async fn read_stdout(handle: Option<tokio::process::ChildStdout>) -> String {
    use tokio::io::AsyncReadExt;
    match handle {
        Some(mut h) => {
            let mut buf = Vec::new();
            let _ = h.read_to_end(&mut buf).await;
            String::from_utf8_lossy(&buf).to_string()
        }
        None => String::new(),
    }
}

/// Read all bytes from an optional child stderr handle.
async fn read_stderr(handle: Option<tokio::process::ChildStderr>) -> String {
    use tokio::io::AsyncReadExt;
    match handle {
        Some(mut h) => {
            let mut buf = Vec::new();
            let _ = h.read_to_end(&mut buf).await;
            String::from_utf8_lossy(&buf).to_string()
        }
        None => String::new(),
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

/// Send SIGTERM, wait up to 5 seconds, then SIGKILL if still alive.
async fn graceful_shutdown(child: &mut tokio::process::Child) {
    // Send SIGTERM via the child's id.
    if let Some(id) = child.id() {
        #[cfg(unix)]
        {
            // Use kill(1) to send SIGTERM.
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(id.to_string())
                .status();
        }
        #[cfg(not(unix))]
        {
            let _ = id;
            // On non-Unix, just kill immediately.
            let _ = child.kill().await;
            return;
        }
    }

    // Wait up to 5 seconds for graceful exit.
    let grace = tokio::time::sleep(Duration::from_secs(5));
    tokio::select! {
        _ = child.wait() => {
            // Exited within grace period.
        }
        _ = grace => {
            // Grace period expired — force kill.
            let _ = child.kill().await;
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

    for s in &all_steps {
        if s.sort_key >= current_step.sort_key {
            break;
        }
        if s.status == StepStatus::Complete || s.status == StepStatus::Skipped {
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
fn extract_changed_files_from_diff(diff: &str) -> Vec<String> {
    let mut files = Vec::new();
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ b/")
            && path != "/dev/null"
        {
            files.push(path.to_string());
        }
    }
    files.dedup();
    files
}

/// Set the attempt count for a step directly.
fn increment_step_attempts(conn: &Connection, step_id: &str, attempts: i32) -> Result<()> {
    conn.execute(
        "UPDATE steps SET attempts = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        rusqlite::params![attempts, step_id],
    ).context("Failed to update step attempts")?;
    Ok(())
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
    fn test_increment_step_attempts() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None)
                .unwrap();
        assert_eq!(step.attempts, 0);

        super::increment_step_attempts(&conn, &step.id, 3).unwrap();
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.attempts, 3);
    }

    #[test]
    fn test_build_prior_step_summaries() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let (s1, _) =
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
                .unwrap();
        let (s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None)
                .unwrap();
        let (s3, _) =
            storage::create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None, None)
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
            storage::create_step(&conn, &plan.id, "First", "d1", None, None, &[], None, None)
                .unwrap();
        let (_s2, _) =
            storage::create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None, None)
                .unwrap();
        let (s3, _) =
            storage::create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None, None)
                .unwrap();

        // Only first is complete; second is pending.
        storage::update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let summaries = build_prior_step_summaries(&conn, &plan, &s3).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].title, "First");
    }
}
