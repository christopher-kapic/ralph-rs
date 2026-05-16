// Step executor
//
// Runs a single step through the full lifecycle:
// resolve harness → build prompt → spawn → wait → test → commit/rollback.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tokio::sync::watch;

use crate::config::Config;
use crate::git;
use crate::harness::{self, HarnessOutput};
use crate::hooks::{self, HookContext};
use crate::io_util;
use crate::output::ChunkStream;
use crate::plan::{ChangePolicy, Phase, Plan, Step, StepStatus, TerminationReason, TestStatus};
use crate::prompt::{self, Prompts, RetryContext};
use crate::run_lock::process_start_token;
use crate::signal::{CancelReason, CancelState};
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
    /// Execution was aborted via signal (Ctrl+C / SIGTERM). Terminates the
    /// whole run.
    Aborted,
    /// The operator ran `ralph skip` (or the TUI skip binding) against this
    /// step while it was executing in this process. The harness was killed
    /// via the cancel ladder and the step marked `Skipped`. Distinct from
    /// [`StepOutcome::Aborted`]: a skip drops only this step; the runner
    /// continues to the next one.
    Skipped,
    /// The harness process exceeded the timeout.
    Timeout,
    /// The harness called `ralph question ask` during the attempt, leaving one
    /// or more unanswered `step_questions` rows. Tests + commit are skipped,
    /// any diff is rolled back, and the plan's effective status becomes
    /// [`crate::plan::PlanStatus::Question`] until the user answers (TUI-plan
    /// §17). The runner stops the loop cleanly so the run lock is released.
    PausedForQuestion,
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

/// Per-call options threaded from the runner into [`execute_step`] to drive
/// the per-attempt progress sub-header and prompt preview. Kept separate
/// from the persistent [`Config`] knobs because these are read from CLI
/// flags / output context, not config.json.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExecuteOptions {
    /// Skip truncation on the per-attempt prompt preview.
    pub verbose: bool,
    /// 1-based step position for the progress sub-header. Reserved for
    /// future header layouts that reprint the step number per attempt.
    pub step_num_in_plan: usize,
    /// Total steps in the plan (for the `[N/M]` prefix). Reserved; see above.
    pub step_total: usize,
    /// True when the parent runner is emitting NDJSON. Suppresses the
    /// human-readable sub-header and preview (we emit PromptPrepared events
    /// instead).
    pub json_output: bool,
    /// True when stderr should be ANSI-colored.
    pub color: bool,
    /// Monotonic per-run `seq` counter shared across stdout/stderr drainers
    /// (and across all step invocations in a single `ralph run`). `None`
    /// disables `HarnessChunk` event emission entirely — used by tests and
    /// non-NDJSON runs. Created once per run by the runner so the
    /// counter survives across step boundaries. See TUI-plan §13.1.
    pub chunk_seq: Option<Arc<AtomicU64>>,
    /// Truncation cap for each emitted chunk's `text` payload. Mirrors
    /// [`Config::harness_chunk_max_bytes`]. Ignored when `chunk_seq` is
    /// `None`.
    pub chunk_max_bytes: usize,
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            step_num_in_plan: 0,
            step_total: 0,
            json_output: false,
            color: false,
            chunk_seq: None,
            chunk_max_bytes: 4096,
        }
    }
}

/// Max chars of prompt included in the non-verbose preview and the
/// `RunEvent::PromptPrepared` payload.
const PROMPT_PREVIEW_CHARS: usize = 512;

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

/// Diagnostic message used when an attempt ends with a clean worktree but
/// HEAD has advanced — the harness committed on its own. Ralph owns commits
/// per step, so a step description that tells the agent to commit is the
/// usual cause. Surfaced both in the execution log's `test_results` (visible
/// via `ralph log`) and to stderr at terminal failure.
pub(crate) const NO_CHANGES_AGENT_COMMITTED_HINT: &str = "no changes (worktree clean, but HEAD advanced during attempt — agent appears to have committed). \
     Ralph owns commits per step; remove any 'git commit' / 'git add' instructions from the step description.";

/// Reason a step execution failed terminally.
#[derive(Debug, Clone, Copy)]
enum FailureReason {
    /// Harness exceeded timeout.
    Timeout,
    /// Execution was aborted via signal (Ctrl+C / SIGTERM) — terminates the
    /// whole run.
    Aborted,
    /// Operator ran `ralph skip` against the in-flight step. Not really a
    /// *failure* — the step is intentionally dropped — but it shares
    /// `finalize_failure`'s teardown (rollback of uncommitted work, log row,
    /// post-step hook). Distinct from `Aborted` so the step is marked
    /// `Skipped`/`UserSkipped` and the run advances instead of ending.
    Skipped,
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
            Self::Skipped => StepStatus::Skipped,
            _ => StepStatus::Failed,
        }
    }

    fn to_outcome(self) -> StepOutcome {
        match self {
            Self::Timeout => StepOutcome::Timeout,
            Self::Aborted => StepOutcome::Aborted,
            Self::Skipped => StepOutcome::Skipped,
            _ => StepOutcome::Failed,
        }
    }

    fn hook_label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Aborted => "aborted",
            Self::Skipped => "skipped",
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
    /// True when the parent runner is emitting NDJSON. Threaded to
    /// [`write_phase`] so phase transitions also emit a
    /// [`crate::output::RunEvent::PhaseChanged`].
    json_output: bool,
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
///
/// When `json_output` is true, a [`crate::output::RunEvent::PhaseChanged`]
/// is emitted to stdout after the storage update so NDJSON consumers (the
/// TUI, meta-harnesses) can redraw the phase indicator without polling.
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
    json_output: bool,
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
    )?;
    if json_output {
        crate::output::emit_ndjson(&crate::output::RunEvent::PhaseChanged {
            phase,
            phase_started_at: chrono::Utc::now(),
        })?;
    }
    Ok(())
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
            ctx.json_output,
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
        ctx.json_output,
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

/// Finalize an attempt that paused because the harness left unanswered
/// `step_questions` rows behind (TUI-plan.md §17 "Runner integration").
///
/// Skips tests + commit, rolls back any diff the harness produced, writes the
/// `execution_logs` row with `termination_reason = paused_for_question`, and
/// returns the step's status to [`StepStatus::Pending`] so the user's next
/// `ralph run` (after answering) picks it up cleanly. Leaving it `InProgress`
/// would be swept to `Aborted` at the start of the next run and pollute the
/// audit trail with a synthetic abort. `step.attempts` was already bumped at
/// the top of the retry loop, mirroring the "single counter" rule from §17.
#[allow(clippy::too_many_arguments)]
async fn finalize_paused_for_question(
    ctx: &ExecCtx<'_>,
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    diff: Option<&str>,
    stdout: &str,
    stderr: &str,
    parsed: &ParsedHarnessOutput,
) -> Result<StepResult> {
    let rolled_back = if git::has_uncommitted_changes(ctx.workdir)? {
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
            ctx.json_output,
        )?;
        git::rollback_except(ctx.workdir, ctx.pre_existing_untracked)?;
        true
    } else {
        false
    };

    storage::update_execution_log(
        ctx.conn,
        exec_log_id,
        Some(duration_secs),
        diff,
        &[],
        rolled_back,
        false,
        None,
        Some(stdout),
        Some(stderr),
        parsed.cost_usd,
        parsed.input_tokens,
        parsed.output_tokens,
        parsed.session_id.as_deref(),
        Some(TerminationReason::PausedForQuestion),
        Some(TestStatus::NotRun),
    )?;

    storage::update_step_status(ctx.conn, &ctx.step.id, StepStatus::Pending)?;

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
        ctx.json_output,
    )?;
    hooks::run_post_step(
        ctx.conn,
        ctx.hook_ctx,
        ctx.plan,
        ctx.step,
        attempt,
        "paused",
        ctx.workdir,
    )
    .await?;

    Ok(StepResult {
        outcome: StepOutcome::PausedForQuestion,
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
#[allow(clippy::too_many_arguments)]
pub async fn execute_step(
    conn: &Connection,
    plan: &Plan,
    step: &Step,
    config: &Config,
    workdir: &Path,
    hook_ctx: &HookContext,
    abort_rx: watch::Receiver<CancelState>,
    exec_opts: ExecuteOptions,
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

    // Per-step disk-space gate.
    //
    // A nearly-full filesystem is the only class of failure where we actively
    // don't want to start work — past that point, SQLite writes start failing
    // with SQLITE_FULL and ralph's own state (execution_logs, run_locks) can
    // be corrupted. Check before we even touch the retry loop so a FS that
    // filled between preflight and now still bails out cleanly.
    //
    // `min_free_disk_mb = 0` disables the check (user opt-out).
    if config.min_free_disk_mb > 0 {
        match crate::preflight::disk_space(workdir) {
            Ok(ds) => {
                let required_bytes = config.min_free_disk_mb.saturating_mul(1_048_576);
                if ds.available_bytes < required_bytes {
                    let have_gb = ds.available_gb();
                    let need_gb = config.min_free_disk_mb as f64 / 1024.0;
                    eprintln!(
                        "> Step skipped: only {have_gb:.1} GB free, need >= {need_gb:.1} GB \
                         (config: min_free_disk_mb)"
                    );
                    let attempt = step.attempts + 1;
                    set_step_attempts(conn, &step.id, attempt)?;
                    let exec_log =
                        storage::create_execution_log(conn, &step.id, attempt, None, None)?;
                    let msg = format!(
                        "insufficient disk space: {have_gb:.1} GB free, \
                         need >= {need_gb:.1} GB"
                    );
                    storage::update_execution_log(
                        conn,
                        exec_log.id,
                        Some(0.0),
                        None,
                        &[msg],
                        false,
                        false,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(TerminationReason::InsufficientDiskSpace),
                        Some(TestStatus::NotRun),
                    )?;
                    storage::update_step_status(conn, &step.id, StepStatus::Failed)?;
                    return Ok(StepResult {
                        outcome: StepOutcome::Failed,
                        step_id: step.id.clone(),
                        attempts_used: attempt,
                        commit_hash: None,
                    });
                }
            }
            Err(e) => {
                // Probe failure (non-unix, weird FS) — log and continue.
                // We'd rather run than block on an inscrutable error.
                eprintln!("> Disk space probe failed, continuing: {e}");
            }
        }
    }

    let timeout = config.timeout_secs.map(Duration::from_secs);

    // Resolve harness once (doesn't change between retries).
    let (harness_name, harness_config) = harness::resolve_harness(step, plan, config)?;

    // Resolve agent file path.
    let agent_file_path: Option<PathBuf> = resolve_agent_file(step, plan);

    // Snapshot all steps in the plan so the prompt builder can render the
    // compact "Plan step map" section. Taken once up front (pre-attempt)
    // because steps don't change during a single-step execution, and a
    // consistent snapshot keeps the prompt stable across retries.
    let all_steps = storage::list_steps(conn, &plan.id)?;

    // 1-based position of `step` within its plan. Computed once up front so
    // every `write_phase` call can pass it without reshuffling the plan's
    // step list each time.
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
        json_output: exec_opts.json_output,
    };

    // Previous attempt context for retries.
    let mut prev_diff: Option<String> = None;
    let mut prev_test_output: Option<String> = None;
    let mut prev_files_modified: Vec<String> = Vec::new();

    let mut attempt = step.attempts;

    while attempt < max_attempts {
        attempt += 1;

        // Check cancel before starting. Persist the bumped attempt count and
        // drop an execution-log row so the DB reflects the same attempt number
        // that StepResult reports and the cancel has a visible audit trail.
        // `Aborted` (Ctrl+C) terminates the whole run; `Skipped` (a same-
        // process `ralph skip` of this step) drops only this step.
        if let Some(reason) = *abort_rx.borrow() {
            return finalize_precancel(conn, &step.id, attempt, reason);
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

        // Collect the four-layer prompt model's configurable layers. The
        // Global layer comes from config, the Project layer from the
        // `project_settings` row (a missing row is treated as "no project
        // prompt"), and the Plan layer is the plan's own description.
        let project_settings = storage::get_project_settings(conn, &plan.project)?;
        let prompts = Prompts {
            global: config.prompt.clone(),
            project: project_settings.prompt.clone(),
            plan: Some(plan.description.clone()),
        };

        // Fetch any answered questions for this step so the next-attempt
        // prompt re-injects the user's clarifications between Plan context
        // and Step details (TUI-plan.md §17 "Retry context after answering").
        // First attempts on un-paused steps return an empty slice — the call
        // is one indexed lookup, no need to gate it on attempt > 1.
        let answered_questions = storage::list_answered_questions_for_step(conn, &step.id)?;

        // Build prompt.
        let prompt_text = prompt::build_step_prompt(
            plan,
            step,
            &all_steps,
            agent_name,
            retry_context.as_ref(),
            harness_config.supports_agent_file,
            &prompts,
            &answered_questions,
        );

        // Create execution log entry.
        let exec_log =
            storage::create_execution_log(conn, &step.id, attempt, Some(&prompt_text), None)?;
        let started_at = std::time::Instant::now();

        // Per-attempt progress sub-header and prompt preview. Printed
        // inside the retry loop so every attempt (including retries) gets
        // its own timestamped "started at" line. In NDJSON mode we emit
        // a structured `PromptPrepared` event instead.
        render_attempt_header(
            &exec_opts,
            config,
            harness_name,
            harness_config,
            attempt,
            max_attempts,
        );
        render_prompt_preview(&exec_opts, step, attempt, &prompt_text)?;

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
            exec_opts.json_output,
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
                    exec_opts.json_output,
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
                exec_opts.json_output,
            )?;
            hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir).await?;
            continue;
        }

        // Build harness args and env. `step.model` (if set) overrides the
        // harness's config-level `default_model`; None on both sides means
        // the harness is invoked without any model flag.
        //
        // `prepare_harness_invocation` also decides how the prompt is
        // delivered to the child (stdin / temp file / argv) per the
        // harness's `prompt_input` mode. A retry-context-bloated prompt
        // on an argv-mode harness transparently spills to a temp file so
        // we don't trip `E2BIG` at `execve` time.
        let (args, prompt_delivery) = harness::prepare_harness_invocation(
            harness_name,
            harness_config,
            &prompt_text,
            agent_file_path.as_deref(),
            step.model.as_deref(),
        )?;
        let env_vars = harness::build_harness_env(harness_config, agent_file_path.as_deref());

        // Snapshot HEAD just before the harness runs so we can detect the
        // case where the harness committed on its own (clean worktree +
        // HEAD advanced). Used to upgrade the no_changes diagnostic — see
        // the agent_committed_clean check after the harness completes.
        // `.ok()` because a missing HEAD (empty repo) shouldn't fail the
        // run; we just won't detect the committed-on-its-own case.
        let head_before_harness = git::get_commit_hash(workdir).ok();

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
            exec_opts.json_output,
        )?;

        // Spawn harness subprocess. The tempfile (if any) must outlive
        // the child — drop triggers cleanup, which would yank the prompt
        // out from under the harness mid-run.
        let (child, _prompt_tempfile) = harness::spawn_harness_with_delivery(
            harness_config,
            &args,
            &env_vars,
            workdir,
            prompt_delivery,
        )
        .await?;

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
            exec_opts.json_output,
        )?;

        // Wait with timeout and abort racing. Build the chunk-emitter
        // config from the shared per-run seq counter; pass `None` when the
        // runner isn't streaming NDJSON so the drainer stays a pure
        // tail-capturer.
        let chunk_emit = exec_opts
            .chunk_seq
            .clone()
            .filter(|_| exec_opts.json_output)
            .map(|seq| ChunkEmitConfig {
                seq,
                max_bytes: exec_opts.chunk_max_bytes,
            });
        let emitters = build_chunk_emitters(chunk_emit);
        let wait_result =
            wait_with_timeout_and_abort(child, timeout, abort_rx.clone(), emitters).await;
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

                // Detect "agent appears to have committed on its own": the
                // worktree is clean *and* HEAD advanced during this attempt.
                // Surfaced in the no_changes diagnostic below so authors who
                // told the harness to commit themselves get a self-explanatory
                // error instead of a generic no_changes loop. Both halves
                // must be Some — if either snapshot failed we can't compare
                // safely, so suppress the hint rather than risk a false
                // positive.
                let agent_committed_clean = !has_changes
                    && match (&head_before_harness, git::get_commit_hash(workdir).ok()) {
                        (Some(before), Some(after)) => before != &after,
                        _ => false,
                    };

                // Pause if the harness left unanswered `step_questions` rows
                // behind during this attempt (TUI-plan.md §17). Tested first
                // — even on non-zero exit — so a harness that asks then
                // crashes still surfaces as a pause: the user's clarification
                // is the prerequisite for any retry, regardless of whether
                // the crash was a side effect of the harness's own
                // self-terminate-after-asking path.
                let unanswered =
                    storage::count_unanswered_questions_for_attempt(conn, &step.id, attempt)?;
                if unanswered > 0 {
                    let _ = changed_files; // unused on this path
                    return finalize_paused_for_question(
                        &ctx,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        diff.as_deref(),
                        &output.stdout,
                        &output.stderr,
                        &parsed,
                    )
                    .await;
                }

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
                            exec_opts.json_output,
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
                let policy_allows_no_change_success = step.change_policy == ChangePolicy::Optional;
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
                        exec_opts.json_output,
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
                        exec_opts.json_output,
                    )?;
                    // Build a chunk-emit config mirroring the harness path:
                    // share the per-run `chunk_seq` counter and the same
                    // `chunk_max_bytes` cap. Only enabled when this run is
                    // emitting NDJSON; otherwise pass `None` so the test
                    // runner stays a pure tail-capturer. Per TUI-plan §13.1.
                    let test_chunk_cfg = exec_opts
                        .chunk_seq
                        .clone()
                        .filter(|_| exec_opts.json_output)
                        .map(|seq| test_runner::TestChunkConfig {
                            seq,
                            max_bytes: exec_opts.chunk_max_bytes,
                            sink: Arc::new(|test_index, stream, text, seq| {
                                let _ = crate::output::emit_ndjson(
                                    &crate::output::RunEvent::TestChunk {
                                        test_index,
                                        stream,
                                        text,
                                        seq,
                                    },
                                );
                            }),
                        });
                    let test_results = test_runner::run_tests(
                        &plan.deterministic_tests,
                        workdir,
                        abort_rx.clone(),
                        test_chunk_cfg,
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
                        exec_opts.json_output,
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

                // Commit-ownership invariant: if the worktree is clean *and*
                // HEAD moved during this attempt, the agent committed on its
                // own. That's a contract violation regardless of
                // `change_policy` — under Optional we'd otherwise silently
                // succeed with `committed=false` while HEAD has advanced,
                // breaking provenance for every downstream step. Force the
                // attempt to fail and surface the diagnostic above whatever
                // test output was already recorded so the cause appears
                // first in `ralph log`.
                let (test_passed, test_result_strings, test_aborted) = if agent_committed_clean {
                    let mut strings = test_result_strings;
                    strings.insert(0, NO_CHANGES_AGENT_COMMITTED_HINT.to_string());
                    (false, strings, test_aborted)
                } else {
                    (test_passed, test_result_strings, test_aborted)
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
                            exec_opts.json_output,
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
                        exec_opts.json_output,
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
                        exec_opts.json_output,
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
                        exec_opts.json_output,
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
                    //  - agent_committed_clean -> classify as NoChanges
                    //    regardless of policy. The contract violation is the
                    //    same whether the step was Required or Optional, and
                    //    keeping the same termination_reason means hooks
                    //    keying off `no_changes` cover both cases without
                    //    needing a new label.
                    //  - has_changes     -> tests ran and failed
                    //  - Required + none -> NoChanges (unchanged behavior)
                    //  - Optional + none -> the only way to reach here with
                    //    Optional policy is a failing test run, so we classify
                    //    as TestFailed (tests did run; they just failed).
                    let (reason, term_reason, test_st) = if agent_committed_clean {
                        (
                            FailureReason::NoChanges,
                            TerminationReason::NoChanges,
                            TestStatus::NotRun,
                        )
                    } else if has_changes {
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
                    // Echo the diagnostic to stderr so a runner watching the
                    // live output sees it without having to run `ralph log`.
                    // Gated on json_output so NDJSON streams stay clean —
                    // the same string is already in test_results, which the
                    // log row carries.
                    if agent_committed_clean && !exec_opts.json_output {
                        eprintln!("  hint: {NO_CHANGES_AGENT_COMMITTED_HINT}");
                    }
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
                        exec_opts.json_output,
                    )?;
                    git::rollback_except(workdir, &pre_existing_untracked)?;
                }
                let test_output_summary = test_result_strings.join("\n");
                // This row describes *this* attempt's termination even though
                // the step will retry — record why this attempt failed. Same
                // precedence as the terminal case: agent_committed_clean is
                // a contract violation regardless of policy, so it wins over
                // the policy-specific branches.
                let (retry_term, retry_test_status) = if agent_committed_clean {
                    (TerminationReason::NoChanges, TestStatus::NotRun)
                } else if has_changes {
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
                // invoked on this attempt). Aborted terminates the WHOLE run.
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

            WaitResult::Skipped { stdout, stderr } => {
                // `ralph skip` killed the in-flight harness via the same
                // ladder as Aborted, but only THIS step is dropped — the
                // runner advances. Capture any partial output + parsed JSON
                // so `ralph log` retains diagnostic context for the skipped
                // attempt. test_status is NotRun (tests never ran), and
                // termination_reason is UserSkipped (distinct from the
                // UserInterrupted that Ctrl+C records). committed=false:
                // change-handling (stash/commit/discard) lands in steps
                // 17-18; for now finalize_failure rolls back any uncommitted
                // work, exactly like the abort path.
                let parsed = parse_harness_json(&stdout);
                let has_changes = git::has_uncommitted_changes(workdir)?;
                let diff = if has_changes {
                    Some(git::get_diff(workdir)?)
                } else {
                    None
                };
                let skip_results: Vec<String> = Vec::new();
                let fail_output = FailureOutput {
                    diff: diff.as_deref(),
                    test_results: &skip_results,
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
                    FailureReason::Skipped,
                    Some(&fail_output),
                    TerminationReason::UserSkipped,
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

/// Per-attempt configuration for the chunk-emission side of the harness
/// drainers. `seq` is shared across both stdout and stderr (and across the
/// whole run) so consumers see a strictly monotonic stream.
#[derive(Clone)]
struct ChunkEmitConfig {
    seq: Arc<AtomicU64>,
    max_bytes: usize,
}

/// Build the per-stream `ChunkEmitter` pair used by [`wait_with_timeout_and_abort`].
///
/// Both emitters reference the *same* `seq` counter so a `HarnessChunk`
/// event's `seq` is unique within the run regardless of which stream it came
/// from.
///
/// Production callers go through this wrapper, which uses
/// [`crate::output::emit_ndjson`] as the sink; tests use
/// [`build_chunk_emitters_with_sink`] directly with a capturing sink so they
/// don't have to redirect stdout. Serialization or write failures from the
/// production sink are swallowed (best-effort streaming — losing one chunk
/// should not break the run; the captured tail is still recorded in the
/// execution log).
fn build_chunk_emitters(
    cfg: Option<ChunkEmitConfig>,
) -> (Option<io_util::ChunkEmitter>, Option<io_util::ChunkEmitter>) {
    let sink: io_util::ChunkSink = Arc::new(|stream, text, seq| {
        let _ = crate::output::emit_ndjson(&crate::output::RunEvent::HarnessChunk {
            stream,
            text,
            seq,
        });
    });
    build_chunk_emitters_with_sink(cfg, sink)
}

/// Sink-injectable variant of [`build_chunk_emitters`]. When `cfg` is `None`
/// the sink is dropped without ever being called.
fn build_chunk_emitters_with_sink(
    cfg: Option<ChunkEmitConfig>,
    sink: io_util::ChunkSink,
) -> (Option<io_util::ChunkEmitter>, Option<io_util::ChunkEmitter>) {
    let Some(cfg) = cfg else {
        return (None, None);
    };
    let stdout_emitter = io_util::ChunkEmitter {
        stream: ChunkStream::Stdout,
        seq: cfg.seq.clone(),
        max_bytes: cfg.max_bytes,
        sink: sink.clone(),
    };
    let stderr_emitter = io_util::ChunkEmitter {
        stream: ChunkStream::Stderr,
        seq: cfg.seq.clone(),
        max_bytes: cfg.max_bytes,
        sink,
    };
    (Some(stdout_emitter), Some(stderr_emitter))
}

/// Outcome of waiting for a harness process.
enum WaitResult {
    /// Process completed (may have succeeded or failed).
    Completed(Result<HarnessOutput>),
    /// Process exceeded timeout and was killed. The partial stdout/stderr
    /// captured before the kill are surfaced so the execution log retains
    /// diagnostic context for the failed attempt.
    Timeout { stdout: String, stderr: String },
    /// Abort signal received (Ctrl+C / SIGTERM). Terminates the whole run.
    /// Carries no output — the abort path drops the drain tasks.
    Aborted,
    /// `ralph skip` of the in-flight step tripped the cancel channel with
    /// [`CancelReason::Skipped`]. The harness was killed via the same ladder
    /// as `Aborted`, but only this step is dropped — the run continues.
    /// Distinct variant so the executor records `UserSkipped` (not
    /// `UserInterrupted`) and returns [`StepOutcome::Skipped`]. Partial
    /// stdout/stderr is captured so the skipped attempt's log row retains
    /// diagnostic context.
    Skipped { stdout: String, stderr: String },
}

/// Wait for a child process, racing against an optional timeout and an abort signal.
///
/// - When timeout is `None`: the process runs indefinitely (only the abort
///   signal can stop it early).
/// - When timeout is `Some(d)`: the process is killed after `d` if it
///   hasn't completed.
/// - On **abort**: SIGTERM is sent, followed by a 5-second grace period,
///   then SIGKILL if still running.
///
/// When `emitters` is non-`None` for a stream, that drainer also emits
/// [`crate::output::RunEvent::HarnessChunk`] events one-per-newline as the
/// child produces output. The same `seq` counter is typically shared across
/// stdout and stderr so consumers can reorder by `seq`.
///
/// Production callers build `emitters` via [`build_chunk_emitters`]; tests
/// use [`build_chunk_emitters_with_sink`] to capture events without
/// touching stdout.
async fn wait_with_timeout_and_abort(
    mut child: tokio::process::Child,
    timeout: Option<Duration>,
    mut abort_rx: watch::Receiver<CancelState>,
    emitters: (Option<io_util::ChunkEmitter>, Option<io_util::ChunkEmitter>),
) -> WaitResult {
    // Take stdout/stderr handles before entering select! so we can still
    // access `child` mutably for kill/wait. Spawn concurrent drain tasks
    // *immediately*: a child that writes more than the pipe buffer
    // (~64 KiB) would otherwise block on write(2) while we block on wait(),
    // deadlocking. Draining concurrently keeps the pipe flowing.
    let (stdout_emitter, stderr_emitter) = emitters;
    let stdout_task = io_util::drain_bounded_with_emitter(
        child.stdout.take(),
        HARNESS_OUTPUT_TAIL_BYTES,
        stdout_emitter,
    );
    let stderr_task = io_util::drain_bounded_with_emitter(
        child.stderr.take(),
        HARNESS_OUTPUT_TAIL_BYTES,
        stderr_emitter,
    );

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
                reason = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
                    finish_cancelled(reason, stdout_task, stderr_task).await
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
                reason = wait_for_abort(&mut abort_rx) => {
                    graceful_shutdown(&mut child).await;
                    finish_cancelled(reason, stdout_task, stderr_task).await
                }
            }
        }
    }
}

/// Turn a tripped cancel channel into the matching [`WaitResult`] after the
/// kill ladder has already run.
///
/// - [`CancelReason::Aborted`] → [`WaitResult::Aborted`]. The drain tasks are
///   aborted rather than awaited: a harness that spawned a grandchild
///   inheriting stdout/stderr leaves those pipes open past SIGKILL (the
///   grandchild reparents to init) and the drain loop would block on `read`
///   until it exits. `Aborted` carries no output, so dropping the tasks
///   loses nothing.
/// - [`CancelReason::Skipped`] → [`WaitResult::Skipped`]. We *do* want the
///   partial output for the skipped attempt's log row, but we still can't
///   block indefinitely on a reparented grandchild's pipe — so the drain is
///   best-effort with a short timeout, then the tasks are aborted.
async fn finish_cancelled(
    reason: CancelReason,
    stdout_task: tokio::task::JoinHandle<Vec<u8>>,
    stderr_task: tokio::task::JoinHandle<Vec<u8>>,
) -> WaitResult {
    match reason {
        CancelReason::Aborted => {
            stdout_task.abort();
            stderr_task.abort();
            WaitResult::Aborted
        }
        CancelReason::Skipped => {
            // Best-effort: collect whatever the drainers captured before the
            // kill, but cap the wait so a grandchild holding the pipe open
            // can't wedge the skip path.
            let grace = Duration::from_millis(500);
            let stdout = tokio::time::timeout(grace, io_util::join_drain_string(stdout_task))
                .await
                .unwrap_or_default();
            let stderr = tokio::time::timeout(grace, io_util::join_drain_string(stderr_task))
                .await
                .unwrap_or_default();
            WaitResult::Skipped { stdout, stderr }
        }
    }
}

/// Block until the cancel watch channel is tripped, returning *why*.
///
/// The reason distinguishes a whole-run abort ([`CancelReason::Aborted`],
/// Ctrl+C) from a single-step skip ([`CancelReason::Skipped`],
/// `ralph skip`). Both drive the same kill ladder; the caller branches on
/// the returned reason to decide what to do *after* the child is dead.
async fn wait_for_abort(rx: &mut watch::Receiver<CancelState>) -> CancelReason {
    // If already cancelled, return immediately.
    if let Some(reason) = *rx.borrow() {
        return reason;
    }
    // Wait for a change that sets a cancel reason.
    loop {
        if rx.changed().await.is_err() {
            // Sender dropped — treat as "never cancel" by pending forever.
            std::future::pending::<()>().await;
        }
        if let Some(reason) = *rx.borrow() {
            return reason;
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

/// Set the attempt count for a step to an absolute value.
fn set_step_attempts(conn: &Connection, step_id: &str, attempts: i32) -> Result<()> {
    conn.execute(
        "UPDATE steps SET attempts = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        rusqlite::params![attempts, step_id],
    ).context("Failed to update step attempts")?;
    Ok(())
}

/// Finalize an attempt that was cancelled *before* the harness ran (the
/// cancel flag was already set when we checked it at the top of the loop).
///
/// Persists the bumped attempt count, drops an execution-log row (so the DB
/// reflects the same attempt number `StepResult` reports and the cancel has a
/// visible audit trail), flips the step status, and returns the matching
/// outcome:
///
/// - [`CancelReason::Aborted`] → step `Aborted`, log `UserInterrupted`,
///   outcome [`StepOutcome::Aborted`] (the runner ends the whole run).
/// - [`CancelReason::Skipped`] → step `Skipped`, log `UserSkipped`, outcome
///   [`StepOutcome::Skipped`] (the runner advances to the next step).
///
/// `committed` is always `false` here — no work ran. Change-handling
/// (stash/commit/discard) for the skip case lands in steps 17-18.
fn finalize_precancel(
    conn: &Connection,
    step_id: &str,
    attempt: i32,
    reason: CancelReason,
) -> Result<StepResult> {
    let (step_status, term_reason, outcome) = match reason {
        CancelReason::Aborted => (
            StepStatus::Aborted,
            TerminationReason::UserInterrupted,
            StepOutcome::Aborted,
        ),
        CancelReason::Skipped => (
            StepStatus::Skipped,
            TerminationReason::UserSkipped,
            StepOutcome::Skipped,
        ),
    };
    set_step_attempts(conn, step_id, attempt)?;
    let exec_log = storage::create_execution_log(conn, step_id, attempt, None, None)?;
    storage::update_execution_log(
        conn,
        exec_log.id,
        Some(0.0),
        None,
        &[],
        false, // not committed — no work ran (steps 17-18 add change-handling)
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(term_reason),
        Some(TestStatus::NotRun),
    )?;
    storage::update_step_status(conn, step_id, step_status)?;
    Ok(StepResult {
        outcome,
        step_id: step_id.to_string(),
        attempts_used: attempt,
        commit_hash: None,
    })
}

/// Print the per-attempt start header to stderr:
/// `  -> attempt N/M [started YYYY-MM-DD HH:MM:SS ZONE]`.
///
/// Skipped in NDJSON mode — the JSON consumer already sees `StepStarted`
/// and the new `PromptPrepared` event.
fn render_attempt_header(
    exec_opts: &ExecuteOptions,
    config: &Config,
    harness_name: &str,
    harness_config: &crate::config::HarnessConfig,
    attempt: i32,
    max_attempts: i32,
) {
    if exec_opts.json_output {
        return;
    }
    use std::str::FromStr;
    // Config::load_or_create_config validated the zone up front, so parse
    // failure here means the user edited config.json under us. Fall back
    // to UTC silently rather than blow up the run.
    let tz = chrono_tz::Tz::from_str(&config.display_timezone).unwrap_or(chrono_tz::UTC);
    let now_str = crate::output::format_now_in_tz(&tz);
    let harness_label = crate::output::format_harness_label_with_override(
        harness_name,
        harness_config.color.as_deref(),
        exec_opts.color,
    );
    eprintln!(
        "  -> {} attempt {}/{} [started {}]",
        harness_label, attempt, max_attempts, now_str
    );
}

/// Emit the prompt preview (truncated to 512 chars unless `--verbose`) or,
/// in NDJSON mode, a `PromptPrepared` event with the full char count and a
/// fixed-length preview.
fn render_prompt_preview(
    exec_opts: &ExecuteOptions,
    step: &Step,
    attempt: i32,
    prompt_text: &str,
) -> Result<()> {
    if exec_opts.json_output {
        let total_chars = prompt_text.chars().count();
        let preview: String = prompt_text.chars().take(PROMPT_PREVIEW_CHARS).collect();
        crate::output::emit_ndjson(&crate::output::RunEvent::PromptPrepared {
            step_id: step.id.clone(),
            attempt,
            prompt_chars: total_chars,
            prompt_preview: preview,
        })?;
        return Ok(());
    }

    let mut stderr = std::io::stderr().lock();
    render_prompt_preview_to(&mut stderr, exec_opts.verbose, prompt_text)?;
    Ok(())
}

/// Testable core of [`render_prompt_preview`]: write the human-readable
/// preview section to `writer`, truncating to 512 chars unless `verbose`.
fn render_prompt_preview_to<W: std::io::Write>(
    writer: &mut W,
    verbose: bool,
    prompt_text: &str,
) -> std::io::Result<()> {
    let total_chars = prompt_text.chars().count();
    let (shown, truncated) = if verbose || total_chars <= PROMPT_PREVIEW_CHARS {
        (prompt_text.to_string(), false)
    } else {
        let slice: String = prompt_text.chars().take(PROMPT_PREVIEW_CHARS).collect();
        (slice, true)
    };
    let shown_chars = shown.chars().count();

    writeln!(
        writer,
        "  Prompt ({} of {} chars):",
        shown_chars, total_chars
    )?;
    for raw in shown.lines() {
        // Drop trailing whitespace per line for visual cleanliness; empty
        // lines are preserved so paragraph breaks survive the indent.
        let trimmed = raw.trim_end();
        writeln!(writer, "    {trimmed}")?;
    }
    if truncated {
        writeln!(
            writer,
            "    [truncated — re-run with --verbose to see the full prompt]"
        )?;
    }
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

    // -- Prompt preview rendering ------------------------------------------

    #[test]
    fn test_prompt_display_truncates_to_512_by_default() {
        // Build a prompt with 1024 distinct chars.
        let prompt: String = (0..1024).map(|i| ((i % 26) as u8 + b'a') as char).collect();
        let mut buf: Vec<u8> = Vec::new();
        render_prompt_preview_to(&mut buf, false, &prompt).unwrap();
        let out = String::from_utf8(buf).unwrap();
        // Header records the character counts.
        assert!(
            out.contains("Prompt (512 of 1024 chars):"),
            "expected '(512 of 1024 chars)', got: {out}"
        );
        // Truncation marker appears.
        assert!(
            out.contains("[truncated"),
            "expected truncation marker, got: {out}"
        );
        // Preview never contains the 600th character (`(600 % 26) + 'a' = 'y'`).
        // Stronger check: the body between the header and the marker must
        // not exceed 512 chars of content plus the 4-space indent.
    }

    #[test]
    fn test_prompt_display_full_with_verbose() {
        let prompt = "first line\nsecond line\n";
        let mut buf: Vec<u8> = Vec::new();
        render_prompt_preview_to(&mut buf, true, prompt).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("Prompt ("));
        // No truncation marker when verbose is set.
        assert!(!out.contains("[truncated"));
        // Body preserved with 4-space indent.
        assert!(out.contains("    first line"));
        assert!(out.contains("    second line"));
    }

    #[test]
    fn test_prompt_display_short_prompt_no_truncation_marker() {
        // A short prompt (<=512 chars) must not show the truncation marker
        // even without --verbose.
        let prompt = "tiny prompt";
        let mut buf: Vec<u8> = Vec::new();
        render_prompt_preview_to(&mut buf, false, prompt).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("Prompt (11 of 11 chars):"));
        assert!(!out.contains("[truncated"));
    }

    #[test]
    fn test_prompt_display_strips_trailing_whitespace() {
        let prompt = "line one   \nline two\t\t\n";
        let mut buf: Vec<u8> = Vec::new();
        render_prompt_preview_to(&mut buf, true, prompt).unwrap();
        let out = String::from_utf8(buf).unwrap();
        // No trailing spaces/tabs on rendered lines.
        assert!(out.contains("    line one\n"));
        assert!(out.contains("    line two\n"));
        assert!(!out.contains("    line one   "));
    }

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
    fn test_step_outcome_variants() {
        // Ensure all variants are constructible.
        let outcomes = [
            StepOutcome::Success,
            StepOutcome::Failed,
            StepOutcome::Aborted,
            StepOutcome::Timeout,
            StepOutcome::PausedForQuestion,
        ];
        assert_eq!(outcomes.len(), 5);
        assert_eq!(StepOutcome::Success, StepOutcome::Success);
        assert_ne!(StepOutcome::Success, StepOutcome::Failed);
        assert_ne!(StepOutcome::Success, StepOutcome::PausedForQuestion);
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
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(step.attempts, 0);

        super::set_step_attempts(&conn, &step.id, 3).unwrap();
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.attempts, 3);
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
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(step.attempts, 0);

        let (tx, rx) = watch::channel(None);
        tx.send(Some(crate::signal::CancelReason::Aborted)).unwrap();

        let config = Config::default();
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 120,
        };

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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

        let (_tx, rx) = watch::channel(None);

        let config = Config::default();
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
        let script = format!("#!/bin/sh\nyes | head -c {bytes}\n{touch}exit 0\n",);
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
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, 500_000, true);

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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        // Cap the whole test at 30s so a regression hangs fast rather than
        // stalling the suite forever.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
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
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, bytes, false);

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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let _result = tokio::time::timeout(
            Duration::from_secs(60),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (tx, rx) = watch::channel(None);

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
            let _ = tx.send(Some(crate::signal::CancelReason::Aborted));
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
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

    /// STEP 16: a `Skipped` cancel reason must (a) kill the in-flight
    /// harness child via the SAME ladder Ctrl+C uses, (b) leave the step
    /// `Skipped` (not `Aborted`), and (c) write an execution_logs row with
    /// `termination_reason = user_skipped` and `committed = false` — while
    /// the run as a whole is NOT torn down (StepOutcome::Skipped, distinct
    /// from StepOutcome::Aborted which the runner turns into a whole-run
    /// abort).
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_skip_kills_harness_and_marks_skipped() {
        use crate::config::HarnessConfig;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let shared = TempDir::new().unwrap();
        let pid_path = shared.path().join("pid.txt");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("skip-harness.sh");
        // Record our pid, then block for a long time so the skip lands
        // while the harness is mid-flight.
        let script = format!(
            "#!/bin/sh\necho \"$$\" > {pid}\nsleep 60\n",
            pid = pid_path.to_string_lossy(),
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
            Some("skip"),
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
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "skip".to_string(),
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (tx, rx) = watch::channel(None);

        // Once the harness has written its pid, trip the cancel channel
        // with `Skipped` (NOT `Aborted`) — same channel, same ladder.
        let pid_path_clone = pid_path.clone();
        let skip_task = tokio::spawn(async move {
            for _ in 0..60 {
                if pid_path_clone.exists()
                    && fs::read_to_string(&pid_path_clone)
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = tx.send(Some(CancelReason::Skipped));
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
        )
        .await
        .expect("execute_step did not return within 10s on skip")
        .unwrap();

        skip_task.await.ok();

        // (Aborted vs Skipped kept distinct) — a skip must NOT surface as
        // Aborted (which the runner would turn into a whole-run abort).
        assert_eq!(
            result.outcome,
            StepOutcome::Skipped,
            "skip must yield StepOutcome::Skipped, not Aborted"
        );
        assert_ne!(result.outcome, StepOutcome::Aborted);

        // (a) the harness child was killed.
        let harness_pid: i32 = fs::read_to_string(&pid_path)
            .expect("pid file should exist")
            .trim()
            .parse()
            .expect("pid should parse");
        let mut alive = true;
        for _ in 0..40 {
            // SAFETY: libc::kill with signo=0 is a pure liveness probe.
            let r = unsafe { libc::kill(harness_pid, 0) };
            if r != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !alive,
            "harness (pid {harness_pid}) survived the skip — \
             the cancel ladder did not fire"
        );

        // (b) the step is Skipped (not Aborted).
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Skipped);
        assert_ne!(updated.status, StepStatus::Aborted);

        // (c) an execution_logs row exists with termination_reason
        //     user_skipped and committed = false.
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one execution_log row for the skip");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped),
            "skip must record user_skipped, not user_interrupted",
        );
        assert!(
            !logs[0].committed,
            "no work was kept on the skip path (committed must be false)"
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (tx, rx) = watch::channel(None);

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
            let _ = tx.send(Some(crate::signal::CancelReason::Aborted));
        });

        // Whole test capped at 10s — if the grandchild survives we want a
        // quick failure rather than a stalled suite.
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(
                &conn,
                &plan,
                &step,
                &config,
                &dir,
                &hook_ctx,
                rx,
                ExecuteOptions::default(),
            ),
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
            prompt_input: crate::config::PromptInputMode::Stdin,
            argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
            color: None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
        // Plain "no_changes" path: the agent didn't commit, so the
        // diagnostic hint should NOT fire.
        assert!(
            !logs[0]
                .test_results
                .iter()
                .any(|s| s.contains("HEAD advanced")),
            "vanilla no_changes should not surface the agent-committed hint",
        );
    }

    /// Required policy + harness commits on its own (clean worktree, but
    /// HEAD advanced) → step still Failed with NoChanges, but the execution
    /// log carries the agent-committed diagnostic so the user sees *why*
    /// rather than a generic `no_changes`. Regression guard for the
    /// commit-ownership-contract discoverability fix.
    #[tokio::test(flavor = "current_thread")]
    async fn test_required_step_agent_committed_emits_hint() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Capture HEAD so we can verify it actually moved.
        let head_before = crate::git::get_commit_hash(&dir).unwrap();

        // Harness that touches a file, stages it, commits it, then exits 0
        // — exactly the "Commit when done" failure mode the hint targets.
        let harness_tmp = TempDir::new().unwrap();
        let script = format!(
            "#!/bin/sh\n\
             cd {0}\n\
             touch agent-commit-test.txt\n\
             git add -A\n\
             git -c user.email=agent@test -c user.name=agent commit -m 'agent commit' >/dev/null\n\
             exit 0\n",
            dir.to_string_lossy()
        );
        let harness_path = harness_tmp.path().join("commit-harness.sh");
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
            Some("poly"),
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        // HEAD must actually have advanced — otherwise the hint can't fire
        // and we'd be testing nothing.
        let head_after = crate::git::get_commit_hash(&dir).unwrap();
        assert_ne!(head_before, head_after, "harness should have committed");

        assert_eq!(result.outcome, StepOutcome::Failed);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::NoChanges),
            "machine-readable termination_reason stays NoChanges for hook compat",
        );
        assert!(
            logs[0]
                .test_results
                .iter()
                .any(|s| s.contains("HEAD advanced")
                    && s.contains("agent appears to have committed")),
            "test_results should carry the agent-committed diagnostic; got: {:?}",
            logs[0].test_results,
        );
    }

    /// Write a harness script that touches a file, stages it, commits it,
    /// then exits 0 — the "agent committed on its own, leaving a clean
    /// worktree" failure mode the no_changes hint targets. Used by both
    /// the Required and Optional regression tests so the harness behavior
    /// stays identical across them.
    #[cfg(test)]
    fn write_agent_committing_harness(
        outside_dir: &std::path::Path,
        workdir: &std::path::Path,
    ) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let script = format!(
            "#!/bin/sh\n\
             cd {0}\n\
             touch agent-commit-test.txt\n\
             git add -A\n\
             git -c user.email=agent@test -c user.name=agent commit -m 'agent commit' >/dev/null\n\
             exit 0\n",
            workdir.to_string_lossy()
        );
        let path = outside_dir.join("commit-harness.sh");
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    /// Optional policy + no tests configured + harness commits on its own
    /// → step Failed with NoChanges + the agent-committed diagnostic.
    /// Codex review caught that the prior Optional success branch silently
    /// swallowed this case, recording `committed=false` while HEAD had
    /// advanced. The fix moves the commit-ownership check ahead of the
    /// policy branches; this test guards the regression.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_agent_committed_no_tests_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let head_before = crate::git::get_commit_hash(&dir).unwrap();

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_agent_committing_harness(harness_tmp.path(), &dir);

        let conn = crate::db::open_memory().unwrap();
        // No deterministic tests configured.
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        let head_after = crate::git::get_commit_hash(&dir).unwrap();
        assert_ne!(head_before, head_after, "harness should have committed");

        assert_eq!(
            result.outcome,
            StepOutcome::Failed,
            "Optional + agent-committed must fail, not silently succeed",
        );
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::NoChanges),
            "agent_committed_clean classifies as NoChanges regardless of policy",
        );
        assert!(
            logs[0]
                .test_results
                .iter()
                .any(|s| s.contains("HEAD advanced")
                    && s.contains("agent appears to have committed")),
            "diagnostic must surface in execution log; got: {:?}",
            logs[0].test_results,
        );
        assert!(!logs[0].committed);
    }

    /// Optional policy + tests configured and passing + harness commits on
    /// its own → step still Failed with NoChanges + the agent-committed
    /// diagnostic. Even though the tests passed on the post-agent-commit
    /// tree, the commit-ownership invariant takes precedence: a "passing"
    /// step with `committed=false` while HEAD advanced is the broken
    /// provenance state the fix is preventing.
    #[tokio::test(flavor = "current_thread")]
    async fn test_optional_step_agent_committed_passing_tests_fails() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let head_before = crate::git::get_commit_hash(&dir).unwrap();

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_agent_committing_harness(harness_tmp.path(), &dir);

        let conn = crate::db::open_memory().unwrap();
        // Deterministic test that always passes — to simulate the
        // "agent committed but tests are green on the new tree" path.
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        let head_after = crate::git::get_commit_hash(&dir).unwrap();
        assert_ne!(head_before, head_after, "harness should have committed");

        assert_eq!(
            result.outcome,
            StepOutcome::Failed,
            "Optional + agent-committed must fail even when tests pass",
        );
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::NoChanges),
        );
        assert!(
            logs[0]
                .test_results
                .iter()
                .any(|s| s.contains("HEAD advanced")
                    && s.contains("agent appears to have committed")),
            "diagnostic must lead the test_results vec; got: {:?}",
            logs[0].test_results,
        );
        assert!(!logs[0].committed);
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(result.commit_hash.is_none());

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(!logs[0].committed);
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
            None,
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
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
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
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "exit1".to_string(),
            harness_config_for_script(&harness_path),
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "exit1".to_string(),
            harness_config_for_script(&harness_path),
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "exit1".to_string(),
            harness_config_for_script(&harness_path),
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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
            None,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "exit1".to_string(),
            harness_config_for_script(&harness_path),
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
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

    // -------------------------------------------------------------------
    // HarnessChunk emission (TUI-plan §13.1)
    // -------------------------------------------------------------------

    /// `build_chunk_emitters_with_sink` should produce a `Some(...)`
    /// emitter pair when given a config and `(None, None)` when the config
    /// is absent. This is the wiring contract: `wait_with_timeout_and_abort`
    /// only emits when its emitters are populated.
    #[test]
    fn test_build_chunk_emitters_returns_none_when_cfg_is_none() {
        let dummy_sink: io_util::ChunkSink = Arc::new(|_, _, _| {});
        let (out, err) = build_chunk_emitters_with_sink(None, dummy_sink);
        assert!(out.is_none(), "stdout emitter should be None");
        assert!(err.is_none(), "stderr emitter should be None");
    }

    #[test]
    fn test_build_chunk_emitters_carries_seq_and_max_bytes() {
        let seq = Arc::new(AtomicU64::new(0));
        let cfg = Some(ChunkEmitConfig {
            seq: seq.clone(),
            max_bytes: 128,
        });
        let dummy_sink: io_util::ChunkSink = Arc::new(|_, _, _| {});
        let (out, err) = build_chunk_emitters_with_sink(cfg, dummy_sink);
        let out = out.expect("stdout emitter should be Some");
        let err = err.expect("stderr emitter should be Some");
        assert_eq!(out.stream, ChunkStream::Stdout);
        assert_eq!(err.stream, ChunkStream::Stderr);
        assert_eq!(out.max_bytes, 128);
        assert_eq!(err.max_bytes, 128);
        // Both emitters must reference the *same* counter so seq is
        // monotonic across streams.
        assert!(
            Arc::ptr_eq(&out.seq, &err.seq),
            "stdout and stderr emitters must share the same seq counter"
        );
        assert!(
            Arc::ptr_eq(&out.seq, &seq),
            "emitters must reference the caller's counter"
        );
    }

    /// End-to-end: `wait_with_timeout_and_abort` driving a real subprocess
    /// that prints N stdout lines + M stderr lines must emit N+M
    /// `HarnessChunk` events with `seq` 0..N+M-1 (no gaps, no duplicates),
    /// and lines longer than `max_bytes` must be truncated.
    #[tokio::test(flavor = "current_thread")]
    async fn test_wait_with_timeout_and_abort_emits_harness_chunks() {
        use std::time::Duration;
        use tokio::process::Command;

        // Build a child that prints 3 stdout lines and 1 stderr line. The
        // last stdout line is 50 bytes long so we can verify truncation
        // when max_bytes < 50.
        let long_line = "x".repeat(50);
        let script = format!("echo line-one; echo line-two; echo {long_line}; echo err-one >&2");
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(&script)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdin(std::process::Stdio::null());
        let child = cmd.spawn().expect("spawn sh");

        let collected: Arc<std::sync::Mutex<Vec<(ChunkStream, String, u64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_for_sink = collected.clone();
        let sink: io_util::ChunkSink = Arc::new(move |stream, text, seq| {
            collected_for_sink.lock().unwrap().push((stream, text, seq));
        });

        let max_bytes = 10;
        let cfg = Some(ChunkEmitConfig {
            seq: Arc::new(AtomicU64::new(0)),
            max_bytes,
        });
        let emitters = build_chunk_emitters_with_sink(cfg, sink);

        let (_tx, rx) = watch::channel(None);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            wait_with_timeout_and_abort(child, None, rx, emitters),
        )
        .await
        .expect("wait timed out");

        match result {
            WaitResult::Completed(Ok(_)) => {}
            WaitResult::Completed(Err(e)) => panic!("harness errored: {e}"),
            WaitResult::Timeout { .. } => panic!("unexpected Timeout"),
            WaitResult::Aborted => panic!("unexpected Aborted"),
            WaitResult::Skipped { .. } => panic!("unexpected Skipped"),
        }

        let mut events = collected.lock().unwrap().clone();
        events.sort_by_key(|e| e.2);

        // 4 lines total → 4 events with seq 0..3 (no gaps).
        assert_eq!(events.len(), 4, "expected 4 events, got {events:?}");
        let seqs: Vec<u64> = events.iter().map(|e| e.2).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3], "seq must be 0..N-1");

        // Every emitted text is at most max_bytes bytes.
        for (_, text, _) in &events {
            assert!(
                text.len() <= max_bytes,
                "text exceeded max_bytes: {} bytes ({text:?})",
                text.len(),
            );
        }

        // Verify the long line was truncated and the stderr line landed
        // with the Stderr label.
        let texts: Vec<&str> = events.iter().map(|(_, t, _)| t.as_str()).collect();
        assert!(
            texts.contains(&"line-one"),
            "expected 'line-one' in events: {texts:?}"
        );
        assert!(
            texts.contains(&"line-two"),
            "expected 'line-two' in events: {texts:?}"
        );
        assert!(
            texts
                .iter()
                .any(|t| t.starts_with('x') && t.len() == max_bytes),
            "expected truncated long line in events: {texts:?}"
        );
        assert!(
            events
                .iter()
                .any(|(s, t, _)| *s == ChunkStream::Stderr && t == "err-one"),
            "expected stderr 'err-one' event: {events:?}"
        );
    }

    /// Sanity: when emitters are `(None, None)`, the wait function must
    /// still capture stdout/stderr correctly — the chunk-emit path is
    /// strictly additive and must not regress the tail-capture contract.
    #[tokio::test(flavor = "current_thread")]
    async fn test_wait_with_timeout_and_abort_no_emit_when_emitters_none() {
        use std::time::Duration;
        use tokio::process::Command;

        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("echo captured; echo err-captured >&2")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdin(std::process::Stdio::null());
        let child = cmd.spawn().expect("spawn sh");

        let (_tx, rx) = watch::channel(None);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            wait_with_timeout_and_abort(child, None, rx, (None, None)),
        )
        .await
        .expect("wait timed out");

        match result {
            WaitResult::Completed(Ok(out)) => {
                assert!(out.success);
                assert!(out.stdout.contains("captured"));
                assert!(out.stderr.contains("err-captured"));
            }
            WaitResult::Completed(Err(e)) => panic!("harness errored: {e}"),
            WaitResult::Timeout { .. } => panic!("unexpected Timeout"),
            WaitResult::Aborted => panic!("unexpected Aborted"),
            WaitResult::Skipped { .. } => panic!("unexpected Skipped"),
        }
    }

    // -------------------------------------------------------------------
    // Question pause integration (TUI-plan §17 step 42)
    // -------------------------------------------------------------------

    /// Insert an unanswered `step_questions` row tagged to a given (step,
    /// attempt). Simulates what the harness would do via `ralph question
    /// ask`. Used by the question-pause integration tests to drive the
    /// "harness left a question behind" branch in `execute_step`.
    #[cfg(test)]
    fn insert_unanswered_question(conn: &Connection, step_id: &str, attempt: i32, question: &str) {
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, asked_at)
             VALUES (?1, ?2, ?3, ?4, '[]', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            rusqlite::params![uuid::Uuid::new_v4().to_string(), step_id, attempt, question],
        )
        .expect("seed step_questions row");
    }

    /// Build a minimal Config registering the given harness path under `name`.
    #[cfg(test)]
    fn config_with_harness(name: &str, harness_path: &std::path::Path) -> Config {
        use crate::config::HarnessConfig;
        let mut config = Config::default();
        config.harnesses.insert(
            name.to_string(),
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
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );
        config
    }

    /// Pause path with a clean-exit, no-diff harness. The harness runs cleanly
    /// but a `step_questions` row exists for (step, attempt=1). Expected:
    /// outcome PausedForQuestion, step status reset to Pending,
    /// step.attempts ticked to 1, exec_log row carries paused_for_question +
    /// NotRun, no commit was made.
    #[tokio::test(flavor = "current_thread")]
    async fn test_paused_for_question_no_diff_skips_tests_and_commit() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let head_before = crate::git::get_commit_hash(&dir).unwrap();

        // Noop harness — clean exit, no diff.
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
            // Configure deterministic tests so we can assert they were skipped
            // — pause must skip the test phase even when tests are configured.
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
            Some(2), // budget > 1 to confirm pause does not retry
            None,
            None,
            None,
        )
        .unwrap();

        // Simulate `ralph question ask` writing a row before the harness
        // exits — execute_step will bump step.attempts to 1 then query.
        insert_unanswered_question(&conn, &step.id, 1, "Use SQLite or Postgres?");

        let config = config_with_harness("noop", &harness_path);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        assert_eq!(result.attempts_used, 1);
        assert!(result.commit_hash.is_none());

        // Step status returned to Pending so a re-run picks it up cleanly,
        // and attempts ticked once.
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Pending);
        assert_eq!(updated.attempts, 1);

        // Exactly one log row, carrying the pause termination reason and
        // NotRun test status (tests must NOT have run).
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].attempt, 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::PausedForQuestion)
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
        assert!(
            logs[0].test_results.is_empty(),
            "no tests ran on a paused attempt; test_results must be empty"
        );
        assert!(!logs[0].committed, "pause must not commit");
        assert!(!logs[0].rolled_back, "no diff means nothing to roll back");

        // HEAD did not advance — pause skipped the commit.
        let head_after = crate::git::get_commit_hash(&dir).unwrap();
        assert_eq!(head_before, head_after, "pause must not advance HEAD");

        // The plan's effective status is now Question (derived) even though
        // the underlying plans.status column may still be in_progress.
        let effective = storage::plan_effective_status(&conn, &plan.id).unwrap();
        assert_eq!(effective, crate::plan::PlanStatus::Question);
    }

    /// Pause path with a harness that produced a diff. The diff must be
    /// rolled back as part of the pause finalize, leaving the workdir clean.
    #[tokio::test(flavor = "current_thread")]
    async fn test_paused_for_question_rolls_back_diff() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Harness that writes a file inside the workdir, then exits 0.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, 64, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("touchy"),
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
            None,
        )
        .unwrap();

        insert_unanswered_question(&conn, &step.id, 1, "What name should I use?");

        let config = config_with_harness("touchy", &harness_path);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);

        // Workdir must be clean after pause: the file the harness created
        // was rolled back, and no commit was made.
        assert!(
            !crate::git::has_uncommitted_changes(&dir).unwrap(),
            "pause must roll back any harness-produced diff"
        );
        assert!(
            !dir.join("ralph-test-output.txt").exists(),
            "rolled-back path: harness's file must be gone"
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::PausedForQuestion)
        );
        assert!(logs[0].rolled_back, "rolled_back flag must be set");
        assert!(!logs[0].committed);
    }

    /// Happy path regression: a clean run on a question-enabled plan that
    /// happens to leave NO question rows behind must proceed normally
    /// (commit, success). Confirms the question check is purely additive.
    #[tokio::test(flavor = "current_thread")]
    async fn test_no_questions_proceeds_to_commit_normally() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Harness writes a file so the commit path is exercised.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, 64, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("happy"),
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
            None,
        )
        .unwrap();

        // No `step_questions` row inserted — happy path.

        let config = config_with_harness("happy", &harness_path);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(result.commit_hash.is_some(), "no-question path must commit");

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
        assert!(logs[0].committed);

        // And the plan's effective status reflects the actual stored value
        // — no Question shadow when there are no unanswered rows.
        let effective = storage::plan_effective_status(&conn, &plan.id).unwrap();
        assert_ne!(effective, crate::plan::PlanStatus::Question);
    }

    /// A question row tagged to a *different* attempt (e.g. left over from a
    /// prior, already-answered attempt) must NOT trigger a pause on the
    /// current attempt. The detector scopes by (step, attempt), so an
    /// orphan row at attempt=0 does not interfere with attempt=1's
    /// happy-path completion.
    #[tokio::test(flavor = "current_thread")]
    async fn test_question_for_different_attempt_does_not_pause() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_large_output_harness(harness_tmp.path(), &dir, 64, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "slug",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            Some("happy"),
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
            None,
        )
        .unwrap();

        // Pre-existing answered question on attempt 1 (the upcoming attempt
        // number) — answered rows must not pause. Insert it directly so the
        // helper, which writes unanswered rows, can't be repurposed here.
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, answer, asked_at, answered_at)
             VALUES ('prev', ?1, 1, 'old?', '[]', 'yes', '2026-05-01T10:00:00.000Z', '2026-05-01T11:00:00.000Z')",
            rusqlite::params![&step.id],
        ).unwrap();

        let config = config_with_harness("happy", &harness_path);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(
            &conn,
            &plan,
            &step,
            &config,
            &dir,
            &hook_ctx,
            rx,
            ExecuteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "answered rows must not pause — got {result:?}",
        );
    }
}
