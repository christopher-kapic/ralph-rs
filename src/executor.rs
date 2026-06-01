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
use crate::plan::{
    ChangePolicy, InterruptionKind, InterruptionOption, Phase, Plan, Step, StepStatus,
    TerminationReason, TestStatus,
};
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
// Phase B — auto-blocker on retry exhaustion
// ---------------------------------------------------------------------------

/// Priority-1 option on the auto-raised retry-exhausted blocker — "the human
/// wants a fresh shot." Phase C's resolution handler will detect this exact
/// string to wire the "reset attempts → re-queue" path. Kept as a `pub const`
/// so executor (writer), `commands/run.rs` (Phase C reader), and the TUI all
/// reference one source of truth and `cargo test --lib` catches any drift via
/// the assertions in [`tests`].
pub const RETRY_EXHAUSTED_OPTION_RETRY: &str = "Retry step with parked changes";

/// Priority-2 option on the auto-raised retry-exhausted blocker — the
/// "explicit give-up" fallback. Phase C's resolution handler detects this
/// exact string and flips the step to `StepStatus::Failed`, mirroring the
/// pre-Phase-B terminal behavior. Constant kept `pub` for the same reason as
/// [`RETRY_EXHAUSTED_OPTION_RETRY`].
pub const RETRY_EXHAUSTED_OPTION_FAIL: &str = "Mark step Failed";

/// Soft cap on the body length of the auto-raised retry-exhausted blocker.
/// The inbox UI renders the body verbatim, so a runaway test-output dump
/// (megabytes of stack traces, harness JSON, …) hurts navigation. 8 KiB is
/// generous for a single failing-test summary plus hook stderr while keeping
/// the inbox usable. Truncation is byte-bounded with a tail elision marker
/// so the most recent (and usually most relevant) lines survive.
const RETRY_EXHAUSTED_BODY_MAX_BYTES: usize = 8 * 1024;

/// Truncate `text` to `max_bytes`, keeping the **tail** and prefixing an
/// elision marker. Chosen over a head-keeping truncation because the *last*
/// lines of a test/hook output usually carry the actual failure (assertion
/// text, exit-code line) — head-keeping would lose them. UTF-8-safe via
/// `char_indices()`: we never slice mid-codepoint.
fn truncate_tail_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    // Find the largest start offset i such that `text.len() - i <= max_bytes`
    // AND i lies on a char boundary. Walk from the tail backwards.
    let target = text.len().saturating_sub(max_bytes);
    let mut cut = text.len();
    for (i, _) in text.char_indices() {
        if i >= target {
            cut = i;
            break;
        }
    }
    let elided_bytes = cut;
    format!(
        "... ({elided_bytes} bytes elided from head) ...\n{}",
        &text[cut..]
    )
}

/// Render a single test-command result for the execution-log `test_results`
/// vector (and, via that, the retry prompt's "Previous Test Output" section
/// and the retry-exhausted blocker body).
///
/// Passing commands stay terse (`cmd: pass`). Failing commands keep
/// `cmd: FAIL` as the FIRST line — so prefix-based consumers still parse —
/// then append the command's `output_tail` (already tail-bounded by the test
/// runner) so the retrying agent and the human triaging the blocker see the
/// actual assertion text / compiler error rather than a bare `FAIL`.
fn format_test_result_line(r: &test_runner::TestResult) -> String {
    if r.passed {
        format!("{}: pass", r.command)
    } else {
        let tail = r.output_tail.trim_end();
        if tail.is_empty() {
            format!("{}: FAIL", r.command)
        } else {
            format!("{}: FAIL\n{tail}", r.command)
        }
    }
}

/// Phase E Fix 5: build the retry-exhausted auto-blocker's body from the
/// last up-to-3 attempts in the step's CURRENT cycle. Returns the body
/// already truncated to fit within [`RETRY_EXHAUSTED_BODY_MAX_BYTES`].
///
/// Layout (final-first; the attempt the user is about to triage is on top):
/// ```text
/// Step failed after N attempts.
///
/// ### Attempt N (final)
/// <test_results joined for the persisted attempt N row>
/// (and the live `failure_output.test_results` if it carries additional
///  diagnostic the row's `test_results` didn't capture — typically commit-
///  hook stderr the executor merged via Phase A's `[Commit hook output]`
///  header)
///
/// ### Attempt N-1
/// <test_results joined for the persisted attempt N-1 row>
///
/// ### Attempt N-2
/// <test_results joined for the persisted attempt N-2 row>
/// ```
///
/// Each attempt's content is independently truncated to its share of the
/// budget (`(BUDGET - reserve_for_headers) / num_attempts`), so a single
/// noisy attempt can't crowd the others out of the body.
fn build_retry_exhausted_body(
    conn: &Connection,
    step_id: &str,
    max_attempts: i32,
    failure_reason: FailureReason,
    failure_output: &FailureOutput<'_>,
) -> String {
    const HEADER_RESERVE: usize = 256;

    // Resolve the step's current cycle, then take the last 3 attempts
    // whose `cycle_index` matches it. The auto-blocker is per-cycle: the
    // attempts the user must triage are the ones from THIS cycle, not
    // anything left over from a prior "Retry from scratch".
    let current_cycle: i32 = conn
        .query_row(
            "SELECT current_cycle_index FROM steps WHERE id = ?1",
            rusqlite::params![step_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let all_logs = storage::list_execution_logs_for_step(conn, step_id).unwrap_or_default();
    let mut cycle_logs: Vec<_> = all_logs
        .into_iter()
        .filter(|l| l.cycle_index == current_cycle)
        .collect();
    // `list_execution_logs_for_step` already orders by id ASC. Take the
    // last 3 chronologically.
    let n_logs = cycle_logs.len();
    let take_from = n_logs.saturating_sub(3);
    let last_logs: Vec<_> = cycle_logs.drain(take_from..).collect();
    let shown = last_logs.len().max(1);
    let per_attempt_cap = RETRY_EXHAUSTED_BODY_MAX_BYTES.saturating_sub(HEADER_RESERVE) / shown;

    let preamble = match failure_reason {
        FailureReason::TestFailed => format!("Step failed after {max_attempts} attempts.\n"),
        FailureReason::CommitFailed => format!(
            "Step failed after {max_attempts} attempts (last attempt's commit hooks rejected the change).\n",
        ),
        FailureReason::InsufficientDiskSpace => format!(
            "Step blocked: insufficient disk space (see attempt detail below). Free up disk \
             space and resolve with `{RETRY_EXHAUSTED_OPTION_RETRY}` to resume with the parked changes.\n",
        ),
        _ => format!("Step failed after {max_attempts} attempts.\n"),
    };
    let mut sections: Vec<String> = Vec::new();

    if last_logs.is_empty() {
        // Fall-back when no logs are persisted yet (defensive: the failing
        // attempt's row was updated just above, so this branch is unlikely).
        let body = failure_output.test_results.join("\n");
        sections.push(format!(
            "### Attempt {max_attempts} (final)\n{}",
            truncate_tail_bytes(&body, per_attempt_cap),
        ));
    } else {
        // Render newest first. `last_logs` is oldest→newest; reverse it.
        for (i, log) in last_logs.iter().enumerate().rev() {
            let suffix = if i == last_logs.len() - 1 {
                " (final)"
            } else {
                ""
            };
            // For the final attempt prefer the live `failure_output.test_results`
            // if the persisted row's `test_results` is empty — that can happen
            // when the row's update transaction is still in flight in some
            // edge cases (the surrounding `update_execution_log` above runs
            // first, so this is belt-and-braces).
            let raw = if i == last_logs.len() - 1 && log.test_results.is_empty() {
                failure_output.test_results.join("\n")
            } else {
                log.test_results.join("\n")
            };
            let trimmed = truncate_tail_bytes(&raw, per_attempt_cap);
            sections.push(format!("### Attempt {}{suffix}\n{trimmed}", log.attempt));
        }
    }

    let mut body = preamble;
    body.push('\n');
    body.push_str(&sections.join("\n\n"));
    // Final hard cap — even after per-attempt slicing the section count and
    // per-section overhead could in theory push us over; tail-truncate.
    truncate_tail_bytes(&body, RETRY_EXHAUSTED_BODY_MAX_BYTES)
}

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
    /// The harness called `ralph question ask` / `ralph block` during the
    /// attempt, leaving one or more open native `interruptions` rows. Both
    /// the test phase and the commit are skipped, any diff is rolled back,
    /// the step's branch is marked `Blocked` (derived), and **no retry
    /// budget is consumed** (docs/dag-redesign.md §3.4 / §9 invariant 4).
    /// The scheduler advances to another runnable branch; the plan only
    /// reports [`crate::plan::PlanStatus::Interrupted`] once the runnable
    /// set is exhausted, so a linear plan still pauses exactly as before.
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
    /// Set on a `Success` outcome when nondeterministic review is
    /// effective-enabled for this step (docs/dag-redesign.md §3.2-§3.3 /
    /// §9-inv-2). Carries `(commit_sha, iteration)` of the committed
    /// iteration the read-only reviewer must run against. When `Some`, the
    /// executor deliberately leaves the step `InProgress` (NOT `Complete`)
    /// with `review_status = Pending`: the step reaches `Complete` only
    /// after its review *returns* (§3.3), and its direct dependents stay
    /// non-runnable until then (`deps_satisfied` requires `Complete`). The
    /// runner spawns the concurrent review and finalizes the step.
    ///
    /// `None` for every step when review is not effective-enabled — i.e.
    /// the linear-plan / no-review-config path is byte-identical to before
    /// (the executor writes `Complete` exactly as today).
    pub needs_review: Option<(String, i32)>,
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
    /// True when the runner has just restored this step's previously parked
    /// interruption stash, so any untracked files now visible are step-owned
    /// WIP rather than pre-existing user files that should be preserved.
    pub resumed_parked_worktree: bool,
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
            resumed_parked_worktree: false,
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
    /// Tests failed after exhausting all attempts.
    TestFailed,
    /// Harness produced no changes after exhausting all attempts.
    NoChanges,
    /// Harness exited non-zero (or was killed by a signal) — tests never ran.
    HarnessFailed,
    /// `git commit` (or staging before it) failed — typically a pre-commit
    /// hook rejection (lint, format, policy). Recoverable within retry budget;
    /// surfaces via `TerminationReason::CommitFailed` and feeds hook output
    /// into `previous_failure_reason` for the next prompt.
    CommitFailed,
    /// Per-step disk-space gate breached: free disk dropped below
    /// `config.min_free_disk_mb` mid-run. Routed through
    /// [`raise_retry_exhausted_blocker`] as a recoverable auto-blocker so a
    /// transient FS hiccup doesn't permanently fail the step.
    InsufficientDiskSpace,
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
            Self::CommitFailed => "commit_failed",
            Self::InsufficientDiskSpace => "insufficient_disk_space",
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
/// The fields of a single [`write_phase`] call. `conn` stays a separate lead
/// argument (the DB handle); everything that describes *which* phase
/// transition to record on the run_locks row is bundled here.
struct PhaseWrite<'a> {
    plan: &'a Plan,
    step_id: &'a str,
    step_num: i32,
    attempt: i32,
    max_attempts: i32,
    execution_log_id: Option<i64>,
    phase: Phase,
    current_command: Option<&'a str>,
    child: ChildUpdate<'a>,
    json_output: bool,
}

fn write_phase(conn: &Connection, w: PhaseWrite<'_>) -> Result<()> {
    let PhaseWrite {
        plan,
        step_id,
        step_num,
        attempt,
        max_attempts,
        execution_log_id,
        phase,
        current_command,
        child,
        json_output,
    } = w;
    storage::update_live_phase(
        conn,
        &plan.project,
        phase,
        crate::storage::LivePhase {
            step_id: Some(step_id),
            step_num: Some(step_num),
            attempt: Some(attempt),
            max_attempts: Some(max_attempts),
            execution_log_id,
            current_command,
            child,
        },
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
/// The per-call inputs to [`finalize_failure`] (everything besides the
/// ambient [`ExecCtx`]): which execution-log row / attempt / duration, the
/// [`FailureReason`], optional harness output, and the explicit termination
/// reason + test status to persist.
struct FailureArgs<'a> {
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    reason: FailureReason,
    output: Option<&'a FailureOutput<'a>>,
    termination_reason: TerminationReason,
    test_status: TestStatus,
}

async fn finalize_failure(ctx: &ExecCtx<'_>, args: FailureArgs<'_>) -> Result<StepResult> {
    let FailureArgs {
        exec_log_id,
        duration_secs,
        attempt,
        reason,
        output,
        termination_reason,
        test_status,
    } = args;
    // Fix 3 (defensive, general): every non-skip terminal failure funnels
    // through here (Timeout, HarnessFailed, terminal test failure, the
    // `WaitResult::Aborted` arm). If a `Skipped` reason was pending but the
    // attempt finalized via one of these non-skip arms (e.g. a Skipped that
    // raced and lost the `select!` to a timeout), the global park-kind slot
    // + cancel channel would still carry it and bleed into the next
    // attempt/step. Clear it here. A no-op unless a stale `Skipped` is
    // latched, and it deliberately never disturbs a pending `Aborted` (the
    // legitimate whole-run shutdown must survive this call).
    crate::signal::clear_pending_skip_state();

    // Rollback any uncommitted changes, preserving pre-existing untracked files.
    let rolled_back = if git::has_uncommitted_changes(ctx.workdir)? {
        // Record the rollback phase before invoking git so an external
        // observer sees *why* the runner is touching the tree.
        write_phase(
            ctx.conn,
            PhaseWrite {
                plan: ctx.plan,
                step_id: &ctx.step.id,
                step_num: ctx.step_num,
                attempt,
                max_attempts: ctx.max_attempts,
                execution_log_id: Some(exec_log_id),
                phase: Phase::Rollback,
                current_command: None,
                child: ChildUpdate::Clear,
                json_output: ctx.json_output,
            },
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
            crate::storage::ExecutionLogUpdate {
                duration_secs: Some(duration_secs),
                diff: o.diff,
                test_results: o.test_results,
                rolled_back: o.has_changes,
                harness_stdout: Some(o.stdout),
                harness_stderr: Some(o.stderr),
                cost_usd: o.parsed.cost_usd,
                input_tokens: o.parsed.input_tokens,
                output_tokens: o.parsed.output_tokens,
                session_id: o.parsed.session_id.as_deref(),
                termination_reason: Some(termination_reason),
                test_status: Some(test_status),
                ..Default::default()
            },
        )?;
    } else {
        storage::update_execution_log(
            ctx.conn,
            exec_log_id,
            crate::storage::ExecutionLogUpdate {
                duration_secs: Some(duration_secs),
                rolled_back,
                termination_reason: Some(termination_reason),
                test_status: Some(test_status),
                ..Default::default()
            },
        )?;
    }

    storage::update_step_status(ctx.conn, &ctx.step.id, reason.to_step_status())?;
    write_phase(
        ctx.conn,
        PhaseWrite {
            plan: ctx.plan,
            step_id: &ctx.step.id,
            step_num: ctx.step_num,
            attempt,
            max_attempts: ctx.max_attempts,
            execution_log_id: Some(exec_log_id),
            phase: Phase::PostStepHook,
            current_command: None,
            child: ChildUpdate::Clear,
            json_output: ctx.json_output,
        },
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
        needs_review: None,
    })
}

/// True when the working tree has changes that are NOT entirely accounted
/// for by `pre_existing_untracked` — i.e. there is work the killed harness
/// produced (or modified) that is causally tied to this step.
///
/// A clean tree, or a tree whose only changes are files the user already
/// had untracked before the run started, returns `false`: nothing the skip
/// is responsible for, so parking would clobber the user's own scratch.
fn has_step_attributable_changes(
    workdir: &Path,
    pre_existing_untracked: &[String],
) -> Result<bool> {
    let changed = git::get_all_changed_files(workdir)?;
    Ok(changed.iter().any(|f| !pre_existing_untracked.contains(f)))
}

/// Undo an in-flight attempt that the TUI skip dialog *cancelled* (Esc),
/// without finalizing the step (STEP 18).
///
/// Reached only from the `WaitResult::Skipped` arm when the registry slot
/// carried [`crate::git::ParkStrategyKind::Cancel`]. The harness child is
/// already dead (the cancel ladder killed it before we got here). This
/// function makes the cancelled attempt a no-op from the step's point of
/// view:
///
/// 1. Roll back the killed harness's working-tree changes via
///    [`git::rollback_except`], preserving the user's `pre_existing_untracked`
///    scratch (same preservation rule as the `Discard` park strategy).
/// 2. Emit a [`crate::output::RunEvent::AttemptCancelled`] NDJSON event so
///    a subscribed TUI/log shipper knows the attempt was undone (and that
///    another attempt at the *same* number is coming).
/// 3. Delete the `execution_logs` row this attempt created — it was inserted
///    (with the prompt) *before* the harness spawned, so leaving it would
///    both leak a `UNIQUE(step_id, attempt)` row and make a later resume
///    think the budget was consumed.
/// 4. Clear the process cancel channel so the re-entered attempt isn't
///    immediately swept through `finalize_precancel`.
///
/// The caller (`execute_step`'s retry loop) then steps `attempt` back by one
/// and `continue`s, so the next loop iteration re-runs the *same* attempt
/// number — consuming no retry budget.
/// Build the [`crate::output::RunEvent::AttemptCancelled`] event the
/// executor emits when the TUI skip dialog's Esc/cancel path undoes an
/// in-flight attempt (step 18). Pure (no I/O) so the exact payload — field
/// shape, `step_num` derivation from the i32 `ctx.step_num` — is
/// unit-testable without capturing stdout.
fn attempt_cancelled_event(ctx: &ExecCtx<'_>, attempt: i32) -> crate::output::RunEvent {
    crate::output::RunEvent::AttemptCancelled {
        step_id: ctx.step.id.clone(),
        step_num: ctx.step_num.max(0) as usize,
        attempt,
        at: chrono::Utc::now(),
    }
}

fn cancel_skipped_attempt(ctx: &ExecCtx<'_>, exec_log_id: i64, attempt: i32) -> Result<()> {
    // 1. Roll back the killed harness's work, preserving the user's
    //    pre-existing untracked files. A clean tree makes this a no-op.
    if git::has_uncommitted_changes(ctx.workdir)? {
        git::rollback_except(ctx.workdir, ctx.pre_existing_untracked)
            .context("could not roll back cancelled skip attempt")?;
    }

    // 2. Emit the NDJSON event (best-effort: a dropped event must not break
    //    the run — the durable state is the absence of the log row).
    if ctx.json_output {
        let _ = crate::output::emit_ndjson(&attempt_cancelled_event(ctx, attempt));
    }

    // 3. Delete the execution_logs row this attempt created.
    storage::delete_execution_log(ctx.conn, exec_log_id)?;

    // 4. Reset the cancel channel so the re-entered attempt runs for real.
    crate::signal::clear_cancel_state();

    Ok(())
}

/// Finalize a step that was skipped while its harness was in-flight
/// (`WaitResult::Skipped`, reached only via the `ralph skip` → cancel-ladder
/// path). This is step 16's terminal-skip path, extended for step 17 to
/// *park* the killed harness's uncommitted work per the operator's
/// `--changes` choice instead of unconditionally rolling it back.
///
/// Exactly one `execution_logs` row is written here (reconciling with step
/// 16, which previously routed this case through `finalize_failure`): we no
/// longer call `finalize_failure` for the skip arm at all, so there is no
/// second row. `termination_reason` is always `UserSkipped`; `committed`
/// and `commit_hash` track the parked outcome:
///
/// - `Commit`  → `committed = true`, `commit_hash = <wip sha>`
/// - `Stash`   → `committed = false` (recoverable via the stash, not a commit)
/// - `Discard` → `committed = false`
/// - tree clean / only pre-existing untracked → no parking, `committed = false`
///
/// The park strategy is decided *once* by the caller (the
/// `WaitResult::Skipped` arm) from the process-global slot set by
/// `request_skip_in_flight`, and passed in as `kind`. A `None` slot (e.g. a
/// cross-process SIGTERM-style skip that couldn't set it) is resolved to
/// `Stash` by the caller so a skip never silently destroys work. Threading it
/// as an argument — rather than re-reading the global slot here — removes a
/// store/take race: under load the independent second read could observe the
/// slot before `request_skip_in_flight`'s store landed, silently falling back
/// to `Stash` (which, like `Discard`, also cleans the tree, so only the
/// `rolled_back` bookkeeping diverged — a subtle, load-dependent bug).
/// The per-call inputs to [`finalize_skipped`] besides the ambient
/// [`ExecCtx`]: the execution-log row / attempt / duration, the killed
/// harness's captured stdout+stderr, and the park strategy for its WIP.
struct SkippedArgs<'a> {
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    stdout: &'a str,
    stderr: &'a str,
    kind: crate::git::ParkStrategyKind,
}

async fn finalize_skipped(ctx: &ExecCtx<'_>, args: SkippedArgs<'_>) -> Result<StepResult> {
    let SkippedArgs {
        exec_log_id,
        duration_secs,
        attempt,
        stdout,
        stderr,
        kind,
    } = args;
    let parsed = parse_harness_json(stdout);

    // Capture the diff *before* any parking touches the tree so `ralph log`
    // retains what the skipped attempt produced.
    let had_changes = git::has_uncommitted_changes(ctx.workdir)?;
    let diff = if had_changes {
        Some(git::get_diff(ctx.workdir)?)
    } else {
        None
    };

    let park_relevant = has_step_attributable_changes(ctx.workdir, ctx.pre_existing_untracked)?;

    // Default terminal log shape: nothing committed, nothing rolled back.
    let mut committed = false;
    let mut commit_hash: Option<String> = None;
    let mut rolled_back = false;

    if park_relevant {
        // Record a rollback phase only for the discard strategy (the only
        // one that throws work away) so an external observer sees why the
        // tree is being touched; stash/commit are non-destructive.
        if kind == crate::git::ParkStrategyKind::Discard {
            write_phase(
                ctx.conn,
                PhaseWrite {
                    plan: ctx.plan,
                    step_id: &ctx.step.id,
                    step_num: ctx.step_num,
                    attempt,
                    max_attempts: ctx.max_attempts,
                    execution_log_id: Some(exec_log_id),
                    phase: Phase::Rollback,
                    current_command: None,
                    child: ChildUpdate::Clear,
                    json_output: ctx.json_output,
                },
            )?;
        }

        let strategy = match kind {
            crate::git::ParkStrategyKind::Stash => crate::git::ParkStrategy::Stash {
                label: format!(
                    "ralph-skip/{}/{}/{}",
                    ctx.plan.slug,
                    ctx.step_num,
                    // Millisecond resolution: a 1-second timestamp collides
                    // when the same step is skipped twice within a second,
                    // which would make label-based stash recovery
                    // (`find_stash_by_message`) match the wrong entry.
                    chrono::Utc::now().timestamp_millis()
                ),
            },
            crate::git::ParkStrategyKind::Commit => crate::git::ParkStrategy::Commit {
                subject: format!(
                    "[ralph wip] skipped step {}: {}",
                    ctx.step_num, ctx.step.title
                ),
            },
            crate::git::ParkStrategyKind::Discard => crate::git::ParkStrategy::Discard,
            // Unreachable: the `WaitResult::Skipped` arm peels off `Cancel`
            // (and re-enters the loop) *before* `finalize_skipped` is ever
            // called, so the registry slot can't carry `Cancel` here.
            // Fall back to Discard rather than panic so a future refactor
            // that lets it through still doesn't destroy nothing/lose work
            // silently — it just throws the killed harness's work away.
            crate::git::ParkStrategyKind::Cancel => crate::git::ParkStrategy::Discard,
        };

        let outcome = git::park_changes(
            ctx.workdir,
            strategy,
            ctx.pre_existing_untracked,
            &ctx.step.id,
        )?;

        match outcome {
            crate::git::ParkOutcome::Committed { sha } => {
                committed = true;
                commit_hash = Some(sha);
            }
            crate::git::ParkOutcome::Stashed { .. } => {}
            crate::git::ParkOutcome::Discarded => {
                rolled_back = true;
            }
        }
    }

    storage::update_execution_log(
        ctx.conn,
        exec_log_id,
        crate::storage::ExecutionLogUpdate {
            duration_secs: Some(duration_secs),
            diff: diff.as_deref(),
            rolled_back,
            committed,
            commit_hash: commit_hash.as_deref(),
            harness_stdout: Some(stdout),
            harness_stderr: Some(stderr),
            cost_usd: parsed.cost_usd,
            input_tokens: parsed.input_tokens,
            output_tokens: parsed.output_tokens,
            session_id: parsed.session_id.as_deref(),
            termination_reason: Some(TerminationReason::UserSkipped),
            test_status: Some(TestStatus::NotRun),
            ..Default::default()
        },
    )?;

    storage::update_step_status(ctx.conn, &ctx.step.id, StepStatus::Skipped)?;
    let dependent_count = storage::list_step_dependents(ctx.conn, &ctx.step.id)?.len();
    if dependent_count > 0 && !ctx.json_output {
        eprintln!(
            "warning: skipped step {} '{}' has {} dependent step(s); that branch will remain blocked until you reset, remove, or rewire those dependents",
            ctx.step_num, ctx.step.title, dependent_count
        );
    }
    // A skipped step's pending question/blocker is moot — resolve it so the
    // step does not stay derived-`Blocked` and the plan can finalize
    // `Complete` (a skipped step counts as done). Mirrors the resolution
    // baked into `storage::mark_step_skipped` for the CLI/TUI skip paths.
    storage::resolve_open_interruptions_for_step(
        ctx.conn,
        &ctx.step.id,
        "step skipped — interruption no longer applicable",
    )?;

    write_phase(
        ctx.conn,
        PhaseWrite {
            plan: ctx.plan,
            step_id: &ctx.step.id,
            step_num: ctx.step_num,
            attempt,
            max_attempts: ctx.max_attempts,
            execution_log_id: Some(exec_log_id),
            phase: Phase::PostStepHook,
            current_command: None,
            child: ChildUpdate::Clear,
            json_output: ctx.json_output,
        },
    )?;
    hooks::run_post_step(
        ctx.conn,
        ctx.hook_ctx,
        ctx.plan,
        ctx.step,
        attempt,
        "skipped",
        ctx.workdir,
    )
    .await?;

    Ok(StepResult {
        outcome: StepOutcome::Skipped,
        step_id: ctx.step.id.clone(),
        attempts_used: attempt,
        commit_hash,
        needs_review: None,
    })
}

/// Outcome of [`handle_skipped_attempt`].
enum SkipDisposition {
    /// The step was finalized (Skipped). Bubble this `StepResult` straight
    /// out of `execute_step`.
    Finalized(StepResult),
    /// The TUI skip dialog's Esc/cancel path: the attempt was undone with no
    /// retry budget consumed. The caller must step `attempt` back by one and
    /// re-enter the loop at the same attempt number.
    Reenter,
}

/// Shared skip handling for a `Skipped` cancel reason — used both by the
/// `WaitResult::Skipped` arm (the harness was killed by the cancel ladder)
/// and by the natural-exit-vs-skip race branch in `WaitResult::Completed`
/// (the harness exited on its own in the very `select!` poll a `Skipped`
/// cancel landed in, so `select!` picked `child.wait()` and we never got a
/// `WaitResult::Skipped`). Routing both through one function guarantees the
/// race resolves to *skip this step and advance* — never the whole-run
/// `Aborted` path — and that there is exactly one `execution_logs` row.
///
/// Authoritatively consumes the requested park kind exactly once (the single
/// `take` happens-after the cancel watch fired, so the stored value is
/// visible). `Cancel` → undo the attempt with no budget consumed (re-enter);
/// `Stash`/`Commit`/`Discard` (or a `None` slot → `Stash`) → finalize the
/// step Skipped.
async fn handle_skipped_attempt(
    ctx: &ExecCtx<'_>,
    conn: &Connection,
    exec_log_id: i64,
    duration_secs: f64,
    attempt: i32,
    stdout: &str,
    stderr: &str,
) -> Result<SkipDisposition> {
    let requested_kind = crate::signal::take_requested_park_kind();
    if requested_kind == Some(crate::git::ParkStrategyKind::Cancel) {
        cancel_skipped_attempt(ctx, exec_log_id, attempt)?;
        // Reset the persisted attempt counter — `set_step_attempts` was
        // bumped before the harness spawned; leaving it would make a later
        // resume think the budget was consumed.
        set_step_attempts(conn, &ctx.step.id, attempt - 1)?;
        storage::update_step_status(conn, &ctx.step.id, StepStatus::InProgress)?;
        return Ok(SkipDisposition::Reenter);
    }
    // A `None` slot (cross-process skip that couldn't record a choice)
    // resolves to `Stash` so a skip never silently destroys work.
    let park_kind = requested_kind.unwrap_or(crate::git::ParkStrategyKind::Stash);
    let result = finalize_skipped(
        ctx,
        SkippedArgs {
            exec_log_id,
            duration_secs,
            attempt,
            stdout,
            stderr,
            kind: park_kind,
        },
    )
    .await?;
    // Reset the cancel watch channel now that this step is terminally
    // Skipped. The skip tripped the channel to `Some(Skipped)`; without
    // resetting it, the *next* step's pre-attempt cancel check
    // (`finalize_precancel`) would observe the still-latched `Skipped` and
    // immediately skip that step too (the cross-process-run regression:
    // every subsequent step would skip in milliseconds). `cancel_skipped_attempt`
    // already does this for the Esc/cancel path; the terminal-skip path
    // needs it just the same.
    crate::signal::clear_cancel_state();
    Ok(SkipDisposition::Finalized(result))
}

/// Finalize an attempt that the harness ended by raising an interruption
/// (an open `interruptions` row — a `ralph question ask` *or* a `ralph
/// block` — for this (step, attempt)). The cross-process bridge of
/// docs/dag-redesign.md §7 / §9 invariant 4, mirroring the V23 skip-bridge:
/// the open row is the bridge; a CLI/TUI in a *different* process resolves
/// it; the runner observes the resolution at the next scheduler tick and
/// the step re-runs with the resolution injected (the injection itself is
/// already done by `prompt.rs`).
///
/// Skips tests + commit, rolls back any diff the harness produced, writes
/// the `execution_logs` row with `termination_reason = paused_for_question`,
/// and returns the step's status to [`StepStatus::Pending`] so a re-run
/// picks it up cleanly (the derived `Blocked` overlay shadows `Pending`
/// while the interruption is open — `effective_step_status`).
///
/// **Zero retry budget (HARD invariant — docs/dag-redesign.md §3.4 / §9
/// invariant 4).** `step.attempts` was bumped at the top of the retry loop
/// *before* the harness spawned; we roll it back by one here so the resumed
/// run re-runs the *same* attempt number, exactly like the skip-dialog
/// cancel path (`handle_skipped_attempt`'s `set_step_attempts(.. attempt -
/// 1)`). An interruption is the agent asking for help, not a failed try —
/// it must never burn a retry.
/// A parked stash that has been pushed to git but whose DB pointer row has not
/// yet been written. Pairs [`stash_step_worktree_for_interruption`] (the git
/// side effect) with [`commit_park_atomically`] (the transactional pointer
/// write) so the two can be made consistent: the stash is restored if the
/// transaction that would have recorded it fails to commit.
type ParkedStash = (git::StashRef, Vec<String>);

/// Push the step's in-flight WIP onto the git stash so the working tree is
/// clean while the branch is parked awaiting a human. This is a **pure git
/// side effect** — it writes NO DB row. The caller records the pointer inside
/// its own transaction via [`commit_park_atomically`], which restores this
/// stash if the transaction fails to commit. Returns `None` when there is
/// nothing to park (clean tree, or nothing left after excluding the user's
/// pre-existing untracked files).
fn stash_step_worktree_for_interruption(
    ctx: &ExecCtx<'_>,
    attempt: i32,
    reason: &str,
) -> Result<Option<ParkedStash>> {
    if !git::has_uncommitted_changes(ctx.workdir)? {
        return Ok(None);
    }

    let staged_files = git::list_staged_files(ctx.workdir)?;
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let label = format!(
        "ralph: parked {reason} worktree for plan '{}' step '{}' attempt {} at {ts}",
        ctx.plan.slug, ctx.step.short_id, attempt
    );
    // Exclude the user's pre-existing untracked files from the stash
    // (`ctx.pre_existing_untracked` is the snapshot captured by the runner
    // before this step's harness ever ran). Stashing those files here would
    // make them disappear from the workdir for the lifetime of the park,
    // and — far worse — lose them outright if the parked stash is later
    // dropped administratively (`git stash clear`, IDE plugin, conflict
    // on resume). `git stash push --include-untracked -- ':!<path>' …`
    // says "stash everything *except* these paths", which is exactly the
    // semantics we want.
    let Some(stash_ref) =
        git::stash_push_with_untracked_except(ctx.workdir, &label, ctx.pre_existing_untracked)?
    else {
        return Ok(None);
    };
    Ok(Some((stash_ref, staged_files)))
}

/// Pop a stash created by [`stash_step_worktree_for_interruption`] back onto
/// the working tree. Used as the rollback for [`commit_park_atomically`] when
/// the transaction that would have recorded the parked pointer never
/// committed — so the WIP returns to the tree (and staged files are re-staged)
/// rather than being stranded in a dangling stash with no pointer.
fn restore_parked_stash(ctx: &ExecCtx<'_>, parked: &ParkedStash) -> Result<()> {
    let (stash_ref, staged_files) = parked;
    match git::stash_pop(ctx.workdir, stash_ref)? {
        git::StashPopOutcome::Clean => {
            if !staged_files.is_empty() {
                git::restage_files(ctx.workdir, staged_files);
            }
            Ok(())
        }
        git::StashPopOutcome::Conflicted(stderr) => bail!(
            "restoring the parked worktree from stash {} conflicted, so the preserved work \
             remains on the stash stack.\n{}",
            stash_ref.as_str(),
            stderr,
        ),
        git::StashPopOutcome::NotFound => bail!(
            "the parked stash entry {} disappeared before it could be restored",
            stash_ref.as_str(),
        ),
    }
}

/// Run `write` inside a single transaction, joining the parked-stash pointer
/// row (if any) to it, then commit. The git stash done by
/// [`stash_step_worktree_for_interruption`] is the only piece that cannot live
/// inside the SQLite transaction; if the transaction's writes OR its commit
/// fail (e.g. `SQLITE_FULL` on a near-full disk — the case the interruption
/// auto-blocker is most likely to hit), the stash is popped back onto the
/// working tree so it never strands a parked pointer for a pause that didn't
/// happen, nor a dangling stash with no pointer. Either both the DB state and
/// the parked stash land, or neither does.
fn commit_park_atomically<T>(
    ctx: &ExecCtx<'_>,
    parked: Option<ParkedStash>,
    write: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let result = (|| -> Result<T> {
        let tx = ctx.conn.unchecked_transaction()?;
        let value = write(&tx)?;
        if let Some((stash_ref, staged_files)) = &parked {
            storage::set_step_parked_worktree(&tx, &ctx.step.id, stash_ref.as_str(), staged_files)?;
        }
        tx.commit()?;
        Ok(value)
    })();

    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            if let Some(parked) = &parked
                && let Err(restore_err) = restore_parked_stash(ctx, parked)
            {
                return Err(e.context(format!(
                    "failed to persist the parked interruption; additionally, restoring the \
                     stashed WIP to the working tree failed: {restore_err:#}"
                )));
            }
            Err(e)
        }
    }
}

async fn finalize_paused_for_question(
    ctx: &ExecCtx<'_>,
    exec_log_id: i64,
    attempt: i32,
) -> Result<StepResult> {
    // Park any in-flight diff the harness produced before it raised the
    // interruption. This leaves the repository clean so the scheduler can
    // move on, while preserving the exact WIP for re-application when the
    // step is picked again after the interruption is resolved. The git stash
    // happens here; its DB pointer row is written inside the transaction
    // below (via `commit_park_atomically`) and the stash is popped back if
    // that transaction fails to commit, so the stash and pointer stay
    // consistent.
    let parked = stash_step_worktree_for_interruption(ctx, attempt, "interruption")?;

    // Zero retry budget (HARD invariant — docs/dag-redesign.md §3.4 / §9
    // invariant 4). The pre-spawn `set_step_attempts(.. attempt)` is rolled
    // back AND the `execution_logs` row this attempt created is **deleted**,
    // exactly like the skip-dialog cancel path (`cancel_skipped_attempt`):
    //
    //  - Leaving the row would preserve a cancelled/pause-only attempt
    //    that consumed no retry budget and would duplicate the audit trail
    //    when the resolved step re-runs at the *same* attempt number (the
    //    §3.2 pipeline loops back to iteration `n`, it does not advance to
    //    `n+1`).
    //  - Leaving the bumped counter would make a later resume think the
    //    budget was consumed.
    //
    // The durable record of the pause is the open `interruptions` row
    // itself (its `body` / `asked_at`, then `resolution` / `resolved_at`) —
    // the unified interruption model (§3.4) *is* the audit trail, so a
    // transient paused exec_log row is redundant. `attempt` is always >= 1
    // here (the retry loop increments before spawning), so `attempt - 1` is
    // non-negative.
    //
    // Phase E Fix 4: the three writes (delete the per-attempt log row,
    // roll back the attempts counter, flip status to Pending) — plus the
    // parked-worktree pointer row — run inside a single
    // `unchecked_transaction`. The same race the sibling
    // `raise_retry_exhausted_blocker` documents applies here too: between
    // any two of these writes, a scheduler tick in another process could
    // observe the half-state — most damagingly `(status=Pending, attempts
    // still at the bumped value, no open interruption yet visible)` —
    // re-pick the step, and either burn another attempt or trip the
    // executor's budget guard. Collapsing the writes into one transaction
    // makes every observable state either pre-pause or post-pause; the
    // parked stash is restored if the commit fails.
    commit_park_atomically(ctx, parked, |tx| {
        storage::delete_execution_log(tx, exec_log_id)?;
        set_step_attempts(tx, &ctx.step.id, attempt - 1)?;
        storage::update_step_status(tx, &ctx.step.id, StepStatus::Pending)?;
        Ok(())
    })?;

    write_phase(
        ctx.conn,
        PhaseWrite {
            plan: ctx.plan,
            step_id: &ctx.step.id,
            step_num: ctx.step_num,
            attempt,
            max_attempts: ctx.max_attempts,
            execution_log_id: None,
            phase: Phase::PostStepHook,
            current_command: None,
            child: ChildUpdate::Clear,
            json_output: ctx.json_output,
        },
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
        needs_review: None,
    })
}

/// Phase B — auto-raise a `Blocker` interruption when a step exhausts its
/// retry budget on a **retryable** failure mode (test fail / commit-hook
/// reject) instead of marking the step terminally `Failed`. The blocker
/// carries two ranked recovery options
/// ([`RETRY_EXHAUSTED_OPTION_RETRY`] priority 1,
/// [`RETRY_EXHAUSTED_OPTION_FAIL`] priority 2) so a human can pick a
/// recovery without typing. While the interruption is open the derived
/// `Blocked` overlay shadows the stored `Pending` status
/// (`effective_step_status`), the scheduler advances to another runnable
/// branch (mirroring the existing `PausedForQuestion` path), and the plan
/// only declares itself `Interrupted` once the runnable set is exhausted.
///
/// Crucially distinct from [`finalize_paused_for_question`]:
///
///  - The execution-log row is **kept** (not deleted): the retry budget was
///    fully spent, so this attempt's outcome is part of the audit trail. The
///    `termination_reason` reuses [`TerminationReason::PausedForQuestion`]
///    (no new variant is added — it remains the single "step parked
///    awaiting human" idiom; see Phase B notes in CLAUDE.md).
///  - `step.attempts` is **left at `max_attempts`** (not rolled back).
///    Phase C's "Retry step with parked changes" resolver is the one to call
///    `step reset` and bring the counter back to zero, and an observer
///    inspecting the DB while the blocker is open can tell the step "hit
///    its budget" — the same information the pre-Phase-B `Failed` status
///    used to carry.
///  - Other terminal modes (`HarnessFailed`, `Timeout`, `NoChanges`,
///    `Aborted`) keep their existing terminal `Failed` shape — those modes
///    are not productively retryable from the harness's point of view
///    (timeouts mean nothing reached tests; harness errors mean we never
///    got a usable artifact; no-changes is a contract violation, not a
///    test signal). Only [`FailureReason::TestFailed`] and
///    [`FailureReason::CommitFailed`] route here.
///
/// The storage writes (insert interruption, update step status) run inside a
/// single `unchecked_transaction` so a scheduler tick in another thread can
/// never observe the half-state `(status=Pending, no open interruption)` —
/// which would otherwise let the scheduler immediately re-pick the step and
/// burn another attempt despite our budget already being spent.
async fn raise_retry_exhausted_blocker(
    ctx: &ExecCtx<'_>,
    exec_log_id: Option<i64>,
    duration_secs: f64,
    attempt: i32,
    failure_output: &FailureOutput<'_>,
    failure_reason: FailureReason,
) -> Result<StepResult> {
    debug_assert!(
        matches!(
            failure_reason,
            FailureReason::TestFailed
                | FailureReason::CommitFailed
                | FailureReason::InsufficientDiskSpace
        ),
        "raise_retry_exhausted_blocker must only be called for retryable \
         failure modes (TestFailed / CommitFailed / InsufficientDiskSpace); \
         other modes keep their terminal Failed shape",
    );

    // Park any dirty tree instead of throwing it away. The blocker pauses the
    // branch awaiting a human decision, so we keep the repository clean for
    // other work while preserving the in-progress WIP for automatic restore if
    // the human chooses to continue. This is the git stash only; its DB
    // pointer row joins the transaction below and is restored if the commit
    // fails (see `commit_park_atomically`).
    let parked = stash_step_worktree_for_interruption(ctx, attempt, "retry-exhausted")?;

    // The execution_logs row is *kept* (it carries the full diagnostic
    // payload — stdout/stderr/diff/cost/test_results — captured during this
    // attempt). Reason reuses `PausedForQuestion`: the existing variant
    // already means "step parked awaiting human" and adding a new variant
    // would force every consumer (hook label switches, output formatters,
    // TUI status renderers) to widen its match without giving the human
    // anything new — the option text on the interruption already tells the
    // human which recovery they're picking. Test status maps to whatever
    // the failing phase observed.
    let test_st = match failure_reason {
        FailureReason::TestFailed => TestStatus::Failed,
        FailureReason::CommitFailed => TestStatus::NotRun,
        FailureReason::InsufficientDiskSpace => TestStatus::NotRun,
        _ => TestStatus::NotRun,
    };
    // Insert + park atomically. A scheduler tick observing
    // (status=Pending, no open interruption) would immediately re-pick the
    // step and burn another retry budget despite us already being out — the
    // transaction collapses the read window so the tick can only ever see
    // either the pre-exhaustion or the post-blocker state.
    //
    // *All* of these mutations — the disk-gate caller's attempt bump + log
    // row, the diagnostic payload write, the interruption insert, the status
    // park, and the parked-worktree pointer — commit or roll back together.
    // This matters most for the disk-gate caller (`exec_log_id == None`): on a
    // near-full filesystem the interruption insert can itself fail with
    // SQLITE_FULL, and a non-transactional attempt bump would otherwise leave
    // the step `Pending` with `attempts` burned and no open interruption — the
    // scheduler would then silently re-pick it and spend another attempt on
    // nothing. `commit_park_atomically` also pops the parked stash back onto
    // the working tree if the commit fails, so a rolled-back blocker never
    // strands a pointer-less stash (the bug this structure replaced: the park
    // used to commit its pointer row in autocommit *before* this transaction,
    // surviving a SQLITE_FULL rollback that left no interruption behind it).
    let options = vec![
        InterruptionOption {
            text: RETRY_EXHAUSTED_OPTION_RETRY.to_string(),
            priority: 1,
        },
        InterruptionOption {
            text: RETRY_EXHAUSTED_OPTION_FAIL.to_string(),
            priority: 2,
        },
    ];
    let interruption_id = commit_park_atomically(ctx, parked, |tx| {
        // The retry-loop callers pass the execution-log row they already
        // created across the exhausted attempts (and have already bumped
        // `attempts` via the loop). The disk-gate / pre-loop caller passes
        // `None`: it never entered the loop, so it mints its row and bumps the
        // attempt counter here, inside the transaction.
        let exec_log_id = match exec_log_id {
            Some(id) => id,
            None => {
                set_step_attempts(tx, &ctx.step.id, attempt)?;
                storage::create_execution_log(tx, &ctx.step.id, attempt, None, None)?.id
            }
        };

        storage::update_execution_log(
            tx,
            exec_log_id,
            crate::storage::ExecutionLogUpdate {
                duration_secs: Some(duration_secs),
                diff: failure_output.diff,
                test_results: failure_output.test_results,
                harness_stdout: Some(failure_output.stdout),
                harness_stderr: Some(failure_output.stderr),
                cost_usd: failure_output.parsed.cost_usd,
                input_tokens: failure_output.parsed.input_tokens,
                output_tokens: failure_output.parsed.output_tokens,
                session_id: failure_output.parsed.session_id.as_deref(),
                termination_reason: Some(TerminationReason::PausedForQuestion),
                test_status: Some(test_st),
                ..Default::default()
            },
        )?;

        // Phase E Fix 5: build the blocker body from the last 3 attempts in
        // the CURRENT cycle (V33). Each attempt's section is independently
        // bounded so the total stays under `RETRY_EXHAUSTED_BODY_MAX_BYTES`
        // even when every attempt's output is huge. The final attempt's
        // output is labeled "(final)" and rendered first so the most relevant
        // context (the one the user is about to triage) is on top. The
        // persisted row for this attempt was updated just above; reading the
        // chronological tail gives us the canonical view (including the final
        // attempt's `harness_stderr` commit-hook output).
        let body = build_retry_exhausted_body(
            tx,
            &ctx.step.id,
            ctx.max_attempts,
            failure_reason,
            failure_output,
        );

        let interruption_id = storage::insert_interruption(
            tx,
            &ctx.step.id,
            attempt,
            InterruptionKind::Blocker,
            &body,
            &options,
        )?;
        storage::update_step_status(tx, &ctx.step.id, StepStatus::Pending)?;
        Ok(interruption_id)
    })?;

    // Phase E Fix 4: emit `InterruptionRaised` with `auto_raised=true` for
    // the executor's retry-exhausted auto-blocker. Surfaces post-commit so
    // an NDJSON consumer never sees a "raised" event for a row that lost
    // a race with another writer (the transaction above either fully
    // committed or fully rolled back; we only emit on the success leg).
    crate::output::emit_interruption_raised(
        ctx.conn,
        ctx.json_output,
        &interruption_id,
        &ctx.step.id,
        InterruptionKind::Blocker.as_str(),
        true,
        attempt,
    );

    // Post-step hook: use a dedicated `retry_exhausted` label so hooks can
    // distinguish the executor-raised auto-blocker from the harness-raised
    // interruption path (`finalize_paused_for_question` still fires
    // `"paused"`). Both paths park the branch awaiting a human, but only
    // this one means "burned the full retry budget" — which a hook author
    // might want to surface differently (e.g. paging on retry-exhaustion
    // but only counting harness-side pauses).
    write_phase(
        ctx.conn,
        PhaseWrite {
            plan: ctx.plan,
            step_id: &ctx.step.id,
            step_num: ctx.step_num,
            attempt,
            max_attempts: ctx.max_attempts,
            execution_log_id: None,
            phase: Phase::PostStepHook,
            current_command: None,
            child: ChildUpdate::Clear,
            json_output: ctx.json_output,
        },
    )?;
    hooks::run_post_step(
        ctx.conn,
        ctx.hook_ctx,
        ctx.plan,
        ctx.step,
        attempt,
        "retry_exhausted",
        ctx.workdir,
    )
    .await?;

    Ok(StepResult {
        // Reuse `PausedForQuestion` — the runner already knows how to handle
        // this outcome: drops the step from `executed_step_ids`, advances to
        // another runnable branch, and only declares `Interrupted` when the
        // runnable set is exhausted. A new variant would force the runner's
        // match to widen with no behavioral difference (Phase B intent: keep
        // the scheduler unchanged; the *DB shape* under the outcome is what
        // carries the auto-blocker semantics, observable via the inserted
        // `interruptions` row + ranked options).
        outcome: StepOutcome::PausedForQuestion,
        step_id: ctx.step.id.clone(),
        attempts_used: attempt,
        commit_hash: None,
        needs_review: None,
    })
}

// ---------------------------------------------------------------------------
// Core executor
// ---------------------------------------------------------------------------

/// Per-step constants shared by every iteration of [`execute_step`]'s retry
/// loop. Resolved once up front (harness/agent/timeout don't change between
/// retries) and threaded into [`run_step_attempt`] so the per-attempt helper
/// reads as a single "run one attempt" request alongside the ambient
/// [`ExecCtx`]. Everything here is borrowed for the duration of the run.
struct StepAttemptCtx<'a> {
    config: &'a Config,
    abort_rx: &'a watch::Receiver<CancelState>,
    exec_opts: &'a ExecuteOptions,
    timeout: Option<Duration>,
    harness_name: &'a str,
    harness_config: &'a crate::config::HarnessConfig,
    agent_file_path: Option<&'a Path>,
    all_steps: &'a [Step],
}

/// Every way the body of [`execute_step`]'s retry loop can finish a single
/// attempt. Extracted from the loop body so the control flow that used to be
/// expressed via `return` / `continue` / `attempt -= 1; continue` /
/// fall-through is now explicit and provably exhaustive:
///
/// - [`AttemptOutcome::Return`] — the attempt reached a terminal state and
///   produced a [`StepResult`] the caller must return immediately (every site
///   that previously did `return Ok(...)` / `return finalize_*(...).await` /
///   `return raise_retry_exhausted_blocker(...).await`).
/// - [`AttemptOutcome::Retry`] — the attempt failed retryably; the caller
///   advances to the next attempt (every former `continue`, plus the
///   Completed-arm retry tail that used to fall through to the loop end). The
///   `prev_test_output` / `prev_failure_reason` carried into the next prompt
///   are written through the `&mut` params before this is returned, exactly
///   as the inline code mutated the loop-scoped locals.
/// - [`AttemptOutcome::Reenter`] — a cross-process/late `Skipped` cancel was
///   resolved by re-entering the *same* attempt number (every former
///   `attempt -= 1; continue`). The caller neutralizes the loop's top-of-loop
///   `attempt += 1` bump.
enum AttemptOutcome {
    Return(StepResult),
    Retry,
    Reenter,
}

/// Grouped arguments to [`execute_step`]. Bundles the ambient handles
/// (`conn` / `config` / `hook_ctx`), the plan/step/working-dir under
/// execution, the abort receiver, and the per-run [`ExecuteOptions`] into one
/// payload so the entry point reads as a single "run this step" request.
pub struct ExecuteStepArgs<'a> {
    pub conn: &'a Connection,
    pub plan: &'a Plan,
    pub step: &'a Step,
    pub config: &'a Config,
    pub workdir: &'a Path,
    pub hook_ctx: &'a HookContext,
    pub abort_rx: watch::Receiver<CancelState>,
    pub exec_opts: ExecuteOptions,
}

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
pub async fn execute_step(args: ExecuteStepArgs<'_>) -> Result<StepResult> {
    let ExecuteStepArgs {
        conn,
        plan,
        step,
        config,
        workdir,
        hook_ctx,
        abort_rx,
        exec_opts,
    } = args;
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
    let pre_existing_untracked = if exec_opts.resumed_parked_worktree {
        Vec::new()
    } else {
        git::get_untracked_files(workdir)?
    };

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

    // Per-step disk-space gate.
    //
    // A nearly-full filesystem is the class of failure where we don't want to
    // even start work — past that point, SQLite writes start failing with
    // SQLITE_FULL and ralph's own state (execution_logs, run_locks) can be
    // corrupted. Check before the retry loop so a FS that filled between
    // preflight and now still bails out cleanly.
    //
    // `min_free_disk_mb = 0` disables the check (user opt-out).
    //
    // Route via [`raise_retry_exhausted_blocker`] (NOT terminal Failed): disk
    // pressure is a recoverable environmental failure — the human frees disk
    // and resolves with `RETRY_EXHAUSTED_OPTION_RETRY` to resume with parked changes.
    // Going terminal Failed (the pre-fix shape) burned the entire retry budget
    // on a single transient FS hiccup; the blocker preserves it.
    if config.min_free_disk_mb > 0 {
        match crate::preflight::disk_space(workdir) {
            Ok(ds) => {
                let required_bytes = config.min_free_disk_mb.saturating_mul(1_048_576);
                if ds.available_bytes < required_bytes {
                    let have_gb = ds.available_gb();
                    let need_gb = config.min_free_disk_mb as f64 / 1024.0;
                    eprintln!(
                        "> Step blocked: only {have_gb:.1} GB free, need >= {need_gb:.1} GB \
                         (config: min_free_disk_mb) — raising recoverable blocker"
                    );
                    let attempt = step.attempts + 1;
                    let msg = format!(
                        "insufficient disk space: {have_gb:.1} GB free, \
                         need >= {need_gb:.1} GB (required = {required_bytes} bytes, \
                         available = {} bytes)",
                        ds.available_bytes,
                    );
                    let test_results = vec![msg];
                    let parsed = ParsedHarnessOutput::default();
                    let fail_output = FailureOutput {
                        diff: None,
                        test_results: &test_results,
                        stdout: "",
                        stderr: "",
                        parsed: &parsed,
                        has_changes: false,
                    };
                    // Pass `None`: the attempt bump and the execution-log row
                    // are minted inside the blocker's transaction so a
                    // SQLITE_FULL on the interruption insert (entirely
                    // plausible on the near-full FS that tripped this gate)
                    // can't leave a burned attempt with no open interruption.
                    return raise_retry_exhausted_blocker(
                        &ctx,
                        None,
                        0.0,
                        attempt,
                        &fail_output,
                        FailureReason::InsufficientDiskSpace,
                    )
                    .await;
                }
            }
            Err(e) => {
                // Probe failure (non-unix, weird FS) — log and continue.
                // We'd rather run than block on an inscrutable error.
                eprintln!("> Disk space probe failed, continuing: {e}");
            }
        }
    }

    // Previous attempt context for retries. Post test-then-commit (Phase A)
    // the dirty tree is always on disk between attempts, so the retry prompt
    // omits the diff/files sections — only the failure reason + previous
    // test output (with any commit-hook output appended) ride this struct.
    let mut prev_test_output: Option<String> = None;
    let mut prev_failure_reason: Option<String> = None;

    let mut attempt = step.attempts;

    // Per-step constants for the retry loop. Bundled once here so the
    // per-attempt body lives in [`run_step_attempt`] rather than inline.
    let attempt_ctx = StepAttemptCtx {
        config,
        abort_rx: &abort_rx,
        exec_opts: &exec_opts,
        timeout,
        harness_name,
        harness_config,
        agent_file_path: agent_file_path.as_deref(),
        all_steps: &all_steps,
    };

    while attempt < max_attempts {
        attempt += 1;

        match run_step_attempt(
            &attempt_ctx,
            &ctx,
            attempt,
            &mut prev_test_output,
            &mut prev_failure_reason,
        )
        .await?
        {
            AttemptOutcome::Return(result) => return Ok(result),
            AttemptOutcome::Retry => continue,
            AttemptOutcome::Reenter => {
                // Re-enter at the SAME attempt: the loop bumps `attempt` at
                // the top, so step back one to neutralize that bump.
                attempt -= 1;
                continue;
            }
        }
    }

    // Unreachable: the budget guard above rejects steps that enter with
    // `attempts >= max_attempts`, so the while-loop always runs at least
    // once, and every terminal state returns from inside the loop.
    unreachable!("retry loop should always return via one of its inner branches")
}

/// Run a single attempt of a step's retry loop.
///
/// This is the body of [`execute_step`]'s `while attempt < max_attempts`
/// loop, extracted verbatim. It spawns the harness, waits (racing abort +
/// timeout + cross-process skip), runs deterministic tests, commits on pass
/// (test-then-commit, Phase A), and routes every failure mode (retryable vs.
/// terminal vs. retry-exhausted auto-blocker, Phase B). The caller bumped
/// `attempt` before calling.
///
/// Control flow is reported via [`AttemptOutcome`] (see its doc comment for
/// the mapping from the former inline `return` / `continue` / `attempt -= 1;
/// continue` / fall-through). The retry context fed into the *next* attempt's
/// prompt is written through `prev_test_output` / `prev_failure_reason`
/// before an [`AttemptOutcome::Retry`] is returned, exactly as the inline
/// code mutated the loop-scoped locals.
#[allow(clippy::too_many_lines)]
async fn run_step_attempt(
    actx: &StepAttemptCtx<'_>,
    ctx: &ExecCtx<'_>,
    attempt: i32,
    prev_test_output: &mut Option<String>,
    prev_failure_reason: &mut Option<String>,
) -> Result<AttemptOutcome> {
    let StepAttemptCtx {
        config,
        abort_rx,
        exec_opts,
        timeout,
        harness_name,
        harness_config,
        agent_file_path,
        all_steps,
    } = *actx;
    let conn = ctx.conn;
    let plan = ctx.plan;
    let step = ctx.step;
    let workdir = ctx.workdir;
    let hook_ctx = ctx.hook_ctx;
    let step_num = ctx.step_num;
    let max_attempts = ctx.max_attempts;
    let pre_existing_untracked = ctx.pre_existing_untracked;

    {
        // Check cancel before starting. Persist the bumped attempt count and
        // drop an execution-log row so the DB reflects the same attempt number
        // that StepResult reports and the cancel has a visible audit trail.
        // `Aborted` (Ctrl+C) terminates the whole run; `Skipped` (a same-
        // process `ralph skip` of this step) drops only this step.
        // Copy the reason out and DROP the borrow before calling
        // `finalize_precancel`. Holding the `watch::Receiver::borrow()` read
        // guard across the call would deadlock: `finalize_precancel` may
        // reset the cancel channel (`clear_pending_skip_state` →
        // `clear_cancel_state` → `Sender::send`), and `send` needs the watch
        // *write* lock, which can never be acquired while this read guard is
        // still alive on the same thread.
        let pending_reason = *abort_rx.borrow();
        if let Some(reason) = pending_reason {
            return Ok(AttemptOutcome::Return(finalize_precancel(
                conn, &step.id, attempt, reason,
            )?));
        }

        // Mark step as in-progress and bump attempts.
        storage::update_step_status(conn, &step.id, StepStatus::InProgress)?;
        set_step_attempts(conn, &step.id, attempt)?;

        // Build retry context if this is not the first attempt.
        //
        // Post test-then-commit (Phase A): a failed attempt leaves the dirty
        // tree on disk for the next attempt, so the agent inspects prior work
        // directly via `git diff`. The prompt's diff/files sections are
        // therefore redundant — collapse the context to just attempt/max +
        // previous test output (including any commit-hook output) + previous
        // failure reason. Both `Keep` and `Rollback` paths now produce the
        // same shape; the diff-feeding `Rollback` behavior is vestigial
        // (see the retry-tail block — removal in follow-up PR).
        let retry_context = if attempt > 1 {
            Some(RetryContext {
                attempt,
                max_attempts,
                previous_diff: None,
                previous_test_output: prev_test_output.clone(),
                files_modified: Vec::new(),
                previous_failure_reason: prev_failure_reason.clone(),
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

        // Fetch the BOUNDED resolved-interruption set for this step so the
        // next-attempt prompt re-injects the human's clarifications/unblocks
        // between Plan context and Step details (docs/dag-redesign.md §8
        // item 1). The query `LIMIT`s to the most-recent N resolved
        // interruptions — this is the §4 fix: the prompt feed is bounded in
        // count here and in per-field length inside the formatter. First
        // attempts on never-interrupted steps return an empty slice.
        let resolved_interruptions = storage::list_resolved_interruptions_for_step(
            conn,
            &step.id,
            storage::DEFAULT_RESOLVED_INTERRUPTION_LIMIT,
        )?;

        // Build prompt.
        let prompt_text = prompt::build_step_prompt(&prompt::BuildStepPromptArgs {
            plan,
            step,
            all_steps,
            agent_name,
            retry_context: retry_context.as_ref(),
            harness_supports_agent_file: harness_config.supports_agent_file,
            prompts: &prompts,
            resolved_interruptions: &resolved_interruptions,
        });

        // Create execution log entry.
        let exec_log =
            storage::create_execution_log(conn, &step.id, attempt, Some(&prompt_text), None)?;
        let started_at = std::time::Instant::now();

        // Per-attempt progress sub-header and prompt preview. Printed
        // inside the retry loop so every attempt (including retries) gets
        // its own timestamped "started at" line. In NDJSON mode we emit
        // a structured `PromptPrepared` event instead.
        render_attempt_header(
            exec_opts,
            config,
            harness_name,
            harness_config,
            attempt,
            max_attempts,
        );
        render_prompt_preview(exec_opts, step, attempt, &prompt_text)?;

        // Record the step identity + attempt bookkeeping on the run_locks
        // row. Subsequent `write_phase` calls in this attempt can pass
        // `None` for step_id/step_num/attempt/max_attempts and let COALESCE
        // preserve what we set here. `Clear` the child columns in case a
        // previous attempt left them populated — a new attempt means any
        // prior child is long dead.
        write_phase(
            conn,
            PhaseWrite {
                plan,
                step_id: &step.id,
                step_num,
                attempt,
                max_attempts,
                execution_log_id: Some(exec_log.id),
                phase: Phase::PreStepHook,
                current_command: None,
                child: ChildUpdate::Clear,
                json_output: exec_opts.json_output,
            },
        )?;

        // Run pre-step hook.
        if let Err(e) = hooks::run_pre_step(conn, hook_ctx, plan, step, attempt, workdir).await {
            eprintln!("Pre-step hook failed: {e}");
            // Treat as a failed attempt — skip harness execution.
            let test_result_strings = vec![format!("pre-step hook failed: {e}")];
            storage::update_execution_log(
                conn,
                exec_log.id,
                crate::storage::ExecutionLogUpdate {
                    duration_secs: Some(started_at.elapsed().as_secs_f64()),
                    test_results: &test_result_strings,
                    termination_reason: Some(TerminationReason::HookFailed),
                    test_status: Some(TestStatus::NotRun),
                    ..Default::default()
                },
            )?;
            if attempt >= max_attempts {
                storage::update_step_status(conn, &step.id, StepStatus::Failed)?;
                write_phase(
                    conn,
                    PhaseWrite {
                        plan,
                        step_id: &step.id,
                        step_num,
                        attempt,
                        max_attempts,
                        execution_log_id: Some(exec_log.id),
                        phase: Phase::PostStepHook,
                        current_command: None,
                        child: ChildUpdate::Clear,
                        json_output: exec_opts.json_output,
                    },
                )?;
                hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir)
                    .await?;
                return Ok(AttemptOutcome::Return(StepResult {
                    outcome: StepOutcome::Failed,
                    step_id: step.id.clone(),
                    attempts_used: attempt,
                    commit_hash: None,
                    needs_review: None,
                }));
            }
            *prev_test_output = Some(format!("pre-step hook failed: {e}"));
            *prev_failure_reason = Some("pre-step hook failed".to_string());
            write_phase(
                conn,
                PhaseWrite {
                    plan,
                    step_id: &step.id,
                    step_num,
                    attempt,
                    max_attempts,
                    execution_log_id: Some(exec_log.id),
                    phase: Phase::PostStepHook,
                    current_command: None,
                    child: ChildUpdate::Clear,
                    json_output: exec_opts.json_output,
                },
            )?;
            hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "failed", workdir).await?;
            return Ok(AttemptOutcome::Retry);
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
            agent_file_path,
            step.model.as_deref(),
        )?;
        let env_vars = harness::build_harness_env(harness_config, agent_file_path);

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
            PhaseWrite {
                plan,
                step_id: &step.id,
                step_num,
                attempt,
                max_attempts,
                execution_log_id: Some(exec_log.id),
                phase: Phase::Harness,
                current_command: Some(harness_name),
                child: ChildUpdate::Keep,
                json_output: exec_opts.json_output,
            },
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
            PhaseWrite {
                plan,
                step_id: &step.id,
                step_num,
                attempt,
                max_attempts,
                execution_log_id: Some(exec_log.id),
                phase: Phase::Harness,
                current_command: Some(harness_name),
                child: match child_pid_i64 {
                    Some(pid) => ChildUpdate::Set {
                        pid,
                        start_token: child_token.as_deref(),
                    },
                    None => ChildUpdate::Keep,
                },
                json_output: exec_opts.json_output,
            },
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
        // Race the harness wait against a DB poll for a *cross-process* skip
        // request (the production path: `ralph skip` / the TUI run in a
        // different process from this runner, so the same-process cancel
        // registry can never reach us). When the poll sees a pending
        // `plans.skip_requested_step_id` matching the step we have in-flight,
        // it injects `CancelReason::Skipped` into our own cancel channel —
        // the exact signal the same-process fast path uses — so the existing
        // `wait_with_timeout_and_abort` → `WaitResult::Skipped` →
        // `finalize_skipped`/`cancel_skipped_attempt` handling runs UNCHANGED.
        // The poll future then parks forever so the wait future is the one
        // that resolves the `select!` with the real `WaitResult`.
        let wait_fut = wait_with_timeout_and_abort(child, timeout, abort_rx.clone(), emitters);
        let poll_fut = poll_cross_process_skip(conn, &plan.id, &step.id);
        tokio::pin!(wait_fut);
        let wait_result = tokio::select! {
            r = &mut wait_fut => r,
            // poll_cross_process_skip only returns if polling errors out
            // (e.g. the plan row vanished); in that case stop polling and
            // just wait normally for the harness.
            _ = poll_fut => wait_fut.await,
        };
        let duration_secs = started_at.elapsed().as_secs_f64();

        match wait_result {
            WaitResult::Completed(output) => {
                let output = output.context("Harness process failed")?;

                // Natural-exit-vs-skip race (Fix 2). If a `Skipped` cancel
                // landed in the very `select!` poll where `child.wait()` also
                // became ready, `select!` may have picked the wait arm and
                // produced `Completed` instead of `Skipped`. If we fell
                // through to the test phase, the test runner would see the
                // tripped cancel channel and abort, and `test_aborted` would
                // route us into `finalize_failure(Aborted, UserInterrupted)`
                // → `StepOutcome::Aborted` → the runner tears down the WHOLE
                // run. That violates the invariant "Aborted ends the whole
                // run; Skipped advances." So before any test work, inspect
                // the cancel reason: a pending `Skipped` is routed through
                // the exact same skip handling as `WaitResult::Skipped`
                // (consume the park kind, finalize_skipped /
                // cancel_skipped_attempt). Only a pending `Aborted` is left
                // to drive the whole-run abort below.
                if *abort_rx.borrow() == Some(CancelReason::Skipped) {
                    match handle_skipped_attempt(
                        ctx,
                        conn,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        &output.stdout,
                        &output.stderr,
                    )
                    .await?
                    {
                        SkipDisposition::Finalized(result) => {
                            return Ok(AttemptOutcome::Return(result));
                        }
                        SkipDisposition::Reenter => {
                            return Ok(AttemptOutcome::Reenter);
                        }
                    }
                }
                // Step-scoped tidy: this step finished naturally. The
                // step-targeted poll already consumed any request aimed at
                // THIS step (we'd be in the Skipped branch above otherwise), so
                // the only request that can still be present targets a
                // DIFFERENT, not-yet-running step — clear ours by predicate so
                // a sibling step's queued `ralph skip` survives.
                storage::clear_skip_request_for_step(conn, &plan.id, &step.id)?;

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

                // Pause if the harness raised an open interruption
                // (`ralph question ask` / `ralph block` — native
                // `interruptions` rows) during this attempt
                // (docs/dag-redesign.md §7 harness protocol — the
                // cross-process bridge, mirroring V23 skip). Tested first —
                // even on non-zero exit — so a harness that asks then
                // crashes still surfaces as a pause: the human's
                // clarification is the prerequisite for any retry,
                // regardless of whether the crash was a side effect of the
                // harness's own self-terminate-after-asking path.
                let unanswered =
                    storage::count_unanswered_questions_for_attempt(conn, &step.id, attempt)?;
                if unanswered > 0 {
                    let _ = changed_files; // unused on this path
                    return Ok(AttemptOutcome::Return(
                        finalize_paused_for_question(ctx, exec_log.id, attempt).await?,
                    ));
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
                        return Ok(AttemptOutcome::Return(
                            finalize_failure(
                                ctx,
                                FailureArgs {
                                    exec_log_id: exec_log.id,
                                    duration_secs,
                                    attempt,
                                    reason: FailureReason::HarnessFailed,
                                    output: Some(&fail_output),
                                    termination_reason: TerminationReason::HarnessFailed,
                                    test_status: TestStatus::NotRun,
                                },
                            )
                            .await?,
                        ));
                    }

                    // Retry path. Post test-then-commit, even a crashing
                    // harness leaves its partial work on disk for the next
                    // attempt to build on.
                    //
                    // The one special case that still needs repair is an
                    // agent that committed on its own before crashing:
                    // mixed-reset back to the pre-attempt HEAD so the work
                    // survives as uncommitted changes instead of becoming an
                    // orphan commit.
                    if agent_committed_clean && let Some(before) = &head_before_harness {
                        write_phase(
                            conn,
                            PhaseWrite {
                                plan,
                                step_id: &step.id,
                                step_num,
                                attempt,
                                max_attempts,
                                execution_log_id: Some(exec_log.id),
                                phase: Phase::Rollback,
                                current_command: None,
                                child: ChildUpdate::Clear,
                                json_output: exec_opts.json_output,
                            },
                        )?;
                        git::reset_mixed_to(workdir, before)?;
                    }
                    let rolled_back = false;
                    storage::update_execution_log(
                        conn,
                        exec_log.id,
                        crate::storage::ExecutionLogUpdate {
                            duration_secs: Some(duration_secs),
                            diff: diff.as_deref(),
                            test_results: &test_results,
                            rolled_back,
                            harness_stdout: Some(&output.stdout),
                            harness_stderr: Some(&output.stderr),
                            cost_usd: parsed.cost_usd,
                            input_tokens: parsed.input_tokens,
                            output_tokens: parsed.output_tokens,
                            session_id: parsed.session_id.as_deref(),
                            termination_reason: Some(TerminationReason::HarnessFailed),
                            test_status: Some(TestStatus::NotRun),
                            ..Default::default()
                        },
                    )?;
                    let _ = diff; // dropped — Phase A omits diff from retry context
                    *prev_test_output = Some(test_results.join("\n"));
                    let _ = changed_files; // dropped — Phase A omits files_modified
                    *prev_failure_reason = Some("harness exited non-zero".to_string());
                    return Ok(AttemptOutcome::Retry);
                }

                // ---------------------------------------------------------
                // TEST-THEN-COMMIT (Phase A).
                //
                // The per-iteration commit no longer runs here. It moved to
                // *after* the deterministic test passes (see the
                // `test_passed && has_changes` branch below). Between failed
                // attempts the dirty tree is preserved on disk so the next
                // attempt builds on top.
                //
                // `agent_committed_clean` (worktree clean + HEAD advanced)
                // is still a contract violation — the agent committed its
                // own work. The existing classification/edge-case handling
                // below is preserved verbatim.
                //
                // Tooling (`ralph log` / `step reset`) parses ONLY the
                // `Ralph-*` trailers, never the subject (reuses the
                // `Ralph-Skipped-Step` + `[ralph wip]` precedent).

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
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::PreTestHook,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
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
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::Tests,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
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
                        .map(format_test_result_line)
                        .collect();

                    // Post-test hook.
                    write_phase(
                        conn,
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::PostTestHook,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
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

                // If a cancel landed mid-test, the test runner will have
                // killed its child and set `test_aborted`. The test runner
                // can't tell *why* it was cancelled — it treats any reason
                // as abort. But the disposition differs (Fix 2): a `Skipped`
                // reason that landed during the test phase (after the
                // top-of-arm check) must skip THIS step and advance, not
                // tear the whole run down. Inspect the cancel reason and
                // route a pending `Skipped` through the same skip handling
                // as `WaitResult::Skipped`; only a pending `Aborted` (or no
                // reason at all — a bare test-runner abort) drives the
                // UserInterrupted whole-run abort below.
                if test_aborted && *abort_rx.borrow() == Some(CancelReason::Skipped) {
                    // The test runner already rolled its own child; roll
                    // back the worktree before parking so finalize_skipped's
                    // park sees a consistent tree (mirrors the Aborted arm's
                    // pre-finalize rollback intent).
                    match handle_skipped_attempt(
                        ctx,
                        conn,
                        exec_log.id,
                        duration_secs,
                        attempt,
                        &output.stdout,
                        &output.stderr,
                    )
                    .await?
                    {
                        SkipDisposition::Finalized(result) => {
                            return Ok(AttemptOutcome::Return(result));
                        }
                        SkipDisposition::Reenter => {
                            return Ok(AttemptOutcome::Reenter);
                        }
                    }
                }

                // If Ctrl+C landed mid-test, the test runner will have killed
                // its child; surface this as Aborted rather than a retry-worthy
                // test failure. Capture partial test_results so the log row
                // reflects what actually ran before the abort landed.
                if test_aborted {
                    // A `Skipped` reason was already handled above; reaching
                    // here means `Aborted` (or a bare test-runner abort) —
                    // the whole-run teardown path. Defensively clear any
                    // pending-skip slot/cancel-state so a `Skipped` that
                    // raced and lost can't bleed into a later attempt
                    // (Fix 3). A no-op unless a stale `Skipped` is latched;
                    // never disturbs the `Aborted` reason.
                    crate::signal::clear_pending_skip_state();
                    // Phase A: test-then-commit. No iteration commit was
                    // made this attempt (commits only happen after tests
                    // pass), so the in-flight work is a dirty tree. Drop
                    // it cleanly while preserving the user's pre-existing
                    // untracked scratch files. The `Phase::Rollback` write
                    // is recorded so an external observer sees *why* the
                    // runner is touching the tree.
                    if git::has_uncommitted_changes(workdir)? {
                        write_phase(
                            conn,
                            PhaseWrite {
                                plan,
                                step_id: &step.id,
                                step_num,
                                attempt,
                                max_attempts,
                                execution_log_id: Some(exec_log.id),
                                phase: Phase::Rollback,
                                current_command: None,
                                child: ChildUpdate::Clear,
                                json_output: exec_opts.json_output,
                            },
                        )?;
                        git::rollback_except(workdir, pre_existing_untracked)?;
                    }
                    let fail_output = FailureOutput {
                        diff: diff.as_deref(),
                        test_results: &test_result_strings,
                        stdout: &output.stdout,
                        stderr: &output.stderr,
                        parsed: &parsed,
                        has_changes,
                    };
                    return Ok(AttemptOutcome::Return(
                        finalize_failure(
                            ctx,
                            FailureArgs {
                                exec_log_id: exec_log.id,
                                duration_secs,
                                attempt,
                                reason: FailureReason::Aborted,
                                output: Some(&fail_output),
                                termination_reason: TerminationReason::UserInterrupted,
                                test_status: TestStatus::Aborted,
                            },
                        )
                        .await?,
                    ));
                }

                // Fix 3 (defensive, general): past both `test_aborted`
                // skip/abort checks, every remaining disposition of this
                // `Completed` attempt is non-skip — success (returns just
                // below), terminal failure (via `finalize_failure`, which
                // also clears), or a retry (`continue`). If a `Skipped`
                // reason raced in *after* the top-of-arm check but the step
                // "beat" it (harness/tests finished first), it would
                // otherwise leak its park kind into the next attempt/step.
                // Clear it now. No-op unless a stale `Skipped` is latched;
                // never disturbs a pending `Aborted`.
                crate::signal::clear_pending_skip_state();

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
                        crate::storage::ExecutionLogUpdate {
                            duration_secs: Some(duration_secs),
                            test_results: &test_result_strings,
                            harness_stdout: Some(&output.stdout),
                            harness_stderr: Some(&output.stderr),
                            cost_usd: parsed.cost_usd,
                            input_tokens: parsed.input_tokens,
                            output_tokens: parsed.output_tokens,
                            session_id: parsed.session_id.as_deref(),
                            termination_reason: Some(TerminationReason::Success),
                            test_status: Some(success_test_status),
                            ..Default::default()
                        },
                    )?;

                    storage::update_step_status(conn, &step.id, StepStatus::Complete)?;

                    write_phase(
                        conn,
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::PostStepHook,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
                    )?;
                    hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "complete", workdir)
                        .await?;

                    return Ok(AttemptOutcome::Return(StepResult {
                        outcome: StepOutcome::Success,
                        step_id: step.id.clone(),
                        attempts_used: attempt,
                        commit_hash: None,
                        // Optional-policy no-diff success: nothing was
                        // committed, so there is no SHA for a read-only
                        // reviewer to run `git show` against. Review needs a
                        // committed iteration (§3.2 commits *then* reviews);
                        // a zero-diff success is not reviewable, so it
                        // completes straight from passing tests exactly as
                        // before (linear-plan parity).
                        needs_review: None,
                    }));
                }

                if test_passed && has_changes {
                    // POST-TEST COMMIT (Phase A): tests passed and the
                    // harness left uncommitted changes. Stage and commit now,
                    // *after* the deterministic test, so there is at most one
                    // ralph commit per step (zero on terminal failure / no
                    // changes). A pre-commit hook rejection here is treated
                    // as a retryable failure — same semantics as a test
                    // failure, with the hook stderr appended to the test
                    // output so the next attempt's prompt sees both.
                    write_phase(
                        conn,
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::Commit,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
                    )?;
                    let commit_msg = git::build_iteration_commit_message(
                        &step.short_id,
                        attempt,
                        &step.title,
                        &plan.slug,
                    );
                    let stage_and_commit = git::stage_except(workdir, pre_existing_untracked)
                        .and_then(|_| git::commit_staged(workdir, &commit_msg));
                    if let Err(e) = stage_and_commit {
                        // Pre-commit hook rejection (or other git failure).
                        // Treat as a test failure: keep the dirty tree on
                        // disk, append hook stderr to the test output so the
                        // next attempt sees both, and either retry or
                        // finalize as terminal failure on exhaustion. The
                        // existing CommitFailed-specific terminal/retry
                        // paths are gone (Phase B will reroute terminal
                        // exhaustion through a blocker).
                        let err_text = e.to_string();
                        let mut failure_desc = format!(
                            "commit rejected by pre-commit hook: {}. Fix the lint/style issues reported by the hook.",
                            err_text.trim()
                        );
                        // Lightweight latent-debt heuristic (best-effort, no over-engineering):
                        // scan for path-like tokens; if any has low/no overlap with the
                        // step's `changed_files` this attempt, append note. Signals that
                        // hook errors may be from pre-existing code outside this diff.
                        let mut saw_non_overlapping = false;
                        for token in err_text.split(|c: char| {
                            !c.is_alphanumeric() && c != '/' && c != '.' && c != '-' && c != '_'
                        }) {
                            let clean = token.trim_matches(|c: char| {
                                !c.is_alphanumeric() && c != '/' && c != '.' && c != '-' && c != '_'
                            });
                            if (clean.contains('/') || (clean.contains('.') && clean.len() > 2))
                                && !clean.is_empty()
                            {
                                let overlaps = changed_files.iter().any(|f| {
                                    clean == f.as_str()
                                        || clean.ends_with(f.as_str())
                                        || f.ends_with(clean)
                                });
                                if !overlaps {
                                    saw_non_overlapping = true;
                                    break;
                                }
                            }
                        }
                        if saw_non_overlapping {
                            failure_desc
                                .push_str(" (Note: some errors may be in pre-existing files outside this step's diff — latent debt.)");
                        }

                        // Combine test output + hook output so the next
                        // attempt's prompt surfaces both signals in a single
                        // section. The agent sees that tests passed but the
                        // commit hook rejected — and what the hook said.
                        let combined_output = {
                            let test_summary = test_result_strings.join("\n");
                            if test_summary.is_empty() {
                                format!("[Commit hook output]\n{failure_desc}")
                            } else {
                                format!(
                                    "[Tests passed but commit hook rejected]\n{test_summary}\n\n[Commit hook output]\n{failure_desc}"
                                )
                            }
                        };
                        let combined_result_strings = vec![failure_desc.clone()];

                        if attempt >= max_attempts {
                            // The combined output Phase A built (test summary
                            // + hook stderr inside one string with the
                            // `[Tests passed but commit hook rejected]` /
                            // `[Commit hook output]` headers) is what we want
                            // in the blocker body — surface it via
                            // `test_results` so `raise_retry_exhausted_blocker`
                            // can join+truncate it the same way it does for
                            // the test-fail path. The single-string
                            // `combined_result_strings` already carries
                            // `failure_desc` (the hook stderr); the combined
                            // *block* lives in `combined_output` and is
                            // routed through here as a one-element slice.
                            let combined_results_for_blocker = vec![combined_output.clone()];
                            let fail_output = FailureOutput {
                                diff: diff.as_deref(),
                                test_results: &combined_results_for_blocker,
                                stdout: &output.stdout,
                                stderr: &output.stderr,
                                parsed: &parsed,
                                has_changes,
                            };
                            // Phase B routing: commit-hook failure is a
                            // productively retryable mode (different prompt,
                            // a different attempt, or a code change can pass
                            // the hook), so an exhausted commit-fail becomes
                            // an auto-raised blocker just like an exhausted
                            // test fail — the human picks Retry / Mark
                            // Failed.
                            return Ok(AttemptOutcome::Return(
                                raise_retry_exhausted_blocker(
                                    ctx,
                                    Some(exec_log.id),
                                    duration_secs,
                                    attempt,
                                    &fail_output,
                                    FailureReason::CommitFailed,
                                )
                                .await?,
                            ));
                        }

                        // Retry path: do NOT roll back, do NOT advance HEAD.
                        // Preserve the dirty tree so the next attempt builds
                        // on top with the rejected work still visible.
                        storage::update_execution_log(
                            conn,
                            exec_log.id,
                            crate::storage::ExecutionLogUpdate {
                                duration_secs: Some(duration_secs),
                                diff: diff.as_deref(),
                                test_results: &combined_result_strings,
                                harness_stdout: Some(&output.stdout),
                                harness_stderr: Some(&output.stderr),
                                cost_usd: parsed.cost_usd,
                                input_tokens: parsed.input_tokens,
                                output_tokens: parsed.output_tokens,
                                session_id: parsed.session_id.as_deref(),
                                termination_reason: Some(TerminationReason::CommitFailed),
                                test_status: Some(TestStatus::NotRun),
                                ..Default::default()
                            },
                        )?;
                        let _ = diff; // dropped — Phase A omits diff from retry context
                        *prev_test_output = Some(combined_output);
                        let _ = changed_files; // dropped — Phase A omits files_modified
                        *prev_failure_reason = Some(failure_desc);
                        return Ok(AttemptOutcome::Retry);
                    }
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
                        crate::storage::ExecutionLogUpdate {
                            duration_secs: Some(duration_secs),
                            diff: diff.as_deref(),
                            test_results: &test_result_strings,
                            committed: true,
                            commit_hash: Some(&commit_hash),
                            harness_stdout: Some(&output.stdout),
                            harness_stderr: Some(&output.stderr),
                            cost_usd: parsed.cost_usd,
                            input_tokens: parsed.input_tokens,
                            output_tokens: parsed.output_tokens,
                            session_id: parsed.session_id.as_deref(),
                            termination_reason: Some(TerminationReason::Success),
                            test_status: Some(success_test_status),
                            ..Default::default()
                        },
                    )?;

                    // Review gate (docs/dag-redesign.md §3.2-§3.3 / §9-inv-2).
                    // A step reaches `Complete` only after its review
                    // *returns* (any verdict). When review is
                    // effective-enabled for this step we therefore DO NOT
                    // mark it `Complete` here: we leave it `InProgress` with
                    // `review_status = Pending` and hand the committed SHA
                    // back to the runner via `needs_review`. The runner
                    // spawns the read-only reviewer concurrently with the
                    // next *unrelated* implementation; this step's direct
                    // dependents stay non-runnable (deps_satisfied requires
                    // `Complete`) until the review returns and the
                    // orchestrator finalizes the step.
                    //
                    // When review is NOT effective-enabled (the default — no
                    // review config), this is byte-identical to before: the
                    // executor writes `Complete` and `needs_review` is
                    // `None`, so a linear/no-review plan behaves exactly as
                    // today.
                    let review_on = crate::config::effective_review_enabled(step, plan, config);
                    let needs_review = if review_on {
                        storage::update_step_status(conn, &step.id, StepStatus::InProgress)?;
                        storage::update_step_review_status(
                            conn,
                            &step.id,
                            crate::plan::ReviewStatus::Pending,
                        )?;
                        Some((commit_hash.clone(), attempt))
                    } else {
                        // Review effective-DISABLED at some scope (step / plan
                        // / config) — the §3.3/§6 fast path: record
                        // `review_status = Disabled` and go straight to
                        // `Complete` from passing tests with **no reviewer
                        // spawn**. Writing the explicit `Disabled` variant
                        // (rather than leaving the on-disk NULL, which means
                        // `Pending`) makes "review was off for this step"
                        // durable and observable cross-process — and lets
                        // `ralph doctor` / the TUI distinguish it from a step
                        // that simply has not been reviewed yet. For a
                        // linear/no-review plan this is the only review-status
                        // write that ever happens; `needs_review` stays `None`
                        // so the runner never enters the reviewer block,
                        // keeping behavior byte-identical to pre-review ralph.
                        storage::update_step_review_status(
                            conn,
                            &step.id,
                            crate::plan::ReviewStatus::Disabled,
                        )?;
                        storage::update_step_status(conn, &step.id, StepStatus::Complete)?;
                        None
                    };

                    write_phase(
                        conn,
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::PostStepHook,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
                    )?;
                    hooks::run_post_step(conn, hook_ctx, plan, step, attempt, "complete", workdir)
                        .await?;

                    return Ok(AttemptOutcome::Return(StepResult {
                        outcome: StepOutcome::Success,
                        step_id: step.id.clone(),
                        attempts_used: attempt,
                        commit_hash: Some(commit_hash),
                        needs_review,
                    }));
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
                    // Phase B routing: a retryable failure (TestFailed) that
                    // just exhausted its budget becomes an auto-raised
                    // `Blocker` interruption with ranked recovery options
                    // instead of a terminal `Failed` step. NoChanges (here:
                    // policy-Required with no diff, or `agent_committed_clean`)
                    // is a contract violation, not a productively retryable
                    // mode — it keeps its terminal `Failed` shape because no
                    // amount of re-running the same prompt is likely to undo a
                    // missing-diff failure pattern that already burned the
                    // full retry budget.
                    if matches!(reason, FailureReason::TestFailed) {
                        return Ok(AttemptOutcome::Return(
                            raise_retry_exhausted_blocker(
                                ctx,
                                Some(exec_log.id),
                                duration_secs,
                                attempt,
                                &fail_output,
                                reason,
                            )
                            .await?,
                        ));
                    }
                    return Ok(AttemptOutcome::Return(
                        finalize_failure(
                            ctx,
                            FailureArgs {
                                exec_log_id: exec_log.id,
                                duration_secs,
                                attempt,
                                reason,
                                output: Some(&fail_output),
                                termination_reason: term_reason,
                                test_status: test_st,
                            },
                        )
                        .await?,
                    ));
                }

                // Retry path. Post test-then-commit (Phase A): the failed
                // attempt left a dirty tree on disk and made NO commit; the
                // next attempt builds on top of that work directly. The
                // retry context omits the diff/files (already on disk via
                // `git diff`), passing only the failure reason + previous
                // test output to the next prompt.
                //
                // EDGE CASE — `agent_committed_clean`: the agent committed
                // its OWN work, so `has_changes == false` and HEAD advanced
                // without ralph making a commit. Mixed-reset back to the
                // pre-attempt HEAD un-commits it but keeps every changed
                // file on disk as uncommitted work — so the next attempt's
                // post-test commit picks it up sitting on the right base.
                // Leaving the agent's orphan commit at HEAD instead would let
                // the next attempt build on top of it, ralph's eventual single
                // commit would land on parent=orphan, and `step reset` (which
                // reverts only ralph's commit) would leave the orphan in
                // history — also inflating the reviewer's fixed-SHA diff.
                if agent_committed_clean && let Some(before) = &head_before_harness {
                    write_phase(
                        conn,
                        PhaseWrite {
                            plan,
                            step_id: &step.id,
                            step_num,
                            attempt,
                            max_attempts,
                            execution_log_id: Some(exec_log.id),
                            phase: Phase::Rollback,
                            current_command: None,
                            child: ChildUpdate::Clear,
                            json_output: exec_opts.json_output,
                        },
                    )?;
                    git::reset_mixed_to(workdir, before)?;
                }
                let rolled_back = false;
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
                    crate::storage::ExecutionLogUpdate {
                        duration_secs: Some(duration_secs),
                        diff: diff.as_deref(),
                        test_results: &test_result_strings,
                        rolled_back,
                        harness_stdout: Some(&output.stdout),
                        harness_stderr: Some(&output.stderr),
                        cost_usd: parsed.cost_usd,
                        input_tokens: parsed.input_tokens,
                        output_tokens: parsed.output_tokens,
                        session_id: parsed.session_id.as_deref(),
                        termination_reason: Some(retry_term),
                        test_status: Some(retry_test_status),
                        ..Default::default()
                    },
                )?;
                let _ = diff; // dropped — Phase A omits diff from retry context
                *prev_test_output = Some(test_output_summary);
                let _ = changed_files; // dropped — Phase A omits files_modified
                // Human-readable reason mirrors the termination classification
                // so the prompt (which omits the diff) still states what
                // went wrong.
                *prev_failure_reason = Some(
                    match retry_term {
                        TerminationReason::NoChanges if agent_committed_clean => {
                            "agent committed its own work instead of leaving \
                             changes for ralph (worktree clean, HEAD advanced)"
                        }
                        TerminationReason::NoChanges => "no changes produced",
                        _ => "tests failed",
                    }
                    .to_string(),
                );
                Ok(AttemptOutcome::Retry)
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
                Ok(AttemptOutcome::Return(
                    finalize_failure(
                        ctx,
                        FailureArgs {
                            exec_log_id: exec_log.id,
                            duration_secs,
                            attempt,
                            reason: FailureReason::Timeout,
                            output: Some(&fail_output),
                            termination_reason: TerminationReason::Timeout,
                            test_status: TestStatus::NotRun,
                        },
                    )
                    .await?,
                ))
            }

            WaitResult::Aborted => {
                // Harness was killed before we ever reached the test phase,
                // so test_status is NotRun (the test runner itself was never
                // invoked on this attempt). Aborted terminates the WHOLE run.
                Ok(AttemptOutcome::Return(
                    finalize_failure(
                        ctx,
                        FailureArgs {
                            exec_log_id: exec_log.id,
                            duration_secs,
                            attempt,
                            reason: FailureReason::Aborted,
                            output: None,
                            termination_reason: TerminationReason::UserInterrupted,
                            test_status: TestStatus::NotRun,
                        },
                    )
                    .await?,
                ))
            }

            WaitResult::Skipped { stdout, stderr } => {
                // `ralph skip` killed the in-flight harness via the same
                // ladder as Aborted, but only THIS step is dropped — the
                // runner advances. STEP 17: instead of unconditionally
                // rolling back (step 16's behavior), park the harness's
                // uncommitted work per the operator's `--changes` choice
                // (stash / commit / discard). `finalize_skipped` writes the
                // single `user_skipped` execution_logs row itself — we
                // deliberately do NOT also go through `finalize_failure`, so
                // there is exactly one row.
                //
                // STEP 18: the TUI skip dialog adds a fourth choice — Esc
                // (cancel). It rides the same cancel ladder + registry slot
                // but carries `ParkStrategyKind::Cancel`. On cancel we must
                // NOT finalize the step: roll back the killed harness's work
                // (preserving the user's pre-existing untracked scratch),
                // emit an `attempt_cancelled` NDJSON event, delete the
                // execution_logs row this attempt created (the row is created
                // with the prompt *before* the harness spawns), and re-enter
                // the retry loop at the *same* attempt number. Net effect:
                // the cancelled attempt consumes no retry budget and leaves
                // no `UNIQUE(step_id, attempt)` row.
                //
                // Authoritatively consume the requested park kind exactly
                // once here (inside `handle_skipped_attempt`), then branch /
                // thread it down. Doing the single `take` at this point
                // (after the cancel watch fired, which happens-after the
                // park-kind store) guarantees we observe the stored value.
                match handle_skipped_attempt(
                    ctx,
                    conn,
                    exec_log.id,
                    duration_secs,
                    attempt,
                    &stdout,
                    &stderr,
                )
                .await?
                {
                    SkipDisposition::Finalized(result) => Ok(AttemptOutcome::Return(result)),
                    SkipDisposition::Reenter => {
                        // Re-enter at the SAME attempt: the caller bumps
                        // `attempt` at the top of the loop, so it steps back
                        // one to neutralize that bump.
                        Ok(AttemptOutcome::Reenter)
                    }
                }
            }
        }
    }
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

/// How often the executor polls the DB for a cross-process skip request
/// while a harness is in-flight. 250 ms is well under any human's
/// skip→react expectation while adding negligible DB load (one indexed
/// single-row `SELECT` per tick against the local SQLite file).
const SKIP_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Poll `plans.skip_requested_step_id` for a *cross-process* skip targeting
/// the step we currently have in-flight, and — when found — funnel it into
/// this process's own cancel channel so the existing
/// `WaitResult::Skipped` machinery handles it unchanged.
///
/// This is the production half of the skip bridge. `ralph skip` and the TUI
/// skip dialog run in a different process from the runner; they cannot reach
/// the same-process cancel registry, so they write a durable request
/// (`storage::request_skip`) that the runner — the process that actually
/// owns the harness child — polls here.
///
/// On a match it `take`s the request (atomic read-and-clear) and calls
/// [`crate::signal::inject_skip_with_kind`], which records the park kind and
/// sends `CancelReason::Skipped` into the cancel watch channel
/// `wait_with_timeout_and_abort` is already listening on. We then `pending()`
/// forever so the *wait* future is the one that resolves the caller's
/// `select!` (producing the real `WaitResult::Skipped` with captured
/// output). A request that targets a *different* step is left in place
/// (peek, not take) so it is honored when that step runs rather than wrongly
/// consumed against the in-flight one.
///
/// Returns only on a polling error (e.g. the plan row vanished mid-run); the
/// caller then falls back to a plain wait. A clean tree / no request just
/// keeps ticking.
async fn poll_cross_process_skip(conn: &Connection, plan_id: &str, step_id: &str) {
    loop {
        tokio::time::sleep(SKIP_POLL_INTERVAL).await;
        // Single predicate-guarded read-and-clear: consumes the pending
        // request only if it still targets the step we have in-flight, and
        // leaves a request aimed at a different step untouched (it'll be
        // honored when that step runs). No separate peek, so there is no
        // window for a concurrently re-targeted `ralph skip` to be swallowed.
        match storage::take_skip_request_for_step(conn, plan_id, step_id) {
            Ok(Some(kind)) => {
                // The request targeted the in-flight step and is now cleared.
                // Funnel it into our own cancel channel; from here the
                // existing same-process skip path takes over verbatim.
                if crate::signal::inject_skip_with_kind(kind) {
                    // Skip was injected: let the wait future resolve the
                    // select! with the real `WaitResult::Skipped`.
                    std::future::pending::<()>().await;
                } else {
                    // A whole-run abort (Ctrl+C) was already latched, so the
                    // injector refused to downgrade it to a step skip. The
                    // request is consumed (correct — the run is tearing down
                    // anyway). Stop polling; the wait future will resolve via
                    // the abort path.
                    return;
                }
            }
            // No request, or it targets a different step — keep polling.
            Ok(None) => {}
            Err(_) => return,
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
///
/// Delegates to [`storage::set_step_attempts`] so the V33 cycle-index bump
/// (on attempts: > 0 → 0 transitions) is centralized in one place. The
/// executor-side wrapper is kept only for backwards-compatible call sites
/// inside the hot loop.
fn set_step_attempts(conn: &Connection, step_id: &str, attempts: i32) -> Result<()> {
    storage::set_step_attempts(conn, step_id, attempts)
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
        crate::storage::ExecutionLogUpdate {
            duration_secs: Some(0.0),
            termination_reason: Some(term_reason),
            test_status: Some(TestStatus::NotRun),
            ..Default::default()
        },
    )?;
    storage::update_step_status(conn, step_id, step_status)?;
    // Fix 3 (cross-process leak): a `Skipped` reason caught here at the
    // pre-attempt check finalized THIS step without ever reaching the
    // skip-handling arm that would normally clear the channel. Left
    // latched, the next step's pre-attempt check would observe the same
    // stale `Skipped` and skip it too (and the one after, …). Reset it now.
    // An `Aborted` reason must NOT be cleared — the runner's between-step
    // check still needs to see it to tear the whole run down.
    if reason == CancelReason::Skipped {
        crate::signal::clear_pending_skip_state();
        // Skipping abandons this step's work; its pending question/blocker is
        // moot. Resolve it so the step doesn't stay derived-`Blocked` and the
        // plan can finalize `Complete`. (An `Aborted` step is *not* abandoned
        // — it resumes later — so its interruption is deliberately left open.)
        storage::resolve_open_interruptions_for_step(
            conn,
            step_id,
            "step skipped — interruption no longer applicable",
        )?;
    }
    Ok(StepResult {
        outcome,
        step_id: step_id.to_string(),
        attempts_used: attempt,
        commit_hash: None,
        needs_review: None,
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
    use crate::config::HarnessConfig;

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
        assert_eq!(FailureReason::CommitFailed.hook_label(), "commit_failed");

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
        assert_eq!(
            FailureReason::CommitFailed.to_step_status(),
            StepStatus::Failed
        );

        assert_eq!(FailureReason::NoChanges.to_outcome(), StepOutcome::Failed);
        assert_eq!(FailureReason::TestFailed.to_outcome(), StepOutcome::Failed);
        assert_eq!(
            FailureReason::HarnessFailed.to_outcome(),
            StepOutcome::Failed
        );
        assert_eq!(
            FailureReason::CommitFailed.to_outcome(),
            StepOutcome::Failed
        );
    }

    #[test]
    fn test_set_step_attempts() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "s",
                project: "/p",
                branch_name: "b",
                description: "d",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: None,
                model: None,
                change_policy: None,
                tags: None,
            },
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
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("claude"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: None,
                model: None,
                change_policy: None,
                tags: None,
            },
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("claude"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 0 so a single hook failure is terminal.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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

    /// Harness produces a change (via write_simple_harness), but the
    /// per-iteration `git commit` is rejected by a pre-commit hook installed
    /// via a hermetic `core.hooksPath` temp dir + executable shell script
    /// that exits 1 (with a message mentioning an unrelated path to exercise
    /// the latent-debt note). Verifies terminal CommitFailed + NotRun path,
    /// no crash, hook stderr captured in test_results, using max_retries=0.
    #[tokio::test(flavor = "current_thread")]
    async fn test_commit_failure_terminal_reason() {
        use crate::plan::{TerminationReason, TestStatus};
        use tempfile::TempDir;

        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Hermetic rejecting hook: point core.hooksPath at our dir containing
        // an executable pre-commit that always fails. The message references a
        // path with no overlap to the harness-produced file to exercise the
        // "latent debt" heuristic.
        let hooks_tmp = TempDir::new().unwrap();
        let hooks_dir = hooks_tmp.path().join("reject-hooks");
        fs::create_dir_all(&hooks_dir).unwrap();
        let pre_commit = hooks_dir.join("pre-commit");
        fs::write(
            &pre_commit,
            "#!/bin/sh\necho 'pre-commit hook rejected: style error in unrelated/oldfile.py'\nexit 1\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&pre_commit).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&pre_commit, perms).unwrap();
        Command::new("git")
            .args([
                "config",
                "core.hooksPath",
                hooks_dir.to_string_lossy().as_ref(),
            ])
            .current_dir(&dir)
            .output()
            .unwrap();

        // Harness that produces a change so we reach (and fail at) the Commit phase.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, true);

        let conn = crate::db::open_memory().unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("changing"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: // no retries — commit failure is terminal on first attempt
            None, change_policy: None, tags: None },
        )
        .unwrap();

        // Register the harness under the name used in create_plan.
        let mut config = Config::default();
        config.harnesses.insert(
            "changing".to_string(),
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // Phase B: terminal commit-hook rejection now routes through the
        // auto-raised blocker (commit-hook failures are productively
        // retryable — different prompt/code can pass the hook). The log
        // row's `termination_reason` is `PausedForQuestion` (reused; see
        // `raise_retry_exhausted_blocker`) and `test_status` stays `NotRun`
        // because we never reached the test phase. The commit-failure
        // diagnostic still lives in `test_results` (Phase B carries it
        // through the blocker body and the log row alike).
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::PausedForQuestion),
            "Phase B: exhausted-budget commit-fail parks the step",
        );
        assert_eq!(logs[0].test_status, Some(TestStatus::NotRun));
        assert!(
            logs[0]
                .test_results
                .iter()
                .any(|s| s.contains("commit rejected by pre-commit hook")),
            "test_results must contain the commit-failure diagnostic (hook stderr path exercised): {:?}",
            logs[0].test_results
        );
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].kind, InterruptionKind::Blocker);
        assert_eq!(open[0].options.len(), 2);
    }

    /// A harness that exits successfully but produces no changes should
    /// terminate the log with NoChanges + NotRun.
    #[tokio::test(flavor = "current_thread")]
    async fn test_no_changes_reason() {
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("noop"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: // no retries — single failure is terminal
            None, change_policy: None, tags: None },
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("bigout"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("hugeout"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries=0: we expect this to fail terminally because the
        // harness produces no changes — the point is just that we captured
        // the truncated stdout.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("phases"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Phase Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("pgroup"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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

    /// Fix 2 — natural-exit vs skip race. If the harness exits *on its own*
    /// in the same `select!` poll a `Skipped` cancel is already latched, the
    /// `select!` may pick `child.wait()` → `WaitResult::Completed` instead of
    /// `WaitResult::Skipped`. The test phase would then see the tripped
    /// cancel channel, abort, and (pre-fix) drive
    /// `finalize_failure(Aborted, UserInterrupted)` →
    /// `StepOutcome::Aborted` → the runner tears down the WHOLE run.
    ///
    /// With the fix, *regardless* of which arm `select!` picks, the outcome
    /// must be `Skipped` (this step only) — never `Aborted` — and there must
    /// be exactly one execution_logs row (no finalize_failure +
    /// finalize_skipped double-write). The cancel reason is set BEFORE
    /// `execute_step` and the harness exits 0 immediately, so the race is
    /// genuinely exercised; a deterministic test command + a produced change
    /// make the pre-fix Aborted path the one that would otherwise be taken.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_natural_exit_with_pending_skip_resolves_to_skip_not_abort() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        // Serialize against other tests that mutate the process-global
        // cancel registry / park-kind slot (same rationale as the signal
        // module tests; current_thread runtime rules out guard transfer).
        let _registry_guard = crate::signal::lock_exit_cleanup_test();

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let shared = TempDir::new().unwrap();
        let marker_path = shared.path().join("started.marker");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("quick-harness.sh");
        // Announce we've started (so the skip task knows execute_step is
        // past its pre-attempt cancel check and the harness is genuinely
        // spawned), produce a change, then exit 0 after a tiny sleep —
        // racing the harness's natural exit against the skip the task is
        // about to fire. Whichever side of the `select!` wins, the invariant
        // must hold: outcome Skipped, never Aborted, exactly one log row.
        let script = format!(
            "#!/bin/sh\n\
             cat >/dev/null 2>&1 || true\n\
             echo 'edit' >> {readme}\n\
             : > {marker}\n\
             sleep 0.2\n\
             exit 0\n",
            readme = dir.join("README.md").to_string_lossy(),
            marker = marker_path.to_string_lossy(),
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        // Configure a deterministic test so the pre-fix Completed path would
        // reach the test phase → test_aborted → Aborted.
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &["true".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

        // Register the cancel channel + a park kind (mimicking
        // `request_skip_in_flight`), but do NOT latch `Skipped` yet — that
        // would trip the *pre-attempt* check and never exercise the
        // natural-exit race. A task fires `Skipped` only once the harness
        // has actually started (marker present), i.e. past the pre-attempt
        // check, so the skip lands concurrently with the harness's exit.
        let (tx, rx) = watch::channel(None);
        crate::signal::install_skip_channel_for_test(tx.clone());
        crate::signal::set_requested_park_kind_for_test(crate::git::ParkStrategyKind::Discard);

        let marker_clone = marker_path.clone();
        let tx_skip = tx.clone();
        let skip_task = tokio::spawn(async move {
            for _ in 0..200 {
                if marker_clone.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let _ = tx_skip.send(Some(CancelReason::Skipped));
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx.clone(),
                exec_opts: ExecuteOptions::default(),
            }),
        )
        .await
        .expect("execute_step did not return within 10s")
        .unwrap();

        skip_task.await.ok();

        assert_eq!(
            result.outcome,
            StepOutcome::Skipped,
            "natural-exit-vs-skip race must resolve to Skipped (advance one step)"
        );
        assert_ne!(
            result.outcome,
            StepOutcome::Aborted,
            "must NOT misclassify the race as a whole-run Aborted"
        );

        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Skipped);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(
            logs.len(),
            1,
            "exactly one execution_logs row (no finalize_failure + finalize_skipped double-write)"
        );
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped),
            "must record user_skipped, not user_interrupted"
        );

        // Fix 3: the terminal skip must have reset the cancel channel so a
        // following step would NOT be swept by a stale `Skipped`.
        assert!(
            rx.borrow().is_none(),
            "cancel channel must be cleared after a terminal skip so it \
             does not bleed into the next step"
        );
        // …and the park-kind slot must be empty (consumed exactly once).
        assert!(
            crate::signal::take_requested_park_kind().is_none(),
            "park-kind slot must be consumed exactly once on the skip path"
        );
    }

    /// Fix 3 — park-kind slot leak on non-skip terminal arms. If a `Skipped`
    /// reason was pending but the attempt finalized via a non-skip terminal
    /// arm (here: the harness exits non-zero with no changes → terminal
    /// `Failed`), the global park-kind slot and the cancel channel must be
    /// cleared so they can't bleed into a later attempt/step. A pending
    /// `Aborted`, by contrast, must survive (whole-run shutdown).
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_non_skip_terminal_clears_pending_skip_state() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let _registry_guard = crate::signal::lock_exit_cleanup_test();

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("fail-harness.sh");
        // Exit non-zero, no changes → HarnessFailed, terminal Failed (one
        // attempt, max_retries 0). A genuinely non-skip terminal arm.
        let script = "#!/bin/sh\nexit 3\n".to_string();
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                acceptance_criteria: &[],
                // max_retries = 0 → a single attempt, terminal on failure
                max_retries: Some(0),
                ..Default::default()
            },
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

        // A `Skipped` reason that "raced and lost": latched, plus a stale
        // park kind, but the attempt will finalize via the Failed terminal
        // arm (the harness exits non-zero before any skip handling).
        let (tx, rx) = watch::channel(None);
        crate::signal::install_skip_channel_for_test(tx.clone());
        crate::signal::set_requested_park_kind_for_test(crate::git::ParkStrategyKind::Commit);
        tx.send(Some(CancelReason::Skipped)).unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx.clone(),
                exec_opts: ExecuteOptions::default(),
            }),
        )
        .await
        .expect("execute_step did not return within 10s")
        .unwrap();

        // The harness exits non-zero; depending on the select! race this is
        // either a clean HarnessFailed or routed through the skip path. In
        // BOTH cases the defensive cleanup must leave NO stale skip state.
        assert!(
            matches!(result.outcome, StepOutcome::Failed | StepOutcome::Skipped),
            "expected Failed or Skipped, got {:?}",
            result.outcome
        );
        assert!(
            rx.borrow().is_none(),
            "a non-skip terminal arm (or the skip path) must clear the \
             latched Skipped so it can't leak into the next attempt/step"
        );
        assert!(
            crate::signal::take_requested_park_kind().is_none(),
            "the stale park-kind slot must be cleared on a non-skip terminal arm"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_fast_completed_attempt_clears_unconsumed_db_skip_request() {
        use crate::plan::ChangePolicy;
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("fast-harness.sh");
        fs::write(
            &harness_path,
            "#!/bin/sh\ncat >/dev/null 2>&1 || true\nexit 0\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("fast"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
        )
        .unwrap();

        storage::request_skip(
            &conn,
            &plan.id,
            &step.id,
            crate::git::ParkStrategyKind::Discard,
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "fast".to_string(),
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
            Duration::from_secs(10),
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
        )
        .await
        .expect("execute_step did not return within 10s")
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(
            storage::peek_skip_request(&conn, &plan.id)
                .unwrap()
                .is_none(),
            "a DB skip request that loses the race to natural completion must not survive"
        );
    }

    /// STEP 17 shared driver: run a step whose harness dirties the tree
    /// (a tracked modification + a new untracked file), then skip it
    /// in-flight with `kind`. Routes the skip through
    /// `signal::request_skip_in_flight` (exactly as `runner::skip_step`
    /// does) so the executor's `finalize_skipped` consumes the recorded
    /// park strategy. Returns `(dir, conn, step_id)` for per-strategy
    /// assertions.
    ///
    /// The skip trigger runs on a dedicated **OS thread** (`std::thread::spawn`
    /// with blocking `std::thread::sleep`), NOT a `tokio::spawn` co-located
    /// with `execute_step` on the test's `current_thread` runtime. Under heavy
    /// parallel `cargo test` load a single cooperative scheduler can starve a
    /// co-located skip future's timed poll loop long enough that
    /// `execute_step`'s 15s timeout fires first (`WaitResult::Timeout` instead
    /// of `Skipped` → rare spurious failure that always passes in isolation).
    /// A real thread is OS-preempted regardless of runtime load, so the
    /// readiness gate and skip request are immune to that starvation. This is
    /// a test-runtime artifact only: production runs the signal listener on a
    /// dedicated thread of a multi-threaded runtime.
    ///
    /// Holds `EXIT_CLEANUP_TEST_LOCK` across the `.await`s on purpose:
    /// `install_and_spawn` registers a process-global cancel TX and
    /// `request_skip_in_flight` mutates the global in-flight flag + park-kind
    /// slot, so this must be serialized against the other signal-registry
    /// tests. The non-`Send` `registry_guard` is acquired on, used by, and
    /// returned from the test's own runtime thread — only the skip *trigger*
    /// moves to the OS thread, so the guard never transfers across threads and
    /// switching to a `multi_thread` flavor (which would risk exactly that)
    /// stays unnecessary. The guard is returned to the caller so it stays
    /// alive until the test (and its per-strategy assertions) finishes.
    #[cfg(unix)]
    #[allow(clippy::await_holding_lock)]
    async fn run_inflight_skip_with_changes(
        kind: crate::git::ParkStrategyKind,
    ) -> (
        std::path::PathBuf,
        tempfile::TempDir,
        Connection,
        String,
        std::sync::MutexGuard<'static, ()>,
    ) {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let registry_guard = crate::signal::lock_exit_cleanup_test();

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let shared = TempDir::new().unwrap();
        let pid_path = shared.path().join("pid.txt");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("skip-harness.sh");
        // Dirty the repo (tracked edit + new untracked file), THEN announce
        // our pid and block so the skip lands with real work in the tree.
        let script = format!(
            "#!/bin/sh\n\
             echo 'harness edit' >> {readme}\n\
             echo 'agent output' > {agent}\n\
             echo \"$$\" > {pid}\n\
             sleep 60\n",
            readme = dir.join("README.md").to_string_lossy(),
            agent = dir.join("agent-new.txt").to_string_lossy(),
            pid = pid_path.to_string_lossy(),
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "demo-plan",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Wire the thing",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

        // Register a real cancel TX/RX pair in the process-global registry
        // (as the signal listener would for a live run) so
        // request_skip_in_flight injects into the channel execute_step
        // listens on.
        let (_handle, rx) = crate::signal::install_and_spawn_with_handle();

        let pid_path_clone = pid_path.clone();
        let dir_clone = dir.clone();
        // Drive the skip trigger on a REAL OS thread with BLOCKING sleeps,
        // not a `tokio::spawn` on `execute_step`'s `current_thread` runtime.
        // Rationale: `execute_step` and a co-located skip future share one
        // cooperative scheduler, so under heavy parallel `cargo test` load
        // the runtime can starve the skip future's `tokio::time::sleep` poll
        // loop long enough that `execute_step`'s 15s `tokio::time::timeout`
        // fires first → `WaitResult::Timeout` instead of `Skipped` → a rare
        // spurious failure (always passes in isolation). A dedicated thread
        // with `std::thread::sleep` is preempted by the OS regardless of
        // runtime load, so the readiness gate and skip request are immune to
        // single-runtime starvation. We deliberately do NOT switch the test
        // to the `multi_thread` flavor: the non-`Send` `EXIT_CLEANUP_TEST_LOCK`
        // `registry_guard` must not transfer across threads, and it stays
        // acquired on (and returned from) the test's own runtime thread —
        // only the skip trigger moves off it, so that invariant is preserved.
        let skip_thread = std::thread::spawn(move || {
            // Wait for the harness to have ACTUALLY produced the fixture
            // changes this test asserts on: the tracked README edit and the
            // new untracked file. A generic "tree is dirty" check is still a
            // little too weak under heavy parallel load: the skip can race in
            // after some unrelated write but before the exact discardable
            // work exists, which makes the `rolled_back=true` assertion flaky.
            // The bound is generous (≈30s of attempts) because the only
            // failure mode worth surfacing is the harness never running at
            // all, which the outer 15s `execute_step` timeout already covers.
            let mut dirtied = false;
            for _ in 0..600 {
                let pid_ready = pid_path_clone.exists()
                    && fs::read_to_string(&pid_path_clone)
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false);
                let readme_ready = fs::read_to_string(dir_clone.join("README.md"))
                    .map(|s| s.contains("harness edit"))
                    .unwrap_or(false);
                let agent_ready = dir_clone.join("agent-new.txt").exists();
                if pid_ready && readme_ready && agent_ready {
                    dirtied = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            assert!(
                dirtied,
                "harness never produced the expected worktree changes before skip — test setup race"
            );
            // Mark a step in-flight and request the skip exactly like
            // runner::skip_step's in-flight branch. Both calls are synchronous
            // and operate on process-global atomics/mutexes, so running them
            // on this OS thread is behaviorally identical to the prior
            // `tokio::spawn` — just immune to cooperative starvation.
            let _g = crate::signal::StepInFlightGuard::enter();
            assert!(
                crate::signal::request_skip_in_flight(kind),
                "request_skip_in_flight must signal when a step is in-flight"
            );
        });

        let result = tokio::time::timeout(
            Duration::from_secs(15),
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
        )
        .await
        .expect("execute_step did not return within 15s on skip")
        .unwrap();

        // Join the OS-thread skip trigger. `.join()` returns `Err` if the
        // thread panicked; resume-unwind so the in-thread `dirtied` /
        // `request_skip_in_flight` assertions still fail the test loudly
        // (they previously surfaced via the tokio `JoinError` path).
        if let Err(panic) = skip_thread.join() {
            std::panic::resume_unwind(panic);
        }

        assert_eq!(
            result.outcome,
            StepOutcome::Skipped,
            "skip must yield StepOutcome::Skipped"
        );
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Skipped);

        (dir, tmp, conn, step.id, registry_guard)
    }

    /// STEP 17: `--changes stash` parks the in-flight work in a
    /// `git stash` (labelled `ralph-skip/<slug>/<num>/<ts>`), leaves the
    /// tree clean of the harness's edits, and records committed=false.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_skip_changes_stash_parks_to_stash() {
        let (dir, _tmp, conn, step_id, _registry_guard) =
            run_inflight_skip_with_changes(crate::git::ParkStrategyKind::Stash).await;

        // The harness's tracked edit is gone from the worktree…
        assert!(
            !crate::git::has_uncommitted_changes(&dir).unwrap(),
            "stash must leave the tree clean of the skipped step's changes"
        );
        // …and recoverable from a ralph-skip-labelled stash entry.
        let stash_list = std::process::Command::new("git")
            .args(["stash", "list"])
            .current_dir(&dir)
            .output()
            .unwrap();
        let listing = String::from_utf8_lossy(&stash_list.stdout);
        assert!(
            listing.contains("ralph-skip/demo-plan/1/"),
            "stash list missing ralph-skip label: {listing}"
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one execution_log row");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped)
        );
        assert!(!logs[0].committed, "stash is not a commit");
        assert!(logs[0].commit_hash.is_none());
    }

    /// STEP 17: `--changes commit` parks the work as a WIP commit carrying
    /// the `Ralph-Skipped-Step: <step-id>` trailer; the log row is
    /// committed=true with the commit SHA in commit_hash.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_skip_changes_commit_makes_wip_commit_with_trailer() {
        let (dir, _tmp, conn, step_id, _registry_guard) =
            run_inflight_skip_with_changes(crate::git::ParkStrategyKind::Commit).await;

        // Tree is clean (everything was committed).
        assert!(!crate::git::has_uncommitted_changes(&dir).unwrap());

        let body = std::process::Command::new("git")
            .args(["log", "-1", "--format=%B"])
            .current_dir(&dir)
            .output()
            .unwrap();
        let body = String::from_utf8_lossy(&body.stdout);
        assert!(
            body.contains("[ralph wip] skipped step 1: Wire the thing"),
            "WIP commit subject wrong: {body}"
        );
        assert!(
            body.contains(&format!("Ralph-Skipped-Step: {step_id}")),
            "WIP commit missing step-id trailer: {body}"
        );

        let head = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&dir)
            .output()
            .unwrap();
        let head_sha = String::from_utf8_lossy(&head.stdout).trim().to_string();

        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one execution_log row");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped)
        );
        assert!(logs[0].committed, "commit strategy must set committed=true");
        assert_eq!(
            logs[0].commit_hash.as_deref(),
            Some(head_sha.as_str()),
            "commit_hash must be the WIP commit SHA"
        );
    }

    /// STEP 17: `--changes discard` throws the in-flight work away; the
    /// tree returns to the last commit and the log row is committed=false.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_skip_changes_discard_drops_the_work() {
        let (dir, _tmp, conn, step_id, _registry_guard) =
            run_inflight_skip_with_changes(crate::git::ParkStrategyKind::Discard).await;

        assert!(
            !crate::git::has_uncommitted_changes(&dir).unwrap(),
            "discard must restore a clean tree"
        );
        // The tracked file is back to its committed contents and the
        // harness's new untracked file is gone.
        assert_eq!(
            std::fs::read_to_string(dir.join("README.md")).unwrap(),
            "init"
        );
        assert!(!dir.join("agent-new.txt").exists());

        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 1, "exactly one execution_log row");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped)
        );
        assert!(!logs[0].committed);
        assert!(logs[0].commit_hash.is_none());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_finalize_skipped_discard_records_rolled_back() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let conn = crate::db::open_memory().unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        std::fs::write(dir.join("README.md"), "modified by harness").unwrap();
        std::fs::write(dir.join("agent-new.txt"), "agent output").unwrap();

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let pre: Vec<String> = vec![];
        let ctx = ExecCtx {
            conn: &conn,
            plan: &plan,
            step: &step,
            workdir: &dir,
            pre_existing_untracked: &pre,
            hook_ctx: &hook_ctx,
            step_num: 1,
            max_attempts: 1,
            json_output: false,
        };
        let exec_log_id = storage::create_execution_log(&conn, &step.id, 1, None, None)
            .unwrap()
            .id;

        let result = finalize_skipped(
            &ctx,
            SkippedArgs {
                exec_log_id,
                duration_secs: 0.1,
                attempt: 1,
                stdout: "",
                stderr: "",
                kind: crate::git::ParkStrategyKind::Discard,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Skipped);
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].rolled_back, "discard records rolled_back=true");
        assert!(!logs[0].committed);
        assert!(logs[0].commit_hash.is_none());
    }

    /// STEP 18: the TUI skip dialog's Esc/cancel path. A skip request
    /// carrying [`crate::git::ParkStrategyKind::Cancel`] must:
    ///   - kill the in-flight harness (same cancel ladder),
    ///   - roll the tree back (preserving pre-existing untracked scratch),
    ///   - write NO `execution_logs` row for the cancelled attempt,
    ///   - re-enter the executor at the *SAME* attempt number (no retry
    ///     budget consumed).
    ///
    /// Mechanism under test: the harness records every invocation. The skip
    /// task issues `Cancel` on invocation #1 (executor rolls back, deletes
    /// the attempt-1 log row, resets the cancel channel + attempt counter,
    /// re-enters), then `Skipped`(Stash) on invocation #2 so the re-entered
    /// attempt finalizes and the test terminates. Final assertions prove the
    /// re-entry happened at attempt 1 (budget untouched) and the cancelled
    /// attempt left no log row behind.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_tui_skip_cancel_reenters_same_attempt_no_budget_no_log_row() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;
        use tempfile::TempDir;

        let _registry_guard = crate::signal::lock_exit_cleanup_test();

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // A pre-existing untracked file the user had *before* the run. The
        // cancel rollback must preserve it.
        fs::write(dir.join("user-scratch.txt"), "user data").unwrap();

        let shared = TempDir::new().unwrap();
        let count_path = shared.path().join("invocations.txt");
        let pid_path = shared.path().join("pid.txt");

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("cancel-harness.sh");
        // Every invocation: append a marker to the counter file, dirty the
        // tree (tracked edit + new untracked file), publish our pid, block.
        let script = format!(
            "#!/bin/sh\n\
             echo x >> {count}\n\
             echo 'harness edit' >> {readme}\n\
             echo 'agent output' > {agent}\n\
             echo \"$$\" > {pid}\n\
             sleep 60\n",
            count = count_path.to_string_lossy(),
            readme = dir.join("README.md").to_string_lossy(),
            agent = dir.join("agent-new.txt").to_string_lossy(),
            pid = pid_path.to_string_lossy(),
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "demo-plan",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Wire the thing",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

        let (_handle, rx) = crate::signal::install_and_spawn_with_handle();

        // Wait until the counter file shows `target` invocations, then
        // return once the harness has (re-)published a pid for that run.
        async fn wait_for_invocation(
            count_path: &std::path::Path,
            pid_path: &std::path::Path,
            target: usize,
        ) {
            for _ in 0..240 {
                let n = std::fs::read_to_string(count_path)
                    .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
                    .unwrap_or(0);
                if n >= target
                    && pid_path.exists()
                    && std::fs::read_to_string(pid_path)
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("harness never reached invocation {target}");
        }

        let count_clone = count_path.clone();
        let pid_clone = pid_path.clone();
        let skip_task = tokio::spawn(async move {
            // Invocation #1 → request CANCEL.
            wait_for_invocation(&count_clone, &pid_clone, 1).await;
            let _g = crate::signal::StepInFlightGuard::enter();
            assert!(
                crate::signal::request_skip_in_flight(crate::git::ParkStrategyKind::Cancel),
                "Cancel skip must signal when a step is in-flight"
            );
            // Invocation #2 (the re-entered SAME attempt) → finalize with a
            // real Skipped(Discard) so the test terminates. Discard is used
            // (not Stash) because Stash's `--include-untracked` would also
            // sweep up the user's pre-existing scratch, masking the
            // preservation assertion below; Discard routes through
            // `rollback_except(pre_existing_untracked)`, the same
            // preservation contract the cancel rollback uses.
            wait_for_invocation(&count_clone, &pid_clone, 2).await;
            assert!(
                crate::signal::request_skip_in_flight(crate::git::ParkStrategyKind::Discard),
                "second skip must signal when the re-entered attempt is in-flight"
            );
        });

        let result = tokio::time::timeout(
            Duration::from_secs(20),
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
        )
        .await
        .expect("execute_step did not return within 20s on cancel+skip")
        .unwrap();

        skip_task.await.ok();

        // The harness ran exactly twice: the cancelled attempt and the
        // re-entered same-numbered attempt.
        let invocations = std::fs::read_to_string(&count_path)
            .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
            .unwrap_or(0);
        assert_eq!(
            invocations, 2,
            "harness must run twice (cancelled attempt + re-entered same attempt)"
        );

        // The step finalized as Skipped at attempt 1 — the cancelled
        // attempt consumed NO retry budget (otherwise this would be 2).
        assert_eq!(result.outcome, StepOutcome::Skipped);
        assert_eq!(
            result.attempts_used, 1,
            "re-entry must reuse the SAME attempt number (no budget consumed)"
        );
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Skipped);
        assert_eq!(
            updated.attempts, 1,
            "persisted attempt counter must not advance for the cancelled attempt"
        );

        // Exactly ONE execution_logs row exists — the cancelled attempt's
        // row (created with the prompt before the harness spawned) was
        // deleted, so there is no UNIQUE(step_id, attempt) leak and no row
        // for the cancelled attempt.
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(
            logs.len(),
            1,
            "the cancelled attempt must leave NO execution_logs row; only the \
             final Skipped row should remain (got {logs:?})"
        );
        assert_eq!(logs[0].attempt, 1, "the surviving row is attempt 1");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::UserSkipped),
            "the surviving row is the final Skipped(Stash) finalize"
        );

        // The user's pre-existing untracked scratch survived the cancel
        // rollback untouched.
        assert!(
            dir.join("user-scratch.txt").exists(),
            "cancel rollback must preserve pre-existing untracked files"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("user-scratch.txt")).unwrap(),
            "user data"
        );
    }

    /// STEP 18: the executor's cancel path constructs exactly the documented
    /// `attempt_cancelled` NDJSON event (event tag, `step_id`, `step_num`
    /// derived from the i32 `ExecCtx.step_num`, `attempt`). Pairs with the
    /// `output.rs` serde-shape tests (casing / field names) and the
    /// integration test (the cancel branch is actually taken). Building the
    /// event is the seam `cancel_skipped_attempt` emits through, so this
    /// proves the executor emits the right payload without flaky stdout
    /// capture.
    #[test]
    fn test_attempt_cancelled_event_payload_from_exec_ctx() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "demo-plan",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("skip"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Wire the thing",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let pre: Vec<String> = vec![];
        let ctx = ExecCtx {
            conn: &conn,
            plan: &plan,
            step: &step,
            workdir: &dir,
            pre_existing_untracked: &pre,
            hook_ctx: &hook_ctx,
            step_num: 4,
            max_attempts: 3,
            json_output: true,
        };

        let evt = attempt_cancelled_event(&ctx, 2);
        match evt {
            crate::output::RunEvent::AttemptCancelled {
                step_id,
                step_num,
                attempt,
                ..
            } => {
                assert_eq!(step_id, step.id);
                assert_eq!(step_num, 4, "i32 step_num maps to usize");
                assert_eq!(attempt, 2, "carries the cancelled attempt number");
            }
            other => panic!("expected AttemptCancelled, got {other:?}"),
        }

        // The event must serialize to the documented tag/casing too.
        let val: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&attempt_cancelled_event(&ctx, 1)).unwrap(),
        )
        .unwrap();
        assert_eq!(val["event"], "attempt_cancelled");
        assert_eq!(val["step_id"], step.id);
        assert_eq!(val["step_num"], 4);
        assert_eq!(val["attempt"], 1);
        assert!(val.get("at").is_some(), "timestamp field present");
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("trap"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            execute_step(ExecuteStepArgs {
                conn: &conn,
                plan: &plan,
                step: &step,
                config: &config,
                workdir: &dir,
                hook_ctx: &hook_ctx,
                abort_rx: rx,
                exec_opts: ExecuteOptions::default(),
            }),
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());

        // Default change_policy = Required.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());

        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &["true".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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

    // ---- Step 22: retry loop preserves the dirty tree between attempts ----

    /// Count the commits reachable from HEAD (for double-commit assertions).
    #[cfg(test)]
    fn commit_count(workdir: &std::path::Path) -> usize {
        use std::process::Command;
        let out = Command::new("git")
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(workdir)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
    }

    /// `Keep` (the default) must NOT roll back between failed attempts: the
    /// dirty tree carries forward so the next attempt builds on it. The
    /// harness appends one line to a tracked file per invocation; the
    /// deterministic test only passes once the file has TWO lines. With Keep,
    /// attempt 1's line survives into attempt 2 (no rollback), attempt 2
    /// appends the second line, the test passes, and exactly ONE step commit
    /// results.
    #[tokio::test(flavor = "current_thread")]
    async fn test_keep_strategy_preserves_dirty_tree_between_attempts() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        // Seed a tracked file the harness will append to.
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();
        let base_commits = commit_count(&dir);

        // Harness: append exactly one line per invocation. No commit.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append-harness.sh");
        let script = format!(
            "#!/bin/sh\necho line >> {0}/acc.txt\nexit 0\n",
            dir.to_string_lossy()
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        // Test passes only when acc.txt has exactly two lines — i.e. attempt
        // 1's append survived (no rollback) AND attempt 2 appended again.
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 1 → 2 attempts. Failed attempts preserve the dirty
        // tree (test-then-commit; nothing to roll back).
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "Keep must carry attempt 1's append into attempt 2 so the \
             2-line test passes on attempt 2",
        );
        assert_eq!(result.attempts_used, 2);
        // Phase A (test-then-commit): no commit happens on attempt 1 (test
        // fails), the dirty tree carries forward, attempt 2 appends the
        // second line, test passes, and a SINGLE commit lands. The audit
        // trail collapses to one commit per step (zero on terminal
        // failure).
        assert_eq!(
            commit_count(&dir),
            base_commits + 1,
            "Phase A: exactly one commit total (only after final passing attempt)"
        );
        let final_lines = fs::read_to_string(dir.join("acc.txt")).unwrap();
        assert_eq!(
            final_lines.lines().count(),
            2,
            "both attempts' appends are present (Keep never reverts)"
        );
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        // Attempt 1 failed and did NOT roll back under Keep — its iteration
        // commit stays in history and attempt 2 builds on it.
        let a1 = logs.iter().find(|l| l.attempt == 1).unwrap();
        assert!(
            !a1.rolled_back,
            "Keep must not roll back the failed iteration"
        );
        // The final (successful) commit subject is the per-iteration format.
        let head_msg = {
            let out = std::process::Command::new("git")
                .args(["log", "-1", "--pretty=%s"])
                .current_dir(&dir)
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        assert!(
            head_msg.starts_with(&format!("ralph {}.2 - ", step.short_id)),
            "final commit subject must be the per-iteration format; got: {head_msg}"
        );
        // And the trailers are present + correct on the final commit.
        let head_sha = crate::git::get_commit_hash(&dir).unwrap();
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_STEP_TRAILER)
                .unwrap()
                .as_deref(),
            Some(step.short_id.as_str())
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_NUM_TRAILER)
                .unwrap()
                .as_deref(),
            Some("2")
        );
    }

    /// EDGE CASE (Step 22): under `Keep`, attempt 1's agent commits its own
    /// work and the test then fails (agent_committed_clean). We must NOT roll
    /// back (Keep), but we also must NOT leave the agent's commit in HEAD —
    /// otherwise the eventual success commit would be a SECOND commit on top
    /// of it. The fix mixed-resets to the pre-attempt HEAD, so attempt 1's
    /// work survives as uncommitted changes; attempt 2 adds its line and the
    /// success path produces exactly ONE coherent step commit (no
    /// double-commit, no orphan agent commit, no "nothing to commit").
    #[tokio::test(flavor = "current_thread")]
    async fn test_keep_agent_committed_clean_single_final_commit() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();
        let base_commits = commit_count(&dir);
        let base_head = crate::git::get_commit_hash(&dir).unwrap();

        // A counter so the harness behaves differently per invocation.
        let shared = TempDir::new().unwrap();
        let count_path = shared.path().join("n.txt");

        // Invocation 1: append line1 AND commit it (agent_committed_clean).
        // Invocation 2+: append another line, do NOT commit.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("commit-then-dirty.sh");
        let script = format!(
            "#!/bin/sh\n\
             echo x >> {count}\n\
             N=$(wc -l < {count})\n\
             echo line >> {acc}\n\
             if [ \"$N\" -eq 1 ]; then\n\
               cd {dir}\n\
               git add -A\n\
               git -c user.email=a@a -c user.name=a commit -m 'agent commit' >/dev/null\n\
             fi\n\
             exit 0\n",
            count = count_path.to_string_lossy(),
            acc = dir.join("acc.txt").to_string_lossy(),
            dir = dir.to_string_lossy(),
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        // Passes only when acc.txt has two lines (attempt 1's carried-forward
        // line + attempt 2's new line).
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "attempt 2 should pass once attempt 1's (un-committed) line is \
             carried forward and a second line is added",
        );
        assert_eq!(result.attempts_used, 2);
        // THE key assertion: exactly ONE new commit total. The agent's
        // attempt-1 commit was mixed-reset away (un-committed but kept on
        // disk), so the only new commit is the single ralph step commit.
        assert_eq!(
            commit_count(&dir),
            base_commits + 1,
            "exactly one step commit — no double-commit, no orphan agent commit"
        );
        // The single new commit is ralph's step commit, parented on base.
        let head = crate::git::get_commit_hash(&dir).unwrap();
        assert_ne!(head, base_head);
        assert_eq!(
            result.commit_hash.as_deref(),
            Some(head.as_str()),
            "the success commit is the step commit ralph created"
        );
        let msg = {
            let out = std::process::Command::new("git")
                .args(["log", "-1", "--pretty=%s"])
                .current_dir(&dir)
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        // PER-ITERATION COMMIT format (docs/dag-redesign.md §3.2/§5): the
        // final commit is ralph's iteration `.2` commit (attempt 1 was
        // agent_committed_clean → NO ralph commit, only attempt 2 made one),
        // not the agent's orphan 'agent commit'. Subject is the new
        // `ralph <short_id>.<n> - <title>` format (pre-DAG was
        // `ralph: <title> [step:..., attempt:...]`).
        assert!(
            msg.starts_with(&format!("ralph {}.2 - Acc", step.short_id)),
            "the final commit must be ralph's per-iteration step commit, not \
             the agent's orphan 'agent commit'; got: {msg}"
        );
        // Trailers are present + correct (tooling parses these, not the
        // subject).
        let head_sha = crate::git::get_commit_hash(&dir).unwrap();
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_PLAN_TRAILER)
                .unwrap()
                .as_deref(),
            Some("slug")
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_STEP_TRAILER)
                .unwrap()
                .as_deref(),
            Some(step.short_id.as_str())
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_NUM_TRAILER)
                .unwrap()
                .as_deref(),
            Some("2")
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head_sha, crate::git::ITERATION_REVIEW_TRAILER)
                .unwrap()
                .as_deref(),
            Some("pending")
        );
        // Attempt 1 failed as agent_committed_clean → classified NoChanges,
        // and Keep did NOT roll back (the mixed-reset is not a rollback of
        // the working tree — the line is still there).
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        let a1 = logs.iter().find(|l| l.attempt == 1).unwrap();
        assert_eq!(a1.termination_reason, Some(TerminationReason::NoChanges));
        let final_lines = fs::read_to_string(dir.join("acc.txt")).unwrap();
        assert_eq!(
            final_lines.lines().count(),
            2,
            "both attempts' lines are present in the final tree"
        );
    }

    /// Build an executable harness script at `harness_dir` that appends one
    /// line to `acc.txt` in `workdir` per invocation. Written OUTSIDE the
    /// workdir so the script isn't counted as a step change.
    #[cfg(test)]
    fn write_append_line_harness(
        harness_dir: &std::path::Path,
        workdir: &std::path::Path,
    ) -> std::path::PathBuf {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let p = harness_dir.join("append.sh");
        fs::write(
            &p,
            format!(
                "#!/bin/sh\necho line >> {0}/acc.txt\nexit 0\n",
                workdir.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&p).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&p, perms).unwrap();
        p
    }

    /// Phase A: the commit happens AFTER the deterministic test passes.
    /// The test command asserts the worktree (not HEAD) — so the harness's
    /// uncommitted changes drive the test, and the commit only runs on
    /// success. The commit subject is the `ralph <short_id>.<n> - <title>`
    /// format and carries all four `Ralph-*` trailers (tooling parses
    /// trailers, never the subject).
    #[tokio::test(flavor = "current_thread")]
    async fn test_per_iteration_commit_subject_trailers_after_test() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let base_commits = commit_count(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_append_line_harness(harness_tmp.path(), &dir);

        let conn = crate::db::open_memory().unwrap();
        // Test inspects the WORKTREE (uncommitted harness changes), not
        // HEAD — under Phase A the commit happens after the test passes.
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 1",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Add the thing",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "test reads the worktree (Phase A: commit after test passes)"
        );
        assert_eq!(result.attempts_used, 1);
        // Exactly ONE commit was created (after the test passed).
        assert_eq!(commit_count(&dir), base_commits + 1);
        let head = result.commit_hash.clone().unwrap();
        let subject = {
            let out = std::process::Command::new("git")
                .args(["log", "-1", "--pretty=%s", &head])
                .current_dir(&dir)
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        assert_eq!(
            subject,
            format!("ralph {}.1 - Add the thing", step.short_id),
            "subject format: ralph <short_id>.<n> - <sanitized title>"
        );
        // All four trailers present + correct.
        assert_eq!(
            crate::git::parse_trailer(&dir, &head, crate::git::ITERATION_PLAN_TRAILER)
                .unwrap()
                .as_deref(),
            Some("slug")
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head, crate::git::ITERATION_STEP_TRAILER)
                .unwrap()
                .as_deref(),
            Some(step.short_id.as_str())
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head, crate::git::ITERATION_NUM_TRAILER)
                .unwrap()
                .as_deref(),
            Some("1")
        );
        assert_eq!(
            crate::git::parse_trailer(&dir, &head, crate::git::ITERATION_REVIEW_TRAILER)
                .unwrap()
                .as_deref(),
            Some("pending")
        );
    }

    /// Phase A: a multi-iteration run produces exactly ONE commit total
    /// (only after the final attempt passes tests). Iteration 1 leaves a
    /// dirty tree (1 line appended, test fails, no commit). Iteration 2
    /// appends a second line to that dirty tree (test passes, commit).
    /// The deterministic-test failure of iteration 1 feeds the next
    /// prompt's retry context.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failing_then_passing_attempts_produce_single_commit() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let base_commits = commit_count(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_append_line_harness(harness_tmp.path(), &dir);

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert_eq!(result.attempts_used, 2);
        assert_eq!(
            commit_count(&dir),
            base_commits + 1,
            "Phase A: exactly one commit total (only after final passing attempt)"
        );
        // The single commit is tagged for the final (passing) iteration.
        let its = crate::git::iteration_commits_for_step(&dir, "HEAD", &step.short_id).unwrap();
        let iters: Vec<i32> = its.iter().map(|c| c.iteration).collect();
        assert_eq!(iters, vec![2], "the only commit is iteration 2");
        // Iteration 1's deterministic-test failure fed attempt 2's prompt.
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        let a2 = logs.iter().find(|l| l.attempt == 2).unwrap();
        let a2_prompt = a2.prompt_text.as_deref().unwrap_or("");
        assert!(
            a2_prompt.contains("# Retry Context"),
            "iteration 1's test failure must feed iteration 2's retry context"
        );
    }

    /// Phase A: at most one commit per step regardless of how many attempts
    /// it took — a 2-iteration run leaves exactly one ralph commit (the
    /// final passing one).
    #[tokio::test(flavor = "current_thread")]
    async fn test_at_most_one_commit_per_step() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let base_commits = commit_count(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_append_line_harness(harness_tmp.path(), &dir);

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        // Phase A: at most one commit per step — there is no multi-iteration
        // run to collapse.
        assert_eq!(
            commit_count(&dir),
            base_commits + 1,
            "Phase A: exactly one commit per step"
        );
        let its = crate::git::iteration_commits_for_step(&dir, "HEAD", &step.short_id).unwrap();
        assert_eq!(its.len(), 1, "exactly one commit (the final passing one)");
    }

    /// STEP 42 / docs/dag-redesign.md §3.3/§6/§7 — the **disabled review
    /// fast path**: when review is effective-DISABLED at any scope the step
    /// goes straight to `Complete` from passing tests with
    /// `review_status = Disabled` and **NO reviewer is ever spawned**.
    ///
    /// The no-spawn proof is structural *and* observable here:
    /// 1. `execute_step` returns `needs_review: None` — and the runner's
    ///    reviewer block is gated entirely on `needs_review: Some(..)`, so
    ///    `None` means the runner can never enter `review::run_review`.
    /// 2. We additionally wire a review harness whose script drops a
    ///    sentinel file the instant it is invoked, and assert that sentinel
    ///    never appears: even if some future refactor moved the spawn into
    ///    the executor, this would catch it.
    /// Review is disabled here at the *step* scope (`Some(false)`), which —
    /// per the precedence chain — also proves a step override beats an
    /// enabled global config.
    #[tokio::test(flavor = "current_thread")]
    async fn test_review_disabled_fast_path_no_spawn_marks_disabled_complete() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_append_line_harness(harness_tmp.path(), &dir);

        // A "reviewer" that, if EVER spawned, drops a sentinel file. The
        // test asserts the sentinel never exists.
        let sentinel = harness_tmp.path().join("REVIEWER_WAS_SPAWNED");
        let reviewer_path = harness_tmp.path().join("reviewer.sh");
        fs::write(
            &reviewer_path,
            format!(
                "#!/bin/sh\ntouch {}\necho 'REVIEW PASS'\nexit 0\n",
                sentinel.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&reviewer_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&reviewer_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 1",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        // Step-scope OFF override — wins over the enabled global config.
        storage::set_step_review_enabled(&conn, &step.id, Some(false)).unwrap();
        let step = storage::get_step(&conn, &step.id).unwrap();

        let mut config = Config::default();
        config
            .harnesses
            .insert("poly".to_string(), harness_config_for_script(&harness_path));
        config.harnesses.insert(
            "reviewer".to_string(),
            harness_config_for_script(&reviewer_path),
        );
        // Global review is ENABLED + a real review harness is configured,
        // so the only thing keeping the reviewer from spawning is the
        // step-scope OFF override resolving via effective_review_enabled.
        config.review.enabled = Some(true);
        config.review.harness = "reviewer".to_string();

        // Sanity: effective review really is disabled for this step.
        assert!(
            !crate::config::effective_review_enabled(&step, &plan, &config),
            "step-scope OFF must make effective review disabled"
        );

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        // (1) The fast path returns no review request: the runner can never
        //     enter the reviewer block.
        assert!(
            result.needs_review.is_none(),
            "disabled review must NOT hand a review request back to the runner"
        );
        // (2) The reviewer sentinel must never have been written.
        assert!(
            !sentinel.exists(),
            "the reviewer harness must NEVER be spawned on the disabled fast path"
        );
        // (3) The step is Complete straight from passing tests with
        //     review_status = Disabled (not the on-disk NULL/Pending).
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Complete);
        assert_eq!(s.review_status, Some(crate::plan::ReviewStatus::Disabled));
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &["true".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &["false".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();
        // Phase B: an Optional-policy step whose tests fail with the retry
        // budget exhausted now routes through the auto-raised blocker (a
        // failing-test signal is productively retryable). The outcome is
        // `PausedForQuestion`; the log row's `termination_reason` is
        // `PausedForQuestion` (reused — see `raise_retry_exhausted_blocker`)
        // and `test_status` still reflects what the test phase observed
        // (`Failed`) so the audit trail preserves the underlying signal.
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::PausedForQuestion),
            "Phase B: exhausted-budget test failure parks the step (paused_for_question)",
        );
        assert_eq!(
            logs[0].test_status,
            Some(TestStatus::Failed),
            "test_status preserves the underlying signal",
        );
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].kind, InterruptionKind::Blocker);
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &["true".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Implement",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
    /// writes a file inside `workdir` first to produce a dirty tree.
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("exit1"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: // no retries
            None, change_policy: None, tags: None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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

    /// Phase A: a crashing harness that leaves partial work on disk must let
    /// the next attempt build on it (failed attempts preserve the dirty
    /// tree).
    ///
    /// Attempt 1 appends one line then exits 1; attempt 2 appends the second
    /// line and exits 0 only if attempt 1's work survived.
    #[tokio::test(flavor = "current_thread")]
    async fn test_harness_failure_preserves_dirty_tree() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();

        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append-then-exit.sh");
        let script = format!(
            "#!/bin/sh\n\
             echo line >> {0}/acc.txt\n\
             if [ \"$(wc -l < {0}/acc.txt)\" -ge 2 ]; then\n\
               exit 0\n\
             fi\n\
             exit 1\n",
            dir.to_string_lossy()
        );
        fs::write(&harness_path, script).unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(
            result.commit_hash.is_some(),
            "the second attempt should succeed by building on the first attempt's dirty tree"
        );

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 2, "one failed attempt, then one success");
        assert_eq!(
            logs[0].termination_reason,
            Some(TerminationReason::HarnessFailed)
        );
        assert!(
            !logs[0].rolled_back,
            "failed attempts preserve the dirty tree"
        );
        assert_eq!(logs[1].termination_reason, Some(TerminationReason::Success));
        assert_eq!(
            fs::read_to_string(dir.join("acc.txt"))
                .unwrap()
                .lines()
                .count(),
            2,
            "the second attempt must see attempt 1's line still on disk"
        );
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("exit1"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Review",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: Some(ChangePolicy::Optional),
                tags: None,
            },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("exit1"),
                agent: None,
                // Tests that always pass — they should NOT be run, so this
                // choice is immaterial except to prove that even if someone
                // later changes the code to run them, they couldn't rescue the
                // attempt.
                deterministic_tests: &["true".to_string()],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: None, change_policy: None, tags: // Required
            None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("exit1"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(2), model: // 2 retries = 3 total attempts
            None, change_policy: None, tags: None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
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

    /// Insert an open *native* interruption tagged to a given (step,
    /// attempt). Simulates what the harness would do via `ralph question
    /// ask`. Used by the interruption-pause integration tests to drive the
    /// "harness raised an interruption" branch in `execute_step`. Native
    /// `interruptions` table — no `step_questions`.
    #[cfg(test)]
    fn insert_unanswered_question(conn: &Connection, step_id: &str, attempt: i32, question: &str) {
        storage::insert_interruption(
            conn,
            step_id,
            attempt,
            crate::plan::InterruptionKind::Question,
            question,
            &[],
        )
        .expect("seed native interruption row");
    }

    /// Build a minimal Config registering the given harness path under `name`.
    #[cfg(test)]
    fn config_with_harness(name: &str, harness_path: &std::path::Path) -> Config {
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

    /// Pause path with a clean-exit, no-diff harness. The harness runs
    /// cleanly but an open interruption exists for (step, attempt=1).
    /// Expected: outcome PausedForQuestion, step status reset to Pending,
    /// exec_log row carries paused_for_question + NotRun, no commit.
    ///
    /// **Zero retry budget (HARD invariant — docs/dag-redesign.md §3.4 / §9
    /// invariant 4).** The retry loop bumps `step.attempts` to 1 *before*
    /// the harness spawns; the interruption pause must roll that back so the
    /// persisted counter is **0** afterward — the resumed run re-runs the
    /// same attempt #1 once the interruption is resolved, consuming no
    /// retry. This test is the proof.
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
            crate::storage::NewPlan { slug: "slug", project: &dir.to_string_lossy(), branch_name: "branch", description: "desc", harness: Some("noop"), agent: None, deterministic_tests: // Configure deterministic tests so we can assert they were skipped
            // — pause must skip the test phase even when tests are configured.
            &["true".to_string()] },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(2), model: // budget > 1 to confirm pause does not retry
            None, change_policy: None, tags: None },
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        assert_eq!(result.attempts_used, 1);
        assert!(result.commit_hash.is_none());

        // Step status returned to Pending so a re-run picks it up cleanly.
        // ZERO RETRY BUDGET: the pre-spawn `set_step_attempts(.. 1)` is
        // rolled back, so the persisted counter is 0 — the resumed run
        // re-runs attempt #1, not #2. (docs/dag-redesign.md §3.4 / §9
        // invariant 4, mirroring the skip-dialog cancel path.)
        let updated = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Pending);
        assert_eq!(
            updated.attempts, 0,
            "interruption pause must consume NO retry budget (HARD invariant)"
        );

        // NO exec_log row survives the pause. The paused attempt's row is
        // deleted (zero-budget: the re-run re-uses attempt #1, so leaving
        // the row would collide on UNIQUE(step_id, attempt)). The durable
        // record of the pause is the open `interruptions` row, asserted
        // below via the derived Interrupted status.
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert!(
            logs.is_empty(),
            "interruption pause must delete its exec_log row (zero-budget \
             re-run re-uses the same attempt number)"
        );

        // HEAD did not advance — pause skipped the commit.
        let head_after = crate::git::get_commit_hash(&dir).unwrap();
        assert_eq!(head_before, head_after, "pause must not advance HEAD");

        // The plan's effective status is now Interrupted (derived) even
        // though the underlying plans.status column may still be in_progress.
        // (The harness wrote an open native `interruptions` row that the
        // derivation reads.)
        let effective = storage::plan_effective_status(&conn, &plan.id).unwrap();
        assert_eq!(effective, crate::plan::PlanStatus::Interrupted);
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("touchy"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);

        // Workdir must be clean after pause so the scheduler can move on,
        // but the harness's diff is preserved in a parked stash for later
        // re-application when the interruption is resolved.
        assert!(
            !crate::git::has_uncommitted_changes(&dir).unwrap(),
            "pause must leave the worktree clean after parking the diff"
        );
        assert!(
            !dir.join("ralph-test-output.txt").exists(),
            "parked path: harness's file must be absent until the stash is restored"
        );

        // The paused attempt's exec_log row is deleted (zero-budget re-run
        // re-uses the same attempt number). The durable records are the open
        // interruption row and the parked stash pointer.
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert!(
            logs.is_empty(),
            "interruption pause must delete its exec_log row"
        );
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1, "the open interruption is the durable record");
        let parked = storage::get_step_parked_worktree(&conn, &step.id)
            .unwrap()
            .expect("pause with a diff must park a stash");
        let popped =
            crate::git::stash_pop(&dir, &crate::git::StashRef(parked.stash_sha.clone())).unwrap();
        assert_eq!(popped, crate::git::StashPopOutcome::Clean);
        assert!(
            dir.join("ralph-test-output.txt").exists(),
            "restoring the parked stash must recover the harness's file"
        );
    }

    /// STEP 24 — cross-process interruption bridge, end-to-end:
    ///
    /// 1. The harness raises an interruption (open native `interruptions`
    ///    row) and exits. `execute_step` pauses, marks the branch Blocked
    ///    (derived), and — the HARD invariant — consumes **zero retry
    ///    budget**: a `max_retries = 1` step is *not* exhausted by the
    ///    block.
    /// 2. A *different process* resolves the interruption (modeled here by
    ///    calling `storage::resolve_interruption` directly — same DB write
    ///    the `interruption resolve` CLI / TUI inbox performs).
    /// 3. The runner re-runs the step at the **same attempt #1** (budget was
    ///    not consumed) and it completes. If the block had burned the single
    ///    retry, this second run would instead exhaust the budget and fail —
    ///    so a green second run *is* the zero-budget proof across the bridge.
    #[tokio::test(flavor = "current_thread")]
    async fn test_interruption_bridge_zero_budget_then_cross_process_resolve_requeues() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let harness_tmp = TempDir::new().unwrap();
        // First run: a clean no-op harness (the agent asked, then exited).
        let noop = write_noop_harness(harness_tmp.path());

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("noop"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 1: the whole budget is a single attempt. If the
        // interruption pause consumed it, the re-run could not succeed.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // The agent raised an interruption on attempt 1.
        insert_unanswered_question(&conn, &step.id, 1, "Which DB?");

        let cfg_noop = config_with_harness("noop", &noop);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let paused = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &cfg_noop,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();
        assert_eq!(paused.outcome, StepOutcome::PausedForQuestion);

        // ZERO RETRY BUDGET: the pre-spawn bump was rolled back.
        let after_pause = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(
            after_pause.attempts, 0,
            "interruption pause must consume NO retry budget"
        );
        assert_eq!(after_pause.status, StepStatus::Pending);
        // Derived Blocked overlay shadows Pending while the interruption is
        // open.
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1, "the open interruption is the bridge row");
        let parked = storage::get_step_parked_worktree(&conn, &step.id).unwrap();

        // --- A DIFFERENT PROCESS resolves it (same write the CLI does). ---
        storage::resolve_interruption(&conn, &open[0].id, "SQLite", None).unwrap();
        assert!(
            storage::list_open_interruptions_for_plan(&conn, &plan.id)
                .unwrap()
                .is_empty(),
            "resolution clears the bridge row → step leaves Blocked"
        );

        // Re-run: the runner restores the parked stash before re-queueing the
        // step at attempt #1 (budget intact). Use a harness that produces a
        // change so the step can complete.
        let resumed_parked_worktree = if let Some(parked) = parked {
            let popped =
                crate::git::stash_pop(&dir, &crate::git::StashRef(parked.stash_sha.clone()))
                    .unwrap();
            assert_eq!(popped, crate::git::StashPopOutcome::Clean);
            storage::clear_step_parked_worktree(&conn, &step.id).unwrap();
            true
        } else {
            false
        };

        let committing = write_simple_harness(harness_tmp.path(), &dir, true);
        let cfg_commit = config_with_harness("noop", &committing);
        let step_reloaded = storage::get_step(&conn, &step.id).unwrap();
        let (_tx2, rx2) = watch::channel(None);

        let done = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step_reloaded,
            config: &cfg_commit,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx2,
            exec_opts: ExecuteOptions {
                resumed_parked_worktree,
                ..ExecuteOptions::default()
            },
        })
        .await
        .unwrap();

        assert_eq!(
            done.outcome,
            StepOutcome::Success,
            "with budget intact the re-queued step completes on attempt #1"
        );
        assert_eq!(
            done.attempts_used, 1,
            "the re-run is attempt #1 — the block burned no retry"
        );
        let final_step = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(final_step.status, StepStatus::Complete);
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("happy"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert!(result.commit_hash.is_some(), "no-question path must commit");

        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
        assert!(logs[0].committed);

        // And the plan's effective status reflects the actual stored value
        // — no Interrupted shadow when there are no open interruptions.
        let effective = storage::plan_effective_status(&conn, &plan.id).unwrap();
        assert_ne!(effective, crate::plan::PlanStatus::Interrupted);
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
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("happy"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // Pre-existing *resolved* interruption on attempt 1 (the upcoming
        // attempt number) — resolved rows must not pause. Native
        // `interruptions`: insert then resolve.
        let prev = storage::insert_interruption(
            &conn,
            &step.id,
            1,
            crate::plan::InterruptionKind::Question,
            "old?",
            &[],
        )
        .unwrap();
        storage::resolve_interruption(&conn, &prev, "yes", None).unwrap();

        let config = config_with_harness("happy", &harness_path);
        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(
            result.outcome,
            StepOutcome::Success,
            "answered rows must not pause — got {result:?}",
        );
    }

    // ---------------------------------------------------------------------
    // Phase A: test-then-commit invariants
    // ---------------------------------------------------------------------

    /// Phase A: a single failing attempt leaves the harness's uncommitted
    /// changes on disk for the next attempt and does NOT advance HEAD.
    /// Drives a step through ONE failing-test attempt (max_retries=0) and
    /// asserts the dirty tree is preserved and HEAD is unchanged from the
    /// step's base.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failed_attempt_leaves_dirty_tree_for_next_attempt() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        // Seed a tracked file so the harness's append produces a clean diff.
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();
        let base_head = crate::git::get_commit_hash(&dir).unwrap();
        let base_commits = commit_count(&dir);

        // Harness appends one line. Test demands TWO lines, so attempt 1
        // fails the test.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append.sh");
        fs::write(
            &harness_path,
            format!(
                "#!/bin/sh\necho line >> {}/acc.txt\nexit 0\n",
                dir.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Acc", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: // no retries — single failing attempt
            None, change_policy: None, tags: None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // Phase B: a retryable terminal failure (test fail with budget
        // exhausted) now routes to the auto-raised blocker instead of
        // `Failed`. The runner outcome is `PausedForQuestion` (reused, see
        // `raise_retry_exhausted_blocker`); the stored step status is
        // `Pending` shadowed by the derived `Blocked` overlay; an open
        // `Blocker` interruption with the two ranked recovery options is
        // visible.
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Pending);
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1, "exactly one open interruption");
        assert_eq!(open[0].kind, InterruptionKind::Blocker);
        assert_eq!(open[0].options.len(), 2);
        // HEAD must still not have advanced — same Phase A invariant.
        assert_eq!(
            commit_count(&dir),
            base_commits,
            "Phase A: failed attempt makes no commit"
        );
        assert_eq!(
            crate::git::get_commit_hash(&dir).unwrap(),
            base_head,
            "Phase A: HEAD is unchanged from the step's base after a failing attempt"
        );
    }

    /// Phase A: a multi-attempt run with one failing attempt followed by a
    /// passing attempt produces exactly ONE commit (only after success).
    #[tokio::test(flavor = "current_thread")]
    async fn test_passing_attempt_after_failing_attempt_makes_single_commit() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        // Seed a tracked file so the harness's append produces a diff.
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();
        let base_commits = commit_count(&dir);

        // Harness appends one line per invocation; test needs two lines.
        // Attempt 1 fails (1 line); attempt 2 passes (2 lines after the
        // dirty tree carried forward).
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append.sh");
        fs::write(
            &harness_path,
            format!(
                "#!/bin/sh\necho line >> {}/acc.txt\nexit 0\n",
                dir.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 2",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Acc", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(1), model: // max_retries=1 → up to 2 attempts
            None, change_policy: None, tags: None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::Success);
        assert_eq!(result.attempts_used, 2);
        assert_eq!(
            commit_count(&dir),
            base_commits + 1,
            "Phase A: exactly one commit between step start and step end (only after success)"
        );
        // The single commit is the iteration-2 (passing) commit.
        let its = crate::git::iteration_commits_for_step(&dir, "HEAD", &step.short_id).unwrap();
        assert_eq!(its.len(), 1);
        assert_eq!(its[0].iteration, 2);
    }

    /// Phase A: a commit-hook rejection is retryable. With max_retries=1
    /// and a hook that always rejects, attempt 1 hits the hook, the next
    /// attempt's prompt context contains the hook stderr (concatenated
    /// into the previous_test_output), no commit is made, and after the
    /// budget exhausts the existing terminal-failure path fires
    /// (Phase B will reroute this to a blocker).
    #[tokio::test(flavor = "current_thread")]
    async fn test_commit_hook_rejection_is_retryable() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        let base_commits = commit_count(&dir);

        // Hermetic pre-commit hook that always rejects with a unique marker.
        let hooks_tmp = TempDir::new().unwrap();
        let hooks_dir = hooks_tmp.path().join("reject-hooks");
        fs::create_dir_all(&hooks_dir).unwrap();
        let pre_commit = hooks_dir.join("pre-commit");
        fs::write(
            &pre_commit,
            "#!/bin/sh\necho 'RALPH_PHASE_A_HOOK_MARKER: rejected'\nexit 1\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&pre_commit).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&pre_commit, perms).unwrap();
        Command::new("git")
            .args([
                "config",
                "core.hooksPath",
                hooks_dir.to_string_lossy().as_ref(),
            ])
            .current_dir(&dir)
            .output()
            .unwrap();

        // Harness produces a change every invocation so we reach the commit
        // phase on every attempt.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, true);

        let conn = crate::db::open_memory().unwrap();
        // No deterministic tests — has_changes drives `test_passed` via the
        // "changes produced, no tests configured → treat as passing" path,
        // so we reach the post-test commit phase reliably.
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("changing"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(1), model: // max_retries=1 → up to 2 attempts
            None, change_policy: None, tags: None },
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "changing".to_string(),
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // Phase B: exhausted commit-hook rejection now routes to the
        // auto-raised blocker instead of terminal Failed.
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        assert_eq!(result.attempts_used, 2);
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].kind, InterruptionKind::Blocker);
        assert_eq!(open[0].options.len(), 2);
        // Zero commits — every attempt's commit was hook-rejected.
        assert_eq!(
            commit_count(&dir),
            base_commits,
            "Phase A: hook rejection means no commit was made"
        );
        // Attempt 2's prompt must carry the hook stderr (via the
        // previous_test_output → "## Previous Test Output" section).
        let logs = storage::list_execution_logs_for_step(&conn, &step.id).unwrap();
        let a2 = logs.iter().find(|l| l.attempt == 2).unwrap();
        let a2_prompt = a2.prompt_text.as_deref().unwrap_or("");
        assert!(
            a2_prompt.contains("# Retry Context"),
            "attempt 2 must have a retry context block"
        );
        assert!(
            a2_prompt.contains("commit rejected by pre-commit hook"),
            "attempt 2 prompt must include the hook-rejection diagnostic; got:\n{a2_prompt}"
        );
    }

    // ---------------------------------------------------------------------
    // Phase B: auto-blocker on retry exhaustion
    // ---------------------------------------------------------------------

    /// `truncate_tail_bytes` keeps the tail (which carries the actual
    /// failure on most outputs), prefixes the elision marker, and never
    /// slices mid-codepoint.
    #[test]
    fn test_truncate_tail_bytes_below_cap_is_identity() {
        let s = "abc\ndef\n";
        assert_eq!(truncate_tail_bytes(s, 1024), s);
    }

    #[test]
    fn test_truncate_tail_bytes_above_cap_keeps_tail_and_marks_elision() {
        // 200 lines of "line N" — pick the last ~256 bytes only.
        let text: String = (0..200).map(|i| format!("line {i}\n")).collect();
        let out = truncate_tail_bytes(&text, 256);
        assert!(out.starts_with("..."), "elision marker must be at head");
        assert!(out.contains("bytes elided from head"));
        assert!(
            out.ends_with("line 199\n"),
            "the final line of the input must survive the truncation: {out}",
        );
        // The output is approximately the cap plus the marker length.
        assert!(
            out.len() <= 256 + 128,
            "elision overhead bounded: len={}",
            out.len()
        );
    }

    #[test]
    fn test_truncate_tail_bytes_utf8_boundary_safe() {
        // 4-byte codepoints; cutting at an arbitrary byte would panic.
        let s: String = "🌟".repeat(50);
        // Cap below the string length forces a truncation.
        let out = truncate_tail_bytes(&s, 32);
        // Must not panic and must remain valid UTF-8 (the format! itself
        // would have panicked on an invalid slice).
        assert!(out.contains("🌟"));
    }

    /// Phase B core: a step driven through a test-failing exhausted budget
    /// now raises a Blocker with two ranked options instead of marking the
    /// step `Failed`. The stored step status is `Pending` (shadowed by the
    /// derived `Blocked` overlay via `effective_step_status`), `attempts`
    /// stays at `max_attempts` so an observer can see the budget was spent,
    /// and the working tree is clean with HEAD unchanged.
    #[tokio::test(flavor = "current_thread")]
    async fn test_test_fail_exhaustion_raises_blocker() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        // Seed a tracked file so the harness's append produces a diff with
        // a sensible base.
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();
        let base_head = crate::git::get_commit_hash(&dir).unwrap();
        let base_commits = commit_count(&dir);

        // Harness appends one line; test demands two lines → always fails.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append.sh");
        fs::write(
            &harness_path,
            format!(
                "#!/bin/sh\necho line >> {}/acc.txt\nexit 0\n",
                dir.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        // Test condition that no number of harness invocations can satisfy
        // (the harness adds one line at a time; the test demands 99). This
        // makes every attempt deterministically fail — the dirty-tree
        // carry-forward (Phase A) doesn't accidentally pass attempt 2 the
        // way it does in `test_failing_then_passing_attempts_produce_single_commit`.
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 99",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 1 so we have a small budget (2 attempts) — every
        // attempt fails the same test, so we exhaust the budget and trigger
        // the Phase B path.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        let max_attempts = 1 + step.max_retries.unwrap_or(0);
        assert_eq!(max_attempts, 2);

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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // Outcome reuses PausedForQuestion (see raise_retry_exhausted_blocker).
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        assert_eq!(
            result.attempts_used, max_attempts,
            "attempts_used reflects the exhausted budget",
        );

        // Exactly one open Blocker interruption with the two ranked options.
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1, "exactly one open interruption");
        let blocker = &open[0];
        assert_eq!(blocker.kind, InterruptionKind::Blocker);
        assert_eq!(blocker.state, crate::plan::InterruptionState::Open);
        assert_eq!(blocker.options.len(), 2);
        assert_eq!(blocker.options[0].text, RETRY_EXHAUSTED_OPTION_RETRY);
        assert_eq!(blocker.options[0].priority, 1);
        assert_eq!(blocker.options[1].text, RETRY_EXHAUSTED_OPTION_FAIL);
        assert_eq!(blocker.options[1].priority, 2);
        assert!(
            blocker.body.contains("Step failed after"),
            "body must summarize the exhausted budget; got:\n{}",
            blocker.body,
        );
        // The body mentions the budget count concretely.
        assert!(
            blocker.body.contains(&format!("{max_attempts} attempts")),
            "body must mention the concrete attempt count {max_attempts}; got:\n{}",
            blocker.body,
        );
        // Phase E Fix 5: body carries per-attempt sections. With max_attempts
        // = 2 there are exactly 2 attempts in the cycle, both with the same
        // test-failure output → both sections must appear, the final one
        // marked `(final)`.
        assert!(
            blocker.body.contains("### Attempt 2 (final)"),
            "body must label the final attempt; got:\n{}",
            blocker.body,
        );
        assert!(
            blocker.body.contains("### Attempt 1"),
            "body must include the prior attempt's section; got:\n{}",
            blocker.body,
        );

        // Step is parked as Pending (the derived overlay shadows it with
        // Blocked at presentation time).
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Pending);
        assert_eq!(
            reloaded.attempts, max_attempts,
            "attempts left at max so observers see the budget was spent",
        );

        // Working tree clean, HEAD unchanged, and the preserved WIP is parked
        // in a stash for later re-application if the human chooses Retry.
        assert!(
            !git::has_uncommitted_changes(&dir).unwrap(),
            "auto-blocker must leave the worktree clean after parking",
        );
        assert!(
            storage::get_step_parked_worktree(&conn, &step.id)
                .unwrap()
                .is_some(),
            "retry exhaustion with a dirty tree must park a stash"
        );
        assert_eq!(
            commit_count(&dir),
            base_commits,
            "no commit made on any failing attempt",
        );
        assert_eq!(
            crate::git::get_commit_hash(&dir).unwrap(),
            base_head,
            "HEAD unchanged when the step is parked",
        );
    }

    /// Commit-hook rejection on the last attempt also routes to the Phase B
    /// auto-blocker (productively retryable: a different prompt / different
    /// code may pass the hook). Body must mention hooks, body & log must
    /// carry the hook stderr diagnostic.
    #[tokio::test(flavor = "current_thread")]
    async fn test_commit_hook_failure_exhaustion_raises_blocker() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Hermetic rejecting hook (same pattern as test_commit_failure_terminal_reason).
        let hooks_tmp = TempDir::new().unwrap();
        let hooks_dir = hooks_tmp.path().join("reject-hooks");
        fs::create_dir_all(&hooks_dir).unwrap();
        let pre_commit = hooks_dir.join("pre-commit");
        fs::write(
            &pre_commit,
            "#!/bin/sh\necho 'pre-commit hook rejected: stylebot says no'\nexit 1\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&pre_commit).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&pre_commit, perms).unwrap();
        Command::new("git")
            .args([
                "config",
                "core.hooksPath",
                hooks_dir.to_string_lossy().as_ref(),
            ])
            .current_dir(&dir)
            .output()
            .unwrap();

        // Harness that always produces a change so the commit phase always
        // runs (and always rejects).
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_simple_harness(harness_tmp.path(), &dir, true);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("changing"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries = 0 — single attempt is the entire budget; first
        // commit-hook rejection exhausts it.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "changing".to_string(),
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);

        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1);
        let blocker = &open[0];
        assert_eq!(blocker.kind, InterruptionKind::Blocker);
        assert_eq!(blocker.options.len(), 2);
        assert_eq!(blocker.options[0].text, RETRY_EXHAUSTED_OPTION_RETRY);
        assert_eq!(blocker.options[1].text, RETRY_EXHAUSTED_OPTION_FAIL);
        // Body must mention hooks (CommitFailed branch).
        assert!(
            blocker.body.contains("commit hooks rejected"),
            "blocker body must mention the commit-hooks rejection; got:\n{}",
            blocker.body,
        );
        // Body must carry the commit-rejection diagnostic ralph builds
        // (Phase A's combined "[Commit hook output]" block — hook stderr
        // capture depends on git's behavior with `core.hooksPath`, so we
        // assert on the ralph-built header that's reliably present).
        assert!(
            blocker.body.contains("[Commit hook output]")
                && blocker.body.contains("commit rejected by pre-commit hook"),
            "blocker body must include the combined commit-hook diagnostic; got:\n{}",
            blocker.body,
        );

        // Step status is Pending (derived overlay would surface Blocked).
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Pending);

        // Working tree clean, no commit landed.
        assert!(!git::has_uncommitted_changes(&dir).unwrap());
    }

    /// Phase B is opinionated about WHICH failure modes route to the
    /// blocker: harness-exit failures stay terminally `Failed`. Drive a
    /// step whose harness exits non-zero with max_retries=0 and assert the
    /// step is `Failed` with no interruption.
    #[tokio::test(flavor = "current_thread")]
    async fn test_harness_failure_exhaustion_still_terminal() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Harness that always exits non-zero.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("crash.sh");
        fs::write(&harness_path, "#!/bin/sh\nexit 5\n").unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("crashy"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "crashy".to_string(),
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

        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // HarnessFailed stays terminal Failed — Phase B did NOT over-route.
        assert_eq!(result.outcome, StepOutcome::Failed);
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Failed);
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert!(
            open.is_empty(),
            "no interruption is raised on HarnessFailed exhaustion: {open:?}",
        );
    }

    /// NoChanges (Required policy + agent makes no diff) keeps its
    /// terminal `Failed` shape — Phase B specifically excludes this
    /// (contract violation, not productively retryable).
    #[tokio::test(flavor = "current_thread")]
    async fn test_no_changes_exhaustion_still_terminal() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        // Noop harness — exits 0 producing no changes. With Required
        // policy this is a NoChanges classification.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = write_noop_harness(harness_tmp.path());

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep { title: "Step", description: "desc", agent: None, harness: None, acceptance_criteria: &[], max_retries: Some(0), model: None, change_policy: None, tags: // change_policy defaults to Required
            None },
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
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // NoChanges stays terminally Failed — Phase B did NOT over-route.
        assert_eq!(result.outcome, StepOutcome::Failed);
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Failed);
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert!(
            open.is_empty(),
            "no interruption is raised on NoChanges exhaustion: {open:?}",
        );
    }

    /// Fix #1 (disk-space gate routes to recoverable blocker, not terminal
    /// Failed): set `min_free_disk_mb` to a value any reasonable host can't
    /// possibly satisfy, then drive `execute_step` and assert it raises a
    /// `Blocker` interruption with the two ranked
    /// `RETRY_EXHAUSTED_OPTION_RETRY` / `RETRY_EXHAUSTED_OPTION_FAIL`
    /// options instead of going terminal `Failed`. Verifies (a) the step's
    /// stored status stays `Pending` (the derived `Blocked` overlay shadows
    /// it), (b) the blocker carries the disk-space context, and (c) Phase C
    /// resolution paths (Retry resets attempts to 0; Fail flips to Failed)
    /// behave on this auto-blocker exactly as they do for the test-fail and
    /// commit-fail flavors.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn test_disk_space_gate_raises_recoverable_blocker() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        // max_retries=0 → max_attempts=1: under the pre-fix code a single
        // disk pressure trip burned the whole budget terminally.
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // Pick a min_free_disk_mb so large that no test host can satisfy it.
        // 1 EB (1 048 576 TB) is far above any production disk.
        let config = Config {
            min_free_disk_mb: 1024 * 1024 * 1024,
            ..Config::default()
        };

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // (a) Outcome reuses PausedForQuestion (matches the auto-blocker
        // contract — runner moves to another runnable branch).
        assert_eq!(
            result.outcome,
            StepOutcome::PausedForQuestion,
            "disk pressure must NOT be terminal Failed",
        );

        // (b) A kind=Blocker interruption exists with the two expected
        // ranked options.
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(open.len(), 1, "exactly one open interruption");
        let blocker = &open[0];
        assert_eq!(blocker.kind, InterruptionKind::Blocker);
        assert_eq!(blocker.options.len(), 2);
        assert_eq!(blocker.options[0].text, RETRY_EXHAUSTED_OPTION_RETRY);
        assert_eq!(blocker.options[0].priority, 1);
        assert_eq!(blocker.options[1].text, RETRY_EXHAUSTED_OPTION_FAIL);
        assert_eq!(blocker.options[1].priority, 2);
        // Body mentions disk-space context (the FailureReason-specific
        // preamble in `build_retry_exhausted_body`).
        assert!(
            blocker.body.contains("disk space"),
            "body must mention disk space; got:\n{}",
            blocker.body,
        );

        // Step's stored status is Pending (derived Blocked overlay covers it).
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.status, StepStatus::Pending);

        // (c) Retry resolution: attempts reset to 0, status remains Pending,
        // scheduler is free to re-pick.
        let blocker_id = blocker.id.clone();
        let acted = crate::commands::interruption::apply_retry_exhausted_resolution(
            &conn,
            &dir.to_string_lossy(),
            &blocker_id,
            RETRY_EXHAUSTED_OPTION_RETRY,
        )
        .unwrap();
        assert!(acted, "Retry must be recognized on the disk auto-blocker");
        let reloaded = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(reloaded.attempts, 0, "Retry resets the attempt counter");
        assert_eq!(reloaded.status, StepStatus::Pending);

        // Mark Failed resolution: needs a fresh blocker since the prior one
        // is now resolved.
        let (step2, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step2",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        let (_tx2, rx2) = watch::channel(None);
        let result2 = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step2,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx2,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();
        assert_eq!(result2.outcome, StepOutcome::PausedForQuestion);
        let open2 = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        let blocker2 = open2.iter().find(|b| b.step_id == step2.id).unwrap();
        let acted_fail = crate::commands::interruption::apply_retry_exhausted_resolution(
            &conn,
            &dir.to_string_lossy(),
            &blocker2.id,
            RETRY_EXHAUSTED_OPTION_FAIL,
        )
        .unwrap();
        assert!(acted_fail);
        let reloaded2 = storage::get_step(&conn, &step2.id).unwrap();
        assert_eq!(
            reloaded2.status,
            StepStatus::Failed,
            "Mark Failed resolution must transition the step to terminal Failed",
        );
    }

    // -- Phase E follow-ups ------------------------------------------------

    /// Phase E Fix 1: the executor's retry-exhausted auto-blocker must fire
    /// the post-step hook with the **dedicated** `retry_exhausted` label, NOT
    /// the reused `paused` label the harness-raised-interruption path uses.
    /// This is what lets a hook author tell "branch is parked because the
    /// human asked a question" from "branch is parked because every retry
    /// burned" without round-tripping through the DB.
    #[tokio::test(flavor = "current_thread")]
    async fn test_test_fail_exhaustion_fires_retry_exhausted_hook_label() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);
        fs::write(dir.join("acc.txt"), "").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-m",
                "seed",
            ])
            .current_dir(&dir)
            .output()
            .unwrap();

        // Harness writes one line per attempt; the test demands 99 lines so
        // every attempt fails ⇒ retry exhaustion ⇒ auto-blocker ⇒
        // post-step hook with the new label.
        let harness_tmp = TempDir::new().unwrap();
        let harness_path = harness_tmp.path().join("append.sh");
        fs::write(
            &harness_path,
            format!(
                "#!/bin/sh\necho line >> {}/acc.txt\nexit 0\n",
                dir.to_string_lossy()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&harness_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&harness_path, perms).unwrap();

        let conn = crate::db::open_memory().unwrap();
        let test_cmd = format!(
            "test \"$(wc -l < {0}/acc.txt)\" -eq 99",
            dir.to_string_lossy()
        );
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "slug",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: Some("poly"),
                agent: None,
                deterministic_tests: &[test_cmd],
            },
        )
        .unwrap();
        seed_run_lock_row(&conn, &dir.to_string_lossy());
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Acc",
                description: "desc",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        let mut config = Config::default();
        config.harnesses.insert(
            "poly".to_string(),
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

        // Capture the hook's RALPH_STEP_STATUS via a hermetic post-step hook
        // that writes the label to a marker file. Attach via the storage API
        // exactly as a real run would.
        let marker = tmp.path().join("hook-status.txt");
        let hook_dir = tmp.path().join("hooks-bin");
        fs::create_dir_all(&hook_dir).unwrap();
        let _ = hook_dir; // kept for clarity; hooks are wired by name + lifecycle
        let hook_lib = crate::hook_library::Hook {
            name: "capture-status".to_string(),
            description: String::new(),
            lifecycle: crate::hook_library::Lifecycle::PostStep,
            scope: crate::hook_library::Scope::Global,
            command: format!("echo $RALPH_STEP_STATUS > {}", marker.display()),
        };
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", &hook_lib.name)
            .unwrap();
        let hook_ctx = crate::hooks::HookContext {
            applicable: vec![hook_lib],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let (_tx, rx) = watch::channel(None);
        let result = execute_step(ExecuteStepArgs {
            conn: &conn,
            plan: &plan,
            step: &step,
            config: &config,
            workdir: &dir,
            hook_ctx: &hook_ctx,
            abort_rx: rx,
            exec_opts: ExecuteOptions::default(),
        })
        .await
        .unwrap();

        // Sanity-check the auto-blocker path actually fired.
        assert_eq!(result.outcome, StepOutcome::PausedForQuestion);
        // The hook captured the **new** label.
        let captured = fs::read_to_string(&marker).expect("hook ran and wrote status");
        assert_eq!(
            captured.trim(),
            "retry_exhausted",
            "retry-exhaustion path must use the dedicated `retry_exhausted` \
             hook label, not the reused `paused` one",
        );
    }

    /// Phase E Fix 5: when each attempt's persisted `test_results` is large,
    /// the auto-blocker body still fits within the 8 KiB cap. The body is
    /// built straight from a synthetic DB (no harness needed) so we exercise
    /// `build_retry_exhausted_body` in isolation under adversarial input.
    #[tokio::test(flavor = "current_thread")]
    async fn test_auto_blocker_body_truncates_when_attempts_exceed_cap() {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "big-out",
                project: "/proj-big",
                branch_name: "branch",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: None,
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // 4 attempts in cycle 0, each with a >4KiB test_results blob.
        let big = "x".repeat(4096);
        for attempt in 1..=4_i32 {
            let log = storage::create_execution_log(
                &conn,
                &step.id,
                attempt,
                Some(&format!("attempt {attempt} prompt")),
                None,
            )
            .unwrap();
            storage::update_execution_log(
                &conn,
                log.id,
                crate::storage::ExecutionLogUpdate {
                    duration_secs: Some(0.1),
                    test_results: &[format!("attempt-{attempt}-FAIL:\n{big}")],
                    termination_reason: Some(crate::plan::TerminationReason::TestFailed),
                    test_status: Some(crate::plan::TestStatus::Failed),
                    ..Default::default()
                },
            )
            .unwrap();
        }

        let dummy_parsed = ParsedHarnessOutput::default();
        let live = vec!["attempt-4-FAIL: live".to_string()];
        let failure_output = FailureOutput {
            test_results: &live,
            diff: None,
            stdout: "",
            stderr: "",
            parsed: &dummy_parsed,
            has_changes: false,
        };
        let body = build_retry_exhausted_body(
            &conn,
            &step.id,
            4,
            FailureReason::TestFailed,
            &failure_output,
        );

        assert!(
            body.len() <= RETRY_EXHAUSTED_BODY_MAX_BYTES,
            "body must be capped at 8 KiB; got {} bytes",
            body.len(),
        );
        // Only the last 3 attempts are surfaced (oldest dropped).
        assert!(
            body.contains("### Attempt 4 (final)"),
            "must include the final attempt header; got:\n{body}",
        );
        assert!(body.contains("### Attempt 3"));
        assert!(body.contains("### Attempt 2"));
        assert!(
            !body.contains("### Attempt 1"),
            "Attempt 1 must be dropped (only the last 3 are kept); got:\n{body}",
        );
        // Final attempt's heading appears BEFORE the older ones (final-first).
        let final_pos = body.find("### Attempt 4 (final)").unwrap();
        let older_pos = body.find("### Attempt 3").unwrap();
        assert!(
            final_pos < older_pos,
            "final attempt must be rendered first; got:\n{body}",
        );
    }

    // ---- Phase E Fix 4: finalize_paused_for_question is transactional ----

    /// Source-shape assertion: `finalize_paused_for_question` MUST wrap its
    /// three state writes (delete_execution_log + set_step_attempts +
    /// update_step_status) in a single transaction. The pre-Phase-E shape did
    /// them as three independent writes, letting a concurrent scheduler tick
    /// observe the half-state `(status=Pending, attempts still bumped, no open
    /// interruption)` and re-pick the step. The transaction now lives in the
    /// shared `commit_park_atomically` helper (which also joins the parked
    /// stash's pointer row and restores the stash on commit failure); this
    /// test pins that the three writes route through it via a `tx`-bound
    /// closure rather than running against the bare `ctx.conn`.
    ///
    /// Driving the async function end-to-end would require constructing an
    /// `ExecCtx` (Connection, Config, Plan, Step, workdir, HookContext,
    /// abort_rx, etc.) which is heavy and would still observe the writes
    /// via post-conditions rather than the transaction itself. A source
    /// assertion is the cleanest check that the *atomicity invariant* is
    /// preserved — it fires at `cargo test --lib` if either the helper is
    /// reshaped without a transaction or the transaction commit is dropped.
    #[test]
    fn test_finalize_paused_for_question_is_transactional() {
        let src = include_str!("executor.rs");
        // Locate the function body.
        let signature = "async fn finalize_paused_for_question(";
        let fn_start = src
            .find(signature)
            .expect("finalize_paused_for_question must exist");
        // Take a slice that includes the entire body but stops before the
        // next top-level item (`raise_retry_exhausted_blocker`'s doc/comment
        // block).
        let after_sig = &src[fn_start..];
        let next_fn = after_sig
            .find("\n/// Phase B — auto-raise a `Blocker` interruption")
            .expect("expected the next sibling helper's doc comment");
        let body = &after_sig[..next_fn];

        // The three writes are routed through the transactional helper via a
        // `tx`-bound closure.
        assert!(
            body.contains("commit_park_atomically(ctx, parked,"),
            "finalize_paused_for_question must route its writes through \
             commit_park_atomically so they (and the parked-stash pointer) \
             commit or roll back together; without the transaction a scheduler \
             tick can observe the half-state and re-pick the step",
        );
        assert!(
            body.contains("delete_execution_log(tx,"),
            "delete_execution_log must run inside the transaction closure",
        );
        assert!(
            body.contains("set_step_attempts(tx,"),
            "set_step_attempts must run inside the transaction closure",
        );
        assert!(
            body.contains("update_step_status(tx,"),
            "update_step_status must run inside the transaction closure",
        );
        // Negative assertion: the three writes must not also run against the
        // bare `ctx.conn` (would split the transaction across two writers).
        assert!(
            !body.contains("delete_execution_log(ctx.conn,"),
            "the post-fix shape uses the closure `tx`, never `ctx.conn`, for the three writes",
        );
        assert!(
            !body.contains("set_step_attempts(ctx.conn,"),
            "the post-fix shape uses the closure `tx`, never `ctx.conn`, for the three writes",
        );
        assert!(
            !body.contains("update_step_status(ctx.conn, &ctx.step.id, StepStatus::Pending"),
            "the post-fix shape uses the closure `tx`, never `ctx.conn`, for the three writes",
        );

        // And the helper they route through actually opens + commits a
        // transaction (the atomicity now lives there for both pause paths).
        let helper_start = src
            .find("fn commit_park_atomically<T>(")
            .expect("commit_park_atomically must exist");
        let helper_after = &src[helper_start..];
        let helper_end = helper_after
            .find("\nasync fn finalize_paused_for_question(")
            .expect("commit_park_atomically must precede finalize_paused_for_question");
        let helper_body = &helper_after[..helper_end];
        assert!(
            helper_body.contains("ctx.conn.unchecked_transaction()"),
            "commit_park_atomically must open an unchecked_transaction",
        );
        assert!(
            helper_body.contains("tx.commit()"),
            "commit_park_atomically must commit the transaction it opens",
        );
        assert!(
            helper_body.contains("restore_parked_stash(ctx,"),
            "commit_park_atomically must restore the parked stash if the commit fails",
        );
    }

    // ----- park / restore preserves the user's pre-existing untracked files
    // (Cluster 3 Fix #1) ---------------------------------------------------

    /// Repro: a harness raises `ralph question ask` mid-step.
    /// `finalize_paused_for_question` parks the dirty tree via
    /// `stash_step_worktree_for_interruption`. Before the fix, the park
    /// stashed `--include-untracked` *unconditionally*, sweeping up the
    /// user's pre-existing `notes.txt` along with the harness's WIP — if
    /// an admin later ran `git stash clear` (or the pop conflicted),
    /// `notes.txt` was gone. The fix routes through
    /// `git::stash_push_with_untracked_except` with
    /// `ExecCtx.pre_existing_untracked`, so `notes.txt` stays in the
    /// worktree and is never in the stash to begin with. The resume path
    /// (`restore_parked_step_worktree`) then restores the harness's WIP on
    /// top, leaving the user's file untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn test_park_step_worktree_for_interruption_preserves_pre_existing_untracked() {
        use crate::hooks::HookContext;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        init_git_repo(&dir);

        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "park-preserve",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Step",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // The user's pre-existing untracked file (existed BEFORE `ralph
        // run`). This is what `executor::execute_step` snapshots via
        // `git::get_untracked_files(workdir)` at the top of the function
        // and threads through `ExecCtx.pre_existing_untracked`.
        std::fs::write(dir.join("notes.txt"), "user-owned notes").unwrap();

        // The harness's WIP: a tracked modification + a new untracked file.
        std::fs::write(dir.join("README.md"), "modified by harness").unwrap();
        std::fs::write(dir.join("harness-output.rs"), "fn wip() {}").unwrap();

        let hook_ctx = HookContext {
            applicable: vec![],
            project_dir: dir.clone(),
            hook_timeout_secs: 30,
        };
        let pre = vec!["notes.txt".to_string()];
        let ctx = ExecCtx {
            conn: &conn,
            plan: &plan,
            step: &step,
            workdir: &dir,
            pre_existing_untracked: &pre,
            hook_ctx: &hook_ctx,
            step_num: 1,
            max_attempts: 3,
            json_output: false,
        };

        // Park: stash the WIP, then record its pointer row (the two steps the
        // production paths now run via `stash_step_worktree_for_interruption`
        // + `commit_park_atomically`).
        let parked = stash_step_worktree_for_interruption(&ctx, 1, "interruption")
            .unwrap()
            .expect("there is dirty WIP to park");
        storage::set_step_parked_worktree(&conn, &step.id, parked.0.as_str(), &parked.1).unwrap();

        // The user's pre-existing file MUST still be on disk with its
        // original contents — the whole point of the fix.
        assert!(
            dir.join("notes.txt").exists(),
            "park must NOT stash the user's pre-existing untracked file"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("notes.txt")).unwrap(),
            "user-owned notes"
        );

        // The harness's tracked modification was parked (file reverted to
        // HEAD) and its new untracked file is gone (now lives in the stash).
        assert_eq!(
            std::fs::read_to_string(dir.join("README.md")).unwrap(),
            "init"
        );
        assert!(!dir.join("harness-output.rs").exists());

        // The parked-worktree row was persisted.
        let parked = storage::get_step_parked_worktree(&conn, &step.id)
            .unwrap()
            .expect("park must persist a parked worktree row");

        // Now run the resume path to re-apply the parked stash. We invoke
        // `git::stash_pop` directly with the SHA — same code path
        // `restore_parked_step_worktree` takes for the apply step.
        let stash_ref = crate::git::StashRef(parked.stash_sha.clone());
        let outcome = crate::git::stash_pop(&dir, &stash_ref).unwrap();
        assert_eq!(outcome, crate::git::StashPopOutcome::Clean);

        // After resume the harness's WIP is back AND the user's pre-existing
        // file is *still* there (it never left).
        assert_eq!(
            std::fs::read_to_string(dir.join("README.md")).unwrap(),
            "modified by harness"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("harness-output.rs")).unwrap(),
            "fn wip() {}"
        );
        assert!(dir.join("notes.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dir.join("notes.txt")).unwrap(),
            "user-owned notes",
            "the user's pre-existing file must round-trip through park+restore untouched"
        );
    }
}
