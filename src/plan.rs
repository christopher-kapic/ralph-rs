// Plan and step lifecycle

use chrono::{DateTime, Utc};
use rusqlite::Row;
use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// PlanStatus enum
// ---------------------------------------------------------------------------

/// Status of a plan throughout its lifecycle.
///
/// Note: [`PlanStatus::Interrupted`] is a *derived* status — it is never
/// written to `plans.status`. A plan is reported as `Interrupted` whenever any
/// **open interruption** (a question *or* a blocker — docs/dag-redesign.md
/// §3.4/§6) exists for one of its steps; the underlying lifecycle
/// (in_progress/ready/etc.) stays in the column and un-shadows automatically
/// when the human resolves the last open interruption. The variant exists in
/// the enum so consumers (TUI/JSON output) can render the derived state
/// uniformly with the rest.
///
/// This is the post-Phase-2 rename of the old `Question` variant. For one
/// release the legacy `"question"` string is still **accepted on parse** (a
/// back-compat alias) but it always **serializes** / `as_str`es as
/// `"interrupted"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum PlanStatus {
    Planning,
    Ready,
    InProgress,
    Complete,
    Failed,
    Aborted,
    Archived,
    Interrupted,
}

impl PlanStatus {
    /// Convert to the lowercase string stored in SQLite.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Ready => "ready",
            Self::InProgress => "in_progress",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
            Self::Archived => "archived",
            Self::Interrupted => "interrupted",
        }
    }
}

impl std::fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when parsing an invalid status string.
#[derive(Debug, Clone)]
pub struct ParseStatusError(String);

impl fmt::Display for ParseStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown status: {}", self.0)
    }
}

impl std::error::Error for ParseStatusError {}

impl std::str::FromStr for PlanStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "planning" => Ok(Self::Planning),
            "ready" => Ok(Self::Ready),
            "in_progress" => Ok(Self::InProgress),
            "complete" => Ok(Self::Complete),
            "failed" => Ok(Self::Failed),
            "aborted" => Ok(Self::Aborted),
            "archived" => Ok(Self::Archived),
            "interrupted" => Ok(Self::Interrupted),
            // One-release back-compat alias: the pre-Phase-2 derived status
            // was spelled "question". It is never written to the DB (derived
            // only), but accept it on parse so a value materialized by an
            // older binary still round-trips.
            "question" => Ok(Self::Interrupted),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// StepStatus enum
// ---------------------------------------------------------------------------

/// Status of an individual step.
///
/// Note: [`StepStatus::Blocked`] is an *orthogonal derived overlay*, not a
/// stored lifecycle state (docs/dag-redesign.md §3.3). It is **never written
/// to `steps.status`** — exactly like [`PlanStatus::Interrupted`]. A step
/// *presents* as `Blocked` whenever it has an open interruption (question or
/// blocker); its underlying stored status (`pending`/`in_progress`/…) is
/// preserved underneath and un-shadows automatically the moment the
/// interruption is resolved. The variant only exists in the enum so the
/// derivation helper [`effective_step_status`] can hand callers a single
/// status to render. `as_str`/`FromStr`/serde still support it (round-trip
/// safe), but storage code must never persist it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    InProgress,
    Complete,
    Failed,
    Skipped,
    Aborted,
    Blocked,
}

impl StepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Aborted => "aborted",
            Self::Blocked => "blocked",
        }
    }
}

/// Derive a step's *effective* (presentation) status.
///
/// `Blocked` is an overlay (docs/dag-redesign.md §3.3): when the step has an
/// open interruption it *presents* as [`StepStatus::Blocked`] while its
/// stored lifecycle is preserved underneath. The instant the interruption is
/// resolved (`has_open_interruption == false`) the underlying status
/// un-shadows — this helper simply returns it, so the overlay is fully
/// reversible and never needs a DB write.
///
/// Terminal stored states are *not* overlaid: a `Complete`/`Failed`/`Skipped`
/// /`Aborted` step is done with respect to its branch and a lingering
/// (typically already-resolved-but-stale-flagged) interruption must not make
/// a finished step look re-blocked. Only the active lifecycle states
/// (`Pending`/`InProgress`) take the overlay. This mirrors how
/// `plan_effective_status` upgrades a still-running plan but leaves a
/// completed one alone.
#[allow(dead_code)] // scheduler + Phase 4 TUI status-rendering consumers land in later steps.
pub fn effective_step_status(stored: StepStatus, has_open_interruption: bool) -> StepStatus {
    if !has_open_interruption {
        return stored;
    }
    match stored {
        StepStatus::Pending | StepStatus::InProgress => StepStatus::Blocked,
        // Terminal states are not re-shadowed.
        StepStatus::Complete
        | StepStatus::Failed
        | StepStatus::Skipped
        | StepStatus::Aborted
        | StepStatus::Blocked => stored,
    }
}

impl std::fmt::Display for StepStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for StepStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "complete" => Ok(Self::Complete),
            "failed" => Ok(Self::Failed),
            "skipped" => Ok(Self::Skipped),
            "aborted" => Ok(Self::Aborted),
            "blocked" => Ok(Self::Blocked),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// ChangePolicy enum
// ---------------------------------------------------------------------------

/// Whether a step must produce file changes after the harness runs.
///
/// - [`ChangePolicy::Required`] (default): a clean harness exit with no diff
///   is treated as failure. Appropriate for implementation steps where the
///   absence of changes means the harness did nothing useful.
/// - [`ChangePolicy::Optional`]: a clean harness exit with no diff is a valid
///   success (tests still run if configured). Appropriate for review, audit,
///   or check steps whose prompts direct the harness not to modify code.
///
/// A third `forbidden` variant is reserved for future work but intentionally
/// not implemented here — the enum stays extensible via the non-exhaustive
/// matches each caller performs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "kebab-case")]
pub enum ChangePolicy {
    #[default]
    Required,
    Optional,
}

impl ChangePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Optional => "optional",
        }
    }
}

impl std::fmt::Display for ChangePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ChangePolicy {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "required" => Ok(Self::Required),
            "optional" => Ok(Self::Optional),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// RetryStrategy enum
// ---------------------------------------------------------------------------

/// How a step's working tree is treated between failed attempts.
///
/// Resolved per-step via [`Step::effective_retry_strategy`] with the
/// precedence step > plan > default ([`RetryStrategy::Keep`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "kebab-case")]
pub enum RetryStrategy {
    /// Failed attempts leave the working tree as-is; the next attempt sees
    /// the prior work directly via `git diff`. The retry context therefore
    /// omits the diff (the changes are already on disk for the agent to
    /// inspect and build on).
    #[default]
    Keep,
    /// Failed attempts roll back the working tree before retrying; the prior
    /// attempt's diff is fed into the next attempt's prompt via the retry
    /// context so the agent can learn from — but doesn't inherit — the
    /// rolled-back work.
    Rollback,
}

impl RetryStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Rollback => "rollback",
        }
    }
}

impl std::fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for RetryStrategy {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "keep" => Ok(Self::Keep),
            "rollback" => Ok(Self::Rollback),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Interruption domain model (questions + blockers, unified)
// ---------------------------------------------------------------------------

/// What kind of branch-pausing interruption this is
/// (docs/dag-redesign.md §3.4).
///
/// Questions and blockers are *one* entity — a branch-pausing interrupt that
/// needs a human and may carry text forward into the next prompt. A
/// [`InterruptionKind::Question`] carries proposed [`InterruptionOption`]s
/// with a priority (1 = the agent's best guess); a
/// [`InterruptionKind::Blocker`] has no options (the agent explains what it
/// cannot do and the human resolves it).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum InterruptionKind {
    #[default]
    Question,
    Blocker,
}

impl InterruptionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Question => "question",
            Self::Blocker => "blocker",
        }
    }
}

impl std::fmt::Display for InterruptionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for InterruptionKind {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "question" => Ok(Self::Question),
            "blocker" => Ok(Self::Blocker),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

/// Lifecycle state of an [`Interruption`] (docs/dag-redesign.md §3.4).
///
/// A fresh interruption is [`InterruptionState::Open`]: its step's branch is
/// `Blocked`, **no retry budget is consumed**, and the scheduler moves on to
/// another runnable step. Once a human resolves it the state becomes
/// [`InterruptionState::Resolved`] and the resolution/comment are injected
/// (bounded) into the step's next prompt (§8).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum InterruptionState {
    #[default]
    Open,
    Resolved,
}

impl InterruptionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }
}

impl std::fmt::Display for InterruptionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for InterruptionState {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "resolved" => Ok(Self::Resolved),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// ReviewStatus enum
// ---------------------------------------------------------------------------

/// Per-step nondeterministic-review verdict (docs/dag-redesign.md §3.3).
///
/// Orthogonal to [`StepStatus`] exactly as [`TestStatus`] is orthogonal to
/// the termination reason today: a step reaches `Complete` only after its
/// review has *returned* (any verdict). Stored in the nullable
/// `steps.review_status` TEXT column (V27); a NULL on disk means
/// [`ReviewStatus::Pending`] (not yet reviewed), so the variant set mirrors
/// the §3.3 list verbatim.
///
/// - [`ReviewStatus::Pending`] — review has not started (the on-disk NULL).
/// - [`ReviewStatus::InFlight`] — a read-only reviewer is running against
///   the step's commit SHA.
/// - [`ReviewStatus::Passed`] — reviewer found no defect; the step is
///   `Complete`.
/// - [`ReviewStatus::Failed`] — reviewer rejected; a corrective step is
///   inserted and dependents are re-parented (§10). The step itself still
///   becomes `Complete` (the fix lives in the corrective step).
/// - [`ReviewStatus::Skipped`] — review was skipped for this run.
/// - [`ReviewStatus::Disabled`] — review is off at some scope (step / plan
///   / global, §6); the step is `Complete` straight from passing tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum ReviewStatus {
    #[default]
    Pending,
    InFlight,
    Passed,
    Failed,
    Skipped,
    Disabled,
}

impl ReviewStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Disabled => "disabled",
        }
    }
}

impl std::fmt::Display for ReviewStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ReviewStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "in_flight" => Ok(Self::InFlight),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            "skipped" => Ok(Self::Skipped),
            "disabled" => Ok(Self::Disabled),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

/// One proposed answer to a [`InterruptionKind::Question`] interruption.
///
/// `priority` ranks the agent's proposals (1 = the agent's best guess).
/// Blockers and freeform-only questions carry an empty option list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterruptionOption {
    pub text: String,
    pub priority: i32,
}

/// A unified branch-pausing interruption — a question or a blocker
/// (docs/dag-redesign.md §3.4).
///
/// One entity, one state machine: a [`InterruptionKind::Question`] carries
/// proposed [`options`](Interruption::options) with priority (1 = agent
/// best); a [`InterruptionKind::Blocker`] has no options. Resolving an
/// interruption records `resolution`/`comment` and flips `state` to
/// [`InterruptionState::Resolved`]; while it is
/// [`InterruptionState::Open`] the step's branch is `Blocked` and the
/// scheduler works elsewhere (no retry budget consumed). The
/// resolution/comment are later injected, bounded, into the step's next
/// prompt (§8).
///
/// Native storage/model wiring lands with the Phase 2 interruption CRUD
/// (`storage::insert_interruption` and friends); the `interruption` CLI and
/// TUI inbox land in later DAG-redesign steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interruption {
    pub id: String,
    pub step_id: String,
    pub attempt: i32,
    pub kind: InterruptionKind,
    /// The question text, or the blocker explanation.
    pub body: String,
    /// Questions only (priority 1 = agent's best). Empty for blockers and
    /// freeform-only questions.
    #[serde(default)]
    pub options: Vec<InterruptionOption>,
    /// The chosen option text or freeform answer; `None` while `Open`.
    #[serde(default)]
    pub resolution: Option<String>,
    /// Extra human note, always injectable alongside the resolution.
    #[serde(default)]
    pub comment: Option<String>,
    pub state: InterruptionState,
    pub asked_at: DateTime<Utc>,
    /// Set when `state` becomes [`InterruptionState::Resolved`]; `None`
    /// while `Open`.
    #[serde(default)]
    pub resolved_at: Option<DateTime<Utc>>,
}

impl Interruption {
    /// Read an [`Interruption`] from a SQLite row.
    ///
    /// Expected column order (the native `interruptions` table, V26):
    /// id, step_id, attempt, kind, body, options, resolution, comment,
    /// state, asked_at, resolved_at
    pub fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        use std::str::FromStr;

        let kind_str: String = row.get(3)?;
        let kind = InterruptionKind::from_str(&kind_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let options_json: String = row.get(5)?;
        let options: Vec<InterruptionOption> =
            serde_json::from_str(&options_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        let state_str: String = row.get(8)?;
        let state = InterruptionState::from_str(&state_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let asked_str: String = row.get(9)?;
        let asked_at = parse_datetime(&asked_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let resolved_str: Option<String> = row.get(10)?;
        let resolved_at = match resolved_str {
            Some(s) => Some(parse_datetime(&s).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        Ok(Interruption {
            id: row.get(0)?,
            step_id: row.get(1)?,
            attempt: row.get(2)?,
            kind,
            body: row.get(4)?,
            options,
            resolution: row.get(6)?,
            comment: row.get(7)?,
            state,
            asked_at,
            resolved_at,
        })
    }
}

// ---------------------------------------------------------------------------
// Phase enum
// ---------------------------------------------------------------------------

/// Which sub-stage of a step's execution is currently active.
///
/// Recorded on `run_locks` so an external observer can tell whether a step
/// is mid-harness, mid-test, mid-commit, etc. `Idle` means no step is
/// running — the lock exists but the runner is between steps.
///
/// The executor writes a new phase value to the `run_locks` row at every
/// lifecycle boundary inside [`crate::executor::execute_step`]; `ralph cancel`
/// and `ralph status` read those values back. `Idle` is never written by the
/// executor today — it's reserved for a future "lock held, no step running"
/// state (the runner between steps).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Idle,
    PreStepHook,
    Harness,
    PreTestHook,
    Tests,
    PostTestHook,
    Commit,
    Rollback,
    PostStepHook,
}

impl Phase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::PreStepHook => "pre_step_hook",
            Self::Harness => "harness",
            Self::PreTestHook => "pre_test_hook",
            Self::Tests => "tests",
            Self::PostTestHook => "post_test_hook",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
            Self::PostStepHook => "post_step_hook",
        }
    }
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Phase {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "idle" => Ok(Self::Idle),
            "pre_step_hook" => Ok(Self::PreStepHook),
            "harness" => Ok(Self::Harness),
            "pre_test_hook" => Ok(Self::PreTestHook),
            "tests" => Ok(Self::Tests),
            "post_test_hook" => Ok(Self::PostTestHook),
            "commit" => Ok(Self::Commit),
            "rollback" => Ok(Self::Rollback),
            "post_step_hook" => Ok(Self::PostStepHook),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// TerminationReason enum
// ---------------------------------------------------------------------------

/// Why an execution-log attempt terminated. Stored on `execution_logs` so the
/// terminal outcome is explicit rather than inferred from the
/// `(committed, rolled_back, test_results)` tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    Success,
    UserInterrupted,
    Timeout,
    TestFailed,
    NoChanges,
    HookFailed,
    HarnessFailed,
    CommitFailed,
    RollbackFailed,
    /// Step aborted because free disk space dropped below
    /// `Config::min_free_disk_mb`. Distinct from `HarnessFailed` so the user
    /// can tell the difference between "the agent crashed" and "we never
    /// even started it because the FS was about to fill".
    InsufficientDiskSpace,
    /// Harness exited cleanly but raised one or more open interruptions
    /// (`ralph question ask` / `ralph block` — native `interruptions` rows)
    /// during the attempt. The runner skips tests + commit, rolls back any
    /// diff, marks the branch `Blocked`, and — per docs/dag-redesign.md
    /// §3.4 / §9 invariant 4 — consumes no retry budget. Distinct from
    /// `HarnessFailed` so paused-for-clarification history doesn't pollute
    /// real-failure metrics.
    PausedForQuestion,
    /// Operator pressed `P` (or ran `ralph pause`) to request a graceful
    /// stop after the current step. The runner observed `pause_requested`
    /// between step boundaries and exited cleanly. Distinct from
    /// `UserInterrupted` (mid-step SIGTERM) and `PausedForQuestion`
    /// (harness-driven pause) so the history reflects exactly which
    /// pause path triggered.
    PausedByUser,
    /// Operator ran `ralph skip` (or the TUI skip binding) while this step
    /// was the in-flight step in the runner process. The harness was killed
    /// via the cancel ladder and the step was marked `Skipped`. Distinct
    /// from `UserInterrupted` (Ctrl+C, which terminates the *whole* run)
    /// because a skip only drops the current step and lets the run advance.
    UserSkipped,
    Unknown,
}

impl TerminationReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::UserInterrupted => "user_interrupted",
            Self::Timeout => "timeout",
            Self::TestFailed => "test_failed",
            Self::NoChanges => "no_changes",
            Self::HookFailed => "hook_failed",
            Self::HarnessFailed => "harness_failed",
            Self::CommitFailed => "commit_failed",
            Self::RollbackFailed => "rollback_failed",
            Self::InsufficientDiskSpace => "insufficient_disk_space",
            Self::PausedForQuestion => "paused_for_question",
            Self::PausedByUser => "paused_by_user",
            Self::UserSkipped => "user_skipped",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for TerminationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TerminationReason {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "success" => Ok(Self::Success),
            "user_interrupted" => Ok(Self::UserInterrupted),
            "timeout" => Ok(Self::Timeout),
            "test_failed" => Ok(Self::TestFailed),
            "no_changes" => Ok(Self::NoChanges),
            "hook_failed" => Ok(Self::HookFailed),
            "harness_failed" => Ok(Self::HarnessFailed),
            "commit_failed" => Ok(Self::CommitFailed),
            "rollback_failed" => Ok(Self::RollbackFailed),
            "insufficient_disk_space" => Ok(Self::InsufficientDiskSpace),
            "paused_for_question" => Ok(Self::PausedForQuestion),
            "paused_by_user" => Ok(Self::PausedByUser),
            "user_skipped" => Ok(Self::UserSkipped),
            "unknown" => Ok(Self::Unknown),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// TestStatus enum
// ---------------------------------------------------------------------------

/// Outcome of the test phase for an execution-log attempt. Separate from
/// `TerminationReason` because tests can be "not configured" or "not run"
/// without the attempt itself terminating abnormally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    NotConfigured,
    NotRun,
    Passed,
    Failed,
    Aborted,
    TimedOut,
}

impl TestStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::NotRun => "not_run",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
            Self::TimedOut => "timed_out",
        }
    }
}

impl std::fmt::Display for TestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TestStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "not_configured" => Ok(Self::NotConfigured),
            "not_run" => Ok(Self::NotRun),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            "aborted" => Ok(Self::Aborted),
            "timed_out" => Ok(Self::TimedOut),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Plan struct
// ---------------------------------------------------------------------------

/// Canonical column list for `SELECT` queries against the `plans` table.
///
/// Matches the physical table layout after all migrations: V1 defined every
/// column through `updated_at`, V5 appended `plan_harness`,
/// V16 appended `questions_enabled`, V18 appended `pause_requested`,
/// V19 appended `last_run_branch`, V20 appended `last_run_started_at`,
/// V23 appended `skip_requested_step_id` + `skip_changes`, V24
/// appended `retry_strategy`, and V27 appended `review_enabled` via
/// `ALTER TABLE ... ADD COLUMN`. V10's
/// `prompt_prefix`/`prompt_suffix` and V14's `context_prepend` were
/// dropped again by V21 (preserving the physical order of the remaining
/// columns). Every `Plan`-returning query MUST use this list so
/// [`Plan::from_row`]'s indices line up — a raw `SELECT *` would
/// otherwise swap columns.
pub const PLAN_COLUMNS: &str = "id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at, plan_harness, questions_enabled, pause_requested, last_run_branch, last_run_started_at, skip_requested_step_id, skip_changes, retry_strategy, review_enabled, squash_on_complete";

/// A plan represents a high-level task broken into ordered steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub id: String,
    pub slug: String,
    pub project: String,
    pub branch_name: String,
    pub description: String,
    pub status: PlanStatus,
    pub harness: Option<String>,
    pub agent: Option<String>,
    pub deterministic_tests: Vec<String>,
    pub plan_harness: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Per-plan opt-in for the pause-for-question feature. When `false`
    /// (default), `ralph question ask` / `ralph block` invocations from a
    /// harness against a step in this plan are rejected and no
    /// `interruptions` rows are written. When `true`, the runner inspects
    /// open interruptions after each attempt and blocks the branch. Toggled
    /// via `ralph plan questions on|off` and the `Q` keybinding in the TUI
    /// plan list.
    #[serde(default)]
    pub questions_enabled: bool,
    /// Operator-requested graceful pause flag. When `true`, the runner
    /// finishes the currently-executing step, then exits with
    /// `TerminationReason::PausedByUser` between steps and clears the flag
    /// in the same transaction so a subsequent `ralph resume` doesn't
    /// immediately re-pause. Set via the TUI `P` keybinding or `ralph pause`;
    /// cleared by the runner on entry-to-pause or by pressing `P` again
    /// before the boundary fires.
    #[serde(default)]
    pub pause_requested: bool,
    /// Git branch the plan most recently started a run on. Written by the
    /// runner at run-start (both default and `--current-branch` modes), so
    /// `ralph resume` (no slug) can match the plan whose last run executed
    /// on the current branch — without false-matching via `branch_name`
    /// when the user later creates a new branch sharing a paused plan's
    /// slug. `None` for plans that have never been run.
    #[serde(default)]
    pub last_run_branch: Option<String>,
    /// Wall-clock timestamp at which the plan most recently started a run
    /// (written by the runner alongside `last_run_branch`). Provides the
    /// resume resolver with a stable "last actually ran" anchor, so its
    /// `ORDER BY` can ignore unrelated bumps to `updated_at` (e.g. toggling
    /// `questions_enabled` or `pause_requested`). `None` for never-run plans.
    #[serde(default)]
    pub last_run_started_at: Option<String>,
    /// Step UUID of a pending cross-process skip request, or `None` when no
    /// skip is pending. Written by `ralph skip` / the TUI skip dialog (a
    /// *different* process from the runner that owns the in-flight harness);
    /// the runner reads + clears it mid-attempt and, when it matches the
    /// in-flight step, funnels into the same executor skip path the
    /// same-process cancel registry uses. Scoped to a step id (not a bare
    /// boolean) so a stale request can never skip the wrong step.
    #[serde(default)]
    pub skip_requested_step_id: Option<String>,
    /// Serialized [`crate::git::ParkStrategyKind`]
    /// (`stash`|`commit`|`discard`|`cancel`) the operator chose for the
    /// pending skip. `None` when no skip is pending; an unrecognized value
    /// is treated as the safe `Stash` default by the consumer so a skip
    /// never silently destroys work.
    #[serde(default)]
    pub skip_changes: Option<String>,
    /// Plan-level default retry strategy. `None` means "no plan-level
    /// override" — the effective value falls through to the global default
    /// ([`RetryStrategy::Keep`]) unless a step overrides it. Resolved via
    /// [`Step::effective_retry_strategy`].
    #[serde(default)]
    pub retry_strategy: Option<RetryStrategy>,
    /// Plan-level review on/off override (V27). `None` means "no plan-level
    /// override" — the effective value falls through to the global
    /// `config.review.enabled` (then `false`) unless a step overrides it.
    /// Resolved via [`crate::config::effective_review_enabled`] with the
    /// precedence step > plan > global > false (mirrors `RetryStrategy`).
    /// Stored as a nullable INTEGER (tri-state bool) on disk; wired but not
    /// yet consumed by the runner in this batch.
    #[serde(default)]
    pub review_enabled: Option<bool>,
    /// Per-plan `--squash-on-complete` toggle (V28, docs/dag-redesign.md
    /// §14.1). `false` (the default; on-disk NULL or 0) keeps every
    /// per-iteration step commit (full audit trail — identical to the
    /// step 32/33 output). `true` collapses a step's iteration commits into
    /// a single commit when the step reaches `Complete`, preserving the
    /// `Ralph-*` trailers on the squashed commit. Stored as a nullable
    /// INTEGER; NULL is coerced to `false`.
    #[serde(default)]
    pub squash_on_complete: bool,
}

impl Plan {
    /// Read a Plan from a SQLite row.
    ///
    /// Expected column order matches [`PLAN_COLUMNS`]:
    /// id, slug, project, branch_name, description, status, harness, agent,
    /// deterministic_tests, created_at, updated_at, plan_harness,
    /// questions_enabled, pause_requested, last_run_branch,
    /// last_run_started_at, skip_requested_step_id, skip_changes,
    /// retry_strategy, review_enabled, squash_on_complete
    pub fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        let status_str: String = row.get(5)?;
        let status: PlanStatus = status_str.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let tests_json: String = row.get(8)?;
        let deterministic_tests: Vec<String> = serde_json::from_str(&tests_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let created_str: String = row.get(9)?;
        let created_at = parse_datetime(&created_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let updated_str: String = row.get(10)?;
        let updated_at = parse_datetime(&updated_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(e))
        })?;

        // `questions_enabled` and `pause_requested` are INTEGER NOT NULL
        // DEFAULT 0 on disk; SQLite has no native bool, so read as i64 and
        // coerce.
        let questions_enabled_int: i64 = row.get(12)?;
        let pause_requested_int: i64 = row.get(13)?;

        // `retry_strategy` is a nullable TEXT column (V24). NULL means "no
        // plan-level override" — resolution falls through to the global
        // default. A non-null value must parse to a known variant.
        let retry_strategy_str: Option<String> = row.get(18)?;
        let retry_strategy = match retry_strategy_str {
            Some(s) => Some(s.parse::<RetryStrategy>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    18,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        // `review_enabled` is a nullable INTEGER column (V27) at index 19.
        // NULL means "no plan-level override" — resolution falls through to
        // the global `config.review.enabled` (then `false`). SQLite has no
        // native bool, so read as `Option<i64>` and coerce non-null to a
        // bool (any non-zero = true), mirroring the `questions_enabled`
        // integer-to-bool handling above.
        let review_enabled: Option<bool> = row.get::<_, Option<i64>>(19)?.map(|v| v != 0);

        // `squash_on_complete` is a nullable INTEGER column (V28) at index
        // 20. NULL (pre-V28 / never-set) coerces to `false` — the default-OFF
        // behavior. SQLite has no native bool, so read as `Option<i64>` and
        // treat any non-zero as true (same pattern as `review_enabled` /
        // `questions_enabled`). `.ok()`-tolerant for SELECTs/raw test inserts
        // that predate the column.
        let squash_on_complete: bool = row
            .get::<_, Option<i64>>(20)
            .ok()
            .flatten()
            .map(|v| v != 0)
            .unwrap_or(false);

        Ok(Plan {
            id: row.get(0)?,
            slug: row.get(1)?,
            project: row.get(2)?,
            branch_name: row.get(3)?,
            description: row.get(4)?,
            status,
            harness: row.get(6)?,
            agent: row.get(7)?,
            deterministic_tests,
            plan_harness: row.get(11)?,
            created_at,
            updated_at,
            questions_enabled: questions_enabled_int != 0,
            pause_requested: pause_requested_int != 0,
            last_run_branch: row.get(14)?,
            last_run_started_at: row.get(15)?,
            skip_requested_step_id: row.get(16)?,
            skip_changes: row.get(17)?,
            retry_strategy,
            review_enabled,
            squash_on_complete,
        })
    }
}

// ---------------------------------------------------------------------------
// Step struct
// ---------------------------------------------------------------------------

/// A single step within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    /// Plan-unique, lifetime-stable short handle (V25). Replaces the
    /// positional step number as the user-facing selector
    /// (docs/dag-redesign.md §3.1); the internal UUID [`Step::id`] is
    /// unchanged. `#[serde(default)]` so pre-V25 exported plan JSON (which
    /// lacks the field) still deserializes — minting happens at step
    /// creation / import-backfill, not here.
    #[serde(default)]
    pub short_id: String,
    pub plan_id: String,
    pub sort_key: String,
    pub title: String,
    pub description: String,
    pub agent: Option<String>,
    pub harness: Option<String>,
    pub acceptance_criteria: Vec<String>,
    pub status: StepStatus,
    pub attempts: i32,
    pub max_retries: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Per-step model override forwarded via the harness's `model_args`
    /// template. `None` means "fall back to the harness's `default_model`
    /// (or omit the model flag entirely if that's also None)".
    #[serde(default)]
    pub model: Option<String>,
    /// Operator-supplied reason recorded when the step was skipped via
    /// `ralph skip --reason <r>`. `None` for non-skipped steps or skips
    /// that omitted the flag.
    #[serde(default)]
    pub skipped_reason: Option<String>,
    /// Whether this step must produce file changes to succeed. Defaults to
    /// [`ChangePolicy::Required`] so old exported plan JSON (and any caller
    /// that forgets the field) keeps the pre-V12 behavior.
    #[serde(default)]
    pub change_policy: ChangePolicy,
    /// Free-form string tags attached to this step. Storage + CRUD only;
    /// no execution-model semantics today. Stored on the DB row as a JSON
    /// array of strings. Defaults to empty for pre-V13 rows and old
    /// exported plan JSON that lacks the field.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Step-level retry-strategy override. `None` means "no step-level
    /// override" — resolution falls through to the plan's value and then
    /// the global default ([`RetryStrategy::Keep`]). Resolved via
    /// [`Step::effective_retry_strategy`].
    #[serde(default)]
    pub retry_strategy: Option<RetryStrategy>,
    /// Step-level review on/off override (V27). `None` means "no step-level
    /// override" — resolution falls through to the plan's value and then
    /// the global `config.review.enabled` (then `false`). Resolved via
    /// [`crate::config::effective_review_enabled`] with the precedence
    /// step then plan then global then false (mirroring `RetryStrategy`).
    /// Stored as a nullable INTEGER tri-state bool; wired but not yet
    /// consumed in this batch.
    #[serde(default)]
    pub review_enabled: Option<bool>,
    /// Per-step nondeterministic-review verdict (V27). `None` (the on-disk
    /// NULL) means [`ReviewStatus::Pending`] — not yet reviewed. Orthogonal
    /// to [`Step::status`] exactly as `TestStatus` is orthogonal to the
    /// termination reason. Wired but not yet consumed in this batch.
    #[serde(default)]
    pub review_status: Option<ReviewStatus>,
    /// For a reviewer-inserted corrective step (§10), the `steps.id` of the
    /// step it corrects. `None` for ordinary, non-corrective steps. Wired
    /// but not yet consumed in this batch.
    #[serde(default)]
    pub corrects_step_id: Option<String>,
}

impl Step {
    /// Read a Step from a SQLite row.
    ///
    /// Expected column order:
    /// id, plan_id, sort_key, title, description, agent, harness,
    /// acceptance_criteria, status, attempts, max_retries, created_at,
    /// updated_at, model, skipped_reason, change_policy, tags,
    /// retry_strategy, short_id, review_enabled, review_status,
    /// corrects_step_id
    pub fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        let criteria_json: String = row.get(7)?;
        let acceptance_criteria: Vec<String> =
            serde_json::from_str(&criteria_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

        let status_str: String = row.get(8)?;
        let status: StepStatus = status_str.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let created_str: String = row.get(11)?;
        let created_at = parse_datetime(&created_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let updated_str: String = row.get(12)?;
        let updated_at = parse_datetime(&updated_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let change_policy_str: String = row.get(15)?;
        let change_policy: ChangePolicy = change_policy_str.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(15, rusqlite::types::Type::Text, Box::new(e))
        })?;

        // Tags are stored as a JSON array on column 16. SELECTs that predate
        // V13 won't include the column (handled by callers using the
        // canonical column list); for callers that do include it, a NULL or
        // empty string is defensively treated as an empty list so raw rows
        // from legacy inserts keep round-tripping.
        let tags_raw: Option<String> = row.get(16).ok();
        let tags: Vec<String> = match tags_raw.as_deref() {
            None | Some("") => Vec::new(),
            Some(json) => serde_json::from_str(json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    16,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        };

        // `retry_strategy` is a nullable TEXT column (V24) at index 17. NULL
        // means "no step-level override"; a non-null value must parse to a
        // known variant.
        let retry_strategy_str: Option<String> = row.get(17)?;
        let retry_strategy = match retry_strategy_str {
            Some(s) => Some(s.parse::<RetryStrategy>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    17,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        // `short_id` is the V25 plan-unique handle on column 18. SELECTs
        // that predate V25 omit the column and raw test inserts may leave
        // it NULL; either case is defensively mapped to an empty string so
        // legacy rows keep round-tripping (mirrors the `tags` handling).
        let short_id: String = row.get::<_, String>(18).ok().unwrap_or_default();

        // V27 review columns at indices 19/20/21. SELECTs that predate V27
        // omit them and raw test inserts may leave them NULL; `.get(..).ok()`
        // + the `Option` mapping defensively treats either case as the
        // inherit / pending default so legacy rows keep round-tripping
        // (mirrors the `short_id` / `tags` handling above).
        //
        // - `review_enabled` (19): nullable INTEGER tri-state bool; non-null
        //   coerces to a bool (any non-zero = true), like `questions_enabled`
        //   on `Plan`.
        // - `review_status` (20): nullable TEXT; a non-null value must parse
        //   to a known `ReviewStatus` variant (NULL = pending).
        // - `corrects_step_id` (21): nullable TEXT step-id pointer.
        let review_enabled: Option<bool> = row
            .get::<_, Option<i64>>(19)
            .ok()
            .flatten()
            .map(|v| v != 0);
        let review_status_str: Option<String> = row.get::<_, Option<String>>(20).ok().flatten();
        let review_status = match review_status_str {
            Some(s) => Some(s.parse::<ReviewStatus>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    20,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };
        let corrects_step_id: Option<String> = row.get::<_, Option<String>>(21).ok().flatten();

        Ok(Step {
            id: row.get(0)?,
            short_id,
            plan_id: row.get(1)?,
            sort_key: row.get(2)?,
            title: row.get(3)?,
            description: row.get(4)?,
            agent: row.get(5)?,
            harness: row.get(6)?,
            acceptance_criteria,
            status,
            attempts: row.get(9)?,
            max_retries: row.get(10)?,
            created_at,
            updated_at,
            model: row.get(13)?,
            skipped_reason: row.get(14)?,
            change_policy,
            tags,
            retry_strategy,
            review_enabled,
            review_status,
            corrects_step_id,
        })
    }

    /// Resolve the effective retry strategy for this step.
    ///
    /// Precedence is **step > plan > default**: a step-level override wins
    /// over a plan-level default, which in turn wins over the built-in
    /// default ([`RetryStrategy::Keep`]). `None` at a level means "defer to
    /// the next level down".
    #[allow(dead_code)] // consumed by the executor retry loop in a later step
    pub fn effective_retry_strategy(&self, plan: &Plan) -> RetryStrategy {
        self.retry_strategy
            .or(plan.retry_strategy)
            .unwrap_or(RetryStrategy::Keep)
    }
}

// The pre-Phase-2 `AnsweredQuestion` struct was removed in the §8/§4 cutover.
// Prompt assembly now consumes the bounded, interruption-native
// `Interruption` model (see `Interruption` above and
// `storage::list_resolved_interruptions_for_step`).

// ---------------------------------------------------------------------------
// ExecutionLog struct
// ---------------------------------------------------------------------------

/// A log entry for one attempt at executing a step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionLog {
    pub id: i64,
    pub step_id: String,
    pub attempt: i32,
    pub started_at: DateTime<Utc>,
    pub duration_secs: Option<f64>,
    pub prompt_text: Option<String>,
    pub diff: Option<String>,
    pub test_results: Vec<String>,
    pub rolled_back: bool,
    pub committed: bool,
    pub commit_hash: Option<String>,
    pub harness_stdout: Option<String>,
    pub harness_stderr: Option<String>,
    pub cost_usd: Option<f64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub termination_reason: Option<TerminationReason>,
    #[serde(default)]
    pub test_status: Option<TestStatus>,
}

impl ExecutionLog {
    /// Read an ExecutionLog from a SQLite row.
    ///
    /// Expected column order:
    /// id, step_id, attempt, started_at, duration_secs, prompt_text, diff,
    /// test_results, rolled_back, committed, commit_hash,
    /// harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens,
    /// session_id, termination_reason, test_status
    pub fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        let started_str: String = row.get(3)?;
        let started_at = parse_datetime(&started_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let tests_json: String = row.get(7)?;
        let test_results: Vec<String> = serde_json::from_str(&tests_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let rolled_back_int: i32 = row.get(8)?;
        let committed_int: i32 = row.get(9)?;

        let termination_reason_str: Option<String> = row.get(17)?;
        let termination_reason = match termination_reason_str {
            Some(s) => Some(s.parse::<TerminationReason>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    17,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        let test_status_str: Option<String> = row.get(18)?;
        let test_status = match test_status_str {
            Some(s) => Some(s.parse::<TestStatus>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    18,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        Ok(ExecutionLog {
            id: row.get(0)?,
            step_id: row.get(1)?,
            attempt: row.get(2)?,
            started_at,
            duration_secs: row.get(4)?,
            prompt_text: row.get(5)?,
            diff: row.get(6)?,
            test_results,
            rolled_back: rolled_back_int != 0,
            committed: committed_int != 0,
            commit_hash: row.get(10)?,
            harness_stdout: row.get(11)?,
            harness_stderr: row.get(12)?,
            cost_usd: row.get(13)?,
            input_tokens: row.get(14)?,
            output_tokens: row.get(15)?,
            session_id: row.get(16)?,
            termination_reason,
            test_status,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse an ISO 8601 datetime string from SQLite.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    // SQLite stores as "YYYY-MM-DDTHH:MM:SS.fffZ"
    s.parse::<DateTime<Utc>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    #[test]
    fn test_plan_status_roundtrip() {
        let statuses = [
            PlanStatus::Planning,
            PlanStatus::Ready,
            PlanStatus::InProgress,
            PlanStatus::Complete,
            PlanStatus::Failed,
            PlanStatus::Aborted,
            PlanStatus::Archived,
            PlanStatus::Interrupted,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: PlanStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_plan_status_interrupted_serde_and_display() {
        // The derived `Interrupted` status must serialize as snake_case
        // (matches every other variant) so the TUI/JSON output renders it
        // uniformly without a special case.
        assert_eq!(PlanStatus::Interrupted.as_str(), "interrupted");
        assert_eq!(PlanStatus::Interrupted.to_string(), "interrupted");
        assert_eq!(
            serde_json::to_string(&PlanStatus::Interrupted).unwrap(),
            r#""interrupted""#,
        );
        let parsed: PlanStatus = "interrupted".parse().unwrap();
        assert_eq!(parsed, PlanStatus::Interrupted);

        // One-release back-compat: the legacy "question" spelling still
        // parses (to the renamed variant) but never serializes back out.
        let legacy: PlanStatus = "question".parse().unwrap();
        assert_eq!(legacy, PlanStatus::Interrupted);
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#""interrupted""#,
            "legacy alias must serialize forward as `interrupted`",
        );
    }

    #[test]
    fn test_step_status_roundtrip() {
        let statuses = [
            StepStatus::Pending,
            StepStatus::InProgress,
            StepStatus::Complete,
            StepStatus::Failed,
            StepStatus::Skipped,
            StepStatus::Aborted,
            StepStatus::Blocked,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: StepStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
            // serde must agree with the FromStr/as_str pair.
            assert_eq!(
                serde_json::to_string(status).unwrap(),
                format!(r#""{s}""#),
                "{status:?} must serialize as its snake_case as_str()"
            );
        }
    }

    #[test]
    fn test_effective_step_status_blocked_is_derived_and_reversible() {
        // No open interruption: identity for every stored state.
        for stored in [
            StepStatus::Pending,
            StepStatus::InProgress,
            StepStatus::Complete,
            StepStatus::Failed,
            StepStatus::Skipped,
            StepStatus::Aborted,
        ] {
            assert_eq!(
                effective_step_status(stored, false),
                stored,
                "no open interruption ⇒ stored status passes through unchanged"
            );
        }

        // Open interruption shadows only the *active* lifecycle states.
        assert_eq!(
            effective_step_status(StepStatus::Pending, true),
            StepStatus::Blocked
        );
        assert_eq!(
            effective_step_status(StepStatus::InProgress, true),
            StepStatus::Blocked
        );

        // Terminal states are NOT re-shadowed by a (stale) open flag.
        for terminal in [
            StepStatus::Complete,
            StepStatus::Failed,
            StepStatus::Skipped,
            StepStatus::Aborted,
        ] {
            assert_eq!(
                effective_step_status(terminal, true),
                terminal,
                "terminal {terminal:?} must not present as Blocked"
            );
        }

        // Reversible: the same stored status un-shadows the instant the
        // interruption resolves (overlay carries no state of its own).
        let stored = StepStatus::InProgress;
        assert_eq!(effective_step_status(stored, true), StepStatus::Blocked);
        assert_eq!(
            effective_step_status(stored, false),
            StepStatus::InProgress,
            "resolving the interruption restores the underlying status"
        );
    }

    #[test]
    fn test_blocked_overlay_is_never_persisted_by_storage() {
        // The overlay is presentation-only: prove no storage write path can
        // land `blocked` in `steps.status`. We round-trip a step through the
        // DB and assert the column never holds 'blocked' even after deriving
        // a Blocked presentation from an open interruption.
        let conn = db::open_memory().unwrap();
        let plan =
            crate::storage::create_plan(&conn, "ov", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = crate::storage::create_step(
            &conn, &plan.id, "S", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();

        // Raise an open interruption, then derive the presentation status.
        crate::storage::insert_interruption(
            &conn,
            &step.id,
            1,
            InterruptionKind::Blocker,
            "blocked on access",
            &[],
        )
        .unwrap();
        let reloaded = crate::storage::get_step(&conn, &step.id).unwrap();
        let has_open = !crate::storage::list_open_interruptions_for_plan(&conn, &plan.id)
            .unwrap()
            .is_empty();
        assert_eq!(
            effective_step_status(reloaded.status, has_open),
            StepStatus::Blocked,
            "derived presentation is Blocked while the interruption is open"
        );

        // The *stored* column is still the underlying lifecycle, never
        // 'blocked'.
        let raw: String = conn
            .query_row(
                "SELECT status FROM steps WHERE id = ?1",
                rusqlite::params![step.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_ne!(raw, "blocked", "Blocked must never be written to the DB");
        assert_eq!(reloaded.status, StepStatus::Pending);
    }

    #[test]
    fn test_plan_status_serialize_lowercase() {
        let json = serde_json::to_string(&PlanStatus::InProgress).unwrap();
        assert_eq!(json, r#""in_progress""#);
    }

    #[test]
    fn test_step_status_serialize_lowercase() {
        let json = serde_json::to_string(&StepStatus::InProgress).unwrap();
        assert_eq!(json, r#""in_progress""#);
    }

    #[test]
    fn test_plan_from_row() {
        let conn = db::open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, harness, agent, deterministic_tests, plan_harness)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                "p1",
                "my-plan",
                "/tmp/proj",
                "feat/branch",
                "A test plan",
                "claude-code",
                "opus",
                r#"["cargo test","cargo clippy"]"#,
                "goose",
            ],
        )
        .expect("insert plan");

        let query = format!("SELECT {PLAN_COLUMNS} FROM plans WHERE id = ?1");
        let plan = conn
            .query_row(&query, ["p1"], Plan::from_row)
            .expect("query plan");

        assert_eq!(plan.id, "p1");
        assert_eq!(plan.slug, "my-plan");
        assert_eq!(plan.project, "/tmp/proj");
        assert_eq!(plan.branch_name, "feat/branch");
        assert_eq!(plan.description, "A test plan");
        assert_eq!(plan.status, PlanStatus::Planning);
        assert_eq!(plan.harness.as_deref(), Some("claude-code"));
        assert_eq!(plan.agent.as_deref(), Some("opus"));
        assert_eq!(plan.deterministic_tests, vec!["cargo test", "cargo clippy"]);
        assert_eq!(plan.plan_harness.as_deref(), Some("goose"));
    }

    #[test]
    fn test_plan_columns_matches_physical_table_order() {
        // PLAN_COLUMNS must enumerate columns in the order SQLite stores them,
        // so `from_row` indices line up even if a caller were to use
        // `SELECT *`. Guard against someone editing PLAN_COLUMNS without
        // checking the migration layout.
        let conn = db::open_memory().expect("open_memory");
        let physical: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .expect("prepare")
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        let canonical: Vec<&str> = PLAN_COLUMNS.split(", ").collect();
        assert_eq!(
            physical.iter().map(String::as_str).collect::<Vec<_>>(),
            canonical,
            "PLAN_COLUMNS drifted from the physical plans table layout"
        );
    }

    #[test]
    fn test_plan_from_row_roundtrip_via_plan_columns() {
        // Round-trip every field through the canonical SELECT list.
        let conn = db::open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, plan_harness)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                "p1",
                "my-plan",
                "/tmp/proj",
                "feat/branch",
                "A test plan",
                "in_progress",
                "claude-code",
                "opus",
                r#"["cargo test"]"#,
                "goose",
            ],
        )
        .expect("insert plan");

        let query = format!("SELECT {PLAN_COLUMNS} FROM plans WHERE id = ?1");
        let plan = conn
            .query_row(&query, ["p1"], Plan::from_row)
            .expect("query plan");

        assert_eq!(plan.id, "p1");
        assert_eq!(plan.slug, "my-plan");
        assert_eq!(plan.project, "/tmp/proj");
        assert_eq!(plan.branch_name, "feat/branch");
        assert_eq!(plan.description, "A test plan");
        assert_eq!(plan.status, PlanStatus::InProgress);
        assert_eq!(plan.harness.as_deref(), Some("claude-code"));
        assert_eq!(plan.agent.as_deref(), Some("opus"));
        assert_eq!(plan.deterministic_tests, vec!["cargo test"]);
        assert_eq!(plan.plan_harness.as_deref(), Some("goose"));
        // Confirm timestamps parsed as real DateTimes rather than swapped with
        // plan_harness — the bug this refactor prevents.
        assert!(plan.created_at <= plan.updated_at);
    }

    #[test]
    fn test_step_from_row() {
        let conn = db::open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "branch", "desc"],
        )
        .expect("insert plan");

        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, short_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                "s1",
                "p1",
                "a0",
                "Step 1",
                "First step",
                "opus",
                "claude-code",
                r#"["tests pass","lint clean"]"#,
                "in_progress",
                2,
                3,
                "abcd1234",
            ],
        )
        .expect("insert step");

        let step = conn
            .query_row(
                "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at, model, skipped_reason, change_policy, tags, retry_strategy, short_id FROM steps WHERE id = ?1",
                ["s1"],
                Step::from_row,
            )
            .expect("query step");

        assert_eq!(step.id, "s1");
        assert_eq!(step.short_id, "abcd1234");
        assert_eq!(step.plan_id, "p1");
        assert_eq!(step.sort_key, "a0");
        assert_eq!(step.title, "Step 1");
        assert_eq!(step.status, StepStatus::InProgress);
        assert_eq!(step.attempts, 2);
        assert_eq!(step.max_retries, Some(3));
        assert_eq!(step.acceptance_criteria, vec!["tests pass", "lint clean"]);
    }

    #[test]
    fn test_execution_log_from_row() {
        let conn = db::open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "branch", "desc"],
        )
        .expect("insert plan");

        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "desc"],
        )
        .expect("insert step");

        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            rusqlite::params![
                "s1",
                1,
                45.5,
                "implement feature",
                "+added line",
                r#"["test1: pass","test2: fail"]"#,
                0,
                1,
                "abc123",
                "stdout output",
                "stderr output",
                0.05,
                1000,
                500,
                "session-1",
            ],
        )
        .expect("insert log");

        let log = conn
            .query_row(
                "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id, termination_reason, test_status FROM execution_logs WHERE step_id = ?1",
                ["s1"],
                ExecutionLog::from_row,
            )
            .expect("query log");

        assert_eq!(log.step_id, "s1");
        assert_eq!(log.attempt, 1);
        assert_eq!(log.duration_secs, Some(45.5));
        assert_eq!(log.prompt_text.as_deref(), Some("implement feature"));
        assert_eq!(log.diff.as_deref(), Some("+added line"));
        assert_eq!(log.test_results, vec!["test1: pass", "test2: fail"]);
        assert!(!log.rolled_back);
        assert!(log.committed);
        assert_eq!(log.commit_hash.as_deref(), Some("abc123"));
        assert_eq!(log.cost_usd, Some(0.05));
        assert_eq!(log.input_tokens, Some(1000));
        assert_eq!(log.output_tokens, Some(500));
        assert_eq!(log.session_id.as_deref(), Some("session-1"));
    }

    #[test]
    fn test_plan_status_display() {
        assert_eq!(PlanStatus::InProgress.to_string(), "in_progress");
        assert_eq!(PlanStatus::Planning.to_string(), "planning");
    }

    #[test]
    fn test_step_status_display() {
        assert_eq!(StepStatus::InProgress.to_string(), "in_progress");
        assert_eq!(StepStatus::Pending.to_string(), "pending");
    }

    #[test]
    fn test_invalid_plan_status() {
        let result: Result<PlanStatus, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_step_status() {
        let result: Result<StepStatus, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_phase_roundtrip() {
        let phases = [
            Phase::Idle,
            Phase::PreStepHook,
            Phase::Harness,
            Phase::PreTestHook,
            Phase::Tests,
            Phase::PostTestHook,
            Phase::Commit,
            Phase::Rollback,
            Phase::PostStepHook,
        ];
        for phase in &phases {
            let s = phase.as_str();
            let parsed: Phase = s.parse().unwrap();
            assert_eq!(*phase, parsed);
        }
    }

    #[test]
    fn test_phase_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&Phase::PreStepHook).unwrap(),
            r#""pre_step_hook""#,
        );
        assert_eq!(
            serde_json::to_string(&Phase::PostStepHook).unwrap(),
            r#""post_step_hook""#,
        );
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(Phase::Harness.to_string(), "harness");
        assert_eq!(Phase::PreTestHook.to_string(), "pre_test_hook");
    }

    #[test]
    fn test_invalid_phase() {
        let result: Result<Phase, _> = "bogus".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_termination_reason_roundtrip() {
        let reasons = [
            TerminationReason::Success,
            TerminationReason::UserInterrupted,
            TerminationReason::Timeout,
            TerminationReason::TestFailed,
            TerminationReason::NoChanges,
            TerminationReason::HookFailed,
            TerminationReason::HarnessFailed,
            TerminationReason::CommitFailed,
            TerminationReason::RollbackFailed,
            TerminationReason::InsufficientDiskSpace,
            TerminationReason::PausedForQuestion,
            TerminationReason::PausedByUser,
            TerminationReason::UserSkipped,
            TerminationReason::Unknown,
        ];
        for r in &reasons {
            let s = r.as_str();
            let parsed: TerminationReason = s.parse().unwrap();
            assert_eq!(*r, parsed);
        }
    }

    #[test]
    fn test_termination_reason_paused_for_question_serde_and_display() {
        assert_eq!(
            TerminationReason::PausedForQuestion.as_str(),
            "paused_for_question",
        );
        assert_eq!(
            TerminationReason::PausedForQuestion.to_string(),
            "paused_for_question",
        );
        assert_eq!(
            serde_json::to_string(&TerminationReason::PausedForQuestion).unwrap(),
            r#""paused_for_question""#,
        );
        let parsed: TerminationReason = "paused_for_question".parse().unwrap();
        assert_eq!(parsed, TerminationReason::PausedForQuestion);
    }

    #[test]
    fn test_termination_reason_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&TerminationReason::UserInterrupted).unwrap(),
            r#""user_interrupted""#,
        );
    }

    #[test]
    fn test_termination_reason_display() {
        assert_eq!(TerminationReason::Success.to_string(), "success");
        assert_eq!(TerminationReason::CommitFailed.to_string(), "commit_failed");
    }

    #[test]
    fn test_invalid_termination_reason() {
        let result: Result<TerminationReason, _> = "nope".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_test_status_roundtrip() {
        let statuses = [
            TestStatus::NotConfigured,
            TestStatus::NotRun,
            TestStatus::Passed,
            TestStatus::Failed,
            TestStatus::Aborted,
            TestStatus::TimedOut,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: TestStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_test_status_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&TestStatus::NotConfigured).unwrap(),
            r#""not_configured""#,
        );
        assert_eq!(
            serde_json::to_string(&TestStatus::TimedOut).unwrap(),
            r#""timed_out""#,
        );
    }

    #[test]
    fn test_test_status_display() {
        assert_eq!(TestStatus::Passed.to_string(), "passed");
        assert_eq!(TestStatus::TimedOut.to_string(), "timed_out");
    }

    #[test]
    fn test_invalid_test_status() {
        let result: Result<TestStatus, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_change_policy_roundtrip() {
        let policies = [ChangePolicy::Required, ChangePolicy::Optional];
        for p in &policies {
            let s = p.as_str();
            let parsed: ChangePolicy = s.parse().unwrap();
            assert_eq!(*p, parsed);
        }
    }

    #[test]
    fn test_change_policy_default_is_required() {
        assert_eq!(ChangePolicy::default(), ChangePolicy::Required);
    }

    #[test]
    fn test_change_policy_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&ChangePolicy::Required).unwrap(),
            r#""required""#,
        );
        assert_eq!(
            serde_json::to_string(&ChangePolicy::Optional).unwrap(),
            r#""optional""#,
        );
    }

    #[test]
    fn test_change_policy_display() {
        assert_eq!(ChangePolicy::Required.to_string(), "required");
        assert_eq!(ChangePolicy::Optional.to_string(), "optional");
    }

    #[test]
    fn test_invalid_change_policy() {
        let result: Result<ChangePolicy, _> = "forbidden".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_step_serde_defaults_change_policy_when_missing() {
        // Old exported plan JSON lacks `change_policy`. The serde(default)
        // attribute must backfill it to Required so round-tripping through
        // serde doesn't lose or change the effective policy.
        let json = r#"{
            "id": "s1",
            "plan_id": "p1",
            "sort_key": "a0",
            "title": "T",
            "description": "",
            "agent": null,
            "harness": null,
            "acceptance_criteria": [],
            "status": "pending",
            "attempts": 0,
            "max_retries": null,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let step: Step = serde_json::from_str(json).unwrap();
        assert_eq!(step.change_policy, ChangePolicy::Required);
    }

    #[test]
    fn test_step_serde_preserves_optional_change_policy() {
        let json = r#"{
            "id": "s1",
            "plan_id": "p1",
            "sort_key": "a0",
            "title": "Review",
            "description": "",
            "agent": null,
            "harness": null,
            "acceptance_criteria": [],
            "status": "pending",
            "attempts": 0,
            "max_retries": null,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "change_policy": "optional"
        }"#;
        let step: Step = serde_json::from_str(json).unwrap();
        assert_eq!(step.change_policy, ChangePolicy::Optional);
    }

    #[test]
    fn test_retry_strategy_roundtrip() {
        let strategies = [RetryStrategy::Keep, RetryStrategy::Rollback];
        for s in &strategies {
            let token = s.as_str();
            let parsed: RetryStrategy = token.parse().unwrap();
            assert_eq!(*s, parsed);
        }
    }

    #[test]
    fn test_retry_strategy_default_is_keep() {
        assert_eq!(RetryStrategy::default(), RetryStrategy::Keep);
    }

    #[test]
    fn test_retry_strategy_display() {
        assert_eq!(RetryStrategy::Keep.to_string(), "keep");
        assert_eq!(RetryStrategy::Rollback.to_string(), "rollback");
    }

    #[test]
    fn test_retry_strategy_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&RetryStrategy::Keep).unwrap(),
            r#""keep""#,
        );
        assert_eq!(
            serde_json::to_string(&RetryStrategy::Rollback).unwrap(),
            r#""rollback""#,
        );
    }

    #[test]
    fn test_invalid_retry_strategy() {
        let result: Result<RetryStrategy, _> = "discard".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_review_status_roundtrip() {
        let statuses = [
            ReviewStatus::Pending,
            ReviewStatus::InFlight,
            ReviewStatus::Passed,
            ReviewStatus::Failed,
            ReviewStatus::Skipped,
            ReviewStatus::Disabled,
        ];
        for s in &statuses {
            let token = s.as_str();
            let parsed: ReviewStatus = token.parse().unwrap();
            assert_eq!(*s, parsed);
            // serde round-trips through the same snake_case token.
            let json = serde_json::to_string(s).unwrap();
            assert_eq!(json, format!("\"{token}\""));
            let de: ReviewStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*s, de);
        }
    }

    #[test]
    fn test_review_status_default_is_pending() {
        assert_eq!(ReviewStatus::default(), ReviewStatus::Pending);
    }

    #[test]
    fn test_review_status_display() {
        assert_eq!(ReviewStatus::Pending.to_string(), "pending");
        assert_eq!(ReviewStatus::InFlight.to_string(), "in_flight");
        assert_eq!(ReviewStatus::Passed.to_string(), "passed");
        assert_eq!(ReviewStatus::Failed.to_string(), "failed");
        assert_eq!(ReviewStatus::Skipped.to_string(), "skipped");
        assert_eq!(ReviewStatus::Disabled.to_string(), "disabled");
    }

    #[test]
    fn test_review_status_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&ReviewStatus::InFlight).unwrap(),
            r#""in_flight""#,
        );
        assert_eq!(
            serde_json::to_string(&ReviewStatus::Disabled).unwrap(),
            r#""disabled""#,
        );
    }

    #[test]
    fn test_invalid_review_status() {
        let result: Result<ReviewStatus, _> = "approved".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_step_serde_defaults_review_fields_when_missing() {
        // Pre-V27 exported plan JSON lacks the review fields. The
        // `#[serde(default)]` attributes must backfill them to the
        // inherit / pending defaults (all `None`) so round-tripping a
        // legacy bundle doesn't change effective review behavior.
        let json = r#"{
            "id": "s1",
            "plan_id": "p1",
            "sort_key": "a0",
            "title": "T",
            "description": "",
            "agent": null,
            "harness": null,
            "acceptance_criteria": [],
            "status": "pending",
            "attempts": 0,
            "max_retries": null,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let step: Step = serde_json::from_str(json).unwrap();
        assert_eq!(step.review_enabled, None);
        assert_eq!(step.review_status, None);
        assert_eq!(step.corrects_step_id, None);
    }

    #[test]
    fn test_plan_serde_defaults_review_enabled_when_missing() {
        // Pre-V27 exported plan JSON lacks `review_enabled`; serde(default)
        // must backfill it to `None` (inherit global).
        let json = r#"{
            "id": "p1",
            "slug": "s",
            "project": "/p",
            "branch_name": "b",
            "description": "d",
            "status": "planning",
            "harness": null,
            "agent": null,
            "deterministic_tests": [],
            "plan_harness": null,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let plan: Plan = serde_json::from_str(json).unwrap();
        assert_eq!(plan.review_enabled, None);
    }

    #[test]
    fn test_interruption_kind_roundtrip() {
        for k in [InterruptionKind::Question, InterruptionKind::Blocker] {
            let parsed: InterruptionKind = k.as_str().parse().unwrap();
            assert_eq!(k, parsed);
        }
    }

    #[test]
    fn test_interruption_kind_default_is_question() {
        assert_eq!(InterruptionKind::default(), InterruptionKind::Question);
    }

    #[test]
    fn test_interruption_kind_display() {
        assert_eq!(InterruptionKind::Question.to_string(), "question");
        assert_eq!(InterruptionKind::Blocker.to_string(), "blocker");
    }

    #[test]
    fn test_interruption_kind_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&InterruptionKind::Question).unwrap(),
            r#""question""#,
        );
        assert_eq!(
            serde_json::to_string(&InterruptionKind::Blocker).unwrap(),
            r#""blocker""#,
        );
    }

    #[test]
    fn test_invalid_interruption_kind() {
        let result: Result<InterruptionKind, _> = "answer".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_interruption_state_roundtrip() {
        for s in [InterruptionState::Open, InterruptionState::Resolved] {
            let parsed: InterruptionState = s.as_str().parse().unwrap();
            assert_eq!(s, parsed);
        }
    }

    #[test]
    fn test_interruption_state_default_is_open() {
        assert_eq!(InterruptionState::default(), InterruptionState::Open);
    }

    #[test]
    fn test_interruption_state_display() {
        assert_eq!(InterruptionState::Open.to_string(), "open");
        assert_eq!(InterruptionState::Resolved.to_string(), "resolved");
    }

    #[test]
    fn test_interruption_state_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&InterruptionState::Open).unwrap(),
            r#""open""#,
        );
        assert_eq!(
            serde_json::to_string(&InterruptionState::Resolved).unwrap(),
            r#""resolved""#,
        );
    }

    #[test]
    fn test_invalid_interruption_state() {
        let result: Result<InterruptionState, _> = "closed".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_interruption_serde_round_trip_snake_case() {
        let interruption = Interruption {
            id: "i1".into(),
            step_id: "s1".into(),
            attempt: 2,
            kind: InterruptionKind::Question,
            body: "Which database driver?".into(),
            options: vec![
                InterruptionOption {
                    text: "rusqlite (bundled)".into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: "sqlx".into(),
                    priority: 2,
                },
            ],
            resolution: Some("rusqlite (bundled)".into()),
            comment: Some("matches the zero-system-deps goal".into()),
            state: InterruptionState::Resolved,
            asked_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            resolved_at: Some("2026-01-02T00:00:00Z".parse().unwrap()),
        };

        let json = serde_json::to_string(&interruption).unwrap();
        // kind/state must serialize as snake_case tokens.
        assert!(json.contains(r#""kind":"question""#));
        assert!(json.contains(r#""state":"resolved""#));

        let back: Interruption = serde_json::from_str(&json).unwrap();
        assert_eq!(interruption, back);
    }

    #[test]
    fn test_interruption_blocker_serde_defaults_empty_options() {
        // A blocker omits options/resolution/comment/resolved_at; the
        // serde(default) attributes must backfill them.
        let json = r#"{
            "id": "i2",
            "step_id": "s2",
            "attempt": 1,
            "kind": "blocker",
            "body": "needs sudo to install package",
            "state": "open",
            "asked_at": "2026-01-01T00:00:00Z"
        }"#;
        let blocker: Interruption = serde_json::from_str(json).unwrap();
        assert_eq!(blocker.kind, InterruptionKind::Blocker);
        assert_eq!(blocker.state, InterruptionState::Open);
        assert!(blocker.options.is_empty());
        assert_eq!(blocker.resolution, None);
        assert_eq!(blocker.comment, None);
        assert_eq!(blocker.resolved_at, None);
    }

    #[test]
    fn test_effective_retry_strategy_precedence() {
        // Build a minimal Plan/Step pair and vary only the two
        // retry_strategy fields across all four combinations.
        fn make_plan(rs: Option<RetryStrategy>) -> Plan {
            Plan {
                id: "p1".into(),
                slug: "s".into(),
                project: "/p".into(),
                branch_name: "b".into(),
                description: "d".into(),
                status: PlanStatus::Planning,
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
                retry_strategy: rs,
                review_enabled: None,
                squash_on_complete: false,
            }
        }
        fn make_step(rs: Option<RetryStrategy>) -> Step {
            Step {
                id: "st1".into(),
                short_id: String::new(),
                plan_id: "p1".into(),
                sort_key: "a0".into(),
                title: "t".into(),
                description: "d".into(),
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
                change_policy: ChangePolicy::Required,
                tags: vec![],
                retry_strategy: rs,
                review_enabled: None,
                review_status: None,
                corrects_step_id: None,
            }
        }

        // (step None, plan None) -> default Keep
        assert_eq!(
            make_step(None).effective_retry_strategy(&make_plan(None)),
            RetryStrategy::Keep,
        );
        // (step Some, plan None) -> step
        assert_eq!(
            make_step(Some(RetryStrategy::Rollback)).effective_retry_strategy(&make_plan(None)),
            RetryStrategy::Rollback,
        );
        // (step None, plan Some) -> plan
        assert_eq!(
            make_step(None).effective_retry_strategy(&make_plan(Some(RetryStrategy::Rollback))),
            RetryStrategy::Rollback,
        );
        // (step Some, plan Some) -> step wins
        assert_eq!(
            make_step(Some(RetryStrategy::Keep))
                .effective_retry_strategy(&make_plan(Some(RetryStrategy::Rollback))),
            RetryStrategy::Keep,
        );
    }
}
