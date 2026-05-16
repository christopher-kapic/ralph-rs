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
/// Note: [`PlanStatus::Question`] is a *derived* status — it is never written
/// to `plans.status`. A plan is reported as `Question` whenever any unanswered
/// `step_questions` row exists for one of its steps; the underlying lifecycle
/// (in_progress/ready/etc.) stays in the column and un-shadows automatically
/// when the user answers. The variant exists in the enum so consumers
/// (TUI/JSON output) can render the derived state uniformly with the rest.
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
    Question,
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
            Self::Question => "question",
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
            "question" => Ok(Self::Question),
            other => Err(ParseStatusError(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// StepStatus enum
// ---------------------------------------------------------------------------

/// Status of an individual step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    InProgress,
    Complete,
    Failed,
    Skipped,
    Aborted,
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
        }
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
    /// Harness exited cleanly but recorded one or more unanswered
    /// `step_questions` rows during the attempt. The runner skips tests +
    /// commit, rolls back any diff, and pauses the plan until the user
    /// answers. Distinct from `HarnessFailed` so paused-for-clarification
    /// history doesn't pollute real-failure metrics.
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
/// V23 appended `skip_requested_step_id` + `skip_changes`, and V24
/// appended `retry_strategy` via `ALTER TABLE ... ADD COLUMN`. V10's
/// `prompt_prefix`/`prompt_suffix` and V14's `context_prepend` were
/// dropped again by V21 (preserving the physical order of the remaining
/// columns). Every `Plan`-returning query MUST use this list so
/// [`Plan::from_row`]'s indices line up — a raw `SELECT *` would
/// otherwise swap columns.
pub const PLAN_COLUMNS: &str = "id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at, plan_harness, questions_enabled, pause_requested, last_run_branch, last_run_started_at, skip_requested_step_id, skip_changes, retry_strategy";

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
    /// (default), `ralph question ask` invocations from a harness against a
    /// step in this plan are rejected and no `step_questions` rows are
    /// written. When `true`, the runner inspects unanswered questions
    /// after each attempt and may pause the plan. Toggled via
    /// `ralph plan questions on|off` and the `Q` keybinding in the TUI
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
}

impl Plan {
    /// Read a Plan from a SQLite row.
    ///
    /// Expected column order matches [`PLAN_COLUMNS`]:
    /// id, slug, project, branch_name, description, status, harness, agent,
    /// deterministic_tests, created_at, updated_at, plan_harness,
    /// questions_enabled, pause_requested, last_run_branch,
    /// last_run_started_at, skip_requested_step_id, skip_changes,
    /// retry_strategy
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
}

impl Step {
    /// Read a Step from a SQLite row.
    ///
    /// Expected column order:
    /// id, plan_id, sort_key, title, description, agent, harness,
    /// acceptance_criteria, status, attempts, max_retries, created_at,
    /// updated_at, model, skipped_reason, change_policy, tags,
    /// retry_strategy
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

        Ok(Step {
            id: row.get(0)?,
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

// ---------------------------------------------------------------------------
// AnsweredQuestion struct
// ---------------------------------------------------------------------------

/// A question that the harness asked via `ralph question ask` and the user
/// has since answered. Returned by
/// [`crate::storage::list_answered_questions_for_step`] in chronological order
/// and rendered into the prompt's "Previously answered questions" section on
/// the next attempt of the step (TUI-plan.md §17).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnsweredQuestion {
    pub question: String,
    pub answer: String,
}

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
            PlanStatus::Question,
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: PlanStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
        }
    }

    #[test]
    fn test_plan_status_question_serde_and_display() {
        // The derived `Question` status must serialize as snake_case
        // (matches every other variant) so the TUI/JSON output renders it
        // uniformly without a special case.
        assert_eq!(PlanStatus::Question.as_str(), "question");
        assert_eq!(PlanStatus::Question.to_string(), "question");
        assert_eq!(
            serde_json::to_string(&PlanStatus::Question).unwrap(),
            r#""question""#,
        );
        let parsed: PlanStatus = "question".parse().unwrap();
        assert_eq!(parsed, PlanStatus::Question);
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
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: StepStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
        }
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
            "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
            ],
        )
        .expect("insert step");

        let step = conn
            .query_row(
                "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at, model, skipped_reason, change_policy, tags, retry_strategy FROM steps WHERE id = ?1",
                ["s1"],
                Step::from_row,
            )
            .expect("query step");

        assert_eq!(step.id, "s1");
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
            }
        }
        fn make_step(rs: Option<RetryStrategy>) -> Step {
            Step {
                id: "st1".into(),
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
