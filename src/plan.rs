// Plan and step lifecycle

use chrono::{DateTime, Utc};
use rusqlite::Row;
use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// PlanStatus enum
// ---------------------------------------------------------------------------

/// Status of a plan throughout its lifecycle.
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
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum,
)]
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
/// column through `updated_at`, V5 appended `plan_harness`, and V10 appended
/// `prompt_prefix` and `prompt_suffix` via `ALTER TABLE ... ADD COLUMN`.
/// Every `Plan`-returning query MUST use this list so [`Plan::from_row`]'s
/// indices line up — a raw `SELECT *` would otherwise swap columns.
pub const PLAN_COLUMNS: &str = "id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at, plan_harness, prompt_prefix, prompt_suffix";

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
    #[serde(default)]
    pub prompt_prefix: Option<String>,
    #[serde(default)]
    pub prompt_suffix: Option<String>,
}

impl Plan {
    /// Read a Plan from a SQLite row.
    ///
    /// Expected column order matches [`PLAN_COLUMNS`]:
    /// id, slug, project, branch_name, description, status, harness, agent,
    /// deterministic_tests, created_at, updated_at, plan_harness,
    /// prompt_prefix, prompt_suffix
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
            prompt_prefix: row.get(12)?,
            prompt_suffix: row.get(13)?,
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
}

impl Step {
    /// Read a Step from a SQLite row.
    ///
    /// Expected column order:
    /// id, plan_id, sort_key, title, description, agent, harness,
    /// acceptance_criteria, status, attempts, max_retries, created_at,
    /// updated_at, model, skipped_reason, change_policy
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
            rusqlite::Error::FromSqlConversionFailure(
                15,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;

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
        })
    }
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
        ];
        for status in &statuses {
            let s = status.as_str();
            let parsed: PlanStatus = s.parse().unwrap();
            assert_eq!(*status, parsed);
        }
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
                "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at, model, skipped_reason, change_policy FROM steps WHERE id = ?1",
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
            TerminationReason::Unknown,
        ];
        for r in &reasons {
            let s = r.as_str();
            let parsed: TerminationReason = s.parse().unwrap();
            assert_eq!(*r, parsed);
        }
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
        assert_eq!(
            TerminationReason::CommitFailed.to_string(),
            "commit_failed"
        );
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
}
