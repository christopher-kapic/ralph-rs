// Plan and step lifecycle
#![allow(dead_code)]

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
// Plan struct
// ---------------------------------------------------------------------------

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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Plan {
    /// Read a Plan from a SQLite row.
    ///
    /// Expected column order:
    /// id, slug, project, branch_name, description, status,
    /// harness, agent, deterministic_tests, created_at, updated_at
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
            created_at,
            updated_at,
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
}

impl Step {
    /// Read a Step from a SQLite row.
    ///
    /// Expected column order:
    /// id, plan_id, sort_key, title, description, agent, harness,
    /// acceptance_criteria, status, attempts, max_retries, created_at, updated_at
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
}

impl ExecutionLog {
    /// Read an ExecutionLog from a SQLite row.
    ///
    /// Expected column order:
    /// id, step_id, attempt, started_at, duration_secs, prompt_text, diff,
    /// test_results, rolled_back, committed, commit_hash,
    /// harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id
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
            "INSERT INTO plans (id, slug, project, branch_name, description, harness, agent, deterministic_tests)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                "p1",
                "my-plan",
                "/tmp/proj",
                "feat/branch",
                "A test plan",
                "claude-code",
                "opus",
                r#"["cargo test","cargo clippy"]"#,
            ],
        )
        .expect("insert plan");

        let plan = conn
            .query_row(
                "SELECT id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at FROM plans WHERE id = ?1",
                ["p1"],
                Plan::from_row,
            )
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
                "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at FROM steps WHERE id = ?1",
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
                "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id FROM execution_logs WHERE step_id = ?1",
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
}
