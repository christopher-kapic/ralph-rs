// Output formatting — centralized helpers for display and JSON serialization.

use crate::plan::{ExecutionLog, Plan, PlanStatus, Step, StepStatus};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::io::{self, BufRead, IsTerminal, Write};

// ---------------------------------------------------------------------------
// NDJSON run events
// ---------------------------------------------------------------------------

/// Events emitted as NDJSON (one JSON object per line) when `--json` is active
/// on `run` or `resume`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    StepStarted {
        step_id: String,
        step_title: String,
        step_num: usize,
        step_total: usize,
    },
    StepFinished {
        step_id: String,
        step_title: String,
        step_num: usize,
        step_total: usize,
        outcome: String,
        attempts: i32,
        duration_secs: f64,
    },
    PlanComplete {
        plan_slug: String,
        final_status: PlanStatus,
        steps_executed: usize,
        steps_succeeded: usize,
        steps_failed: usize,
    },
}

/// Write a single NDJSON record to stdout and flush immediately.
///
/// This is the **only** path that writes to stdout in JSON/run mode.
pub fn emit_ndjson<T: Serialize>(value: &T) {
    // Ignore write errors (broken pipe, etc.) — same behavior as println!.
    let mut out = io::stdout().lock();
    let _ = serde_json::to_writer(&mut out, value);
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}

// ---------------------------------------------------------------------------
// OutputFormat enum
// ---------------------------------------------------------------------------

/// Selects between human-readable and machine-readable output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Plain,
    Json,
}

// ---------------------------------------------------------------------------
// OutputContext — threaded through every command handler
// ---------------------------------------------------------------------------

/// Aggregated output settings derived from CLI flags and environment.
#[derive(Debug, Clone)]
pub struct OutputContext {
    /// Whether to emit JSON or human-readable output.
    pub format: OutputFormat,
    /// Suppress progress / banner output when true.
    #[allow(dead_code)] // Wired in a later step.
    pub quiet: bool,
    /// Whether ANSI color codes should be emitted.
    pub color: bool,
}

impl OutputContext {
    /// Build an `OutputContext` from the parsed CLI flags.
    ///
    /// The final `color` value is `false` when any of the following hold:
    /// - `--no-color` was passed
    /// - `NO_COLOR` env var is set
    /// - stdout is not a TTY
    /// - `--json` was passed (machine output should never contain ANSI)
    pub fn from_cli(json: bool, quiet: bool, no_color: bool) -> Self {
        let format = if json {
            OutputFormat::Json
        } else {
            OutputFormat::Plain
        };
        let color = !json && !no_color && should_use_color();
        Self {
            format,
            quiet,
            color,
        }
    }
}

// ---------------------------------------------------------------------------
// Color / TTY detection
// ---------------------------------------------------------------------------

/// Returns `true` when ANSI escape codes should be emitted.
///
/// Conditions for color output:
/// - stdout is a TTY **and**
/// - the `NO_COLOR` environment variable is **not** set (any value counts).
pub fn should_use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    io::stdout().is_terminal()
}

// ---------------------------------------------------------------------------
// Status display helpers
// ---------------------------------------------------------------------------

/// Return a Unicode icon with ANSI color for the given step/plan status.
///
/// When `color` is false the bare icon is returned without escape codes.
pub fn status_icon(status: StepStatus, color: bool) -> &'static str {
    match (status, color) {
        (StepStatus::Pending, true) => "\x1b[90m○\x1b[0m",
        (StepStatus::InProgress, true) => "\x1b[34m▶\x1b[0m",
        (StepStatus::Complete, true) => "\x1b[32m✔\x1b[0m",
        (StepStatus::Failed, true) => "\x1b[31m✘\x1b[0m",
        (StepStatus::Skipped, true) => "\x1b[90m⊘\x1b[0m",
        (StepStatus::Aborted, true) => "\x1b[31m⊘\x1b[0m",
        (StepStatus::Pending, false) => "○",
        (StepStatus::InProgress, false) => "▶",
        (StepStatus::Complete, false) => "✔",
        (StepStatus::Failed, false) => "✘",
        (StepStatus::Skipped, false) => "⊘",
        (StepStatus::Aborted, false) => "⊘",
    }
}

/// Return the status string wrapped in ANSI color codes.
///
/// When `color` is false the plain status string is returned.
pub fn colored_status(status: StepStatus, color: bool) -> String {
    if !color {
        return status.as_str().to_string();
    }
    let code = match status {
        StepStatus::Pending => "\x1b[90m",
        StepStatus::InProgress => "\x1b[34m",
        StepStatus::Complete => "\x1b[32m",
        StepStatus::Failed => "\x1b[31m",
        StepStatus::Skipped => "\x1b[90m",
        StepStatus::Aborted => "\x1b[31m",
    };
    format!("{code}{}\x1b[0m", status.as_str())
}

/// Return a Unicode icon with ANSI color for plan-level statuses.
///
/// When `color` is false the bare icon is returned without escape codes.
pub fn plan_status_icon(status: PlanStatus, color: bool) -> &'static str {
    match (status, color) {
        (PlanStatus::Planning, true) => "\x1b[33m◯\x1b[0m",
        (PlanStatus::Ready, true) => "\x1b[36m◉\x1b[0m",
        (PlanStatus::InProgress, true) => "\x1b[34m▶\x1b[0m",
        (PlanStatus::Complete, true) => "\x1b[32m✔\x1b[0m",
        (PlanStatus::Failed, true) => "\x1b[31m✘\x1b[0m",
        (PlanStatus::Aborted, true) => "\x1b[31m⊘\x1b[0m",
        (PlanStatus::Archived, true) => "\x1b[90m▪\x1b[0m",
        (PlanStatus::Planning, false) => "◯",
        (PlanStatus::Ready, false) => "◉",
        (PlanStatus::InProgress, false) => "▶",
        (PlanStatus::Complete, false) => "✔",
        (PlanStatus::Failed, false) => "✘",
        (PlanStatus::Aborted, false) => "⊘",
        (PlanStatus::Archived, false) => "▪",
    }
}

/// Return the plan status string wrapped in ANSI color codes.
///
/// When `color` is false the plain status string is returned.
pub fn colored_plan_status(status: PlanStatus, color: bool) -> String {
    if !color {
        return status.as_str().to_string();
    }
    let code = match status {
        PlanStatus::Planning => "\x1b[33m",
        PlanStatus::Ready => "\x1b[36m",
        PlanStatus::InProgress => "\x1b[34m",
        PlanStatus::Complete => "\x1b[32m",
        PlanStatus::Failed => "\x1b[31m",
        PlanStatus::Aborted => "\x1b[31m",
        PlanStatus::Archived => "\x1b[90m",
    };
    format!("{code}{}\x1b[0m", status.as_str())
}

// ---------------------------------------------------------------------------
// General formatting helpers
// ---------------------------------------------------------------------------

/// Wrap text in ANSI bold when `color` is true, otherwise return as-is.
pub fn bold(text: &str, color: bool) -> String {
    if color {
        format!("\x1b[1m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

/// A green checkmark icon, colored when `color` is true.
pub fn check_icon(color: bool) -> &'static str {
    if color {
        "\x1b[32m\u{2714}\x1b[0m"
    } else {
        "\u{2714}"
    }
}

/// A colored severity icon for doctor checks.
pub fn severity_icon(severity: &str, color: bool) -> &'static str {
    match (severity, color) {
        ("pass", true) => "\x1b[32m\u{2714}\x1b[0m",
        ("warning", true) => "\x1b[33m\u{26a0}\x1b[0m",
        ("error", true) => "\x1b[31m\u{2718}\x1b[0m",
        ("pass", false) => "\u{2714}",
        ("warning", false) => "\u{26a0}",
        ("error", false) => "\u{2718}",
        _ => "?",
    }
}

/// A log-entry status icon: committed (green check), rolled-back (red ↺), or pending (gray ○).
pub fn log_status_icon(committed: bool, rolled_back: bool, color: bool) -> &'static str {
    match (committed, rolled_back, color) {
        (true, _, true) => "\x1b[32m\u{2714}\x1b[0m",
        (_, true, true) => "\x1b[31m\u{21ba}\x1b[0m",
        (_, _, true) => "\x1b[90m\u{25cb}\x1b[0m",
        (true, _, false) => "\u{2714}",
        (_, true, false) => "\u{21ba}",
        (_, _, false) => "\u{25cb}",
    }
}

// ---------------------------------------------------------------------------
// Interactive confirmation
// ---------------------------------------------------------------------------

/// Prompt the user for a yes/no confirmation on stdin.
///
/// Accepts `y`, `Y`, `yes`, `YES`, `Yes` (and similar) as affirmative.
/// Returns `false` for everything else (including empty input and EOF).
pub fn confirm(prompt: &str) -> Result<bool> {
    confirm_with_reader(prompt, &mut io::stdin().lock(), &mut io::stderr())
}

/// Testable confirmation implementation that reads from an arbitrary reader.
fn confirm_with_reader(
    prompt: &str,
    reader: &mut dyn BufRead,
    writer: &mut dyn Write,
) -> Result<bool> {
    write!(writer, "{} [y/N] ", prompt)?;
    writer.flush()?;
    let mut line = String::new();
    let bytes = reader.read_line(&mut line)?;
    if bytes == 0 {
        // EOF
        return Ok(false);
    }
    let trimmed = line.trim();
    Ok(matches!(trimmed, "y" | "Y" | "yes" | "Yes" | "YES"))
}

// ---------------------------------------------------------------------------
// JSON-friendly summary structs
// ---------------------------------------------------------------------------

/// Lightweight, serializable summary of a [`Plan`].
#[derive(Debug, Clone, Serialize)]
pub struct PlanSummary {
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
}

impl From<&Plan> for PlanSummary {
    fn from(p: &Plan) -> Self {
        Self {
            id: p.id.clone(),
            slug: p.slug.clone(),
            project: p.project.clone(),
            branch_name: p.branch_name.clone(),
            description: p.description.clone(),
            status: p.status,
            harness: p.harness.clone(),
            agent: p.agent.clone(),
            deterministic_tests: p.deterministic_tests.clone(),
            plan_harness: p.plan_harness.clone(),
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

/// Lightweight, serializable summary of a [`Step`].
#[derive(Debug, Clone, Serialize)]
pub struct StepSummary {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl From<&Step> for StepSummary {
    fn from(s: &Step) -> Self {
        Self {
            id: s.id.clone(),
            plan_id: s.plan_id.clone(),
            sort_key: s.sort_key.clone(),
            title: s.title.clone(),
            description: s.description.clone(),
            agent: s.agent.clone(),
            harness: s.harness.clone(),
            acceptance_criteria: s.acceptance_criteria.clone(),
            status: s.status,
            attempts: s.attempts,
            max_retries: s.max_retries,
            created_at: s.created_at,
            updated_at: s.updated_at,
            model: s.model.clone(),
        }
    }
}

/// Lightweight, serializable summary of an [`ExecutionLog`] entry.
#[derive(Debug, Clone, Serialize)]
pub struct LogEntrySummary {
    pub id: i64,
    pub step_id: String,
    pub attempt: i32,
    pub started_at: DateTime<Utc>,
    pub duration_secs: Option<f64>,
    pub test_results: Vec<String>,
    pub rolled_back: bool,
    pub committed: bool,
    pub commit_hash: Option<String>,
    pub cost_usd: Option<f64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub session_id: Option<String>,
    /// Included when `--full` or `--lines` is specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    /// Included when `--full` or `--lines` is specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
}

impl LogEntrySummary {
    /// Build a summary, controlling stdout/stderr inclusion via [`LogOutputMode`].
    ///
    /// - `Hidden` → `stdout`/`stderr` are `None` (omitted from JSON).
    /// - `Truncated(n)` → include up to `n` lines per stream.
    /// - `Full` → include full text, no truncation.
    pub fn new(l: &ExecutionLog, mode: &crate::commands::LogOutputMode) -> Self {
        use crate::commands::LogOutputMode;

        let include = |text: &Option<String>| -> Option<String> {
            match mode {
                LogOutputMode::Hidden => None,
                LogOutputMode::Full => text.clone(),
                LogOutputMode::Truncated(n) => text
                    .as_ref()
                    .map(|s| s.lines().take(*n).collect::<Vec<_>>().join("\n")),
            }
        };

        Self {
            id: l.id,
            step_id: l.step_id.clone(),
            attempt: l.attempt,
            started_at: l.started_at,
            duration_secs: l.duration_secs,
            test_results: l.test_results.clone(),
            rolled_back: l.rolled_back,
            committed: l.committed,
            commit_hash: l.commit_hash.clone(),
            cost_usd: l.cost_usd,
            input_tokens: l.input_tokens,
            output_tokens: l.output_tokens,
            session_id: l.session_id.clone(),
            stdout: include(&l.harness_stdout),
            stderr: include(&l.harness_stderr),
        }
    }
}

impl From<&ExecutionLog> for LogEntrySummary {
    fn from(l: &ExecutionLog) -> Self {
        Self {
            id: l.id,
            step_id: l.step_id.clone(),
            attempt: l.attempt,
            started_at: l.started_at,
            duration_secs: l.duration_secs,
            test_results: l.test_results.clone(),
            rolled_back: l.rolled_back,
            committed: l.committed,
            commit_hash: l.commit_hash.clone(),
            cost_usd: l.cost_usd,
            input_tokens: l.input_tokens,
            output_tokens: l.output_tokens,
            session_id: l.session_id.clone(),
            stdout: None,
            stderr: None,
        }
    }
}

/// JSON output for the `status` command.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSummary {
    pub slug: String,
    pub status: PlanStatus,
    pub branch_name: String,
    pub steps: StepCounts,
}

/// Step count breakdown for the status command.
#[derive(Debug, Clone, Serialize)]
pub struct StepCounts {
    pub total: usize,
    pub complete: usize,
    pub failed: usize,
    pub skipped: usize,
    pub pending: usize,
    pub in_progress: usize,
}

/// JSON output for the `plan dependency list` command.
#[derive(Debug, Clone, Serialize)]
pub struct DependencyListSummary {
    pub slug: String,
    pub depends_on: Vec<String>,
    pub depended_on_by: Vec<String>,
}

/// JSON output for the `plan show` command (plan + steps).
#[derive(Debug, Clone, Serialize)]
pub struct PlanShowSummary {
    #[serde(flatten)]
    pub plan: PlanSummary,
    pub steps: Vec<StepSummary>,
}

/// JSON output for the `agents list` command.
#[derive(Debug, Clone, Serialize)]
pub struct AgentInfo {
    pub name: String,
    pub size_bytes: u64,
}

/// JSON output for the `hooks list` command.
#[derive(Debug, Clone, Serialize)]
pub struct HookInfo {
    pub name: String,
    pub lifecycle: String,
    pub scope: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -- should_use_color ---------------------------------------------------

    #[test]
    fn test_should_use_color_respects_no_color_env() {
        // When NO_COLOR is set, should_use_color must return false regardless
        // of terminal state.  We set the env var, check, then restore.
        let prev = std::env::var_os("NO_COLOR");
        // SAFETY: test is single-threaded; we restore the original value immediately.
        unsafe { std::env::set_var("NO_COLOR", "1") };
        assert!(!should_use_color());
        match prev {
            Some(val) => unsafe { std::env::set_var("NO_COLOR", val) },
            None => unsafe { std::env::remove_var("NO_COLOR") },
        }
    }

    // -- status_icon --------------------------------------------------------

    #[test]
    fn test_status_icon_with_color() {
        let icon = status_icon(StepStatus::Complete, true);
        assert!(
            icon.contains('\x1b'),
            "expected ANSI escape in colored icon"
        );
        assert!(icon.contains('✔'));
    }

    #[test]
    fn test_status_icon_without_color() {
        let icon = status_icon(StepStatus::Complete, false);
        assert!(!icon.contains('\x1b'), "no ANSI escapes expected");
        assert_eq!(icon, "✔");
    }

    #[test]
    fn test_status_icon_all_variants() {
        for status in [
            StepStatus::Pending,
            StepStatus::InProgress,
            StepStatus::Complete,
            StepStatus::Failed,
            StepStatus::Skipped,
            StepStatus::Aborted,
        ] {
            let plain = status_icon(status, false);
            assert!(!plain.is_empty());
            let colored = status_icon(status, true);
            assert!(colored.contains('\x1b'));
        }
    }

    // -- colored_status -----------------------------------------------------

    #[test]
    fn test_colored_status_with_color() {
        let out = colored_status(StepStatus::Failed, true);
        assert!(out.contains('\x1b'));
        assert!(out.contains("failed"));
    }

    #[test]
    fn test_colored_status_without_color() {
        let out = colored_status(StepStatus::Failed, false);
        assert!(!out.contains('\x1b'));
        assert_eq!(out, "failed");
    }

    // -- plan_status_icon ---------------------------------------------------

    #[test]
    fn test_plan_status_icon_all_variants() {
        for status in [
            PlanStatus::Planning,
            PlanStatus::Ready,
            PlanStatus::InProgress,
            PlanStatus::Complete,
            PlanStatus::Failed,
            PlanStatus::Aborted,
            PlanStatus::Archived,
        ] {
            let plain = plan_status_icon(status, false);
            assert!(!plain.is_empty());
            let colored = plan_status_icon(status, true);
            assert!(colored.contains('\x1b'));
        }
    }

    // -- confirm ------------------------------------------------------------

    #[test]
    fn test_confirm_y() {
        let mut input = Cursor::new(b"y\n");
        let mut output = Vec::new();
        assert!(confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_capital_y() {
        let mut input = Cursor::new(b"Y\n");
        let mut output = Vec::new();
        assert!(confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_yes() {
        let mut input = Cursor::new(b"yes\n");
        let mut output = Vec::new();
        assert!(confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_n() {
        let mut input = Cursor::new(b"n\n");
        let mut output = Vec::new();
        assert!(!confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_empty() {
        let mut input = Cursor::new(b"\n");
        let mut output = Vec::new();
        assert!(!confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_eof() {
        let mut input = Cursor::new(b"");
        let mut output = Vec::new();
        assert!(!confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_prompt_displayed() {
        let mut input = Cursor::new(b"n\n");
        let mut output = Vec::new();
        confirm_with_reader("Are you sure?", &mut input, &mut output).unwrap();
        let displayed = String::from_utf8(output).unwrap();
        assert!(displayed.contains("Are you sure?"));
        assert!(displayed.contains("[y/N]"));
    }

    // -- JSON summary structs -----------------------------------------------

    #[test]
    fn test_plan_summary_json_snake_case() {
        let summary = PlanSummary {
            id: "abc".into(),
            slug: "my-plan".into(),
            project: "/tmp".into(),
            branch_name: "feat/x".into(),
            description: "A plan".into(),
            status: PlanStatus::InProgress,
            harness: Some("claude-code".into()),
            agent: None,
            deterministic_tests: vec!["cargo test".into()],
            plan_harness: Some("goose".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&summary).unwrap();
        // Verify snake_case keys
        assert!(json.contains("\"branch_name\""));
        assert!(json.contains("\"deterministic_tests\""));
        assert!(json.contains("\"created_at\""));
        assert!(json.contains("\"updated_at\""));
        assert!(json.contains("\"in_progress\""));
        // Verify it round-trips through serde_json::Value
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["slug"], "my-plan");
    }

    #[test]
    fn test_step_summary_json_snake_case() {
        let summary = StepSummary {
            id: "s1".into(),
            plan_id: "p1".into(),
            sort_key: "a0".into(),
            title: "Step 1".into(),
            description: "desc".into(),
            agent: None,
            harness: None,
            acceptance_criteria: vec!["tests pass".into()],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"plan_id\""));
        assert!(json.contains("\"sort_key\""));
        assert!(json.contains("\"acceptance_criteria\""));
        assert!(json.contains("\"max_retries\""));
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["status"], "pending");
    }

    #[test]
    fn test_log_entry_summary_json_snake_case() {
        let summary = LogEntrySummary {
            id: 1,
            step_id: "s1".into(),
            attempt: 1,
            started_at: Utc::now(),
            duration_secs: Some(12.5),
            test_results: vec!["ok".into()],
            rolled_back: false,
            committed: true,
            commit_hash: Some("abc123".into()),
            cost_usd: Some(0.01),
            input_tokens: Some(500),
            output_tokens: Some(200),
            session_id: None,
            stdout: None,
            stderr: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"step_id\""));
        assert!(json.contains("\"started_at\""));
        assert!(json.contains("\"duration_secs\""));
        assert!(json.contains("\"test_results\""));
        assert!(json.contains("\"rolled_back\""));
        assert!(json.contains("\"commit_hash\""));
        assert!(json.contains("\"cost_usd\""));
        assert!(json.contains("\"input_tokens\""));
        assert!(json.contains("\"output_tokens\""));
        assert!(json.contains("\"session_id\""));
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["committed"], true);
    }
}
