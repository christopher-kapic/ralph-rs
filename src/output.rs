// Output formatting — centralized helpers for display and JSON serialization.

use crate::plan::{
    ChangePolicy, ExecutionLog, Phase, Plan, PlanStatus, Step, StepStatus, TerminationReason,
    TestStatus,
};
use crate::run_lock::LiveRun;
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
    /// Emitted at run start when orphaned InProgress step rows are flipped to
    /// Aborted. See `storage::sweep_stale_in_progress`.
    StaleStepsSwept { steps: Vec<StaleStep> },
    /// Emitted mid-run when the step list grows (steps inserted by the
    /// running agent via `ralph step add`) between iterations of the runner
    /// loop.
    PlanGrew { steps: Vec<StaleStep> },
    /// Emitted immediately before the harness is spawned for a given
    /// attempt. `prompt_preview` is always the first 512 chars of the
    /// prompt (regardless of `--verbose`) so JSON consumers see a stable
    /// bounded payload; the full prompt lives in `execution_log`.
    PromptPrepared {
        step_id: String,
        attempt: i32,
        prompt_chars: usize,
        prompt_preview: String,
    },
}

/// Compact reference to a step for NDJSON payloads.
#[derive(Debug, Clone, Serialize)]
pub struct StaleStep {
    pub step_id: String,
    pub step_num: usize,
    pub title: String,
}

/// Write a single NDJSON record to stdout and flush immediately.
///
/// This is the **only** path that writes to stdout in JSON/run mode.
/// Serialization and write errors are propagated: silently swallowing them
/// would produce corrupt machine-readable output.
pub fn emit_ndjson<T: Serialize>(value: &T) -> Result<()> {
    let mut out = io::stdout().lock();
    emit_ndjson_to(&mut out, value)
}

/// Testable variant of [`emit_ndjson`] that writes to an arbitrary writer.
fn emit_ndjson_to<W: Write, T: Serialize>(writer: &mut W, value: &T) -> Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
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

/// Return the termination-reason string wrapped in ANSI color codes.
///
/// When `color` is false the plain string is returned. Green for Success,
/// yellow for NoChanges (benign optional-policy no-op), gray for Unknown,
/// red for every terminal-error variant.
pub fn colored_termination_reason(reason: TerminationReason, color: bool) -> String {
    if !color {
        return reason.as_str().to_string();
    }
    let code = match reason {
        TerminationReason::Success => "\x1b[32m",
        TerminationReason::UserInterrupted
        | TerminationReason::Timeout
        | TerminationReason::TestFailed
        | TerminationReason::HookFailed
        | TerminationReason::HarnessFailed
        | TerminationReason::CommitFailed
        | TerminationReason::RollbackFailed
        | TerminationReason::InsufficientDiskSpace => "\x1b[31m",
        TerminationReason::NoChanges => "\x1b[33m",
        TerminationReason::Unknown => "\x1b[90m",
    };
    format!("{code}{}\x1b[0m", reason.as_str())
}

/// Return the test-status string wrapped in ANSI color codes.
///
/// When `color` is false the plain string is returned. Green for Passed,
/// red for Failed/Aborted/TimedOut, gray for NotConfigured/NotRun.
pub fn colored_test_status(status: TestStatus, color: bool) -> String {
    if !color {
        return status.as_str().to_string();
    }
    let code = match status {
        TestStatus::Passed => "\x1b[32m",
        TestStatus::Failed | TestStatus::Aborted | TestStatus::TimedOut => "\x1b[31m",
        TestStatus::NotConfigured | TestStatus::NotRun => "\x1b[90m",
    };
    format!("{code}{}\x1b[0m", status.as_str())
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

// ---------------------------------------------------------------------------
// Harness color map
// ---------------------------------------------------------------------------

/// Return the brand/accent color for a known harness name, or `None` for
/// unknown harnesses.
///
/// The colors here are the "canonical" per-harness hues used by the progress
/// header and any TUI widgets that want a consistent per-harness highlight.
/// Users can override per-harness via `HarnessConfig.color` in config.json.
pub fn harness_color(name: &str) -> Option<ratatui::style::Color> {
    use ratatui::style::Color;
    match name {
        "claude" => Some(Color::Rgb(0xcc, 0x8b, 0x89)),
        "codex" => Some(Color::Rgb(0x7a, 0xa8, 0xc1)),
        "opencode" => Some(Color::Rgb(0xf3, 0xb2, 0x6d)),
        "copilot" => Some(Color::Rgb(0xac, 0x4d, 0xb6)),
        _ => None,
    }
}

/// Parse a lenient `#RRGGBB` hex string into an `(r, g, b)` triple.
///
/// Returns `Err` on any of: missing leading `#`, wrong length, or any
/// non-hex digit. Callers (primarily `Config::load`) use the error message
/// verbatim in validation diagnostics.
pub fn parse_hex_color(s: &str) -> Result<(u8, u8, u8), String> {
    let trimmed = s.trim();
    let hex = match trimmed.strip_prefix('#') {
        Some(rest) => rest,
        None => return Err(format!("color '{trimmed}' must start with '#'")),
    };
    if hex.len() != 6 {
        return Err(format!(
            "color '{trimmed}' must be #RRGGBB (got {} hex digits)",
            hex.len()
        ));
    }
    let parse = |slice: &str, name: &str| -> Result<u8, String> {
        u8::from_str_radix(slice, 16)
            .map_err(|_| format!("color '{trimmed}' has invalid {name} component '{slice}'"))
    };
    let r = parse(&hex[0..2], "red")?;
    let g = parse(&hex[2..4], "green")?;
    let b = parse(&hex[4..6], "blue")?;
    Ok((r, g, b))
}

/// Resolve the effective harness color, preferring a per-harness config
/// override over the hardcoded [`harness_color`] map.
///
/// `override_hex` is the optional `color` field on [`crate::config::HarnessConfig`].
/// Invalid hex strings fall back to the hardcoded map; `Config::load` is
/// expected to reject malformed values up front, so this branch only
/// matters if a hex value snuck past validation.
pub fn resolved_harness_color(
    name: &str,
    override_hex: Option<&str>,
) -> Option<ratatui::style::Color> {
    use ratatui::style::Color;
    if let Some(hex) = override_hex
        && let Ok((r, g, b)) = parse_hex_color(hex)
    {
        return Some(Color::Rgb(r, g, b));
    }
    harness_color(name)
}

/// Format a harness name for human-readable stderr output.
///
/// When `color_enabled` is false, returns the name as-is. When true, wraps
/// the name in ANSI bold + 24-bit foreground color if the harness has a
/// known color (from [`harness_color`]); otherwise still bolds but emits
/// no color escape.
#[allow(dead_code)]
pub fn format_harness_label(name: &str, color_enabled: bool) -> String {
    format_harness_label_with_override(name, None, color_enabled)
}

/// Variant of [`format_harness_label`] that consults a per-harness config
/// override (hex `#RRGGBB`) before falling back to the hardcoded map.
pub fn format_harness_label_with_override(
    name: &str,
    override_hex: Option<&str>,
    color_enabled: bool,
) -> String {
    if !color_enabled {
        return name.to_string();
    }
    if let Some(ratatui::style::Color::Rgb(r, g, b)) = resolved_harness_color(name, override_hex) {
        return format!("\x1b[1;38;2;{r};{g};{b}m{name}\x1b[0m");
    }
    // Unknown harness: bold without color.
    format!("\x1b[1m{name}\x1b[0m")
}

// ---------------------------------------------------------------------------
// Timezone-aware "now" formatting
// ---------------------------------------------------------------------------

/// Format the current instant in the supplied IANA timezone.
///
/// Output shape: `YYYY-MM-DD HH:MM:SS TZABBR` — e.g. `2026-04-22 14:32:07 EDT`.
/// Used by the progress-header "started at" stamp so users see a local time
/// matching their `display_timezone` config instead of UTC.
pub fn format_now_in_tz(tz: &chrono_tz::Tz) -> String {
    format_instant_in_tz(chrono::Utc::now(), tz)
}

/// Testable variant of [`format_now_in_tz`] that formats a specific instant.
pub fn format_instant_in_tz(utc: DateTime<Utc>, tz: &chrono_tz::Tz) -> String {
    utc.with_timezone(tz)
        .format("%Y-%m-%d %H:%M:%S %Z")
        .to_string()
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
/// Accepts any case-insensitive variant of `y` or `yes` (e.g. `y`, `Y`,
/// `yes`, `Yes`, `YES`, `yEs`) as affirmative. Returns `false` for everything
/// else (including empty input and EOF).
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
    Ok(trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes"))
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_suffix: Option<String>,
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
            prompt_prefix: p.prompt_prefix.clone(),
            prompt_suffix: p.prompt_suffix.clone(),
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
    /// Always serialized (no skip_serializing_if) so JSON consumers see the
    /// policy explicitly rather than having to infer a default. Matches the
    /// `ExportedStep` emission policy.
    pub change_policy: ChangePolicy,
    /// Free-form string tags. Always serialized (even when empty) so JSON
    /// consumers know the field is present and default-empty rather than
    /// unsupported.
    pub tags: Vec<String>,
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
            change_policy: s.change_policy,
            tags: s.tags.clone(),
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
    /// Why the attempt terminated. Populated from the V11 `termination_reason`
    /// column on `execution_logs`; absent only for in-progress rows that
    /// haven't yet written a terminal outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<TerminationReason>,
    /// Outcome of the test phase. Separate from `termination_reason` because
    /// tests can be "not configured" or "not run" without the attempt itself
    /// terminating abnormally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_status: Option<TestStatus>,
}

/// Split a total line budget across stdout and stderr.
///
/// When one stream fits in its fair share, the other gets the remainder; when
/// both exceed half, the budget is split evenly (with any odd extra line going
/// to stdout). The combined output never exceeds `total`.
pub fn split_lines_budget(
    stdout_lines: usize,
    stderr_lines: usize,
    total: usize,
) -> (usize, usize) {
    let half = total / 2;
    let half_up = total - half;
    match (stdout_lines <= half, stderr_lines <= half) {
        (true, true) => (stdout_lines, stderr_lines),
        (true, false) => (stdout_lines, total - stdout_lines),
        (false, true) => (total - stderr_lines, stderr_lines),
        (false, false) => (half_up, half),
    }
}

impl LogEntrySummary {
    /// Build a summary, controlling stdout/stderr inclusion via [`LogOutputMode`].
    ///
    /// - `Hidden` → `stdout`/`stderr` are `None` (omitted from JSON).
    /// - `Truncated(n)` → include at most `n` lines **combined** across both
    ///   streams, allocated proportionally (see [`split_lines_budget`]).
    /// - `Full` → include full text, no truncation.
    pub fn new(l: &ExecutionLog, mode: &crate::commands::LogOutputMode) -> Self {
        use crate::commands::LogOutputMode;

        let (stdout, stderr) = match mode {
            LogOutputMode::Hidden => (None, None),
            LogOutputMode::Full => (l.harness_stdout.clone(), l.harness_stderr.clone()),
            LogOutputMode::Truncated(n) => {
                let stdout_lines = l.harness_stdout.as_deref().map(count_lines).unwrap_or(0);
                let stderr_lines = l.harness_stderr.as_deref().map(count_lines).unwrap_or(0);
                let (out_cap, err_cap) = split_lines_budget(stdout_lines, stderr_lines, *n);
                let take_head = |text: &Option<String>, cap: usize| -> Option<String> {
                    text.as_ref()
                        .map(|s| s.lines().take(cap).collect::<Vec<_>>().join("\n"))
                };
                (
                    take_head(&l.harness_stdout, out_cap),
                    take_head(&l.harness_stderr, err_cap),
                )
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
            stdout,
            stderr,
            termination_reason: l.termination_reason,
            test_status: l.test_status,
        }
    }
}

fn count_lines(s: &str) -> usize {
    s.lines().count()
}

/// JSON output for the `status` command.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSummary {
    pub slug: String,
    pub status: PlanStatus,
    pub branch_name: String,
    pub steps: StepCounts,
    /// Live-run snapshot: present when a `ralph run` is currently active for
    /// this project and its recorded plan matches (or is unbound and covers
    /// the project broadly). Absent when no live row exists, or the live row
    /// is for a different plan than the one being queried.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveRunDisplay>,
}

/// Serializable projection of a [`LiveRun`] for the `status` command.
///
/// Timestamps are kept as raw strings so the struct mirrors the on-disk row;
/// `phase_elapsed_secs` is a computed field populated at construction time
/// when `phase_started_at` parses as a chrono timestamp.
#[derive(Debug, Clone, Serialize)]
pub struct LiveRunDisplay {
    pub pid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_slug: Option<String>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_num: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase_elapsed_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_pid: Option<i64>,
}

impl LiveRunDisplay {
    /// Project a [`LiveRun`] into its display form, computing
    /// `phase_elapsed_secs = now() - phase_started_at`. Parse failures on the
    /// timestamp leave `phase_elapsed_secs` as `None` rather than erroring —
    /// the point is to surface best-effort observability, not to refuse
    /// output when the server clock wrote an unparseable string.
    pub fn from_live_run(lr: &LiveRun) -> Self {
        let phase_elapsed_secs = lr.phase_started_at.as_deref().and_then(|s| {
            s.parse::<DateTime<Utc>>()
                .ok()
                .map(|started| (Utc::now() - started).num_milliseconds() as f64 / 1000.0)
        });
        LiveRunDisplay {
            pid: lr.pid,
            plan_slug: lr.plan_slug.clone(),
            started_at: lr.started_at.clone(),
            step_id: lr.step_id.clone(),
            step_num: lr.step_num,
            attempt: lr.attempt,
            max_attempts: lr.max_attempts,
            phase: lr.phase,
            phase_started_at: lr.phase_started_at.clone(),
            phase_elapsed_secs,
            current_command: lr.current_command.clone(),
            child_pid: lr.child_pid,
        }
    }
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

/// JSON output for the `cancel` command.
#[derive(Debug, Clone, Serialize)]
pub struct CancelSummary {
    /// Whether cancel actually had a live run to signal. `false` means no
    /// active row was found — cancel was a no-op.
    pub cancelled: bool,
    /// Whether the graceful SIGTERM was bypassed (`--force`) or the target
    /// failed to release in time and was escalated to SIGKILL.
    pub forced: bool,
    /// Plan slug of the cancelled run, if the live row recorded one.
    pub plan_slug: Option<String>,
    /// 1-based step number in the plan, if the live row had progressed into a
    /// step.
    pub step_num: Option<i32>,
    /// Phase the runner was in when cancel fired.
    pub phase: Option<String>,
    /// Attempt number at the time of cancel.
    pub attempt: Option<i32>,
    /// Configured max attempts for the step.
    pub max_attempts: Option<i32>,
    /// Pid of the runner that was signalled.
    pub pid: Option<i64>,
    /// `true` when the target process was already dead (pid missing or start
    /// token mismatch); cancel only cleaned up bookkeeping in that case.
    pub already_dead: bool,
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

    // -- emit_ndjson --------------------------------------------------------

    #[test]
    fn test_emit_ndjson_serialization_error_propagates() {
        // A value whose Serialize impl always fails should produce an Err from
        // emit_ndjson_to — not silently swallow the error and emit a blank
        // line into the NDJSON stream.
        struct FailSerialize;
        impl Serialize for FailSerialize {
            fn serialize<S: serde::Serializer>(
                &self,
                _serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("forced failure"))
            }
        }

        let mut buf: Vec<u8> = Vec::new();
        let result = emit_ndjson_to(&mut buf, &FailSerialize);
        assert!(result.is_err(), "serialization error must propagate");
    }

    #[test]
    fn test_emit_ndjson_ok_writes_newline_terminated_json() {
        #[derive(Serialize)]
        struct Payload {
            x: i32,
        }
        let mut buf: Vec<u8> = Vec::new();
        emit_ndjson_to(&mut buf, &Payload { x: 42 }).unwrap();
        assert_eq!(buf, b"{\"x\":42}\n");
    }

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
    fn test_confirm_mixed_case_yes() {
        for variant in ["yEs", "YeS", "yES", "YES", "Yes"] {
            let mut input = Cursor::new(format!("{variant}\n").into_bytes());
            let mut output = Vec::new();
            assert!(
                confirm_with_reader("Delete?", &mut input, &mut output).unwrap(),
                "variant {variant} should be affirmative"
            );
        }
    }

    #[test]
    fn test_confirm_n() {
        let mut input = Cursor::new(b"n\n");
        let mut output = Vec::new();
        assert!(!confirm_with_reader("Delete?", &mut input, &mut output).unwrap());
    }

    #[test]
    fn test_confirm_no_variants() {
        for variant in ["no", "No", "NO", "nO", "nope", "n "] {
            let mut input = Cursor::new(format!("{variant}\n").into_bytes());
            let mut output = Vec::new();
            assert!(
                !confirm_with_reader("Delete?", &mut input, &mut output).unwrap(),
                "variant {variant:?} should be negative"
            );
        }
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
            prompt_prefix: None,
            prompt_suffix: None,
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
            change_policy: ChangePolicy::Required,
            tags: vec![],
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
            termination_reason: None,
            test_status: None,
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

    // -- split_lines_budget -------------------------------------------------

    #[test]
    fn test_split_lines_budget_both_fit() {
        // Neither stream exceeds half of the budget: return both unchanged.
        assert_eq!(split_lines_budget(3, 4, 50), (3, 4));
        assert_eq!(split_lines_budget(0, 0, 50), (0, 0));
    }

    #[test]
    fn test_split_lines_budget_one_small_one_large() {
        // Small stream keeps all of its lines; large stream gets the remainder.
        assert_eq!(split_lines_budget(3, 100, 50), (3, 47));
        assert_eq!(split_lines_budget(100, 3, 50), (47, 3));
    }

    #[test]
    fn test_split_lines_budget_both_large_even_split() {
        // Both streams exceed half: split evenly; odd extra goes to stdout.
        assert_eq!(split_lines_budget(100, 100, 50), (25, 25));
        assert_eq!(split_lines_budget(100, 100, 51), (26, 25));
    }

    #[test]
    fn test_split_lines_budget_total_never_exceeds_budget() {
        // Exhaustively confirm the contract: out_cap + err_cap <= budget.
        for out in [0usize, 1, 5, 24, 25, 26, 49, 50, 100] {
            for err in [0usize, 1, 5, 24, 25, 26, 49, 50, 100] {
                for budget in [0usize, 1, 2, 49, 50, 51] {
                    let (a, b) = split_lines_budget(out, err, budget);
                    assert!(
                        a + b <= budget,
                        "budget exceeded: out={out} err={err} budget={budget} got=({a},{b})"
                    );
                }
            }
        }
    }

    // -- LogEntrySummary::new truncation ------------------------------------

    #[test]
    fn test_log_entry_summary_truncated_respects_total_budget() {
        use crate::commands::LogOutputMode;

        let big = (1..=100)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let log = ExecutionLog {
            id: 1,
            step_id: "s1".into(),
            attempt: 1,
            started_at: Utc::now(),
            duration_secs: None,
            prompt_text: None,
            diff: None,
            test_results: vec![],
            rolled_back: false,
            committed: true,
            commit_hash: None,
            harness_stdout: Some(big.clone()),
            harness_stderr: Some(big),
            cost_usd: None,
            input_tokens: None,
            output_tokens: None,
            session_id: None,
            termination_reason: None,
            test_status: None,
        };
        let s = LogEntrySummary::new(&log, &LogOutputMode::Truncated(50));
        let out_lines = s.stdout.as_deref().map(|s| s.lines().count()).unwrap_or(0);
        let err_lines = s.stderr.as_deref().map(|s| s.lines().count()).unwrap_or(0);
        assert!(
            out_lines + err_lines <= 50,
            "expected total <= 50, got stdout={out_lines} stderr={err_lines}"
        );
        // With two equally large streams the split is 25/25.
        assert_eq!(out_lines, 25);
        assert_eq!(err_lines, 25);
    }

    // -- LiveRunDisplay / StatusSummary / termination-reason ---------------

    /// Build a LiveRun with a phase_started_at a few seconds in the past so
    /// from_live_run can compute a positive elapsed duration.
    fn sample_live_run() -> LiveRun {
        let started = Utc::now() - chrono::Duration::seconds(12);
        LiveRun {
            project: "/tmp/proj-roundtrip".into(),
            pid: 12345,
            pid_start_token: Some("tok".into()),
            plan_id: Some("plan-uuid".into()),
            plan_slug: Some("my-slug".into()),
            started_at: "2026-04-21T17:23:10.000Z".into(),
            step_id: Some("step-uuid".into()),
            step_num: Some(3),
            attempt: Some(2),
            max_attempts: Some(4),
            phase: Some(Phase::Tests),
            phase_started_at: Some(started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            current_command: Some("pnpm turbo test --filter=rne".into()),
            execution_log_id: Some(99),
            child_pid: Some(54321),
            child_start_token: Some("child-tok".into()),
            updated_at: None,
            source_branch: None,
            stash_sha: None,
        }
    }

    #[test]
    fn test_live_run_display_json_includes_phase_elapsed_secs() {
        let live = sample_live_run();
        let disp = LiveRunDisplay::from_live_run(&live);
        assert!(disp.phase_elapsed_secs.is_some());
        let elapsed = disp.phase_elapsed_secs.unwrap();
        assert!(
            (11.0..120.0).contains(&elapsed),
            "expected ~12s elapsed, got {elapsed}"
        );
        let json = serde_json::to_string(&disp).unwrap();
        assert!(json.contains("\"phase\":\"tests\""));
        assert!(json.contains("\"phase_elapsed_secs\""));
        assert!(json.contains("\"attempt\":2"));
        assert!(json.contains("\"max_attempts\":4"));
        assert!(json.contains("\"current_command\":\"pnpm turbo test --filter=rne\""));
        assert!(json.contains("\"pid\":12345"));
    }

    #[test]
    fn test_live_run_display_malformed_phase_started_at_yields_none() {
        let mut live = sample_live_run();
        live.phase_started_at = Some("not-a-timestamp".into());
        let disp = LiveRunDisplay::from_live_run(&live);
        assert!(disp.phase_elapsed_secs.is_none());
    }

    #[test]
    fn test_status_summary_omits_live_when_none() {
        let summary = StatusSummary {
            slug: "my-plan".into(),
            status: PlanStatus::InProgress,
            branch_name: "feat/x".into(),
            steps: StepCounts {
                total: 3,
                complete: 1,
                failed: 0,
                skipped: 0,
                pending: 2,
                in_progress: 0,
            },
            live: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(
            !json.contains("\"live\""),
            "live should be omitted when None, got {json}"
        );
    }

    #[test]
    fn test_status_summary_includes_live_when_populated() {
        let summary = StatusSummary {
            slug: "my-plan".into(),
            status: PlanStatus::InProgress,
            branch_name: "feat/x".into(),
            steps: StepCounts {
                total: 3,
                complete: 1,
                failed: 0,
                skipped: 0,
                pending: 2,
                in_progress: 1,
            },
            live: Some(LiveRunDisplay::from_live_run(&sample_live_run())),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"live\":{"));
        assert!(json.contains("\"phase\":\"tests\""));
    }

    #[test]
    fn test_log_entry_summary_includes_termination_reason_and_test_status() {
        let summary = LogEntrySummary {
            id: 1,
            step_id: "s1".into(),
            attempt: 2,
            started_at: Utc::now(),
            duration_secs: Some(5.0),
            test_results: vec![],
            rolled_back: false,
            committed: false,
            commit_hash: None,
            cost_usd: None,
            input_tokens: None,
            output_tokens: None,
            session_id: None,
            stdout: None,
            stderr: None,
            termination_reason: Some(TerminationReason::UserInterrupted),
            test_status: Some(TestStatus::Passed),
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"termination_reason\":\"user_interrupted\""));
        assert!(json.contains("\"test_status\":\"passed\""));
    }

    #[test]
    fn test_log_entry_summary_omits_termination_and_test_status_when_none() {
        let summary = LogEntrySummary {
            id: 1,
            step_id: "s1".into(),
            attempt: 1,
            started_at: Utc::now(),
            duration_secs: None,
            test_results: vec![],
            rolled_back: false,
            committed: false,
            commit_hash: None,
            cost_usd: None,
            input_tokens: None,
            output_tokens: None,
            session_id: None,
            stdout: None,
            stderr: None,
            termination_reason: None,
            test_status: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert!(!json.contains("\"termination_reason\""));
        assert!(!json.contains("\"test_status\""));
    }

    #[test]
    fn test_colored_termination_reason_color_off() {
        assert_eq!(
            colored_termination_reason(TerminationReason::UserInterrupted, false),
            "user_interrupted"
        );
        assert_eq!(
            colored_termination_reason(TerminationReason::Success, false),
            "success"
        );
    }

    #[test]
    fn test_colored_termination_reason_color_on() {
        let s = colored_termination_reason(TerminationReason::Success, true);
        assert!(s.contains('\x1b'));
        assert!(s.contains("success"));
        assert!(s.contains("\x1b[32m")); // green
        let s = colored_termination_reason(TerminationReason::UserInterrupted, true);
        assert!(s.contains("\x1b[31m")); // red
        let s = colored_termination_reason(TerminationReason::NoChanges, true);
        assert!(s.contains("\x1b[33m")); // yellow
        let s = colored_termination_reason(TerminationReason::Unknown, true);
        assert!(s.contains("\x1b[90m")); // gray
    }

    #[test]
    fn test_colored_test_status_color_off() {
        assert_eq!(colored_test_status(TestStatus::Passed, false), "passed");
        assert_eq!(colored_test_status(TestStatus::Failed, false), "failed");
    }

    #[test]
    fn test_colored_test_status_color_on() {
        let s = colored_test_status(TestStatus::Passed, true);
        assert!(s.contains("\x1b[32m"));
        let s = colored_test_status(TestStatus::Failed, true);
        assert!(s.contains("\x1b[31m"));
        let s = colored_test_status(TestStatus::NotConfigured, true);
        assert!(s.contains("\x1b[90m"));
    }

    // -- harness colors / labels -------------------------------------------

    #[test]
    fn test_harness_color_known() {
        use ratatui::style::Color;
        assert_eq!(harness_color("claude"), Some(Color::Rgb(0xcc, 0x8b, 0x89)));
        assert_eq!(harness_color("codex"), Some(Color::Rgb(0x7a, 0xa8, 0xc1)));
        assert_eq!(
            harness_color("opencode"),
            Some(Color::Rgb(0xf3, 0xb2, 0x6d))
        );
        assert_eq!(harness_color("copilot"), Some(Color::Rgb(0xac, 0x4d, 0xb6)));
    }

    #[test]
    fn test_harness_color_unknown_returns_none() {
        assert_eq!(harness_color("goose"), None);
        assert_eq!(harness_color("pi"), None);
        assert_eq!(harness_color(""), None);
        assert_eq!(harness_color("does-not-exist"), None);
    }

    #[test]
    fn test_parse_hex_color_valid() {
        assert_eq!(parse_hex_color("#cc8b89"), Ok((0xcc, 0x8b, 0x89)));
        assert_eq!(parse_hex_color("#FFFFFF"), Ok((0xff, 0xff, 0xff)));
        assert_eq!(parse_hex_color("#000000"), Ok((0, 0, 0)));
    }

    #[test]
    fn test_parse_hex_color_invalid() {
        assert!(parse_hex_color("cc8b89").is_err()); // missing #
        assert!(parse_hex_color("#cc8b8").is_err()); // too short
        assert!(parse_hex_color("#cc8b8901").is_err()); // too long
        assert!(parse_hex_color("#ggggggg").is_err()); // non-hex digits
    }

    #[test]
    fn test_format_harness_label_color_off() {
        assert_eq!(format_harness_label("claude", false), "claude");
        assert_eq!(format_harness_label("unknown", false), "unknown");
    }

    #[test]
    fn test_format_harness_label_color_on_known() {
        let out = format_harness_label("claude", true);
        assert!(out.contains("\x1b[1;38;2;204;139;137m"));
        assert!(out.contains("claude"));
        assert!(out.ends_with("\x1b[0m"));
    }

    #[test]
    fn test_format_harness_label_color_on_unknown_is_bold_no_color() {
        let out = format_harness_label("goose", true);
        assert!(out.contains("\x1b[1m"));
        assert!(out.contains("goose"));
        assert!(!out.contains("38;2;"));
    }

    #[test]
    fn test_format_harness_label_override_takes_precedence() {
        let out = format_harness_label_with_override("claude", Some("#010203"), true);
        assert!(out.contains("\x1b[1;38;2;1;2;3m"));
    }

    // -- format_now_in_tz / format_instant_in_tz ---------------------------

    #[test]
    fn test_format_now_in_tz_known_timezone() {
        // Fixed instant: 2026-04-22T18:32:07Z. In UTC this formats as the
        // same date and time with the "UTC" abbreviation.
        let utc: DateTime<Utc> = "2026-04-22T18:32:07Z".parse().unwrap();
        let s = format_instant_in_tz(utc, &chrono_tz::UTC);
        assert_eq!(s, "2026-04-22 18:32:07 UTC");
    }

    #[test]
    fn test_format_now_in_tz_smoke_live_call() {
        // Live call: just verify the string has the expected shape and the
        // timezone abbreviation is present.
        let s = format_now_in_tz(&chrono_tz::UTC);
        assert!(s.ends_with(" UTC"));
        // YYYY-MM-DD HH:MM:SS is 19 chars.
        assert!(s.len() >= 19 + 1 + 3);
    }
}
