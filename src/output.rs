// Output formatting — centralized helpers for display and JSON serialization.

use crate::plan::{
    ChangePolicy, ExecutionLog, Phase, Plan, PlanStatus, Step, StepStatus, TerminationReason,
    TestStatus,
};
use crate::run_lock::LiveRun;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, IsTerminal, Write};

// ---------------------------------------------------------------------------
// NDJSON run events
// ---------------------------------------------------------------------------

/// Which output stream a chunk came from.
///
/// Wraps the `stream` field of [`RunEvent::HarnessChunk`] and
/// [`RunEvent::TestChunk`] so consumers don't have to string-match on a
/// free-form value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkStream {
    Stdout,
    Stderr,
}

/// Events emitted as NDJSON (one JSON object per line) when `--json` is active
/// on `run` or `resume`. The TUI subscribes to this stream when it spawns its
/// own runner (see `tui::events`), so [`Deserialize`] is required alongside
/// the producer-side [`Serialize`] derive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    /// First event emitted by `ralph run` / `ralph resume` when `--json` is
    /// active: announces the run's start instant so subscribers (the TUI's
    /// in-process attach path, log shippers) can drive the elapsed-time
    /// display without polling `run_locks`. Anchors the elapsed-timer base
    /// case for the gap between run start and the first phase transition.
    RunStarted {
        plan_slug: String,
        started_at: DateTime<Utc>,
    },
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
    /// Line-buffered chunk of harness output, one emit per newline.
    /// `seq` is monotonic per run so consumers can reorder if needed.
    /// `text` is truncated past `Config.harness_chunk_max_bytes`.
    HarnessChunk {
        stream: ChunkStream,
        text: String,
        seq: u64,
    },
    /// Line-buffered chunk of deterministic-test output. Same shape as
    /// `HarnessChunk` but `test_index` indexes into `plan.deterministic_tests`.
    TestChunk {
        test_index: usize,
        stream: ChunkStream,
        text: String,
        seq: u64,
    },
    /// Emitted on every transition recorded into `run_locks.phase`. Lets the
    /// TUI redraw the phase indicator without polling. `phase_started_at`
    /// mirrors the `run_locks.phase_started_at` value written alongside the
    /// transition so subscribers can derive elapsed-since-phase-began without
    /// a separate DB poll.
    PhaseChanged {
        phase: Phase,
        phase_started_at: DateTime<Utc>,
    },
    /// Emitted when an in-flight attempt is cancelled via the TUI skip
    /// dialog's Esc/cancel path (step 18). Unlike `step_finished`, this is
    /// **not** terminal for the step: the runner rolled back the killed
    /// harness's work, wrote no `execution_logs` row, and is re-entering the
    /// retry loop at the *same* `attempt` number — so the cancelled attempt
    /// consumes no retry budget. Consumers should treat it as "the attempt
    /// was undone; expect another `prompt_prepared`/`phase_changed` for the
    /// same `step_id` at the same `attempt`". `attempt` is the 1-based number
    /// of the attempt that was cancelled.
    AttemptCancelled {
        step_id: String,
        step_num: usize,
        attempt: i32,
        at: DateTime<Utc>,
    },
    /// Emitted when a queued skip request targeted this step, but the
    /// attempt had already completed naturally by the time the executor got
    /// to the cleanup point. The request is cleared and ignored; the step
    /// continues through tests/commit according to the completed attempt.
    SkipRequestIgnored {
        step_id: String,
        step_num: usize,
        attempt: i32,
        reason: String,
        at: DateTime<Utc>,
    },
    /// Emitted the moment a read-only reviewer is spawned against a
    /// committed iteration (docs/dag-redesign.md §3.2/§9-inv-2). Lets the
    /// TUI show a "reviewing" badge without polling. The review runs
    /// concurrently with the next unrelated implementation; it is read-only
    /// w.r.t. the working tree (fixed `commit_sha`).
    ReviewStarted {
        step_id: String,
        step_num: usize,
        commit_sha: String,
        iteration: i32,
    },
    /// Emitted when a reviewer returns a verdict. `passed = true` ⇒
    /// `REVIEW PASS` (the step is `Complete`/`Passed`); `false` ⇒
    /// `REVIEW FAIL` (a corrective step is requested — see
    /// `corrective_step_requested`). The matching `Ralph-Review` commit
    /// trailer is annotated `passed`/`failed` alongside this event.
    ReviewFinished {
        step_id: String,
        step_num: usize,
        commit_sha: String,
        iteration: i32,
        passed: bool,
    },
    /// The reviewer-side half of the §9-inv-3 structured channel: a failed
    /// review *requests* (never performs) a corrective-step insertion. The
    /// orchestrator — the SOLE DAG writer — consumes the matching
    /// `corrective_step_requests` bridge row at a scheduler tick and performs
    /// the §10 insert + re-parent. A reviewer subprocess never writes step
    /// rows/edges; this event + the DB bridge row ARE the request.
    CorrectiveStepRequested {
        reviewed_step_id: String,
        reviewed_step_num: usize,
        commit_sha: String,
        iteration: i32,
        issues: i32,
    },
    /// Emitted when the orchestrator (sole writer) has inserted corrective
    /// step `A′` and re-parented every former dependent of `A` onto it
    /// (§10). `corrects_step_id` is the reviewed step `A`.
    CorrectiveStepInserted {
        corrective_step_id: String,
        corrective_short_id: String,
        corrects_step_id: String,
    },
    /// Emitted when the review→correction→review chain hits the per-plan
    /// `max_review_corrections` cap (§10 item 4 / §14.5): instead of
    /// spawning another correction, the orchestrator raises a
    /// `kind=blocker` interruption ("review loop — needs human") on the
    /// offending step and stops the chain.
    ReviewLoopEscalated {
        step_id: String,
        step_num: usize,
        chain_len: usize,
        cap: usize,
    },
    /// Emitted when the runner exits cleanly because the operator set
    /// `plans.pause_requested` (TUI `[P]` keybinding or `ralph pause`).
    /// Distinct from `plan_complete`/`summary` so the TUI can surface the
    /// "Paused. Use `ralph resume` to continue." toast and so machine
    /// consumers can distinguish a deliberate pause from completion.
    PausedByUser { plan_slug: String },
    /// Emitted on every new interruption (question / blocker) write, no
    /// matter who triggered it: harness-raised (`ralph question ask` /
    /// `ralph block`), executor-raised auto-blocker on retry exhaustion,
    /// or TUI/CLI-injected. `auto_raised` discriminates the executor's
    /// retry-exhausted auto-blocker — Phase E reversed the pre-existing
    /// "no NDJSON for interruptions" stance precisely because the auto-
    /// raised one is the case a TUI / log shipper most wants to react to
    /// without polling. Consumers that don't want auto-raised noise can
    /// gate on `auto_raised == false`.
    InterruptionRaised {
        interruption_id: String,
        step_id: String,
        plan_slug: String,
        kind: String,
        // `default` so a consumer built before these fields existed (or a
        // future stream that drops them) still deserializes the event rather
        // than having `consume_lines` silently discard the whole line. The
        // emitter always writes them; the defaults only ever apply on the
        // parse side under version skew. `false`/`0` are the safe readings:
        // "not auto-raised, attempt unknown".
        #[serde(default)]
        auto_raised: bool,
        #[serde(default)]
        attempt: i32,
        raised_at: DateTime<Utc>,
    },
    /// Emitted on every interruption resolution, no matter who closed it
    /// (CLI `ralph interruption resolve`, TUI inbox, or a programmatic
    /// resolution from the orchestrator's auto-resolve paths). Pairs with
    /// `interruption_raised` by `interruption_id`.
    InterruptionResolved {
        interruption_id: String,
        step_id: String,
        plan_slug: String,
        resolution: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        comment: Option<String>,
        resolved_at: DateTime<Utc>,
    },
    /// Final event for `ralph run`, replacing the role of `plan_complete` for
    /// human-readable summary consumers. `plan_complete` is **kept** for one
    /// release as a compat shim (still emitted alongside `summary`) so
    /// meta-harnesses pinned to it don't break.
    Summary {
        plan_status: PlanStatus,
        steps_complete: usize,
        steps_total: usize,
        duration_secs: f64,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        cost_usd: Option<f64>,
        started_at: DateTime<Utc>,
        ended_at: DateTime<Utc>,
    },
}

/// Compact reference to a step for NDJSON payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Best-effort emit of an [`RunEvent::InterruptionRaised`]. Looks the plan
/// slug up by `step_id` so callers (storage, the executor, the harness
/// CLI handlers) don't have to plumb it through. Silently swallows errors
/// — these events are advisory; failing to look up a plan slug (e.g.,
/// orphaned step row, missing FK) must NOT break the underlying insert.
///
/// Gated on caller-passed `json_output` so the function is a no-op outside
/// NDJSON mode (the existing pattern used by `runner.rs` for all other
/// `RunEvent` variants — emitting unconditionally would corrupt the
/// human-readable stdout).
pub fn emit_interruption_raised(
    conn: &rusqlite::Connection,
    json_output: bool,
    interruption_id: &str,
    step_id: &str,
    kind: &str,
    auto_raised: bool,
    attempt: i32,
) {
    if !json_output {
        return;
    }
    let plan_slug = match plan_slug_for_step(conn, step_id) {
        Some(s) => s,
        None => return,
    };
    let _ = emit_ndjson(&RunEvent::InterruptionRaised {
        interruption_id: interruption_id.to_string(),
        step_id: step_id.to_string(),
        plan_slug,
        kind: kind.to_string(),
        auto_raised,
        attempt,
        raised_at: chrono::Utc::now(),
    });
}

/// Best-effort emit of an [`RunEvent::InterruptionResolved`]. Same shape /
/// rationale as [`emit_interruption_raised`].
pub fn emit_interruption_resolved(
    conn: &rusqlite::Connection,
    json_output: bool,
    interruption_id: &str,
    step_id: &str,
    resolution: &str,
    comment: Option<&str>,
) {
    if !json_output {
        return;
    }
    let plan_slug = match plan_slug_for_step(conn, step_id) {
        Some(s) => s,
        None => return,
    };
    let _ = emit_ndjson(&RunEvent::InterruptionResolved {
        interruption_id: interruption_id.to_string(),
        step_id: step_id.to_string(),
        plan_slug,
        resolution: resolution.to_string(),
        comment: comment.map(|s| s.to_string()),
        resolved_at: chrono::Utc::now(),
    });
}

fn plan_slug_for_step(conn: &rusqlite::Connection, step_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT p.slug FROM plans p JOIN steps s ON s.plan_id = p.id WHERE s.id = ?1",
        rusqlite::params![step_id],
        |r| r.get::<_, String>(0),
    )
    .ok()
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

    /// Emit a human-readable status line unless output is quiet or machine
    /// formatted. Intended for success/progress banners, not warnings/errors.
    pub fn status(&self, message: impl std::fmt::Display) {
        if !self.quiet && self.format == OutputFormat::Plain {
            eprintln!("{message}");
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
        // Blocked is the §3.3 derived overlay (open interruption). Closest
        // ANSI to the §12.5 orange is yellow; a distinct `?` glyph reads as
        // "needs a human" alongside the plan-level derived status.
        (StepStatus::Blocked, true) => "\x1b[33m?\x1b[0m",
        (StepStatus::Pending, false) => "○",
        (StepStatus::InProgress, false) => "▶",
        (StepStatus::Complete, false) => "✔",
        (StepStatus::Failed, false) => "✘",
        (StepStatus::Skipped, false) => "⊘",
        (StepStatus::Aborted, false) => "⊘",
        (StepStatus::Blocked, false) => "?",
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
        StepStatus::Blocked => "\x1b[33m",
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
        (PlanStatus::Interrupted, true) => "\x1b[33m?\x1b[0m",
        (PlanStatus::Planning, false) => "◯",
        (PlanStatus::Ready, false) => "◉",
        (PlanStatus::InProgress, false) => "▶",
        (PlanStatus::Complete, false) => "✔",
        (PlanStatus::Failed, false) => "✘",
        (PlanStatus::Aborted, false) => "⊘",
        (PlanStatus::Archived, false) => "▪",
        (PlanStatus::Interrupted, false) => "?",
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
        TerminationReason::NoChanges
        | TerminationReason::PausedForQuestion
        | TerminationReason::PausedByUser
        | TerminationReason::UserSkipped => "\x1b[33m",
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
        PlanStatus::Interrupted => "\x1b[33m",
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

/// Format a duration in seconds as a collapsing `Hh Mm Ss` string.
///
/// The largest non-zero unit is the leftmost shown; smaller units are always
/// present once a larger one appears (so minutes/seconds aren't dropped):
/// - `< 60s` → `Ns` (e.g. `45s`)
/// - `< 1h`  → `Mm Ss` (e.g. `2m 3s`, `1m 0s`)
/// - `≥ 1h`  → `Hh Mm Ss` (e.g. `1h 6m 40s`)
///
/// Input is `f64` seconds (matching [`ExecutionLog::duration_secs`]). The
/// value is truncated toward zero to whole seconds; negative inputs clamp
/// to `0`.
pub fn format_duration_secs(secs: f64) -> String {
    let total = if secs.is_finite() && secs > 0.0 {
        secs.floor() as u64
    } else {
        0
    };
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
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
    /// The plan-unique 8-char DAG handle. Always serialized (like
    /// `change_policy`/`tags`, matching `ExportedStep`) so a JSON consumer
    /// authoring the DAG — e.g. parsing `step add --import-json --json`
    /// output to wire `depends_on` — can read back the (possibly pinned)
    /// id rather than getting no handle at all.
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
            short_id: s.short_id.clone(),
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

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LogRecordSummary {
    ExecutionLog {
        #[serde(flatten)]
        log: LogEntrySummary,
    },
    SkippedStep {
        step_id: String,
        step_num: usize,
        title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    SkipWipCommit {
        step_num: usize,
        short_sha: String,
    },
    IterationCommits {
        short_id: String,
        title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        review_status: Option<String>,
        commits: Vec<IterationCommitSummary>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct IterationCommitSummary {
    pub iteration: i32,
    pub short_sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_verdict: Option<String>,
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
    /// `plans.pause_requested`. Only emitted (in JSON) and only printed (in
    /// plain text) when set, so a normal status report stays compact.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub pause_requested: bool,
    /// Count of open interruptions (questions or blockers) for steps in this
    /// plan. Always present (0 when none). Wired via
    /// `storage::list_open_interruptions_for_plan` (or equivalent COUNT) so
    /// JSON consumers can detect the derived blocked/interrupted state
    /// without an extra query. When >0 the plan is effectively blocked for
    /// progress.
    pub open_interruptions: usize,
}

/// Serializable projection of a [`LiveRun`] for the `status` command.
///
/// Timestamps are kept as raw strings so the struct mirrors the on-disk row;
/// `phase_elapsed_secs` and `state` are computed fields populated at
/// construction time (see [`LiveRunDisplay::from_live_run`]).
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
    /// Derived coarse-grained state string for the live run (and surfaced
    /// in `status --json`). Values are drawn from the agent-suggested set
    /// (harness | committing | testing | pre_test_hook | post_test_hook |
    /// rollback | paused | blocked | crashed | idle) with closest practical
    /// mappings for the full Phase space (pre_step_hook / post_step_hook
    /// are emitted as-is). Computed in `from_live_run` from Phase + signals
    /// (child_pid, updated_at staleness, elapsed) and plan context
    /// (open_interruptions count, pause_requested).
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_pid: Option<i64>,
}

impl LiveRunDisplay {
    /// Project a [`LiveRun`] into its display form, computing
    /// `phase_elapsed_secs = now() - phase_started_at` and a derived `state`
    /// string. Parse failures on timestamps leave computed fields as `None`
    /// rather than erroring — the point is to surface best-effort
    /// observability.
    ///
    /// `open_interruptions` and `pause_requested` come from the plan context
    /// at call site (in `build_status_summary`) so that `state` can prefer
    /// "blocked" (when count > 0) or "paused".
    pub fn from_live_run(lr: &LiveRun, open_interruptions: usize, pause_requested: bool) -> Self {
        let phase_elapsed_secs = lr.phase_started_at.as_deref().and_then(|s| {
            s.parse::<DateTime<Utc>>()
                .ok()
                .map(|started| (Utc::now() - started).num_milliseconds() as f64 / 1000.0)
        });

        // Derive `state` with the following precedence (per full agent-suggested
        // enum decision):
        //   crashed (heuristic) > blocked (open_interruptions > 0) > paused (pause_requested)
        //   > phase-derived value (remapping "commit"->"committing", "tests"->"testing";
        //     PreStepHook/PostStepHook surface as "pre_step_hook"/"post_step_hook").
        //
        // Crashed heuristic (conservative, documented). BOTH arms require
        // `child_pid` to be None: `updated_at` is only bumped on phase
        // transitions (no intra-phase heartbeat), so a legitimately
        // long-running harness call or slow test suite leaves it stale while
        // a live child is still recorded. A recorded child therefore means
        // "in progress, not crashed" regardless of staleness.
        // - child_pid is None AND `updated_at` parses and its age is > 5 minutes, OR
        // - child_pid is None AND phase is Harness or Commit AND
        //   phase_elapsed_secs > 300s (catches a runner that died mid-phase
        //   without a final updated_at bump).
        let crashed = {
            let mut is_crashed = false;
            if lr.child_pid.is_none() {
                let stale_phase_without_child = matches!(
                    lr.phase,
                    Some(
                        Phase::PreStepHook
                            | Phase::Commit
                            | Phase::Rollback
                            | Phase::PostStepHook
                            | Phase::Idle
                    )
                );
                if stale_phase_without_child
                    && let Some(ref ua) = lr.updated_at
                    && let Ok(dt) = ua.parse::<DateTime<Utc>>()
                    && (Utc::now() - dt).num_minutes() > 5
                {
                    is_crashed = true;
                }
                if !is_crashed
                    && matches!(lr.phase, Some(Phase::Harness) | Some(Phase::Commit))
                    && phase_elapsed_secs.is_some_and(|e| e > 300.0)
                {
                    is_crashed = true;
                }
            }
            is_crashed
        };

        let state = if crashed {
            "crashed".to_string()
        } else if open_interruptions > 0 {
            "blocked".to_string()
        } else if pause_requested {
            "paused".to_string()
        } else {
            match lr.phase {
                Some(Phase::Commit) => "committing".to_string(),
                Some(Phase::Tests) => "testing".to_string(),
                Some(p) => p.as_str().to_string(),
                None => "idle".to_string(),
            }
        };

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
            state,
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

/// JSON output for the `step dependency list` command. Step analogue of
/// [`DependencyListSummary`]; identifiers are step short ids.
#[derive(Debug, Clone, Serialize)]
pub struct StepDependencyListSummary {
    pub short_id: String,
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

    // -- format_duration_secs ----------------------------------------------

    #[test]
    fn test_format_duration_secs_boundaries() {
        assert_eq!(format_duration_secs(0.0), "0s");
        assert_eq!(format_duration_secs(0.5), "0s"); // sub-second truncates down
        assert_eq!(format_duration_secs(1.0), "1s");
        assert_eq!(format_duration_secs(59.0), "59s");
        assert_eq!(format_duration_secs(59.9), "59s"); // still < 60 after trunc
        assert_eq!(format_duration_secs(60.0), "1m 0s");
        assert_eq!(format_duration_secs(61.0), "1m 1s");
        assert_eq!(format_duration_secs(3599.0), "59m 59s");
        assert_eq!(format_duration_secs(3600.0), "1h 0m 0s");
        assert_eq!(format_duration_secs(3661.0), "1h 1m 1s");
        assert_eq!(format_duration_secs(7322.0), "2h 2m 2s");
    }

    #[test]
    fn test_format_duration_secs_negative_clamps_to_zero() {
        assert_eq!(format_duration_secs(-1.0), "0s");
        assert_eq!(format_duration_secs(-3661.0), "0s");
    }

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
            short_id: "abcd1234".into(),
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
        assert!(json.contains("\"short_id\":\"abcd1234\""));
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
            cycle_index: 0,
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
            parent_tui_pid: None,
        }
    }

    #[test]
    fn test_live_run_display_json_includes_phase_elapsed_secs() {
        let live = sample_live_run();
        let disp = LiveRunDisplay::from_live_run(&live, 0, false);
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
        let disp = LiveRunDisplay::from_live_run(&live, 0, false);
        assert!(disp.phase_elapsed_secs.is_none());
    }

    #[test]
    fn test_live_run_display_does_not_mark_childless_tests_stale_as_crashed() {
        let mut live = sample_live_run();
        live.child_pid = None;
        live.phase = Some(Phase::Tests);
        live.updated_at = Some(
            (Utc::now() - chrono::Duration::minutes(10))
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        );

        let disp = LiveRunDisplay::from_live_run(&live, 0, false);
        assert_eq!(disp.state, "testing");
    }

    #[test]
    fn test_live_run_display_marks_stale_internal_phase_without_child_as_crashed() {
        let mut live = sample_live_run();
        live.child_pid = None;
        live.phase = Some(Phase::Commit);
        live.updated_at = Some(
            (Utc::now() - chrono::Duration::minutes(10))
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        );

        let disp = LiveRunDisplay::from_live_run(&live, 0, false);
        assert_eq!(disp.state, "crashed");
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
            pause_requested: false,
            open_interruptions: 0,
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
            live: Some(LiveRunDisplay::from_live_run(&sample_live_run(), 0, false)),
            pause_requested: false,
            open_interruptions: 0,
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

    // -- RunEvent JSON shapes (TUI-plan §13.1 additions) --------------------

    #[test]
    fn test_harness_chunk_event_json_shape() {
        let evt = RunEvent::HarnessChunk {
            stream: ChunkStream::Stdout,
            text: "hello\n".into(),
            seq: 7,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "harness_chunk");
        assert_eq!(val["stream"], "stdout");
        assert_eq!(val["text"], "hello\n");
        assert_eq!(val["seq"], 7);
    }

    #[test]
    fn test_harness_chunk_stderr_serializes_snake_case() {
        let evt = RunEvent::HarnessChunk {
            stream: ChunkStream::Stderr,
            text: "boom".into(),
            seq: 0,
        };
        let json = serde_json::to_string(&evt).unwrap();
        assert!(json.contains("\"stream\":\"stderr\""));
    }

    #[test]
    fn test_test_chunk_event_json_shape() {
        let evt = RunEvent::TestChunk {
            test_index: 2,
            stream: ChunkStream::Stderr,
            text: "FAIL\n".into(),
            seq: 42,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "test_chunk");
        assert_eq!(val["test_index"], 2);
        assert_eq!(val["stream"], "stderr");
        assert_eq!(val["text"], "FAIL\n");
        assert_eq!(val["seq"], 42);
    }

    #[test]
    fn test_phase_changed_event_json_shape() {
        let phase_started_at: DateTime<Utc> = "2026-04-22T18:00:05Z".parse().unwrap();
        let evt = RunEvent::PhaseChanged {
            phase: Phase::Tests,
            phase_started_at,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "phase_changed");
        assert_eq!(val["phase"], "tests");
        assert_eq!(val["phase_started_at"], "2026-04-22T18:00:05Z");
    }

    #[test]
    fn test_phase_changed_serializes_each_phase_snake_case() {
        let phase_started_at: DateTime<Utc> = "2026-04-22T18:00:00Z".parse().unwrap();
        for (phase, expected) in [
            (Phase::Idle, "idle"),
            (Phase::PreStepHook, "pre_step_hook"),
            (Phase::Harness, "harness"),
            (Phase::PreTestHook, "pre_test_hook"),
            (Phase::Tests, "tests"),
            (Phase::PostTestHook, "post_test_hook"),
            (Phase::Commit, "commit"),
            (Phase::Rollback, "rollback"),
            (Phase::PostStepHook, "post_step_hook"),
        ] {
            let evt = RunEvent::PhaseChanged {
                phase,
                phase_started_at,
            };
            let val: serde_json::Value =
                serde_json::from_str(&serde_json::to_string(&evt).unwrap()).unwrap();
            assert_eq!(val["phase"], expected);
        }
    }

    #[test]
    fn test_run_started_event_json_shape() {
        let started_at: DateTime<Utc> = "2026-04-22T18:00:00Z".parse().unwrap();
        let evt = RunEvent::RunStarted {
            plan_slug: "tui-v1".into(),
            started_at,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "run_started");
        assert_eq!(val["plan_slug"], "tui-v1");
        assert_eq!(val["started_at"], "2026-04-22T18:00:00Z");
    }

    #[test]
    fn test_summary_event_json_shape_with_cost() {
        let started: DateTime<Utc> = "2026-04-22T18:00:00Z".parse().unwrap();
        let ended: DateTime<Utc> = "2026-04-22T18:32:07Z".parse().unwrap();
        let evt = RunEvent::Summary {
            plan_status: PlanStatus::Complete,
            steps_complete: 3,
            steps_total: 3,
            duration_secs: 1927.0,
            cost_usd: Some(0.42),
            started_at: started,
            ended_at: ended,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "summary");
        assert_eq!(val["plan_status"], "complete");
        assert_eq!(val["steps_complete"], 3);
        assert_eq!(val["steps_total"], 3);
        assert_eq!(val["duration_secs"], 1927.0);
        assert_eq!(val["cost_usd"], 0.42);
        assert_eq!(val["started_at"], "2026-04-22T18:00:00Z");
        assert_eq!(val["ended_at"], "2026-04-22T18:32:07Z");
    }

    #[test]
    fn test_summary_event_omits_cost_when_none() {
        let evt = RunEvent::Summary {
            plan_status: PlanStatus::Failed,
            steps_complete: 1,
            steps_total: 4,
            duration_secs: 12.5,
            cost_usd: None,
            started_at: Utc::now(),
            ended_at: Utc::now(),
        };
        let json = serde_json::to_string(&evt).unwrap();
        assert!(
            !json.contains("\"cost_usd\""),
            "cost_usd must be omitted when None, got {json}"
        );
        assert!(json.contains("\"event\":\"summary\""));
    }

    #[test]
    fn test_attempt_cancelled_event_json_shape() {
        // STEP 18: schema documented in docs/ndjson-events.md. Field
        // names/casing must match the sibling lifecycle events
        // (`step_id`, `step_num`, `attempt`, snake_case `at` timestamp).
        let at: DateTime<Utc> = "2026-05-16T09:30:00Z".parse().unwrap();
        let evt = RunEvent::AttemptCancelled {
            step_id: "11111111-2222-3333-4444-555555555555".to_string(),
            step_num: 3,
            attempt: 2,
            at,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "attempt_cancelled");
        assert_eq!(val["step_id"], "11111111-2222-3333-4444-555555555555");
        assert_eq!(val["step_num"], 3);
        assert_eq!(val["attempt"], 2);
        assert_eq!(val["at"], "2026-05-16T09:30:00Z");
    }

    #[test]
    fn test_attempt_cancelled_roundtrips_through_deserialize() {
        // The TUI subscriber path requires Deserialize alongside Serialize.
        let at: DateTime<Utc> = "2026-05-16T10:00:00Z".parse().unwrap();
        let evt = RunEvent::AttemptCancelled {
            step_id: "abc".to_string(),
            step_num: 1,
            attempt: 1,
            at,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let back: RunEvent = serde_json::from_str(&json).unwrap();
        match back {
            RunEvent::AttemptCancelled {
                step_id,
                step_num,
                attempt,
                at: at_back,
            } => {
                assert_eq!(step_id, "abc");
                assert_eq!(step_num, 1);
                assert_eq!(attempt, 1);
                assert_eq!(at_back, at);
            }
            other => panic!("expected AttemptCancelled, got {other:?}"),
        }
    }

    #[test]
    fn test_skip_request_ignored_event_json_shape() {
        let at: DateTime<Utc> = "2026-05-16T10:30:00Z".parse().unwrap();
        let evt = RunEvent::SkipRequestIgnored {
            step_id: "abc".to_string(),
            step_num: 2,
            attempt: 1,
            reason: "attempt already completed".to_string(),
            at,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(val["event"], "skip_request_ignored");
        assert_eq!(val["step_id"], "abc");
        assert_eq!(val["step_num"], 2);
        assert_eq!(val["attempt"], 1);
        assert_eq!(val["reason"], "attempt already completed");
        assert_eq!(val["at"], "2026-05-16T10:30:00Z");

        let back: RunEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, RunEvent::SkipRequestIgnored { .. }));
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
