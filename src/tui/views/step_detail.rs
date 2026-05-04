// Step detail view (TUI-plan.md §8 + §18 Q5)
//
// Skeleton for the per-step pane stack reached via `enter` from plan-detail.
// This module owns the structural state — pane focus, zen-mode toggle, and the
// auto-zen threshold logic — but does not yet render pane bodies; subsequent
// steps fill in read-only renders, the appended-prompt navigator, and editor
// handoffs. For now the panes draw as bordered placeholders so the layout is
// observable while the rest of the v1 plan lands.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use rusqlite::Connection;

use crate::config::Config;
use crate::plan::{ChangePolicy, ExecutionLog, Plan, Step, StepStatus};
use crate::prompt::DEFAULT_CONTEXT_PREPEND;
use crate::storage::{self, ProjectSettings};
use crate::tui::chrome::{self, Chrome};
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};

/// Sentinel rendered in dim style when a pane's source-of-truth value is
/// `None` or empty. Distinguishes "no value configured" from "configured to
/// the empty string" — the latter renders as a single literal blank line.
const NONE_PLACEHOLDER: &str = "(none)";

/// Sentinel rendered for an empty bottom-row cell (no harness/model/agent
/// resolved). Bottom-row cells are single-line so we use an em-dash rather
/// than the wordier `(none)`.
const EMPTY_CELL: &str = "—";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Below this terminal width the TUI forces zen mode and disables `z`. The
/// wide-sidebar layout sketched in §8 needs ~25 cols of step list plus eight
/// stacked panes' worth of room on the right; under 100 cols the panes get
/// uselessly squashed.
pub const AUTO_ZEN_WIDTH_THRESHOLD: u16 = 100;

/// Toast text the first time the terminal width drops below the threshold.
pub const AUTO_ZEN_TOAST: &str = "Terminal too narrow — zen mode forced.";

/// Width (cols) of the full step-list sidebar in non-zen mode. Mirrors the
/// plan-detail layout so the sidebar is visually identical when the user
/// arrives in step detail (TUI-plan.md §18 Q5).
pub const SIDEBAR_FULL_WIDTH: u16 = 25;

/// Width (cols) of the thin gutter shown in zen mode: enough room for a
/// 1-col status glyph, a space, and a 1- or 2-digit step number.
pub const SIDEBAR_ZEN_WIDTH: u16 = 4;

/// Toast text shown when `c` is pressed but neither `$EDITOR` nor `$VISUAL`
/// is set — the editor handoff returns `Ok(None)` per TUI-plan §8 + §14.
/// Pushed as an error-styled toast so the red color signals "this didn't
/// work" rather than the green "Saved." confirmation.
pub const NO_EDITOR_TOAST: &str =
    "No $EDITOR set — set one in your shell to edit prompts in-place.";

/// Toast text shown after a successful editor round-trip that produced a
/// different value from the initial buffer.
pub const SAVED_TOAST: &str = "Saved.";

/// Toast text shown after an editor round-trip that produced the same value
/// as the initial buffer (or that the user didn't modify).
pub const NO_CHANGES_TOAST: &str = "No changes.";

/// Prefix used when toasting a parse failure from the Step-prompt or Tests
/// pane editor handoff. The full toast appends the parser's error message so
/// the user can fix the structural problem and re-edit.
pub const PARSE_ERROR_TOAST_PREFIX: &str = "Edit not saved: ";

/// Header line used by the Universal-, Project-, and Plan-prompt panes'
/// two-section editor format. The body that follows up to the next header
/// (or EOF) is the `prompt_prefix` value.
const PREFIX_HEADER: &str = "## Prefix";

/// Header line for the Suffix section — paired with [`PREFIX_HEADER`] above.
const SUFFIX_HEADER: &str = "## Suffix";

/// Top-level header introducing the title in the Step-prompt editor format.
/// Marks a single-line section: the first non-blank line after this header
/// is the title and any further content before the next header is rejected.
const STEP_TITLE_HEADER: &str = "# Title";

/// Header for the Step-prompt description section (multi-line body).
const STEP_DESCRIPTION_HEADER: &str = "## Description";

/// Header for the Step-prompt acceptance-criteria section (bulleted list).
const STEP_CRITERIA_HEADER: &str = "## Acceptance criteria";

// ---------------------------------------------------------------------------
// Pane enum
// ---------------------------------------------------------------------------

/// Stacked panes that make up the step detail body, ordered top-to-bottom per
/// the §8 sketch. `j`/`k` (or `↓`/`↑`) move focus between adjacent panes,
/// wrapping at the ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    UniversalPrompt,
    ProjectPrompt,
    PlanContextPrepend,
    PlanPrompt,
    StepPrompt,
    Appended,
    Tests,
    BottomRow,
}

impl Pane {
    /// Display order — index into the pane stack from top to bottom. Drives
    /// the wrapping nav arithmetic below.
    pub const ORDER: [Pane; 8] = [
        Pane::UniversalPrompt,
        Pane::ProjectPrompt,
        Pane::PlanContextPrepend,
        Pane::PlanPrompt,
        Pane::StepPrompt,
        Pane::Appended,
        Pane::Tests,
        Pane::BottomRow,
    ];

    /// Position in [`Self::ORDER`].
    fn index(self) -> usize {
        Self::ORDER.iter().position(|p| *p == self).expect("pane in ORDER")
    }

    /// Title shown on the pane's bordered block. Kept here so renderers and
    /// tests share a single source of truth for the heading text.
    pub fn title(self) -> &'static str {
        match self {
            Pane::UniversalPrompt => "Universal prompt",
            Pane::ProjectPrompt => "Project prompt",
            Pane::PlanContextPrepend => "Plan context prepend",
            Pane::PlanPrompt => "Plan prompt",
            Pane::StepPrompt => "Step prompt",
            Pane::Appended => "Appended",
            Pane::Tests => "Tests",
            Pane::BottomRow => "Harness │ Model │ Agent │ Change policy",
        }
    }
}

// ---------------------------------------------------------------------------
// Edit handoff (TUI-plan.md §8 "Editing — `c`" + §18 Q3)
// ---------------------------------------------------------------------------

/// Outcome of a `c` editor handoff on one of the editable text panes.
/// Drives which toast the dispatcher pushes after the handoff returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditOutcome {
    /// `$EDITOR`/`$VISUAL` not set, OR the editor exited non-zero — nothing
    /// was persisted. Maps to the [`NO_EDITOR_TOAST`] message in red.
    NoEditor,
    /// Editor exited zero with a value different from the initial buffer.
    /// The new value has already been written back to the source of truth.
    Saved,
    /// Editor exited zero but the value matches the initial buffer (no
    /// edits to record). Nothing was written back.
    NoChanges,
    /// Editor exited zero but the saved buffer failed structural validation
    /// (e.g. missing section header in the Step-prompt format). Nothing was
    /// written; the dispatcher toasts a red error and the user is expected
    /// to retry. The string is the parser's diagnostic — short enough to
    /// fit the toast bar.
    ParseError(String),
}

/// Format a prefix/suffix pair into the two-section markdown blob shown to
/// `$EDITOR`. Section headers are always emitted so the editor sees the
/// structure even when one (or both) values are unset.
///
/// Round-trips through [`parse_wrap_pane`] when the user makes no edits —
/// trailing newlines on each value are normalized to one to avoid spurious
/// "Saved" outcomes from editors that auto-append a final newline.
pub(crate) fn format_wrap_pane(prefix: Option<&str>, suffix: Option<&str>) -> String {
    let mut out = String::new();
    out.push_str(PREFIX_HEADER);
    out.push('\n');
    if let Some(s) = prefix {
        out.push_str(s);
        if !s.ends_with('\n') {
            out.push('\n');
        }
    }
    out.push('\n');
    out.push_str(SUFFIX_HEADER);
    out.push('\n');
    if let Some(s) = suffix {
        out.push_str(s);
        if !s.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// Parse a two-section markdown blob written by `$EDITOR` back into its
/// prefix/suffix pair. Whitespace-only sections become `None`; section
/// content is trimmed of leading/trailing whitespace so editor-added blank
/// lines don't drift the value across round-trips.
///
/// Tolerant of missing headers: text before the first header is dropped, and
/// a missing section just yields `None` for that side.
pub(crate) fn parse_wrap_pane(text: &str) -> (Option<String>, Option<String>) {
    enum Section {
        None,
        Prefix,
        Suffix,
    }
    let mut section = Section::None;
    let mut prefix = String::new();
    let mut suffix = String::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == PREFIX_HEADER {
            section = Section::Prefix;
        } else if trimmed == SUFFIX_HEADER {
            section = Section::Suffix;
        } else {
            match section {
                Section::None => {}
                Section::Prefix => {
                    if !prefix.is_empty() {
                        prefix.push('\n');
                    }
                    prefix.push_str(line);
                }
                Section::Suffix => {
                    if !suffix.is_empty() {
                        suffix.push('\n');
                    }
                    suffix.push_str(line);
                }
            }
        }
    }

    (trim_to_option(&prefix), trim_to_option(&suffix))
}

/// Trim and lift to `Option<String>` — empty/whitespace-only becomes `None`,
/// otherwise returns `Some(trimmed)`.
fn trim_to_option(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Parsed step-prompt sections returned by [`parse_step_pane`]. Re-assembled
/// by `edit_step_prompt_pane` into a `storage::update_step_fields_ext` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepPaneParts {
    pub title: String,
    pub description: String,
    pub acceptance_criteria: Vec<String>,
}

/// Render the Step-prompt pane's three-section editor format. Headers are
/// always emitted so the user sees the structure even when one section is
/// empty. Acceptance criteria render as `- <item>` bullets so the round-trip
/// parser has an unambiguous list shape to detect.
pub(crate) fn format_step_pane(
    title: &str,
    description: &str,
    acceptance_criteria: &[String],
) -> String {
    let mut out = String::new();
    out.push_str(STEP_TITLE_HEADER);
    out.push('\n');
    out.push_str(title.trim());
    out.push('\n');
    out.push('\n');
    out.push_str(STEP_DESCRIPTION_HEADER);
    out.push('\n');
    let desc = description.trim_end_matches('\n');
    if !desc.is_empty() {
        out.push_str(desc);
        out.push('\n');
    }
    out.push('\n');
    out.push_str(STEP_CRITERIA_HEADER);
    out.push('\n');
    for c in acceptance_criteria {
        out.push_str("- ");
        out.push_str(c.trim());
        out.push('\n');
    }
    out
}

/// Parse the Step-prompt three-section blob written by `$EDITOR` back into
/// its components. Returns an error describing the parse problem when the
/// blob is malformed — the caller toasts the error and leaves the source of
/// truth untouched.
///
/// Validation rules:
/// - The `# Title` section must contain exactly one non-blank line.
/// - All three headers (Title, Description, Acceptance criteria) must
///   appear at least once. Missing headers yield an error so a user who
///   accidentally deletes a section while editing is told what's wrong
///   rather than silently overwriting with empty data.
/// - Bullet lines under "Acceptance criteria" begin with `-` or `*` followed
///   by whitespace; non-bullet, non-blank lines are rejected.
pub(crate) fn parse_step_pane(text: &str) -> Result<StepPaneParts> {
    enum Section {
        None,
        Title,
        Description,
        Criteria,
    }

    let mut section = Section::None;
    let mut saw_title = false;
    let mut saw_description = false;
    let mut saw_criteria = false;

    let mut title_lines: Vec<String> = Vec::new();
    let mut description_lines: Vec<String> = Vec::new();
    let mut criteria: Vec<String> = Vec::new();

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed == STEP_TITLE_HEADER {
            section = Section::Title;
            saw_title = true;
            continue;
        }
        if trimmed == STEP_DESCRIPTION_HEADER {
            section = Section::Description;
            saw_description = true;
            continue;
        }
        if trimmed == STEP_CRITERIA_HEADER {
            section = Section::Criteria;
            saw_criteria = true;
            continue;
        }

        match section {
            Section::None => {
                // Free-form text before any header is silently dropped — the
                // file is not a general-purpose markdown document.
            }
            Section::Title => {
                if !trimmed.is_empty() {
                    title_lines.push(trimmed.to_string());
                }
            }
            Section::Description => {
                description_lines.push(line.to_string());
            }
            Section::Criteria => {
                if trimmed.is_empty() {
                    continue;
                }
                let bullet = trimmed
                    .strip_prefix("- ")
                    .or_else(|| trimmed.strip_prefix("* "))
                    .or_else(|| {
                        // Tolerate a bare leading `-` / `*` with no space
                        // (some editors auto-strip trailing whitespace), but
                        // reject anything else so a stray paragraph doesn't
                        // get silently absorbed as a criterion.
                        if trimmed == "-" || trimmed == "*" {
                            Some("")
                        } else {
                            None
                        }
                    });
                match bullet {
                    Some(item) => {
                        let item = item.trim();
                        if !item.is_empty() {
                            criteria.push(item.to_string());
                        }
                    }
                    None => {
                        anyhow::bail!(
                            "Acceptance criteria line {} is not a bullet (`- item`): {trimmed:?}",
                            idx + 1
                        );
                    }
                }
            }
        }
    }

    if !saw_title {
        anyhow::bail!("Missing `{STEP_TITLE_HEADER}` header");
    }
    if !saw_description {
        anyhow::bail!("Missing `{STEP_DESCRIPTION_HEADER}` header");
    }
    if !saw_criteria {
        anyhow::bail!("Missing `{STEP_CRITERIA_HEADER}` header");
    }
    if title_lines.is_empty() {
        anyhow::bail!("Title is empty");
    }
    if title_lines.len() > 1 {
        anyhow::bail!(
            "Title must be a single line; got {} non-blank lines",
            title_lines.len()
        );
    }

    // Trim leading/trailing blank lines from the description body so editor
    // noise doesn't drift the value across no-op round-trips. Internal blank
    // lines are preserved verbatim.
    let description = trim_blank_block(&description_lines);

    Ok(StepPaneParts {
        title: title_lines.into_iter().next().unwrap(),
        description,
        acceptance_criteria: criteria,
    })
}

/// Strip leading and trailing fully-blank lines from `lines`, then re-join
/// the remaining lines with `\n`. Used to normalize the description body so
/// editor-injected blank lines around a section don't flip a no-op edit to
/// "Saved".
fn trim_blank_block(lines: &[String]) -> String {
    let start = lines.iter().position(|l| !l.trim().is_empty());
    let end = lines.iter().rposition(|l| !l.trim().is_empty());
    match (start, end) {
        (Some(s), Some(e)) => lines[s..=e].join("\n"),
        _ => String::new(),
    }
}

/// Render the Tests pane editor format: one test command per line, with a
/// short comment header explaining the format. Blank entries are not emitted
/// so the round-trip parser observes the same list it was given.
pub(crate) fn format_tests_pane(tests: &[String]) -> String {
    let mut out = String::new();
    out.push_str("# One test command per line. Blank lines and `#` comments are ignored.\n");
    out.push('\n');
    for t in tests {
        let line = t.trim();
        if !line.is_empty() {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Parse the Tests pane editor format into the list of test commands. Blank
/// lines and lines whose first non-whitespace char is `#` are ignored;
/// everything else is taken verbatim (after trimming surrounding whitespace).
///
/// Returns an error only if the blob is *visibly* malformed — today the
/// parser is permissive enough that almost any text round-trips, so the
/// error path exists mainly to give the editor handoff a fallible API
/// matching the step-prompt parser. A single-line input that turns out to
/// be only comments is a valid "clear all tests" edit.
pub(crate) fn parse_tests_pane(text: &str) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('#') {
            continue;
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// View state
// ---------------------------------------------------------------------------

/// Step-detail view state. Independent of rendering and crossterm input so
/// pane focus + zen-mode behavior can be unit-tested without a terminal.
pub struct StepDetailApp {
    /// Plan being viewed — needed for the breadcrumb and (in later steps) for
    /// composing the upper prompt panes' contents.
    pub plan: Plan,

    /// All steps in the plan, in sort_key order. Drives the sidebar / gutter.
    pub steps: Vec<Step>,

    /// Index into `steps` for the step currently being viewed. The cursor
    /// follows the same step across pop/push transitions; vertical j/k in this
    /// view moves between *panes*, not between steps.
    pub selected_step_index: usize,

    /// Currently focused pane (initial focus is [`Pane::StepPrompt`] per §8).
    pub focused_pane: Pane,

    /// User-toggled zen state. `true` once the user has pressed `z` to
    /// collapse the sidebar; ignored when `auto_zen` is in effect (auto-zen
    /// always wins). Reset on view pop by the dispatcher dropping this struct.
    user_zen: bool,

    /// True when the most recently observed terminal width was below the
    /// threshold and zen mode is being forced. While forced, `z` is disabled.
    auto_zen: bool,

    /// Ratchet flag — once an auto-zen-trigger toast has been shown for this
    /// view instance, don't show it again on subsequent shrink/grow cycles.
    auto_zen_toast_shown: bool,

    /// Most recently observed terminal width. `0` means the view has not yet
    /// been rendered; stays clamped to the last value drawn.
    pub terminal_width: u16,

    /// Whether the user has requested to pop this view back to plan-detail
    /// (`q` / `h` / `←` / Ctrl-C, or `h` on a non-Appended pane per §8).
    pub should_pop: bool,

    /// Toast queue rendered over the bottom chrome row. The auto-zen trigger
    /// pushes onto this; later steps will use it for "Saved." style edit
    /// confirmations.
    pub toasts: ToastQueue,

    /// Universal-prompt prefix sourced from `Config.prompt_prefix`. Cloned at
    /// construction so the view doesn't need to retain a `&Config` reference.
    pub config_prompt_prefix: Option<String>,

    /// Universal-prompt suffix sourced from `Config.prompt_suffix`.
    pub config_prompt_suffix: Option<String>,

    /// Project-prompt wrap pair sourced from the `project_settings` row.
    pub project_settings: ProjectSettings,

    /// `Config.default_harness` — used as the bottom-row Harness fallback when
    /// neither `step.harness` nor `plan.harness` is set.
    pub default_harness_name: String,

    /// Lookup of `harness_name → default_model` from `Config.harnesses`. The
    /// bottom-row Model cell consults this only when `step.model` is `None`,
    /// per the §8 "step → plan → config fallback" rule (plans don't currently
    /// store a per-plan model — fallback skips straight to the harness's
    /// configured default).
    pub harness_default_models: HashMap<String, Option<String>>,

    /// Execution-log rows for the focused step, sorted by `attempt` ASC (oldest
    /// first). Source for the Appended pane; `h`/`l` paginate by index.
    pub execution_logs: Vec<ExecutionLog>,

    /// Index into `execution_logs` for the attempt currently shown in the
    /// Appended pane. Defaults to the most recent attempt (`len - 1`) on view
    /// construction, per TUI-plan.md §8 ("most recent by default"). When
    /// `execution_logs` is empty this is `0` and ignored by the renderer.
    pub appended_attempt_index: usize,
}

impl StepDetailApp {
    /// Create a new view focused on the step at `selected_step_index`. The
    /// caller is expected to pre-clamp the index into `0..steps.len()` (or
    /// pass `0` for an empty plan, in which case the view renders an empty
    /// sidebar).
    pub fn new(
        plan: Plan,
        steps: Vec<Step>,
        selected_step_index: usize,
        config: &Config,
        project_settings: ProjectSettings,
        execution_logs: Vec<ExecutionLog>,
    ) -> Self {
        let clamped = if steps.is_empty() {
            0
        } else {
            selected_step_index.min(steps.len() - 1)
        };
        let harness_default_models = config
            .harnesses
            .iter()
            .map(|(name, hc)| (name.clone(), hc.default_model.clone()))
            .collect();
        let appended_attempt_index = execution_logs.len().saturating_sub(1);
        Self {
            plan,
            steps,
            selected_step_index: clamped,
            focused_pane: Pane::StepPrompt,
            user_zen: false,
            auto_zen: false,
            auto_zen_toast_shown: false,
            terminal_width: 0,
            should_pop: false,
            toasts: ToastQueue::new(),
            config_prompt_prefix: config.prompt_prefix.clone(),
            config_prompt_suffix: config.prompt_suffix.clone(),
            project_settings,
            default_harness_name: config.default_harness.clone(),
            harness_default_models,
            execution_logs,
            appended_attempt_index,
        }
    }

    // -- Pane focus ------------------------------------------------------

    /// Move focus to the previous pane in the stack, wrapping from the top
    /// pane back to the bottom row.
    pub fn focus_up(&mut self) {
        let i = self.focused_pane.index();
        let next = if i == 0 { Pane::ORDER.len() - 1 } else { i - 1 };
        self.focused_pane = Pane::ORDER[next];
    }

    /// Move focus to the next pane in the stack, wrapping from the bottom
    /// row back to the top pane.
    pub fn focus_down(&mut self) {
        let i = self.focused_pane.index();
        let next = (i + 1) % Pane::ORDER.len();
        self.focused_pane = Pane::ORDER[next];
    }

    // -- Zen mode --------------------------------------------------------

    /// Toggle the user-driven zen state. No-op while auto-zen is forcing zen
    /// mode (the spec disables `z` in that state — TUI-plan.md §18 Q5).
    /// Returns `true` when the toggle was applied.
    pub fn toggle_zen(&mut self) -> bool {
        if self.auto_zen {
            return false;
        }
        self.user_zen = !self.user_zen;
        true
    }

    /// True when the sidebar should render as the thin gutter — either the
    /// user toggled it or the terminal is too narrow to fit the full sidebar.
    pub fn is_zen_mode(&self) -> bool {
        self.user_zen || self.auto_zen
    }

    /// True when zen mode is being forced by the auto-zen threshold (and `z`
    /// is therefore disabled).
    pub fn is_zen_forced(&self) -> bool {
        self.auto_zen
    }

    /// Recompute auto-zen state from a terminal width. Returns the toast text
    /// to push when this transition is the *first* sub-threshold render of
    /// the view — callers feed the result into [`Self::toasts`] (or use
    /// [`Self::observe_terminal_width`], which does that internally).
    pub fn update_auto_zen(&mut self, width: u16) -> Option<&'static str> {
        self.terminal_width = width;
        let was_forced = self.auto_zen;
        self.auto_zen = width < AUTO_ZEN_WIDTH_THRESHOLD;
        if self.auto_zen && !was_forced && !self.auto_zen_toast_shown {
            self.auto_zen_toast_shown = true;
            Some(AUTO_ZEN_TOAST)
        } else {
            None
        }
    }

    /// Convenience wrapper: observe the latest terminal width and push the
    /// auto-zen toast onto [`Self::toasts`] if we just crossed the threshold.
    /// Used by the renderer; tests use the lower-level [`Self::update_auto_zen`].
    pub fn observe_terminal_width(&mut self, width: u16, now: Instant) {
        if let Some(text) = self.update_auto_zen(width) {
            self.toasts.push(text, ToastKind::Info, now);
        }
    }

    // -- Pop -------------------------------------------------------------

    /// Signal the dispatcher to pop this view back to plan-detail.
    pub fn request_pop(&mut self) {
        self.should_pop = true;
    }

    // -- Appended pane pagination (TUI-plan.md §8 "Appended-prompt navigation")

    /// Currently focused execution log in the Appended pane, or `None` when
    /// the step has no recorded attempts yet.
    pub fn current_appended_log(&self) -> Option<&ExecutionLog> {
        self.execution_logs.get(self.appended_attempt_index)
    }

    /// True when the Appended pane is on the leftmost (oldest) attempt — the
    /// state where `h`/`←` falls through to popping the view per §8.
    pub fn appended_at_leftmost(&self) -> bool {
        self.appended_attempt_index == 0
    }

    /// True when the Appended pane is on the rightmost (newest) attempt;
    /// `l`/`→` is a no-op in this state.
    pub fn appended_at_rightmost(&self) -> bool {
        self.execution_logs.is_empty()
            || self.appended_attempt_index + 1 >= self.execution_logs.len()
    }

    /// Move the Appended pane to the previous (older) attempt. Returns true
    /// when the index actually moved; false at the leftmost or with empty
    /// logs (so callers can fall through to a pop).
    pub fn appended_prev(&mut self) -> bool {
        if self.execution_logs.is_empty() || self.appended_attempt_index == 0 {
            return false;
        }
        self.appended_attempt_index -= 1;
        true
    }

    /// Move the Appended pane to the next (newer) attempt. Returns true when
    /// the index actually moved; false when already at the rightmost or with
    /// empty logs.
    pub fn appended_next(&mut self) -> bool {
        if self.appended_at_rightmost() {
            return false;
        }
        self.appended_attempt_index += 1;
        true
    }

    /// Handle `h` / `←` in the step-detail view per §8. The Appended pane
    /// intercepts the key first when a previous attempt exists; otherwise
    /// (Appended at leftmost OR any other pane focused) the view pops back
    /// to plan-detail.
    pub fn handle_left(&mut self) {
        if self.focused_pane == Pane::Appended && self.appended_prev() {
            return;
        }
        self.request_pop();
    }

    /// Handle `l` / `→` in the step-detail view per §8. Only meaningful on
    /// the Appended pane (advances to the next attempt); a no-op elsewhere.
    pub fn handle_right(&mut self) {
        if self.focused_pane == Pane::Appended {
            self.appended_next();
        }
    }

    /// Title shown on the Appended pane's bordered block. Includes the
    /// `(attempt N/M)` segment when the step has at least one execution log;
    /// falls back to the bare label when there are no recorded attempts.
    pub fn appended_pane_title(&self) -> String {
        if self.execution_logs.is_empty() {
            return Pane::Appended.title().to_string();
        }
        let total = self.execution_logs.len();
        let n = self.appended_attempt_index + 1;
        format!("Appended (attempt {n}/{total})")
    }

    // -- Step accessors --------------------------------------------------

    /// The step currently being viewed, or `None` when the plan has no steps.
    pub fn current_step(&self) -> Option<&Step> {
        self.steps.get(self.selected_step_index)
    }

    /// 1-based step number rendered in the breadcrumb / gutter, or `None`
    /// for an empty plan.
    pub fn current_step_number(&self) -> Option<usize> {
        if self.steps.is_empty() {
            None
        } else {
            Some(self.selected_step_index + 1)
        }
    }

    /// `"step N: <title>"` segment for the breadcrumb; empty string when no
    /// steps exist (kept non-`None` so the breadcrumb still renders cleanly).
    pub fn breadcrumb_step_segment(&self) -> String {
        match self.current_step() {
            Some(step) => format!("step {}: {}", self.selected_step_index + 1, step.title),
            None => "(no steps)".to_string(),
        }
    }

    // -- Bottom-row resolved values --------------------------------------

    /// Resolved harness name for the current step:
    /// `step.harness ?? plan.harness ?? config.default_harness`.
    pub fn effective_harness(&self) -> String {
        self.current_step()
            .and_then(|s| s.harness.clone())
            .or_else(|| self.plan.harness.clone())
            .unwrap_or_else(|| self.default_harness_name.clone())
    }

    /// Resolved model for the current step:
    /// `step.model ?? Config.harnesses[<resolved harness>].default_model`.
    /// Plans don't currently carry a per-plan model column, so the fallback
    /// jumps straight from step to the harness's config-level default.
    pub fn effective_model(&self) -> Option<String> {
        if let Some(step) = self.current_step()
            && let Some(model) = step.model.clone()
        {
            return Some(model);
        }
        let harness = self.effective_harness();
        self.harness_default_models
            .get(&harness)
            .cloned()
            .flatten()
    }

    /// Resolved agent for the current step: `step.agent ?? plan.agent`.
    pub fn effective_agent(&self) -> Option<String> {
        self.current_step()
            .and_then(|s| s.agent.clone())
            .or_else(|| self.plan.agent.clone())
    }

    /// Resolved change policy. The column is non-nullable on the step row
    /// (defaults to [`ChangePolicy::Required`]), so this is just the step's
    /// own value — included as a method for symmetry with the other cells.
    /// Returns the policy's default when the plan is empty (so a no-step
    /// view still renders the bottom row without panicking).
    pub fn effective_change_policy(&self) -> ChangePolicy {
        self.current_step()
            .map(|s| s.change_policy)
            .unwrap_or_default()
    }

    // -- `c` editor handoff for single-string panes (TUI-plan.md §8 + §18 Q3)

    /// `c` on the Universal pane: round-trip `Config.prompt_prefix` and
    /// `Config.prompt_suffix` through `$EDITOR` and persist via
    /// [`Config::save_at`]. Mutates the in-memory `config` so subsequent
    /// reads of the same struct see the new values, and refreshes the
    /// app's mirrored copies so the pane re-renders without a reload.
    ///
    /// `edit_fn` is the editor-handoff callback — production passes
    /// `tui::editor::edit_in_editor`; tests pass a closure backed by a
    /// shell-script mock to avoid spawning a real editor.
    pub fn edit_universal_pane<E>(
        &mut self,
        config: &mut Config,
        config_dir: &Path,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = format_wrap_pane(
            config.prompt_prefix.as_deref(),
            config.prompt_suffix.as_deref(),
        );
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let (new_prefix, new_suffix) = parse_wrap_pane(&new_text);
        if new_prefix == config.prompt_prefix && new_suffix == config.prompt_suffix {
            return Ok(EditOutcome::NoChanges);
        }
        config.prompt_prefix = new_prefix.clone();
        config.prompt_suffix = new_suffix.clone();
        config.save_at(config_dir)?;
        self.config_prompt_prefix = new_prefix;
        self.config_prompt_suffix = new_suffix;
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Project pane: round-trip the project-scope prompt prefix
    /// and suffix through `$EDITOR` and persist via the storage helpers.
    /// Each side updates independently so a no-op on one half doesn't churn
    /// its `updated_at` stamp.
    pub fn edit_project_pane<E>(
        &mut self,
        conn: &Connection,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = format_wrap_pane(
            self.project_settings.prompt_prefix.as_deref(),
            self.project_settings.prompt_suffix.as_deref(),
        );
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let (new_prefix, new_suffix) = parse_wrap_pane(&new_text);
        if new_prefix == self.project_settings.prompt_prefix
            && new_suffix == self.project_settings.prompt_suffix
        {
            return Ok(EditOutcome::NoChanges);
        }
        if new_prefix != self.project_settings.prompt_prefix {
            storage::set_project_prompt_prefix(conn, &self.plan.project, new_prefix.as_deref())?;
            self.project_settings.prompt_prefix = new_prefix;
        }
        if new_suffix != self.project_settings.prompt_suffix {
            storage::set_project_prompt_suffix(conn, &self.plan.project, new_suffix.as_deref())?;
            self.project_settings.prompt_suffix = new_suffix;
        }
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Plan-context-prepend pane: round-trip
    /// `plan.context_prepend` through `$EDITOR` and persist via
    /// [`storage::set_plan_context_prepend`].
    ///
    /// When `plan.context_prepend` is `None`, the editor is seeded with the
    /// system [`DEFAULT_CONTEXT_PREPEND`] so the user sees what the runner
    /// would inject — but a no-op edit leaves the column as `None` (so the
    /// plan continues to track the default if it changes upstream). Any
    /// real edit pins the override to `Some(text)`, including the empty
    /// string (the documented escape hatch for "no prepend at all").
    pub fn edit_plan_context_prepend_pane<E>(
        &mut self,
        conn: &Connection,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = match self.plan.context_prepend.as_deref() {
            Some(s) => s.to_string(),
            None => DEFAULT_CONTEXT_PREPEND.to_string(),
        };
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        // Normalize trailing-newline noise so editors that auto-append a
        // newline don't flip a no-op edit into a "Saved" outcome.
        let new_normalized = new_text.trim_end_matches('\n').to_string();
        let initial_normalized = initial.trim_end_matches('\n');
        if new_normalized == initial_normalized {
            return Ok(EditOutcome::NoChanges);
        }
        storage::set_plan_context_prepend(conn, &self.plan.id, Some(&new_normalized))?;
        self.plan.context_prepend = Some(new_normalized);
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Plan-prompt pane: round-trip `plan.prompt_prefix` and
    /// `plan.prompt_suffix` through `$EDITOR` using the same two-section
    /// format as the universal/project panes. Each side is updated
    /// independently so a no-op on one half doesn't churn `updated_at`.
    pub fn edit_plan_prompt_pane<E>(
        &mut self,
        conn: &Connection,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = format_wrap_pane(
            self.plan.prompt_prefix.as_deref(),
            self.plan.prompt_suffix.as_deref(),
        );
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let (new_prefix, new_suffix) = parse_wrap_pane(&new_text);
        if new_prefix == self.plan.prompt_prefix && new_suffix == self.plan.prompt_suffix {
            return Ok(EditOutcome::NoChanges);
        }
        if new_prefix != self.plan.prompt_prefix {
            storage::set_plan_prompt_prefix(conn, &self.plan.id, new_prefix.as_deref())?;
            self.plan.prompt_prefix = new_prefix;
        }
        if new_suffix != self.plan.prompt_suffix {
            storage::set_plan_prompt_suffix(conn, &self.plan.id, new_suffix.as_deref())?;
            self.plan.prompt_suffix = new_suffix;
        }
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Step-prompt pane: round-trip `step.title`,
    /// `step.description`, and `step.acceptance_criteria` through `$EDITOR`
    /// as a single three-section markdown blob and persist via
    /// [`storage::update_step_fields_ext`].
    ///
    /// On parse failure (missing header, empty title, malformed bullet)
    /// returns [`EditOutcome::ParseError`] without writing — the dispatcher
    /// shows a red toast and the user re-edits.
    ///
    /// No-ops when the plan has no steps under the current selection (the
    /// pane already renders a `(no steps)` placeholder in that case).
    pub fn edit_step_prompt_pane<E>(
        &mut self,
        conn: &Connection,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let Some(step) = self.steps.get(self.selected_step_index).cloned() else {
            return Ok(EditOutcome::NoChanges);
        };
        let initial = format_step_pane(&step.title, &step.description, &step.acceptance_criteria);
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let parts = match parse_step_pane(&new_text) {
            Ok(p) => p,
            Err(e) => return Ok(EditOutcome::ParseError(e.to_string())),
        };

        let title_changed = parts.title != step.title;
        let description_changed = parts.description != step.description;
        let criteria_changed = parts.acceptance_criteria != step.acceptance_criteria;

        if !title_changed && !description_changed && !criteria_changed {
            return Ok(EditOutcome::NoChanges);
        }

        storage::update_step_fields_ext(
            conn,
            &step.id,
            title_changed.then_some(parts.title.as_str()),
            description_changed.then_some(parts.description.as_str()),
            None,
            None,
            criteria_changed.then_some(parts.acceptance_criteria.as_slice()),
            None,
            None,
            None,
            None,
        )?;
        // Refresh the in-memory step so the pane re-renders without a reload.
        if let Some(step_mut) = self.steps.get_mut(self.selected_step_index) {
            if title_changed {
                step_mut.title = parts.title;
            }
            if description_changed {
                step_mut.description = parts.description;
            }
            if criteria_changed {
                step_mut.acceptance_criteria = parts.acceptance_criteria;
            }
        }
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Tests pane: round-trip `plan.deterministic_tests` through
    /// `$EDITOR` (one test per line, blank/`#`-prefixed lines ignored) and
    /// persist via [`storage::set_plan_deterministic_tests`].
    pub fn edit_tests_pane<E>(
        &mut self,
        conn: &Connection,
        edit_fn: E,
    ) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = format_tests_pane(&self.plan.deterministic_tests);
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let new_tests = match parse_tests_pane(&new_text) {
            Ok(t) => t,
            Err(e) => return Ok(EditOutcome::ParseError(e.to_string())),
        };
        if new_tests == self.plan.deterministic_tests {
            return Ok(EditOutcome::NoChanges);
        }
        storage::set_plan_deterministic_tests(conn, &self.plan.id, &new_tests)?;
        self.plan.deterministic_tests = new_tests;
        Ok(EditOutcome::Saved)
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the step-detail view, including persistent chrome and toast overlay.
pub fn draw(frame: &mut Frame, app: &mut StepDetailApp) {
    let now = Instant::now();
    app.toasts.prune(now);
    app.observe_terminal_width(frame.area().width, now);

    let step_segment = app.breadcrumb_step_segment();
    let crumbs: [&str; 3] = ["ralph", app.plan.slug.as_str(), step_segment.as_str()];
    let hint = "[j/k] pane  [h/←] back  [z] zen  [q] back";
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint,
            cwd: Path::new(&app.plan.project),
        },
    );

    if body.width == 0 || body.height == 0 {
        return;
    }

    let sidebar_w = if app.is_zen_mode() {
        SIDEBAR_ZEN_WIDTH
    } else {
        SIDEBAR_FULL_WIDTH
    };
    let sidebar_w = sidebar_w.min(body.width.saturating_sub(1).max(1));

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(sidebar_w), Constraint::Min(0)])
        .split(body);

    draw_sidebar(frame, app, main[0]);
    draw_pane_stack(frame, app, main[1]);

    if let Some(toast) = app.toasts.current() {
        let area = frame.area();
        if area.height >= 1 && area.width > 0 {
            render_toast_overlay(frame, area, &toast.text, toast.color);
        }
    }
}

fn render_toast_overlay(frame: &mut Frame, area: Rect, text: &str, color: ratatui::style::Color) {
    let max_toast = area.width.saturating_sub(1).max(1);
    let desired = text.chars().count().min(max_toast as usize) as u16;
    if desired == 0 {
        return;
    }
    let toast_area = Rect {
        x: area.x,
        y: area.y + area.height - 1,
        width: desired,
        height: 1,
    };
    frame.render_widget(Clear, toast_area);
    let para = Paragraph::new(Span::styled(
        text.chars().take(toast_area.width as usize).collect::<String>(),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(para, toast_area);
}

fn draw_sidebar(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    if app.is_zen_mode() {
        draw_zen_gutter(frame, app, area);
    } else {
        draw_full_sidebar(frame, app, area);
    }
}

fn draw_full_sidebar(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    let block = Block::default()
        .title(format!(" {} ", app.plan.slug))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 {
        return;
    }
    let visible = inner.height as usize;
    let total = app.steps.len();
    let start = app
        .selected_step_index
        .saturating_sub(visible.saturating_sub(1) / 2);
    let end = (start + visible).min(total);

    for (slot, idx) in (start..end).enumerate() {
        let step = &app.steps[idx];
        let row = Rect {
            x: inner.x,
            y: inner.y + slot as u16,
            width: inner.width,
            height: 1,
        };
        let glyph = status_glyph(step.status);
        let label = format!("{glyph} {}. {}", idx + 1, step.title);
        let mut style = status_style(step.status);
        if idx == app.selected_step_index {
            style = style
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD);
        }
        let para = Paragraph::new(Span::styled(
            chrome::right_truncate(&label, row.width as usize),
            style,
        ));
        frame.render_widget(para, row);
    }
}

fn draw_zen_gutter(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    // Bordered gutter so the view's two halves are still visually separated;
    // contents are just `<glyph> <num>` per row.
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let visible = inner.height as usize;
    let total = app.steps.len();
    let start = app
        .selected_step_index
        .saturating_sub(visible.saturating_sub(1) / 2);
    let end = (start + visible).min(total);

    for (slot, idx) in (start..end).enumerate() {
        let step = &app.steps[idx];
        let row = Rect {
            x: inner.x,
            y: inner.y + slot as u16,
            width: inner.width,
            height: 1,
        };
        let glyph = status_glyph(step.status);
        let label = format!("{glyph}{}", idx + 1);
        let mut style = status_style(step.status);
        if idx == app.selected_step_index {
            style = style
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD);
        }
        let para = Paragraph::new(Span::styled(
            chrome::right_truncate(&label, row.width as usize),
            style,
        ));
        frame.render_widget(para, row);
    }
}

fn draw_pane_stack(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    // Equal-share layout for the seven multi-line panes plus a fixed-height
    // bottom row. The bottom row only renders one line of text, so pinning it
    // at 3 cells (border + content + border) keeps it from greedily eating
    // half the screen on tall terminals while leaving the seven prompt panes
    // to share the remainder.
    let mut constraints: Vec<Constraint> = Pane::ORDER
        .iter()
        .map(|p| match p {
            Pane::BottomRow => Constraint::Length(3),
            _ => Constraint::Min(3),
        })
        .collect();
    // Layout::split panics if constraints is empty, but ORDER has 8 entries
    // so this is unreachable; the explicit assertion documents the invariant.
    debug_assert!(!constraints.is_empty());
    if constraints.is_empty() {
        constraints.push(Constraint::Min(3));
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, pane) in Pane::ORDER.iter().enumerate() {
        draw_pane(frame, app, *pane, chunks[i]);
    }
}

/// Draw one pane — the bordered block plus its read-only body content. The
/// body is sourced from whichever column / config field §8 designates as the
/// pane's source of truth.
fn draw_pane(frame: &mut Frame, app: &StepDetailApp, pane: Pane, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let focused = pane == app.focused_pane;
    let border_color = if focused { theme::CURSOR } else { Color::Cyan };
    let mut style = Style::default().fg(border_color);
    if focused {
        style = style.add_modifier(Modifier::BOLD);
    }
    let title_text = match pane {
        Pane::Appended => app.appended_pane_title(),
        _ => pane.title().to_string(),
    };
    let block = Block::default()
        .title(format!(" {title_text} "))
        .borders(Borders::ALL)
        .border_style(style);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    match pane {
        Pane::UniversalPrompt => render_wrap_pane(
            frame,
            inner,
            app.config_prompt_prefix.as_deref(),
            app.config_prompt_suffix.as_deref(),
        ),
        Pane::ProjectPrompt => render_wrap_pane(
            frame,
            inner,
            app.project_settings.prompt_prefix.as_deref(),
            app.project_settings.prompt_suffix.as_deref(),
        ),
        Pane::PlanContextPrepend => render_text_pane(
            frame,
            inner,
            // `None` means "use DEFAULT_CONTEXT_PREPEND"; `Some("")` is the
            // power-user "no prepend" escape hatch and renders as a literal
            // blank — distinguishable from the dim `(none)` placeholder.
            match app.plan.context_prepend.as_deref() {
                Some(s) => Some(s),
                None => Some(DEFAULT_CONTEXT_PREPEND),
            },
        ),
        Pane::PlanPrompt => render_wrap_pane(
            frame,
            inner,
            app.plan.prompt_prefix.as_deref(),
            app.plan.prompt_suffix.as_deref(),
        ),
        Pane::StepPrompt => render_step_prompt(frame, app, inner),
        Pane::Appended => render_appended(frame, app, inner),
        Pane::Tests => render_tests(frame, app, inner),
        Pane::BottomRow => render_bottom_row(frame, app, inner),
    }
}

/// Render a single text body (plan-context-prepend pane). `None` becomes the
/// dim `(none)` placeholder; `Some("")` renders as an empty body.
fn render_text_pane(frame: &mut Frame, area: Rect, text: Option<&str>) {
    let para = match text {
        None => Paragraph::new(Span::styled(
            NONE_PLACEHOLDER,
            Style::default().fg(theme::CHROME_DIM),
        )),
        Some(s) => Paragraph::new(s.to_string()),
    }
    .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Render a prefix/suffix wrap pane (Universal, Project, Plan prompt). Both
/// halves render with bolded labels so the operator can tell which is which
/// even when one side is empty. When both are absent we collapse to a single
/// `(none)` line to match the other read-only renders.
fn render_wrap_pane(frame: &mut Frame, area: Rect, prefix: Option<&str>, suffix: Option<&str>) {
    let mut lines: Vec<Line> = Vec::new();
    let label_style = Style::default().add_modifier(Modifier::BOLD);

    if prefix.is_none() && suffix.is_none() {
        lines.push(Line::from(Span::styled(
            NONE_PLACEHOLDER,
            Style::default().fg(theme::CHROME_DIM),
        )));
    } else {
        lines.push(Line::from(Span::styled("Prefix:", label_style)));
        match prefix {
            Some(s) if !s.is_empty() => {
                for line in s.lines() {
                    lines.push(Line::from(line.to_string()));
                }
            }
            Some(_) => {
                lines.push(Line::from(Span::styled(
                    "(empty)",
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
            None => {
                lines.push(Line::from(Span::styled(
                    NONE_PLACEHOLDER,
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("Suffix:", label_style)));
        match suffix {
            Some(s) if !s.is_empty() => {
                for line in s.lines() {
                    lines.push(Line::from(line.to_string()));
                }
            }
            Some(_) => {
                lines.push(Line::from(Span::styled(
                    "(empty)",
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
            None => {
                lines.push(Line::from(Span::styled(
                    NONE_PLACEHOLDER,
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
        }
    }

    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Render the Step prompt pane: title, description, and the bulleted
/// acceptance criteria.
fn render_step_prompt(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    let Some(step) = app.current_step() else {
        let para = Paragraph::new(Span::styled(
            "(no steps)",
            Style::default().fg(theme::CHROME_DIM),
        ));
        frame.render_widget(para, area);
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    let bold = Style::default().add_modifier(Modifier::BOLD);

    lines.push(Line::from(Span::styled(step.title.clone(), bold)));

    if !step.description.is_empty() {
        lines.push(Line::from(""));
        for line in step.description.lines() {
            lines.push(Line::from(line.to_string()));
        }
    }

    if !step.acceptance_criteria.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("Acceptance:", bold)));
        for c in &step.acceptance_criteria {
            lines.push(Line::from(format!("  • {c}")));
        }
    }

    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Render the Appended pane body: read-only retry context for the focused
/// attempt. Spec wording (TUI-plan.md §8) is "previous diff, test output,
/// modified files" — i.e., the data sourced from attempt N-1 and appended to
/// attempt N's prompt. Attempt 1 has no previous attempt, so it renders a
/// dim placeholder; an empty execution log renders the same shape it had
/// before any attempts ran.
fn render_appended(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    if app.execution_logs.is_empty() {
        let para = Paragraph::new(Span::styled(
            "(retry context appears here once an attempt has run)",
            Style::default().fg(theme::CHROME_DIM),
        ))
        .wrap(Wrap { trim: false });
        frame.render_widget(para, area);
        return;
    }

    // First attempt has no preceding log to source retry context from.
    if app.appended_attempt_index == 0 {
        let para = Paragraph::new(Span::styled(
            "(first attempt — no appended retry context)",
            Style::default().fg(theme::CHROME_DIM),
        ))
        .wrap(Wrap { trim: false });
        frame.render_widget(para, area);
        return;
    }

    // Source-of-truth: the previous attempt's diff and test output. Match
    // format_retry_context's truncation knobs (200 / 100 lines) so the pane
    // shows the same content the runner actually appended.
    let prev = &app.execution_logs[app.appended_attempt_index - 1];
    let bold = Style::default().add_modifier(Modifier::BOLD);
    let dim = Style::default().fg(theme::CHROME_DIM);
    let mut lines: Vec<Line> = Vec::new();

    lines.push(Line::from(Span::styled("Previous diff:", bold)));
    match prev.diff.as_deref() {
        Some(d) if !d.is_empty() => {
            for line in truncate_lines(d, 200).lines() {
                lines.push(Line::from(line.to_string()));
            }
        }
        _ => lines.push(Line::from(Span::styled(NONE_PLACEHOLDER, dim))),
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("Previous test output:", bold)));
    if prev.test_results.is_empty() {
        lines.push(Line::from(Span::styled(NONE_PLACEHOLDER, dim)));
    } else {
        let joined = prev.test_results.join("\n");
        for line in truncate_lines(&joined, 100).lines() {
            lines.push(Line::from(line.to_string()));
        }
    }

    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Cap `text` at `max_lines` lines, appending an `... (N lines omitted) ...`
/// marker when truncated. Mirrors the runner's `truncate_text` so the pane
/// surfaces the same view the prompt builder appends.
fn truncate_lines(text: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= max_lines {
        text.to_string()
    } else {
        let omitted = lines.len() - max_lines;
        let head = &lines[..max_lines];
        format!("{}\n... ({omitted} lines omitted) ...", head.join("\n"))
    }
}

/// Render the Tests pane: `plan.deterministic_tests` as a bulleted list, or
/// a dim placeholder if no tests are configured.
fn render_tests(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    let lines: Vec<Line> = if app.plan.deterministic_tests.is_empty() {
        vec![Line::from(Span::styled(
            NONE_PLACEHOLDER,
            Style::default().fg(theme::CHROME_DIM),
        ))]
    } else {
        app.plan
            .deterministic_tests
            .iter()
            .map(|t| Line::from(format!("• {t}")))
            .collect()
    };
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// Render the bottom row: four cells (Harness / Model / Agent / Change
/// policy) split horizontally inside the pane's inner area. Effective values
/// follow the step → plan → config fallback per §8.
fn render_bottom_row(frame: &mut Frame, app: &StepDetailApp, area: Rect) {
    let cells = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
        ])
        .split(area);

    let entries = [
        ("Harness", Some(app.effective_harness())),
        ("Model", app.effective_model()),
        ("Agent", app.effective_agent()),
        (
            "Change policy",
            Some(app.effective_change_policy().to_string()),
        ),
    ];

    for (i, (label, value)) in entries.iter().enumerate() {
        if cells[i].width == 0 {
            continue;
        }
        let value_span = match value {
            Some(s) if !s.is_empty() => Span::raw(s.clone()),
            _ => Span::styled(EMPTY_CELL.to_string(), Style::default().fg(theme::CHROME_DIM)),
        };
        let line = Line::from(vec![
            Span::styled(
                format!("{label}: "),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            value_span,
        ]);
        let para = Paragraph::new(line);
        frame.render_widget(para, cells[i]);
    }
}

fn status_glyph(status: StepStatus) -> &'static str {
    match status {
        StepStatus::Pending => "○",
        StepStatus::InProgress => "▶",
        StepStatus::Complete => "✔",
        StepStatus::Failed => "✘",
        StepStatus::Skipped => "⊘",
        StepStatus::Aborted => "⊘",
    }
}

fn status_style(status: StepStatus) -> Style {
    match status {
        StepStatus::Complete => Style::default().fg(theme::STATUS_COMPLETE),
        StepStatus::InProgress => Style::default()
            .fg(theme::STATUS_IN_PROGRESS)
            .add_modifier(Modifier::BOLD),
        StepStatus::Failed => Style::default().fg(theme::STATUS_FAILED),
        StepStatus::Skipped => Style::default().fg(theme::CHROME_DIM),
        StepStatus::Aborted => Style::default().fg(theme::STATUS_FAILED),
        StepStatus::Pending => Style::default().fg(theme::STATUS_PENDING),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ChangePolicy, Plan, PlanStatus, Step, StepStatus};
    use chrono::Utc;

    fn make_plan() -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "tui-v1".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "tui-v1".to_string(),
            description: String::new(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
        }
    }

    fn make_steps(n: usize) -> Vec<Step> {
        (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                plan_id: "p1".to_string(),
                sort_key: format!("a{i}"),
                title: format!("Step {}", i + 1),
                description: format!("Description {}", i + 1),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                status: if i == 0 {
                    StepStatus::Complete
                } else if i == 1 {
                    StepStatus::InProgress
                } else {
                    StepStatus::Pending
                },
                attempts: 0,
                max_retries: Some(3),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                model: None,
                skipped_reason: None,
                change_policy: ChangePolicy::Required,
                tags: vec![],
            })
            .collect()
    }

    fn make_app(n: usize, selected: usize) -> StepDetailApp {
        StepDetailApp::new(
            make_plan(),
            make_steps(n),
            selected,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        )
    }

    /// Build a fake execution log row for a given attempt number with optional
    /// diff / test output. Matches the column shape of `ExecutionLog` so
    /// pagination tests can construct multi-attempt histories.
    fn make_log(attempt: i32, diff: Option<&str>, test_output: Option<&str>) -> ExecutionLog {
        ExecutionLog {
            id: attempt as i64,
            step_id: "s0".to_string(),
            attempt,
            started_at: Utc::now(),
            duration_secs: Some(1.0),
            prompt_text: None,
            diff: diff.map(str::to_string),
            test_results: test_output.map(|s| vec![s.to_string()]).unwrap_or_default(),
            rolled_back: false,
            committed: false,
            commit_hash: None,
            harness_stdout: None,
            harness_stderr: None,
            cost_usd: None,
            input_tokens: None,
            output_tokens: None,
            session_id: None,
            termination_reason: None,
            test_status: None,
        }
    }

    fn make_app_with_logs(n: usize, selected: usize, logs: Vec<ExecutionLog>) -> StepDetailApp {
        StepDetailApp::new(
            make_plan(),
            make_steps(n),
            selected,
            &Config::default(),
            ProjectSettings::default(),
            logs,
        )
    }

    // -- Pane order / titles ------------------------------------------------

    #[test]
    fn pane_order_matches_section_8_layout() {
        // The §8 sketch lists exactly these eight panes top to bottom.
        assert_eq!(
            Pane::ORDER,
            [
                Pane::UniversalPrompt,
                Pane::ProjectPrompt,
                Pane::PlanContextPrepend,
                Pane::PlanPrompt,
                Pane::StepPrompt,
                Pane::Appended,
                Pane::Tests,
                Pane::BottomRow,
            ]
        );
    }

    // -- Initial focus ------------------------------------------------------

    #[test]
    fn initial_focus_is_step_prompt() {
        let app = make_app(3, 0);
        assert_eq!(app.focused_pane, Pane::StepPrompt);
    }

    #[test]
    fn initial_focus_is_step_prompt_even_with_no_steps() {
        let app = make_app(0, 0);
        assert_eq!(app.focused_pane, Pane::StepPrompt);
    }

    // -- Pane navigation ----------------------------------------------------

    #[test]
    fn focus_down_walks_full_stack_and_wraps() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::UniversalPrompt;
        let mut seen = vec![app.focused_pane];
        for _ in 0..Pane::ORDER.len() {
            app.focus_down();
            seen.push(app.focused_pane);
        }
        // After ORDER.len() down-presses we should have walked through every
        // pane and wrapped back to the start.
        assert_eq!(seen.first(), Some(&Pane::UniversalPrompt));
        assert_eq!(seen.last(), Some(&Pane::UniversalPrompt));
        // The middle of the trace should hit each pane exactly once.
        let middle: Vec<Pane> = seen[..Pane::ORDER.len()].to_vec();
        assert_eq!(middle, Pane::ORDER.to_vec());
    }

    #[test]
    fn focus_up_walks_stack_in_reverse_and_wraps() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::UniversalPrompt;
        // First up-press wraps to the bottom row.
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::BottomRow);
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::Tests);
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::Appended);
    }

    #[test]
    fn focus_down_from_bottom_row_wraps_to_universal() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::BottomRow;
        app.focus_down();
        assert_eq!(app.focused_pane, Pane::UniversalPrompt);
    }

    #[test]
    fn focus_up_from_universal_wraps_to_bottom_row() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::UniversalPrompt;
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::BottomRow);
    }

    #[test]
    fn focus_down_from_step_prompt_advances_to_appended() {
        // Sanity check that the initial focus + one down-press lands on the
        // Appended pane (where step 24 will add the `h`/`l` paginator).
        let mut app = make_app(3, 0);
        app.focus_down();
        assert_eq!(app.focused_pane, Pane::Appended);
    }

    // -- Zen toggle ---------------------------------------------------------

    #[test]
    fn zen_mode_defaults_to_false() {
        let app = make_app(3, 0);
        assert!(!app.is_zen_mode());
        assert!(!app.is_zen_forced());
    }

    #[test]
    fn toggle_zen_flips_user_zen_state() {
        let mut app = make_app(3, 0);
        assert!(app.toggle_zen());
        assert!(app.is_zen_mode());
        assert!(app.toggle_zen());
        assert!(!app.is_zen_mode());
    }

    #[test]
    fn toggle_zen_is_disabled_while_auto_zen_forces() {
        let mut app = make_app(3, 0);
        // Drop below threshold to force auto-zen.
        app.update_auto_zen(80);
        assert!(app.is_zen_forced());
        assert!(app.is_zen_mode());

        // `z` is a no-op while forced.
        let applied = app.toggle_zen();
        assert!(!applied, "toggle_zen should report no change while forced");
        assert!(app.is_zen_mode(), "still in zen mode (forced)");
        assert!(app.is_zen_forced());
    }

    #[test]
    fn toggle_zen_resumes_after_terminal_grows_back() {
        let mut app = make_app(3, 0);
        // Force, then expand back above threshold.
        app.update_auto_zen(80);
        app.update_auto_zen(120);
        assert!(!app.is_zen_forced());
        assert!(!app.is_zen_mode());

        // User can toggle freely again.
        assert!(app.toggle_zen());
        assert!(app.is_zen_mode());
    }

    // -- Auto-zen threshold -------------------------------------------------

    #[test]
    fn auto_zen_triggers_below_threshold() {
        let mut app = make_app(3, 0);
        let toast = app.update_auto_zen(AUTO_ZEN_WIDTH_THRESHOLD - 1);
        assert!(app.is_zen_forced());
        assert!(app.is_zen_mode());
        assert_eq!(toast, Some(AUTO_ZEN_TOAST));
    }

    #[test]
    fn auto_zen_does_not_trigger_at_threshold() {
        let mut app = make_app(3, 0);
        let toast = app.update_auto_zen(AUTO_ZEN_WIDTH_THRESHOLD);
        assert!(!app.is_zen_forced());
        assert_eq!(toast, None);
    }

    #[test]
    fn auto_zen_does_not_trigger_above_threshold() {
        let mut app = make_app(3, 0);
        let toast = app.update_auto_zen(120);
        assert!(!app.is_zen_forced());
        assert_eq!(toast, None);
    }

    #[test]
    fn auto_zen_releases_when_terminal_grows_back() {
        let mut app = make_app(3, 0);
        app.update_auto_zen(80);
        assert!(app.is_zen_forced());
        let toast = app.update_auto_zen(120);
        assert!(!app.is_zen_forced());
        // No second toast — we already reported the cross-over.
        assert_eq!(toast, None);
    }

    #[test]
    fn auto_zen_toast_only_shows_once_per_view_instance() {
        let mut app = make_app(3, 0);
        // First sub-threshold render: toast.
        assert_eq!(
            app.update_auto_zen(AUTO_ZEN_WIDTH_THRESHOLD - 1),
            Some(AUTO_ZEN_TOAST)
        );
        // Grow back, shrink again — no second toast.
        assert_eq!(app.update_auto_zen(120), None);
        assert_eq!(
            app.update_auto_zen(AUTO_ZEN_WIDTH_THRESHOLD - 1),
            None
        );
    }

    #[test]
    fn auto_zen_overrides_user_zen_off() {
        // User toggled zen off (default), but terminal forces it on. The
        // composite `is_zen_mode` should still report true.
        let mut app = make_app(3, 0);
        assert!(!app.user_zen);
        app.update_auto_zen(50);
        assert!(app.is_zen_mode());
    }

    #[test]
    fn observe_terminal_width_pushes_toast() {
        let mut app = make_app(3, 0);
        let now = Instant::now();
        app.observe_terminal_width(50, now);
        assert_eq!(app.toasts.len(), 1);
        assert_eq!(app.toasts.current().unwrap().text, AUTO_ZEN_TOAST);
    }

    #[test]
    fn observe_terminal_width_idempotent_across_redraws() {
        let mut app = make_app(3, 0);
        let now = Instant::now();
        app.observe_terminal_width(50, now);
        app.observe_terminal_width(50, now);
        // Toast pushed exactly once even across multiple redraws.
        assert_eq!(app.toasts.len(), 1);
    }

    // -- Pop ----------------------------------------------------------------

    #[test]
    fn request_pop_sets_flag() {
        let mut app = make_app(3, 0);
        assert!(!app.should_pop);
        app.request_pop();
        assert!(app.should_pop);
    }

    // -- Step accessors -----------------------------------------------------

    #[test]
    fn current_step_returns_selected() {
        let app = make_app(3, 1);
        assert_eq!(app.current_step().map(|s| s.id.as_str()), Some("s1"));
        assert_eq!(app.current_step_number(), Some(2));
    }

    #[test]
    fn current_step_clamps_into_range() {
        // Construction should clamp an out-of-range selected_step_index.
        let app = make_app(3, 99);
        assert_eq!(app.selected_step_index, 2);
        assert_eq!(app.current_step().map(|s| s.id.as_str()), Some("s2"));
    }

    #[test]
    fn current_step_none_when_empty() {
        let app = make_app(0, 0);
        assert!(app.current_step().is_none());
        assert_eq!(app.current_step_number(), None);
    }

    #[test]
    fn breadcrumb_step_segment_includes_number_and_title() {
        let app = make_app(3, 1);
        assert_eq!(app.breadcrumb_step_segment(), "step 2: Step 2");
    }

    #[test]
    fn breadcrumb_step_segment_when_no_steps() {
        let app = make_app(0, 0);
        assert_eq!(app.breadcrumb_step_segment(), "(no steps)");
    }

    // -- Rendering smoke tests ----------------------------------------------

    #[test]
    fn draw_renders_breadcrumb_with_step_segment() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = make_app(3, 1);
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let top = (0..buffer.area().width)
            .map(|x| buffer[(x, 0)].symbol())
            .collect::<String>();
        assert!(
            top.contains("ralph › tui-v1 › step 2: Step 2"),
            "expected breadcrumb with step segment: {top:?}"
        );
    }

    #[test]
    fn draw_below_threshold_pushes_auto_zen_toast() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = make_app(3, 0);
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        assert!(app.is_zen_forced());
        // The auto-zen toast was pushed and is still current (TTL is 3s).
        assert_eq!(
            app.toasts.current().map(|t| t.text.as_str()),
            Some(AUTO_ZEN_TOAST)
        );
    }

    #[test]
    fn draw_above_threshold_does_not_force_zen() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = make_app(3, 0);
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        assert!(!app.is_zen_forced());
        assert!(!app.is_zen_mode());
        assert!(app.toasts.is_empty());
    }

    #[test]
    fn draw_does_not_panic_on_tiny_terminal() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(10, 6);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = make_app(3, 0);
        terminal.draw(|f| draw(f, &mut app)).unwrap();
    }

    // -- Read-only pane content (TUI-plan.md §8) ---------------------------

    /// Concatenate every cell of the rendered buffer into one big string so
    /// tests can assert that a given substring appears anywhere on the
    /// screen. The TestBackend buffer is row-major, but `Buffer::content()`
    /// already iterates in render order, so we just join symbols.
    fn render_to_string(width: u16, height: u16, app: &mut StepDetailApp) -> String {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, app)).unwrap();
        let buf = terminal.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buf.area().height {
            for x in 0..buf.area().width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn universal_pane_renders_config_prefix_and_suffix() {
        let plan = make_plan();
        let steps = make_steps(3);
        let config = Config {
            prompt_prefix: Some("CFG-PREFIX-MARKER".to_string()),
            prompt_suffix: Some("CFG-SUFFIX-MARKER".to_string()),
            ..Config::default()
        };
        let mut app =
            StepDetailApp::new(plan, steps, 0, &config, ProjectSettings::default(), Vec::new());
        let screen = render_to_string(140, 60, &mut app);
        assert!(screen.contains("Universal prompt"), "{screen}");
        assert!(screen.contains("CFG-PREFIX-MARKER"), "{screen}");
        assert!(screen.contains("CFG-SUFFIX-MARKER"), "{screen}");
    }

    #[test]
    fn universal_pane_shows_none_when_unset() {
        let plan = make_plan();
        let steps = make_steps(1);
        // Default Config has a global prefix seeded by `ralph init`; clear
        // both fields so the pane shows the (none) placeholder.
        let config = Config {
            prompt_prefix: None,
            prompt_suffix: None,
            ..Config::default()
        };
        let mut app =
            StepDetailApp::new(plan, steps, 0, &config, ProjectSettings::default(), Vec::new());
        let screen = render_to_string(140, 60, &mut app);
        // The (none) placeholder must appear at least once — Universal is
        // the first pane in the stack, so confirming any (none) is present
        // is sufficient evidence the placeholder branch is reachable.
        assert!(
            screen.contains(NONE_PLACEHOLDER),
            "expected (none) placeholder somewhere: {screen}"
        );
    }

    #[test]
    fn project_pane_renders_project_settings() {
        let plan = make_plan();
        let steps = make_steps(2);
        let project_settings = ProjectSettings {
            prompt_prefix: Some("PROJ-PRE-MARK".to_string()),
            prompt_suffix: Some("PROJ-SUF-MARK".to_string()),
        };
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            project_settings,
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        assert!(screen.contains("Project prompt"), "{screen}");
        assert!(screen.contains("PROJ-PRE-MARK"), "{screen}");
        assert!(screen.contains("PROJ-SUF-MARK"), "{screen}");
    }

    #[test]
    fn plan_context_prepend_pane_uses_default_when_none() {
        // When plan.context_prepend is None, the pane renders
        // DEFAULT_CONTEXT_PREPEND verbatim (the same string the runner injects
        // into every step prompt).
        let mut plan = make_plan();
        plan.context_prepend = None;
        let steps = make_steps(1);
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        // The default prepend opens with "# Ralph context" — distinctive
        // enough to confirm the fallback wired up correctly.
        assert!(screen.contains("Ralph context"), "{screen}");
    }

    #[test]
    fn plan_context_prepend_pane_uses_override() {
        let mut plan = make_plan();
        plan.context_prepend = Some("OVERRIDE-PREPEND-MARKER".to_string());
        let steps = make_steps(1);
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        assert!(screen.contains("OVERRIDE-PREPEND-MARKER"), "{screen}");
        // The default's "Ralph context" header must NOT leak into the pane
        // when an override is set (verifies we don't render *both*).
        assert!(
            !screen.contains("Ralph context"),
            "override must replace default, got: {screen}"
        );
    }

    #[test]
    fn plan_prompt_pane_renders_plan_wraps() {
        let mut plan = make_plan();
        plan.prompt_prefix = Some("PLAN-PRE-MARK".to_string());
        plan.prompt_suffix = Some("PLAN-SUF-MARK".to_string());
        let mut app = StepDetailApp::new(
            plan,
            make_steps(1),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        assert!(screen.contains("Plan prompt"), "{screen}");
        assert!(screen.contains("PLAN-PRE-MARK"), "{screen}");
        assert!(screen.contains("PLAN-SUF-MARK"), "{screen}");
    }

    #[test]
    fn step_prompt_pane_renders_title_description_and_criteria() {
        let plan = make_plan();
        let mut steps = make_steps(2);
        steps[1].title = "STEP-TITLE-MARK".to_string();
        steps[1].description = "STEP-DESC-MARK".to_string();
        steps[1].acceptance_criteria = vec!["CRIT-A-MARK".into(), "CRIT-B-MARK".into()];
        let mut app = StepDetailApp::new(
            plan,
            steps,
            1,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(160, 80, &mut app);
        assert!(screen.contains("STEP-TITLE-MARK"), "{screen}");
        assert!(screen.contains("STEP-DESC-MARK"), "{screen}");
        assert!(screen.contains("CRIT-A-MARK"), "{screen}");
        assert!(screen.contains("CRIT-B-MARK"), "{screen}");
        assert!(screen.contains("Acceptance:"), "{screen}");
    }

    #[test]
    fn tests_pane_renders_deterministic_tests_as_bullets() {
        let mut plan = make_plan();
        plan.deterministic_tests = vec!["cargo test".into(), "cargo clippy".into()];
        let mut app = StepDetailApp::new(
            plan,
            make_steps(1),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        assert!(screen.contains("• cargo test"), "{screen}");
        assert!(screen.contains("• cargo clippy"), "{screen}");
    }

    #[test]
    fn tests_pane_shows_none_when_no_tests() {
        let mut plan = make_plan();
        plan.deterministic_tests.clear();
        let mut app = StepDetailApp::new(
            plan,
            make_steps(1),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(140, 60, &mut app);
        // The (none) placeholder appears in several panes — search the
        // pane stack between "Tests" and the bottom row to confirm the
        // tests pane itself rendered the placeholder.
        let tests_idx = screen.find("Tests").expect("tests pane title rendered");
        let after_tests = &screen[tests_idx..];
        let bottom_idx = after_tests
            .find("Harness")
            .expect("bottom-row title rendered");
        let tests_body = &after_tests[..bottom_idx];
        assert!(
            tests_body.contains(NONE_PLACEHOLDER),
            "expected (none) within tests pane body: {tests_body}"
        );
    }

    // -- Bottom-row resolved values ----------------------------------------

    #[test]
    fn effective_harness_falls_back_to_plan_then_config() {
        let mut plan = make_plan();
        plan.harness = Some("plan-harness".to_string());
        let mut steps = make_steps(2);
        // Step 0 has no harness override → falls back to plan.
        // Step 1 overrides → wins.
        steps[1].harness = Some("step-harness".to_string());

        let app = StepDetailApp::new(
            plan.clone(),
            steps.clone(),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_harness(), "plan-harness");

        let app = StepDetailApp::new(
            plan,
            steps,
            1,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_harness(), "step-harness");
    }

    #[test]
    fn effective_harness_uses_default_when_neither_step_nor_plan_set() {
        let mut plan = make_plan();
        plan.harness = None;
        let app = StepDetailApp::new(
            plan,
            make_steps(1),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_harness(), Config::default().default_harness);
    }

    #[test]
    fn effective_model_prefers_step_then_harness_default() {
        let mut config = Config::default();
        // Give the default harness a configured default model so the
        // fallback path is observable.
        config
            .harnesses
            .get_mut(&config.default_harness.clone())
            .unwrap()
            .default_model = Some("default-model".to_string());

        // Step has no model override → fallback to harness default.
        let plan = make_plan();
        let app = StepDetailApp::new(
            plan.clone(),
            make_steps(1),
            0,
            &config,
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_model().as_deref(), Some("default-model"));

        // Step overrides → wins.
        let mut steps = make_steps(1);
        steps[0].model = Some("step-model".to_string());
        let app = StepDetailApp::new(
            plan,
            steps,
            0,
            &config,
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_model().as_deref(), Some("step-model"));
    }

    #[test]
    fn effective_agent_falls_back_from_step_to_plan() {
        let mut plan = make_plan();
        plan.agent = Some("plan-agent".to_string());
        let mut steps = make_steps(2);
        steps[1].agent = Some("step-agent".to_string());

        let app = StepDetailApp::new(
            plan.clone(),
            steps.clone(),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_agent().as_deref(), Some("plan-agent"));

        let app = StepDetailApp::new(
            plan,
            steps,
            1,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_agent().as_deref(), Some("step-agent"));
    }

    #[test]
    fn effective_change_policy_returns_step_value() {
        let plan = make_plan();
        let mut steps = make_steps(1);
        steps[0].change_policy = ChangePolicy::Optional;
        let app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        assert_eq!(app.effective_change_policy(), ChangePolicy::Optional);
    }

    #[test]
    fn bottom_row_renders_all_four_cells() {
        let mut plan = make_plan();
        plan.harness = Some("plan-harness".to_string());
        plan.agent = Some("plan-agent".to_string());
        let mut steps = make_steps(1);
        steps[0].model = Some("BOTTOM-MODEL".to_string());
        steps[0].change_policy = ChangePolicy::Optional;

        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(160, 80, &mut app);
        // All four cell labels must appear, with their resolved values.
        assert!(screen.contains("Harness:"), "{screen}");
        assert!(screen.contains("plan-harness"), "{screen}");
        assert!(screen.contains("Model:"), "{screen}");
        assert!(screen.contains("BOTTOM-MODEL"), "{screen}");
        assert!(screen.contains("Agent:"), "{screen}");
        assert!(screen.contains("plan-agent"), "{screen}");
        assert!(screen.contains("Change policy:"), "{screen}");
        assert!(screen.contains("optional"), "{screen}");
    }

    // -- Appended pane pagination (TUI-plan.md §8) -------------------------

    #[test]
    fn appended_default_index_is_most_recent_attempt() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let app = make_app_with_logs(1, 0, logs);
        // Default is the last index (newest) per §8 "most recent attempt by default".
        assert_eq!(app.appended_attempt_index, 2);
        assert_eq!(app.current_appended_log().map(|l| l.attempt), Some(3));
    }

    #[test]
    fn appended_default_index_is_zero_when_no_logs() {
        let app = make_app_with_logs(1, 0, Vec::new());
        // saturating_sub keeps index at 0 with no logs; current_appended_log
        // is None.
        assert_eq!(app.appended_attempt_index, 0);
        assert!(app.current_appended_log().is_none());
    }

    #[test]
    fn appended_at_leftmost_is_true_when_index_zero() {
        let app = make_app_with_logs(1, 0, vec![make_log(1, None, None)]);
        assert!(app.appended_at_leftmost());
    }

    #[test]
    fn appended_at_leftmost_is_true_with_empty_logs() {
        // Empty logs is treated as already at the leftmost so `h` falls
        // through to popping the view (there's nothing to paginate).
        let app = make_app_with_logs(1, 0, Vec::new());
        assert!(app.appended_at_leftmost());
    }

    #[test]
    fn appended_at_rightmost_is_true_at_last_index_and_when_empty() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let app = make_app_with_logs(1, 0, logs);
        // Default lands on the rightmost (newest).
        assert!(app.appended_at_rightmost());

        let app = make_app_with_logs(1, 0, Vec::new());
        assert!(app.appended_at_rightmost());
    }

    #[test]
    fn appended_prev_decrements_and_returns_true() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        assert_eq!(app.appended_attempt_index, 2);
        assert!(app.appended_prev());
        assert_eq!(app.appended_attempt_index, 1);
        assert!(app.appended_prev());
        assert_eq!(app.appended_attempt_index, 0);
        // Already at leftmost — returns false, index unchanged.
        assert!(!app.appended_prev());
        assert_eq!(app.appended_attempt_index, 0);
    }

    #[test]
    fn appended_prev_returns_false_with_no_logs() {
        let mut app = make_app_with_logs(1, 0, Vec::new());
        assert!(!app.appended_prev());
    }

    #[test]
    fn appended_next_increments_and_returns_true() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        // Walk back to attempt 1, then advance forward.
        app.appended_attempt_index = 0;
        assert!(app.appended_next());
        assert_eq!(app.appended_attempt_index, 1);
        assert!(app.appended_next());
        assert_eq!(app.appended_attempt_index, 2);
        // Already at rightmost — returns false.
        assert!(!app.appended_next());
        assert_eq!(app.appended_attempt_index, 2);
    }

    #[test]
    fn appended_next_returns_false_with_no_logs() {
        let mut app = make_app_with_logs(1, 0, Vec::new());
        assert!(!app.appended_next());
    }

    #[test]
    fn handle_left_pops_for_non_appended_pane_regardless_of_logs() {
        // Even with paginatable logs, h on a non-Appended pane just pops.
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::StepPrompt;
        app.handle_left();
        assert!(app.should_pop);
    }

    #[test]
    fn handle_left_paginates_when_appended_focused_and_not_leftmost() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::Appended;
        // Default index is rightmost (2). h moves to 1 without popping.
        app.handle_left();
        assert_eq!(app.appended_attempt_index, 1);
        assert!(!app.should_pop);

        // h again moves to leftmost (0), still no pop.
        app.handle_left();
        assert_eq!(app.appended_attempt_index, 0);
        assert!(!app.should_pop);
    }

    #[test]
    fn handle_left_pops_when_appended_at_leftmost() {
        // From the leftmost (oldest) attempt, h pops the view per §8's
        // explicit "back to plan-detail" special case.
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::Appended;
        app.appended_attempt_index = 0;
        app.handle_left();
        assert_eq!(app.appended_attempt_index, 0, "still at leftmost");
        assert!(app.should_pop);
    }

    #[test]
    fn handle_left_pops_when_appended_focused_with_no_logs() {
        // Empty logs is treated as already-at-leftmost — h pops.
        let mut app = make_app_with_logs(1, 0, Vec::new());
        app.focused_pane = Pane::Appended;
        app.handle_left();
        assert!(app.should_pop);
    }

    #[test]
    fn handle_left_pops_when_appended_focused_with_single_attempt() {
        // Single attempt ⇒ also at leftmost; h pops.
        let mut app = make_app_with_logs(1, 0, vec![make_log(1, None, None)]);
        app.focused_pane = Pane::Appended;
        assert!(app.appended_at_leftmost());
        app.handle_left();
        assert!(app.should_pop);
    }

    #[test]
    fn handle_right_paginates_when_appended_focused_and_not_rightmost() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::Appended;
        app.appended_attempt_index = 0;
        app.handle_right();
        assert_eq!(app.appended_attempt_index, 1);
        assert!(!app.should_pop);

        app.handle_right();
        assert_eq!(app.appended_attempt_index, 2);
        assert!(!app.should_pop);
    }

    #[test]
    fn handle_right_no_op_at_rightmost() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::Appended;
        // Default index is rightmost.
        app.handle_right();
        assert_eq!(app.appended_attempt_index, 1);
        assert!(!app.should_pop);
    }

    #[test]
    fn handle_right_no_op_for_non_appended_pane() {
        // l on any other pane is a no-op (TUI-plan.md §8 keys table).
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        app.focused_pane = Pane::StepPrompt;
        app.appended_attempt_index = 0;
        app.handle_right();
        assert_eq!(
            app.appended_attempt_index, 0,
            "non-Appended l must not paginate"
        );
        assert!(!app.should_pop);
    }

    #[test]
    fn handle_right_no_op_with_no_logs() {
        let mut app = make_app_with_logs(1, 0, Vec::new());
        app.focused_pane = Pane::Appended;
        app.handle_right();
        assert!(!app.should_pop);
    }

    // -- Appended pane title -----------------------------------------------

    #[test]
    fn appended_pane_title_includes_attempt_count() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None), make_log(3, None, None)];
        let app = make_app_with_logs(1, 0, logs);
        // Default is rightmost ⇒ attempt 3/3.
        assert_eq!(app.appended_pane_title(), "Appended (attempt 3/3)");
    }

    #[test]
    fn appended_pane_title_changes_with_pagination() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        assert_eq!(app.appended_pane_title(), "Appended (attempt 2/2)");
        app.appended_prev();
        assert_eq!(app.appended_pane_title(), "Appended (attempt 1/2)");
    }

    #[test]
    fn appended_pane_title_falls_back_when_no_logs() {
        let app = make_app_with_logs(1, 0, Vec::new());
        // No attempts yet ⇒ bare title.
        assert_eq!(app.appended_pane_title(), "Appended");
    }

    // -- Appended pane render ----------------------------------------------

    #[test]
    fn appended_pane_renders_dynamic_title_when_attempts_exist() {
        let logs = vec![make_log(1, None, None), make_log(2, None, None)];
        let mut app = make_app_with_logs(1, 0, logs);
        let screen = render_to_string(160, 80, &mut app);
        assert!(
            screen.contains("Appended (attempt 2/2)"),
            "expected dynamic title, got: {screen}"
        );
    }

    #[test]
    fn appended_pane_renders_first_attempt_placeholder() {
        // On attempt 1 there is no preceding attempt to source retry context
        // from — the body must be an explicit dim placeholder, not a panic.
        let logs = vec![make_log(1, Some("ignored"), Some("ignored"))];
        let mut app = make_app_with_logs(1, 0, logs);
        let screen = render_to_string(160, 80, &mut app);
        assert!(
            screen.contains("first attempt"),
            "expected first-attempt placeholder, got: {screen}"
        );
    }

    #[test]
    fn appended_pane_renders_previous_diff_and_test_output() {
        let logs = vec![
            make_log(1, Some("DIFF-MARK"), Some("TEST-MARK")),
            make_log(2, None, None),
        ];
        let mut app = make_app_with_logs(1, 0, logs);
        // Default lands on attempt 2 — its "appended" content is sourced
        // from attempt 1's diff and test output.
        let screen = render_to_string(160, 80, &mut app);
        assert!(screen.contains("Previous diff:"), "{screen}");
        assert!(screen.contains("DIFF-MARK"), "{screen}");
        assert!(screen.contains("Previous test output:"), "{screen}");
        assert!(screen.contains("TEST-MARK"), "{screen}");
    }

    #[test]
    fn bottom_row_renders_em_dash_for_empty_agent_and_model() {
        // No step.model, no harness default model, no agent anywhere — the
        // Model and Agent cells must show the EMPTY_CELL sentinel.
        let mut config = Config::default();
        for hc in config.harnesses.values_mut() {
            hc.default_model = None;
        }
        let mut plan = make_plan();
        plan.agent = None;
        let mut steps = make_steps(1);
        steps[0].agent = None;
        steps[0].model = None;

        let mut app =
            StepDetailApp::new(plan, steps, 0, &config, ProjectSettings::default(), Vec::new());
        let screen = render_to_string(160, 80, &mut app);
        assert!(
            screen.contains(EMPTY_CELL),
            "expected em-dash for empty cell: {screen}"
        );
    }

    // -- format_wrap_pane / parse_wrap_pane (TUI-plan.md §8 + §18 Q3) -----

    #[test]
    fn format_wrap_pane_emits_both_headers_when_both_set() {
        let s = format_wrap_pane(Some("hello"), Some("world"));
        assert!(s.contains("## Prefix\nhello"), "got: {s}");
        assert!(s.contains("## Suffix\nworld"), "got: {s}");
    }

    #[test]
    fn format_wrap_pane_emits_both_headers_when_both_none() {
        let s = format_wrap_pane(None, None);
        assert!(s.contains("## Prefix"), "got: {s}");
        assert!(s.contains("## Suffix"), "got: {s}");
        // Bodies are empty — no spurious content slips between headers.
        let (p, q) = parse_wrap_pane(&s);
        assert_eq!(p, None);
        assert_eq!(q, None);
    }

    #[test]
    fn parse_wrap_pane_round_trips_when_both_set() {
        let s = format_wrap_pane(Some("a\nb"), Some("c"));
        let (p, q) = parse_wrap_pane(&s);
        assert_eq!(p.as_deref(), Some("a\nb"));
        assert_eq!(q.as_deref(), Some("c"));
    }

    #[test]
    fn parse_wrap_pane_round_trips_with_one_side_unset() {
        let s = format_wrap_pane(Some("only-prefix"), None);
        let (p, q) = parse_wrap_pane(&s);
        assert_eq!(p.as_deref(), Some("only-prefix"));
        assert_eq!(q, None);

        let s = format_wrap_pane(None, Some("only-suffix"));
        let (p, q) = parse_wrap_pane(&s);
        assert_eq!(p, None);
        assert_eq!(q.as_deref(), Some("only-suffix"));
    }

    #[test]
    fn parse_wrap_pane_treats_whitespace_only_section_as_none() {
        let text = "## Prefix\n   \n\n## Suffix\n\n";
        let (p, q) = parse_wrap_pane(text);
        assert_eq!(p, None);
        assert_eq!(q, None);
    }

    #[test]
    fn parse_wrap_pane_drops_text_before_first_header() {
        // Anything written above the first `## Prefix` is treated as a
        // free-form comment by the user and discarded — the editor file is
        // not a general-purpose markdown document.
        let text = "stray noise\n## Prefix\nbody\n## Suffix\nsuf\n";
        let (p, q) = parse_wrap_pane(text);
        assert_eq!(p.as_deref(), Some("body"));
        assert_eq!(q.as_deref(), Some("suf"));
    }

    #[test]
    fn parse_wrap_pane_handles_missing_suffix_section() {
        // A user could delete the suffix header entirely; parser treats the
        // missing section as `None` rather than panicking.
        let text = "## Prefix\nbody\n";
        let (p, q) = parse_wrap_pane(text);
        assert_eq!(p.as_deref(), Some("body"));
        assert_eq!(q, None);
    }

    // -- edit_universal_pane ----------------------------------------------

    /// Helper: closure-based fake editor that returns a fixed result without
    /// shelling out. The closure receives the initial buffer (so tests can
    /// assert on what the editor would have seen) and returns the value the
    /// editor "saved" — `None` simulates the no-editor / non-zero-exit path.
    fn fake_editor(returning: Option<String>) -> impl FnOnce(&str) -> Result<Option<String>> {
        move |_initial| Ok(returning)
    }

    /// Helper: closure that records the initial buffer it was given for the
    /// caller to inspect. Returns `None` to simulate a missing editor so
    /// the handler short-circuits without touching the source of truth.
    fn capturing_editor(
        seen: std::rc::Rc<std::cell::RefCell<Option<String>>>,
    ) -> impl FnOnce(&str) -> Result<Option<String>> {
        move |initial| {
            *seen.borrow_mut() = Some(initial.to_string());
            Ok(None)
        }
    }

    fn make_step_app() -> StepDetailApp {
        StepDetailApp::new(
            make_plan(),
            make_steps(1),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        )
    }

    #[test]
    fn edit_universal_pane_seeds_editor_with_current_pair() {
        let mut config = Config {
            prompt_prefix: Some("PRE".to_string()),
            prompt_suffix: Some("SUF".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();

        let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
        let mut app = make_step_app();
        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), capturing_editor(seen.clone()))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);

        let buf = seen.borrow().clone().expect("editor was invoked");
        assert!(buf.contains("## Prefix\nPRE\n"), "got: {buf}");
        assert!(buf.contains("## Suffix\nSUF\n"), "got: {buf}");
    }

    #[test]
    fn edit_universal_pane_returns_no_editor_when_closure_returns_none() {
        let mut config = Config::default();
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();
        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), fake_editor(None))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
        // Nothing was persisted.
        assert!(
            !tmp.path().join("config.json").exists(),
            "config.json must not be written on no-editor"
        );
    }

    #[test]
    fn edit_universal_pane_returns_no_changes_when_buffer_unchanged() {
        let mut config = Config {
            prompt_prefix: Some("hello".to_string()),
            prompt_suffix: None,
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let initial = format_wrap_pane(
            config.prompt_prefix.as_deref(),
            config.prompt_suffix.as_deref(),
        );
        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), fake_editor(Some(initial)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
        // Still didn't persist (nothing changed).
        assert!(
            !tmp.path().join("config.json").exists(),
            "no-changes path must not write the config"
        );
        // Config struct unchanged.
        assert_eq!(config.prompt_prefix.as_deref(), Some("hello"));
        assert_eq!(config.prompt_suffix, None);
    }

    #[test]
    fn edit_universal_pane_persists_on_change() {
        let mut config = Config {
            prompt_prefix: Some("old-pre".to_string()),
            prompt_suffix: Some("old-suf".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let new_text = format_wrap_pane(Some("new-pre"), Some("new-suf"));
        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), fake_editor(Some(new_text)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // In-memory config and app mirrors both updated.
        assert_eq!(config.prompt_prefix.as_deref(), Some("new-pre"));
        assert_eq!(config.prompt_suffix.as_deref(), Some("new-suf"));
        assert_eq!(app.config_prompt_prefix.as_deref(), Some("new-pre"));
        assert_eq!(app.config_prompt_suffix.as_deref(), Some("new-suf"));

        // File written and parses back to the new values.
        let path = tmp.path().join("config.json");
        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        let reloaded: Config = serde_json::from_str(&contents).unwrap();
        assert_eq!(reloaded.prompt_prefix.as_deref(), Some("new-pre"));
        assert_eq!(reloaded.prompt_suffix.as_deref(), Some("new-suf"));
    }

    #[test]
    fn edit_universal_pane_can_clear_both_fields() {
        // Saving a buffer with empty bodies clears prefix and suffix to
        // `None` — this is the "wipe my universal prompt" workflow.
        let mut config = Config {
            prompt_prefix: Some("X".to_string()),
            prompt_suffix: Some("Y".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let cleared = "## Prefix\n\n## Suffix\n".to_string();
        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), fake_editor(Some(cleared)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert_eq!(config.prompt_prefix, None);
        assert_eq!(config.prompt_suffix, None);
    }

    #[cfg(unix)]
    #[test]
    fn edit_universal_pane_round_trips_through_mock_editor_script() {
        // Acceptance test from the step description: shell out to a real
        // mock $EDITOR script via `tui::editor::edit_at`, then verify the
        // change persists into config.json.
        use crate::tui::editor::edit_at;
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("ed.sh");
        // Editor writes a known prefix/suffix pair, ignoring the seeded
        // file contents.
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '## Prefix\\nMOCK-PRE\\n\\n## Suffix\\nMOCK-SUF\\n' > \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut config = Config {
            prompt_prefix: Some("before-pre".to_string()),
            prompt_suffix: Some("before-suf".to_string()),
            ..Config::default()
        };
        let mut app = make_step_app();

        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), |initial| {
                edit_at(
                    script.to_str().unwrap(),
                    &tmp.path().join("buf.md"),
                    initial,
                )
            })
            .unwrap();

        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded: Config = serde_json::from_str(
            &std::fs::read_to_string(tmp.path().join("config.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(reloaded.prompt_prefix.as_deref(), Some("MOCK-PRE"));
        assert_eq!(reloaded.prompt_suffix.as_deref(), Some("MOCK-SUF"));
    }

    // -- edit_project_pane ------------------------------------------------

    /// Build a StepDetailApp pointed at a fresh in-memory plan row in `conn`.
    /// The DB state and the in-memory `app.plan` / `app.project_settings`
    /// match at construction time so writes can be verified by reading the
    /// row back.
    fn setup_project_app(conn: &Connection, project: &str) -> StepDetailApp {
        let plan = crate::storage::create_plan(
            conn,
            "tui-v1",
            project,
            "tui-v1",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        StepDetailApp::new(
            plan,
            Vec::new(),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        )
    }

    #[test]
    fn edit_project_pane_no_editor_short_circuits() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app.edit_project_pane(&conn, fake_editor(None)).unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
        // Nothing should be inserted into project_settings.
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt_prefix, None);
        assert_eq!(stored.prompt_suffix, None);
    }

    #[test]
    fn edit_project_pane_no_changes_skips_writes() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.project_settings.prompt_prefix = Some("a".to_string());
        let buffer = format_wrap_pane(Some("a"), None);
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
        // No row written; storage still reflects the absence.
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt_prefix, None);
        assert_eq!(stored.prompt_suffix, None);
    }

    #[test]
    fn edit_project_pane_persists_changed_pair() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let buffer = format_wrap_pane(Some("hello"), Some("world"));
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // Both halves landed in the project_settings row.
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt_prefix.as_deref(), Some("hello"));
        assert_eq!(stored.prompt_suffix.as_deref(), Some("world"));
        // App's own mirror is in sync so the pane re-renders correctly.
        assert_eq!(app.project_settings.prompt_prefix.as_deref(), Some("hello"));
        assert_eq!(app.project_settings.prompt_suffix.as_deref(), Some("world"));
    }

    #[test]
    fn edit_project_pane_can_clear_only_one_side() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        // Seed both sides via the storage helpers so the row exists.
        storage::set_project_prompt_prefix(&conn, "/proj", Some("PRE")).unwrap();
        storage::set_project_prompt_suffix(&conn, "/proj", Some("SUF")).unwrap();
        app.project_settings.prompt_prefix = Some("PRE".to_string());
        app.project_settings.prompt_suffix = Some("SUF".to_string());

        // User clears just the suffix — prefix must be untouched.
        let buffer = format_wrap_pane(Some("PRE"), None);
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt_prefix.as_deref(), Some("PRE"));
        assert_eq!(stored.prompt_suffix, None);
    }

    // -- edit_plan_context_prepend_pane -----------------------------------

    #[test]
    fn edit_plan_context_prepend_no_editor_short_circuits() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_plan_context_prepend_pane(&conn, fake_editor(None))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
    }

    #[test]
    fn edit_plan_context_prepend_seeds_default_when_unset() {
        // When `plan.context_prepend` is None, the editor sees the system
        // DEFAULT_CONTEXT_PREPEND so the user can edit-from-default.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        assert!(app.plan.context_prepend.is_none());

        let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
        let _ = app
            .edit_plan_context_prepend_pane(&conn, capturing_editor(seen.clone()))
            .unwrap();
        let buf = seen.borrow().clone().expect("editor was invoked");
        assert_eq!(buf, DEFAULT_CONTEXT_PREPEND);
    }

    #[test]
    fn edit_plan_context_prepend_no_op_on_default_keeps_override_none() {
        // Closing the editor without changes when override was None should
        // leave context_prepend as None (not pin it to the default's text).
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_plan_context_prepend_pane(
                &conn,
                fake_editor(Some(DEFAULT_CONTEXT_PREPEND.to_string())),
            )
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.context_prepend, None);
        assert_eq!(app.plan.context_prepend, None);
    }

    #[test]
    fn edit_plan_context_prepend_normalizes_trailing_newline() {
        // Many editors append a trailing newline on save — a no-op edit
        // should still be NoChanges, not Saved.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.plan.context_prepend = Some("snippet".to_string());
        storage::set_plan_context_prepend(&conn, &app.plan.id, Some("snippet")).unwrap();

        let outcome = app
            .edit_plan_context_prepend_pane(&conn, fake_editor(Some("snippet\n".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
    }

    #[test]
    fn edit_plan_context_prepend_persists_changed_value() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_plan_context_prepend_pane(&conn, fake_editor(Some("CUSTOM-PREPEND".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.context_prepend.as_deref(), Some("CUSTOM-PREPEND"));
        assert_eq!(app.plan.context_prepend.as_deref(), Some("CUSTOM-PREPEND"));
    }

    #[test]
    fn edit_plan_context_prepend_persists_empty_string_escape_hatch() {
        // The empty-string override is the documented "no prepend at all"
        // escape hatch (see Plan::context_prepend doc) and must round-trip
        // through the c-handoff distinct from None.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_plan_context_prepend_pane(&conn, fake_editor(Some(String::new())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.context_prepend.as_deref(), Some(""));
    }

    #[cfg(unix)]
    #[test]
    fn edit_plan_context_prepend_with_mock_editor_script() {
        // Acceptance test: shell out to a real mock $EDITOR via the lower
        // `edit_at` helper and verify the new value lands in the plans row.
        use crate::tui::editor::edit_at;
        use std::os::unix::fs::PermissionsExt;

        let conn = crate::db::open_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("ed.sh");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf 'SCRIPT-WROTE-THIS\\n' > \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_plan_context_prepend_pane(&conn, |initial| {
                edit_at(
                    script.to_str().unwrap(),
                    &tmp.path().join("buf.md"),
                    initial,
                )
            })
            .unwrap();

        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.context_prepend.as_deref(),
            Some("SCRIPT-WROTE-THIS")
        );
    }

    // -- Step-prompt pane format/parse round-trips ------------------------

    #[test]
    fn parse_step_pane_round_trips_full_step() {
        let formatted = format_step_pane(
            "My step title",
            "First desc line\n\nSecond desc paragraph",
            &["Criterion A".to_string(), "Criterion B".to_string()],
        );
        let parts = parse_step_pane(&formatted).unwrap();
        assert_eq!(parts.title, "My step title");
        assert_eq!(parts.description, "First desc line\n\nSecond desc paragraph");
        assert_eq!(
            parts.acceptance_criteria,
            vec!["Criterion A".to_string(), "Criterion B".to_string()]
        );
    }

    #[test]
    fn parse_step_pane_round_trips_with_no_criteria() {
        // A step legitimately has no acceptance criteria (the existing CLI
        // accepts `--criteria` zero times); the round-trip must preserve
        // that as an empty Vec rather than a phantom one-item list.
        let formatted = format_step_pane("title", "body", &[]);
        let parts = parse_step_pane(&formatted).unwrap();
        assert_eq!(parts.title, "title");
        assert_eq!(parts.description, "body");
        assert!(parts.acceptance_criteria.is_empty());
    }

    #[test]
    fn parse_step_pane_round_trips_with_empty_description() {
        let formatted = format_step_pane("title", "", &["only-crit".to_string()]);
        let parts = parse_step_pane(&formatted).unwrap();
        assert_eq!(parts.title, "title");
        assert_eq!(parts.description, "");
        assert_eq!(parts.acceptance_criteria, vec!["only-crit".to_string()]);
    }

    #[test]
    fn parse_step_pane_accepts_asterisk_bullets() {
        // The renderer always uses `- ` but we accept `* ` on input for
        // editor-paste convenience.
        let text = "# Title\nT\n## Description\n## Acceptance criteria\n* a\n* b\n";
        let parts = parse_step_pane(text).unwrap();
        assert_eq!(
            parts.acceptance_criteria,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn parse_step_pane_trims_trailing_blank_lines_in_description() {
        let text = "# Title\nT\n## Description\n\nbody\n\n\n## Acceptance criteria\n";
        let parts = parse_step_pane(text).unwrap();
        assert_eq!(parts.description, "body");
    }

    #[test]
    fn parse_step_pane_rejects_missing_title_header() {
        let text = "## Description\nbody\n## Acceptance criteria\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("# Title"),
            "expected title-header complaint: {err}"
        );
    }

    #[test]
    fn parse_step_pane_rejects_missing_description_header() {
        let text = "# Title\nT\n## Acceptance criteria\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("## Description"),
            "expected description-header complaint: {err}"
        );
    }

    #[test]
    fn parse_step_pane_rejects_missing_criteria_header() {
        let text = "# Title\nT\n## Description\nbody\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("## Acceptance criteria"),
            "expected criteria-header complaint: {err}"
        );
    }

    #[test]
    fn parse_step_pane_rejects_empty_title() {
        let text = "# Title\n\n## Description\nbody\n## Acceptance criteria\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("Title is empty"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_step_pane_rejects_multi_line_title() {
        let text = "# Title\nline-one\nline-two\n## Description\n## Acceptance criteria\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("single line"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_step_pane_rejects_non_bullet_in_criteria() {
        let text = "# Title\nT\n## Description\n## Acceptance criteria\nstray paragraph\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(
            err.to_string().contains("not a bullet"),
            "got: {err}"
        );
    }

    // -- Tests pane format/parse round-trips -----------------------------

    #[test]
    fn parse_tests_pane_round_trips_simple_list() {
        let formatted = format_tests_pane(&[
            "cargo build".to_string(),
            "cargo test".to_string(),
        ]);
        let parsed = parse_tests_pane(&formatted).unwrap();
        assert_eq!(
            parsed,
            vec!["cargo build".to_string(), "cargo test".to_string()]
        );
    }

    #[test]
    fn parse_tests_pane_round_trips_empty_list() {
        let formatted = format_tests_pane(&[]);
        let parsed = parse_tests_pane(&formatted).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_tests_pane_ignores_blank_and_comment_lines() {
        // Mixed blanks, comments, and real test commands — only the real
        // commands survive in the right order.
        let text = "\
# header comment
cargo build

# inline comment
cargo test
   # indented comment
cargo clippy
";
        let parsed = parse_tests_pane(text).unwrap();
        assert_eq!(
            parsed,
            vec![
                "cargo build".to_string(),
                "cargo test".to_string(),
                "cargo clippy".to_string(),
            ]
        );
    }

    #[test]
    fn parse_tests_pane_trims_each_line() {
        let text = "  cargo build  \n\tcargo test\n";
        let parsed = parse_tests_pane(text).unwrap();
        assert_eq!(
            parsed,
            vec!["cargo build".to_string(), "cargo test".to_string()]
        );
    }

    #[test]
    fn parse_tests_pane_treats_comment_only_as_clear_all() {
        // The user wipes every test by leaving the file with only the help
        // comment header — that should round-trip to an empty list, not
        // an error.
        let text = "# only the help text remains\n";
        let parsed = parse_tests_pane(text).unwrap();
        assert!(parsed.is_empty());
    }

    // -- edit_plan_prompt_pane --------------------------------------------

    #[test]
    fn edit_plan_prompt_no_editor_short_circuits() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app.edit_plan_prompt_pane(&conn, fake_editor(None)).unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
    }

    #[test]
    fn edit_plan_prompt_no_changes_skips_writes() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.plan.prompt_prefix = Some("a".to_string());
        let buffer = format_wrap_pane(Some("a"), None);
        let outcome = app
            .edit_plan_prompt_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
        // DB row still reflects the absence of the suffix and the seeded prefix.
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.prompt_prefix, None);
        assert_eq!(reloaded.prompt_suffix, None);
    }

    #[test]
    fn edit_plan_prompt_persists_changed_pair() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let buffer = format_wrap_pane(Some("PLAN-PRE"), Some("PLAN-SUF"));
        let outcome = app
            .edit_plan_prompt_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.prompt_prefix.as_deref(), Some("PLAN-PRE"));
        assert_eq!(reloaded.prompt_suffix.as_deref(), Some("PLAN-SUF"));
        assert_eq!(app.plan.prompt_prefix.as_deref(), Some("PLAN-PRE"));
        assert_eq!(app.plan.prompt_suffix.as_deref(), Some("PLAN-SUF"));
    }

    // -- edit_step_prompt_pane --------------------------------------------

    /// Build a StepDetailApp with a plan + a single step materialized in `conn`,
    /// so writes via `update_step_fields_ext` land on a real row that can
    /// then be reloaded.
    fn setup_step_app(conn: &Connection) -> StepDetailApp {
        let plan = crate::storage::create_plan(
            conn,
            "tui-v1",
            "/proj",
            "tui-v1",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        let (step, _pos) = crate::storage::create_step(
            conn,
            &plan.id,
            "Original title",
            "Original description",
            None,
            None,
            &["original-crit".to_string()],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        StepDetailApp::new(
            plan,
            vec![step],
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        )
    }

    #[test]
    fn edit_step_prompt_no_editor_short_circuits() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_step_app(&conn);
        let outcome = app.edit_step_prompt_pane(&conn, fake_editor(None)).unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
    }

    #[test]
    fn edit_step_prompt_no_changes_when_buffer_round_trips_unchanged() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_step_app(&conn);
        let initial = format_step_pane(
            &app.steps[0].title,
            &app.steps[0].description,
            &app.steps[0].acceptance_criteria,
        );
        let outcome = app
            .edit_step_prompt_pane(&conn, fake_editor(Some(initial)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
    }

    #[test]
    fn edit_step_prompt_persists_changed_step() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_step_app(&conn);
        let buffer = format_step_pane(
            "NEW-TITLE",
            "NEW-DESCRIPTION",
            &["NEW-CRIT-A".to_string(), "NEW-CRIT-B".to_string()],
        );
        let outcome = app
            .edit_step_prompt_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // App's in-memory step is in sync.
        assert_eq!(app.steps[0].title, "NEW-TITLE");
        assert_eq!(app.steps[0].description, "NEW-DESCRIPTION");
        assert_eq!(
            app.steps[0].acceptance_criteria,
            vec!["NEW-CRIT-A".to_string(), "NEW-CRIT-B".to_string()]
        );
        // DB row reloads to the same values.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].title, "NEW-TITLE");
        assert_eq!(reloaded[0].description, "NEW-DESCRIPTION");
        assert_eq!(
            reloaded[0].acceptance_criteria,
            vec!["NEW-CRIT-A".to_string(), "NEW-CRIT-B".to_string()]
        );
    }

    #[test]
    fn edit_step_prompt_returns_parse_error_on_missing_header() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_step_app(&conn);
        // Strip out the description header — parser must reject this and
        // *not* write to the steps row.
        let bad = "# Title\nstill the title\n## Acceptance criteria\n- c\n";
        let outcome = app
            .edit_step_prompt_pane(&conn, fake_editor(Some(bad.to_string())))
            .unwrap();
        match outcome {
            EditOutcome::ParseError(msg) => {
                assert!(msg.contains("## Description"), "got: {msg}");
            }
            other => panic!("expected ParseError, got {other:?}"),
        }
        // The row still has the original title — no partial save happened.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].title, "Original title");
        assert_eq!(reloaded[0].description, "Original description");
    }

    #[test]
    fn edit_step_prompt_returns_parse_error_on_empty_title() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_step_app(&conn);
        let bad = "# Title\n\n## Description\nbody\n## Acceptance criteria\n";
        let outcome = app
            .edit_step_prompt_pane(&conn, fake_editor(Some(bad.to_string())))
            .unwrap();
        assert!(matches!(outcome, EditOutcome::ParseError(_)));
        // No write happened.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].title, "Original title");
    }

    // -- edit_tests_pane --------------------------------------------------

    #[test]
    fn edit_tests_pane_no_editor_short_circuits() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app.edit_tests_pane(&conn, fake_editor(None)).unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
    }

    #[test]
    fn edit_tests_pane_no_changes_when_unchanged() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.plan.deterministic_tests = vec!["cargo build".to_string()];
        storage::set_plan_deterministic_tests(&conn, &app.plan.id, &app.plan.deterministic_tests)
            .unwrap();
        let initial = format_tests_pane(&app.plan.deterministic_tests);
        let outcome = app
            .edit_tests_pane(&conn, fake_editor(Some(initial)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
    }

    #[test]
    fn edit_tests_pane_persists_changed_list() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let buffer = format_tests_pane(&[
            "cargo test".to_string(),
            "cargo clippy".to_string(),
        ]);
        let outcome = app
            .edit_tests_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert_eq!(
            app.plan.deterministic_tests,
            vec!["cargo test".to_string(), "cargo clippy".to_string()]
        );
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.deterministic_tests,
            vec!["cargo test".to_string(), "cargo clippy".to_string()]
        );
    }

    #[test]
    fn edit_tests_pane_can_clear_all_tests() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.plan.deterministic_tests = vec!["cargo test".to_string()];
        storage::set_plan_deterministic_tests(&conn, &app.plan.id, &app.plan.deterministic_tests)
            .unwrap();
        // User wipes the file down to just comments — round-trip yields an
        // empty list and the storage row is updated.
        let outcome = app
            .edit_tests_pane(&conn, fake_editor(Some("# (no tests)\n".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert!(app.plan.deterministic_tests.is_empty());
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert!(reloaded.deterministic_tests.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn edit_tests_pane_round_trips_through_mock_editor_script() {
        // Acceptance test mirroring step 25's mock-script test for the
        // single-string panes — exercise the real editor::edit_at handoff.
        use crate::tui::editor::edit_at;
        use std::os::unix::fs::PermissionsExt;

        let conn = crate::db::open_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("ed.sh");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf 'cargo test\\ncargo clippy -- -D warnings\\n' > \"$1\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_tests_pane(&conn, |initial| {
                edit_at(
                    script.to_str().unwrap(),
                    &tmp.path().join("buf.md"),
                    initial,
                )
            })
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let reloaded = storage::get_plan_by_slug(&conn, "tui-v1", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.deterministic_tests,
            vec!["cargo test".to_string(), "cargo clippy -- -D warnings".to_string()]
        );
    }
}
