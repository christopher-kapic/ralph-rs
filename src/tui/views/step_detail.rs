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
use crossterm::event::MouseEvent;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use rusqlite::Connection;

use crate::config::Config;
use crate::plan::{ChangePolicy, ExecutionLog, Plan, Step, StepStatus};
use crate::storage::{self, ProjectPromptSource, ProjectSettings};
use crate::tui::chrome::{self, Chrome};
use crate::tui::help::{self, HelpState};
use crate::tui::read_only::{self, ReadOnly};
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};
use crate::tui::views::step_detail_picker::{BottomCell, PickerKind, PickerOutcome, PickerState};
use crate::tui::widgets::palette_bar::{self, PaletteBarState};

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
    /// Global-layer prompt (`Config.prompt`). Labelled "Global (universal)"
    /// in user-facing text — "universal" is kept as a synonym only.
    GlobalPrompt,
    /// Project-layer prompt. Resolves file-first from
    /// `<workdir>/.ralph/prompt.md`, else the `project_settings.prompt` DB
    /// column; the edit handler routes the write to whichever is active.
    ProjectPrompt,
    /// Plan-layer prompt — this IS `plan.description`.
    PlanPrompt,
    StepPrompt,
    /// Open question `interruptions` rows for the focused step
    /// (TUI-plan.md §17). Sits between [`Pane::StepPrompt`] and
    /// [`Pane::Appended`] so the user sees the harness's pending blockers in
    /// the same vertical region that holds the harness's own prompt.
    OpenQuestions,
    Appended,
    Tests,
    BottomRow,
}

impl Pane {
    /// Display order — index into the pane stack from top to bottom. Drives
    /// the wrapping nav arithmetic below.
    pub const ORDER: [Pane; 8] = [
        Pane::GlobalPrompt,
        Pane::ProjectPrompt,
        Pane::PlanPrompt,
        Pane::StepPrompt,
        Pane::OpenQuestions,
        Pane::Appended,
        Pane::Tests,
        Pane::BottomRow,
    ];

    /// Position in [`Self::ORDER`].
    fn index(self) -> usize {
        Self::ORDER
            .iter()
            .position(|p| *p == self)
            .expect("pane in ORDER")
    }

    /// Title shown on the pane's bordered block. Kept here so renderers and
    /// tests share a single source of truth for the heading text.
    pub fn title(self) -> &'static str {
        match self {
            Pane::GlobalPrompt => "Global (universal) prompt",
            Pane::ProjectPrompt => "Project prompt",
            Pane::PlanPrompt => "Plan prompt",
            Pane::StepPrompt => "Step prompt",
            Pane::OpenQuestions => "Open question(s)",
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

/// Format a single prompt layer into the blob shown to `$EDITOR`. The buffer
/// is just the raw prompt text (the four-layer model has one content blob
/// per layer — no prefix/suffix split). A trailing newline is normalized in
/// so editors that auto-append one don't produce a spurious "Saved" outcome
/// on an otherwise-unchanged round-trip through [`parse_prompt_pane`].
pub(crate) fn format_prompt_pane(prompt: Option<&str>) -> String {
    match prompt {
        Some(s) if !s.is_empty() => {
            if s.ends_with('\n') {
                s.to_string()
            } else {
                format!("{s}\n")
            }
        }
        _ => String::new(),
    }
}

/// Parse the blob written by `$EDITOR` back into a single prompt layer.
/// Whitespace-only input becomes `None` so saving an empty buffer clears the
/// layer; otherwise the content is trimmed of leading/trailing whitespace so
/// editor-added blank lines don't drift the value across round-trips.
pub(crate) fn parse_prompt_pane(text: &str) -> Option<String> {
    trim_to_option(text)
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

    /// Universal (Global-layer) prompt sourced from `Config.prompt`. Cloned
    /// at construction so the view doesn't need to retain a `&Config`
    /// reference.
    pub config_prompt: Option<String>,

    /// Project-layer prompt sourced from the `project_settings` row.
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

    /// Currently focused sub-cell within the bottom row. `h`/`l` walks
    /// between cells while [`Pane::BottomRow`] has focus; `c` opens the
    /// picker for the focused cell (TUI-plan.md §8 "Bottom-row inline editors").
    pub bottom_focus: BottomCell,

    /// Active bottom-row picker, or `None` when no picker is open. Set by
    /// [`Self::open_picker_for_focused_cell`] and cleared by either a
    /// `Cancelled` or `Submit` outcome from the picker's key loop.
    pub picker: Option<PickerState>,

    /// Read-only attach state (TUI-plan.md §13.2). When `Locked`, the `c`
    /// editor handoff on every editable pane is suppressed and the
    /// persistent banner replaces the bottom hint line. The dispatcher
    /// updates this each poll tick via [`Self::set_read_only`].
    pub read_only: ReadOnly,

    /// Open question `interruptions` rows for the *focused* step,
    /// ordered oldest first. Drives the [`Pane::OpenQuestions`] body.
    /// Refreshed by the dispatcher each poll tick.
    pub open_questions_for_step: Vec<storage::OpenQuestion>,

    /// Cursor within the [`Pane::OpenQuestions`] pane (0-based). j/k moves
    /// it while the pane is focused; out-of-range values are clamped on
    /// every refresh.
    pub selected_question_index: usize,

    /// Total number of unanswered questions across the *whole plan* (every
    /// step). Drives the resume-modal trigger: when the user answers the
    /// last open question for the plan, the dispatcher pops the modal.
    pub plan_open_questions_count: usize,

    /// Active answer modal, or `None` when no question is being answered.
    /// Set by [`Self::open_answer_modal`]; cleared by either a Cancel or a
    /// successful Submit.
    pub answer_modal: Option<crate::tui::views::answer_modal::AnswerModal>,

    /// Active resume-implementation modal, or `None`. Spawned by
    /// [`Self::note_answer_persisted`] when the just-applied answer was the
    /// plan's last open question; cleared by either Accept or Decline.
    pub resume_modal: Option<crate::tui::views::answer_modal::ResumeModal>,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// touching pane navigation or modal handlers (TUI-plan.md §15).
    pub help: HelpState,
    /// Slash/colon command palette state (TUI-plan.md §9). `Some` while the
    /// bar is open; the dispatcher routes every key through
    /// [`PaletteBarState::on_key`] before any view bindings fire. `/` and
    /// `:` open it.
    pub palette_bar: Option<PaletteBarState>,

    /// User-driven sidebar width override. When `Some(w)`, the layout uses
    /// `w` (clamped 4..=80) instead of the zen-derived constant — set by a
    /// mouse drag on the divider. Cleared by `z` so zen toggling stays
    /// predictable. Session-only — never persisted.
    pub sidebar_w_override: Option<u16>,

    /// Body width recorded during the most recent `draw()`. Used by
    /// [`Self::handle_mouse`] to clamp the cursor's column when computing a
    /// new override; zero before the first frame, in which case mouse
    /// handling no-ops.
    pub last_body_width: u16,

    /// Sidebar column width recorded during the most recent `draw()`. Acts
    /// as the divider column for hit-testing the start-of-drag click in
    /// [`Self::handle_mouse`]. Zero before the first frame.
    pub last_sidebar_w: u16,

    /// True while a left-mouse drag started on the divider column is
    /// active. Cleared on `MouseEventKind::Up(Left)`.
    pub dragging_sidebar: bool,

    /// Per-pane vertical scroll offsets, indexed by [`Pane::index`]. The
    /// focused pane reads/writes its own offset via `J`/`K`, paging keys,
    /// and the mouse wheel so long prompt bodies remain readable.
    pane_scroll: [u16; 8],

    /// Per-pane body heights from the most recent render, paired with
    /// [`Self::pane_line_counts`] so scroll offsets clamp to the visible
    /// content instead of drifting past the end.
    pane_body_heights: [u16; 8],

    /// Per-pane wrapped line counts from the most recent render. Used only
    /// for scroll clamping.
    pane_line_counts: [u16; 8],
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
            config_prompt: config.prompt.clone(),
            project_settings,
            default_harness_name: config.default_harness.clone(),
            harness_default_models,
            execution_logs,
            appended_attempt_index,
            bottom_focus: BottomCell::Harness,
            picker: None,
            read_only: ReadOnly::Editable,
            open_questions_for_step: Vec::new(),
            selected_question_index: 0,
            plan_open_questions_count: 0,
            answer_modal: None,
            resume_modal: None,
            help: HelpState::new(),
            palette_bar: None,
            sidebar_w_override: None,
            last_body_width: 0,
            last_sidebar_w: 0,
            dragging_sidebar: false,
            pane_scroll: [0; 8],
            pane_body_heights: [0; 8],
            pane_line_counts: [0; 8],
        }
    }

    /// Open the palette with `prefix` as the trigger key (`/` or `:`).
    /// TUI-plan.md §9.
    pub fn open_palette(&mut self, prefix: char) {
        self.palette_bar = Some(PaletteBarState::new(prefix));
    }

    /// Close the palette without dispatching. TUI-plan.md §9.
    pub fn close_palette(&mut self) {
        self.palette_bar = None;
    }

    /// Whether the palette bar is currently open and consuming keys.
    pub fn palette_active(&self) -> bool {
        self.palette_bar.is_some()
    }

    /// Mouse-event entry point routed from the dispatcher's event loop.
    /// Implements draggable resize of the sidebar via mouse drag on the
    /// divider column: a left-button press within ±1 column of the current
    /// `last_sidebar_w` arms a drag (and disables `user_zen` so the override
    /// takes precedence), subsequent drags update `sidebar_w_override`
    /// directly to the cursor's column (clamped 4..=80), and release clears
    /// the drag flag. No-op before the first frame (`last_body_width == 0`).
    pub fn handle_mouse(&mut self, event: MouseEvent) {
        use crossterm::event::{MouseButton, MouseEventKind};

        match event.kind {
            MouseEventKind::ScrollDown => {
                self.scroll_focused_pane_down();
                return;
            }
            MouseEventKind::ScrollUp => {
                self.scroll_focused_pane_up();
                return;
            }
            _ => {}
        }

        if self.last_body_width == 0 {
            return;
        }
        let divider_col = self.last_sidebar_w as i32;

        match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let col = event.column as i32;
                if (col - divider_col).abs() <= 1 {
                    self.dragging_sidebar = true;
                    // Drag explicitly opts out of zen — the override should
                    // win regardless of zen toggle state. `auto_zen` is
                    // recomputed each frame so we leave it alone.
                    self.user_zen = false;
                }
            }
            MouseEventKind::Drag(MouseButton::Left) if self.dragging_sidebar => {
                let w = event.column.clamp(4, 80);
                self.sidebar_w_override = Some(w);
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.dragging_sidebar = false;
            }
            _ => {}
        }
    }

    /// Update the read-only state. Called by the dispatcher after each
    /// `run_locks` poll. While `Locked`, the `c` editor handoff and the
    /// `a` (answer question) keybinding are suppressed per TUI-plan.md
    /// §13.2; navigation, `S` (cancel), `q` (quit), and `?` (help) stay
    /// active.
    pub fn set_read_only(&mut self, state: ReadOnly) {
        self.read_only = state;
    }

    /// True when `c` (editor handoff) and `a` (answer question) should be
    /// honored. False during read-only attach (TUI-plan.md §13.2). The
    /// future step-detail dispatcher consults this before invoking any of
    /// the `edit_*_pane` methods or opening the question-answer modal.
    pub fn can_edit_panes(&self) -> bool {
        !self.read_only.is_locked()
    }

    fn pane_scroll(&self, pane: Pane) -> u16 {
        self.pane_scroll[pane.index()]
    }

    fn set_pane_metrics(&mut self, pane: Pane, body_height: u16, line_count: u16) -> u16 {
        let idx = pane.index();
        self.pane_body_heights[idx] = body_height;
        self.pane_line_counts[idx] = line_count.max(1);
        let max = self.max_pane_scroll(pane);
        if self.pane_scroll[idx] > max {
            self.pane_scroll[idx] = max;
        }
        self.pane_scroll[idx]
    }

    fn max_pane_scroll(&self, pane: Pane) -> u16 {
        let idx = pane.index();
        self.pane_line_counts[idx].saturating_sub(self.pane_body_heights[idx].max(1))
    }

    fn pane_is_scrollable(pane: Pane) -> bool {
        pane != Pane::BottomRow
    }

    pub fn scroll_focused_pane_down(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        let idx = self.focused_pane.index();
        let max = self.max_pane_scroll(self.focused_pane);
        self.pane_scroll[idx] = self.pane_scroll[idx].saturating_add(1).min(max);
    }

    pub fn scroll_focused_pane_up(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        let idx = self.focused_pane.index();
        self.pane_scroll[idx] = self.pane_scroll[idx].saturating_sub(1);
    }

    pub fn page_focused_pane_down(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        let idx = self.focused_pane.index();
        let page = self.pane_body_heights[idx].max(1);
        let max = self.max_pane_scroll(self.focused_pane);
        self.pane_scroll[idx] = self.pane_scroll[idx].saturating_add(page).min(max);
    }

    pub fn page_focused_pane_up(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        let idx = self.focused_pane.index();
        let page = self.pane_body_heights[idx].max(1);
        self.pane_scroll[idx] = self.pane_scroll[idx].saturating_sub(page);
    }

    pub fn scroll_focused_pane_to_top(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        self.pane_scroll[self.focused_pane.index()] = 0;
    }

    pub fn scroll_focused_pane_to_bottom(&mut self) {
        if !Self::pane_is_scrollable(self.focused_pane) {
            return;
        }
        let idx = self.focused_pane.index();
        self.pane_scroll[idx] = self.max_pane_scroll(self.focused_pane);
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
    /// Returns `true` when the toggle was applied. Always clears any active
    /// `sidebar_w_override` (even when the toggle is suppressed by auto-zen)
    /// so zen behavior remains predictable after a mouse-drag resize.
    pub fn toggle_zen(&mut self) -> bool {
        self.sidebar_w_override = None;
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

    // -- Open-question pane (TUI-plan.md §17) ----------------------------

    /// Replace the open-question list for the focused step (after a DB
    /// poll). Clamps `selected_question_index` into the new range so the
    /// cursor never escapes the pane.
    pub fn set_open_questions_for_step(&mut self, questions: Vec<storage::OpenQuestion>) {
        self.open_questions_for_step = questions;
        if self.selected_question_index >= self.open_questions_for_step.len() {
            self.selected_question_index = self.open_questions_for_step.len().saturating_sub(1);
        }
    }

    /// Update the cached plan-wide open-question count. Drives the
    /// resume-modal trigger.
    pub fn set_plan_open_questions_count(&mut self, count: usize) {
        self.plan_open_questions_count = count;
    }

    /// True when the [`Pane::OpenQuestions`] pane has at least one row
    /// to render — the renderer drops the placeholder body in that case
    /// and the `a` keybinding has something to target.
    pub fn has_open_questions_for_step(&self) -> bool {
        !self.open_questions_for_step.is_empty()
    }

    /// Currently focused open question on the [`Pane::OpenQuestions`] pane,
    /// or `None` when the step has no open questions.
    pub fn focused_open_question(&self) -> Option<&storage::OpenQuestion> {
        self.open_questions_for_step
            .get(self.selected_question_index)
    }

    /// Move the question-pane cursor down one row, wrapping at the bottom.
    /// No-op when there are zero or one open questions.
    pub fn select_question_next(&mut self) {
        let n = self.open_questions_for_step.len();
        if n <= 1 {
            return;
        }
        self.selected_question_index = (self.selected_question_index + 1) % n;
    }

    /// Move the question-pane cursor up one row, wrapping at the top.
    pub fn select_question_prev(&mut self) {
        let n = self.open_questions_for_step.len();
        if n <= 1 {
            return;
        }
        if self.selected_question_index == 0 {
            self.selected_question_index = n - 1;
        } else {
            self.selected_question_index -= 1;
        }
    }

    // -- Answer modal ----------------------------------------------------

    /// Open the answer modal for the question currently focused in the
    /// [`Pane::OpenQuestions`] pane. No-op when the pane is empty,
    /// when the pane isn't focused, when read-only attach is active, or
    /// when a modal is already open.
    pub fn open_answer_modal(&mut self) -> bool {
        if !self.can_edit_panes() || self.answer_modal.is_some() {
            return false;
        }
        if self.focused_pane != Pane::OpenQuestions {
            return false;
        }
        let Some(q) = self.focused_open_question() else {
            return false;
        };
        self.answer_modal = Some(crate::tui::views::answer_modal::AnswerModal::new(
            q.id.clone(),
            q.question.clone(),
            q.suggestions.clone(),
        ));
        true
    }

    /// Close the answer modal (Cancel path). Idempotent.
    pub fn close_answer_modal(&mut self) {
        self.answer_modal = None;
    }

    // -- Resume-implementation modal -------------------------------------

    /// Inform the App that the dispatcher just persisted an answer.
    /// `previous_run_current_branch` mirrors the previous run's
    /// `--current-branch` flag so the resume modal carries it forward when
    /// the user accepts.
    ///
    /// When the just-applied answer was the *last* open question for the
    /// plan (i.e. `plan_open_questions_count` is now zero), this opens the
    /// resume modal. Otherwise the modal stays closed.
    pub fn note_answer_persisted(&mut self, previous_run_current_branch: bool) {
        self.answer_modal = None;
        if self.plan_open_questions_count == 0 && self.resume_modal.is_none() {
            self.resume_modal = Some(crate::tui::views::answer_modal::ResumeModal::new(
                self.plan.slug.clone(),
                previous_run_current_branch,
            ));
        }
    }

    /// Close the resume modal without spawning a runner (Decline path).
    pub fn close_resume_modal(&mut self) {
        self.resume_modal = None;
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

    /// Handle `h` / `←` in the step-detail view per §8. Three priority
    /// branches:
    ///
    /// 1. Appended pane with a previous attempt available — paginate left.
    /// 2. BottomRow pane with a sub-cell to the left — move bottom focus.
    /// 3. Otherwise (any pane, edges fall through) — pop the view.
    pub fn handle_left(&mut self) {
        if self.focused_pane == Pane::Appended && self.appended_prev() {
            return;
        }
        if self.focused_pane == Pane::BottomRow
            && let Some(prev) = self.bottom_focus.move_left()
        {
            self.bottom_focus = prev;
            return;
        }
        self.request_pop();
    }

    /// Handle `l` / `→` in the step-detail view per §8 + step 27. Either
    /// advances the Appended-pane attempt index or moves bottom-row focus
    /// one cell to the right; on any other pane (or at the row's right
    /// edge) it's a no-op.
    pub fn handle_right(&mut self) {
        if self.focused_pane == Pane::Appended {
            self.appended_next();
            return;
        }
        if self.focused_pane == Pane::BottomRow
            && let Some(next) = self.bottom_focus.move_right()
        {
            self.bottom_focus = next;
        }
    }

    // -- Bottom-row pickers (TUI-plan.md §8 + step 27) -------------------

    /// Open the picker for the currently focused bottom-row sub-cell.
    /// `agents` is the sorted, deduplicated list of agent filenames (without
    /// the `.md` extension) — passed in rather than fetched from disk so
    /// tests can inject a known list and the dispatcher can refresh once
    /// per picker open.
    ///
    /// No-op when the focused pane isn't [`Pane::BottomRow`] or when a
    /// picker is already open. When the plan has no steps, the picker still
    /// builds against the resolved (fallback) values — the Submit path then
    /// short-circuits in `apply_picker_submit` since there's no step row to
    /// write to.
    pub fn open_picker_for_focused_cell(&mut self, agents: &[String]) {
        if self.focused_pane != Pane::BottomRow || self.picker.is_some() {
            return;
        }
        let harnesses = self.harness_picker_options();
        let harness = self.effective_harness();
        let model_default = self.harness_default_models.get(&harness).cloned().flatten();
        let current_model = self.current_step().and_then(|s| s.model.clone());
        let current_agent = self.current_step().and_then(|s| s.agent.clone());
        let current_policy = self.effective_change_policy();

        let picker = match self.bottom_focus {
            BottomCell::Harness => PickerState::for_harness(&harnesses, Some(harness.as_str())),
            BottomCell::Model => {
                PickerState::for_model(model_default.as_deref(), current_model.as_deref())
            }
            BottomCell::Agent => PickerState::for_agent(agents, current_agent.as_deref()),
            BottomCell::ChangePolicy => PickerState::for_change_policy(current_policy),
        };
        self.picker = Some(picker);
    }

    /// Close any open picker without writing — used by the Cancelled
    /// outcome and on view pop.
    pub fn close_picker(&mut self) {
        self.picker = None;
    }

    /// Sorted, deduplicated `Config.harnesses` keys, derived from the
    /// per-harness `default_model` lookup we already cached at construction.
    /// Sorting keeps the picker order deterministic across renders.
    fn harness_picker_options(&self) -> Vec<String> {
        let mut names: Vec<String> = self.harness_default_models.keys().cloned().collect();
        names.sort();
        names.dedup();
        names
    }

    /// Apply a picker submission. Writes the chosen value through
    /// [`storage::update_step_fields_ext`] and refreshes the matching
    /// in-memory step field so the bottom row re-renders the new value
    /// without a reload. No-op when the plan has no steps under the
    /// current selection.
    ///
    /// `kind` and `value` come straight from [`PickerOutcome::Submit`].
    /// The Change-policy submission is parsed back to the enum;
    /// unrecognized strings (which the picker can't produce in normal use)
    /// are rejected to keep the column constraint intact.
    pub fn apply_picker_submit(
        &mut self,
        conn: &Connection,
        kind: PickerKind,
        value: &str,
    ) -> anyhow::Result<()> {
        let Some(step) = self.steps.get(self.selected_step_index).cloned() else {
            // Empty plan — the picker shouldn't have opened, but be defensive.
            return Ok(());
        };
        match kind {
            PickerKind::Harness => {
                storage::update_step_fields_ext(
                    conn,
                    &step.id,
                    None,
                    None,
                    None,
                    Some(Some(value)),
                    None,
                    None,
                    None,
                    None,
                    None,
                )?;
                if let Some(s) = self.steps.get_mut(self.selected_step_index) {
                    s.harness = Some(value.to_string());
                }
            }
            PickerKind::Model => {
                storage::update_step_fields_ext(
                    conn,
                    &step.id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(Some(value)),
                    None,
                    None,
                )?;
                if let Some(s) = self.steps.get_mut(self.selected_step_index) {
                    s.model = Some(value.to_string());
                }
            }
            PickerKind::Agent => {
                storage::update_step_fields_ext(
                    conn,
                    &step.id,
                    None,
                    None,
                    Some(Some(value)),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )?;
                if let Some(s) = self.steps.get_mut(self.selected_step_index) {
                    s.agent = Some(value.to_string());
                }
            }
            PickerKind::ChangePolicy => {
                let policy: ChangePolicy = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Unrecognized change policy: {value}"))?;
                storage::update_step_fields_ext(
                    conn,
                    &step.id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(policy),
                    None,
                )?;
                if let Some(s) = self.steps.get_mut(self.selected_step_index) {
                    s.change_policy = policy;
                }
            }
        }
        Ok(())
    }

    /// Drive one key event into the active picker. Wraps
    /// [`PickerState::handle_key`] so the dispatcher doesn't have to clone
    /// the `kind` / `value` out of the borrow checker's way: returning
    /// `Some(Submit)` means the caller should call
    /// [`Self::apply_picker_submit`] and then `close_picker`.
    pub fn picker_handle_key(&mut self, key: crossterm::event::KeyEvent) -> Option<PickerOutcome> {
        let picker = self.picker.as_mut()?;
        let outcome = picker.handle_key(key);
        // The state machine reports Pending when the user confirms the
        // synthetic Custom… row — flip the mode so the next render shows
        // the input field.
        if outcome == PickerOutcome::Pending
            && key.code == crossterm::event::KeyCode::Enter
            && picker.is_custom_row_selected()
        {
            picker.enter_custom_input();
        }
        Some(outcome)
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
        self.harness_default_models.get(&harness).cloned().flatten()
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

    /// `c` on the Universal pane: round-trip the Global-layer prompt
    /// (`Config.prompt`) through `$EDITOR` and persist via
    /// [`Config::save_at`]. Mutates the in-memory `config` so subsequent
    /// reads of the same struct see the new value, and refreshes the app's
    /// mirrored copy so the pane re-renders without a reload.
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
        let initial = format_prompt_pane(config.prompt.as_deref());
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let new_prompt = parse_prompt_pane(&new_text);
        if new_prompt == config.prompt {
            return Ok(EditOutcome::NoChanges);
        }
        config.prompt = new_prompt.clone();
        config.save_at(config_dir)?;
        self.config_prompt = new_prompt;
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Project pane: round-trip the Project-layer prompt through
    /// `$EDITOR` and persist to whichever source is *active*.
    ///
    /// Precedence follows [`storage::resolve_project_prompt`]: when
    /// `<workdir>/.ralph/prompt.md` is present (and non-blank) the file is
    /// the source of truth, so the edit is seeded from and written back to
    /// that file via [`storage::write_project_prompt_file`]. Otherwise the
    /// DB column (`project_settings.prompt`) is edited via
    /// [`storage::set_project_prompt`]. The precedence itself is not
    /// re-implemented here — the storage resolver decides.
    pub fn edit_project_pane<E>(&mut self, conn: &Connection, edit_fn: E) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let (resolved, source) = storage::resolve_project_prompt(conn, &self.plan.project)?;
        let initial = format_prompt_pane(resolved.prompt.as_deref());
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        let new_prompt = parse_prompt_pane(&new_text);
        if new_prompt == resolved.prompt {
            return Ok(EditOutcome::NoChanges);
        }
        match source {
            ProjectPromptSource::File(_) => match new_prompt.as_deref() {
                Some(content) => storage::write_project_prompt_file(&self.plan.project, content)?,
                // Clearing a file-backed prompt removes the file so the DB
                // (or nothing) becomes the active source again.
                None => storage::delete_project_prompt_file(&self.plan.project)?,
            },
            ProjectPromptSource::Db => {
                storage::set_project_prompt(conn, &self.plan.project, new_prompt.as_deref())?
            }
        }
        self.project_settings.prompt = new_prompt;
        Ok(EditOutcome::Saved)
    }

    /// `c` on the Plan pane: round-trip the Plan-layer prompt — which IS
    /// `plan.description` — through `$EDITOR` and persist via
    /// [`storage::update_plan_description`]. The in-memory `plan` mirror is
    /// refreshed so the pane re-renders without a reload.
    pub fn edit_plan_prompt_pane<E>(&mut self, conn: &Connection, edit_fn: E) -> Result<EditOutcome>
    where
        E: FnOnce(&str) -> Result<Option<String>>,
    {
        let initial = format_prompt_pane(Some(self.plan.description.as_str()));
        let new_text = match edit_fn(&initial)? {
            None => return Ok(EditOutcome::NoEditor),
            Some(s) => s,
        };
        // The plan description is a plain `String` (not `Option`); an empty
        // edit clears it to the empty string rather than `None`.
        let new_desc = parse_prompt_pane(&new_text).unwrap_or_default();
        if new_desc == self.plan.description {
            return Ok(EditOutcome::NoChanges);
        }
        storage::update_plan_description(conn, &self.plan.id, &new_desc)?;
        self.plan.description = new_desc;
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
    pub fn edit_step_prompt_pane<E>(&mut self, conn: &Connection, edit_fn: E) -> Result<EditOutcome>
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
    pub fn edit_tests_pane<E>(&mut self, conn: &Connection, edit_fn: E) -> Result<EditOutcome>
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
    let normal_hint = "[j/k] pane  [J/K] scroll  [h/←] back  [z] zen  [/:] cmd  [q] back";
    let palette_hint = "[tab] complete  [enter] submit  [esc] cancel";
    let hint = if app.palette_active() {
        palette_hint
    } else {
        normal_hint
    };
    let banner = read_only::banner(app.read_only);
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint,
            cwd: Path::new(&app.plan.project),
            banner: banner.as_deref(),
            running_indicator: None,
        },
    );

    if body.width == 0 || body.height == 0 {
        return;
    }

    // The user-driven mouse-drag override wins over the zen-derived width,
    // so a deliberate resize survives subsequent re-renders. `z` clears the
    // override, restoring zen-driven behavior. TUI-plan.md, step 27.
    let sidebar_w = match app.sidebar_w_override {
        Some(w) => w,
        None => {
            if app.is_zen_mode() {
                SIDEBAR_ZEN_WIDTH
            } else {
                SIDEBAR_FULL_WIDTH
            }
        }
    };
    let sidebar_w = sidebar_w.min(body.width.saturating_sub(1).max(1));

    // Cache the dimensions for `handle_mouse` — see `Self::handle_mouse`.
    app.last_body_width = body.width;
    app.last_sidebar_w = sidebar_w;

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(sidebar_w), Constraint::Min(0)])
        .split(body);

    draw_sidebar(frame, app, main[0]);
    draw_pane_stack(frame, app, main[1]);

    if let Some(picker) = &app.picker {
        super::step_detail_picker::render(frame, frame.area(), picker);
    }

    // §17 modals are last so they composite over everything else.
    if let Some(modal) = &app.answer_modal {
        render_answer_modal(frame, frame.area(), modal);
    } else if let Some(modal) = &app.resume_modal {
        render_resume_modal(frame, frame.area(), modal);
    }

    if let Some(toast) = app.toasts.current() {
        let area = frame.area();
        if area.height >= 1 && area.width > 0 {
            render_toast_overlay(frame, area, &toast.text, toast.color);
        }
    }

    // Help overlay sits on top of everything else when `?` has been pressed.
    if app.help.is_visible() {
        let area = frame.area();
        help::render(frame, area, &help::for_step_detail());
    }

    // Palette bar overlays the bottom chrome row when active. TUI-plan.md §9.
    if let Some(state) = app.palette_bar.as_ref() {
        let area = frame.area();
        let strip_height = 4.min(area.height);
        if strip_height > 0 {
            let palette_area = Rect {
                x: area.x,
                y: area.y + area.height - strip_height,
                width: area.width,
                height: strip_height,
            };
            palette_bar::render(frame, palette_area, state);
        }
    }
}

/// Render the answer modal as a centered overlay. Layout mirrors the §17
/// sketch: the question line, suggestions numbered from `[1]`, then `[c]
/// Custom answer` and `[esc] Cancel` rows.
fn render_answer_modal(
    frame: &mut Frame,
    area: Rect,
    modal: &crate::tui::views::answer_modal::AnswerModal,
) {
    // An open interruption is the §12.5 "blocked / interrupted" concept;
    // style its modal via the single mapping so it matches the plan-list
    // dot and a blocked step glyph (one concept, one color).
    let interrupted = theme::plan_status_color(crate::plan::PlanStatus::Interrupted);
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(
            "❓ ",
            Style::default()
                .fg(interrupted)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            modal.question.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]));
    lines.push(Line::from(""));
    for (i, sug) in modal.suggestions.iter().enumerate() {
        lines.push(Line::from(vec![
            Span::styled(
                format!("  [{}] ", i + 1),
                Style::default()
                    .fg(theme::SELECTION)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(sug.clone()),
        ]));
    }
    lines.push(Line::from(vec![
        Span::styled(
            "  [c] ",
            Style::default()
                .fg(theme::CURSOR)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("Custom answer (opens $EDITOR)"),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "  [esc] ",
            Style::default()
                .fg(theme::CHROME_DIM)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("Cancel"),
    ]));

    let dialog = centered_modal_rect(area, &lines, " Answer question ");
    frame.render_widget(Clear, dialog);
    let block = Block::default()
        .title(" Answer question ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(interrupted));
    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// Render the resume-implementation prompt as a centered overlay. The text
/// matches §17's "All questions answered. Resume implementation now? [Y/n]".
fn render_resume_modal(
    frame: &mut Frame,
    area: Rect,
    modal: &crate::tui::views::answer_modal::ResumeModal,
) {
    let body_lines = vec![
        Line::from("All questions answered."),
        Line::from(""),
        Line::from(format!(
            "Resume implementation for `{slug}`? [Y/n]",
            slug = modal.plan_slug,
        )),
    ];
    let dialog = centered_modal_rect(area, &body_lines, " Resume run ");
    frame.render_widget(Clear, dialog);
    let block = Block::default()
        .title(" Resume run ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let para = Paragraph::new(body_lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// Shared centering math for the §17 modals: pick a width that fits the
/// longest line (capped at the available area), a height that fits the
/// body plus borders, and center inside `area`.
fn centered_modal_rect(area: Rect, body: &[Line], title: &str) -> Rect {
    let body_w = body
        .iter()
        .map(|l| {
            l.spans
                .iter()
                .map(|s| s.content.chars().count())
                .sum::<usize>()
        })
        .max()
        .unwrap_or(0);
    let title_w = title.chars().count();
    let desired_w = body_w.max(title_w) as u16 + 4;
    let width = desired_w.min(area.width).max(20.min(area.width));
    let desired_h = (body.len() as u16) + 2; // borders top + bottom
    let height = desired_h.min(area.height).max(5.min(area.height));
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect {
        x,
        y,
        width,
        height,
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
        text.chars()
            .take(toast_area.width as usize)
            .collect::<String>(),
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

fn draw_pane_stack(frame: &mut Frame, app: &mut StepDetailApp, area: Rect) {
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
fn draw_pane(frame: &mut Frame, app: &mut StepDetailApp, pane: Pane, area: Rect) {
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
        Pane::GlobalPrompt => {
            let text = app.config_prompt.clone();
            render_text_pane(frame, app, pane, inner, text.as_deref())
        }
        Pane::ProjectPrompt => {
            let text = app.project_settings.prompt.clone();
            render_text_pane(frame, app, pane, inner, text.as_deref())
        }
        // The Plan layer IS `plan.description`. An empty description renders
        // the `(none)` placeholder via `render_text_pane`.
        Pane::PlanPrompt => {
            let text = if app.plan.description.is_empty() {
                None
            } else {
                Some(app.plan.description.clone())
            };
            render_text_pane(frame, app, pane, inner, text.as_deref())
        }
        Pane::StepPrompt => render_step_prompt(frame, app, inner),
        Pane::OpenQuestions => render_open_questions(frame, app, inner),
        Pane::Appended => render_appended(frame, app, inner),
        Pane::Tests => render_tests(frame, app, inner),
        Pane::BottomRow => render_bottom_row(frame, app, inner),
    }
}

fn wrapped_visual_line_count(chars: usize, width: u16) -> u16 {
    if width == 0 {
        return 0;
    }
    chars.max(1).div_ceil(width as usize).min(u16::MAX as usize) as u16
}

fn text_visual_line_count(text: &str, width: u16) -> u16 {
    if width == 0 {
        return 0;
    }
    text.split('\n')
        .map(|line| wrapped_visual_line_count(line.chars().count(), width) as usize)
        .sum::<usize>()
        .min(u16::MAX as usize) as u16
}

fn line_visual_line_count(line: &Line, width: u16) -> u16 {
    wrapped_visual_line_count(
        line.spans
            .iter()
            .map(|span| span.content.chars().count())
            .sum(),
        width,
    )
}

fn lines_visual_line_count(lines: &[Line], width: u16) -> u16 {
    if width == 0 {
        return 0;
    }
    lines
        .iter()
        .map(|line| line_visual_line_count(line, width) as usize)
        .sum::<usize>()
        .max(1)
        .min(u16::MAX as usize) as u16
}

fn render_scrolled_paragraph<'a>(
    frame: &mut Frame,
    app: &mut StepDetailApp,
    pane: Pane,
    area: Rect,
    paragraph: Paragraph<'a>,
    line_count: u16,
) {
    let scroll = app.set_pane_metrics(pane, area.height, line_count);
    frame.render_widget(paragraph.scroll((scroll, 0)), area);
}

/// Render the [`Pane::OpenQuestions`] body. Each unanswered question is
/// shown as a `❓ <text>` line followed by indented `[N] suggestion`
/// rows. The currently focused question is bolded so j/k feedback is
/// visible even when the pane itself isn't focused.
fn render_open_questions(frame: &mut Frame, app: &mut StepDetailApp, area: Rect) {
    if app.open_questions_for_step.is_empty() {
        render_scrolled_paragraph(
            frame,
            app,
            Pane::OpenQuestions,
            area,
            Paragraph::new(Span::styled(
                "(no open questions for this step)",
                Style::default().fg(theme::CHROME_DIM),
            )),
            1,
        );
        return;
    }
    let bold = Style::default().add_modifier(Modifier::BOLD);
    let dim = Style::default().fg(theme::CHROME_DIM);
    let mut lines: Vec<Line> = Vec::new();
    // Open interruptions are the §12.5 blocked/interrupted concept — color
    // them via the single mapping (one concept, one color across screens).
    let interrupted = theme::plan_status_color(crate::plan::PlanStatus::Interrupted);
    for (i, q) in app.open_questions_for_step.iter().enumerate() {
        let focused = i == app.selected_question_index;
        let header_style = if focused {
            Style::default()
                .fg(interrupted)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(interrupted)
        };
        let mut header = vec![
            Span::styled(if focused { "▶ " } else { "  " }, header_style),
            Span::styled("❓ ", header_style),
            Span::raw(q.question.clone()),
        ];
        if focused {
            header.push(Span::styled("  [a]nswer", bold));
        }
        lines.push(Line::from(header));
        for (sidx, sug) in q.suggestions.iter().enumerate() {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("    [{}] ", sidx + 1),
                    Style::default().fg(theme::SELECTION),
                ),
                Span::raw(sug.clone()),
            ]));
        }
        if q.suggestions.is_empty() {
            lines.push(Line::from(Span::styled(
                "    (no suggestions — use [c] for a custom answer)",
                dim,
            )));
        }
        if i + 1 < app.open_questions_for_step.len() {
            lines.push(Line::from(""));
        }
    }
    let line_count = lines_visual_line_count(&lines, area.width);
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    render_scrolled_paragraph(frame, app, Pane::OpenQuestions, area, para, line_count);
}

/// Render a single text body (plan-context-prepend pane). `None` becomes the
/// dim `(none)` placeholder; `Some("")` renders as an empty body.
fn render_text_pane(
    frame: &mut Frame,
    app: &mut StepDetailApp,
    pane: Pane,
    area: Rect,
    text: Option<&str>,
) {
    let (para, line_count) = match text {
        None => (
            Paragraph::new(Span::styled(
                NONE_PLACEHOLDER,
                Style::default().fg(theme::CHROME_DIM),
            ))
            .wrap(Wrap { trim: false }),
            1,
        ),
        Some(s) => (
            Paragraph::new(s.to_string()).wrap(Wrap { trim: false }),
            text_visual_line_count(s, area.width),
        ),
    };
    render_scrolled_paragraph(frame, app, pane, area, para, line_count);
}

/// Render the Step prompt pane: title, description, and the bulleted
/// acceptance criteria.
fn render_step_prompt(frame: &mut Frame, app: &mut StepDetailApp, area: Rect) {
    let Some(step) = app.current_step() else {
        render_scrolled_paragraph(
            frame,
            app,
            Pane::StepPrompt,
            area,
            Paragraph::new(Span::styled(
                "(no steps)",
                Style::default().fg(theme::CHROME_DIM),
            )),
            1,
        );
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    let bold = Style::default().add_modifier(Modifier::BOLD);

    lines.push(Line::from(Span::styled(step.title.clone(), bold)));

    // docs/dag-redesign.md §12.1/§12.5: surface the step's effective status
    // (Blocked overlay derived from an open interruption — §3.3), its
    // `review_status` badge, and the `↳ corrects <short_id>` marker for a
    // reviewer-inserted corrective step. All colors route through the
    // single TUI-wide §12.5 mapping so step-detail can't drift from the
    // outline glyph / plan-list dot.
    {
        let eff =
            crate::plan::effective_step_status(step.status, app.has_open_questions_for_step());
        let mut status_spans = vec![
            Span::styled(
                format!("{} ", crate::tui::widgets::outline_list::status_glyph(eff)),
                Style::default().fg(theme::step_status_color(eff)),
            ),
            Span::styled(
                format!("{} ", step.short_id),
                Style::default().fg(theme::CHROME_DIM),
            ),
            Span::styled(
                eff.as_str().to_string(),
                Style::default().fg(theme::step_status_color(eff)),
            ),
        ];
        let rs = step
            .review_status
            .unwrap_or(crate::plan::ReviewStatus::Pending);
        if let Some((badge, color)) = crate::tui::widgets::outline_list::review_badge(rs) {
            status_spans.push(Span::raw("  "));
            status_spans.push(Span::styled(
                badge,
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ));
        }
        if let Some(cid) = step.corrects_step_id.as_deref()
            && let Some(corrected) = app.steps.iter().find(|s| s.id == cid)
        {
            status_spans.push(Span::styled(
                format!("  ↳ corrects {}", corrected.short_id),
                Style::default().fg(theme::CHROME_DIM),
            ));
        }
        lines.push(Line::from(status_spans));
    }

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

    let line_count = lines_visual_line_count(&lines, area.width);
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    render_scrolled_paragraph(frame, app, Pane::StepPrompt, area, para, line_count);
}

/// Render the Appended pane body: read-only retry context for the focused
/// attempt. Spec wording (TUI-plan.md §8) is "previous diff, test output,
/// modified files" — i.e., the data sourced from attempt N-1 and appended to
/// attempt N's prompt. Attempt 1 has no previous attempt, so it renders a
/// dim placeholder; an empty execution log renders the same shape it had
/// before any attempts ran.
fn render_appended(frame: &mut Frame, app: &mut StepDetailApp, area: Rect) {
    if app.execution_logs.is_empty() {
        render_scrolled_paragraph(
            frame,
            app,
            Pane::Appended,
            area,
            Paragraph::new(Span::styled(
                "(retry context appears here once an attempt has run)",
                Style::default().fg(theme::CHROME_DIM),
            ))
            .wrap(Wrap { trim: false }),
            1,
        );
        return;
    }

    // First attempt has no preceding log to source retry context from.
    if app.appended_attempt_index == 0 {
        render_scrolled_paragraph(
            frame,
            app,
            Pane::Appended,
            area,
            Paragraph::new(Span::styled(
                "(first attempt — no appended retry context)",
                Style::default().fg(theme::CHROME_DIM),
            ))
            .wrap(Wrap { trim: false }),
            1,
        );
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

    let line_count = lines_visual_line_count(&lines, area.width);
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    render_scrolled_paragraph(frame, app, Pane::Appended, area, para, line_count);
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
fn render_tests(frame: &mut Frame, app: &mut StepDetailApp, area: Rect) {
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
    let line_count = lines_visual_line_count(&lines, area.width);
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    render_scrolled_paragraph(frame, app, Pane::Tests, area, para, line_count);
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
        (
            BottomCell::Harness,
            "Harness",
            Some(app.effective_harness()),
        ),
        (BottomCell::Model, "Model", app.effective_model()),
        (BottomCell::Agent, "Agent", app.effective_agent()),
        (
            BottomCell::ChangePolicy,
            "Change policy",
            Some(app.effective_change_policy().to_string()),
        ),
    ];

    let row_focused = app.focused_pane == Pane::BottomRow;
    for (i, (cell, label, value)) in entries.iter().enumerate() {
        if cells[i].width == 0 {
            continue;
        }
        let cell_focused = row_focused && app.bottom_focus == *cell;
        let label_style = if cell_focused {
            Style::default()
                .fg(theme::CURSOR)
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::REVERSED)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        };
        let value_span = match value {
            Some(s) if !s.is_empty() => {
                if *cell == BottomCell::Harness
                    && let Some(color) = crate::output::harness_color(s)
                {
                    Span::styled(
                        s.clone(),
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    )
                } else {
                    Span::raw(s.clone())
                }
            }
            _ => Span::styled(
                EMPTY_CELL.to_string(),
                Style::default().fg(theme::CHROME_DIM),
            ),
        };
        let line = Line::from(vec![
            Span::styled(format!("{label}: "), label_style),
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
        // §3.3 derived overlay (open interruption — question or blocker).
        StepStatus::Blocked => "?",
    }
}

fn status_style(status: StepStatus) -> Style {
    // Color comes from the single TUI-wide §12.5 mapping
    // (`theme::step_status_color`). The only per-status *non-color* styling
    // kept here is the bold emphasis on an in-progress step (a weight
    // decision, not a color choice).
    let style = Style::default().fg(theme::step_status_color(status));
    match status {
        StepStatus::InProgress => style.add_modifier(Modifier::BOLD),
        _ => style,
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
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        }
    }

    fn make_steps(n: usize) -> Vec<Step> {
        (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                short_id: String::new(),
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
                retry_strategy: None,
                review_enabled: None,
                review_status: None,
                corrects_step_id: None,
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
            cycle_index: 0,
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
    fn pane_order_matches_four_layer_layout() {
        // The four-layer prompt model collapses the prompt panes to
        // Global → Project → Plan → Step; §17 adds the OpenQuestions pane
        // between StepPrompt and Appended.
        assert_eq!(
            Pane::ORDER,
            [
                Pane::GlobalPrompt,
                Pane::ProjectPrompt,
                Pane::PlanPrompt,
                Pane::StepPrompt,
                Pane::OpenQuestions,
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
        app.focused_pane = Pane::GlobalPrompt;
        let mut seen = vec![app.focused_pane];
        for _ in 0..Pane::ORDER.len() {
            app.focus_down();
            seen.push(app.focused_pane);
        }
        // After ORDER.len() down-presses we should have walked through every
        // pane and wrapped back to the start.
        assert_eq!(seen.first(), Some(&Pane::GlobalPrompt));
        assert_eq!(seen.last(), Some(&Pane::GlobalPrompt));
        // The middle of the trace should hit each pane exactly once.
        let middle: Vec<Pane> = seen[..Pane::ORDER.len()].to_vec();
        assert_eq!(middle, Pane::ORDER.to_vec());
    }

    #[test]
    fn focus_up_walks_stack_in_reverse_and_wraps() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::GlobalPrompt;
        // First up-press wraps to the bottom row.
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::BottomRow);
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::Tests);
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::Appended);
    }

    #[test]
    fn focus_down_from_bottom_row_wraps_to_global() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::BottomRow;
        app.focus_down();
        assert_eq!(app.focused_pane, Pane::GlobalPrompt);
    }

    #[test]
    fn focus_up_from_global_wraps_to_bottom_row() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::GlobalPrompt;
        app.focus_up();
        assert_eq!(app.focused_pane, Pane::BottomRow);
    }

    #[test]
    fn focus_down_from_step_prompt_advances_to_open_questions() {
        // §17 inserts OpenQuestions between StepPrompt and Appended, so the
        // first down-press from the initial focus lands on OpenQuestions.
        let mut app = make_app(3, 0);
        app.focus_down();
        assert_eq!(app.focused_pane, Pane::OpenQuestions);
    }

    #[test]
    fn focused_pane_scroll_clamps_to_last_rendered_metrics() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::StepPrompt;
        app.set_pane_metrics(Pane::StepPrompt, 3, 7);

        for _ in 0..10 {
            app.scroll_focused_pane_down();
        }
        assert_eq!(app.pane_scroll(Pane::StepPrompt), 4);

        app.page_focused_pane_up();
        assert_eq!(app.pane_scroll(Pane::StepPrompt), 1);
        app.scroll_focused_pane_to_top();
        assert_eq!(app.pane_scroll(Pane::StepPrompt), 0);
        app.scroll_focused_pane_to_bottom();
        assert_eq!(app.pane_scroll(Pane::StepPrompt), 4);
    }

    #[test]
    fn bottom_row_does_not_scroll() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::BottomRow;
        app.set_pane_metrics(Pane::BottomRow, 2, 20);

        app.scroll_focused_pane_down();
        app.page_focused_pane_down();
        app.scroll_focused_pane_to_bottom();

        assert_eq!(app.pane_scroll(Pane::BottomRow), 0);
    }

    // -- Open-question pane (TUI-plan.md §17) -------------------------------

    fn make_question(step_id: &str, q: &str, suggestions: &[&str]) -> storage::OpenQuestion {
        storage::OpenQuestion {
            id: format!("q-{step_id}-{q}").chars().take(60).collect(),
            step_id: step_id.to_string(),
            plan_id: "p1".to_string(),
            plan_slug: "tui-v1".to_string(),
            step_num: 1,
            step_title: "Step".to_string(),
            attempt: 1,
            question: q.to_string(),
            suggestions: suggestions.iter().map(|s| s.to_string()).collect(),
            kind: crate::plan::InterruptionKind::Question,
            asked_at: "2026-05-04T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn set_open_questions_for_step_clamps_cursor() {
        let mut app = make_app(3, 0);
        app.set_open_questions_for_step(vec![
            make_question("s0", "q1", &["a", "b"]),
            make_question("s0", "q2", &["c"]),
            make_question("s0", "q3", &[]),
        ]);
        app.selected_question_index = 2;
        // Refresh with two questions — cursor must clamp into the new range.
        app.set_open_questions_for_step(vec![
            make_question("s0", "q1", &["a"]),
            make_question("s0", "q2", &["b"]),
        ]);
        assert_eq!(app.selected_question_index, 1);
    }

    #[test]
    fn set_open_questions_for_step_resets_cursor_when_empty() {
        let mut app = make_app(3, 0);
        app.set_open_questions_for_step(vec![make_question("s0", "q1", &[])]);
        app.selected_question_index = 0;
        app.set_open_questions_for_step(vec![]);
        assert_eq!(app.selected_question_index, 0);
        assert!(!app.has_open_questions_for_step());
    }

    #[test]
    fn select_question_next_wraps_around() {
        let mut app = make_app(3, 0);
        app.set_open_questions_for_step(vec![
            make_question("s0", "q1", &[]),
            make_question("s0", "q2", &[]),
        ]);
        assert_eq!(app.selected_question_index, 0);
        app.select_question_next();
        assert_eq!(app.selected_question_index, 1);
        app.select_question_next();
        assert_eq!(app.selected_question_index, 0);
    }

    #[test]
    fn select_question_prev_wraps_around() {
        let mut app = make_app(3, 0);
        app.set_open_questions_for_step(vec![
            make_question("s0", "q1", &[]),
            make_question("s0", "q2", &[]),
            make_question("s0", "q3", &[]),
        ]);
        app.select_question_prev();
        assert_eq!(app.selected_question_index, 2);
        app.select_question_prev();
        assert_eq!(app.selected_question_index, 1);
    }

    #[test]
    fn select_question_with_zero_or_one_is_noop() {
        let mut app = make_app(3, 0);
        // Zero questions: navigation is a no-op.
        app.select_question_next();
        assert_eq!(app.selected_question_index, 0);
        app.select_question_prev();
        assert_eq!(app.selected_question_index, 0);
        // One question: cursor stays put.
        app.set_open_questions_for_step(vec![make_question("s0", "q1", &[])]);
        app.select_question_next();
        assert_eq!(app.selected_question_index, 0);
        app.select_question_prev();
        assert_eq!(app.selected_question_index, 0);
    }

    #[test]
    fn open_answer_modal_only_when_pane_focused_and_questions_present() {
        let mut app = make_app(3, 0);
        // No questions → no-op.
        assert!(!app.open_answer_modal());
        assert!(app.answer_modal.is_none());

        // Questions present but pane not focused → no-op.
        app.set_open_questions_for_step(vec![make_question(
            "s0",
            "Pick crate",
            &["tracing", "log"],
        )]);
        app.focused_pane = Pane::StepPrompt;
        assert!(!app.open_answer_modal());
        assert!(app.answer_modal.is_none());

        // Pane focused → modal opens with the focused question's data.
        app.focused_pane = Pane::OpenQuestions;
        assert!(app.open_answer_modal());
        let modal = app.answer_modal.as_ref().expect("modal opened");
        assert_eq!(modal.question, "Pick crate");
        assert_eq!(
            modal.suggestions,
            vec!["tracing".to_string(), "log".to_string()]
        );
    }

    #[test]
    fn open_answer_modal_idempotent_while_modal_open() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::OpenQuestions;
        app.set_open_questions_for_step(vec![make_question("s0", "Q", &["a"])]);
        assert!(app.open_answer_modal());
        // Second call returns false — the modal is already showing the
        // first question; we don't replace it on a second `a` press.
        assert!(!app.open_answer_modal());
    }

    #[test]
    fn open_answer_modal_blocked_when_read_only() {
        use crate::tui::read_only::ReadOnly;
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::OpenQuestions;
        app.set_open_questions_for_step(vec![make_question("s0", "Q", &["a"])]);
        app.set_read_only(ReadOnly::Locked { pid: 4242 });
        assert!(!app.open_answer_modal());
        assert!(app.answer_modal.is_none());
    }

    #[test]
    fn close_answer_modal_clears_state() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::OpenQuestions;
        app.set_open_questions_for_step(vec![make_question("s0", "Q", &["a"])]);
        app.open_answer_modal();
        app.close_answer_modal();
        assert!(app.answer_modal.is_none());
    }

    // -- Resume modal logic (TUI-plan.md §17) -------------------------------

    #[test]
    fn note_answer_persisted_opens_resume_modal_when_plan_count_zero() {
        let mut app = make_app(3, 0);
        app.set_plan_open_questions_count(0);
        app.note_answer_persisted(true);
        let modal = app.resume_modal.as_ref().expect("resume modal opened");
        assert_eq!(modal.plan_slug, "tui-v1");
        assert!(modal.current_branch);
    }

    #[test]
    fn note_answer_persisted_does_not_open_modal_when_questions_remain() {
        let mut app = make_app(3, 0);
        app.set_plan_open_questions_count(2);
        app.note_answer_persisted(false);
        assert!(app.resume_modal.is_none());
    }

    #[test]
    fn note_answer_persisted_closes_answer_modal() {
        let mut app = make_app(3, 0);
        app.focused_pane = Pane::OpenQuestions;
        app.set_open_questions_for_step(vec![make_question("s0", "Q", &["a"])]);
        app.open_answer_modal();
        app.set_plan_open_questions_count(1);
        // Answer-modal state must clear regardless of whether the plan-wide
        // count drops to zero.
        app.note_answer_persisted(false);
        assert!(app.answer_modal.is_none());
        assert!(app.resume_modal.is_none());
    }

    #[test]
    fn note_answer_persisted_idempotent_for_resume_modal() {
        // Calling the helper twice in a row (e.g. polling re-detected the
        // count is zero) must not stack two resume modals.
        let mut app = make_app(3, 0);
        app.set_plan_open_questions_count(0);
        app.note_answer_persisted(false);
        let first = app.resume_modal.clone();
        app.note_answer_persisted(true);
        // The second call is a no-op while the modal stays open — the
        // current_branch flag from the first call is preserved.
        assert_eq!(app.resume_modal, first);
    }

    #[test]
    fn close_resume_modal_clears_state() {
        let mut app = make_app(3, 0);
        app.set_plan_open_questions_count(0);
        app.note_answer_persisted(false);
        assert!(app.resume_modal.is_some());
        app.close_resume_modal();
        assert!(app.resume_modal.is_none());
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
        assert_eq!(app.update_auto_zen(AUTO_ZEN_WIDTH_THRESHOLD - 1), None);
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
    fn universal_pane_renders_config_prompt() {
        let plan = make_plan();
        let steps = make_steps(3);
        let config = Config {
            prompt: Some("CFG-PROMPT-MARKER".to_string()),
            ..Config::default()
        };
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &config,
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(160, 100, &mut app);
        assert!(screen.contains("Global (universal)"), "{screen}");
        assert!(screen.contains("CFG-PROMPT-MARKER"), "{screen}");
    }

    #[test]
    fn universal_pane_shows_none_when_unset() {
        let plan = make_plan();
        let steps = make_steps(1);
        // Default Config has a global prompt seeded by `ralph init`; clear
        // it so the pane shows the (none) placeholder.
        let config = Config {
            prompt: None,
            ..Config::default()
        };
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &config,
            ProjectSettings::default(),
            Vec::new(),
        );
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
            prompt: Some("PROJ-PROMPT-MARK".to_string()),
        };
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            project_settings,
            Vec::new(),
        );
        let screen = render_to_string(160, 100, &mut app);
        assert!(screen.contains("Project prompt"), "{screen}");
        assert!(screen.contains("PROJ-PROMPT-MARK"), "{screen}");
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
        let screen = render_to_string(160, 100, &mut app);
        assert!(screen.contains("STEP-TITLE-MARK"), "{screen}");
        assert!(screen.contains("STEP-DESC-MARK"), "{screen}");
        assert!(screen.contains("CRIT-A-MARK"), "{screen}");
        assert!(screen.contains("CRIT-B-MARK"), "{screen}");
        assert!(screen.contains("Acceptance:"), "{screen}");
    }

    #[test]
    fn step_prompt_pane_surfaces_review_badge_and_corrects_marker() {
        // docs/dag-redesign.md §12.1/§12.5: the Step pane must surface the
        // review verdict badge and the `↳ corrects <short_id>` marker for a
        // reviewer-inserted corrective step, colored via the §12.5 mapping.
        let plan = make_plan();
        let mut steps = make_steps(2);
        steps[0].short_id = "aaaa1111".to_string();
        steps[1].short_id = "apri0000".to_string();
        steps[1].review_status = Some(crate::plan::ReviewStatus::Failed);
        steps[1].corrects_step_id = Some(steps[0].id.clone());
        let mut app = StepDetailApp::new(
            plan,
            steps,
            1,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(160, 100, &mut app);
        assert!(
            screen.contains("review✘"),
            "review badge missing:\n{screen}"
        );
        assert!(
            screen.contains("↳ corrects aaaa1111"),
            "corrects marker missing:\n{screen}"
        );
    }

    #[test]
    fn step_prompt_pane_shows_blocked_overlay_when_step_has_open_question() {
        // §3.3 derived overlay: an open interruption makes the step present
        // as Blocked in step-detail too (one concept, one color, TUI-wide).
        let plan = make_plan();
        let mut steps = make_steps(1);
        steps[0].short_id = "bbbb2222".to_string();
        steps[0].status = StepStatus::InProgress;
        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        app.set_open_questions_for_step(vec![storage::OpenQuestion {
            id: "q1".to_string(),
            step_id: "s0".to_string(),
            plan_id: "p1".to_string(),
            plan_slug: "plan".to_string(),
            step_num: 1,
            step_title: "t".to_string(),
            attempt: 1,
            question: "Q?".to_string(),
            suggestions: vec![],
            kind: crate::plan::InterruptionKind::Question,
            asked_at: Utc::now().to_rfc3339(),
        }]);
        let screen = render_to_string(160, 100, &mut app);
        assert!(
            screen.contains("blocked"),
            "blocked overlay text missing in step pane:\n{screen}"
        );
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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
        let logs = vec![
            make_log(1, None, None),
            make_log(2, None, None),
            make_log(3, None, None),
        ];
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

        let mut app = StepDetailApp::new(
            plan,
            steps,
            0,
            &config,
            ProjectSettings::default(),
            Vec::new(),
        );
        let screen = render_to_string(160, 80, &mut app);
        assert!(
            screen.contains(EMPTY_CELL),
            "expected em-dash for empty cell: {screen}"
        );
    }

    // -- format_prompt_pane / parse_prompt_pane (TUI-plan.md §8 + §18 Q3) --

    #[test]
    fn format_prompt_pane_normalizes_trailing_newline() {
        // A value without a trailing newline gets one appended so editors
        // that auto-add a final newline don't make the round-trip look
        // "changed"; a value that already ends in one is left as-is.
        assert_eq!(format_prompt_pane(Some("hello")), "hello\n");
        assert_eq!(format_prompt_pane(Some("hello\n")), "hello\n");
    }

    #[test]
    fn format_prompt_pane_empty_for_none_or_blank() {
        assert_eq!(format_prompt_pane(None), "");
        assert_eq!(format_prompt_pane(Some("")), "");
        // Round-trips back to None (the "clear this layer" workflow).
        assert_eq!(parse_prompt_pane(&format_prompt_pane(None)), None);
    }

    #[test]
    fn parse_prompt_pane_round_trips_multiline_content() {
        let s = format_prompt_pane(Some("line a\n\nline b"));
        assert_eq!(parse_prompt_pane(&s).as_deref(), Some("line a\n\nline b"));
    }

    #[test]
    fn parse_prompt_pane_treats_whitespace_only_as_none() {
        assert_eq!(parse_prompt_pane("   \n\n  \n"), None);
        assert_eq!(parse_prompt_pane(""), None);
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
    fn edit_universal_pane_seeds_editor_with_current_prompt() {
        let mut config = Config {
            prompt: Some("CURRENT PROMPT".to_string()),
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
        assert_eq!(buf, "CURRENT PROMPT\n");
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
            prompt: Some("hello".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let initial = format_prompt_pane(config.prompt.as_deref());
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
        assert_eq!(config.prompt.as_deref(), Some("hello"));
    }

    #[test]
    fn edit_universal_pane_persists_on_change() {
        let mut config = Config {
            prompt: Some("old prompt".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let outcome = app
            .edit_universal_pane(
                &mut config,
                tmp.path(),
                fake_editor(Some("new prompt".to_string())),
            )
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // In-memory config and app mirror both updated.
        assert_eq!(config.prompt.as_deref(), Some("new prompt"));
        assert_eq!(app.config_prompt.as_deref(), Some("new prompt"));

        // File written and parses back to the new value.
        let path = tmp.path().join("config.json");
        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        let reloaded: Config = serde_json::from_str(&contents).unwrap();
        assert_eq!(reloaded.prompt.as_deref(), Some("new prompt"));
    }

    #[test]
    fn edit_universal_pane_can_clear_the_prompt() {
        // Saving an empty buffer clears the prompt to `None` — this is the
        // "wipe my universal prompt" workflow.
        let mut config = Config {
            prompt: Some("X".to_string()),
            ..Config::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let mut app = make_step_app();

        let outcome = app
            .edit_universal_pane(&mut config, tmp.path(), fake_editor(Some(String::new())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert_eq!(config.prompt, None);
        assert_eq!(app.config_prompt, None);
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
        // Editor writes a known prompt, ignoring the seeded file contents.
        std::fs::write(&script, "#!/bin/sh\nprintf 'MOCK PROMPT\\n' > \"$1\"\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut config = Config {
            prompt: Some("before".to_string()),
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
        let reloaded: Config =
            serde_json::from_str(&std::fs::read_to_string(tmp.path().join("config.json")).unwrap())
                .unwrap();
        assert_eq!(reloaded.prompt.as_deref(), Some("MOCK PROMPT"));
    }

    // -- edit_project_pane ------------------------------------------------

    /// Build a StepDetailApp pointed at a fresh in-memory plan row in `conn`.
    /// The DB state and the in-memory `app.plan` / `app.project_settings`
    /// match at construction time so writes can be verified by reading the
    /// row back.
    fn setup_project_app(conn: &Connection, project: &str) -> StepDetailApp {
        let plan =
            crate::storage::create_plan(conn, "tui-v1", project, "tui-v1", "desc", None, None, &[])
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
        assert_eq!(stored.prompt, None);
    }

    #[test]
    fn edit_project_pane_no_changes_skips_writes() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        // Seed the DB row (the active source for `/proj`, which has no
        // checked-in `.ralph/prompt.md`). The handler resolves the buffer
        // from the resolver, not the in-memory mirror.
        storage::set_project_prompt(&conn, "/proj", Some("a")).unwrap();
        let buffer = format_prompt_pane(Some("a"));
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
        // Row unchanged.
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt.as_deref(), Some("a"));
    }

    #[test]
    fn edit_project_pane_persists_changed_prompt() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some("hello world".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // The new prompt landed in the project_settings row.
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt.as_deref(), Some("hello world"));
        // App's own mirror is in sync so the pane re-renders correctly.
        assert_eq!(app.project_settings.prompt.as_deref(), Some("hello world"));
    }

    #[test]
    fn edit_project_pane_can_clear_the_prompt() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        // Seed a prompt via the storage helper so the row exists.
        storage::set_project_prompt(&conn, "/proj", Some("PROMPT")).unwrap();
        app.project_settings.prompt = Some("PROMPT".to_string());

        // User clears the prompt by saving an empty buffer.
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(String::new())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        let stored = storage::get_project_settings(&conn, "/proj").unwrap();
        assert_eq!(stored.prompt, None);
        assert_eq!(app.project_settings.prompt, None);
    }

    #[test]
    fn edit_project_pane_routes_to_db_when_no_file() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        let mut app = setup_project_app(&conn, &project);

        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some("to db".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);

        // The DB column got the value; no file was created.
        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("to db")
        );
        assert!(!storage::project_prompt_file_path(&project).exists());
    }

    #[test]
    fn edit_project_pane_routes_to_file_when_present() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        // A checked-in file is the active source; the DB also has a value
        // that must NOT be touched.
        storage::write_project_prompt_file(&project, "old file value").unwrap();
        storage::set_project_prompt(&conn, &project, Some("db untouched")).unwrap();
        let mut app = setup_project_app(&conn, &project);

        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some("new file value".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);

        // File got the edit; DB column is unchanged.
        assert_eq!(
            storage::read_project_prompt_file(&project)
                .unwrap()
                .as_deref(),
            Some("new file value")
        );
        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("db untouched")
        );
    }

    #[test]
    fn edit_project_pane_clearing_file_backed_prompt_deletes_file() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        storage::write_project_prompt_file(&project, "file content").unwrap();
        let mut app = setup_project_app(&conn, &project);

        // Saving an empty buffer over a file-backed prompt removes the file.
        let outcome = app
            .edit_project_pane(&conn, fake_editor(Some(String::new())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert!(!storage::project_prompt_file_path(&project).exists());
    }

    // -- edit_plan_prompt_pane --------------------------------------------

    #[test]
    fn edit_plan_prompt_pane_seeds_editor_with_plan_description() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        app.plan.description = "CURRENT PLAN DESC".to_string();

        let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
        let outcome = app
            .edit_plan_prompt_pane(&conn, capturing_editor(seen.clone()))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoEditor);
        assert_eq!(
            seen.borrow().clone().expect("editor invoked"),
            "CURRENT PLAN DESC\n"
        );
    }

    #[test]
    fn edit_plan_prompt_pane_no_changes_when_unchanged() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");
        // setup_project_app created the plan with description "desc".
        let buffer = format_prompt_pane(Some("desc"));
        let outcome = app
            .edit_plan_prompt_pane(&conn, fake_editor(Some(buffer)))
            .unwrap();
        assert_eq!(outcome, EditOutcome::NoChanges);
    }

    #[test]
    fn edit_plan_prompt_pane_persists_changed_description() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");

        let outcome = app
            .edit_plan_prompt_pane(&conn, fake_editor(Some("NEW DESC".to_string())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        assert_eq!(app.plan.description, "NEW DESC");
        let reloaded = storage::get_plan_by_id(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded.description, "NEW DESC");
    }

    #[test]
    fn edit_plan_prompt_pane_empty_buffer_clears_to_empty_string() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_project_app(&conn, "/proj");

        let outcome = app
            .edit_plan_prompt_pane(&conn, fake_editor(Some(String::new())))
            .unwrap();
        assert_eq!(outcome, EditOutcome::Saved);
        // plan.description is a String, not Option — clears to "".
        assert_eq!(app.plan.description, "");
        let reloaded = storage::get_plan_by_id(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded.description, "");
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
        assert_eq!(
            parts.description,
            "First desc line\n\nSecond desc paragraph"
        );
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
        assert!(err.to_string().contains("Title is empty"), "got: {err}");
    }

    #[test]
    fn parse_step_pane_rejects_multi_line_title() {
        let text = "# Title\nline-one\nline-two\n## Description\n## Acceptance criteria\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(err.to_string().contains("single line"), "got: {err}");
    }

    #[test]
    fn parse_step_pane_rejects_non_bullet_in_criteria() {
        let text = "# Title\nT\n## Description\n## Acceptance criteria\nstray paragraph\n";
        let err = parse_step_pane(text).unwrap_err();
        assert!(err.to_string().contains("not a bullet"), "got: {err}");
    }

    // -- Tests pane format/parse round-trips -----------------------------

    #[test]
    fn parse_tests_pane_round_trips_simple_list() {
        let formatted = format_tests_pane(&["cargo build".to_string(), "cargo test".to_string()]);
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

    // -- edit_step_prompt_pane --------------------------------------------

    /// Build a StepDetailApp with a plan + a single step materialized in `conn`,
    /// so writes via `update_step_fields_ext` land on a real row that can
    /// then be reloaded.
    fn setup_step_app(conn: &Connection) -> StepDetailApp {
        let plan =
            crate::storage::create_plan(conn, "tui-v1", "/proj", "tui-v1", "desc", None, None, &[])
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
        let buffer = format_tests_pane(&["cargo test".to_string(), "cargo clippy".to_string()]);
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
            vec![
                "cargo test".to_string(),
                "cargo clippy -- -D warnings".to_string()
            ]
        );
    }

    // -- Bottom-row sub-cell focus + picker integration (TUI-plan §8 + step 27) --

    use crate::tui::views::step_detail_picker::{
        BottomCell, PickerKind, PickerMode, PickerOutcome,
    };
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn k(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// Build a step-app with a real DB-backed plan + step row so picker
    /// submissions can be verified by reloading the row. Mirrors
    /// `setup_step_app` but used here for picker tests.
    fn setup_picker_app(conn: &Connection) -> StepDetailApp {
        let plan =
            crate::storage::create_plan(conn, "tui-v1", "/proj", "tui-v1", "desc", None, None, &[])
                .unwrap();
        let (step, _pos) = crate::storage::create_step(
            conn,
            &plan.id,
            "Original title",
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
        StepDetailApp::new(
            plan,
            vec![step],
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        )
    }

    // -- bottom_focus: default + h/l movement -----------------------------

    #[test]
    fn bottom_focus_defaults_to_harness() {
        let app = make_app(1, 0);
        assert_eq!(app.bottom_focus, BottomCell::Harness);
    }

    #[test]
    fn handle_right_on_bottom_row_walks_cells() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.handle_right();
        assert_eq!(app.bottom_focus, BottomCell::Model);
        app.handle_right();
        assert_eq!(app.bottom_focus, BottomCell::Agent);
        app.handle_right();
        assert_eq!(app.bottom_focus, BottomCell::ChangePolicy);
        // At the rightmost — no-op.
        app.handle_right();
        assert_eq!(app.bottom_focus, BottomCell::ChangePolicy);
        assert!(!app.should_pop);
    }

    #[test]
    fn handle_left_on_bottom_row_walks_cells_and_pops_at_leftmost() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.handle_left();
        assert_eq!(app.bottom_focus, BottomCell::Agent);
        app.handle_left();
        assert_eq!(app.bottom_focus, BottomCell::Model);
        app.handle_left();
        assert_eq!(app.bottom_focus, BottomCell::Harness);
        // At the leftmost — h falls through to popping the view per §8.
        assert!(!app.should_pop);
        app.handle_left();
        assert!(app.should_pop);
    }

    #[test]
    fn handle_right_on_non_bottom_pane_does_not_move_bottom_focus() {
        // Sanity check — l on Step prompt must not pollute bottom_focus.
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::StepPrompt;
        app.handle_right();
        assert_eq!(app.bottom_focus, BottomCell::Harness);
    }

    #[test]
    fn handle_left_on_non_bottom_pane_pops_without_touching_bottom_focus() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::StepPrompt;
        app.bottom_focus = BottomCell::Model; // pre-existing focus
        app.handle_left();
        assert!(app.should_pop);
        assert_eq!(app.bottom_focus, BottomCell::Model);
    }

    // -- open_picker_for_focused_cell -------------------------------------

    #[test]
    fn open_picker_no_op_when_not_on_bottom_row() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::StepPrompt;
        app.open_picker_for_focused_cell(&[]);
        assert!(app.picker.is_none());
    }

    #[test]
    fn open_picker_no_op_when_already_open() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.open_picker_for_focused_cell(&[]);
        assert!(app.picker.is_some());
        let kind_before = app.picker.as_ref().unwrap().kind;
        // Try to open again with a different focus — the existing picker
        // must not be replaced.
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);
        assert_eq!(app.picker.as_ref().unwrap().kind, kind_before);
    }

    #[test]
    fn open_picker_for_harness_cell_uses_config_keys() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::Harness;
        app.open_picker_for_focused_cell(&[]);
        let picker = app.picker.as_ref().expect("picker open");
        assert_eq!(picker.kind, PickerKind::Harness);
        // The default Config seeds at least the "claude" harness; the
        // picker should preselect the effective harness.
        assert!(!picker.items.is_empty());
    }

    #[test]
    fn open_picker_for_model_cell_includes_custom() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::Model;
        app.open_picker_for_focused_cell(&[]);
        let picker = app.picker.as_ref().expect("picker open");
        assert_eq!(picker.kind, PickerKind::Model);
        // At least one entry — the synthetic Custom… row is always present.
        assert!(!picker.items.is_empty());
    }

    #[test]
    fn open_picker_for_agent_cell_uses_provided_list() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::Agent;
        app.open_picker_for_focused_cell(&["alpha".into(), "beta".into()]);
        let picker = app.picker.as_ref().expect("picker open");
        assert_eq!(picker.kind, PickerKind::Agent);
        assert_eq!(picker.items.len(), 2);
    }

    #[test]
    fn open_picker_for_change_policy_lists_two_options() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);
        let picker = app.picker.as_ref().expect("picker open");
        assert_eq!(picker.kind, PickerKind::ChangePolicy);
        assert_eq!(picker.items.len(), 2);
    }

    #[test]
    fn close_picker_clears_state() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.open_picker_for_focused_cell(&[]);
        assert!(app.picker.is_some());
        app.close_picker();
        assert!(app.picker.is_none());
    }

    // -- picker_handle_key key plumbing -----------------------------------

    #[test]
    fn picker_handle_key_returns_none_when_no_picker_open() {
        let mut app = make_app(1, 0);
        assert!(app.picker_handle_key(k(KeyCode::Esc)).is_none());
    }

    #[test]
    fn picker_handle_key_esc_returns_cancelled() {
        // Acceptance criterion: esc cancellation (no DB write).
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);
        let outcome = app.picker_handle_key(k(KeyCode::Esc)).unwrap();
        assert_eq!(outcome, PickerOutcome::Cancelled);
    }

    #[test]
    fn picker_handle_key_enter_on_value_returns_submit() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);
        // Move from required (idx 0) to optional (idx 1).
        let _ = app.picker_handle_key(k(KeyCode::Char('j')));
        let outcome = app.picker_handle_key(k(KeyCode::Enter)).unwrap();
        match outcome {
            PickerOutcome::Submit { kind, value } => {
                assert_eq!(kind, PickerKind::ChangePolicy);
                assert_eq!(value, "optional");
            }
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn picker_handle_key_enter_on_custom_flips_into_input_mode() {
        let mut app = make_app(1, 0);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::Model;
        app.open_picker_for_focused_cell(&[]);
        // Walk to the last item — the synthetic Custom… row.
        let len = app.picker.as_ref().unwrap().items.len();
        for _ in 0..len.saturating_sub(1) {
            let _ = app.picker_handle_key(k(KeyCode::Char('j')));
        }
        // Confirm — picker_handle_key should flip into input mode.
        let outcome = app.picker_handle_key(k(KeyCode::Enter)).unwrap();
        assert_eq!(outcome, PickerOutcome::Pending);
        assert!(matches!(
            app.picker.as_ref().unwrap().mode,
            PickerMode::CustomInput { .. }
        ));
    }

    // -- apply_picker_submit: DB writes -----------------------------------

    #[test]
    fn apply_picker_submit_harness_writes_through_update_step_fields_ext() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.apply_picker_submit(&conn, PickerKind::Harness, "codex")
            .unwrap();
        // In-memory step refreshed.
        assert_eq!(app.steps[0].harness.as_deref(), Some("codex"));
        // DB row reloads to the same value.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].harness.as_deref(), Some("codex"));
    }

    #[test]
    fn apply_picker_submit_model_writes_through_update_step_fields_ext() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.apply_picker_submit(&conn, PickerKind::Model, "claude-opus-4-7")
            .unwrap();
        assert_eq!(app.steps[0].model.as_deref(), Some("claude-opus-4-7"));
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].model.as_deref(), Some("claude-opus-4-7"));
    }

    #[test]
    fn apply_picker_submit_agent_writes_through_update_step_fields_ext() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.apply_picker_submit(&conn, PickerKind::Agent, "rust-impl")
            .unwrap();
        assert_eq!(app.steps[0].agent.as_deref(), Some("rust-impl"));
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].agent.as_deref(), Some("rust-impl"));
    }

    #[test]
    fn apply_picker_submit_change_policy_writes_optional() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        // Default policy is required — switch to optional.
        assert_eq!(app.steps[0].change_policy, ChangePolicy::Required);
        app.apply_picker_submit(&conn, PickerKind::ChangePolicy, "optional")
            .unwrap();
        assert_eq!(app.steps[0].change_policy, ChangePolicy::Optional);
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].change_policy, ChangePolicy::Optional);
    }

    #[test]
    fn apply_picker_submit_change_policy_rejects_unknown_string() {
        // The picker can't produce an invalid string in normal use, but the
        // apply path validates as a defense against future code that builds
        // a PickerOutcome::Submit by hand.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        let res = app.apply_picker_submit(&conn, PickerKind::ChangePolicy, "garbage");
        assert!(res.is_err());
        // Step row untouched — change_policy stays at the default.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].change_policy, ChangePolicy::Required);
    }

    #[test]
    fn apply_picker_submit_no_op_on_empty_plan() {
        // Empty plan ⇒ no step row to write to. The apply path silently
        // returns Ok rather than panicking on the missing index.
        let conn = crate::db::open_memory().unwrap();
        let plan =
            crate::storage::create_plan(&conn, "empty", "/proj2", "empty", "desc", None, None, &[])
                .unwrap();
        let mut app = StepDetailApp::new(
            plan,
            Vec::new(),
            0,
            &Config::default(),
            ProjectSettings::default(),
            Vec::new(),
        );
        let res = app.apply_picker_submit(&conn, PickerKind::Harness, "codex");
        assert!(res.is_ok());
    }

    // -- End-to-end flows: open → confirm → apply / open → cancel ---------

    #[test]
    fn end_to_end_open_select_apply_change_policy() {
        // Acceptance: selection + DB write through the full picker flow.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);

        // Move from Required (idx 0) to Optional (idx 1) and confirm.
        let _ = app.picker_handle_key(k(KeyCode::Char('j')));
        let outcome = app.picker_handle_key(k(KeyCode::Enter)).unwrap();
        match outcome {
            PickerOutcome::Submit { kind, value } => {
                app.apply_picker_submit(&conn, kind, &value).unwrap();
                app.close_picker();
            }
            other => panic!("expected Submit, got {other:?}"),
        }
        assert!(app.picker.is_none());
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].change_policy, ChangePolicy::Optional);
    }

    #[test]
    fn end_to_end_open_esc_no_db_write() {
        // Acceptance: esc cancellation leaves the row untouched.
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::ChangePolicy;
        app.open_picker_for_focused_cell(&[]);
        let outcome = app.picker_handle_key(k(KeyCode::Esc)).unwrap();
        assert_eq!(outcome, PickerOutcome::Cancelled);
        app.close_picker();
        assert!(app.picker.is_none());
        // Row unchanged.
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].change_policy, ChangePolicy::Required);
    }

    #[test]
    fn end_to_end_model_custom_input_submits_typed_value() {
        let conn = crate::db::open_memory().unwrap();
        let mut app = setup_picker_app(&conn);
        app.focused_pane = Pane::BottomRow;
        app.bottom_focus = BottomCell::Model;
        app.open_picker_for_focused_cell(&[]);

        // Walk to the Custom… row (always last).
        let len = app.picker.as_ref().unwrap().items.len();
        for _ in 0..len.saturating_sub(1) {
            let _ = app.picker_handle_key(k(KeyCode::Char('j')));
        }
        // Confirm — flips into input mode.
        let _ = app.picker_handle_key(k(KeyCode::Enter)).unwrap();
        for c in "claude-opus-4-7".chars() {
            let _ = app.picker_handle_key(k(KeyCode::Char(c)));
        }
        // Submit the typed value.
        let outcome = app.picker_handle_key(k(KeyCode::Enter)).unwrap();
        match outcome {
            PickerOutcome::Submit { kind, value } => {
                assert_eq!(kind, PickerKind::Model);
                app.apply_picker_submit(&conn, kind, &value).unwrap();
            }
            other => panic!("expected Submit, got {other:?}"),
        }
        let reloaded = crate::storage::list_steps(&conn, &app.plan.id).unwrap();
        assert_eq!(reloaded[0].model.as_deref(), Some("claude-opus-4-7"));
    }

    // -- Read-only attach lockdown (TUI-plan.md §13.2) -------------------

    #[test]
    fn test_step_detail_read_only_default_is_editable() {
        let app = make_app(3, 0);
        assert!(!app.read_only.is_locked());
        assert!(app.can_edit_panes());
    }

    #[test]
    fn test_step_detail_set_read_only_blocks_can_edit() {
        let mut app = make_app(3, 0);
        app.set_read_only(ReadOnly::Locked { pid: 4242 });
        assert!(app.read_only.is_locked());
        assert!(
            !app.can_edit_panes(),
            "c (every pane edit) and a (answer question) must be blocked"
        );
    }

    #[test]
    fn test_step_detail_release_unlocks_can_edit() {
        let mut app = make_app(3, 0);
        app.set_read_only(ReadOnly::Locked { pid: 1 });
        assert!(!app.can_edit_panes());
        app.set_read_only(ReadOnly::Editable);
        assert!(
            app.can_edit_panes(),
            "edits must come back when lock is released"
        );
    }

    // -- Help overlay (TUI-plan.md §15) ---------------------------------

    #[test]
    fn step_detail_help_default_hidden() {
        let app = make_app(3, 0);
        assert!(!app.help.is_visible());
    }

    #[test]
    fn step_detail_help_intercepts_question_mark_and_esc() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = make_app(3, 0);
        let q = KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE);
        assert_eq!(
            app.help.intercept_key(q),
            crate::tui::help::InterceptResult::Opened
        );
        assert!(app.help.is_visible());

        let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        assert_eq!(
            app.help.intercept_key(esc),
            crate::tui::help::InterceptResult::Closed
        );
        assert!(!app.help.is_visible());
    }

    // -- Palette (TUI-plan.md §9) ---------------------------------------

    #[test]
    fn step_detail_palette_default_inactive() {
        let app = make_app(3, 0);
        assert!(!app.palette_active());
        assert!(app.palette_bar.is_none());
    }

    #[test]
    fn step_detail_palette_open_records_prefix() {
        let mut app = make_app(3, 0);
        app.open_palette('/');
        assert!(app.palette_active());
        assert_eq!(app.palette_bar.as_ref().unwrap().prefix, '/');
        app.close_palette();
        app.open_palette(':');
        assert_eq!(app.palette_bar.as_ref().unwrap().prefix, ':');
    }

    #[test]
    fn step_detail_palette_close_drops_state() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = make_app(3, 0);
        app.open_palette('/');
        let _ = app
            .palette_bar
            .as_mut()
            .unwrap()
            .on_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));
        assert_eq!(app.palette_bar.as_ref().unwrap().input, "r");
        app.close_palette();
        assert!(!app.palette_active());
    }

    #[test]
    fn step_detail_palette_esc_yields_cancel_outcome() {
        use crate::tui::widgets::palette_bar::PaletteBarOutcome;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = make_app(3, 0);
        app.open_palette('/');
        let out = app
            .palette_bar
            .as_mut()
            .unwrap()
            .on_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(out, PaletteBarOutcome::Cancel);
    }

    #[test]
    fn step_detail_palette_enter_yields_submit_outcome_and_parses() {
        use crate::tui::palette::PaletteCommand;
        use crate::tui::widgets::palette_bar::PaletteBarOutcome;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = make_app(3, 0);
        app.open_palette('/');
        let bar = app.palette_bar.as_mut().unwrap();
        for c in "step edit --tags".chars() {
            let _ = bar.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        let out = bar.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let input = match out {
            PaletteBarOutcome::Submit(s) => s,
            other => panic!("expected Submit, got {other:?}"),
        };
        assert_eq!(
            crate::tui::palette::parse(&input),
            Ok(PaletteCommand::StepEditTags)
        );
    }

    // -- Sidebar mouse-drag override (TUI-plan.md, step 27) ---------------

    /// Construct a [`MouseEvent`] at `(column, row)` with the given kind.
    /// Mirrors the helper in plan_detail's tests.
    fn mouse_event(
        column: u16,
        row: u16,
        kind: crossterm::event::MouseEventKind,
    ) -> crossterm::event::MouseEvent {
        crossterm::event::MouseEvent {
            kind,
            column,
            row,
            modifiers: crossterm::event::KeyModifiers::NONE,
        }
    }

    #[test]
    fn sidebar_w_override_defaults_to_none() {
        let app = make_app(3, 0);
        assert!(app.sidebar_w_override.is_none());
        assert_eq!(app.last_body_width, 0);
        assert_eq!(app.last_sidebar_w, 0);
        assert!(!app.dragging_sidebar);
    }

    #[test]
    fn sidebar_w_override_set_and_clear() {
        // (a) State-machine test: set → clamp → clear.
        let mut app = make_app(3, 0);
        app.sidebar_w_override = Some(30);
        assert_eq!(app.sidebar_w_override, Some(30));

        // The clamp is applied at the call site in `handle_mouse`; assert the
        // post-clamp values cover both bounds.
        let clamped_low = 1u16.clamp(4, 80);
        let clamped_high = 200u16.clamp(4, 80);
        assert_eq!(clamped_low, 4);
        assert_eq!(clamped_high, 80);

        app.sidebar_w_override = None;
        assert!(app.sidebar_w_override.is_none());
    }

    #[test]
    fn sidebar_drag_down_drag_up_sets_override() {
        // (b) Dispatcher-style: Down at the divider arms the drag, Drag
        //     updates `sidebar_w_override` to the cursor column, Up clears
        //     the drag flag.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = make_app(3, 0);
        app.last_body_width = 120;
        app.last_sidebar_w = 25;

        assert!(app.sidebar_w_override.is_none());
        assert!(!app.dragging_sidebar);

        // Press at column 25 (the divider): drag is armed.
        app.handle_mouse(mouse_event(25, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(app.dragging_sidebar);

        // Drag to column 40: override matches the cursor column.
        app.handle_mouse(mouse_event(40, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.sidebar_w_override, Some(40));

        app.handle_mouse(mouse_event(40, 5, MouseEventKind::Up(MouseButton::Left)));
        assert!(!app.dragging_sidebar);
    }

    #[test]
    fn sidebar_drag_press_off_divider_does_not_arm() {
        // ±1 column tolerance: pressing far from the divider should not
        // arm a drag, so subsequent drag events leave the override alone.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = make_app(3, 0);
        app.last_body_width = 120;
        app.last_sidebar_w = 25;

        // Divider at column 25; press at column 10.
        app.handle_mouse(mouse_event(10, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_sidebar);

        app.handle_mouse(mouse_event(60, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert!(
            app.sidebar_w_override.is_none(),
            "drag without arming must not set override"
        );
    }

    #[test]
    fn sidebar_drag_clamps_to_4_and_80() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = make_app(3, 0);
        app.last_body_width = 200;
        app.last_sidebar_w = 25;

        app.handle_mouse(mouse_event(25, 5, MouseEventKind::Down(MouseButton::Left)));
        // Drag far left — clamped to 4.
        app.handle_mouse(mouse_event(0, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.sidebar_w_override, Some(4));

        // Drag far right — clamped to 80.
        app.handle_mouse(mouse_event(150, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.sidebar_w_override, Some(80));

        app.handle_mouse(mouse_event(150, 5, MouseEventKind::Up(MouseButton::Left)));
        assert!(!app.dragging_sidebar);
    }

    #[test]
    fn sidebar_drag_within_one_column_arms() {
        // ±1 column hit-test: pressing one column either side of the
        // divider should still arm the drag.
        use crossterm::event::{MouseButton, MouseEventKind};
        for col in [24u16, 25, 26] {
            let mut app = make_app(3, 0);
            app.last_body_width = 120;
            app.last_sidebar_w = 25;
            app.handle_mouse(mouse_event(col, 5, MouseEventKind::Down(MouseButton::Left)));
            assert!(
                app.dragging_sidebar,
                "press at col {col} (divider 25) should arm drag",
            );
        }
    }

    #[test]
    fn sidebar_drag_starts_clears_user_zen() {
        // Dragging takes priority over zen — user_zen is dropped on Down so
        // the override path drives rendering.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = make_app(3, 0);
        app.last_body_width = 120;
        app.last_sidebar_w = 4;
        // User had toggled zen on (sidebar_w would be SIDEBAR_ZEN_WIDTH=4).
        let _ = app.toggle_zen();
        assert!(app.is_zen_mode());

        app.handle_mouse(mouse_event(4, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(app.dragging_sidebar);
        assert!(
            !app.is_zen_mode(),
            "starting a drag must drop user-driven zen so the override wins",
        );
    }

    #[test]
    fn handle_mouse_no_op_before_first_draw() {
        // Before the first frame `last_body_width` is zero; mouse events
        // must not panic and must not arm a drag.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = make_app(3, 0);
        assert_eq!(app.last_body_width, 0);

        app.handle_mouse(mouse_event(0, 0, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_sidebar);
        assert!(app.sidebar_w_override.is_none());
    }

    #[test]
    fn toggle_zen_clears_sidebar_override() {
        // (c) Pressing `z` resets the override so rendering falls back to
        //     the zen-derived constant. Verified both directions: the
        //     override is cleared, and `is_zen_mode` flips as before.
        let mut app = make_app(3, 0);
        app.sidebar_w_override = Some(50);
        assert_eq!(app.sidebar_w_override, Some(50));
        assert!(!app.is_zen_mode());

        let applied = app.toggle_zen();
        assert!(applied);
        assert!(app.is_zen_mode(), "z toggles zen on as before");
        assert!(
            app.sidebar_w_override.is_none(),
            "z must clear the override so the zen-derived constant applies",
        );

        // Toggling back off also keeps the override cleared.
        app.sidebar_w_override = Some(40);
        let applied = app.toggle_zen();
        assert!(applied);
        assert!(!app.is_zen_mode());
        assert!(app.sidebar_w_override.is_none());
    }

    #[test]
    fn toggle_zen_clears_override_even_when_auto_zen_forces() {
        // Auto-zen suppresses the user toggle, but the override clear still
        // happens so a subsequent terminal grow-back doesn't leave a stale
        // override behind.
        let mut app = make_app(3, 0);
        app.update_auto_zen(80);
        assert!(app.is_zen_forced());
        app.sidebar_w_override = Some(60);

        let applied = app.toggle_zen();
        assert!(!applied, "z is suppressed under auto-zen");
        assert!(
            app.sidebar_w_override.is_none(),
            "auto-zen-suppressed `z` still clears the override",
        );
    }
}
