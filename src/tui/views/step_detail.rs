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

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::config::Config;
use crate::plan::{ChangePolicy, ExecutionLog, Plan, Step, StepStatus};
use crate::prompt::DEFAULT_CONTEXT_PREPEND;
use crate::storage::ProjectSettings;
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
}
