// Step detail view (TUI-plan.md §8 + §18 Q5)
//
// Skeleton for the per-step pane stack reached via `enter` from plan-detail.
// This module owns the structural state — pane focus, zen-mode toggle, and the
// auto-zen threshold logic — but does not yet render pane bodies; subsequent
// steps fill in read-only renders, the appended-prompt navigator, and editor
// handoffs. For now the panes draw as bordered placeholders so the layout is
// observable while the rest of the v1 plan lands.

use std::path::Path;
use std::time::Instant;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::plan::{Plan, Step, StepStatus};
use crate::tui::chrome::{self, Chrome};
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};

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
}

impl StepDetailApp {
    /// Create a new view focused on the step at `selected_step_index`. The
    /// caller is expected to pre-clamp the index into `0..steps.len()` (or
    /// pass `0` for an empty plan, in which case the view renders an empty
    /// sidebar).
    pub fn new(plan: Plan, steps: Vec<Step>, selected_step_index: usize) -> Self {
        let clamped = if steps.is_empty() {
            0
        } else {
            selected_step_index.min(steps.len() - 1)
        };
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
    // Equal-share layout: every pane gets a Constraint::Min(3) row so each is
    // tall enough to render its border + at least one content line. Step-23
    // will replace the placeholders with real per-pane bodies.
    let constraints: Vec<Constraint> =
        Pane::ORDER.iter().map(|_| Constraint::Min(3)).collect();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (i, pane) in Pane::ORDER.iter().enumerate() {
        let focused = *pane == app.focused_pane;
        let border_color = if focused { theme::CURSOR } else { Color::Cyan };
        let mut block = Block::default()
            .title(format!(" {} ", pane.title()))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(border_color));
        if focused {
            block = block.border_style(
                Style::default()
                    .fg(border_color)
                    .add_modifier(Modifier::BOLD),
            );
        }
        frame.render_widget(block, chunks[i]);
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
        StepDetailApp::new(make_plan(), make_steps(n), selected)
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
}
