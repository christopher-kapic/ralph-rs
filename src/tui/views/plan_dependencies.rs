// Plan-dependencies sub-view (TUI-plan.md §1, step 33).
//
// A focused sub-view over the plan-detail screen that lets the user inspect,
// add, and remove the dependency edges of the focused plan. Entered via the
// `D` keybinding from plan-detail or any of the `/plan dependency …` palette
// commands.
//
// State machine: two modes. `List` shows the current dependencies as a
// `ratatui::Table`; `a` opens the `Picker` mode showing every other
// non-archived plan in the project that isn't already a direct dependency.
// `d` in `List` mode requests removal of the highlighted edge.
//
// The view is split into a pure state machine (`PlanDependenciesApp`) and a
// renderer (`render`) so we can drive the state machine in tests without a
// real terminal. All write-throughs (add / remove via `storage::*`) live in
// the dispatcher loop in `commands::run::run_plan_dependencies_tui`; this
// module only exposes the user's intent through `Outcome` and surfaces toasts
// for feedback.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState};

use crate::tui::help::{self, HelpState};
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};

// ---------------------------------------------------------------------------
// Sub-view state
// ---------------------------------------------------------------------------

/// Lightweight projection of [`crate::plan::Plan`] used by the sub-view.
///
/// The sub-view only needs the plan's id (for storage write-throughs and the
/// cycle pre-check) and the user-visible slug (for the table rows). The
/// dispatcher builds these from `storage::list_plans` before calling
/// [`PlanDependenciesApp::new`] / [`PlanDependenciesApp::refresh`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRef {
    pub id: String,
    pub slug: String,
}

/// Whether the sub-view is showing the current dependency list or the
/// candidate picker for adding a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Default mode: dependency table with `a`/`d`/`q`/`<esc>` keybindings.
    List,
    /// Slug-picker overlay opened by `a`. `j`/`k` moves the cursor, `Enter`
    /// requests an add, `<esc>`/`q` falls back to `List`.
    Picker,
}

/// What [`PlanDependenciesApp::handle_key`] returns each turn. The dispatcher
/// loop runs the side effects (storage writes, refresh, toast push) and keeps
/// looping on `Pending`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed; no side effect required.
    Pending,
    /// User pressed `q` / `<esc>` / Ctrl-C in `List` mode — pop the sub-view.
    Pop,
    /// User pressed `Enter` on a picker row — request the new dependency
    /// edge `plan_id -> dep_plan_id`. Caller must run the cycle pre-check
    /// and then call [`crate::storage::add_plan_dependency`].
    AddRequested { dep_plan_id: String },
    /// User pressed `d` on a row — request removal of the dependency edge
    /// via [`crate::storage::remove_plan_dependency`].
    RemoveRequested { dep_plan_id: String },
}

/// Sub-view state.
pub struct PlanDependenciesApp {
    /// Plan whose dependencies are being edited (parent plan).
    pub plan_id: String,
    /// Display slug of the parent plan, used in the title bar.
    pub plan_slug: String,
    /// Current dependency edges, in display order.
    pub deps: Vec<PlanRef>,
    /// Pre-filtered candidate list for the picker — every non-archived plan
    /// in the project that isn't `plan_id` and isn't already in `deps`.
    pub candidates: Vec<PlanRef>,
    /// 0-based cursor in the dependency table.
    pub list_cursor: usize,
    /// 0-based cursor in the candidate picker.
    pub picker_cursor: usize,
    /// Current mode (List or Picker).
    pub mode: Mode,
    /// Toast queue rendered over the bottom hint row.
    pub toasts: ToastQueue,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// passing keys to the per-mode handler (TUI-plan.md §15).
    pub help: HelpState,
}

impl PlanDependenciesApp {
    /// Build a new sub-view with the given dependency / candidate snapshots.
    pub fn new(
        plan_id: String,
        plan_slug: String,
        deps: Vec<PlanRef>,
        candidates: Vec<PlanRef>,
    ) -> Self {
        Self {
            plan_id,
            plan_slug,
            deps,
            candidates,
            list_cursor: 0,
            picker_cursor: 0,
            mode: Mode::List,
            toasts: ToastQueue::new(),
            help: HelpState::new(),
        }
    }

    /// Replace the edge / candidate snapshots after a successful write,
    /// clamping cursors so they don't dangle past the new list lengths.
    pub fn refresh(&mut self, deps: Vec<PlanRef>, candidates: Vec<PlanRef>) {
        self.deps = deps;
        self.candidates = candidates;
        if self.list_cursor >= self.deps.len() {
            self.list_cursor = self.deps.len().saturating_sub(1);
        }
        if self.picker_cursor >= self.candidates.len() {
            self.picker_cursor = self.candidates.len().saturating_sub(1);
        }
    }

    /// Push a toast onto the queue using the system clock for `expires_at`.
    pub fn push_toast(&mut self, msg: impl Into<String>, kind: ToastKind) {
        self.toasts.push(msg, kind, std::time::Instant::now());
    }

    /// Mouse-event entry point routed from the dispatcher's event loop.
    /// No-op by default — see [`super::plan_list::PlanListApp::handle_mouse`]
    /// for the rationale. Per-view drag handling is added in later steps.
    pub fn handle_mouse(&mut self, _event: MouseEvent) {}

    /// Pure key handler. Routes to the per-mode handler so tests can drive
    /// arbitrary key sequences without crossterm.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // §15 help overlay: route `?` toggle / dismissal first. While the
        // overlay is up the sub-view's per-mode handlers are skipped so
        // `j/k`/`a`/`d` don't fire under it.
        if self.help.intercept_key(key) != help::InterceptResult::Passthrough {
            return Outcome::Pending;
        }

        // Ctrl-C always pops the sub-view, mirroring the plan-detail view.
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Outcome::Pop;
        }
        match self.mode {
            Mode::List => self.handle_list_key(key),
            Mode::Picker => self.handle_picker_key(key),
        }
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            // Navigation
            KeyCode::Char('j') | KeyCode::Down => {
                if !self.deps.is_empty() && self.list_cursor + 1 < self.deps.len() {
                    self.list_cursor += 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.list_cursor > 0 {
                    self.list_cursor -= 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                if !self.deps.is_empty() {
                    self.list_cursor = 0;
                }
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !self.deps.is_empty() {
                    self.list_cursor = self.deps.len() - 1;
                }
                Outcome::Pending
            }

            // Open picker for adding a new dependency.
            KeyCode::Char('a') => {
                if self.candidates.is_empty() {
                    self.push_toast("No other plans available to depend on.", ToastKind::Info);
                    return Outcome::Pending;
                }
                self.picker_cursor = 0;
                self.mode = Mode::Picker;
                Outcome::Pending
            }

            // Remove the highlighted edge.
            KeyCode::Char('d') => {
                if let Some(target) = self.deps.get(self.list_cursor) {
                    Outcome::RemoveRequested {
                        dep_plan_id: target.id.clone(),
                    }
                } else {
                    Outcome::Pending
                }
            }

            // Pop the sub-view.
            KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => Outcome::Pop,

            _ => Outcome::Pending,
        }
    }

    fn handle_picker_key(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if !self.candidates.is_empty() && self.picker_cursor + 1 < self.candidates.len() {
                    self.picker_cursor += 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.picker_cursor > 0 {
                    self.picker_cursor -= 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                if !self.candidates.is_empty() {
                    self.picker_cursor = 0;
                }
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !self.candidates.is_empty() {
                    self.picker_cursor = self.candidates.len() - 1;
                }
                Outcome::Pending
            }

            KeyCode::Enter => {
                if let Some(candidate) = self.candidates.get(self.picker_cursor) {
                    Outcome::AddRequested {
                        dep_plan_id: candidate.id.clone(),
                    }
                } else {
                    Outcome::Pending
                }
            }

            // Cancel back to the list mode (not pop the whole sub-view).
            KeyCode::Esc | KeyCode::Char('q') => {
                self.mode = Mode::List;
                Outcome::Pending
            }

            _ => Outcome::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

const LIST_HINT: &str = " [a] add   [d] remove   [j/k] move   [q/Esc] back ";
const PICKER_HINT: &str = " [Enter] add   [j/k] move   [Esc] cancel ";

/// Draw the dependency table over `area`. When the picker is open, render the
/// picker overlay on top.
pub fn render(frame: &mut Frame, area: Rect, app: &mut PlanDependenciesApp) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    // -- Title + hint row --------------------------------------------------
    let title = format!(" Dependencies — {} ", app.plan_slug);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Reserve the bottom row for the hint / toast.
    let body_h = inner.height.saturating_sub(1);
    let body_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: body_h,
    };
    let hint_area = Rect {
        x: inner.x,
        y: inner.y + body_h,
        width: inner.width,
        height: 1,
    };

    // -- Body: dependency table -------------------------------------------
    if app.deps.is_empty() {
        let para = Paragraph::new(Line::from(Span::styled(
            "(no dependencies — press `a` to add one)",
            Style::default().fg(theme::CHROME_DIM),
        )));
        frame.render_widget(para, body_area);
    } else {
        let header = Row::new(vec![Cell::from("#"), Cell::from("Slug")])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(0);
        let rows: Vec<Row> = app
            .deps
            .iter()
            .enumerate()
            .map(|(i, dep)| {
                Row::new(vec![
                    Cell::from(format!("{}", i + 1)),
                    Cell::from(dep.slug.clone()),
                ])
            })
            .collect();
        let widths = [Constraint::Length(4), Constraint::Min(1)];
        let table = Table::new(rows, widths)
            .header(header)
            .row_highlight_style(
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::REVERSED)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("");
        let mut state = TableState::default();
        if !app.deps.is_empty() {
            state.select(Some(app.list_cursor.min(app.deps.len() - 1)));
        }
        frame.render_stateful_widget(table, body_area, &mut state);
    }

    // -- Hint / toast row -------------------------------------------------
    let now = std::time::Instant::now();
    app.toasts.prune(now);
    let hint_line = if let Some(toast) = app.toasts.current() {
        Line::from(Span::styled(
            toast.text.clone(),
            Style::default()
                .fg(toast.color)
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        let hint = match app.mode {
            Mode::List => LIST_HINT,
            Mode::Picker => PICKER_HINT,
        };
        Line::from(Span::styled(hint, Style::default().fg(theme::CHROME_DIM)))
    };
    let hint = Paragraph::new(hint_line);
    frame.render_widget(hint, hint_area);

    // -- Picker overlay ---------------------------------------------------
    if app.mode == Mode::Picker {
        render_picker(frame, area, app);
    }

    // -- Help overlay -----------------------------------------------------
    if app.help.is_visible() {
        help::render(frame, area, &help::for_plan_dependencies());
    }
}

fn render_picker(frame: &mut Frame, area: Rect, app: &PlanDependenciesApp) {
    let dialog = centered_picker_rect(area, app);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(" Add dependency ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let mut lines: Vec<Line> = Vec::new();
    if app.candidates.is_empty() {
        lines.push(Line::from(Span::styled(
            "(no candidates)",
            Style::default().fg(theme::CHROME_DIM),
        )));
    } else {
        for (i, candidate) in app.candidates.iter().enumerate() {
            let style = if i == app.picker_cursor {
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::REVERSED)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!(" {} ", candidate.slug),
                style,
            )));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        PICKER_HINT,
        Style::default().add_modifier(Modifier::BOLD),
    ));

    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, dialog);
}

fn centered_picker_rect(area: Rect, app: &PlanDependenciesApp) -> Rect {
    use ratatui::layout::{Flex, Layout};

    let max_label = app
        .candidates
        .iter()
        .map(|c| c.slug.chars().count())
        .max()
        .unwrap_or(0);
    let body_w = max_label.max(PICKER_HINT.chars().count()) + 4;
    let desired_w = (body_w as u16).max(40).min(area.width);
    let row_count = app.candidates.len().max(1);
    let desired_h = ((row_count + 4) as u16).min(area.height);
    if desired_w == 0 || desired_h == 0 {
        return Rect {
            x: area.x,
            y: area.y,
            width: 0,
            height: 0,
        };
    }
    let [vert] = Layout::vertical([Constraint::Length(desired_h)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(desired_w)])
        .flex(Flex::Center)
        .areas(vert);
    horiz
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::storage;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    fn pref(id: &str, slug: &str) -> PlanRef {
        PlanRef {
            id: id.to_string(),
            slug: slug.to_string(),
        }
    }

    // -- Pure state-machine tests ----------------------------------------

    #[test]
    fn new_starts_in_list_mode_with_zero_cursor() {
        let app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "dep-a"), pref("d2", "dep-b")],
            vec![pref("c1", "cand-x")],
        );
        assert_eq!(app.mode, Mode::List);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.picker_cursor, 0);
        assert_eq!(app.deps.len(), 2);
    }

    #[test]
    fn j_moves_list_cursor_down_clamped() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b")],
            vec![],
        );
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
        // At the bottom: clamps.
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn k_moves_list_cursor_up_clamped() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b")],
            vec![],
        );
        app.list_cursor = 1;
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
        // At the top: clamps.
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn g_jumps_to_top_capital_g_to_bottom() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b"), pref("d3", "c")],
            vec![],
        );
        app.handle_key(key(KeyCode::Char('G')));
        assert_eq!(app.list_cursor, 2);
        app.handle_key(key(KeyCode::Char('g')));
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn a_opens_picker_when_candidates_exist() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![pref("c1", "x")]);
        assert_eq!(app.handle_key(key(KeyCode::Char('a'))), Outcome::Pending);
        assert_eq!(app.mode, Mode::Picker);
        assert_eq!(app.picker_cursor, 0);
    }

    #[test]
    fn a_with_no_candidates_toasts_and_stays_in_list() {
        let mut app = PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('a'))), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
        let toast = app.toasts.current().expect("toast pushed");
        assert!(toast.text.contains("No other plans"));
    }

    #[test]
    fn d_emits_remove_request_for_cursor_row() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b")],
            vec![],
        );
        app.list_cursor = 1;
        let outcome = app.handle_key(key(KeyCode::Char('d')));
        assert_eq!(
            outcome,
            Outcome::RemoveRequested {
                dep_plan_id: "d2".into()
            }
        );
    }

    #[test]
    fn d_with_empty_deps_is_pending() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![pref("c1", "x")]);
        assert_eq!(app.handle_key(key(KeyCode::Char('d'))), Outcome::Pending);
    }

    #[test]
    fn q_pops_in_list_mode() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![pref("d1", "a")], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('q'))), Outcome::Pop);
    }

    #[test]
    fn esc_pops_in_list_mode() {
        let mut app = PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pop);
    }

    #[test]
    fn ctrl_c_pops_in_either_mode() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![pref("c1", "x")]);
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
        // Same in picker mode.
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::Picker);
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
    }

    #[test]
    fn picker_j_k_navigates_clamped() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![],
            vec![pref("c1", "x"), pref("c2", "y")],
        );
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::Picker);
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.picker_cursor, 1);
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.picker_cursor, 1);
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.picker_cursor, 0);
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.picker_cursor, 0);
    }

    #[test]
    fn picker_enter_emits_add_request() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![],
            vec![pref("c1", "x"), pref("c2", "y")],
        );
        app.handle_key(key(KeyCode::Char('a')));
        app.handle_key(key(KeyCode::Char('j')));
        let outcome = app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            outcome,
            Outcome::AddRequested {
                dep_plan_id: "c2".into()
            }
        );
    }

    #[test]
    fn picker_esc_falls_back_to_list_mode() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![pref("c1", "x")]);
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::Picker);
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
    }

    #[test]
    fn picker_q_falls_back_to_list_mode() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![pref("c1", "x")]);
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.handle_key(key(KeyCode::Char('q'))), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
    }

    #[test]
    fn refresh_clamps_cursors_to_new_lengths() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b"), pref("d3", "c")],
            vec![pref("c1", "x"), pref("c2", "y")],
        );
        app.list_cursor = 2;
        app.picker_cursor = 1;
        // Shrink to one dep and zero candidates.
        app.refresh(vec![pref("d1", "a")], vec![]);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.picker_cursor, 0);
    }

    #[test]
    fn refresh_with_empty_lists_zeroes_cursors() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a")],
            vec![pref("c1", "x")],
        );
        app.list_cursor = 0;
        app.picker_cursor = 0;
        app.refresh(vec![], vec![]);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.picker_cursor, 0);
    }

    #[test]
    fn unknown_key_in_list_is_pending() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![pref("d1", "a")], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('x'))), Outcome::Pending);
    }

    // -- Render smoke tests ----------------------------------------------

    #[test]
    fn render_list_mode_does_not_panic() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "dep-a"), pref("d2", "dep-b")],
            vec![pref("c1", "cand-x")],
        );
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, f.area(), &mut app)).unwrap();
    }

    #[test]
    fn render_empty_list_shows_placeholder() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![],
            vec![pref("c1", "cand-x")],
        );
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, f.area(), &mut app)).unwrap();
        let buf = terminal.backend().buffer().clone();
        let mut found = false;
        for y in 0..buf.area.height {
            let mut row = String::new();
            for x in 0..buf.area.width {
                let cell = &buf[(x, y)];
                row.push_str(cell.symbol());
            }
            if row.contains("no dependencies") {
                found = true;
            }
        }
        assert!(found, "expected empty-state placeholder in render");
    }

    #[test]
    fn render_picker_mode_does_not_panic() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a")],
            vec![pref("c1", "cand-x"), pref("c2", "cand-y")],
        );
        app.handle_key(key(KeyCode::Char('a')));
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, f.area(), &mut app)).unwrap();
    }

    // -- Help overlay (TUI-plan.md §15) ---------------------------------

    #[test]
    fn help_state_default_hidden() {
        let app = PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![]);
        assert!(!app.help.is_visible());
    }

    #[test]
    fn handle_key_question_mark_opens_help_in_list_mode() {
        let mut app =
            PlanDependenciesApp::new("p1".into(), "parent".into(), vec![pref("d1", "a")], vec![]);
        // `?` is consumed by the help routing — returns Pending and the
        // sub-view's per-mode handler doesn't fire.
        let r = app.handle_key(key(KeyCode::Char('?')));
        assert_eq!(r, Outcome::Pending);
        assert!(app.help.is_visible());
    }

    #[test]
    fn handle_key_esc_closes_help_without_popping() {
        let mut app = PlanDependenciesApp::new("p1".into(), "parent".into(), vec![], vec![]);
        app.help.open();
        // Without the help intercept, `<esc>` in List mode would pop the
        // sub-view (Outcome::Pop). With the overlay open it must just close
        // the overlay and stay in the sub-view (Outcome::Pending).
        let r = app.handle_key(key(KeyCode::Esc));
        assert_eq!(r, Outcome::Pending);
        assert!(!app.help.is_visible());
    }

    #[test]
    fn handle_key_swallows_input_while_help_visible() {
        let mut app = PlanDependenciesApp::new(
            "p1".into(),
            "parent".into(),
            vec![pref("d1", "a"), pref("d2", "b")],
            vec![],
        );
        app.help.open();
        // `j` would normally move the cursor; with the overlay up it must
        // be consumed instead.
        let before = app.list_cursor;
        let r = app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(r, Outcome::Pending);
        assert_eq!(app.list_cursor, before, "j must not move cursor under help");
        assert!(app.help.is_visible());
    }

    // -- End-to-end storage round-trip tests -----------------------------

    fn make_plan(conn: &rusqlite::Connection, slug: &str, project: &str) -> String {
        storage::create_plan(
            conn,
            slug,
            project,
            &format!("br-{slug}"),
            "d",
            None,
            None,
            &[],
        )
        .expect("create_plan")
        .id
    }

    #[test]
    fn end_to_end_add_persists_through_storage() {
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let parent_id = make_plan(&conn, "parent", project);
        let a_id = make_plan(&conn, "dep-a", project);
        let b_id = make_plan(&conn, "dep-b", project);

        let candidates = vec![pref(&a_id, "dep-a"), pref(&b_id, "dep-b")];
        let mut app =
            PlanDependenciesApp::new(parent_id.clone(), "parent".into(), vec![], candidates);

        // Drive: a → Enter on first candidate → AddRequested.
        app.handle_key(key(KeyCode::Char('a')));
        let outcome = app.handle_key(key(KeyCode::Enter));
        let dep_id = match outcome {
            Outcome::AddRequested { dep_plan_id } => dep_plan_id,
            other => panic!("expected AddRequested, got {other:?}"),
        };
        assert_eq!(dep_id, a_id);

        // Caller-side: cycle pre-check then write through storage.
        assert!(!storage::would_create_cycle(&conn, &parent_id, &dep_id).unwrap());
        storage::add_plan_dependency(&conn, &parent_id, &dep_id).unwrap();
        let edges = storage::list_plan_dependencies(&conn, &parent_id).unwrap();
        assert_eq!(edges, vec![a_id.clone()]);
    }

    #[test]
    fn end_to_end_remove_persists_through_storage() {
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let parent_id = make_plan(&conn, "parent", project);
        let a_id = make_plan(&conn, "dep-a", project);
        storage::add_plan_dependency(&conn, &parent_id, &a_id).unwrap();

        let mut app = PlanDependenciesApp::new(
            parent_id.clone(),
            "parent".into(),
            vec![pref(&a_id, "dep-a")],
            vec![],
        );

        let outcome = app.handle_key(key(KeyCode::Char('d')));
        let removed = match outcome {
            Outcome::RemoveRequested { dep_plan_id } => dep_plan_id,
            other => panic!("expected RemoveRequested, got {other:?}"),
        };
        assert_eq!(removed, a_id);

        storage::remove_plan_dependency(&conn, &parent_id, &removed).unwrap();
        let edges = storage::list_plan_dependencies(&conn, &parent_id).unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn cycle_pre_check_rejects_back_edge() {
        // Build A -> B; trying to add B -> A should be rejected by
        // would_create_cycle before ever hitting storage.
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let a_id = make_plan(&conn, "a", project);
        let b_id = make_plan(&conn, "b", project);
        storage::add_plan_dependency(&conn, &a_id, &b_id).unwrap();

        // The candidate list for B would include A (no direct edge B->A yet).
        let mut app =
            PlanDependenciesApp::new(b_id.clone(), "b".into(), vec![], vec![pref(&a_id, "a")]);
        app.handle_key(key(KeyCode::Char('a')));
        let outcome = app.handle_key(key(KeyCode::Enter));
        let dep_id = match outcome {
            Outcome::AddRequested { dep_plan_id } => dep_plan_id,
            other => panic!("expected AddRequested, got {other:?}"),
        };
        assert!(storage::would_create_cycle(&conn, &b_id, &dep_id).unwrap());
        // And the storage layer's defensive check also rejects.
        let err = storage::add_plan_dependency(&conn, &b_id, &dep_id).unwrap_err();
        assert!(format!("{err}").contains("cycle"));
    }

    #[test]
    fn cycle_pre_check_rejects_self_edge() {
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let a_id = make_plan(&conn, "a", project);
        // would_create_cycle treats self-edges as cycles.
        assert!(storage::would_create_cycle(&conn, &a_id, &a_id).unwrap());
    }
}
