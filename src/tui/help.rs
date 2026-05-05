// Help overlay (TUI-plan.md §15 "help overlay" + §16 phase 13).
//
// `?` toggles a centered modal listing the bindings for the current view,
// grouped by category (Navigation, Selection, Edit, Run, etc.). `?` or
// `<esc>` closes it. The overlay is rendered by each view's draw function
// when [`HelpState::is_visible`]; the dispatcher intercepts input via
// [`HelpState::intercept_key`] so it can short-circuit normal dispatch
// while the overlay is open.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::theme;

// ---------------------------------------------------------------------------
// Help model
// ---------------------------------------------------------------------------

/// One (key, action description) row in a help category.
pub type Binding = (&'static str, &'static str);

/// One category of bindings (e.g. "Navigation", "Selection") rendered as a
/// header followed by a list of `(key, action)` rows.
#[derive(Debug, Clone)]
pub struct Group {
    pub name: &'static str,
    pub bindings: Vec<Binding>,
}

impl Group {
    pub fn new(name: &'static str, bindings: Vec<Binding>) -> Self {
        Self { name, bindings }
    }
}

/// Help-overlay model — a title plus categorized bindings.
#[derive(Debug, Clone)]
pub struct HelpModel {
    pub title: String,
    pub groups: Vec<Group>,
}

impl HelpModel {
    pub fn new(title: impl Into<String>, groups: Vec<Group>) -> Self {
        Self {
            title: title.into(),
            groups,
        }
    }
}

// ---------------------------------------------------------------------------
// State + intercept
// ---------------------------------------------------------------------------

/// What the dispatcher should do with a key event after consulting
/// [`HelpState::intercept_key`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterceptResult {
    /// Help overlay was just opened by `?`. Caller should not dispatch this
    /// key into the view's normal handler.
    Opened,
    /// Help overlay was just closed by `?` / `<esc>` / `q` / Ctrl-C. Caller
    /// should not dispatch this key into the view's normal handler.
    Closed,
    /// Help overlay was visible and consumed the (unrelated) key. Caller
    /// should not dispatch this key into the view's normal handler.
    Consumed,
    /// Help overlay had nothing to do with this key. Caller should dispatch
    /// it normally.
    Passthrough,
}

/// Per-view help-overlay state. Defaults to hidden.
#[derive(Debug, Default)]
pub struct HelpState {
    visible: bool,
}

impl HelpState {
    pub fn new() -> Self {
        Self { visible: false }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self) {
        self.visible = true;
    }

    pub fn close(&mut self) {
        self.visible = false;
    }

    /// Inspect a key event and return whether help routing handled it.
    ///
    /// Routing rules:
    /// - Hidden + `?`: open the overlay → [`InterceptResult::Opened`].
    /// - Visible + `?` / `<esc>` / `q` / Ctrl-C: close → [`InterceptResult::Closed`].
    /// - Visible + any other key: swallow → [`InterceptResult::Consumed`].
    /// - Hidden + any other key: [`InterceptResult::Passthrough`].
    pub fn intercept_key(&mut self, key: KeyEvent) -> InterceptResult {
        if self.visible {
            let close = matches!(
                key.code,
                KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q')
            ) || (matches!(key.code, KeyCode::Char('c'))
                && key.modifiers.contains(KeyModifiers::CONTROL));
            if close {
                self.visible = false;
                InterceptResult::Closed
            } else {
                InterceptResult::Consumed
            }
        } else if matches!(key.code, KeyCode::Char('?')) {
            self.visible = true;
            InterceptResult::Opened
        } else {
            InterceptResult::Passthrough
        }
    }
}

// ---------------------------------------------------------------------------
// Per-view help models
// ---------------------------------------------------------------------------

/// Help model for the plan-list view (TUI-plan.md §5).
pub fn for_plan_list() -> HelpModel {
    HelpModel::new(
        "Help — Plan list",
        vec![
            Group::new(
                "Navigation",
                vec![
                    ("j / ↓", "Next plan"),
                    ("k / ↑", "Previous plan"),
                    ("g", "Jump to top"),
                    ("G", "Jump to bottom"),
                    ("enter / → / l", "Open highlighted plan"),
                ],
            ),
            Group::new(
                "Selection",
                vec![
                    ("space", "Toggle selection on plan"),
                    ("<esc>", "Clear selection / quit if none"),
                ],
            ),
            Group::new(
                "Edit",
                vec![
                    ("i / a", "Create new plan"),
                    ("A", "Approve highlighted plan"),
                    ("Q", "Toggle questions on highlighted plan"),
                    ("d", "Archive selection or cursor target"),
                ],
            ),
            Group::new(
                "Other",
                vec![
                    ("r", "Refresh from DB"),
                    ("?", "Toggle this help"),
                    ("/ or :", "Open command palette"),
                    ("q / Ctrl-C", "Quit TUI"),
                ],
            ),
        ],
    )
}

/// Help model for the archived-list view (TUI-plan.md §6).
pub fn for_archived_list() -> HelpModel {
    HelpModel::new(
        "Help — Archived plans",
        vec![
            Group::new(
                "Navigation",
                vec![
                    ("j / ↓", "Next plan"),
                    ("k / ↑", "Previous plan"),
                    ("g", "Jump to top"),
                    ("G", "Jump to bottom"),
                    ("← / h / q", "Back to plan list"),
                ],
            ),
            Group::new(
                "Selection",
                vec![
                    ("space", "Toggle selection on plan"),
                    ("<esc>", "Clear selection"),
                ],
            ),
            Group::new(
                "Actions",
                vec![
                    ("enter / → / l", "Unarchive selection or cursor target"),
                    ("d", "Permanently delete (with confirm)"),
                ],
            ),
            Group::new(
                "Other",
                vec![("?", "Toggle this help"), ("Ctrl-C", "Back to plan list")],
            ),
        ],
    )
}

/// Help model for the plan-detail view (TUI-plan.md §7).
pub fn for_plan_detail() -> HelpModel {
    HelpModel::new(
        "Help — Plan detail",
        vec![
            Group::new(
                "Navigation",
                vec![
                    ("j / ↓", "Next step"),
                    ("k / ↑", "Previous step"),
                    ("enter / → / l", "Open step detail"),
                    ("← / h / q", "Back to plan list"),
                ],
            ),
            Group::new(
                "Selection",
                vec![
                    ("space", "Toggle selection on step"),
                    ("<esc>", "Clear selection"),
                ],
            ),
            Group::new(
                "Edit",
                vec![
                    ("i", "Insert step above cursor"),
                    ("a", "Append step below cursor"),
                    ("d", "Delete selection or cursor step"),
                    ("r", "Reset highlighted step"),
                    ("Shift-J", "Move step down"),
                    ("Shift-K", "Move step up"),
                    ("Q", "Toggle questions for this plan"),
                ],
            ),
            Group::new(
                "Run",
                vec![
                    ("R", "Run / resume this plan"),
                    ("S", "Stop the live run"),
                    ("s", "Skip the running step"),
                ],
            ),
            Group::new(
                "Sub-views",
                vec![
                    ("D", "Plan dependencies"),
                    ("A", "Answer oldest open question"),
                ],
            ),
            Group::new(
                "Other",
                vec![
                    ("?", "Toggle this help"),
                    ("/ or :", "Open command palette"),
                    ("Ctrl-C", "Back to plan list"),
                ],
            ),
        ],
    )
}

/// Help model for the step-detail view (TUI-plan.md §8 + §17).
pub fn for_step_detail() -> HelpModel {
    HelpModel::new(
        "Help — Step detail",
        vec![
            Group::new(
                "Navigation",
                vec![
                    ("j / ↓", "Focus next pane"),
                    ("k / ↑", "Focus previous pane"),
                    ("h / ←", "Pane-specific left / pop view"),
                    ("l / →", "Pane-specific right / next attempt"),
                ],
            ),
            Group::new(
                "Edit",
                vec![
                    ("c", "Open focused pane in $EDITOR"),
                    ("a", "Answer focused open question"),
                ],
            ),
            Group::new("View", vec![("z", "Toggle zen mode (collapse sidebar)")]),
            Group::new(
                "Other",
                vec![
                    ("?", "Toggle this help"),
                    ("q / <esc> / Ctrl-C", "Back to plan detail"),
                ],
            ),
        ],
    )
}

/// Help model for the plan-dependencies sub-view (TUI-plan.md §1, step 33).
pub fn for_plan_dependencies() -> HelpModel {
    HelpModel::new(
        "Help — Plan dependencies",
        vec![
            Group::new(
                "Navigation",
                vec![
                    ("j / ↓", "Next row"),
                    ("k / ↑", "Previous row"),
                    ("g / Home", "Jump to top"),
                    ("G / End", "Jump to bottom"),
                ],
            ),
            Group::new(
                "Edit",
                vec![
                    ("a", "Open picker to add a dependency"),
                    ("d", "Remove highlighted dependency"),
                    ("enter", "(picker) Add highlighted candidate"),
                ],
            ),
            Group::new(
                "Other",
                vec![
                    ("?", "Toggle this help"),
                    ("q / <esc> / h / ← / Ctrl-C", "Back to plan detail"),
                ],
            ),
        ],
    )
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Render the help overlay as a centered modal over `area`. Caller is
/// expected to have drawn the background view immediately prior; `Clear` is
/// applied to the dialog rect so the overlay sits crisply on top.
pub fn render(frame: &mut Frame, area: Rect, model: &HelpModel) {
    let dialog = centered_rect(area, model);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let lines = build_lines(model);
    let block = Block::default()
        .title(format!(" {} ", model.title))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// Total content height (in rows) that the model would render with one row
/// per binding plus blank-line separators between groups, plus the closing
/// hint line. Exposed so [`centered_rect`] can size the overlay.
pub fn line_count(model: &HelpModel) -> usize {
    let mut total = 0usize;
    for (i, group) in model.groups.iter().enumerate() {
        if i > 0 {
            total += 1; // blank line between groups
        }
        total += 1 + group.bindings.len(); // header + rows
    }
    // closing hint
    total += 2;
    total
}

/// Width of the longest displayed line in the rendered model, used for sizing
/// the overlay. Includes the 2-col left padding ("  ") on binding rows.
pub fn max_line_width(model: &HelpModel) -> usize {
    let mut max = model.title.chars().count() + 2;
    for group in &model.groups {
        max = max.max(group.name.chars().count());
        for (key, action) in &group.bindings {
            // "  " + key + " " + action, with key padded to KEY_COL.
            let row = 2 + key.chars().count().max(KEY_COL) + 1 + action.chars().count();
            max = max.max(row);
        }
    }
    max = max.max("  ? or <esc> to close".chars().count());
    max
}

/// Column width reserved for the key column on every binding row. Keeps the
/// action descriptions left-aligned even with mixed-length key labels.
const KEY_COL: usize = 22;

fn build_lines(model: &HelpModel) -> Vec<Line<'_>> {
    let mut lines: Vec<Line<'_>> = Vec::new();
    for (i, group) in model.groups.iter().enumerate() {
        if i > 0 {
            lines.push(Line::from(""));
        }
        lines.push(Line::from(Span::styled(
            group.name,
            Style::default()
                .fg(theme::SELECTION)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, action) in &group.bindings {
            let key_padded = pad_right(key, KEY_COL);
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(key_padded, Style::default().fg(theme::CURSOR)),
                Span::raw(" "),
                Span::raw(*action),
            ]));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  ? or <esc> to close",
        Style::default().add_modifier(Modifier::DIM),
    )));
    lines
}

fn pad_right(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width {
        s.to_string()
    } else {
        let mut out = String::with_capacity(width);
        out.push_str(s);
        for _ in len..width {
            out.push(' ');
        }
        out
    }
}

fn centered_rect(area: Rect, model: &HelpModel) -> Rect {
    let body_h = line_count(model) as u16;
    // 2 border rows + body
    let desired_h = body_h.saturating_add(2);
    let height = desired_h.min(area.height).max(5.min(area.height));

    let body_w = max_line_width(model) as u16;
    // 2 border cols + 2 padding
    let desired_w = body_w.saturating_add(4);
    let width = desired_w.min(area.width).max(20.min(area.width));

    let [vert] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(width)])
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
    use crossterm::event::{KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- HelpState::intercept_key -----------------------------------------

    #[test]
    fn question_mark_opens_when_hidden() {
        let mut s = HelpState::new();
        assert!(!s.is_visible());
        let r = s.intercept_key(key(KeyCode::Char('?')));
        assert_eq!(r, InterceptResult::Opened);
        assert!(s.is_visible());
    }

    #[test]
    fn question_mark_closes_when_visible() {
        let mut s = HelpState::new();
        s.open();
        let r = s.intercept_key(key(KeyCode::Char('?')));
        assert_eq!(r, InterceptResult::Closed);
        assert!(!s.is_visible());
    }

    #[test]
    fn esc_closes_when_visible() {
        let mut s = HelpState::new();
        s.open();
        let r = s.intercept_key(key(KeyCode::Esc));
        assert_eq!(r, InterceptResult::Closed);
        assert!(!s.is_visible());
    }

    #[test]
    fn q_closes_when_visible() {
        let mut s = HelpState::new();
        s.open();
        let r = s.intercept_key(key(KeyCode::Char('q')));
        assert_eq!(r, InterceptResult::Closed);
        assert!(!s.is_visible());
    }

    #[test]
    fn ctrl_c_closes_when_visible() {
        let mut s = HelpState::new();
        s.open();
        let r = s.intercept_key(key_mod(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(r, InterceptResult::Closed);
        assert!(!s.is_visible());
    }

    #[test]
    fn other_keys_consumed_when_visible() {
        let mut s = HelpState::new();
        s.open();
        let r = s.intercept_key(key(KeyCode::Char('j')));
        assert_eq!(r, InterceptResult::Consumed);
        assert!(s.is_visible(), "consumed key must not close the overlay");
    }

    #[test]
    fn other_keys_passthrough_when_hidden() {
        let mut s = HelpState::new();
        let r = s.intercept_key(key(KeyCode::Char('j')));
        assert_eq!(r, InterceptResult::Passthrough);
        assert!(!s.is_visible());
    }

    #[test]
    fn esc_does_not_open_help() {
        // Esc on a hidden overlay must not open it (Esc is overloaded
        // for clear-selection / cancel in views).
        let mut s = HelpState::new();
        let r = s.intercept_key(key(KeyCode::Esc));
        assert_eq!(r, InterceptResult::Passthrough);
        assert!(!s.is_visible());
    }

    // -- Per-view help models ---------------------------------------------

    #[test]
    fn plan_list_has_navigation_and_other() {
        let m = for_plan_list();
        assert!(m.title.contains("Plan list"));
        let names: Vec<&str> = m.groups.iter().map(|g| g.name).collect();
        assert!(names.contains(&"Navigation"));
        assert!(names.contains(&"Other"));
        // Sanity: every group has at least one binding.
        for g in &m.groups {
            assert!(!g.bindings.is_empty(), "{} has no bindings", g.name);
        }
    }

    #[test]
    fn archived_list_documents_destructive_d() {
        let m = for_archived_list();
        let actions: Vec<&str> = m
            .groups
            .iter()
            .flat_map(|g| g.bindings.iter().map(|(_, a)| *a))
            .collect();
        assert!(
            actions.iter().any(|a| a.contains("Permanently delete")),
            "archived-list help missing destructive d entry: {actions:?}"
        );
    }

    #[test]
    fn plan_detail_has_run_group() {
        let m = for_plan_detail();
        let names: Vec<&str> = m.groups.iter().map(|g| g.name).collect();
        assert!(names.contains(&"Run"));
        assert!(names.contains(&"Edit"));
    }

    #[test]
    fn step_detail_documents_editor_handoff() {
        let m = for_step_detail();
        let actions: Vec<&str> = m
            .groups
            .iter()
            .flat_map(|g| g.bindings.iter().map(|(_, a)| *a))
            .collect();
        assert!(
            actions.iter().any(|a| a.contains("$EDITOR")),
            "step-detail help missing $EDITOR entry: {actions:?}"
        );
    }

    #[test]
    fn plan_dependencies_documents_picker() {
        let m = for_plan_dependencies();
        let actions: Vec<&str> = m
            .groups
            .iter()
            .flat_map(|g| g.bindings.iter().map(|(_, a)| *a))
            .collect();
        assert!(
            actions.iter().any(|a| a.contains("picker")),
            "deps help missing picker entry: {actions:?}"
        );
    }

    #[test]
    fn every_view_help_documents_question_mark_toggle() {
        for m in [
            for_plan_list(),
            for_archived_list(),
            for_plan_detail(),
            for_step_detail(),
            for_plan_dependencies(),
        ] {
            let has_q = m
                .groups
                .iter()
                .flat_map(|g| g.bindings.iter())
                .any(|(k, _)| k.contains('?'));
            assert!(has_q, "{} missing ? binding", m.title);
        }
    }

    // -- Rendering --------------------------------------------------------

    fn render_to_string(width: u16, height: u16, m: &HelpModel) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                let area = f.area();
                render(f, area, m);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn render_shows_title_and_groups() {
        let m = for_plan_list();
        // Use a tall backend so the model's content fits without clipping
        // — the plan-list help is the densest of the per-view models.
        let out = render_to_string(80, 30, &m);
        assert!(out.contains("Plan list"), "title missing:\n{out}");
        assert!(
            out.contains("Navigation"),
            "Navigation group missing:\n{out}"
        );
        assert!(
            out.contains("? or <esc> to close"),
            "footer missing:\n{out}"
        );
    }

    #[test]
    fn render_does_not_panic_on_tiny_terminal() {
        let m = for_plan_detail();
        let _ = render_to_string(10, 5, &m);
    }

    #[test]
    fn render_skips_when_area_is_zero() {
        // A degenerate area must not panic. Drawing into a 1x1 backend just
        // exercises the small-area path.
        let m = for_plan_list();
        let _ = render_to_string(1, 1, &m);
    }

    #[test]
    fn line_count_includes_separators_and_footer() {
        // 1 group with 2 bindings: 1 header + 2 rows + 2 footer = 5
        let m = HelpModel::new("x", vec![Group::new("G", vec![("k", "a"), ("j", "b")])]);
        assert_eq!(line_count(&m), 5);

        // 2 groups: G1 header (1) + 1 row + blank + G2 header (1) + 2 rows
        //           + 2 footer = 8
        let m = HelpModel::new(
            "x",
            vec![
                Group::new("G1", vec![("k", "a")]),
                Group::new("G2", vec![("k", "a"), ("j", "b")]),
            ],
        );
        assert_eq!(line_count(&m), 8);
    }

    #[test]
    fn max_line_width_pads_to_key_col() {
        // Action wider than key+gap should drive the width.
        let m = HelpModel::new(
            "t",
            vec![Group::new(
                "G",
                vec![("k", "a very long action description")],
            )],
        );
        let w = max_line_width(&m);
        assert!(w >= "a very long action description".chars().count());
    }
}
