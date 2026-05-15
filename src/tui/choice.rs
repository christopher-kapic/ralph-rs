// Generic single-select dialog primitive.
//
// A vertically-navigated list of choices rendered as a centered modal
// overlay. Each row is one `T` (the item type supplies its own label via
// [`ChoiceItem`]); the widget owns focus, navigation, and rendering.
//
// Navigation is deliberately **clamped, not wrapped** — wrapping a 2–4
// item list is disorienting (you press `j` once too many and silently
// jump back to the top). `j`/`↓` move down, `k`/`↑` move up, both stop
// at the edges. `Enter` confirms the focused row, `Esc`/`Ctrl-C` cancel.
//
// The state machine ([`Choice::handle_key`]) and the renderer
// ([`render`]) are factored apart so transitions can be unit-tested
// without a real terminal — the same split used by [`super::dialog`] and
// [`super::run_dialog`]. The `/run` branch-choice dialog is the first
// consumer; the skip dialog (plan phase 5) is the second.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::theme;

/// One selectable row in a [`Choice`]. Implementors supply the text shown
/// for the row; everything else (focus, navigation, render) is the
/// widget's job.
pub trait ChoiceItem {
    fn label(&self) -> String;
}

/// Result of feeding one key to a [`Choice`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChoiceOutcome<T> {
    /// Key consumed (navigation or unrecognized); dialog stays open.
    Pending,
    /// User dismissed the dialog (`Esc` or `Ctrl-C`).
    Cancelled,
    /// User confirmed (`Enter`) — carries a clone of the focused choice.
    Confirmed(T),
}

/// A vertically-navigated single-select dialog.
///
/// Construct with [`Choice::new`], feed key events to
/// [`Choice::handle_key`] until it returns a terminal [`ChoiceOutcome`],
/// and render with [`render`].
pub struct Choice<T> {
    /// The rows, top to bottom.
    pub choices: Vec<T>,
    /// Index of the highlighted row. Always a valid index when `choices`
    /// is non-empty (clamped on construction and on every move).
    focused: usize,
}

impl<T: Clone> Choice<T> {
    /// Build a dialog over `choices`, highlighting `default_index`.
    /// `default_index` is clamped into range; an empty list leaves
    /// `focused == 0`.
    pub fn new(choices: Vec<T>, default_index: usize) -> Self {
        let last = choices.len().saturating_sub(1);
        Self {
            focused: default_index.min(last),
            choices,
        }
    }

    /// Index of the currently-highlighted row.
    pub fn focused_index(&self) -> usize {
        self.focused
    }

    /// The currently-highlighted choice, or `None` if the list is empty.
    pub fn focused(&self) -> Option<&T> {
        self.choices.get(self.focused)
    }

    /// Process one key event. Vertical movement clamps at both edges
    /// (no wrap). Returns the terminal outcome for `Enter`/`Esc`/`Ctrl-C`
    /// and [`ChoiceOutcome::Pending`] for navigation or unrecognized keys.
    pub fn handle_key(&mut self, key: KeyEvent) -> ChoiceOutcome<T> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            return ChoiceOutcome::Cancelled;
        }
        match key.code {
            KeyCode::Esc => ChoiceOutcome::Cancelled,
            KeyCode::Enter => self
                .choices
                .get(self.focused)
                .cloned()
                .map(ChoiceOutcome::Confirmed)
                .unwrap_or(ChoiceOutcome::Pending),
            KeyCode::Char('j') | KeyCode::Down => {
                let last = self.choices.len().saturating_sub(1);
                if self.focused < last {
                    self.focused += 1;
                }
                ChoiceOutcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.focused = self.focused.saturating_sub(1);
                ChoiceOutcome::Pending
            }
            // Horizontal keys and anything else are intentionally inert —
            // a vertical list has no rightward move, so `l`/`→` on the
            // last column simply does nothing (no wrap).
            _ => ChoiceOutcome::Pending,
        }
    }
}

/// Draw the dialog as a centered overlay over `area`. The caller renders
/// the background view first; `Clear` blanks just the dialog rectangle.
pub fn render<T: ChoiceItem>(frame: &mut Frame, area: Rect, title: &str, choice: &Choice<T>) {
    let lines: Vec<Line> = choice
        .choices
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let label = c.label();
            if i == choice.focused {
                Line::from(Span::styled(
                    format!("> {label}"),
                    Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD),
                ))
            } else {
                Line::from(format!("  {label}"))
            }
        })
        .collect();

    let content_w = choice
        .choices
        .iter()
        .map(|c| c.label().chars().count())
        .max()
        .unwrap_or(0)
        + 2; // "> " / "  " prefix

    let body_lines = lines.len().max(1) as u16;
    let max_w = content_w.max(title.chars().count()).max(20) as u16;
    let height = (body_lines + 2).min(area.height).max(3.min(area.height));
    let width = (max_w + 4).min(area.width).max(20.min(area.width));

    let [vert] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(vert);

    frame.render_widget(Clear, horiz);
    let block = Block::default()
        .title(Span::styled(
            title.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let para = Paragraph::new(lines).block(block).wrap(Wrap { trim: false });
    frame.render_widget(para, horiz);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal `ChoiceItem` for exercising the widget in isolation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Pick {
        A,
        B,
        C,
    }

    impl ChoiceItem for Pick {
        fn label(&self) -> String {
            match self {
                Pick::A => "Alpha".to_string(),
                Pick::B => "Beta".to_string(),
                Pick::C => "Gamma".to_string(),
            }
        }
    }

    fn picks() -> Choice<Pick> {
        Choice::new(vec![Pick::A, Pick::B, Pick::C], 0)
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- construction -----------------------------------------------------

    #[test]
    fn new_clamps_default_index_into_range() {
        let c = Choice::new(vec![Pick::A, Pick::B], 99);
        assert_eq!(c.focused_index(), 1);
    }

    #[test]
    fn new_honors_in_range_default_index() {
        let c = Choice::new(vec![Pick::A, Pick::B, Pick::C], 2);
        assert_eq!(c.focused_index(), 2);
        assert_eq!(c.focused(), Some(&Pick::C));
    }

    #[test]
    fn empty_choices_focus_is_zero_and_enter_is_pending() {
        let mut c: Choice<Pick> = Choice::new(vec![], 3);
        assert_eq!(c.focused_index(), 0);
        assert_eq!(c.focused(), None);
        assert_eq!(c.handle_key(key(KeyCode::Enter)), ChoiceOutcome::Pending);
    }

    // -- navigation clamps at edges --------------------------------------

    #[test]
    fn j_and_down_move_focus_down_and_clamp_at_bottom() {
        let mut c = picks();
        assert_eq!(c.handle_key(key(KeyCode::Char('j'))), ChoiceOutcome::Pending);
        assert_eq!(c.focused_index(), 1);
        assert_eq!(c.handle_key(key(KeyCode::Down)), ChoiceOutcome::Pending);
        assert_eq!(c.focused_index(), 2);
        // Already at the last row — further down does nothing (no wrap).
        c.handle_key(key(KeyCode::Char('j')));
        c.handle_key(key(KeyCode::Down));
        assert_eq!(c.focused_index(), 2);
    }

    #[test]
    fn k_and_up_move_focus_up_and_clamp_at_top() {
        let mut c = Choice::new(vec![Pick::A, Pick::B, Pick::C], 2);
        assert_eq!(c.handle_key(key(KeyCode::Char('k'))), ChoiceOutcome::Pending);
        assert_eq!(c.focused_index(), 1);
        assert_eq!(c.handle_key(key(KeyCode::Up)), ChoiceOutcome::Pending);
        assert_eq!(c.focused_index(), 0);
        // Already at the first row — further up does nothing (no wrap).
        c.handle_key(key(KeyCode::Char('k')));
        c.handle_key(key(KeyCode::Up));
        assert_eq!(c.focused_index(), 0);
    }

    #[test]
    fn horizontal_and_unrecognized_keys_do_not_move_or_terminate() {
        let mut c = picks();
        for code in [
            KeyCode::Right,
            KeyCode::Left,
            KeyCode::Char('l'),
            KeyCode::Char('h'),
            KeyCode::Char('x'),
            KeyCode::Tab,
        ] {
            assert_eq!(c.handle_key(key(code)), ChoiceOutcome::Pending);
            assert_eq!(c.focused_index(), 0);
        }
    }

    // -- Enter confirms the focused variant ------------------------------

    #[test]
    fn enter_returns_the_focused_variant() {
        let mut c = picks();
        assert_eq!(
            c.handle_key(key(KeyCode::Enter)),
            ChoiceOutcome::Confirmed(Pick::A)
        );
        c.handle_key(key(KeyCode::Char('j')));
        assert_eq!(
            c.handle_key(key(KeyCode::Enter)),
            ChoiceOutcome::Confirmed(Pick::B)
        );
        c.handle_key(key(KeyCode::Char('j')));
        assert_eq!(
            c.handle_key(key(KeyCode::Enter)),
            ChoiceOutcome::Confirmed(Pick::C)
        );
    }

    // -- Esc / Ctrl-C cancel ---------------------------------------------

    #[test]
    fn esc_returns_cancelled() {
        let mut c = picks();
        assert_eq!(c.handle_key(key(KeyCode::Esc)), ChoiceOutcome::Cancelled);
    }

    #[test]
    fn ctrl_c_returns_cancelled() {
        let mut c = picks();
        assert_eq!(
            c.handle_key(key_with(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            ChoiceOutcome::Cancelled
        );
    }

    // -- render smoke -----------------------------------------------------

    #[test]
    fn render_writes_all_labels_and_title() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut c = picks();
        c.handle_key(key(KeyCode::Char('j'))); // focus Beta
        terminal
            .draw(|f| {
                let area = f.area();
                render(f, area, " Pick one ", &c);
            })
            .unwrap();
        let buf = terminal.backend().buffer().clone();
        let dump: String = (0..buf.area().height)
            .map(|y| {
                (0..buf.area().width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("Pick one"), "title missing:\n{dump}");
        assert!(dump.contains("Alpha"), "Alpha missing:\n{dump}");
        assert!(dump.contains("Beta"), "Beta missing:\n{dump}");
        assert!(dump.contains("Gamma"), "Gamma missing:\n{dump}");
    }
}
