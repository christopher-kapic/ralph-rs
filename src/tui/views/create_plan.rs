// Create-plan modal (TUI-plan.md §5).
//
// A three-field inline form (slug → description → tests) shown over the
// plan-list view. The modal is split into a pure state machine
// (`CreatePlanModal::handle_key`) and a renderer (`render`) so the state
// machine can be unit-tested without a real terminal. The event loop in
// `commands::run::plan_list_create_plan` drives the modal: it reads keys,
// asks `handle_key` for an `Outcome`, and on `Submit` hands the parsed
// fields to `storage::create_plan`.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::tui::theme;

/// Which of the three fields currently has focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Slug,
    Description,
    Tests,
}

impl Field {
    /// Next field in the slug → description → tests cycle (Tab).
    fn next(self) -> Self {
        match self {
            Field::Slug => Field::Description,
            Field::Description => Field::Tests,
            Field::Tests => Field::Slug,
        }
    }

    /// Previous field (BackTab).
    fn prev(self) -> Self {
        match self {
            Field::Slug => Field::Tests,
            Field::Description => Field::Slug,
            Field::Tests => Field::Description,
        }
    }
}

/// What the modal returns from `handle_key`. The event loop loops on
/// `Pending`, exits on `Cancelled`, and on `Submit` calls into storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed, modal stays open.
    Pending,
    /// User cancelled (`Esc` or `Ctrl-C`).
    Cancelled,
    /// User submitted a non-empty slug. `tests` is the comma-split,
    /// trimmed, non-empty list parsed from the tests field.
    Submit {
        slug: String,
        description: String,
        tests: Vec<String>,
    },
}

/// Modal state — three text buffers plus the focused field.
pub struct CreatePlanModal {
    pub focused: Field,
    pub slug: String,
    pub description: String,
    pub tests: String,
}

impl Default for CreatePlanModal {
    fn default() -> Self {
        Self::new()
    }
}

impl CreatePlanModal {
    pub fn new() -> Self {
        Self {
            focused: Field::Slug,
            slug: String::new(),
            description: String::new(),
            tests: String::new(),
        }
    }

    /// Pure key-event handler. Splits responsibility with the event loop:
    /// returns the next state via `Outcome` rather than mutating shared
    /// terminal state, so tests can drive arbitrary key sequences without
    /// touching crossterm.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // Esc / Ctrl-C cancels regardless of which field has focus.
        if key.code == KeyCode::Esc {
            return Outcome::Cancelled;
        }
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Outcome::Cancelled;
        }

        match key.code {
            KeyCode::Tab => {
                self.focused = self.focused.next();
                Outcome::Pending
            }
            KeyCode::BackTab => {
                self.focused = self.focused.prev();
                Outcome::Pending
            }
            KeyCode::Enter => {
                // On the last field, Enter submits; on earlier fields, Enter
                // advances to the next. Submit refuses to fire with an empty
                // slug — focus bounces back to the slug field instead so the
                // user can type one.
                if self.focused == Field::Tests {
                    self.try_submit()
                } else {
                    self.focused = self.focused.next();
                    Outcome::Pending
                }
            }
            KeyCode::Backspace => {
                self.buffer_mut().pop();
                Outcome::Pending
            }
            KeyCode::Char(c) => {
                // Ignore Ctrl-modified chars other than Ctrl-C (handled
                // above) so e.g. Ctrl-A doesn't insert literal "a".
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    return Outcome::Pending;
                }
                self.buffer_mut().push(c);
                Outcome::Pending
            }
            _ => Outcome::Pending,
        }
    }

    /// Attempt to submit. Bounces focus back to Slug when the slug is empty
    /// (after trimming) so the user gets visible cursor feedback rather than
    /// a silent no-op.
    fn try_submit(&mut self) -> Outcome {
        let slug = self.slug.trim().to_string();
        if slug.is_empty() {
            self.focused = Field::Slug;
            return Outcome::Pending;
        }
        Outcome::Submit {
            slug,
            description: self.description.trim().to_string(),
            tests: parse_tests(&self.tests),
        }
    }

    fn buffer_mut(&mut self) -> &mut String {
        match self.focused {
            Field::Slug => &mut self.slug,
            Field::Description => &mut self.description,
            Field::Tests => &mut self.tests,
        }
    }
}

/// Split a comma-separated tests field into a `Vec<String>`. Whitespace
/// around each entry is trimmed and empty entries are dropped, so
/// `"cargo test, , cargo clippy "` becomes
/// `vec!["cargo test", "cargo clippy"]`.
pub fn parse_tests(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(String::from)
        .collect()
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

const TITLE: &str = " New plan ";
const HINT: &str = " [Enter] next/submit   [Tab] focus   [Esc] cancel ";

/// Draw the modal as an overlay over the supplied area. The caller is
/// expected to render the background view immediately prior — `Clear`
/// blanks just the dialog rectangle so the live tile list stays visible
/// behind the unused area.
pub fn render(frame: &mut Frame, area: Rect, modal: &CreatePlanModal) {
    let dialog = centered_rect(area);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(TITLE)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let lines = vec![
        field_line("Slug:        ", &modal.slug, modal.focused == Field::Slug),
        field_line(
            "Description: ",
            &modal.description,
            modal.focused == Field::Description,
        ),
        field_line("Tests:       ", &modal.tests, modal.focused == Field::Tests),
        Line::from(""),
        Line::from(Span::styled(
            "Tests are comma-separated shell commands (e.g. `cargo test, cargo clippy`).",
            Style::default().fg(theme::CHROME_DIM),
        )),
        Line::from(""),
        Line::styled(HINT, Style::default().add_modifier(Modifier::BOLD)),
    ];

    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// One labelled input row. The focused row wears a trailing block-cursor
/// glyph so the user sees where typed input will land.
fn field_line<'a>(label: &'a str, value: &'a str, focused: bool) -> Line<'a> {
    let label_style = Style::default()
        .fg(theme::CURSOR)
        .add_modifier(Modifier::BOLD);
    let value_style = Style::default();
    let mut spans = vec![
        Span::styled(label, label_style),
        Span::styled(value, value_style),
    ];
    if focused {
        spans.push(Span::styled(
            "▌",
            Style::default()
                .fg(theme::CURSOR)
                .add_modifier(Modifier::SLOW_BLINK),
        ));
    }
    Line::from(spans)
}

/// Compute the centered dialog rectangle. Width is bounded so the modal
/// never spans the full terminal — it should feel like a dialog, not a
/// view replacement.
fn centered_rect(area: Rect) -> Rect {
    let desired_w = 60u16.min(area.width);
    let desired_h = 9u16.min(area.height);
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
    use crossterm::event::KeyEvent;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    fn type_into(modal: &mut CreatePlanModal, s: &str) {
        for c in s.chars() {
            assert_eq!(modal.handle_key(key(KeyCode::Char(c))), Outcome::Pending);
        }
    }

    // -- focus cycling -----------------------------------------------------

    #[test]
    fn new_starts_focused_on_slug() {
        let modal = CreatePlanModal::new();
        assert_eq!(modal.focused, Field::Slug);
        assert!(modal.slug.is_empty());
        assert!(modal.description.is_empty());
        assert!(modal.tests.is_empty());
    }

    #[test]
    fn tab_cycles_forward() {
        let mut modal = CreatePlanModal::new();
        modal.handle_key(key(KeyCode::Tab));
        assert_eq!(modal.focused, Field::Description);
        modal.handle_key(key(KeyCode::Tab));
        assert_eq!(modal.focused, Field::Tests);
        modal.handle_key(key(KeyCode::Tab));
        assert_eq!(modal.focused, Field::Slug);
    }

    #[test]
    fn back_tab_cycles_backward() {
        let mut modal = CreatePlanModal::new();
        modal.handle_key(key(KeyCode::BackTab));
        assert_eq!(modal.focused, Field::Tests);
        modal.handle_key(key(KeyCode::BackTab));
        assert_eq!(modal.focused, Field::Description);
        modal.handle_key(key(KeyCode::BackTab));
        assert_eq!(modal.focused, Field::Slug);
    }

    // -- typing ------------------------------------------------------------

    #[test]
    fn typing_lands_in_focused_buffer() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "hello");
        assert_eq!(modal.slug, "hello");
        modal.handle_key(key(KeyCode::Tab));
        type_into(&mut modal, "world");
        assert_eq!(modal.description, "world");
        modal.handle_key(key(KeyCode::Tab));
        type_into(&mut modal, "cargo test");
        assert_eq!(modal.tests, "cargo test");
        // Slug is unchanged after typing into other fields.
        assert_eq!(modal.slug, "hello");
    }

    #[test]
    fn backspace_pops_focused_buffer() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "abc");
        modal.handle_key(key(KeyCode::Backspace));
        assert_eq!(modal.slug, "ab");
        modal.handle_key(key(KeyCode::Backspace));
        modal.handle_key(key(KeyCode::Backspace));
        modal.handle_key(key(KeyCode::Backspace)); // extra pop on empty is a no-op
        assert_eq!(modal.slug, "");
    }

    #[test]
    fn ctrl_modified_chars_are_ignored() {
        let mut modal = CreatePlanModal::new();
        // Ctrl-A should NOT insert "a"; only Ctrl-C should fire (and that's
        // covered by `cancelled_by_ctrl_c`).
        let outcome = modal.handle_key(key_with_mod(KeyCode::Char('a'), KeyModifiers::CONTROL));
        assert_eq!(outcome, Outcome::Pending);
        assert!(modal.slug.is_empty());
    }

    // -- enter advancement / submit ---------------------------------------

    #[test]
    fn enter_advances_from_slug_to_description() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "my-plan");
        let outcome = modal.handle_key(key(KeyCode::Enter));
        assert_eq!(outcome, Outcome::Pending);
        assert_eq!(modal.focused, Field::Description);
    }

    #[test]
    fn enter_advances_from_description_to_tests() {
        let mut modal = CreatePlanModal::new();
        modal.focused = Field::Description;
        let outcome = modal.handle_key(key(KeyCode::Enter));
        assert_eq!(outcome, Outcome::Pending);
        assert_eq!(modal.focused, Field::Tests);
    }

    #[test]
    fn enter_on_tests_with_filled_slug_submits() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "my-plan");
        modal.handle_key(key(KeyCode::Tab));
        type_into(&mut modal, "do the thing");
        modal.handle_key(key(KeyCode::Tab));
        type_into(&mut modal, "cargo test, cargo clippy");
        let outcome = modal.handle_key(key(KeyCode::Enter));
        match outcome {
            Outcome::Submit {
                slug,
                description,
                tests,
            } => {
                assert_eq!(slug, "my-plan");
                assert_eq!(description, "do the thing");
                assert_eq!(tests, vec!["cargo test", "cargo clippy"]);
            }
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn submit_trims_leading_trailing_whitespace() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "  spaced  ");
        modal.handle_key(key(KeyCode::Tab));
        type_into(&mut modal, "  desc  ");
        modal.focused = Field::Tests;
        let outcome = modal.handle_key(key(KeyCode::Enter));
        match outcome {
            Outcome::Submit {
                slug, description, ..
            } => {
                assert_eq!(slug, "spaced");
                assert_eq!(description, "desc");
            }
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn submit_with_empty_tests_returns_empty_vec() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "p");
        modal.focused = Field::Tests;
        let outcome = modal.handle_key(key(KeyCode::Enter));
        match outcome {
            Outcome::Submit { tests, .. } => assert!(tests.is_empty()),
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn enter_on_tests_with_empty_slug_bounces_to_slug() {
        let mut modal = CreatePlanModal::new();
        modal.focused = Field::Tests;
        let outcome = modal.handle_key(key(KeyCode::Enter));
        assert_eq!(outcome, Outcome::Pending);
        assert_eq!(modal.focused, Field::Slug);
    }

    #[test]
    fn enter_on_tests_with_only_whitespace_slug_bounces_to_slug() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "   ");
        modal.focused = Field::Tests;
        let outcome = modal.handle_key(key(KeyCode::Enter));
        assert_eq!(outcome, Outcome::Pending);
        assert_eq!(modal.focused, Field::Slug);
    }

    // -- cancel -----------------------------------------------------------

    #[test]
    fn cancelled_by_esc() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "p");
        let outcome = modal.handle_key(key(KeyCode::Esc));
        assert_eq!(outcome, Outcome::Cancelled);
    }

    #[test]
    fn cancelled_by_ctrl_c() {
        let mut modal = CreatePlanModal::new();
        let outcome = modal.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(outcome, Outcome::Cancelled);
    }

    // -- parse_tests ------------------------------------------------------

    #[test]
    fn parse_tests_empty_string_returns_empty() {
        assert!(parse_tests("").is_empty());
    }

    #[test]
    fn parse_tests_single_command() {
        assert_eq!(parse_tests("cargo test"), vec!["cargo test"]);
    }

    #[test]
    fn parse_tests_splits_on_commas_and_trims() {
        assert_eq!(
            parse_tests("cargo test, cargo clippy ,  cargo build"),
            vec!["cargo test", "cargo clippy", "cargo build"]
        );
    }

    #[test]
    fn parse_tests_drops_empty_entries() {
        assert_eq!(
            parse_tests(", cargo test, , cargo clippy ,"),
            vec!["cargo test", "cargo clippy"]
        );
    }

    #[test]
    fn parse_tests_only_commas_returns_empty() {
        assert!(parse_tests(",,,").is_empty());
    }

    // -- render smoke -----------------------------------------------------

    fn render_to_string(width: u16, height: u16, modal: &CreatePlanModal) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render(frame, area, modal);
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
    fn render_shows_title_and_field_labels() {
        let mut modal = CreatePlanModal::new();
        type_into(&mut modal, "my-plan");
        let out = render_to_string(80, 24, &modal);
        assert!(out.contains("New plan"), "title missing:\n{out}");
        assert!(out.contains("Slug:"), "slug label missing:\n{out}");
        assert!(out.contains("Description:"), "desc label missing:\n{out}");
        assert!(out.contains("Tests:"), "tests label missing:\n{out}");
        assert!(out.contains("my-plan"), "slug value missing:\n{out}");
    }

    #[test]
    fn render_does_not_panic_on_tiny_terminal() {
        let modal = CreatePlanModal::new();
        let _ = render_to_string(8, 4, &modal);
    }
}
