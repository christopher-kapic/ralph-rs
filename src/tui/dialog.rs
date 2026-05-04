// Confirm-dialog primitive (TUI-plan.md §11).
//
// A blocking modal yes/no dialog used by destructive flows (archive, delete,
// cancel) that need explicit confirmation. The renderer and key-decision
// logic are factored apart from the event loop so they can be unit-tested
// without a real terminal.

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::theme;

/// One confirm dialog instance — title, body, and which button is the
/// default action (selected by Enter).
pub struct Confirm<'a> {
    pub title: &'a str,
    pub body: &'a str,
    /// Action chosen when the user presses Enter without typing y/n. Also
    /// drives the [Y/n] vs [y/N] hint.
    pub default: bool,
}

/// What a key press in the dialog means. Returned by `decide_key` so tests
/// can exercise the mapping without driving the event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Yes,
    No,
    /// Key was unrecognized — keep waiting for input.
    Pending,
}

/// Map a key event to a `Decision`. Pure for tests.
pub fn decide_key(key: KeyEvent, default: bool) -> Decision {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => Decision::Yes,
        KeyCode::Char('n') | KeyCode::Char('N') => Decision::No,
        KeyCode::Esc => Decision::No,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Decision::No,
        KeyCode::Enter => {
            if default {
                Decision::Yes
            } else {
                Decision::No
            }
        }
        _ => Decision::Pending,
    }
}

/// Render the dialog as an overlay over `area`. The caller is expected to
/// clear/redraw any background separately; this primitive only owns the
/// dialog rectangle.
pub fn render(frame: &mut Frame, area: Rect, c: &Confirm<'_>) {
    let dialog = centered_rect(area, c);
    frame.render_widget(Clear, dialog);

    let hint = if c.default {
        " [Y/n] confirm   [Esc] cancel "
    } else {
        " [y/N] confirm   [Esc] cancel "
    };
    let lines: Vec<Line> = c
        .body
        .lines()
        .map(Line::from)
        .chain(std::iter::once(Line::from("")))
        .chain(std::iter::once(Line::styled(
            hint,
            Style::default().add_modifier(Modifier::BOLD),
        )))
        .collect();

    let block = Block::default()
        .title(format!(" {} ", c.title))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let para = Paragraph::new(lines).block(block).wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// Compute the centered rectangle for the dialog given the available area.
fn centered_rect(area: Rect, c: &Confirm<'_>) -> Rect {
    let body_lines = c.body.lines().count().max(1) as u16;
    // 2 border rows + 1 spacer + 1 hint row + body
    let desired_h = body_lines.saturating_add(4);
    let height = desired_h.min(area.height).max(5.min(area.height));

    let body_w = c
        .body
        .lines()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0);
    let title_w = c.title.chars().count() + 2;
    let hint_w = " [y/N] confirm   [Esc] cancel ".chars().count();
    let desired_w = body_w.max(title_w).max(hint_w) as u16 + 4;
    let width = desired_w.min(area.width).max(20.min(area.width));

    let [vert] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(vert);
    horiz
}

/// Block until the user accepts or rejects the dialog. Renders over the
/// terminal's current contents — the caller is expected to have rendered the
/// background view immediately prior.
pub fn run<B: Backend>(terminal: &mut Terminal<B>, c: &Confirm<'_>) -> Result<bool> {
    loop {
        terminal.draw(|f| {
            let area = f.area();
            render(f, area, c);
        })?;
        match event::read()? {
            Event::Key(key) if key.kind == KeyEventKind::Press => match decide_key(key, c.default)
            {
                Decision::Yes => return Ok(true),
                Decision::No => return Ok(false),
                Decision::Pending => continue,
            },
            _ => continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- decide_key -------------------------------------------------------

    #[test]
    fn lowercase_y_is_yes() {
        assert_eq!(decide_key(key(KeyCode::Char('y')), false), Decision::Yes);
    }

    #[test]
    fn uppercase_y_is_yes() {
        assert_eq!(decide_key(key(KeyCode::Char('Y')), false), Decision::Yes);
    }

    #[test]
    fn lowercase_n_is_no() {
        assert_eq!(decide_key(key(KeyCode::Char('n')), true), Decision::No);
    }

    #[test]
    fn uppercase_n_is_no() {
        assert_eq!(decide_key(key(KeyCode::Char('N')), true), Decision::No);
    }

    #[test]
    fn esc_is_no() {
        assert_eq!(decide_key(key(KeyCode::Esc), true), Decision::No);
        assert_eq!(decide_key(key(KeyCode::Esc), false), Decision::No);
    }

    #[test]
    fn ctrl_c_is_no() {
        assert_eq!(
            decide_key(
                key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
                true
            ),
            Decision::No
        );
    }

    #[test]
    fn enter_uses_default_yes() {
        assert_eq!(decide_key(key(KeyCode::Enter), true), Decision::Yes);
    }

    #[test]
    fn enter_uses_default_no() {
        assert_eq!(decide_key(key(KeyCode::Enter), false), Decision::No);
    }

    #[test]
    fn unrecognized_key_is_pending() {
        assert_eq!(decide_key(key(KeyCode::Char('z')), false), Decision::Pending);
        assert_eq!(decide_key(key(KeyCode::Tab), false), Decision::Pending);
        assert_eq!(decide_key(key(KeyCode::Char(' ')), false), Decision::Pending);
    }

    // -- render -----------------------------------------------------------

    fn render_to_string(width: u16, height: u16, c: &Confirm<'_>) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render(frame, area, c);
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
    fn render_shows_title_and_body() {
        let out = render_to_string(
            60,
            10,
            &Confirm {
                title: "Archive plan",
                body: "Archive `my-plan`?",
                default: false,
            },
        );
        assert!(out.contains("Archive plan"), "title missing:\n{out}");
        assert!(out.contains("my-plan"), "body missing:\n{out}");
        assert!(out.contains("[y/N]"), "default-no hint missing:\n{out}");
    }

    #[test]
    fn render_uses_default_yes_hint() {
        let out = render_to_string(
            60,
            10,
            &Confirm {
                title: "Save",
                body: "Save changes?",
                default: true,
            },
        );
        assert!(out.contains("[Y/n]"), "default-yes hint missing:\n{out}");
    }

    #[test]
    fn render_does_not_panic_on_tiny_terminal() {
        let _ = render_to_string(
            10,
            5,
            &Confirm {
                title: "Long title that overflows",
                body: "A very long body line that definitely won't fit in ten columns",
                default: false,
            },
        );
    }

    #[test]
    fn centered_rect_fits_inside_area() {
        let area = Rect::new(0, 0, 80, 24);
        let c = Confirm {
            title: "T",
            body: "Body",
            default: false,
        };
        let r = centered_rect(area, &c);
        assert!(r.x + r.width <= area.x + area.width);
        assert!(r.y + r.height <= area.y + area.height);
        assert!(r.width > 0 && r.height > 0);
    }
}
