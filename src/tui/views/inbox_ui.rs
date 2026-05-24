// Interruptions-inbox rendering (docs/dag-redesign.md §12.3 / §12.4).
//
// Presentation only — the state machine lives in `inbox.rs`. Draws the
// cross-branch list (open items in the §12.5 orange "blocked/interrupted"
// color, resolved items dimmed via `theme::CHROME_DIM`), the chrome
// breadcrumb with the open-count badge, and — while in run-through — the
// §12.4 ranked-answer / blocker modal on top.

use std::path::Path;

use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

use crate::plan::{InterruptionKind, PlanStatus};
use crate::tui::chrome::{self, Chrome};
use crate::tui::help;
use crate::tui::theme;
use crate::tui::views::answer_modal::{InterruptionFocus, InterruptionModal};
use crate::tui::views::inbox::{InboxMode, InboxState};

/// Render the whole inbox view.
pub fn draw(frame: &mut Frame, app: &InboxState, project: &Path) {
    let badge = format!("inbox ({})", app.open_count());
    let crumbs: [&str; 2] = ["ralph", badge.as_str()];
    let hint = match app.mode() {
        InboxMode::List => "[j/k] nav  [enter/a] answer all  [?] help  [q] back",
        InboxMode::RunThrough => {
            "[j/k] options  [tab] field  [f] freeform  [m] comment  [enter] resolve  [esc] list"
        }
    };
    let body = chrome::render(frame, &Chrome::new(&crumbs, hint, project));

    draw_list(frame, app, body);

    if let Some(modal) = app.modal() {
        render_interruption_modal(frame, frame.area(), modal);
    }

    if app.help.is_visible() {
        help::render(frame, frame.area(), &help::for_inbox());
    }
}

fn draw_list(frame: &mut Frame, app: &InboxState, area: Rect) {
    let block = Block::default()
        .title(" Interruptions ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ratatui::style::Color::Cyan));

    if app.items().is_empty() {
        let para = Paragraph::new(Span::styled(
            "  No interruptions — every branch is unblocked.",
            Style::default().fg(theme::CHROME_DIM),
        ))
        .block(block);
        frame.render_widget(para, area);
        return;
    }

    let items: Vec<ListItem> = app
        .items()
        .iter()
        .map(|it| {
            let kind_glyph = match it.interruption.kind {
                InterruptionKind::Question => "?",
                InterruptionKind::Blocker => "■",
            };
            let label = format!(
                "{kind_glyph} [{}/{}] {}",
                it.plan_slug, it.step_short_id, it.interruption.body
            );
            let style = if it.is_open() {
                // §12.5: an open interruption is the orange
                // blocked/interrupted concept — route through the single
                // mapping so it reads identically to the plan dot / glyph.
                Style::default().fg(theme::plan_status_color(PlanStatus::Interrupted))
            } else {
                // Resolved items stay visible but dimmed (§12.3).
                Style::default()
                    .fg(theme::CHROME_DIM)
                    .add_modifier(Modifier::DIM)
            };
            ListItem::new(Line::from(Span::styled(label, style)))
        })
        .collect();

    let mut ls = ListState::default();
    ls.select(Some(app.cursor()));
    let list = List::new(items)
        .block(block)
        .highlight_style(
            Style::default()
                .bg(theme::CURSOR)
                .fg(ratatui::style::Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    frame.render_stateful_widget(list, area, &mut ls);
}

/// The §12.4 ranked-answer / blocker modal.
fn render_interruption_modal(frame: &mut Frame, area: Rect, modal: &InterruptionModal) {
    let mut lines: Vec<Line> = Vec::new();
    let title = if modal.is_blocker() {
        " Resolve blocker "
    } else {
        " Answer question "
    };
    lines.push(Line::from(Span::styled(
        modal.body.clone(),
        Style::default().add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(""));

    // Phase C: option-list rendering is driven by `options.is_empty()`, not
    // by `is_blocker()`. The Phase B auto-raised retry-exhausted blocker is
    // a Blocker that DOES carry two ranked options (Retry / Fail); hiding
    // them behind an `is_blocker` branch left the human with no visible
    // way to choose. A harness-raised blocker still has empty options and
    // falls through to the freeform-only placeholder.
    if modal.options.is_empty() {
        let placeholder = if modal.is_blocker() {
            "Blocker — resolve / resolve-with-comment (no options)."
        } else {
            "Freeform-only question (no proposed answers)."
        };
        lines.push(Line::from(Span::styled(
            placeholder,
            Style::default().fg(theme::CHROME_DIM),
        )));
    } else {
        let header = if modal.is_blocker() {
            "Proposed resolutions (priority order; #1 = agent's best):"
        } else {
            "Proposed answers (priority order; #1 = agent's best):"
        };
        lines.push(Line::from(Span::styled(
            header,
            Style::default().add_modifier(Modifier::BOLD),
        )));
        for (i, opt) in modal.options.iter().enumerate() {
            let selected = modal.focus == InterruptionFocus::Options && i == modal.selected_option;
            let marker = if selected { "▶" } else { " " };
            let style = if selected {
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!("  {marker} {}. {}", i + 1, opt.text),
                style,
            )));
        }
    }

    lines.push(Line::from(""));
    let ff = if modal.freeform.is_empty() {
        "(empty — press f to edit in $EDITOR)".to_string()
    } else {
        modal.freeform.clone()
    };
    let ff_focus = modal.focus == InterruptionFocus::Freeform;
    lines.push(Line::from(vec![
        Span::styled(
            "Freeform: ",
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(if ff_focus {
                    theme::CURSOR
                } else {
                    ratatui::style::Color::Reset
                }),
        ),
        Span::raw(ff),
    ]));
    let cm = if modal.comment.is_empty() {
        "(none — press m to add)".to_string()
    } else {
        modal.comment.clone()
    };
    let cm_focus = modal.focus == InterruptionFocus::Comment;
    lines.push(Line::from(vec![
        Span::styled(
            "Comment: ",
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(if cm_focus {
                    theme::CURSOR
                } else {
                    ratatui::style::Color::Reset
                }),
        ),
        Span::raw(cm),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "[tab] field  [f] freeform  [m] comment  [enter] resolve  [esc] cancel",
        Style::default().add_modifier(Modifier::DIM),
    )));

    let h = (lines.len() as u16 + 2)
        .min(area.height)
        .max(5.min(area.height));
    let w = 70.min(area.width).max(20.min(area.width));
    let [v] = Layout::vertical([Constraint::Length(h)])
        .flex(Flex::Center)
        .areas(area);
    let [dialog] = Layout::horizontal([Constraint::Length(w)])
        .flex(Flex::Center)
        .areas(v);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::plan_status_color(PlanStatus::Interrupted)));
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        dialog,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Interruption, InterruptionKind, InterruptionOption, InterruptionState};
    use crate::tui::views::inbox::InboxItem;
    use chrono::Utc;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn item(id: &str, open: bool, kind: InterruptionKind) -> InboxItem {
        InboxItem {
            interruption: Interruption {
                id: id.to_string(),
                step_id: format!("step-{id}"),
                attempt: 1,
                kind,
                body: format!("Body {id}"),
                options: vec![InterruptionOption {
                    text: "opt".to_string(),
                    priority: 1,
                }],
                resolution: None,
                comment: None,
                state: if open {
                    InterruptionState::Open
                } else {
                    InterruptionState::Resolved
                },
                asked_at: Utc::now(),
                resolved_at: None,
            },
            plan_slug: "p".to_string(),
            step_short_id: format!("s{id}"),
        }
    }

    fn render_to_string(app: &InboxState) -> String {
        let backend = TestBackend::new(90, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, app, Path::new("/tmp"))).unwrap();
        let buf = terminal.backend().buffer().clone();
        (0..buf.area().height)
            .map(|y| {
                (0..buf.area().width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn renders_open_count_badge_and_items() {
        let app = InboxState::new(vec![
            item("1", true, InterruptionKind::Question),
            item("2", false, InterruptionKind::Blocker),
        ]);
        let out = render_to_string(&app);
        assert!(out.contains("inbox (1)"), "badge missing:\n{out}");
        assert!(out.contains("Body 1"), "open item missing:\n{out}");
        assert!(
            out.contains("Body 2"),
            "resolved item still visible:\n{out}"
        );
    }

    #[test]
    fn run_through_renders_modal_with_priority_options() {
        let mut app = InboxState::new(vec![item("1", true, InterruptionKind::Question)]);
        app.start_run_through();
        let out = render_to_string(&app);
        assert!(
            out.contains("Proposed answers"),
            "modal options header missing:\n{out}"
        );
        assert!(out.contains("Freeform:"), "freeform field missing:\n{out}");
    }

    #[test]
    fn blocker_modal_states_no_options() {
        let mut app = InboxState::new(vec![{
            let mut it = item("b", true, InterruptionKind::Blocker);
            it.interruption.options.clear();
            it
        }]);
        app.start_run_through();
        let out = render_to_string(&app);
        assert!(
            out.contains("no options"),
            "blocker modal should say no options:\n{out}"
        );
    }

    #[test]
    fn empty_inbox_renders_placeholder() {
        let app = InboxState::new(vec![]);
        let out = render_to_string(&app);
        assert!(
            out.contains("No interruptions"),
            "empty placeholder missing:\n{out}"
        );
    }
}
