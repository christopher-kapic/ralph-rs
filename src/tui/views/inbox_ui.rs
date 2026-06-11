// Interruptions-inbox rendering (docs/dag-redesign.md §12.3 / §12.4).
//
// Presentation only — the state machine lives in `inbox.rs`. Draws the
// cross-branch list (open items in the §12.5 orange "blocked/interrupted"
// color, resolved items dimmed via `theme::CHROME_DIM`), the chrome
// breadcrumb with the open-count badge, and — while in run-through — the
// §12.4 ranked-answer / blocker modal on top.

use std::path::Path;
use std::time::Instant;

use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

use crate::plan::{InterruptionKind, PlanStatus};
use crate::tui::chrome::{self, Chrome, display_width, right_truncate};
use crate::tui::help;
use crate::tui::theme;
use crate::tui::views::answer_modal::{InterruptionFocus, InterruptionModal};
use crate::tui::views::inbox::{InboxMode, InboxState};

const LIST_BODY_PREVIEW_COLS: usize = 96;

/// Render the whole inbox view.
pub fn draw(frame: &mut Frame, app: &mut InboxState, project: &Path) {
    app.toasts.prune(Instant::now());
    let badge = format!("inbox ({})", app.open_count());
    let crumbs: [&str; 2] = ["ralph", badge.as_str()];
    let hint = match app.mode() {
        InboxMode::List => "[j/k/g/G] nav  [enter/a] answer all  [?] help  [q] back",
        InboxMode::RunThrough => {
            "[j/k] options  [tab] field  [f] freeform  [m] comment  [enter] resolve  [esc] list"
        }
    };
    let body = chrome::render(frame, &Chrome::new(&crumbs, hint, project));

    draw_list(frame, app, body);

    if let Some(modal) = app.modal() {
        render_interruption_modal(frame, frame.area(), modal);
    }

    if let Some(toast) = app.toasts.current() {
        render_toast_overlay(frame, frame.area(), &toast.text, toast.color);
    }

    if app.help.is_visible() {
        help::render(frame, frame.area(), &help::for_inbox());
    }
}

fn render_toast_overlay(frame: &mut Frame, area: Rect, text: &str, color: ratatui::style::Color) {
    let max_toast = area.width.saturating_sub(1).max(1);
    let clipped = right_truncate(text, max_toast as usize);
    let desired = display_width(&clipped).min(max_toast as usize) as u16;
    if desired == 0 || area.height == 0 {
        return;
    }
    let toast_area = Rect {
        x: area.x,
        y: area.y + area.height - 1,
        width: desired,
        height: 1,
    };
    let para = Paragraph::new(clipped).style(Style::default().fg(color));
    frame.render_widget(para, toast_area);
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
                it.plan_slug,
                it.step_short_id,
                inbox_body_preview(&it.interruption.body)
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

fn inbox_body_preview(body: &str) -> String {
    let multiline = body.lines().count() > 1;
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated = display_width(&collapsed) > LIST_BODY_PREVIEW_COLS;
    let mut preview = right_truncate(&collapsed, LIST_BODY_PREVIEW_COLS);
    if preview.is_empty() {
        preview.push_str("(empty)");
    }
    if multiline || truncated {
        preview = format!("[more: enter/a] {preview}");
    }
    preview
}

/// The §12.4 ranked-answer / blocker modal. Shared with step-detail's inline
/// answer flow so both surfaces render one identical modal.
pub(crate) fn render_interruption_modal(frame: &mut Frame, area: Rect, modal: &InterruptionModal) {
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

    fn render_to_string(app: &mut InboxState) -> String {
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
        let mut app = InboxState::new(vec![
            item("1", true, InterruptionKind::Question),
            item("2", false, InterruptionKind::Blocker),
        ]);
        let out = render_to_string(&mut app);
        assert!(out.contains("inbox (1)"), "badge missing:\n{out}");
        assert!(out.contains("Body 1"), "open item missing:\n{out}");
        assert!(
            out.contains("Body 2"),
            "resolved item still visible:\n{out}"
        );
    }

    #[test]
    fn list_row_marks_multiline_body_as_readable_in_modal() {
        let mut it = item("1", true, InterruptionKind::Question);
        it.interruption.body = "first line\nsecond line".to_string();
        let mut app = InboxState::new(vec![it]);

        let out = render_to_string(&mut app);

        assert!(
            out.contains("first line second line"),
            "body collapsed:\n{out}"
        );
        assert!(
            out.contains("[more: enter/a]"),
            "multiline affordance missing:\n{out}"
        );
    }

    #[test]
    fn list_row_marks_long_body_as_truncated() {
        let mut it = item("1", true, InterruptionKind::Question);
        it.interruption.body = "x".repeat(LIST_BODY_PREVIEW_COLS + 20);
        let mut app = InboxState::new(vec![it]);

        let out = render_to_string(&mut app);

        assert!(
            out.contains("[more: enter/a]"),
            "long-body affordance missing:\n{out}"
        );
    }

    #[test]
    fn body_preview_truncates_by_display_width() {
        let preview = inbox_body_preview(&"界".repeat(LIST_BODY_PREVIEW_COLS));
        assert!(
            display_width(&preview) <= LIST_BODY_PREVIEW_COLS + display_width("[more: enter/a] "),
            "wide-glyph preview must be display-width bounded: {preview:?}"
        );
        assert!(
            preview.contains('…'),
            "wide-glyph truncation should use the normal ellipsis marker: {preview:?}"
        );
    }

    #[test]
    fn run_through_renders_modal_with_priority_options() {
        let mut app = InboxState::new(vec![item("1", true, InterruptionKind::Question)]);
        app.start_run_through();
        let out = render_to_string(&mut app);
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
        let out = render_to_string(&mut app);
        assert!(
            out.contains("no options"),
            "blocker modal should say no options:\n{out}"
        );
    }

    #[test]
    fn empty_inbox_renders_placeholder() {
        let mut app = InboxState::new(vec![]);
        let out = render_to_string(&mut app);
        assert!(
            out.contains("No interruptions"),
            "empty placeholder missing:\n{out}"
        );
    }

    #[test]
    fn renders_toast_overlay() {
        let mut app = InboxState::new(vec![item("1", true, InterruptionKind::Question)]);
        app.push_toast("No $EDITOR set", crate::tui::toast::ToastKind::Error);

        let out = render_to_string(&mut app);

        assert!(out.contains("No $EDITOR set"), "toast missing:\n{out}");
    }
}
