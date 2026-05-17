// Shared compact step-list widget (TUI-plan.md §5 / §7).
//
// Renders the column of `<glyph> <num>. <title> [N]?` rows shown both as the
// left sidebar of plan-detail (interactive) and as the right preview pane of
// plan-list (read-only). Behavior is purely presentational: the caller passes
// the steps slice, a multi-selection set, an optional cursor index, an
// active-run flag, and a `ListState` for scroll persistence.
//
// Cursor highlight uses `theme::CURSOR` (yellow #f7d135) per §12. The
// selection `[N]` badge uses `theme::SELECTION` (cyan #56d0d9) and is
// 1-based by selection order. The active-run flag is plumbed through so
// callers (plan-detail when bound to a runner) can opt into running-state
// emphasis on the cursor row; passive callers (plan-list preview) leave it
// false.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState};

use crate::plan::{Step, StepStatus};
use crate::tui::selection::Selection;
use crate::tui::theme;

/// Single-glyph indicator for a [`StepStatus`]. Public so callers that need
/// to mirror the same glyph outside of a list row (status banners, log
/// lines) stay in sync with the list rendering.
pub fn status_glyph(status: StepStatus) -> &'static str {
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

/// Foreground color for a row given its step status. Used by both the row
/// styler here and the right-pane status line in plan-detail so the two
/// stay aligned.
fn status_fg(status: StepStatus) -> Color {
    match status {
        StepStatus::Complete => theme::STATUS_COMPLETE,
        StepStatus::InProgress => theme::STATUS_IN_PROGRESS,
        StepStatus::Failed => theme::STATUS_FAILED,
        StepStatus::Skipped => theme::CHROME_DIM,
        StepStatus::Aborted => theme::STATUS_FAILED,
        StepStatus::Pending => theme::STATUS_PENDING,
        // §3.3 overlay; reuse the existing derived-question token (the
        // §12.5 `STATUS_BLOCKED` rename lands with the Phase 4 TUI work).
        StepStatus::Blocked => theme::STATUS_QUESTION,
    }
}

/// Render the compact step list into `area`.
///
/// - `cursor_index`: when `Some`, that row gets the `theme::CURSOR`
///   highlight; when `None`, no cursor is drawn (used by read-only
///   previews).
/// - `active_run`: signals that a runner is bound to this plan; lets the
///   widget add subtle emphasis on the highlighted row so the user can
///   tell at a glance that a run is in flight.
/// - `list_state`: carries the viewport offset across frames. Callers
///   should keep one `ListState` per logical list so scroll position
///   survives re-renders.
#[allow(clippy::too_many_arguments)]
pub fn render(
    frame: &mut Frame,
    area: Rect,
    steps: &[Step],
    selection: &Selection<String>,
    cursor_index: Option<usize>,
    active_run: bool,
    title: &str,
    list_state: &mut ListState,
) {
    let items: Vec<ListItem> = steps
        .iter()
        .enumerate()
        .map(|(i, step)| {
            let glyph = status_glyph(step.status);
            let label = format!("{glyph} {}. {}", i + 1, step.title);
            let mut row_style = Style::default().fg(status_fg(step.status));
            if matches!(step.status, StepStatus::InProgress) {
                row_style = row_style.add_modifier(Modifier::BOLD);
            }
            let mut spans = vec![Span::styled(label, row_style)];
            if let Some(n) = selection.index_of(&step.id) {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(
                    format!("[{n}]"),
                    Style::default()
                        .fg(theme::SELECTION)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();

    let block = Block::default()
        .title(format!(" {title} "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    // Cursor row uses theme::CURSOR as background with black text for
    // contrast (TUI-plan.md §12). When a run is bound, layer REVERSED on
    // top so the cursor reads as "active" — visible but unobtrusive.
    let mut highlight = Style::default()
        .bg(theme::CURSOR)
        .fg(Color::Black)
        .add_modifier(Modifier::BOLD);
    if active_run {
        highlight = highlight.add_modifier(Modifier::REVERSED);
    }

    let list = List::new(items)
        .block(block)
        .highlight_style(highlight)
        .highlight_symbol("> ");

    list_state.select(cursor_index);
    frame.render_stateful_widget(list, area, list_state);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ChangePolicy, Step, StepStatus};
    use chrono::Utc;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;

    fn make_step(idx: usize, title: &str, status: StepStatus) -> Step {
        Step {
            id: format!("s{idx}"),
            short_id: String::new(),
            plan_id: "p1".to_string(),
            sort_key: format!("a{idx}"),
            title: title.to_string(),
            description: String::new(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status,
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
        }
    }

    fn render_to_buffer(
        steps: &[Step],
        selection: &Selection<String>,
        cursor_index: Option<usize>,
        active_run: bool,
        title: &str,
        w: u16,
        h: u16,
    ) -> Buffer {
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut list_state = ListState::default();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render(
                    frame,
                    area,
                    steps,
                    selection,
                    cursor_index,
                    active_run,
                    title,
                    &mut list_state,
                );
            })
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn buffer_text(buffer: &Buffer) -> String {
        (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Find the y row of the cell whose symbol matches `needle` somewhere
    /// in the row. Returns the first match (top-down).
    fn find_row(buffer: &Buffer, needle: &str) -> Option<u16> {
        for y in 0..buffer.area().height {
            let line: String = (0..buffer.area().width)
                .map(|x| buffer[(x, y)].symbol())
                .collect();
            if line.contains(needle) {
                return Some(y);
            }
        }
        None
    }

    #[test]
    fn renders_all_step_titles() {
        let steps = vec![
            make_step(0, "Alpha task", StepStatus::Pending),
            make_step(1, "Bravo task", StepStatus::Complete),
            make_step(2, "Charlie task", StepStatus::Failed),
        ];
        let selection: Selection<String> = Selection::new();
        let buffer = render_to_buffer(&steps, &selection, Some(0), false, "demo", 60, 8);
        let rendered = buffer_text(&buffer);
        assert!(
            rendered.contains("Alpha task"),
            "missing Alpha:\n{rendered}"
        );
        assert!(
            rendered.contains("Bravo task"),
            "missing Bravo:\n{rendered}"
        );
        assert!(
            rendered.contains("Charlie task"),
            "missing Charlie:\n{rendered}"
        );
    }

    #[test]
    fn cursor_row_uses_theme_cursor_background() {
        // Cursor highlight bg should be theme::CURSOR (#f7d135). We pick
        // row 1 (Bravo) so the highlighted row isn't the first item, which
        // makes the assertion robust against any leading-row chrome.
        let steps = vec![
            make_step(0, "Alpha", StepStatus::Pending),
            make_step(1, "Bravo", StepStatus::Pending),
            make_step(2, "Charlie", StepStatus::Pending),
        ];
        let selection: Selection<String> = Selection::new();
        let buffer = render_to_buffer(&steps, &selection, Some(1), false, "demo", 40, 8);

        let row_y = find_row(&buffer, "Bravo").expect("Bravo row should be rendered");
        // Sample a column inside the title text — the highlight covers the
        // entire row width.
        let mut found_cursor_bg = false;
        for x in 0..buffer.area().width {
            let cell = &buffer[(x, row_y)];
            if cell.style().bg == Some(theme::CURSOR) {
                found_cursor_bg = true;
                break;
            }
        }
        assert!(
            found_cursor_bg,
            "expected at least one cell on the cursor row to use theme::CURSOR ({:?}) as bg",
            theme::CURSOR
        );
    }

    #[test]
    fn no_cursor_when_index_is_none() {
        // When cursor_index is None, no row should pick up the
        // theme::CURSOR background. Used for read-only preview panes.
        let steps = vec![
            make_step(0, "Alpha", StepStatus::Pending),
            make_step(1, "Bravo", StepStatus::Pending),
        ];
        let selection: Selection<String> = Selection::new();
        let buffer = render_to_buffer(&steps, &selection, None, false, "demo", 40, 8);
        for y in 0..buffer.area().height {
            for x in 0..buffer.area().width {
                assert_ne!(
                    buffer[(x, y)].style().bg,
                    Some(theme::CURSOR),
                    "no cell should carry theme::CURSOR bg when cursor_index is None"
                );
            }
        }
    }

    #[test]
    fn selection_badge_renders_for_selected_indices() {
        // Toggle s1 then s2 — they should render `[1]` and `[2]` badges
        // (1-based selection order) in the second and third rows.
        let steps = vec![
            make_step(0, "Alpha", StepStatus::Pending),
            make_step(1, "Bravo", StepStatus::Pending),
            make_step(2, "Charlie", StepStatus::Pending),
        ];
        let mut selection: Selection<String> = Selection::new();
        selection.toggle("s1".to_string());
        selection.toggle("s2".to_string());

        let buffer = render_to_buffer(&steps, &selection, Some(0), false, "demo", 60, 8);
        let rendered = buffer_text(&buffer);

        // Bravo row carries [1], Charlie carries [2], Alpha carries no badge.
        let bravo_y = find_row(&buffer, "Bravo").expect("Bravo row");
        let charlie_y = find_row(&buffer, "Charlie").expect("Charlie row");
        let alpha_y = find_row(&buffer, "Alpha").expect("Alpha row");

        let row_text = |y: u16| -> String {
            (0..buffer.area().width)
                .map(|x| buffer[(x, y)].symbol())
                .collect()
        };
        assert!(
            row_text(bravo_y).contains("[1]"),
            "Bravo row should carry [1]:\n{rendered}"
        );
        assert!(
            row_text(charlie_y).contains("[2]"),
            "Charlie row should carry [2]:\n{rendered}"
        );
        assert!(
            !row_text(alpha_y).contains("["),
            "Alpha row should not carry a badge:\n{rendered}"
        );

        // Verify the badge cells use theme::SELECTION as fg.
        let mut found_selection_fg = false;
        for x in 0..buffer.area().width {
            let cell = &buffer[(x, bravo_y)];
            if cell.symbol() == "1" && cell.style().fg == Some(theme::SELECTION) {
                found_selection_fg = true;
                break;
            }
        }
        assert!(
            found_selection_fg,
            "selection badge should render with theme::SELECTION fg color"
        );
    }

    #[test]
    fn status_glyphs_match_step_status_mapping() {
        // Confirm the row-prefix glyph for each StepStatus matches the
        // canonical mapping: pending=○, in_progress=▶, complete=✔,
        // failed=✘, skipped=⊘, aborted=⊘.
        let steps = vec![
            make_step(0, "P", StepStatus::Pending),
            make_step(1, "I", StepStatus::InProgress),
            make_step(2, "C", StepStatus::Complete),
            make_step(3, "F", StepStatus::Failed),
            make_step(4, "S", StepStatus::Skipped),
            make_step(5, "A", StepStatus::Aborted),
        ];
        let selection: Selection<String> = Selection::new();
        let buffer = render_to_buffer(&steps, &selection, Some(0), false, "demo", 30, 12);
        let rendered = buffer_text(&buffer);

        // Each glyph should appear at least once in the rendered output.
        // Aborted reuses the skipped glyph, so a single ⊘ in the output is
        // sufficient evidence that both statuses render correctly given
        // the dedicated unit assertions on `status_glyph` below.
        for glyph in ["○", "▶", "✔", "✘", "⊘"] {
            assert!(
                rendered.contains(glyph),
                "missing glyph {glyph}:\n{rendered}"
            );
        }

        // And spot-check the pure mapping function for completeness.
        assert_eq!(status_glyph(StepStatus::Pending), "○");
        assert_eq!(status_glyph(StepStatus::InProgress), "▶");
        assert_eq!(status_glyph(StepStatus::Complete), "✔");
        assert_eq!(status_glyph(StepStatus::Failed), "✘");
        assert_eq!(status_glyph(StepStatus::Skipped), "⊘");
        assert_eq!(status_glyph(StepStatus::Aborted), "⊘");
    }

    #[test]
    fn empty_steps_does_not_panic() {
        // Edge case: a plan with no steps should still render the chrome
        // block without panicking.
        let steps: Vec<Step> = vec![];
        let selection: Selection<String> = Selection::new();
        let buffer = render_to_buffer(&steps, &selection, None, false, "demo", 40, 6);
        let rendered = buffer_text(&buffer);
        assert!(rendered.contains("demo"), "title should still render");
    }
}
