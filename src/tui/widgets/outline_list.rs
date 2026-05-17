// Dependency-outline list widget (docs/dag-redesign.md §12.1 / §12.5).
//
// Renders the topologically-ordered, depth-indented step outline that
// replaces the flat positional step list in plan-detail. Purely
// presentational: the caller passes the already-projected
// [`OutlineRow`]s (from `OutlineState::visible_rows`), a multi-selection
// set, the cursor index, and a `ListState` for scroll persistence.
//
// Every status color routes through the single TUI-wide §12.5 mapping
// (`theme::step_status_color`), including the derived `Blocked` overlay
// (orange) and the `review_status` badge (Reviewing=blue, …), so the DAG
// glyph can never drift from the plan-list dot / chrome.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState};

use crate::plan::{ReviewStatus, StepStatus};
use crate::tui::selection::Selection;
use crate::tui::theme;
use crate::tui::views::outline_view::OutlineRow;

/// Single-glyph indicator for an effective [`StepStatus`] (the derived
/// `Blocked` overlay already folded in by `effective_step_status`).
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

/// Short badge token for a non-trivial `review_status`. `None` for
/// `Pending`/`Disabled` (no badge — not yet reviewed / review off is the
/// silent default). The badge color routes through the §12.5 mapping so
/// "Reviewing" is the same blue as the step glyph would be.
pub fn review_badge(rs: ReviewStatus) -> Option<(&'static str, Color)> {
    match rs {
        ReviewStatus::Pending | ReviewStatus::Disabled => None,
        ReviewStatus::InFlight => {
            Some(("review?", theme::step_status_color(StepStatus::InProgress)))
        }
        ReviewStatus::Passed => Some(("review✔", theme::step_status_color(StepStatus::Complete))),
        ReviewStatus::Failed => Some(("review✘", theme::step_status_color(StepStatus::Failed))),
        ReviewStatus::Skipped => Some(("review⊘", theme::CHROME_DIM)),
    }
}

/// Render the dependency outline into `area`.
///
/// - `rows`: the visible (possibly focus-filtered) projection.
/// - `selection`: keyed by step id for the `[N]` badge.
/// - `cursor_index`: `Some` highlights that row; `None` = read-only preview.
/// - `active_run`: layers REVERSED on the cursor row when a runner is bound.
/// - `title`: the bordered block title (plan slug, or `focus: <short_id>`).
#[allow(clippy::too_many_arguments)]
pub fn render(
    frame: &mut Frame,
    area: Rect,
    rows: &[OutlineRow],
    selection: &Selection<String>,
    cursor_index: Option<usize>,
    active_run: bool,
    title: &str,
    list_state: &mut ListState,
) {
    let items: Vec<ListItem> = rows
        .iter()
        .map(|row| {
            let indent = "  ".repeat(row.depth);
            let glyph = status_glyph(row.effective_status);
            let label = format!("{indent}{glyph} {} {}", row.short_id, row.title);
            let mut row_style = Style::default().fg(theme::step_status_color(row.effective_status));
            if matches!(row.effective_status, StepStatus::InProgress) {
                row_style = row_style.add_modifier(Modifier::BOLD);
            }
            let mut spans = vec![Span::styled(label, row_style)];

            // `↳ corrects <short_id>` marker for reviewer-inserted steps.
            if let Some(corrected) = &row.corrects_short_id {
                spans.push(Span::styled(
                    format!("  ↳ corrects {corrected}"),
                    Style::default().fg(theme::CHROME_DIM),
                ));
            }

            // Inline `deps: a1b2 c3d4` for a join (>1 dependency).
            if row.is_join() {
                spans.push(Span::styled(
                    format!("  deps: {}", row.join_deps.join(" ")),
                    Style::default().fg(theme::CHROME_DIM),
                ));
            }

            // Review badge (Reviewing=blue, …) — §12.1 / §12.5.
            if let Some((badge, color)) = review_badge(row.review_status) {
                spans.push(Span::raw("  "));
                spans.push(Span::styled(
                    badge,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ));
            }

            // Multi-select `[N]` badge.
            if let Some(n) = selection.index_of(&row.step_id) {
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
    use crate::plan::{Step, StepStatus};
    use crate::tui::views::outline_view::OutlineState;
    use chrono::Utc;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use std::collections::{HashMap, HashSet};

    fn step(short_id: &str, sort_key: &str) -> Step {
        Step {
            id: format!("uuid-{short_id}"),
            short_id: short_id.to_string(),
            plan_id: "p1".to_string(),
            sort_key: sort_key.to_string(),
            title: format!("title {short_id}"),
            description: String::new(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: Default::default(),
            tags: vec![],
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        }
    }

    fn edge(deps_of: &mut HashMap<String, Vec<String>>, s: &str, deps: &[&str]) {
        deps_of.insert(
            format!("uuid-{s}"),
            deps.iter().map(|d| format!("uuid-{d}")).collect(),
        );
    }

    fn render_to_string(rows: &[OutlineRow], title: &str) -> String {
        let backend = TestBackend::new(70, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut ls = ListState::default();
        let sel: Selection<String> = Selection::new();
        terminal
            .draw(|f| {
                let area = f.area();
                render(f, area, rows, &sel, Some(0), false, title, &mut ls);
            })
            .unwrap();
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
    fn renders_indent_glyph_and_join_deps() {
        let steps = vec![
            step("aaaa", "a0"),
            step("bbbb", "a1"),
            step("cccc", "a2"),
            step("dddd", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbb", &["aaaa"]);
        edge(&mut deps_of, "cccc", &["aaaa"]);
        edge(&mut deps_of, "dddd", &["bbbb", "cccc"]);
        let st = OutlineState::new(steps, deps_of, HashSet::new());
        let out = render_to_string(&st.visible_rows(), "my-plan");
        // Root short id and the join's inline deps list both appear.
        assert!(out.contains("aaaa"), "root short id missing:\n{out}");
        assert!(
            out.contains("deps: bbbb cccc"),
            "join deps list missing:\n{out}"
        );
    }

    #[test]
    fn renders_corrects_marker_and_review_badge() {
        let mut ap = step("apri", "a1");
        ap.corrects_step_id = Some("uuid-aaaa".to_string());
        ap.review_status = Some(ReviewStatus::Failed);
        let steps = vec![step("aaaa", "a0"), ap];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "apri", &["aaaa"]);
        let st = OutlineState::new(steps, deps_of, HashSet::new());
        let out = render_to_string(&st.visible_rows(), "p");
        assert!(
            out.contains("↳ corrects aaaa"),
            "corrects marker missing:\n{out}"
        );
        assert!(out.contains("review✘"), "review badge missing:\n{out}");
    }

    #[test]
    fn blocked_overlay_uses_orange_via_theme_helper() {
        let mut steps = vec![step("aaaa", "a0")];
        steps[0].status = StepStatus::InProgress;
        let blocked: HashSet<String> = ["uuid-aaaa".to_string()].into_iter().collect();
        let st = OutlineState::new(steps, HashMap::new(), blocked);
        let rows = st.visible_rows();
        assert_eq!(rows[0].effective_status, StepStatus::Blocked);

        let backend = TestBackend::new(40, 4);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut ls = ListState::default();
        let sel: Selection<String> = Selection::new();
        terminal
            .draw(|f| {
                let area = f.area();
                render(f, area, &rows, &sel, None, false, "p", &mut ls);
            })
            .unwrap();
        let buf = terminal.backend().buffer().clone();
        let orange = theme::step_status_color(StepStatus::Blocked);
        let found = (0..buf.area().height)
            .any(|y| (0..buf.area().width).any(|x| buf[(x, y)].style().fg == Some(orange)));
        assert!(
            found,
            "blocked row must use the §12.5 orange via theme helper"
        );
    }

    #[test]
    fn review_badge_pending_and_disabled_are_silent() {
        assert!(review_badge(ReviewStatus::Pending).is_none());
        assert!(review_badge(ReviewStatus::Disabled).is_none());
        assert!(review_badge(ReviewStatus::InFlight).is_some());
        assert!(review_badge(ReviewStatus::Passed).is_some());
        assert!(review_badge(ReviewStatus::Failed).is_some());
    }
}
