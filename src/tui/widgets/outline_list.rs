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

/// The presentational inputs to [`render`] besides the `frame` / `area` /
/// `list_state` rendering handles: the visible `rows`, the `[N]`-badge
/// `selection`, the optional `cursor_index`, the `active_run` flag, and the
/// bordered block `title`.
pub struct RenderOutline<'a> {
    pub rows: &'a [OutlineRow],
    pub selection: &'a Selection<String>,
    pub cursor_index: Option<usize>,
    pub active_run: bool,
    pub title: &'a str,
}

/// Render the dependency outline into `area`.
///
/// - `rows`: the visible (possibly focus-filtered) projection.
/// - `selection`: keyed by step id for the `[N]` badge.
/// - `cursor_index`: `Some` highlights that row; `None` = read-only preview.
/// - `active_run`: layers REVERSED on the cursor row when a runner is bound.
/// - `title`: the bordered block title (plan slug, or `focus: <short_id>`).
pub fn render(frame: &mut Frame, area: Rect, args: RenderOutline<'_>, list_state: &mut ListState) {
    let RenderOutline {
        rows,
        selection,
        cursor_index,
        active_run,
        title,
    } = args;
    let items: Vec<ListItem> = rows
        .iter()
        .map(|row| {
            let glyph = status_glyph(row.effective_status);
            // `tree_prefix` is the pre-built ├──/└──/│ ASCII tree art,
            // already aligned for indent — no per-render depth math here.
            let label = format!("{}{glyph} {} {}", row.tree_prefix, row.short_id, row.title);
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
                render(
                    f,
                    area,
                    RenderOutline {
                        rows,
                        selection: &sel,
                        cursor_index: Some(0),
                        active_run: false,
                        title,
                    },
                    &mut ls,
                );
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
    fn renders_tree_art_connectors_for_branching_subtree() {
        // Root A with two children (A.1, A.2). Independent root B with
        // children B.1 (which has B.1.1, B.1.2) and B.2 — the user's
        // sketched example. Tree art should:
        //   - leave roots at column 0 (no connector);
        //   - use ├── for non-last siblings, └── for the last;
        //   - draw a │ continuation column under a non-last ancestor
        //     (B.1's children sit under B's "│   ");
        //   - blank out the column under a last-child ancestor.
        // Independent roots therefore share no vertical stem.
        let steps = vec![
            step("aaaa", "a0"),
            step("a1aa", "a1"),
            step("a2aa", "a2"),
            step("bbbb", "b0"),
            step("b1aa", "b1"),
            step("b11a", "b2"),
            step("b12a", "b3"),
            step("b2aa", "b4"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "a1aa", &["aaaa"]);
        edge(&mut deps_of, "a2aa", &["aaaa"]);
        edge(&mut deps_of, "b1aa", &["bbbb"]);
        edge(&mut deps_of, "b11a", &["b1aa"]);
        edge(&mut deps_of, "b12a", &["b1aa"]);
        edge(&mut deps_of, "b2aa", &["bbbb"]);
        let st = OutlineState::new(steps, deps_of, HashSet::new());
        let rows = st.visible_rows();

        // Each row's prefix is computed once in visible_rows; assert the
        // exact shape so a regression in the helper is loud.
        let prefixes: Vec<&str> = rows.iter().map(|r| r.tree_prefix.as_str()).collect();
        assert_eq!(
            prefixes,
            vec![
                "",         // aaaa (root)
                "├── ",     // a1aa (first child of A)
                "└── ",     // a2aa (last child of A)
                "",         // bbbb (root B)
                "├── ",     // b1aa (first child of B, not last → │ continues)
                "│   ├── ", // b11a (first child of B.1, under B's │)
                "│   └── ", // b12a (last child of B.1, B's │ still continues)
                "└── ",     // b2aa (last child of B)
            ]
        );

        // And the assembled string really lands the connectors in front
        // of the glyph + short_id (canary against renderer drift).
        let out = render_to_string(&rows, "my-plan");
        assert!(
            out.contains("│   ├── ○ b11a"),
            "b11a should sit under B's continuation column:\n{out}"
        );
        assert!(
            out.contains("└── ○ b2aa"),
            "b2aa should render with the └── connector:\n{out}"
        );
    }

    #[test]
    fn renders_join_deps_with_tree_art() {
        // Diamond: a → {b, c}; d depends on b AND c. The visual parent of
        // d is c (the deepest in-set dep with the higher row index), so
        // tree art draws d under c (└── from c). The other parent (b) is
        // still surfaced via the inline `deps: …` annotation — tree art
        // never silently hides the second edge of a join.
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
        let rows = st.visible_rows();
        let out = render_to_string(&rows, "my-plan");
        assert!(out.contains("aaaa"), "root short id missing:\n{out}");
        assert!(
            out.contains("deps: bbbb cccc"),
            "join deps list missing:\n{out}"
        );

        let d = rows.iter().find(|r| r.short_id == "dddd").unwrap();
        assert_eq!(
            d.tree_prefix, "    └── ",
            "d should connect to its deepest dep (c) with a last-child elbow"
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
                render(
                    f,
                    area,
                    RenderOutline {
                        rows: &rows,
                        selection: &sel,
                        cursor_index: None,
                        active_run: false,
                        title: "p",
                    },
                    &mut ls,
                );
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
