// Plan list view (TUI-plan.md §5)
//
// Renders the "list of plan tiles" landing screen. Each tile is six rows tall
// and shows the plan slug, a "Ran" or "Created" timestamp, and a colored
// status dot followed by completed/total step counts. This module is the
// state + rendering surface for the read-only plan list; multi-select,
// archive, and create-plan flows land in later steps of the tui-v1 plan.

use std::path::Path;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Widget};

use crate::plan::{Plan, PlanStatus};
use crate::tui::chrome::{self, Chrome};
use crate::tui::theme;

/// Height of a single plan tile (including its top + bottom border rows).
/// Matches the layout sketch in TUI-plan.md §5.
pub const TILE_HEIGHT: u16 = 6;

// ---------------------------------------------------------------------------
// Per-tile data
// ---------------------------------------------------------------------------

/// A plan plus the precomputed counters and timestamp that the tile renders.
///
/// Built once per refresh from a `Plan` row + its step list + most recent
/// `execution_logs.started_at`. Keeping this separate from `Plan` lets
/// `PlanListApp` tests construct deterministic tiles without touching the DB.
#[derive(Debug, Clone)]
pub struct PlanTile {
    pub plan: Plan,
    pub completed: u32,
    pub total: u32,
    /// Most recent activity stamp. If `had_run` is true this is the
    /// `MAX(execution_logs.started_at)`; otherwise it's `plan.created_at`.
    pub last_activity: DateTime<Utc>,
    /// True if any `execution_logs` row exists for this plan. Drives the
    /// "Ran" vs. "Created" prefix on the timestamp line.
    pub had_run: bool,
}

// ---------------------------------------------------------------------------
// View state
// ---------------------------------------------------------------------------

/// Plan-list view state.
///
/// Independent of rendering and input handling so it can be unit-tested
/// without a terminal. Selection multi-select / [N] badges are not part of
/// step 12; that lands in tui-v1 step 14.
pub struct PlanListApp {
    /// Tiles in display order (most recent first; archived excluded).
    pub tiles: Vec<PlanTile>,
    /// Currently highlighted tile (0-based). 0 even when `tiles` is empty.
    pub selected_index: usize,
    /// Top of the visible tile window — first tile shown on screen. Bumped
    /// during navigation to keep `selected_index` on-screen.
    pub scroll_offset: usize,
    /// Whether the user has requested a quit.
    pub should_quit: bool,
    /// Whether the user has requested to open the highlighted tile (push
    /// the plan-detail view). The dispatcher consumes this and resets it.
    pub open_request: bool,
    /// IANA timezone used to format the per-tile timestamp. Sourced from
    /// `Config.display_timezone` and validated by `Config::validate` at load
    /// time, so an invalid string here is a programming error.
    pub display_timezone: String,
    /// The project root the TUI is operating against — drives the chrome
    /// `cwd` rendering and is exposed so the dispatcher can route `enter`
    /// events to plan-detail without re-resolving.
    pub project: String,
}

impl PlanListApp {
    /// Construct a new plan-list view with cursor on the first tile.
    pub fn new(tiles: Vec<PlanTile>, project: impl Into<String>, display_timezone: impl Into<String>) -> Self {
        Self {
            tiles,
            selected_index: 0,
            scroll_offset: 0,
            should_quit: false,
            open_request: false,
            display_timezone: display_timezone.into(),
            project: project.into(),
        }
    }

    // -- Navigation -------------------------------------------------------

    /// Move cursor down one tile, wrapping at the bottom.
    pub fn navigate_down(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = (self.selected_index + 1) % self.tiles.len();
    }

    /// Move cursor up one tile, wrapping at the top.
    pub fn navigate_up(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        if self.selected_index == 0 {
            self.selected_index = self.tiles.len() - 1;
        } else {
            self.selected_index -= 1;
        }
    }

    /// Jump to the first tile (`g`).
    pub fn jump_top(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = 0;
    }

    /// Jump to the last tile (`G`).
    pub fn jump_bottom(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = self.tiles.len() - 1;
    }

    // -- Routing ----------------------------------------------------------

    /// Signal the dispatcher to push plan-detail for the highlighted tile.
    ///
    /// Returns the selected plan slug if a tile is highlighted. Sets the
    /// `open_request` flag so frame loops can act on it once and reset.
    pub fn request_open(&mut self) -> Option<String> {
        if self.tiles.is_empty() {
            return None;
        }
        self.open_request = true;
        Some(self.tiles[self.selected_index].plan.slug.clone())
    }

    /// Signal that the user wants to quit.
    pub fn request_quit(&mut self) {
        self.should_quit = true;
    }
}

// ---------------------------------------------------------------------------
// Tile content formatting
// ---------------------------------------------------------------------------

/// Format a tile's timestamp line per the §5 spec.
///
/// Output shape: `"Ran Mon DD at HH:MM AM/PM"` if `tile.had_run`, else
/// `"Created Mon DD at HH:MM AM/PM"`. Rendered in `display_timezone`.
pub fn format_tile_timestamp(tile: &PlanTile, display_timezone: &str) -> String {
    let tz = chrono_tz::Tz::from_str(display_timezone).unwrap_or(chrono_tz::UTC);
    let local = tile.last_activity.with_timezone(&tz);
    // `%b %e` produces "May  4" with a leading space for single-digit days;
    // we want "May 4". Using `%-d` (Linux) / `%#d` (Windows) is non-portable,
    // so we trim manually after `%b %_d` (space-padded day).
    let raw = local.format("%b %e at %l:%M %p").to_string();
    let collapsed = collapse_spaces(&raw);
    let prefix = if tile.had_run { "Ran" } else { "Created" };
    format!("{prefix} {collapsed}")
}

/// Collapse runs of spaces into single spaces, matching the human-readable
/// form (`"Apr  4 at  3:05 PM"` → `"Apr 4 at 3:05 PM"`).
fn collapse_spaces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.chars() {
        if ch == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

/// Status-dot color for a plan tile.
///
/// Colors come from TUI-plan.md §5 status legend. `PlanStatus::Question` is a
/// derived state (§17): callers stamp it onto `tile.plan.status` whenever an
/// unanswered question row exists for the plan.
fn status_dot_color(status: PlanStatus) -> ratatui::style::Color {
    match status {
        PlanStatus::Complete => theme::STATUS_COMPLETE,
        PlanStatus::InProgress => theme::STATUS_IN_PROGRESS,
        PlanStatus::Planning | PlanStatus::Ready => theme::STATUS_PENDING,
        PlanStatus::Failed | PlanStatus::Aborted | PlanStatus::Archived => theme::STATUS_FAILED,
        PlanStatus::Question => theme::STATUS_QUESTION,
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the plan-list view, including persistent chrome.
pub fn draw(frame: &mut Frame, app: &mut PlanListApp) {
    let crumbs: [&str; 1] = ["ralph"];
    let hint = "[j/k] nav  [g/G] top/bottom  [enter] open  [q] quit";
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint,
            cwd: Path::new(&app.project),
        },
    );
    update_scroll(app, body.height);
    render_tiles(frame.buffer_mut(), body, app);
}

/// Render the tile column into the supplied buffer area.
///
/// Exposed so unit tests can render straight into a `Buffer` without
/// constructing a `Terminal` / `TestBackend`.
pub fn render_tiles(buf: &mut Buffer, area: Rect, app: &PlanListApp) {
    if app.tiles.is_empty() {
        let empty = Paragraph::new("No plans for this project. Press `i` to create one.");
        empty.render(area, buf);
        return;
    }
    let tile_h = TILE_HEIGHT;
    let visible = (area.height / tile_h) as usize;
    if visible == 0 {
        return;
    }
    let end = (app.scroll_offset + visible).min(app.tiles.len());
    for (slot, idx) in (app.scroll_offset..end).enumerate() {
        let tile = &app.tiles[idx];
        let tile_area = Rect {
            x: area.x,
            y: area.y + (slot as u16) * tile_h,
            width: area.width,
            height: tile_h,
        };
        render_tile(buf, tile_area, tile, idx == app.selected_index, &app.display_timezone);
    }
}

/// Adjust `scroll_offset` so the highlighted tile is visible.
fn update_scroll(app: &mut PlanListApp, body_height: u16) {
    if app.tiles.is_empty() {
        app.scroll_offset = 0;
        return;
    }
    let tile_h = TILE_HEIGHT as usize;
    let visible = (body_height as usize) / tile_h.max(1);
    if visible == 0 {
        app.scroll_offset = app.selected_index;
        return;
    }
    if app.selected_index < app.scroll_offset {
        app.scroll_offset = app.selected_index;
    } else if app.selected_index >= app.scroll_offset + visible {
        app.scroll_offset = app.selected_index + 1 - visible;
    }
}

fn render_tile(buf: &mut Buffer, area: Rect, tile: &PlanTile, highlighted: bool, tz: &str) {
    let border_style = if highlighted {
        Style::default().fg(theme::CURSOR)
    } else {
        Style::default().fg(theme::CHROME_DIM)
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style);

    let inner = block.inner(area);
    block.render(area, buf);

    if inner.height == 0 {
        return;
    }

    // Build the tile body — title / blank / timestamp / dot, packed into the
    // 4 inner rows of a 6-row tile (TUI-plan.md §5). Each row is rendered
    // independently so a clipped inner area (e.g. terminal too short)
    // gracefully drops the bottom rows instead of corrupting layout.
    let title = Paragraph::new(Line::from(Span::styled(
        truncate(tile.plan.slug.as_str(), inner.width as usize),
        Style::default().add_modifier(Modifier::BOLD),
    )));
    let title_area = Rect { height: 1, ..inner };
    title.render(title_area, buf);

    if inner.height >= 3 {
        let ts_area = Rect {
            y: inner.y + 2,
            height: 1,
            ..inner
        };
        let ts_text = format_tile_timestamp(tile, tz);
        let ts = Paragraph::new(truncate(&ts_text, inner.width as usize));
        ts.render(ts_area, buf);
    }

    if inner.height >= 4 {
        let dot_area = Rect {
            y: inner.y + 3,
            height: 1,
            ..inner
        };
        let dot_color = status_dot_color(tile.plan.status);
        let line = Line::from(vec![
            Span::styled("● ", Style::default().fg(dot_color)),
            Span::raw(format!("{}/{}", tile.completed, tile.total)),
        ]);
        let dot = Paragraph::new(line);
        dot.render(dot_area, buf);
    }
}

/// Right-truncate a string to fit `max_cols` display columns, appending `…`
/// when truncation occurs. Width is approximated by `char` count, which is
/// good enough for the slugs / timestamps the tile renders (no CJK / emoji
/// in those fields).
fn truncate(s: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }
    if s.chars().count() <= max_cols {
        return s.to_string();
    }
    if max_cols == 1 {
        return "…".to_string();
    }
    let take = max_cols - 1;
    let mut out: String = s.chars().take(take).collect();
    out.push('…');
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Plan, PlanStatus};
    use chrono::TimeZone;

    fn make_plan(slug: &str) -> Plan {
        Plan {
            id: format!("id-{slug}"),
            slug: slug.to_string(),
            project: "/proj".to_string(),
            branch_name: "b".to_string(),
            description: String::new(),
            status: PlanStatus::Ready,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
        }
    }

    fn make_tile(slug: &str) -> PlanTile {
        let plan = make_plan(slug);
        PlanTile {
            last_activity: plan.created_at,
            plan,
            completed: 0,
            total: 1,
            had_run: false,
        }
    }

    fn make_tiles(n: usize) -> Vec<PlanTile> {
        (0..n).map(|i| make_tile(&format!("plan-{i}"))).collect()
    }

    #[test]
    fn test_initial_cursor_is_zero() {
        let app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        assert_eq!(app.selected_index, 0);
        assert_eq!(app.scroll_offset, 0);
        assert!(!app.should_quit);
        assert!(!app.open_request);
    }

    #[test]
    fn test_navigate_down_advances() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.navigate_down();
        assert_eq!(app.selected_index, 1);
        app.navigate_down();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn test_navigate_down_wraps_to_top() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        app.navigate_down();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_navigate_up_goes_back() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        app.navigate_up();
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn test_navigate_up_wraps_to_bottom() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.navigate_up();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn test_jump_top() {
        let mut app = PlanListApp::new(make_tiles(5), "/proj", "UTC");
        app.selected_index = 4;
        app.jump_top();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_jump_bottom() {
        let mut app = PlanListApp::new(make_tiles(5), "/proj", "UTC");
        app.jump_bottom();
        assert_eq!(app.selected_index, 4);
    }

    #[test]
    fn test_navigate_empty_tiles_is_noop() {
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        app.navigate_down();
        app.navigate_up();
        app.jump_top();
        app.jump_bottom();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_request_open_returns_selected_slug() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        let slug = app.request_open();
        assert_eq!(slug.as_deref(), Some("plan-1"));
        assert!(app.open_request);
    }

    #[test]
    fn test_request_open_empty_returns_none() {
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        let slug = app.request_open();
        assert!(slug.is_none());
        assert!(!app.open_request);
    }

    #[test]
    fn test_request_quit() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        assert!(!app.should_quit);
        app.request_quit();
        assert!(app.should_quit);
    }

    // -- Timestamp formatting ------------------------------------------------

    #[test]
    fn test_format_tile_timestamp_created_in_utc() {
        let mut tile = make_tile("p");
        tile.last_activity = Utc.with_ymd_and_hms(2026, 5, 4, 14, 32, 0).unwrap();
        tile.had_run = false;
        let s = format_tile_timestamp(&tile, "UTC");
        // 2:32 PM in UTC, May 4
        assert_eq!(s, "Created May 4 at 2:32 PM");
    }

    #[test]
    fn test_format_tile_timestamp_ran_in_local_tz() {
        let mut tile = make_tile("p");
        // 2026-05-04 14:32 UTC = 10:32 AM EDT (May DST)
        tile.last_activity = Utc.with_ymd_and_hms(2026, 5, 4, 14, 32, 0).unwrap();
        tile.had_run = true;
        let s = format_tile_timestamp(&tile, "America/New_York");
        assert_eq!(s, "Ran May 4 at 10:32 AM");
    }

    #[test]
    fn test_format_tile_timestamp_two_digit_day() {
        let mut tile = make_tile("p");
        // Verify the day is rendered without the leading-space padding `%e`
        // would otherwise emit.
        tile.last_activity = Utc.with_ymd_and_hms(2026, 5, 12, 9, 0, 0).unwrap();
        tile.had_run = true;
        let s = format_tile_timestamp(&tile, "UTC");
        assert_eq!(s, "Ran May 12 at 9:00 AM");
    }

    #[test]
    fn test_format_tile_timestamp_invalid_tz_falls_back_to_utc() {
        // Defense-in-depth: an invalid IANA name should not panic. In
        // practice `Config::validate` rejects these at load time.
        let mut tile = make_tile("p");
        tile.last_activity = Utc.with_ymd_and_hms(2026, 5, 4, 0, 0, 0).unwrap();
        tile.had_run = true;
        let s = format_tile_timestamp(&tile, "Not/A_Real_Zone");
        assert_eq!(s, "Ran May 4 at 12:00 AM");
    }

    // -- Truncation ----------------------------------------------------------

    #[test]
    fn test_truncate_within_width() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_overflow() {
        assert_eq!(truncate("hello world", 8), "hello w…");
    }

    #[test]
    fn test_truncate_zero_width() {
        assert_eq!(truncate("anything", 0), "");
    }

    // -- Rendering smoke -----------------------------------------------------

    #[test]
    fn test_render_tile_writes_slug_and_status_dot() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        let area = buf.area;
        let mut tile = make_tile("my-plan");
        tile.completed = 3;
        tile.total = 7;
        tile.had_run = true;
        tile.last_activity = Utc.with_ymd_and_hms(2026, 5, 4, 14, 32, 0).unwrap();
        render_tile(&mut buf, area, &tile, true, "UTC");

        // Title row contains the slug.
        let row1 = (0..40)
            .map(|x| buf[(x, 1)].symbol())
            .collect::<String>();
        assert!(row1.contains("my-plan"), "got row1: {row1:?}");

        // Timestamp row contains "Ran May 4".
        let row3 = (0..40)
            .map(|x| buf[(x, 3)].symbol())
            .collect::<String>();
        assert!(row3.contains("Ran May 4"), "got row3: {row3:?}");

        // Status row contains the dot + counts. Inside a 6-row tile the
        // dot lives at y=4 (top border 0, title 1, blank 2, date 3, dot 4,
        // bottom border 5).
        let row4 = (0..40)
            .map(|x| buf[(x, 4)].symbol())
            .collect::<String>();
        assert!(row4.contains("●"), "got row4: {row4:?}");
        assert!(row4.contains("3/7"), "got row4: {row4:?}");
    }

    #[test]
    fn test_render_tiles_empty_writes_placeholder() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 60, 4));
        let area = buf.area;
        let app = PlanListApp::new(vec![], "/proj", "UTC");
        render_tiles(&mut buf, area, &app);
        let row0 = (0..60).map(|x| buf[(x, 0)].symbol()).collect::<String>();
        assert!(row0.contains("No plans"), "got row0: {row0:?}");
    }

    #[test]
    fn test_render_tiles_renders_only_visible_window() {
        // 12 rows / 6-row tiles = 2 tiles fit. With cursor on tile 4 the
        // viewport should slide so tile 4 is the last visible tile.
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 12));
        let area = buf.area;
        let mut app = PlanListApp::new(make_tiles(5), "/proj", "UTC");
        app.selected_index = 4;
        update_scroll(&mut app, 12);
        assert_eq!(app.scroll_offset, 3);
        render_tiles(&mut buf, area, &app);

        // The first visible tile's title row (y=1) should be plan-3.
        let row1 = (0..30).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(row1.contains("plan-3"), "got row1: {row1:?}");
        // The second visible tile's title row (y=7) should be plan-4.
        let row7 = (0..30).map(|x| buf[(x, 7)].symbol()).collect::<String>();
        assert!(row7.contains("plan-4"), "got row7: {row7:?}");
    }

    #[test]
    fn test_update_scroll_keeps_cursor_in_view() {
        let mut app = PlanListApp::new(make_tiles(10), "/proj", "UTC");
        // body 24 rows / 6 = 4 tiles fit
        app.selected_index = 0;
        update_scroll(&mut app, 24);
        assert_eq!(app.scroll_offset, 0);

        // Cursor below the visible window — offset should advance.
        app.selected_index = 6;
        update_scroll(&mut app, 24);
        assert_eq!(app.scroll_offset, 3);

        // Cursor jumps back above the offset — offset should retreat.
        app.selected_index = 1;
        update_scroll(&mut app, 24);
        assert_eq!(app.scroll_offset, 1);
    }

    #[test]
    fn test_status_dot_color_legend() {
        // Spot-check the §5 legend rather than re-listing every variant.
        assert_eq!(status_dot_color(PlanStatus::Complete), theme::STATUS_COMPLETE);
        assert_eq!(status_dot_color(PlanStatus::InProgress), theme::STATUS_IN_PROGRESS);
        assert_eq!(status_dot_color(PlanStatus::Ready), theme::STATUS_PENDING);
        assert_eq!(status_dot_color(PlanStatus::Planning), theme::STATUS_PENDING);
        assert_eq!(status_dot_color(PlanStatus::Failed), theme::STATUS_FAILED);
        assert_eq!(status_dot_color(PlanStatus::Aborted), theme::STATUS_FAILED);
        assert_eq!(status_dot_color(PlanStatus::Question), theme::STATUS_QUESTION);
    }
}
