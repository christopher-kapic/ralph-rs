// Archived plan list view (TUI-plan.md §6)
//
// Same layout and tile rendering as the main plan list, but the tile source
// set is `PlanStatus = Archived`. The keybindings are different: `enter`
// unarchives, `d` permanently deletes (with confirm), and `←`/`h`/`q` pops
// back to the main plan list.

use std::path::Path;
use std::time::Instant;

use crossterm::event::MouseEvent;
use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Clear, Paragraph, Widget};

use crate::plan::Plan;
use crate::tui::chrome::{self, Chrome};
use crate::tui::help::{self, HelpState};
use crate::tui::selection::Selection;
use crate::tui::toast::ToastQueue;
use crate::tui::views::plan_list::{self, PlanTile, TILE_HEIGHT};
use crate::tui::widgets::palette_bar::{self, PaletteBarState};

/// Archived-list view state. Mirrors `PlanListApp` minus the archived
/// sentinel — every tile here is itself an archived plan.
pub struct ArchivedListApp {
    /// Tiles in display order (most recent first).
    pub tiles: Vec<PlanTile>,
    /// Currently highlighted tile (0-based).
    pub selected_index: usize,
    /// Top of the visible tile window.
    pub scroll_offset: usize,
    /// Multi-selection keyed by `Plan.id` so selection survives a refresh.
    pub selection: Selection<String>,
    /// IANA timezone for the per-tile timestamp line.
    pub display_timezone: String,
    /// Project root, used by the chrome `cwd` rendering.
    pub project: String,
    /// Toast queue rendered over the bottom chrome row.
    pub toasts: ToastQueue,
    /// Set when the user asks to return to the main plan list (`←`/`h`/`q`).
    pub should_pop: bool,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// touching any view bindings (TUI-plan.md §15).
    pub help: HelpState,
    /// Slash/colon command palette state (TUI-plan.md §9). `Some` while the
    /// bar is open; the dispatcher routes every key through
    /// [`PaletteBarState::on_key`] before any view bindings fire.
    pub palette_bar: Option<PaletteBarState>,
}

impl ArchivedListApp {
    /// Build a new archived-list view with cursor on the first tile.
    pub fn new(
        tiles: Vec<PlanTile>,
        project: impl Into<String>,
        display_timezone: impl Into<String>,
    ) -> Self {
        Self {
            tiles,
            selected_index: 0,
            scroll_offset: 0,
            selection: Selection::new(),
            display_timezone: display_timezone.into(),
            project: project.into(),
            toasts: ToastQueue::new(),
            should_pop: false,
            help: HelpState::new(),
            palette_bar: None,
        }
    }

    /// Open the palette with `prefix` as the trigger key (`/` or `:`).
    /// TUI-plan.md §9.
    pub fn open_palette(&mut self, prefix: char) {
        self.palette_bar = Some(PaletteBarState::new(prefix));
    }

    /// Close the palette without dispatching. TUI-plan.md §9.
    pub fn close_palette(&mut self) {
        self.palette_bar = None;
    }

    /// Whether the palette bar is currently open and consuming keys.
    pub fn palette_active(&self) -> bool {
        self.palette_bar.is_some()
    }

    /// Mouse-event entry point routed from the dispatcher's event loop.
    /// No-op by default — see [`super::plan_list::PlanListApp::handle_mouse`]
    /// for the rationale. Per-view drag handling is added in later steps.
    pub fn handle_mouse(&mut self, _event: MouseEvent) {}

    // -- Navigation -------------------------------------------------------

    pub fn navigate_down(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = (self.selected_index + 1) % self.tiles.len();
    }

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

    pub fn jump_top(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = 0;
    }

    pub fn jump_bottom(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        self.selected_index = self.tiles.len() - 1;
    }

    // -- Selection --------------------------------------------------------

    /// Toggle multi-select on the currently highlighted tile (`space`).
    pub fn toggle_selection(&mut self) {
        if self.tiles.is_empty() {
            return;
        }
        let id = self.tiles[self.selected_index].plan.id.clone();
        self.selection.toggle(id);
    }

    pub fn clear_selection(&mut self) {
        self.selection.clear();
    }

    /// `<esc>`: clear an active selection or pop back to the plan list when
    /// nothing is selected. Returns true if a selection was consumed.
    pub fn escape(&mut self) -> bool {
        if self.selection.is_empty() {
            self.should_pop = true;
            false
        } else {
            self.selection.clear();
            true
        }
    }

    // -- Routing ----------------------------------------------------------

    /// Pop back to the main plan list (`←`/`h`/`q`).
    pub fn request_pop(&mut self) {
        self.should_pop = true;
    }

    // -- Cursor target ----------------------------------------------------

    /// The plan currently under the cursor, if any.
    pub fn cursor_plan(&self) -> Option<&Plan> {
        self.tiles.get(self.selected_index).map(|t| &t.plan)
    }

    /// Plan IDs the next destructive / unarchive action targets:
    /// selection wins over the cursor; empty when there are no tiles and no
    /// selection.
    pub fn action_targets(&self) -> Vec<String> {
        if !self.selection.is_empty() {
            self.selection.as_slice().to_vec()
        } else if !self.tiles.is_empty() {
            vec![self.tiles[self.selected_index].plan.id.clone()]
        } else {
            Vec::new()
        }
    }

    // -- Refresh ----------------------------------------------------------

    /// Replace the tile list (after a DB mutation: unarchive or delete) and
    /// reset transient state. Selection is cleared and the cursor is clamped
    /// to the new range.
    pub fn refresh_tiles(&mut self, tiles: Vec<PlanTile>) {
        self.tiles = tiles;
        self.selection.clear();
        if self.tiles.is_empty() {
            self.selected_index = 0;
            self.scroll_offset = 0;
        } else if self.selected_index >= self.tiles.len() {
            self.selected_index = self.tiles.len() - 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the archived-list view, including persistent chrome.
pub fn draw(frame: &mut Frame, app: &mut ArchivedListApp) {
    app.toasts.prune(Instant::now());

    let crumbs: [&str; 2] = ["ralph", "Archived plans"];
    let normal_hint =
        "[j/k] nav  [enter] unarchive  [space] select  [d] delete  [/:] cmd  [h/←/q] back";
    let palette_hint = "[tab] complete  [enter] submit  [esc] cancel";
    let hint = if app.palette_active() {
        palette_hint
    } else {
        normal_hint
    };
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint,
            cwd: Path::new(&app.project),
            banner: None,
            running_indicator: None,
        },
    );
    update_scroll(app, body.height);
    render_tiles(frame.buffer_mut(), body, app);

    if let Some(toast) = app.toasts.current() {
        let area = frame.area();
        if area.height >= 1 && area.width > 0 {
            render_toast_overlay(frame, area, &toast.text, toast.color);
        }
    }

    // Help overlay sits on top of everything else when `?` has been pressed.
    if app.help.is_visible() {
        let area = frame.area();
        help::render(frame, area, &help::for_archived_list());
    }

    // Palette bar overlays the bottom chrome row when active. TUI-plan.md §9.
    if let Some(state) = app.palette_bar.as_ref() {
        let area = frame.area();
        let strip_height = 4.min(area.height);
        if strip_height > 0 {
            let palette_area = Rect {
                x: area.x,
                y: area.y + area.height - strip_height,
                width: area.width,
                height: strip_height,
            };
            palette_bar::render(frame, palette_area, state);
        }
    }
}

fn render_toast_overlay(frame: &mut Frame, area: Rect, text: &str, color: ratatui::style::Color) {
    let max_toast = area.width.saturating_sub(1).max(1);
    let desired = text.chars().count().min(max_toast as usize) as u16;
    if desired == 0 {
        return;
    }
    let toast_area = Rect {
        x: area.x,
        y: area.y + area.height - 1,
        width: desired,
        height: 1,
    };
    frame.render_widget(Clear, toast_area);
    let para = Paragraph::new(Span::styled(
        plan_list::truncate(text, toast_area.width as usize),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(para, toast_area);
}

/// Render the tile column. Reuses [`plan_list::render_tile`] verbatim — the
/// archived view shares the §5 plan-tile body.
pub fn render_tiles(buf: &mut Buffer, area: Rect, app: &ArchivedListApp) {
    if app.tiles.is_empty() {
        let empty = Paragraph::new("No archived plans for this project.");
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
        let highlighted = idx == app.selected_index;
        let badge = app.selection.index_of(&tile.plan.id);
        plan_list::render_tile(
            buf,
            tile_area,
            tile,
            highlighted,
            badge,
            &app.display_timezone,
        );
    }
}

fn update_scroll(app: &mut ArchivedListApp, body_height: u16) {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Plan, PlanStatus};
    use chrono::{TimeZone, Utc};

    fn make_plan(slug: &str) -> Plan {
        Plan {
            id: format!("id-{slug}"),
            slug: slug.to_string(),
            project: "/proj".to_string(),
            branch_name: "b".to_string(),
            description: String::new(),
            status: PlanStatus::Archived,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
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
            unanswered_questions: 0,
            oldest_question: None,
        }
    }

    fn make_tiles(n: usize) -> Vec<PlanTile> {
        (0..n).map(|i| make_tile(&format!("arch-{i}"))).collect()
    }

    // -- Navigation ---------------------------------------------------------

    #[test]
    fn navigate_down_advances_and_wraps() {
        let mut app = ArchivedListApp::new(make_tiles(3), "/proj", "UTC");
        app.navigate_down();
        assert_eq!(app.selected_index, 1);
        app.navigate_down();
        assert_eq!(app.selected_index, 2);
        app.navigate_down();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn navigate_up_wraps_to_bottom() {
        let mut app = ArchivedListApp::new(make_tiles(3), "/proj", "UTC");
        app.navigate_up();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn jump_top_and_bottom() {
        let mut app = ArchivedListApp::new(make_tiles(4), "/proj", "UTC");
        app.jump_bottom();
        assert_eq!(app.selected_index, 3);
        app.jump_top();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn navigation_on_empty_is_noop() {
        let mut app = ArchivedListApp::new(vec![], "/proj", "UTC");
        app.navigate_down();
        app.navigate_up();
        app.jump_top();
        app.jump_bottom();
        assert_eq!(app.selected_index, 0);
    }

    // -- Selection ----------------------------------------------------------

    #[test]
    fn toggle_selection_uses_plan_id() {
        let mut app = ArchivedListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        app.toggle_selection();
        assert!(app.selection.is_selected(&"id-arch-1".to_string()));
        app.toggle_selection();
        assert!(!app.selection.is_selected(&"id-arch-1".to_string()));
    }

    #[test]
    fn toggle_selection_empty_is_noop() {
        let mut app = ArchivedListApp::new(vec![], "/proj", "UTC");
        app.toggle_selection();
        assert!(app.selection.is_empty());
    }

    // -- Targets ------------------------------------------------------------

    #[test]
    fn action_targets_returns_cursor_when_no_selection() {
        let mut app = ArchivedListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        assert_eq!(app.action_targets(), vec!["id-arch-1".to_string()]);
    }

    #[test]
    fn action_targets_returns_selection_in_pick_order() {
        let mut app = ArchivedListApp::new(make_tiles(4), "/proj", "UTC");
        app.selected_index = 2;
        app.toggle_selection();
        app.selected_index = 0;
        app.toggle_selection();
        app.selected_index = 3;
        app.toggle_selection();
        assert_eq!(
            app.action_targets(),
            vec![
                "id-arch-2".to_string(),
                "id-arch-0".to_string(),
                "id-arch-3".to_string(),
            ]
        );
    }

    #[test]
    fn action_targets_empty_when_no_tiles_and_no_selection() {
        let app = ArchivedListApp::new(vec![], "/proj", "UTC");
        assert!(app.action_targets().is_empty());
    }

    // -- Refresh ------------------------------------------------------------

    #[test]
    fn refresh_tiles_clears_selection_and_clamps_cursor() {
        let mut app = ArchivedListApp::new(make_tiles(4), "/proj", "UTC");
        app.toggle_selection();
        app.selected_index = 3;
        app.refresh_tiles(make_tiles(2));
        assert!(app.selection.is_empty());
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn refresh_tiles_resets_when_list_empties() {
        let mut app = ArchivedListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        app.refresh_tiles(vec![]);
        assert_eq!(app.selected_index, 0);
        assert_eq!(app.scroll_offset, 0);
    }

    // -- Escape / pop -------------------------------------------------------

    #[test]
    fn escape_with_no_selection_pops() {
        let mut app = ArchivedListApp::new(make_tiles(2), "/proj", "UTC");
        let consumed = app.escape();
        assert!(!consumed);
        assert!(app.should_pop);
    }

    #[test]
    fn escape_with_selection_clears_only() {
        let mut app = ArchivedListApp::new(make_tiles(2), "/proj", "UTC");
        app.toggle_selection();
        let consumed = app.escape();
        assert!(consumed);
        assert!(!app.should_pop);
        assert!(app.selection.is_empty());
    }

    #[test]
    fn request_pop_sets_flag() {
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        app.request_pop();
        assert!(app.should_pop);
    }

    // -- Rendering ----------------------------------------------------------

    #[test]
    fn render_tiles_renders_archived_plans() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 12));
        let area = buf.area;
        let app = ArchivedListApp::new(make_tiles(2), "/proj", "UTC");
        render_tiles(&mut buf, area, &app);
        // First tile's title row.
        let row = (0..30).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(
            row.contains("arch-0"),
            "expected first archived plan slug: {row:?}"
        );
    }

    #[test]
    fn render_tiles_empty_writes_placeholder() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 60, 4));
        let area = buf.area;
        let app = ArchivedListApp::new(vec![], "/proj", "UTC");
        render_tiles(&mut buf, area, &app);
        let row = (0..60).map(|x| buf[(x, 0)].symbol()).collect::<String>();
        assert!(row.contains("No archived"), "got row0: {row:?}");
    }

    #[test]
    fn draw_renders_breadcrumb_with_archived_header() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let top = (0..buffer.area().width)
            .map(|x| buffer[(x, 0)].symbol())
            .collect::<String>();
        assert!(
            top.contains("Archived plans"),
            "expected breadcrumb header: {top:?}"
        );
    }

    // -- Help overlay (TUI-plan.md §15) ---------------------------------

    #[test]
    fn help_state_default_hidden() {
        let app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        assert!(!app.help.is_visible());
    }

    #[test]
    fn help_intercepts_question_mark_and_esc() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        let q = KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE);
        assert_eq!(
            app.help.intercept_key(q),
            crate::tui::help::InterceptResult::Opened
        );
        assert!(app.help.is_visible());

        let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        assert_eq!(
            app.help.intercept_key(esc),
            crate::tui::help::InterceptResult::Closed
        );
        assert!(!app.help.is_visible());
    }

    // -- Palette (TUI-plan.md §9) ---------------------------------------

    #[test]
    fn palette_default_inactive() {
        let app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        assert!(!app.palette_active());
        assert!(app.palette_bar.is_none());
    }

    #[test]
    fn palette_open_records_prefix() {
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        assert!(app.palette_active());
        assert_eq!(app.palette_bar.as_ref().unwrap().prefix, '/');
        app.close_palette();
        app.open_palette(':');
        assert_eq!(app.palette_bar.as_ref().unwrap().prefix, ':');
    }

    #[test]
    fn palette_close_drops_state() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        let _ = app
            .palette_bar
            .as_mut()
            .unwrap()
            .on_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));
        assert_eq!(app.palette_bar.as_ref().unwrap().input, "r");
        app.close_palette();
        assert!(!app.palette_active());
    }

    #[test]
    fn palette_esc_yields_cancel_outcome() {
        use crate::tui::widgets::palette_bar::PaletteBarOutcome;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        let out = app
            .palette_bar
            .as_mut()
            .unwrap()
            .on_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(out, PaletteBarOutcome::Cancel);
    }

    #[test]
    fn palette_enter_yields_submit_outcome_and_parses() {
        use crate::tui::palette::PaletteCommand;
        use crate::tui::widgets::palette_bar::PaletteBarOutcome;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = ArchivedListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        let bar = app.palette_bar.as_mut().unwrap();
        for c in "plan unarchive foo".chars() {
            let _ = bar.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        let out = bar.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let input = match out {
            PaletteBarOutcome::Submit(s) => s,
            other => panic!("expected Submit, got {other:?}"),
        };
        assert_eq!(
            crate::tui::palette::parse(&input),
            Ok(PaletteCommand::PlanUnarchive("foo".to_string()))
        );
    }
}
