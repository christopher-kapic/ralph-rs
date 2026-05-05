// Plan list view (TUI-plan.md §5)
//
// Renders the "list of plan tiles" landing screen. Each tile is six rows tall
// and shows the plan slug, a "Ran" or "Created" timestamp, and a colored
// status dot followed by completed/total step counts. This module is the
// state + rendering surface for the read-only plan list; multi-select,
// archive, and create-plan flows land in later steps of the tui-v1 plan.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

use chrono::{DateTime, Utc};
use crossterm::event::MouseEvent;
use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, ListState, Paragraph, Widget};

use crate::plan::{Plan, PlanStatus, Step};
use crate::tui::chrome::{self, Chrome};
use crate::tui::help::{self, HelpState};
use crate::tui::read_only::{self, ReadOnly};
use crate::tui::selection::Selection;
use crate::tui::theme;
use crate::tui::toast::ToastQueue;
use crate::tui::widgets::palette_bar::{self, PaletteBarState};
use crate::tui::widgets::step_list;

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
    /// Number of unanswered `step_questions` rows across this plan's steps.
    /// When non-zero the tile draws the purple `STATUS_QUESTION` dot
    /// (overriding the underlying plan status) and a teaser line per
    /// TUI-plan.md §17.
    pub unanswered_questions: u32,
    /// Verbatim text of the oldest unanswered question for this plan,
    /// truncated by the renderer to the tile width. Only populated when
    /// `unanswered_questions > 0`.
    pub oldest_question: Option<String>,
}

// ---------------------------------------------------------------------------
// View state
// ---------------------------------------------------------------------------

/// What the user requested to open with `enter` / `→` / `l`. Either a plan
/// (push plan-detail) or the archived plan list (push `View::ArchivedList`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenRequest {
    Plan(String),
    Archived,
}

/// Plan-list view state.
///
/// Independent of rendering and input handling so it can be unit-tested
/// without a terminal.
pub struct PlanListApp {
    /// Tiles in display order (most recent first; archived excluded).
    pub tiles: Vec<PlanTile>,
    /// Number of archived plans for this project. When non-zero an extra
    /// "Archived (N)" tile is rendered after the regular tiles and is also
    /// reachable by the cursor (TUI-plan.md §5).
    pub archived_count: u32,
    /// Currently highlighted tile (0-based). 0 even when `tiles` is empty.
    /// Indexes the combined navigable list: regular tiles first, then the
    /// archived sentinel slot when [`Self::archived_tile_visible`].
    pub selected_index: usize,
    /// Top of the visible tile window — first tile shown on screen. Bumped
    /// during navigation to keep `selected_index` on-screen.
    pub scroll_offset: usize,
    /// Multi-selection state, keyed by `Plan.id` so the selection survives a
    /// refresh that re-orders the tile list. See `Selection` for ordering
    /// semantics that drive the `[N]` badge.
    pub selection: Selection<String>,
    /// Whether the user has requested a quit.
    pub should_quit: bool,
    /// Whether the user has requested to open the highlighted tile (push
    /// the plan-detail view, or the archived list when the cursor is on the
    /// archived sentinel). The dispatcher consumes this and resets it.
    pub open_request: Option<OpenRequest>,
    /// IANA timezone used to format the per-tile timestamp. Sourced from
    /// `Config.display_timezone` and validated by `Config::validate` at load
    /// time, so an invalid string here is a programming error.
    pub display_timezone: String,
    /// The project root the TUI is operating against — drives the chrome
    /// `cwd` rendering and is exposed so the dispatcher can route `enter`
    /// events to plan-detail without re-resolving.
    pub project: String,
    /// Toast queue rendered over the bottom chrome row. The dispatcher pushes
    /// onto this after destructive operations (`d` archive) so the user sees
    /// a transient confirmation.
    pub toasts: ToastQueue,
    /// Read-only attach state (TUI-plan.md §13.2). When `Locked`, the edit
    /// keybindings (`i`/`a`/`A`/`d`/`Q`) are suppressed and the persistent
    /// banner replaces the bottom hint line.
    pub read_only: ReadOnly,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// touching any view bindings (TUI-plan.md §15).
    pub help: HelpState,
    /// Per-plan cache of step lists for the right-pane preview
    /// (TUI-plan.md §5). Populated lazily by the dispatcher when the
    /// cursor moves to a plan whose steps haven't been fetched yet, and
    /// cleared by [`Self::refresh_tiles`] so a refresh re-loads counts.
    pub step_preview_cache: HashMap<String, Vec<Step>>,
    /// Always-empty selection passed to `step_list::render` for the
    /// read-only preview pane — the widget API requires one even though the
    /// preview never participates in multi-select.
    pub preview_selection: Selection<String>,
    /// Persistent `ListState` for the right preview pane so its scroll
    /// offset survives across frames. Reset to default whenever the
    /// highlighted plan changes (see [`Self::preview_keyed_plan`]).
    pub preview_list_state: ListState,
    /// The plan_id whose steps the preview pane is currently keyed to.
    /// `draw` compares this to the cursor target each frame and resets
    /// `preview_list_state` when they differ, so a freshly-shown plan
    /// always starts at the top of its step list.
    pub preview_keyed_plan: Option<String>,
    /// Slash/colon command palette state (TUI-plan.md §9). `Some` while the
    /// bar is open; the dispatcher routes every key through
    /// [`PaletteBarState::on_key`] before any view bindings fire. `/` and
    /// `:` open it.
    pub palette_bar: Option<PaletteBarState>,

    /// Horizontal split between the tile column (left) and the step-list
    /// preview pane (right) as a percent of the body width given to the
    /// left pane. Default 40, clamped to 20..=80 by the mouse-drag handler
    /// so neither pane can collapse. Session-only — never persisted.
    pub split_pct: u16,

    /// Body width recorded during the most recent `draw()`. Used by
    /// [`Self::handle_mouse`] to convert the cursor's absolute column into
    /// a percent of the body's horizontal extent. Zero before the first
    /// frame; mouse handling no-ops while it's zero.
    pub last_body_width: u16,

    /// True while a left-mouse drag started on the divider column is
    /// active. Cleared on `MouseEventKind::Up(Left)`.
    pub dragging_split: bool,
}

impl PlanListApp {
    /// Construct a new plan-list view with cursor on the first tile and no
    /// archived tile visible. Use [`Self::with_archived_count`] to attach the
    /// archived plan count if you have it at construction time.
    pub fn new(
        tiles: Vec<PlanTile>,
        project: impl Into<String>,
        display_timezone: impl Into<String>,
    ) -> Self {
        Self {
            tiles,
            archived_count: 0,
            selected_index: 0,
            scroll_offset: 0,
            selection: Selection::new(),
            should_quit: false,
            open_request: None,
            display_timezone: display_timezone.into(),
            project: project.into(),
            toasts: ToastQueue::new(),
            read_only: ReadOnly::Editable,
            help: HelpState::new(),
            step_preview_cache: HashMap::new(),
            preview_selection: Selection::new(),
            preview_list_state: ListState::default(),
            preview_keyed_plan: None,
            palette_bar: None,
            split_pct: 40,
            last_body_width: 0,
            dragging_split: false,
        }
    }

    /// Open the palette with `prefix` as the trigger key (`/` or `:`). Resets
    /// any in-flight buffer. TUI-plan.md §9.
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
    /// Implements draggable resize of the horizontal split between the tile
    /// column and the step-list preview pane: a left-button press within ±1
    /// column of the current divider arms a drag, subsequent drags recompute
    /// `split_pct` from `cursor_column / last_body_width` (clamped 20..=80),
    /// and release clears the drag flag.
    pub fn handle_mouse(&mut self, event: MouseEvent) {
        use crossterm::event::{MouseButton, MouseEventKind};

        if self.last_body_width == 0 {
            return;
        }
        let body_width = self.last_body_width as u32;
        let divider_col = (body_width * self.split_pct as u32 / 100) as i32;

        match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let col = event.column as i32;
                if (col - divider_col).abs() <= 1 {
                    self.dragging_split = true;
                }
            }
            MouseEventKind::Drag(MouseButton::Left) if self.dragging_split => {
                let pct = (event.column as u32 * 100) / body_width.max(1);
                self.split_pct = pct.clamp(20, 80) as u16;
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.dragging_split = false;
            }
            _ => {}
        }
    }

    /// Update the read-only state (called by the dispatcher each
    /// poll tick). Storing this on the App keeps draw + input handling
    /// thin: both consult the same source of truth without an extra
    /// argument. TUI-plan.md §13.2.
    pub fn set_read_only(&mut self, state: ReadOnly) {
        self.read_only = state;
    }

    /// Builder-style setter for the archived plan count. Drives the optional
    /// "Archived (N)" sentinel tile.
    pub fn with_archived_count(mut self, count: u32) -> Self {
        self.archived_count = count;
        self
    }

    // -- Archived sentinel ------------------------------------------------

    /// True when the archived sentinel tile should be rendered + reachable.
    pub fn archived_tile_visible(&self) -> bool {
        self.archived_count > 0
    }

    /// Total number of cursor-reachable items: regular tiles plus the
    /// archived sentinel when visible.
    pub fn navigable_count(&self) -> usize {
        self.tiles.len() + if self.archived_tile_visible() { 1 } else { 0 }
    }

    /// True when the cursor is currently on the archived sentinel tile.
    pub fn is_archived_cursor(&self) -> bool {
        self.archived_tile_visible() && self.selected_index >= self.tiles.len()
    }

    // -- Navigation -------------------------------------------------------

    /// Move cursor down one tile, wrapping at the bottom.
    pub fn navigate_down(&mut self) {
        let n = self.navigable_count();
        if n == 0 {
            return;
        }
        self.selected_index = (self.selected_index + 1) % n;
    }

    /// Move cursor up one tile, wrapping at the top.
    pub fn navigate_up(&mut self) {
        let n = self.navigable_count();
        if n == 0 {
            return;
        }
        if self.selected_index == 0 {
            self.selected_index = n - 1;
        } else {
            self.selected_index -= 1;
        }
    }

    /// Jump to the first tile (`g`).
    pub fn jump_top(&mut self) {
        if self.navigable_count() == 0 {
            return;
        }
        self.selected_index = 0;
    }

    /// Jump to the last tile (`G`).
    pub fn jump_bottom(&mut self) {
        let n = self.navigable_count();
        if n == 0 {
            return;
        }
        self.selected_index = n - 1;
    }

    // -- Selection --------------------------------------------------------

    /// Toggle multi-select on the currently highlighted tile (`space`).
    /// No-op when the tile list is empty or the cursor is on the archived
    /// sentinel — the sentinel can't participate in multi-select.
    pub fn toggle_selection(&mut self) {
        if self.tiles.is_empty() || self.is_archived_cursor() {
            return;
        }
        let id = self.tiles[self.selected_index].plan.id.clone();
        self.selection.toggle(id);
    }

    /// Clear all selections without touching cursor or quit state.
    pub fn clear_selection(&mut self) {
        self.selection.clear();
    }

    /// Handle `<esc>` per TUI-plan.md §5: clear the selection if any items
    /// are selected, otherwise quit. Returns `true` when the press
    /// consumed a non-empty selection (so the caller can suppress quit).
    pub fn escape(&mut self) -> bool {
        if self.selection.is_empty() {
            self.should_quit = true;
            false
        } else {
            self.selection.clear();
            true
        }
    }

    // -- Routing ----------------------------------------------------------

    /// Signal the dispatcher to push plan-detail for the highlighted tile, or
    /// the archived list when the archived sentinel is selected.
    ///
    /// Returns the [`OpenRequest`] when something is highlighted. Sets
    /// `open_request` so frame loops can act on it once and reset.
    pub fn request_open(&mut self) -> Option<OpenRequest> {
        if self.is_archived_cursor() {
            let req = OpenRequest::Archived;
            self.open_request = Some(req.clone());
            return Some(req);
        }
        if let Some(tile) = self.tiles.get(self.selected_index) {
            let req = OpenRequest::Plan(tile.plan.slug.clone());
            self.open_request = Some(req.clone());
            return Some(req);
        }
        None
    }

    /// Signal that the user wants to quit.
    pub fn request_quit(&mut self) {
        self.should_quit = true;
    }

    // -- Cursor target ----------------------------------------------------

    /// The plan currently under the cursor, if any. Used by single-target
    /// keybindings (`A` approve, `Q` toggle questions) that act only on the
    /// highlighted tile and ignore multi-selection. Returns `None` when the
    /// cursor is on the archived sentinel.
    pub fn cursor_plan(&self) -> Option<&Plan> {
        if self.is_archived_cursor() {
            return None;
        }
        self.tiles.get(self.selected_index).map(|t| &t.plan)
    }

    // -- Archive ----------------------------------------------------------

    /// Plan IDs that the next `d` archive should affect, per TUI-plan.md §5:
    /// selection wins over the cursor target. With at least one tile selected,
    /// returns the selection in pick order; otherwise returns just the
    /// highlighted tile's plan id. Empty if there are no tiles, nothing
    /// selected, or the cursor is parked on the archived sentinel (which has
    /// no underlying plan to archive).
    pub fn archive_targets(&self) -> Vec<String> {
        if !self.selection.is_empty() {
            self.selection.as_slice().to_vec()
        } else if !self.tiles.is_empty() && !self.is_archived_cursor() {
            vec![self.tiles[self.selected_index].plan.id.clone()]
        } else {
            Vec::new()
        }
    }

    /// Replace one plan's row in-place after a single-plan mutation (e.g. `A`
    /// approve, `Q` questions toggle). Unlike [`Self::refresh_tiles`], this
    /// preserves selection, cursor, and scroll — appropriate for cursor-only
    /// actions that do not consume the selection. No-op if `updated.id` is
    /// not currently in the tile list.
    pub fn update_plan_in_place(&mut self, updated: Plan) {
        if let Some(tile) = self.tiles.iter_mut().find(|t| t.plan.id == updated.id) {
            tile.plan = updated;
        }
    }

    /// Replace the tile list and archived count (after a DB refresh) and
    /// reset transient state: selection is cleared and the cursor is clamped
    /// into the new navigable range. `scroll_offset` is left to be
    /// re-computed on the next draw via `update_scroll`.
    pub fn refresh_tiles(&mut self, tiles: Vec<PlanTile>, archived_count: u32) {
        self.tiles = tiles;
        self.archived_count = archived_count;
        self.selection.clear();
        // Drop the preview cache: step counts may have changed underneath,
        // and the keyed-plan check would otherwise keep stale step rows on
        // screen even after the underlying tile re-rendered fresh totals.
        self.step_preview_cache.clear();
        self.preview_keyed_plan = None;
        self.preview_list_state = ListState::default();
        let nav = self.navigable_count();
        if nav == 0 {
            self.selected_index = 0;
            self.scroll_offset = 0;
        } else if self.selected_index >= nav {
            self.selected_index = nav - 1;
        }
    }

    // -- Preview cache (right pane, TUI-plan.md §5) -----------------------

    /// `plan_id` currently under the cursor, or `None` when the cursor is
    /// on the archived sentinel or the tile list is empty. Used by the
    /// dispatcher to decide which plan's steps to fetch for the preview.
    pub fn highlighted_plan_id(&self) -> Option<&str> {
        if self.is_archived_cursor() {
            return None;
        }
        self.tiles
            .get(self.selected_index)
            .map(|t| t.plan.id.as_str())
    }

    /// Insert (or overwrite) the cached step list for `plan_id`.
    pub fn cache_preview_steps(&mut self, plan_id: String, steps: Vec<Step>) {
        self.step_preview_cache.insert(plan_id, steps);
    }

    /// Whether the preview cache already has steps for `plan_id`.
    pub fn preview_cache_contains(&self, plan_id: &str) -> bool {
        self.step_preview_cache.contains_key(plan_id)
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
    app.toasts.prune(Instant::now());

    let crumbs: [&str; 1] = ["ralph"];
    let normal_hint = "[j/k] nav  [enter] open  [space] select  [i] new  [A] approve  [Q] questions  [d] archive  [/:] cmd  [q] quit";
    let palette_hint = "[tab] complete  [enter] submit  [esc] cancel";
    let hint = if app.palette_active() {
        palette_hint
    } else {
        normal_hint
    };
    let banner = read_only::banner(app.read_only);
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint,
            cwd: Path::new(&app.project),
            banner: banner.as_deref(),
            running_indicator: None,
        },
    );

    // §5: split the body horizontally — `app.split_pct` percent for the
    // tile column on the left, the remainder for the step-list preview of
    // the highlighted plan on the right. The preview is read-only and
    // drawn via the shared `step_list` widget so its rows stay
    // pixel-identical to plan-detail. `split_pct` defaults to 40 and is
    // moved by mouse drag on the divider; `last_body_width` is captured
    // here so `handle_mouse` can convert cursor column → percent.
    app.last_body_width = body.width;
    let split_pct = app.split_pct;
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(split_pct),
            Constraint::Percentage(100 - split_pct),
        ])
        .split(body);
    let left = panes[0];
    let right = panes[1];

    update_scroll(app, left.height);
    render_tiles(frame.buffer_mut(), left, app);
    render_step_preview(frame, right, app);

    // Toast slot lives over the bottom chrome row — overwrites the hint while
    // a toast is current, leaves cwd/version on the right alone.
    if let Some(toast) = app.toasts.current() {
        let area = frame.area();
        if area.height >= 1 && area.width > 0 {
            render_toast_overlay(frame, area, &toast.text, toast.color);
        }
    }

    // Help overlay sits on top of everything else when `?` has been pressed.
    if app.help.is_visible() {
        let area = frame.area();
        help::render(frame, area, &help::for_plan_list());
    }

    // Palette bar overlays the bottom chrome row when active. Drawn after
    // the help overlay so a visible palette is the topmost layer when
    // both happen to be open (the dispatcher prevents this in practice
    // by routing keys to the palette first, but the layering here is
    // defensive). TUI-plan.md §9.
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

/// Render the right-pane step-list preview of the highlighted plan.
///
/// Blank when the cursor is on the archived sentinel or the tile list is
/// empty (TUI-plan.md §5). Re-keys [`PlanListApp::preview_list_state`] on
/// cursor moves so the new pane starts at the top instead of carrying the
/// previous plan's scroll offset.
fn render_step_preview(frame: &mut Frame, area: Rect, app: &mut PlanListApp) {
    if app.is_archived_cursor() {
        return;
    }
    let Some(tile) = app.tiles.get(app.selected_index) else {
        return;
    };
    let plan_id = tile.plan.id.clone();
    let plan_slug = tile.plan.slug.clone();

    if app.preview_keyed_plan.as_deref() != Some(plan_id.as_str()) {
        app.preview_list_state = ListState::default();
        app.preview_keyed_plan = Some(plan_id.clone());
    }

    let Some(steps) = app.step_preview_cache.get(&plan_id) else {
        return;
    };
    step_list::render(
        frame,
        area,
        steps,
        &app.preview_selection,
        None,
        false,
        plan_slug.as_str(),
        &mut app.preview_list_state,
    );
}

fn render_toast_overlay(frame: &mut Frame, area: Rect, text: &str, color: ratatui::style::Color) {
    // Cap toast width so a short message doesn't clobber the bottom-right
    // cwd/version column. A 1-char trailing pad keeps the toast visually
    // distinct from the cwd when both fit on the row.
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
        truncate(text, toast_area.width as usize),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(para, toast_area);
}

/// Render the tile column into the supplied buffer area.
///
/// Exposed so unit tests can render straight into a `Buffer` without
/// constructing a `Terminal` / `TestBackend`.
pub fn render_tiles(buf: &mut Buffer, area: Rect, app: &PlanListApp) {
    if app.tiles.is_empty() && !app.archived_tile_visible() {
        let empty = Paragraph::new("No plans for this project. Press `i` to create one.");
        empty.render(area, buf);
        return;
    }
    let tile_h = TILE_HEIGHT;
    let visible = (area.height / tile_h) as usize;
    if visible == 0 {
        return;
    }
    let nav = app.navigable_count();
    let end = (app.scroll_offset + visible).min(nav);
    for (slot, idx) in (app.scroll_offset..end).enumerate() {
        let tile_area = Rect {
            x: area.x,
            y: area.y + (slot as u16) * tile_h,
            width: area.width,
            height: tile_h,
        };
        let highlighted = idx == app.selected_index;
        if idx < app.tiles.len() {
            let tile = &app.tiles[idx];
            let badge = app.selection.index_of(&tile.plan.id);
            render_tile(
                buf,
                tile_area,
                tile,
                highlighted,
                badge,
                &app.display_timezone,
            );
        } else {
            // Archived sentinel — last navigable slot when visible.
            render_archived_sentinel(buf, tile_area, app.archived_count, highlighted);
        }
    }
}

/// Adjust `scroll_offset` so the highlighted tile is visible.
fn update_scroll(app: &mut PlanListApp, body_height: u16) {
    let nav = app.navigable_count();
    if nav == 0 {
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

/// Render the "Archived (N)" sentinel tile with a red border (TUI-plan.md
/// §5). When the cursor is on this slot the border switches to CURSOR.
pub(crate) fn render_archived_sentinel(
    buf: &mut Buffer,
    area: Rect,
    count: u32,
    highlighted: bool,
) {
    let border_style = if highlighted {
        Style::default().fg(theme::CURSOR)
    } else {
        Style::default().fg(theme::STATUS_FAILED)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style);
    let inner = block.inner(area);
    block.render(area, buf);
    if inner.height == 0 {
        return;
    }
    let title = format!("Archived ({count})");
    let title_para = Paragraph::new(Line::from(Span::styled(
        truncate(&title, inner.width as usize),
        Style::default().add_modifier(Modifier::BOLD),
    )));
    let title_area = Rect { height: 1, ..inner };
    title_para.render(title_area, buf);
    if inner.height >= 2 {
        let hint = "Press → / l / enter to view";
        let hint_area = Rect {
            y: inner.y + 1,
            height: 1,
            ..inner
        };
        let hint_para = Paragraph::new(truncate(hint, inner.width as usize));
        hint_para.render(hint_area, buf);
    }
}

/// Render a single plan tile into `area`. `pub(crate)` so the archived list
/// view can reuse the exact same tile body (TUI-plan.md §6 says the archived
/// view has the same layout as the plan list).
pub(crate) fn render_tile(
    buf: &mut Buffer,
    area: Rect,
    tile: &PlanTile,
    highlighted: bool,
    selection_badge: Option<usize>,
    tz: &str,
) {
    // §5 border/title color rules:
    //   default     → CHROME_DIM
    //   highlighted → CURSOR (#f7d135)
    //   selected    → SELECTION (#56d0d9)
    //   both        → SELECTION border + CURSOR-tinted title
    let selected = selection_badge.is_some();
    let border_style = match (highlighted, selected) {
        (_, true) => Style::default().fg(theme::SELECTION),
        (true, false) => Style::default().fg(theme::CURSOR),
        (false, false) => Style::default().fg(theme::CHROME_DIM),
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
    let mut title_style = Style::default().add_modifier(Modifier::BOLD);
    if highlighted && selected {
        // "Highlighted **and** selected" tints the title with CURSOR so the
        // user can see both states at once even though the border is
        // SELECTION-colored.
        title_style = title_style.fg(theme::CURSOR);
    }
    // Reserve space at the right edge of the title row for the `[N]` badge
    // when this tile is selected. Badge format is `[<digits>]`, e.g. `[12]`.
    let badge_text = selection_badge.map(|n| format!("[{n}]"));
    let badge_cols = badge_text
        .as_deref()
        .map(|s| s.chars().count())
        .unwrap_or(0);
    // §5: when `questions_enabled` is on, reserve 2 cols at the left of the
    // title row for a `?` glyph + space separator (top-left corner badge).
    let q_cols: usize = if tile.plan.questions_enabled { 2 } else { 0 };
    let title_max = (inner.width as usize).saturating_sub(badge_cols + q_cols);
    let title = Paragraph::new(Line::from(Span::styled(
        truncate(tile.plan.slug.as_str(), title_max),
        title_style,
    )));
    let title_area = Rect {
        x: inner.x.saturating_add(q_cols as u16),
        y: inner.y,
        width: inner.width.saturating_sub(q_cols as u16),
        height: 1,
    };
    title.render(title_area, buf);
    if tile.plan.questions_enabled && inner.width > 0 {
        let q_para = Paragraph::new(Span::styled(
            "?",
            Style::default()
                .fg(theme::STATUS_QUESTION)
                .add_modifier(Modifier::BOLD),
        ));
        let q_area = Rect {
            x: inner.x,
            y: inner.y,
            width: 1,
            height: 1,
        };
        q_para.render(q_area, buf);
    }
    if let Some(text) = badge_text
        && (inner.width as usize) >= badge_cols
    {
        let badge = Paragraph::new(Span::styled(
            text,
            Style::default()
                .fg(theme::SELECTION)
                .add_modifier(Modifier::BOLD),
        ));
        let badge_area = Rect {
            x: inner.x + inner.width - badge_cols as u16,
            y: inner.y,
            width: badge_cols as u16,
            height: 1,
        };
        badge.render(badge_area, buf);
    }

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
        // §17: derived `Question` status overrides the stored status whenever
        // any unanswered question row exists for this plan.
        let dot_color = if tile.unanswered_questions > 0 {
            theme::STATUS_QUESTION
        } else {
            status_dot_color(tile.plan.status)
        };
        let line = Line::from(vec![
            Span::styled("● ", Style::default().fg(dot_color)),
            Span::raw(format!("{}/{}", tile.completed, tile.total)),
        ]);
        let dot = Paragraph::new(line);
        dot.render(dot_area, buf);
    }

    // §17: when this plan has unanswered questions, render a one-line teaser
    // of the oldest question on the date row (between title and status). The
    // teaser is dim purple to keep the tile-wide `STATUS_QUESTION` association
    // without overpowering the title.
    if tile.unanswered_questions > 0
        && let Some(q) = tile.oldest_question.as_deref()
        && inner.height >= 3
    {
        let teaser_area = Rect {
            y: inner.y + 2,
            height: 1,
            ..inner
        };
        // Overwrite the timestamp line: §17 says the teaser replaces the
        // standard line on plans with open questions.
        let label = format!("? {q}");
        let para = Paragraph::new(Span::styled(
            truncate(&label, inner.width as usize),
            Style::default().fg(theme::STATUS_QUESTION),
        ));
        para.render(teaser_area, buf);
    }
}

/// Right-truncate a string to fit `max_cols` display columns, appending `…`
/// when truncation occurs. Width is approximated by `char` count, which is
/// good enough for the slugs / timestamps the tile renders (no CJK / emoji
/// in those fields).
pub(crate) fn truncate(s: &str, max_cols: usize) -> String {
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
            pause_requested: false,
            last_run_branch: None,
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
        (0..n).map(|i| make_tile(&format!("plan-{i}"))).collect()
    }

    #[test]
    fn test_initial_cursor_is_zero() {
        let app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        assert_eq!(app.selected_index, 0);
        assert_eq!(app.scroll_offset, 0);
        assert!(!app.should_quit);
        assert!(app.open_request.is_none());
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
        let req = app.request_open();
        assert_eq!(req, Some(OpenRequest::Plan("plan-1".to_string())));
        assert_eq!(
            app.open_request,
            Some(OpenRequest::Plan("plan-1".to_string()))
        );
    }

    #[test]
    fn test_request_open_empty_returns_none() {
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        let req = app.request_open();
        assert!(req.is_none());
        assert!(app.open_request.is_none());
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
        render_tile(&mut buf, area, &tile, true, None, "UTC");

        // Title row contains the slug.
        let row1 = (0..40).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(row1.contains("my-plan"), "got row1: {row1:?}");

        // Timestamp row contains "Ran May 4".
        let row3 = (0..40).map(|x| buf[(x, 3)].symbol()).collect::<String>();
        assert!(row3.contains("Ran May 4"), "got row3: {row3:?}");

        // Status row contains the dot + counts. Inside a 6-row tile the
        // dot lives at y=4 (top border 0, title 1, blank 2, date 3, dot 4,
        // bottom border 5).
        let row4 = (0..40).map(|x| buf[(x, 4)].symbol()).collect::<String>();
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

    // -- Selection (state) --------------------------------------------------

    #[test]
    fn test_toggle_selection_uses_plan_id() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        app.toggle_selection();
        assert!(app.selection.is_selected(&"id-plan-1".to_string()));
        assert_eq!(app.selection.len(), 1);

        // Toggling the same tile again clears it.
        app.toggle_selection();
        assert!(!app.selection.is_selected(&"id-plan-1".to_string()));
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_toggle_selection_preserves_order_across_tiles() {
        let mut app = PlanListApp::new(make_tiles(4), "/proj", "UTC");
        app.selected_index = 2;
        app.toggle_selection();
        app.selected_index = 0;
        app.toggle_selection();
        app.selected_index = 3;
        app.toggle_selection();
        // Order = pick order, not list order.
        assert_eq!(
            app.selection.as_slice(),
            &[
                "id-plan-2".to_string(),
                "id-plan-0".to_string(),
                "id-plan-3".to_string(),
            ]
        );
        assert_eq!(app.selection.index_of(&"id-plan-2".to_string()), Some(1));
        assert_eq!(app.selection.index_of(&"id-plan-0".to_string()), Some(2));
        assert_eq!(app.selection.index_of(&"id-plan-3".to_string()), Some(3));
    }

    #[test]
    fn test_toggle_selection_empty_tiles_is_noop() {
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");
        app.toggle_selection();
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_escape_with_no_selection_quits() {
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        let consumed = app.escape();
        assert!(!consumed, "esc without selection should not be consumed");
        assert!(app.should_quit);
    }

    #[test]
    fn test_escape_with_selection_clears_and_does_not_quit() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        let consumed = app.escape();
        assert!(consumed, "esc with selection should be consumed");
        assert!(app.selection.is_empty());
        assert!(!app.should_quit);
    }

    // -- Selection (rendering) ---------------------------------------------

    #[test]
    fn test_render_tile_with_badge_emits_index_marker() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        let tile = make_tile("plan");
        // Selected, not highlighted: SELECTION border + [N] badge.
        render_tile(&mut buf, area, &tile, false, Some(2), "UTC");
        let row1 = (0..30).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(row1.contains("plan"), "title missing: {row1:?}");
        assert!(row1.contains("[2]"), "badge missing: {row1:?}");
    }

    #[test]
    fn test_render_tile_badge_paints_top_border_corner_with_selection_color() {
        // Selected tile (no cursor) → border color is SELECTION on every
        // cell of the top border row.
        let mut buf = Buffer::empty(Rect::new(0, 0, 20, 6));
        let area = buf.area;
        let tile = make_tile("plan");
        render_tile(&mut buf, area, &tile, false, Some(1), "UTC");
        // Top-left corner cell carries the border style.
        let style = buf[(0, 0)].style();
        assert_eq!(style.fg, Some(theme::SELECTION));
    }

    #[test]
    fn test_render_tile_highlighted_only_uses_cursor_border() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 20, 6));
        let area = buf.area;
        let tile = make_tile("plan");
        render_tile(&mut buf, area, &tile, true, None, "UTC");
        let style = buf[(0, 0)].style();
        assert_eq!(style.fg, Some(theme::CURSOR));
    }

    #[test]
    fn test_render_tile_highlighted_and_selected_uses_selection_border() {
        // §5: both states → SELECTION border with CURSOR-tinted title.
        let mut buf = Buffer::empty(Rect::new(0, 0, 20, 6));
        let area = buf.area;
        let tile = make_tile("plan");
        render_tile(&mut buf, area, &tile, true, Some(1), "UTC");
        // Border color is SELECTION.
        assert_eq!(buf[(0, 0)].style().fg, Some(theme::SELECTION));
        // The first character of the slug ("p") sits at inner.x = 1, y = 1.
        // Its fg should be CURSOR.
        assert_eq!(buf[(1, 1)].symbol(), "p");
        assert_eq!(buf[(1, 1)].style().fg, Some(theme::CURSOR));
    }

    #[test]
    fn test_render_tiles_renders_badge_for_selected_plan() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.toggle_selection();
        render_tiles(&mut buf, area, &app);
        let row1 = (0..30).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(row1.contains("[1]"), "expected [1] badge: {row1:?}");
    }

    // -- `?` corner badge for questions_enabled (TUI-plan.md §5) -----------

    #[test]
    fn render_tile_with_questions_enabled_renders_corner_question_glyph() {
        // §5: when `questions_enabled` is true, the tile shows a `?` glyph
        // in the top-LEFT corner of the title row (inner.x, inner.y).
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        let mut tile = make_tile("plan");
        tile.plan.questions_enabled = true;
        render_tile(&mut buf, area, &tile, false, None, "UTC");
        // Title row is row 1 (inner.y = 1 with single-cell border).
        // First inner column (x=1) should hold the `?`.
        assert_eq!(buf[(1, 1)].symbol(), "?");
        assert_eq!(buf[(1, 1)].style().fg, Some(theme::STATUS_QUESTION));
    }

    #[test]
    fn render_tile_without_questions_enabled_omits_corner_question_glyph() {
        // questions_enabled = false → no `?` in the top-left, and the title
        // starts flush at inner.x as before.
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        let tile = make_tile("plan");
        assert!(!tile.plan.questions_enabled);
        render_tile(&mut buf, area, &tile, false, None, "UTC");
        // No `?` at the top-left inner cell.
        assert_ne!(buf[(1, 1)].symbol(), "?");
        // Title still anchored at inner.x — first slug char "p" sits at x=1.
        assert_eq!(buf[(1, 1)].symbol(), "p");
    }

    #[test]
    fn render_tile_selected_with_questions_enabled_renders_both_badges() {
        // §5: a selected, questions-enabled tile shows BOTH the `?` glyph
        // (top-left) and the `[N]` selection-order badge (top-right). The
        // two badges live on opposite sides of the title row and don't
        // collide.
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        let mut tile = make_tile("plan");
        tile.plan.questions_enabled = true;
        render_tile(&mut buf, area, &tile, false, Some(2), "UTC");
        // Top-left: `?` glyph.
        assert_eq!(buf[(1, 1)].symbol(), "?");
        assert_eq!(buf[(1, 1)].style().fg, Some(theme::STATUS_QUESTION));
        // Top-right: `[2]` badge somewhere on the title row.
        let row1 = (0..30).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(row1.contains("[2]"), "expected [2] badge: {row1:?}");
        // Title slug shifts right by 2 cols to leave room for `?` + space.
        // First slug char "p" now sits at x = inner.x + 2 = 3.
        assert_eq!(buf[(3, 1)].symbol(), "p");
    }

    // -- Cursor target ------------------------------------------------------

    #[test]
    fn cursor_plan_returns_highlighted_plan() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        let plan = app.cursor_plan().expect("expected cursor plan");
        assert_eq!(plan.slug, "plan-1");
    }

    #[test]
    fn cursor_plan_empty_returns_none() {
        let app = PlanListApp::new(vec![], "/proj", "UTC");
        assert!(app.cursor_plan().is_none());
    }

    // -- Archive targets ----------------------------------------------------

    #[test]
    fn archive_targets_returns_cursor_when_no_selection() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        assert_eq!(app.archive_targets(), vec!["id-plan-1".to_string()]);
    }

    #[test]
    fn archive_targets_returns_selection_in_order_when_present() {
        let mut app = PlanListApp::new(make_tiles(4), "/proj", "UTC");
        // Select 2, 0, 3 in pick order; cursor still on 2 (last toggled).
        app.selected_index = 2;
        app.toggle_selection();
        app.selected_index = 0;
        app.toggle_selection();
        app.selected_index = 3;
        app.toggle_selection();
        // Cursor target id-plan-3 must NOT short-circuit the selection list.
        assert_eq!(
            app.archive_targets(),
            vec![
                "id-plan-2".to_string(),
                "id-plan-0".to_string(),
                "id-plan-3".to_string(),
            ]
        );
    }

    #[test]
    fn archive_targets_empty_tiles_no_selection() {
        let app = PlanListApp::new(vec![], "/proj", "UTC");
        assert!(app.archive_targets().is_empty());
    }

    // -- In-place update ----------------------------------------------------

    #[test]
    fn update_plan_in_place_replaces_matching_tile() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 1;
        let mut updated = make_plan("plan-1");
        updated.status = PlanStatus::Ready;
        updated.questions_enabled = true;
        app.update_plan_in_place(updated);
        let tile = &app.tiles[1];
        assert_eq!(tile.plan.status, PlanStatus::Ready);
        assert!(tile.plan.questions_enabled);
        // Other tiles unchanged.
        assert_eq!(app.tiles[0].plan.status, PlanStatus::Ready);
        assert!(!app.tiles[0].plan.questions_enabled);
    }

    #[test]
    fn update_plan_in_place_preserves_selection_and_cursor() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        app.toggle_selection();
        app.selected_index = 0;
        app.toggle_selection();
        assert_eq!(app.selection.len(), 2);

        let mut updated = make_plan("plan-1");
        updated.status = PlanStatus::Failed;
        app.update_plan_in_place(updated);

        // Selection and cursor are untouched.
        assert_eq!(app.selection.len(), 2);
        assert_eq!(app.selected_index, 0);
        assert!(app.selection.is_selected(&"id-plan-2".to_string()));
        assert!(app.selection.is_selected(&"id-plan-0".to_string()));
    }

    #[test]
    fn update_plan_in_place_no_match_is_noop() {
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        let mut other = make_plan("other");
        other.id = "id-not-in-list".to_string();
        app.update_plan_in_place(other);
        // Tiles unchanged.
        assert_eq!(app.tiles.len(), 2);
        assert_eq!(app.tiles[0].plan.slug, "plan-0");
        assert_eq!(app.tiles[1].plan.slug, "plan-1");
    }

    // -- Refresh tiles ------------------------------------------------------

    #[test]
    fn refresh_tiles_clears_selection() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        app.refresh_tiles(make_tiles(2), 0);
        assert!(app.selection.is_empty());
    }

    #[test]
    fn refresh_tiles_clamps_cursor_into_new_range() {
        let mut app = PlanListApp::new(make_tiles(5), "/proj", "UTC");
        app.selected_index = 4;
        // Simulate archiving the last 3 plans — only 2 left.
        app.refresh_tiles(make_tiles(2), 0);
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn refresh_tiles_resets_cursor_when_list_empties() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        app.refresh_tiles(vec![], 0);
        assert_eq!(app.selected_index, 0);
        assert_eq!(app.scroll_offset, 0);
    }

    #[test]
    fn refresh_tiles_preserves_in_range_cursor() {
        let mut app = PlanListApp::new(make_tiles(5), "/proj", "UTC");
        app.selected_index = 1;
        app.refresh_tiles(make_tiles(4), 0);
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn refresh_tiles_clamps_cursor_to_archived_sentinel_when_only_archived_remain() {
        // Last regular plan was just archived — tile list empties but the
        // archived sentinel takes its place, so the cursor should land on it.
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        app.selected_index = 1;
        app.refresh_tiles(vec![], 1);
        assert!(app.archived_tile_visible());
        assert_eq!(app.selected_index, 0);
        assert!(app.is_archived_cursor());
    }

    // -- Toast rendering ----------------------------------------------------

    #[test]
    fn draw_renders_toast_text_over_bottom_row() {
        use crate::tui::toast::ToastKind;
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.toasts
            .push("Archived 1 plan.", ToastKind::Success, Instant::now());

        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let bottom_y = buffer.area().height - 1;
        let bottom_row = (0..buffer.area().width)
            .map(|x| buffer[(x, bottom_y)].symbol())
            .collect::<String>();
        assert!(
            bottom_row.contains("Archived 1 plan."),
            "toast text missing on bottom row: {bottom_row:?}"
        );
        // Toast should be styled with TOAST_SUCCESS color.
        assert_eq!(buffer[(0, bottom_y)].style().fg, Some(theme::TOAST_SUCCESS));
    }

    // -- Archived sentinel --------------------------------------------------

    #[test]
    fn archived_tile_invisible_when_count_zero() {
        let app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        assert!(!app.archived_tile_visible());
        assert_eq!(app.navigable_count(), 2);
    }

    #[test]
    fn archived_tile_visible_when_count_positive() {
        let app = PlanListApp::new(make_tiles(2), "/proj", "UTC").with_archived_count(7);
        assert!(app.archived_tile_visible());
        assert_eq!(app.navigable_count(), 3);
    }

    #[test]
    fn navigation_includes_archived_sentinel_slot() {
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC").with_archived_count(1);
        // index 0 → 1 → 2 (sentinel) → wraps back to 0
        assert_eq!(app.selected_index, 0);
        app.navigate_down();
        assert_eq!(app.selected_index, 1);
        app.navigate_down();
        assert_eq!(app.selected_index, 2);
        assert!(app.is_archived_cursor());
        app.navigate_down();
        assert_eq!(app.selected_index, 0);
        assert!(!app.is_archived_cursor());
    }

    #[test]
    fn jump_bottom_lands_on_archived_when_visible() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC").with_archived_count(2);
        app.jump_bottom();
        assert_eq!(app.selected_index, 3);
        assert!(app.is_archived_cursor());
    }

    #[test]
    fn cursor_plan_returns_none_when_on_archived_sentinel() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(1);
        app.selected_index = 1;
        assert!(app.is_archived_cursor());
        assert!(app.cursor_plan().is_none());
    }

    #[test]
    fn toggle_selection_on_archived_sentinel_is_noop() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(1);
        app.selected_index = 1;
        app.toggle_selection();
        assert!(app.selection.is_empty());
    }

    #[test]
    fn archive_targets_empty_when_cursor_on_archived_sentinel() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(1);
        app.selected_index = 1;
        assert!(app.archive_targets().is_empty());
    }

    #[test]
    fn request_open_on_archived_sentinel_returns_archived_variant() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(1);
        app.selected_index = 1;
        let req = app.request_open();
        assert_eq!(req, Some(OpenRequest::Archived));
        assert_eq!(app.open_request, Some(OpenRequest::Archived));
    }

    #[test]
    fn request_open_on_plan_when_no_tiles_but_archived_visible() {
        // Empty tile list + archived sentinel: selected_index 0 IS the
        // sentinel, so open should produce Archived rather than None.
        let mut app = PlanListApp::new(vec![], "/proj", "UTC").with_archived_count(3);
        let req = app.request_open();
        assert_eq!(req, Some(OpenRequest::Archived));
    }

    #[test]
    fn render_tiles_renders_archived_sentinel_at_bottom() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 18));
        let area = buf.area;
        let app = PlanListApp::new(make_tiles(2), "/proj", "UTC").with_archived_count(5);
        render_tiles(&mut buf, area, &app);
        // Sentinel sits at the bottom, after the two tiles (each 6 rows).
        // Top of sentinel border = row 12. Title row = row 13.
        let sentinel_title = (0..40).map(|x| buf[(x, 13)].symbol()).collect::<String>();
        assert!(
            sentinel_title.contains("Archived (5)"),
            "expected 'Archived (5)' on row 13: {sentinel_title:?}"
        );
        let sentinel_hint = (0..40).map(|x| buf[(x, 14)].symbol()).collect::<String>();
        assert!(
            sentinel_hint.contains("enter"),
            "expected enter hint on row 14: {sentinel_hint:?}"
        );
    }

    #[test]
    fn render_tiles_skips_archived_sentinel_when_count_zero() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 18));
        let area = buf.area;
        let app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        render_tiles(&mut buf, area, &app);
        // Row 13 (where the sentinel would land) is blank.
        let row = (0..40).map(|x| buf[(x, 13)].symbol()).collect::<String>();
        assert!(
            !row.contains("Archived"),
            "sentinel should not render when count = 0: {row:?}"
        );
    }

    #[test]
    fn render_archived_sentinel_uses_red_border_when_not_highlighted() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        render_archived_sentinel(&mut buf, area, 4, false);
        // Top-left border cell carries the border style.
        assert_eq!(buf[(0, 0)].style().fg, Some(theme::STATUS_FAILED));
    }

    #[test]
    fn render_archived_sentinel_uses_cursor_border_when_highlighted() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 30, 6));
        let area = buf.area;
        render_archived_sentinel(&mut buf, area, 4, true);
        assert_eq!(buf[(0, 0)].style().fg, Some(theme::CURSOR));
    }

    #[test]
    fn render_tiles_empty_with_archived_visible_renders_only_sentinel() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        let area = buf.area;
        let app = PlanListApp::new(vec![], "/proj", "UTC").with_archived_count(2);
        render_tiles(&mut buf, area, &app);
        let row = (0..40).map(|x| buf[(x, 1)].symbol()).collect::<String>();
        assert!(
            row.contains("Archived (2)"),
            "expected sentinel title: {row:?}"
        );
        // The "No plans" placeholder must not render when the sentinel exists.
        let row0 = (0..40).map(|x| buf[(x, 0)].symbol()).collect::<String>();
        assert!(
            !row0.contains("No plans"),
            "placeholder should be suppressed: {row0:?}"
        );
    }

    #[test]
    fn test_status_dot_color_legend() {
        // Spot-check the §5 legend rather than re-listing every variant.
        assert_eq!(
            status_dot_color(PlanStatus::Complete),
            theme::STATUS_COMPLETE
        );
        assert_eq!(
            status_dot_color(PlanStatus::InProgress),
            theme::STATUS_IN_PROGRESS
        );
        assert_eq!(status_dot_color(PlanStatus::Ready), theme::STATUS_PENDING);
        assert_eq!(
            status_dot_color(PlanStatus::Planning),
            theme::STATUS_PENDING
        );
        assert_eq!(status_dot_color(PlanStatus::Failed), theme::STATUS_FAILED);
        assert_eq!(status_dot_color(PlanStatus::Aborted), theme::STATUS_FAILED);
        assert_eq!(
            status_dot_color(PlanStatus::Question),
            theme::STATUS_QUESTION
        );
    }

    // -- Question surfaces (TUI-plan.md §17) ---------------------------------

    #[test]
    fn render_tile_with_open_questions_uses_purple_dot() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        let area = buf.area;
        let mut tile = make_tile("with-q");
        tile.unanswered_questions = 2;
        tile.oldest_question = Some("Pick a logging crate".to_string());
        render_tile(&mut buf, area, &tile, false, None, "UTC");
        // Dot row: y=4. The dot is at inner.x = 1 (border).
        assert_eq!(buf[(1, 4)].symbol(), "●");
        assert_eq!(buf[(1, 4)].style().fg, Some(theme::STATUS_QUESTION));
    }

    #[test]
    fn render_tile_with_open_questions_renders_teaser() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        let area = buf.area;
        let mut tile = make_tile("with-q");
        tile.unanswered_questions = 1;
        tile.oldest_question = Some("Pick a logging crate".to_string());
        render_tile(&mut buf, area, &tile, false, None, "UTC");
        // Teaser overwrites the date row at y=3.
        let row3 = (0..40).map(|x| buf[(x, 3)].symbol()).collect::<String>();
        assert!(
            row3.contains("Pick a logging crate"),
            "expected question teaser on date row: {row3:?}"
        );
    }

    #[test]
    fn render_tile_without_open_questions_keeps_status_color() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 40, 6));
        let area = buf.area;
        let mut tile = make_tile("no-q");
        tile.plan.status = PlanStatus::Complete;
        render_tile(&mut buf, area, &tile, false, None, "UTC");
        // Dot at (1, 4) should be STATUS_COMPLETE, not STATUS_QUESTION.
        assert_eq!(buf[(1, 4)].symbol(), "●");
        assert_eq!(buf[(1, 4)].style().fg, Some(theme::STATUS_COMPLETE));
    }

    // -- Read-only attach lockdown (TUI-plan.md §13.2) -------------------

    #[test]
    fn test_read_only_default_is_editable() {
        let app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        assert_eq!(app.read_only, ReadOnly::Editable);
    }

    #[test]
    fn test_set_read_only_updates_state() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.set_read_only(ReadOnly::Locked { pid: 4242 });
        assert_eq!(app.read_only, ReadOnly::Locked { pid: 4242 });
        assert!(app.read_only.is_locked());
    }

    // -- Help overlay (TUI-plan.md §15) ---------------------------------

    #[test]
    fn help_state_default_hidden() {
        let app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        assert!(!app.help.is_visible());
    }

    #[test]
    fn help_intercepts_question_mark_and_esc() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        let q = KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE);
        let r = app.help.intercept_key(q);
        assert_eq!(r, crate::tui::help::InterceptResult::Opened);
        assert!(app.help.is_visible());

        let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        let r = app.help.intercept_key(esc);
        assert_eq!(r, crate::tui::help::InterceptResult::Closed);
        assert!(!app.help.is_visible());
    }

    // -- Two-pane layout (TUI-plan.md §5) -----------------------------------

    fn make_step(plan_id: &str, idx: usize, title: &str) -> Step {
        use crate::plan::{ChangePolicy, StepStatus};
        Step {
            id: format!("{plan_id}-step-{idx}"),
            plan_id: plan_id.to_string(),
            sort_key: format!("a{idx}"),
            title: title.to_string(),
            description: String::new(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
        }
    }

    /// Render a slice of `area` rows as a single newline-joined string for
    /// substring-based assertions. Used by the two-pane preview tests below
    /// so they can be expressive about what region they're inspecting.
    fn region_text(
        buffer: &ratatui::buffer::Buffer,
        x: u16,
        y: u16,
        w: u16,
        h: u16,
    ) -> String {
        let mut out = String::new();
        for row in y..(y + h) {
            for col in x..(x + w) {
                out.push_str(buffer[(col, row)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn draw_splits_body_into_left_and_right_panes() {
        // The body should be split 40/60 horizontally — both halves carry
        // visible chrome (left tiles, right step-list block) and neither is
        // empty when there's a highlighted plan with cached steps.
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![make_step("id-plan-0", 0, "alpha-task")],
        );

        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();

        // Left pane is 40% of 80 = 32 cols; the tile border `┌` sits at
        // column 0 of the body row (y=1 because chrome reserves y=0).
        assert_eq!(buffer[(0, 1)].symbol(), "┌", "left tile border missing");

        // Right pane is the remaining 48 cols starting at x=32. The step
        // list widget renders its own bordered block, so the top-left
        // corner of that block sits at (32, 1).
        assert_eq!(
            buffer[(32, 1)].symbol(),
            "┌",
            "right pane border missing at split boundary"
        );
    }

    #[test]
    fn draw_right_pane_renders_highlighted_plans_steps() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![
                make_step("id-plan-0", 0, "alpha-task"),
                make_step("id-plan-0", 1, "beta-task"),
            ],
        );

        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();

        // Right pane occupies cols 32..80, body rows 1..13.
        let right = region_text(&buffer, 32, 1, 48, 12);
        assert!(
            right.contains("alpha-task"),
            "expected alpha-task in right pane:\n{right}"
        );
        assert!(
            right.contains("beta-task"),
            "expected beta-task in right pane:\n{right}"
        );
        // Title of the bordered block is the plan slug.
        assert!(
            right.contains("plan-0"),
            "expected plan-0 title in right pane:\n{right}"
        );
    }

    #[test]
    fn draw_right_pane_blank_for_archived_tile_cursor() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(2);
        // Even with steps cached for the regular plan, parking the cursor
        // on the archived sentinel must leave the right pane blank.
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![make_step("id-plan-0", 0, "alpha-task")],
        );
        app.selected_index = 1;
        assert!(app.is_archived_cursor());

        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();

        let right = region_text(&buffer, 32, 1, 48, 12);
        // No bordered block, no step titles, no plan slug — the right
        // pane should be entirely whitespace.
        assert!(
            right.chars().all(|c| c == ' ' || c == '\n'),
            "expected blank right pane when cursor is on archived sentinel:\n{right}"
        );
    }

    #[test]
    fn draw_right_pane_blank_when_no_plans() {
        // No tiles → no preview either. The "No plans" placeholder still
        // renders on the left, but the right pane stays empty.
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(vec![], "/proj", "UTC");

        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();

        let right = region_text(&buffer, 32, 1, 48, 12);
        assert!(
            right.chars().all(|c| c == ' ' || c == '\n'),
            "expected blank right pane when there are no plans:\n{right}"
        );
    }

    #[test]
    fn draw_re_keys_right_pane_when_cursor_moves_to_a_different_plan() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![make_step("id-plan-0", 0, "alpha-task")],
        );
        app.cache_preview_steps(
            "id-plan-1".to_string(),
            vec![make_step("id-plan-1", 0, "zeta-task")],
        );

        // First draw: cursor on plan-0 — right pane shows alpha-task and
        // the keyed plan tracks plan-0.
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let right = region_text(&buffer, 32, 1, 48, 12);
        assert!(
            right.contains("alpha-task"),
            "expected alpha-task before nav:\n{right}"
        );
        assert!(
            !right.contains("zeta-task"),
            "zeta-task should not appear before nav:\n{right}"
        );
        assert_eq!(
            app.preview_keyed_plan.as_deref(),
            Some("id-plan-0"),
            "preview_keyed_plan should track plan-0 after first draw"
        );

        // Move cursor to plan-1 and re-draw.
        app.navigate_down();
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let right = region_text(&buffer, 32, 1, 48, 12);
        assert!(
            right.contains("zeta-task"),
            "expected zeta-task after nav:\n{right}"
        );
        assert!(
            !right.contains("alpha-task"),
            "alpha-task should be gone after nav:\n{right}"
        );
        assert_eq!(
            app.preview_keyed_plan.as_deref(),
            Some("id-plan-1"),
            "preview_keyed_plan should re-key to plan-1 after navigation"
        );
    }

    #[test]
    fn refresh_tiles_clears_preview_cache_and_keying() {
        let mut app = PlanListApp::new(make_tiles(2), "/proj", "UTC");
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![make_step("id-plan-0", 0, "alpha")],
        );
        app.preview_keyed_plan = Some("id-plan-0".to_string());
        app.refresh_tiles(make_tiles(2), 0);
        assert!(app.step_preview_cache.is_empty());
        assert!(app.preview_keyed_plan.is_none());
    }

    #[test]
    fn highlighted_plan_id_returns_cursor_target() {
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.selected_index = 2;
        assert_eq!(app.highlighted_plan_id(), Some("id-plan-2"));
    }

    #[test]
    fn highlighted_plan_id_none_for_archived_sentinel() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC").with_archived_count(1);
        app.selected_index = 1;
        assert!(app.highlighted_plan_id().is_none());
    }

    #[test]
    fn highlighted_plan_id_none_for_empty_tiles() {
        let app = PlanListApp::new(vec![], "/proj", "UTC");
        assert!(app.highlighted_plan_id().is_none());
    }

    // -- Palette (TUI-plan.md §9) ---------------------------------------

    #[test]
    fn palette_default_inactive() {
        let app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        assert!(!app.palette_active());
        assert!(app.palette_bar.is_none());
    }

    #[test]
    fn palette_open_records_prefix() {
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
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
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        let _ = app.palette_bar.as_mut().unwrap().on_key(KeyEvent::new(
            KeyCode::Char('r'),
            KeyModifiers::NONE,
        ));
        assert_eq!(app.palette_bar.as_ref().unwrap().input, "r");
        app.close_palette();
        assert!(!app.palette_active());
    }

    #[test]
    fn palette_esc_yields_cancel_outcome() {
        use crate::tui::widgets::palette_bar::PaletteBarOutcome;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
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
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.open_palette('/');
        let bar = app.palette_bar.as_mut().unwrap();
        for c in "run".chars() {
            let _ = bar.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        let out = bar.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let input = match out {
            PaletteBarOutcome::Submit(s) => s,
            other => panic!("expected Submit, got {other:?}"),
        };
        assert_eq!(
            crate::tui::palette::parse(&input),
            Ok(PaletteCommand::Run(None))
        );
    }

    // -- Mouse-drag split (step 28) ---------------------------------------

    /// Construct a [`MouseEvent`] at `(column, row)` with the given kind.
    /// Helper for the divider-drag tests below — the modifiers field is
    /// always empty since the drag bindings ignore them.
    fn mouse_event(
        column: u16,
        row: u16,
        kind: crossterm::event::MouseEventKind,
    ) -> crossterm::event::MouseEvent {
        crossterm::event::MouseEvent {
            kind,
            column,
            row,
            modifiers: crossterm::event::KeyModifiers::NONE,
        }
    }

    #[test]
    fn split_drag_updates_split_pct_in_plan_list() {
        // Down on the divider, drag right by ~20 columns, release.
        // Body width 100 with default split_pct 40 puts the divider at
        // column 40; dragging the cursor to column 60 should land at 60%.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.last_body_width = 100;

        assert_eq!(app.split_pct, 40);
        assert!(!app.dragging_split);

        app.handle_mouse(mouse_event(40, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(
            app.dragging_split,
            "Down at the divider column should arm the drag"
        );

        app.handle_mouse(mouse_event(60, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 60, "drag to col 60 / 100 → 60%");

        app.handle_mouse(mouse_event(60, 5, MouseEventKind::Up(MouseButton::Left)));
        assert!(!app.dragging_split, "Up should clear the drag flag");
    }

    #[test]
    fn split_drag_press_off_divider_does_not_arm_in_plan_list() {
        // ±1 column tolerance: pressing far from the divider should not
        // arm a drag, so subsequent drag events leave split_pct alone.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.last_body_width = 100;

        // Divider is at column 40; press at column 10.
        app.handle_mouse(mouse_event(10, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_split);

        app.handle_mouse(mouse_event(70, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 40, "drag without arming should not resize");
    }

    #[test]
    fn split_drag_clamps_to_20_and_80_in_plan_list() {
        // Past column 0 still yields 20%; past column 80% still yields 80%.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        app.last_body_width = 100;

        app.handle_mouse(mouse_event(40, 5, MouseEventKind::Down(MouseButton::Left)));
        // Drag to column 0 (and slightly past via underflow-safe path).
        app.handle_mouse(mouse_event(0, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 20, "left clamp at 20%");

        // Drag far right; with body_width 100, column 95 maps to 95%, but
        // the clamp pins the percent at 80.
        app.handle_mouse(mouse_event(95, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 80, "right clamp at 80%");

        app.handle_mouse(mouse_event(95, 5, MouseEventKind::Up(MouseButton::Left)));
        assert!(!app.dragging_split);
    }

    #[test]
    fn handle_mouse_no_op_before_first_draw_in_plan_list() {
        // Before the first frame `last_body_width` is zero; mouse events
        // must not panic and must not arm a drag (divider would be at 0).
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanListApp::new(make_tiles(3), "/proj", "UTC");
        assert_eq!(app.last_body_width, 0);

        app.handle_mouse(mouse_event(0, 0, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_split);
    }

    #[test]
    fn draw_re_renders_after_drag_with_new_split_geometry() {
        // After a mouse drag updates split_pct from 40 → 60, the next draw
        // must place the right pane's bordered block at the new split
        // boundary (column 48 of an 80-wide terminal) instead of the
        // default column 32.
        use crossterm::event::{MouseButton, MouseEventKind};
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(80, 14);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = PlanListApp::new(make_tiles(1), "/proj", "UTC");
        app.cache_preview_steps(
            "id-plan-0".to_string(),
            vec![make_step("id-plan-0", 0, "alpha-task")],
        );

        // First draw to populate `last_body_width` and prove the default
        // 40/60 layout puts the right pane border at column 32.
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        assert_eq!(
            buffer[(32, 1)].symbol(),
            "┌",
            "default split should put right pane border at col 32"
        );
        assert_eq!(app.last_body_width, 80);

        // Arm and execute a drag from divider col 32 → col 48 (60% of 80).
        app.handle_mouse(mouse_event(32, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(app.dragging_split);
        app.handle_mouse(mouse_event(48, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 60);
        app.handle_mouse(mouse_event(48, 5, MouseEventKind::Up(MouseButton::Left)));

        // Re-draw with the new split. The right pane's bordered block now
        // starts at column 48; column 32 is back inside the left tile area.
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        assert_eq!(
            buffer[(48, 1)].symbol(),
            "┌",
            "after drag → 60%, right pane border should sit at col 48"
        );
        assert_ne!(
            buffer[(32, 1)].symbol(),
            "┌",
            "col 32 should no longer hold a pane border after the drag"
        );
    }
}
