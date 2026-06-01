// Plan detail view state
//
// Manages the state tracked by the plan-detail view of the TUI: selected step,
// input mode, execution timer, multi-selection, and step list. This module is
// independent of rendering and input handling so that it can be unit-tested
// without a terminal.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use crossterm::event::MouseEvent;
use ratatui::layout::Rect;
use ratatui::widgets::ListState;

use crate::config::Config;
use crate::frac_index::{self, FracIndexError};
use crate::plan::{ExecutionLog, Phase, Plan, Step, StepStatus};
use crate::run_lock::LiveRun;
use crate::tui::events::{StreamMode, TAIL_BUFFER_LINES, TAIL_VISIBLE_LINES};
use crate::tui::help::HelpState;
use crate::tui::read_only::ReadOnly;
use crate::tui::selection::Selection;
use crate::tui::toast::ToastQueue;
use crate::tui::views::outline_view::OutlineState;
use crate::tui::widgets::palette_bar::PaletteBarState;

// ---------------------------------------------------------------------------
// Input mode
// ---------------------------------------------------------------------------

/// Where the AddStep prompt is targeted: above the highlighted step (`i`) or
/// below it (`a`). The dispatcher uses this to compute the new sort_key
/// relative to the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddPosition {
    Above,
    Below,
}

/// Determines how keyboard input is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// Normal navigation mode (j/k/i/a/d/r/J/K/s/space/q).
    Normal,
    /// Inline text input for adding a new step at `position`.
    AddStep(AddPosition),
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

/// Core state for the plan-detail view of the TUI.
pub struct PlanDetailApp {
    /// The plan currently being displayed / executed.
    pub plan: Plan,

    /// Steps in sort_key order.
    pub steps: Vec<Step>,

    /// Currently highlighted step in the list (0-based).
    pub selected_index: usize,

    /// Current input mode.
    pub input_mode: InputMode,

    /// Text buffer for inline input (used in AddStep mode).
    pub input_buffer: String,

    /// Whether the user has requested to pop this view back to the plan list
    /// (`←`/`h`/`q`, or Ctrl-C). The dispatcher consumes this and exits the
    /// plan-detail event loop.
    pub should_pop: bool,

    /// Persistent list widget state so the viewport offset survives across frames.
    pub list_state: ListState,

    /// Default retry budget per step, sourced from `Config.max_retries_per_step`,
    /// used when a step has no explicit `max_retries` override.
    pub default_max_retries: u32,

    /// Multi-selection state, keyed by `Step.id` so the selection survives a
    /// refresh that re-orders the step list. Drives the `[N]` badge on each
    /// step row and the selection-aware `d` delete (TUI-plan.md §7).
    pub selection: Selection<String>,

    /// Toast queue rendered over the bottom chrome row. The dispatcher pushes
    /// onto this after operations (`d` delete, `r` reset, J/K move) so the
    /// user sees a transient confirmation.
    pub toasts: ToastQueue,

    /// Snapshot of the live `run_locks` row for this plan, refreshed by the
    /// dispatcher on each poll tick. `Some` when a runner subprocess (spawned
    /// by `R` here or by an external `ralph run`) is bound to this plan; the
    /// renderer reads `step_num` and `phase` to draw the "Running step N"
    /// banner per TUI-plan.md §7.
    pub live_run: Option<LiveRun>,

    /// True iff the dispatcher has wired a [`crate::tui::events::RunSubscription`]
    /// to this view (TUI-plan.md §13). Drives [`Self::is_run_live`] when no
    /// `live_run` row has been observed yet — the subscription begins emitting
    /// events the moment the child is spawned, so the TUI must render the
    /// running-state right pane without waiting for the DB row to land.
    pub subscribed: bool,

    /// Step number of the most recent `step_started` event (1-based). Drives
    /// the right-pane "Running step N" banner when the subscription is the
    /// active source of truth (per §13, DB poll is dropped for TUI-spawned
    /// runs). Cleared on `step_finished` / `plan_complete` / `summary`.
    pub subscribed_step_num: Option<i32>,

    /// Most recent `phase_changed` event. Same role as [`LiveRun::phase`] but
    /// sourced from the NDJSON stream rather than a DB poll.
    pub current_phase: Option<Phase>,

    /// Run-start instant captured from the NDJSON `run_started` event (the
    /// first event a `--json` runner emits). Used as the elapsed-timer base
    /// for the gap between run start and the first `phase_changed` and as
    /// the primary source of truth while a TUI-spawned subscription is
    /// active. `None` until `run_started` arrives or after
    /// [`Self::detach_subscription`].
    pub subscribed_started_at: Option<DateTime<Utc>>,

    /// Phase-start instant captured from the NDJSON `phase_changed` event.
    /// Preferred over `subscribed_started_at` once a phase has begun so the
    /// "Elapsed" display reflects time-in-phase (matching the DB-poll path
    /// which prefers `LiveRun.phase_started_at` over `started_at`).
    pub subscribed_phase_started_at: Option<DateTime<Utc>>,

    /// Rolling tail of harness stdout/stderr lines (oldest at front, newest
    /// at back). Capped at [`TAIL_BUFFER_LINES`].
    pub harness_tail: VecDeque<String>,

    /// Rolling tail of deterministic-test stdout/stderr lines, same shape as
    /// `harness_tail`.
    pub test_tail: VecDeque<String>,

    /// How many lines (counting from the newest) the user has scrolled back
    /// in the harness tail via `K`. 0 means "follow the newest line"; values
    /// > 0 freeze the view at an older window. `J` decrements toward 0.
    pub harness_tail_scroll: usize,

    /// Scroll offset for the test tail. Same semantics as `harness_tail_scroll`.
    pub test_tail_scroll: usize,

    /// Read-only attach state (TUI-plan.md §13.2). When `Locked`, the edit
    /// keybindings (`i`/`a`/`d`/`r`/`s`/`R`/Shift-`J`/`K`) are suppressed
    /// and the persistent banner replaces the bottom hint line. The
    /// dispatcher updates this each poll tick via [`Self::set_read_only`].
    pub read_only: ReadOnly,

    /// Open question `interruptions` rows for this plan, ordered oldest
    /// first per [`crate::storage::list_open_questions`]. Drives the §17
    /// banner pane and the `A` keybinding that pushes step-detail focused on
    /// the originating step. Refreshed by the dispatcher each poll tick.
    pub open_questions: Vec<crate::storage::OpenQuestion>,
    /// Per-step cache of `execution_logs` rows, keyed by `Step.id` and
    /// ordered by `attempt` ASC. Populated by the dispatcher each poll tick
    /// for steps in a terminal (`Complete`/`Failed`) status so the right
    /// pane can render the total + per-attempt duration breakdown without a
    /// DB roundtrip during `draw`. Steps with no recorded attempts simply
    /// have no entry (or an empty vec) and render no breakdown.
    pub execution_logs: HashMap<String, Vec<ExecutionLog>>,
    /// Preferred plain `ralph run` mode for the `R` keybinding. Defaults to
    /// the classic branch-creating, auto-stashing flow, but when this view
    /// was entered via `ralph run --current-branch` and/or
    /// `--no-auto-stash`, the dispatcher updates it so manual re-runs from
    /// the same screen keep the user's requested run semantics.
    preferred_run_mode: StreamMode,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// passing keys to the per-view input handler (TUI-plan.md §15).
    pub help: HelpState,
    /// Slash/colon command palette state (TUI-plan.md §9). `Some` while the
    /// bar is open; the dispatcher routes every key through
    /// [`PaletteBarState::on_key`] before any view bindings fire. `/` and
    /// `:` open it.
    pub palette_bar: Option<PaletteBarState>,

    /// Horizontal split between the step list (left) and detail pane
    /// (right) as a percent of the body width given to the left pane.
    /// Default 40, clamped to 20..=80 by the mouse-drag handler so neither
    /// pane can collapse. Session-only — never persisted to the DB.
    pub split_pct: u16,

    /// Body width recorded during the most recent `draw()`. Used by
    /// [`Self::handle_mouse`] to convert the cursor's absolute column into
    /// a percent of the body's horizontal extent. Zero before the first
    /// frame; mouse handling no-ops while it's zero.
    pub last_body_width: u16,

    /// True while a left-mouse drag started on the divider column is
    /// active. Cleared on `MouseEventKind::Up(Left)`.
    pub dragging_split: bool,

    /// The bordered `Rect` the step list was drawn into during the most
    /// recent `draw()` — the *outer* area passed to `step_list::render`
    /// (`Block::borders(ALL)` consumes one cell on each side). Used by
    /// [`Self::handle_mouse`] to hit-test a click row to a step index.
    /// Zero-sized before the first frame; mouse hit-testing no-ops then.
    pub step_list_area: Rect,

    /// Set by [`Self::handle_mouse`] when a left click lands on the
    /// already-highlighted step row: the dispatcher consumes this and
    /// opens step-detail (the same effect as pressing `Enter`), then
    /// clears it. `None` means no pending mouse-driven open.
    pub pending_open_step: Option<String>,

    /// Dependency-outline state (docs/dag-redesign.md §12.1/§12.2): the
    /// topological projection of `steps` + the focus/re-root stack. The
    /// dispatcher refreshes it each poll tick from
    /// `storage::list_step_dependency_edges` + the open-interruption set;
    /// the renderer draws `outline.visible_rows()` instead of the flat
    /// `steps` list. `selected_index` is mirrored to/from
    /// `outline.cursor()` so the existing skip/reset/delete cursor-target
    /// helpers keep working against the visible row.
    pub outline: OutlineState,
}

impl PlanDetailApp {
    /// Create a new PlanDetailApp with the given plan and steps.
    pub fn new(plan: Plan, steps: Vec<Step>, config: &Config) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        let outline = OutlineState::new(steps.clone(), Default::default(), Default::default());
        Self {
            plan,
            steps,
            selected_index: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            should_pop: false,
            list_state,
            default_max_retries: config.max_retries_per_step,
            selection: Selection::new(),
            toasts: ToastQueue::new(),
            live_run: None,
            subscribed: false,
            subscribed_step_num: None,
            current_phase: None,
            subscribed_started_at: None,
            subscribed_phase_started_at: None,
            harness_tail: VecDeque::new(),
            test_tail: VecDeque::new(),
            harness_tail_scroll: 0,
            test_tail_scroll: 0,
            read_only: ReadOnly::Editable,
            open_questions: Vec::new(),
            execution_logs: HashMap::new(),
            preferred_run_mode: StreamMode::Run {
                current_branch: false,
                no_auto_stash: false,
            },
            help: HelpState::new(),
            palette_bar: None,
            split_pct: 40,
            last_body_width: 0,
            dragging_split: false,
            step_list_area: Rect::default(),
            pending_open_step: None,
            // Edges/blocked set are unknown at construction (no DB handle
            // here); the dispatcher populates them on the first poll via
            // [`Self::sync_outline`]. Until then the outline degrades to the
            // bare step set with synthesized depth 0 — identical rows to the
            // old flat list, so first-frame rendering never regresses.
            outline,
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

    /// Return the preferred plain-run mode for this view's `R` keybinding.
    pub fn preferred_run_mode(&self) -> StreamMode {
        self.preferred_run_mode
    }

    /// Update the preferred plain-run mode when the view is entered from a
    /// `ralph run` auto-start carrying explicit branch/stash behavior. Resume
    /// auto-starts do not affect the `R` binding's semantics.
    pub fn set_preferred_run_mode(&mut self, mode: StreamMode) {
        if let StreamMode::Run { .. } = mode {
            self.preferred_run_mode = mode;
        }
    }

    /// Map a click at `(column, row)` to **the clicked outline row's index
    /// and its step id**, accounting for the persistent chrome (the
    /// `step_list_area` already excludes the top/bottom chrome rows because
    /// it's the post-`chrome::render` body), the `Block::borders(ALL)`
    /// 1-cell frame the `step_list` widget draws, and the scroll-aware
    /// `ListState` offset. Returns `None` for clicks on the border or
    /// outside the list, or before the first draw. Single-line rows, so the
    /// row delta maps 1:1 to the visible item.
    ///
    /// The rendered rows are `outline.visible_rows()` (topological,
    /// focus-filtered) — docs/dag-redesign.md §12.1/§12.2 — *not* the flat
    /// `self.steps` order. So the hit-test must index into
    /// `outline.visible_rows()` (the same index space the cursor lives in),
    /// never `self.steps`, or a non-linear / focused DAG resolves the wrong
    /// step. Returns the outline row index (for moving the outline cursor)
    /// paired with that row's step id (the open-step-detail target).
    fn step_at(&self, column: u16, row: u16) -> Option<(usize, String)> {
        use ratatui::layout::Position;

        let area = self.step_list_area;
        if area.width <= 2 || area.height <= 2 {
            // Nothing rendered yet, or no room inside the border.
            return None;
        }
        // The `step_list` widget wraps the list in a bordered Block, so the
        // actual list content lives one cell inside the outer area.
        let inner = Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width - 2,
            height: area.height - 2,
        };
        if !inner.contains(Position::new(column, row)) {
            return None;
        }
        let visible_row = (row - inner.y) as usize;
        let idx = self.list_state.offset() + visible_row;
        // Hit-test against the *outline's* visible rows (same index space as
        // `outline.cursor()`), respecting the scroll offset + focus filter.
        self.outline
            .visible_rows()
            .get(idx)
            .map(|r| (idx, r.step_id.clone()))
    }

    /// Mouse-event entry point routed from the dispatcher's event loop.
    ///
    /// State-machine precedence: an in-flight divider drag (`dragging_split`)
    /// owns every event until release so a drag that strays over the step
    /// list doesn't get reinterpreted as a row click. Otherwise:
    ///
    /// * a left press within ±1 column of the divider arms a resize drag;
    /// * a left press on a step row moves the **outline** cursor to it
    ///   (then realigns `selected_index`), and a second press on the
    ///   *already-selected* outline row drills into step-detail (the same
    ///   effect as `Enter`) via [`Self::pending_open_step`];
    /// * the scroll wheel moves the **outline** cursor up / down (mirrors
    ///   `k` / `j`), then realigns `selected_index`.
    ///
    /// Both the click and scroll paths route through `self.outline`
    /// (docs/dag-redesign.md §12.1/§12.2) so the rendered highlight — which
    /// follows `outline.cursor()` — tracks the mouse on **any** DAG shape
    /// (linear, non-linear, focused/re-rooted), exactly as the keyboard
    /// path does. For the degenerate no-edge linear case the outline order
    /// equals construction order, so behavior is identical to before.
    ///
    /// Subsequent drags recompute `split_pct` from
    /// `cursor_column / last_body_width` (clamped 20..=80); release clears
    /// the drag flag.
    pub fn handle_mouse(&mut self, event: MouseEvent) {
        use crossterm::event::{MouseButton, MouseEventKind};

        // Scroll wheel works regardless of body width / draw state and is
        // independent of the divider drag — mirror `k` / `j`. The keyboard
        // path moves the *outline* cursor then realigns `selected_index`
        // (plan_detail_input.rs), so the scroll wheel must too: driving the
        // flat navigators would leave the rendered highlight (which follows
        // `outline.cursor()`) frozen on a non-linear / focused DAG
        // (docs/dag-redesign.md §12.1).
        match event.kind {
            MouseEventKind::ScrollUp => {
                self.outline.navigate_up();
                self.realign_selection_to_outline();
                return;
            }
            MouseEventKind::ScrollDown => {
                self.outline.navigate_down();
                self.realign_selection_to_outline();
                return;
            }
            _ => {}
        }

        if self.last_body_width == 0 {
            return;
        }
        let body_width = self.last_body_width as u32;
        let divider_col = (body_width * self.split_pct as u32 / 100) as i32;

        match event.kind {
            // An armed divider drag takes precedence over row hit-testing
            // so a drag that wanders over the step list keeps resizing.
            MouseEventKind::Drag(MouseButton::Left) if self.dragging_split => {
                let pct = (event.column as u32 * 100) / body_width.max(1);
                self.split_pct = pct.clamp(20, 80) as u16;
            }
            MouseEventKind::Down(MouseButton::Left) => {
                let col = event.column as i32;
                if (col - divider_col).abs() <= 1 {
                    self.dragging_split = true;
                    return;
                }
                if let Some((idx, step_id)) = self.step_at(event.column, event.row) {
                    // Compare the clicked row's step id against the outline's
                    // currently-selected step id — same index space (the
                    // rendered highlight is driven by `outline.cursor()`),
                    // so the "second click on the already-selected row enters
                    // it" contract (CLAUDE.md) can't misfire across the flat
                    // vs. outline index spaces on a non-linear/focused DAG.
                    if self.outline.selected_step_id().as_deref() == Some(step_id.as_str()) {
                        // Click on the highlighted row → same as Enter.
                        self.pending_open_step = Some(step_id);
                    } else {
                        // Click on a different row → move the OUTLINE cursor
                        // to it (mirrors the keyboard / `/focus` palette
                        // path; never pokes a divergent flat index), then
                        // realign `selected_index` so the cursor-target
                        // helpers (skip/reset/delete) stay correct.
                        self.outline.set_cursor(idx);
                        self.realign_selection_to_outline();
                    }
                }
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.dragging_split = false;
            }
            _ => {}
        }
    }

    /// Consume a pending mouse-driven step-detail open, if any. The
    /// dispatcher calls this after [`Self::handle_mouse`] and routes the
    /// returned step id exactly like `InputAction::OpenStepDetail`.
    pub fn take_pending_open_step(&mut self) -> Option<String> {
        self.pending_open_step.take()
    }

    /// Replace the cached open-question list (after a DB poll). Drives the
    /// §17 banner + the `A` keybinding's target. Empty input clears the list,
    /// hiding the banner.
    pub fn set_open_questions(&mut self, questions: Vec<crate::storage::OpenQuestion>) {
        self.open_questions = questions;
    }

    /// Replace the cached execution-log rows for a single step (after a DB
    /// poll). Keyed by `Step.id`; the slice is expected to be ordered by
    /// `attempt` ASC (as returned by
    /// [`crate::storage::list_execution_logs_for_step`]). Drives the right
    /// pane's total + per-attempt duration breakdown for terminal steps.
    pub fn set_execution_logs(&mut self, step_id: &str, logs: Vec<ExecutionLog>) {
        self.execution_logs.insert(step_id.to_string(), logs);
    }

    /// Cached execution-log rows for `step_id`, or an empty slice when none
    /// have been loaded / the step has no recorded attempts.
    pub fn execution_logs_for(&self, step_id: &str) -> &[ExecutionLog] {
        self.execution_logs
            .get(step_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Step ID containing the oldest unanswered question, or `None` when no
    /// questions are open. Used by the `A` keybinding (TUI-plan.md §17) to
    /// pick the step-detail target — pressing `A` focuses on that step.
    pub fn oldest_question_step_id(&self) -> Option<String> {
        self.open_questions.first().map(|q| q.step_id.clone())
    }

    /// Update the read-only state. Called by the dispatcher after each
    /// `run_locks` poll. Storing this on the App keeps draw + input
    /// handling thin: both consult the same source of truth without an
    /// extra argument. TUI-plan.md §13.2.
    pub fn set_read_only(&mut self, state: ReadOnly) {
        self.read_only = state;
    }

    // -- Navigation -------------------------------------------------------

    /// Move selection down one step (wraps around).
    pub fn navigate_down(&mut self) {
        if self.steps.is_empty() {
            return;
        }
        self.selected_index = (self.selected_index + 1) % self.steps.len();
    }

    /// Move selection up one step (wraps around).
    pub fn navigate_up(&mut self) {
        if self.steps.is_empty() {
            return;
        }
        if self.selected_index == 0 {
            self.selected_index = self.steps.len() - 1;
        } else {
            self.selected_index -= 1;
        }
    }

    // -- Add step ---------------------------------------------------------

    /// Switch to AddStep input mode targeting the slot *above* the cursor
    /// (`i`). The dispatcher reads `compute_insert_above_sort_key` to place
    /// the new step on confirm.
    pub fn enter_add_mode_above(&mut self) {
        self.input_mode = InputMode::AddStep(AddPosition::Above);
        self.input_buffer.clear();
    }

    /// Switch to AddStep input mode targeting the slot *below* the cursor
    /// (`a`).
    pub fn enter_add_mode_below(&mut self) {
        self.input_mode = InputMode::AddStep(AddPosition::Below);
        self.input_buffer.clear();
    }

    /// Confirm the add-step input. Returns the trimmed title together with
    /// the position the prompt was opened for, or `None` if the input was
    /// blank (cancelling the add).
    pub fn confirm_add_step(&mut self) -> Option<(AddPosition, String)> {
        let position = match self.input_mode {
            InputMode::AddStep(p) => p,
            InputMode::Normal => return None,
        };
        let title = self.input_buffer.trim().to_string();
        self.input_buffer.clear();
        self.input_mode = InputMode::Normal;
        if title.is_empty() {
            None
        } else {
            Some((position, title))
        }
    }

    /// Cancel inline input and return to Normal mode.
    pub fn cancel_input(&mut self) {
        self.input_buffer.clear();
        self.input_mode = InputMode::Normal;
    }

    // -- Skip -------------------------------------------------------------

    /// Request to skip the currently selected step.
    ///
    /// Returns `Some(step_id)` if the selected step is in a skippable status,
    /// or `None` if skipping is not allowed (e.g. step is already complete).
    pub fn request_skip(&self) -> Option<String> {
        let step = self.selected_step()?;
        match step.status {
            StepStatus::Pending
            | StepStatus::InProgress
            | StepStatus::Failed
            | StepStatus::Aborted
            | StepStatus::Blocked => Some(step.id.clone()),
            StepStatus::Complete | StepStatus::Skipped => None,
        }
    }

    // -- Pop --------------------------------------------------------------

    /// Signal the dispatcher to pop this view back to the plan list.
    /// Driven by `←`/`h`/`q` and Ctrl-C per TUI-plan.md §7.
    pub fn request_pop(&mut self) {
        self.should_pop = true;
    }

    // -- Step state updates -----------------------------------------------

    /// Update the status and attempt count for a step by ID.
    pub fn update_step_status(&mut self, step_id: &str, status: StepStatus, attempts: i32) {
        if let Some(step) = self.steps.iter_mut().find(|s| s.id == step_id) {
            step.status = status;
            step.attempts = attempts;
        }
    }

    /// Insert a new step into the list, maintaining sort_key order.
    pub fn insert_step(&mut self, step: Step) {
        let pos = self
            .steps
            .iter()
            .position(|s| s.sort_key > step.sort_key)
            .unwrap_or(self.steps.len());
        self.steps.insert(pos, step);
    }

    /// Find the first step with `InProgress` status.
    pub fn current_in_progress_step(&self) -> Option<&Step> {
        self.steps
            .iter()
            .find(|s| s.status == StepStatus::InProgress)
    }

    // -- Selection --------------------------------------------------------

    /// Toggle multi-select on the highlighted step (`space`).
    /// No-op when the step list is empty.
    pub fn toggle_selection(&mut self) {
        if self.steps.is_empty() {
            return;
        }
        let id = self.steps[self.selected_index].id.clone();
        self.selection.toggle(id);
    }

    /// Clear all selections without touching cursor or input mode.
    pub fn clear_selection(&mut self) {
        self.selection.clear();
    }

    /// Handle `<esc>` in normal mode: clear the selection if any items are
    /// selected, otherwise no-op (Esc does not pop the view; `q` / `h` / `←`
    /// own that). Returns `true` when a selection was cleared.
    pub fn escape_selection(&mut self) -> bool {
        if self.selection.is_empty() {
            false
        } else {
            self.selection.clear();
            true
        }
    }

    // -- Delete -----------------------------------------------------------

    /// Step IDs that the next `d` delete should affect, per TUI-plan.md §7:
    /// selection wins over the cursor target. With at least one step
    /// selected, returns the selection in pick order; otherwise returns just
    /// the highlighted step's ID. Empty when the step list is empty.
    pub fn delete_targets(&self) -> Vec<String> {
        if !self.selection.is_empty() {
            self.selection.as_slice().to_vec()
        } else if let Some(step) = self.selected_step() {
            vec![step.id.clone()]
        } else {
            Vec::new()
        }
    }

    // -- Reset ------------------------------------------------------------

    /// Step ID to reset, or `None` when the step list is empty. Cursor-only
    /// (selection is ignored — reset is a single-step operation).
    pub fn reset_target(&self) -> Option<String> {
        self.selected_step().map(|s| s.id.clone())
    }

    // -- Move (Shift-J / Shift-K) -----------------------------------------

    /// Step ID for Shift-K (move up), or `None` when the cursor cannot move
    /// up (empty list or already at top).
    pub fn move_up_target(&self) -> Option<String> {
        if self.steps.is_empty() || self.selected_index == 0 {
            None
        } else {
            Some(self.steps[self.selected_index].id.clone())
        }
    }

    /// Step ID for Shift-J (move down), or `None` when the cursor cannot
    /// move down (empty list or already at bottom).
    pub fn move_down_target(&self) -> Option<String> {
        if self.steps.is_empty() || self.selected_index + 1 >= self.steps.len() {
            None
        } else {
            Some(self.steps[self.selected_index].id.clone())
        }
    }

    // -- Sort-key computation ---------------------------------------------

    /// Compute the new sort_key for inserting a step *above* the cursor.
    /// Used by the `i` keybinding's confirm handler.
    pub fn compute_insert_above_sort_key(&self) -> Result<String, FracIndexError> {
        if self.steps.is_empty() {
            return Ok(frac_index::initial_key());
        }
        let cur = &self.steps[self.selected_index];
        if self.selected_index == 0 {
            frac_index::key_between("", &cur.sort_key)
        } else {
            let prev = &self.steps[self.selected_index - 1];
            frac_index::key_between(&prev.sort_key, &cur.sort_key)
        }
    }

    /// Compute the new sort_key for appending a step *below* the cursor.
    /// Used by the `a` keybinding's confirm handler.
    pub fn compute_append_below_sort_key(&self) -> Result<String, FracIndexError> {
        if self.steps.is_empty() {
            return Ok(frac_index::initial_key());
        }
        let cur = &self.steps[self.selected_index];
        if self.selected_index + 1 == self.steps.len() {
            frac_index::key_after(&cur.sort_key)
        } else {
            let next = &self.steps[self.selected_index + 1];
            frac_index::key_between(&cur.sort_key, &next.sort_key)
        }
    }

    /// Compute the new sort_key needed to swap the highlighted step **up**
    /// by one position. Returns `Ok(None)` when the cursor is already at
    /// the top (no move possible).
    pub fn compute_move_up_sort_key(&self) -> Result<Option<String>, FracIndexError> {
        if self.steps.is_empty() || self.selected_index == 0 {
            return Ok(None);
        }
        let target_idx = self.selected_index - 1;
        // The moved step's new key needs to land between target_idx-1 and
        // target_idx, so it sorts before the step currently at target_idx.
        let new_key = if target_idx == 0 {
            frac_index::key_between("", &self.steps[target_idx].sort_key)?
        } else {
            frac_index::key_between(
                &self.steps[target_idx - 1].sort_key,
                &self.steps[target_idx].sort_key,
            )?
        };
        Ok(Some(new_key))
    }

    /// Compute the new sort_key needed to swap the highlighted step **down**
    /// by one position. Returns `Ok(None)` when the cursor is already at
    /// the bottom.
    pub fn compute_move_down_sort_key(&self) -> Result<Option<String>, FracIndexError> {
        if self.steps.is_empty() || self.selected_index + 1 >= self.steps.len() {
            return Ok(None);
        }
        let target_idx = self.selected_index + 1;
        // The moved step's new key needs to land between target_idx and
        // target_idx+1 so it sorts after the step currently at target_idx.
        let new_key = if target_idx + 1 == self.steps.len() {
            frac_index::key_after(&self.steps[target_idx].sort_key)?
        } else {
            frac_index::key_between(
                &self.steps[target_idx].sort_key,
                &self.steps[target_idx + 1].sort_key,
            )?
        };
        Ok(Some(new_key))
    }

    // -- Refresh ----------------------------------------------------------

    /// Replace the step list (after a DB refresh) and clamp the cursor
    /// into the new range. Selection is cleared because the next render
    /// would otherwise show stale `[N]` badges for removed steps.
    pub fn refresh_steps(&mut self, steps: Vec<Step>) {
        self.steps = steps;
        self.selection.clear();
        if self.steps.is_empty() {
            self.selected_index = 0;
        } else if self.selected_index >= self.steps.len() {
            self.selected_index = self.steps.len() - 1;
        }
    }

    /// Replace the step list during a periodic DB poll (TUI-plan.md §7
    /// `R`/`S` flow), preserving cursor and selection by step ID. Differs
    /// from `refresh_steps` — which is called after a user-initiated mutation
    /// — in that selection survives unless an explicitly-selected step has
    /// disappeared.
    pub fn sync_steps_from_db(&mut self, steps: Vec<Step>) {
        let cursor_id = self.steps.get(self.selected_index).map(|s| s.id.clone());
        self.steps = steps;

        // Restore cursor by step ID; on disappearance, clamp the index.
        if let Some(id) = cursor_id
            && let Some(idx) = self.steps.iter().position(|s| s.id == id)
        {
            self.selected_index = idx;
        } else if self.steps.is_empty() {
            self.selected_index = 0;
        } else if self.selected_index >= self.steps.len() {
            self.selected_index = self.steps.len() - 1;
        }

        // Drop selection entries for steps that no longer exist.
        let valid: std::collections::HashSet<String> =
            self.steps.iter().map(|s| s.id.clone()).collect();
        self.selection.retain(|id| valid.contains(id));
    }

    /// Refresh the dependency-outline projection from a DB poll
    /// (docs/dag-redesign.md §12.1). `deps_of` is
    /// `storage::list_step_dependency_edges`; `blocked_ids` is the set of
    /// step ids with ≥1 open interruption (drives the derived `Blocked`
    /// overlay — §3.3). The focus stack + cursor are preserved by id via
    /// [`OutlineState::sync`]. After syncing, `selected_index` is realigned
    /// to the visible-row cursor so the existing cursor-target helpers
    /// (`request_skip`, `reset_target`, …) operate on the row the user
    /// actually sees.
    pub fn sync_outline(
        &mut self,
        deps_of: std::collections::HashMap<String, Vec<String>>,
        blocked_ids: std::collections::HashSet<String>,
    ) {
        self.outline.sync(self.steps.clone(), deps_of, blocked_ids);
        self.realign_selection_to_outline();
    }

    /// Mirror the outline's visible-row cursor onto `selected_index` so the
    /// flat-list cursor-target helpers stay correct under the topological /
    /// focused ordering. `selected_index` indexes `self.steps`; the outline
    /// cursor indexes the *visible* rows, so we resolve the cursor's step id
    /// back to its `self.steps` position.
    pub fn realign_selection_to_outline(&mut self) {
        if let Some(step_id) = self.outline.selected_step_id()
            && let Some(idx) = self.steps.iter().position(|s| s.id == step_id)
        {
            self.selected_index = idx;
        }
    }

    /// Resolve the step the cursor-target helpers (skip/reset/delete) should
    /// act on, going through the outline — the source of truth for what the
    /// user actually sees — first. On a focused / re-rooted DAG,
    /// [`Self::realign_selection_to_outline`] can fail to resolve the outline's
    /// selected id back into `self.steps`, leaving `selected_index` stale; so
    /// we resolve `outline.selected_step_id()` against `self.steps` here and
    /// only fall back to the `selected_index` path when the outline yields
    /// nothing (e.g. an empty outline). Mirrors the Enter/open path, which was
    /// already hardened to resolve through the outline.
    fn selected_step(&self) -> Option<&Step> {
        if let Some(id) = self.outline.selected_step_id()
            && let Some(s) = self.steps.iter().find(|s| s.id == id)
        {
            return Some(s);
        }
        self.steps.get(self.selected_index)
    }

    /// True while the outline is re-rooted on a focus step (§12.2). Drives
    /// the breadcrumb suffix and the `Z`/Esc pop affordance.
    pub fn outline_focused(&self) -> bool {
        self.outline.focus_root().is_some()
    }

    // -- Live-run snapshot ------------------------------------------------

    /// Update the cached live-run snapshot. The elapsed timer is derived
    /// from `LiveRun.phase_started_at` / `started_at` (see
    /// [`Self::elapsed_secs`]), so a fresh snapshot here is enough to make
    /// the "Elapsed" display reflect the current step without any
    /// process-local Instant state.
    pub fn update_live_run(&mut self, live: Option<LiveRun>) {
        self.live_run = live;
    }

    /// 1-based step number reported by the live runner, or `None` when no
    /// run is active for this plan. Used by the right-pane banner. Prefers
    /// the NDJSON-derived `subscribed_step_num` over the DB-derived
    /// `live_run.step_num` so the right pane reflects the freshest event
    /// when both sources are populated (TUI-plan.md §13).
    pub fn live_step_num(&self) -> Option<i32> {
        self.subscribed_step_num
            .or_else(|| self.live_run.as_ref().and_then(|l| l.step_num))
    }

    /// Step ID currently executing under the live runner, or `None` when no
    /// run is active for this plan. Used by `draw_step_detail` to gate the
    /// harness/test output tails on whether the cursor is parked on the
    /// running step (step #29).
    pub fn live_step_id(&self) -> Option<String> {
        self.live_run.as_ref().and_then(|l| l.step_id.clone())
    }

    /// True iff a runner is currently bound to this plan — either via an
    /// active TUI-spawned subscription (TUI-plan.md §13) or via a DB-poll
    /// snapshot of `run_locks` (read-only attach, §13.2).
    pub fn is_run_live(&self) -> bool {
        self.subscribed || self.live_run.is_some()
    }

    // -- NDJSON-stream driven state (TUI-plan.md §13) ---------------------

    /// Mark this view as bound to a TUI-spawned [`crate::tui::events::RunSubscription`].
    /// Resets the per-run state (phase, tails, step number, timer
    /// timestamps) so a fresh run doesn't inherit stale chunks or elapsed
    /// readings from a prior subscription.
    pub fn attach_subscription(&mut self) {
        self.subscribed = true;
        self.subscribed_step_num = None;
        self.current_phase = None;
        self.subscribed_started_at = None;
        self.subscribed_phase_started_at = None;
        self.harness_tail.clear();
        self.test_tail.clear();
        self.harness_tail_scroll = 0;
        self.test_tail_scroll = 0;
    }

    /// Release a previously-attached subscription. Called by the dispatcher
    /// when the channel disconnects (subprocess exited) or the user pops
    /// the view.
    pub fn detach_subscription(&mut self) {
        self.subscribed = false;
        self.subscribed_step_num = None;
        self.current_phase = None;
        self.subscribed_started_at = None;
        self.subscribed_phase_started_at = None;
    }

    /// Push a harness-output line onto the tail, evicting from the front
    /// when the buffer exceeds [`TAIL_BUFFER_LINES`]. Bumps `harness_tail_scroll`
    /// so the view stays anchored at whatever the user scrolled to (i.e. the
    /// addition doesn't yank the visible window forward).
    pub fn push_harness_line(&mut self, line: String) {
        push_into_tail(&mut self.harness_tail, line, &mut self.harness_tail_scroll);
    }

    /// Push a deterministic-test-output line onto the test tail. Mirrors
    /// [`Self::push_harness_line`].
    pub fn push_test_line(&mut self, line: String) {
        push_into_tail(&mut self.test_tail, line, &mut self.test_tail_scroll);
    }

    /// Update the cached current phase and per-phase timer base (NDJSON
    /// `phase_changed` event). The timestamp is the wall-clock instant the
    /// runner recorded the phase transition; storing it on the App makes
    /// `elapsed_secs` independent of any DB poll while a subscription is
    /// active.
    pub fn set_current_phase(&mut self, phase: Phase, phase_started_at: DateTime<Utc>) {
        self.current_phase = Some(phase);
        self.subscribed_phase_started_at = Some(phase_started_at);
    }

    /// Anchor the elapsed-timer base to the run's start instant (NDJSON
    /// `run_started` event). Cleared on detach / `plan_complete`. Setting
    /// this before any `phase_changed` lands lets the right-pane "Elapsed"
    /// counter advance during the pre-first-phase window.
    pub fn note_run_started(&mut self, started_at: DateTime<Utc>) {
        self.subscribed = true;
        self.subscribed_started_at = Some(started_at);
    }

    /// Record that a `step_started` event just arrived: bring the run-live
    /// state online if the subscription's first event preceded the DB-side
    /// row, and latch the step number. The elapsed timer is derived from
    /// the persisted `LiveRun` timestamps, so no process-local clock state
    /// needs resetting here.
    pub fn note_step_started(&mut self, step_id: &str) {
        self.subscribed = true;
        self.subscribed_step_num = self
            .steps
            .iter()
            .position(|s| s.id == step_id)
            .map(|i| (i + 1) as i32);
    }

    /// Record that a `step_finished` event arrived. The timer keeps running
    /// because the next step typically starts within milliseconds; if no
    /// further `step_started` lands, [`Self::note_run_finished`] (driven by
    /// `plan_complete` / `summary`) clears it.
    pub fn note_step_finished(&mut self, _step_id: &str) {
        // Intentionally minimal: the rolling timer is reset on the next
        // `step_started`, and we don't want to flicker the right pane to a
        // "no run" state between consecutive step_started events.
    }

    /// Record that the run as a whole completed (`plan_complete` or
    /// `summary` event). Detaches the subscription state so the right pane
    /// returns to the static idle layout.
    pub fn note_run_finished(&mut self) {
        self.detach_subscription();
    }

    /// Current phase as observed via the NDJSON stream (`phase_changed`),
    /// falling back to the DB-poll snapshot when the subscription hasn't
    /// emitted one yet.
    pub fn current_phase(&self) -> Option<Phase> {
        self.current_phase
            .or_else(|| self.live_run.as_ref().and_then(|l| l.phase))
    }

    /// Read-only view of the harness tail (oldest first). Used by tests
    /// and by the renderer to compute the visible window.
    pub fn harness_tail_lines(&self) -> Vec<String> {
        self.harness_tail.iter().cloned().collect()
    }

    /// Read-only view of the test tail (oldest first).
    pub fn test_tail_lines(&self) -> Vec<String> {
        self.test_tail.iter().cloned().collect()
    }

    /// Compute the visible slice of the harness tail given the right-pane
    /// height in lines. Honors `harness_tail_scroll` so the user can pause
    /// the auto-scroll and inspect older output. Returns oldest-first.
    pub fn visible_harness_tail(&self, visible: usize) -> Vec<String> {
        visible_window(&self.harness_tail, visible, self.harness_tail_scroll)
    }

    /// Compute the visible slice of the test tail. Same semantics as
    /// [`Self::visible_harness_tail`].
    pub fn visible_test_tail(&self, visible: usize) -> Vec<String> {
        visible_window(&self.test_tail, visible, self.test_tail_scroll)
    }

    /// Scroll the tails one line **older** (J/K maps "older" to one of
    /// `J`/`K` depending on the user's mental model — see `plan_detail_input`).
    /// Bumps both tails together so the user only has to remember one shortcut.
    pub fn scroll_tails_older(&mut self) {
        self.harness_tail_scroll =
            (self.harness_tail_scroll + 1).min(self.harness_tail.len().saturating_sub(1));
        self.test_tail_scroll =
            (self.test_tail_scroll + 1).min(self.test_tail.len().saturating_sub(1));
    }

    /// Scroll the tails one line **newer**. Saturates at 0 so we don't
    /// underflow.
    pub fn scroll_tails_newer(&mut self) {
        self.harness_tail_scroll = self.harness_tail_scroll.saturating_sub(1);
        self.test_tail_scroll = self.test_tail_scroll.saturating_sub(1);
    }

    /// True when at least one of the tails has buffered any output. The
    /// J/K input handler consults this to decide between "scroll tails"
    /// and "move step" semantics: with no chunks at all, the user is
    /// clearly trying to reorder steps, not scroll an empty pane.
    pub fn has_tail_output(&self) -> bool {
        !self.harness_tail.is_empty() || !self.test_tail.is_empty()
    }

    // -- Timer ------------------------------------------------------------

    /// Seconds elapsed in the current run phase, derived from the persisted
    /// `LiveRun` row so re-entry from the plan list (which constructs a
    /// fresh `PlanDetailApp`) preserves the accumulated time. Prefers
    /// `phase_started_at` (per-step granularity) and falls back to
    /// `started_at` (whole run) when no phase timestamp has been written
    /// yet. Returns `0.0` when no live run is bound or when the timestamp
    /// fails to parse — both treated as "no useful elapsed to show".
    /// Negative durations from clock skew are clamped to `0.0`.
    pub fn elapsed_secs(&self) -> f64 {
        // Priority: NDJSON-derived timestamps win when present (the
        // subscription stream gives us the freshest possible base instant
        // without a DB roundtrip), falling back to the `LiveRun` snapshot
        // for the read-only attach path. Within each source we prefer
        // phase-start over run-start so the timer reflects time-in-phase
        // — matching the per-step granularity users expect.
        let base = self
            .subscribed_phase_started_at
            .or(self.subscribed_started_at)
            .or_else(|| {
                self.live_run
                    .as_ref()
                    .and_then(|l| l.phase_started_at.as_deref())
                    .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            })
            .or_else(|| {
                self.live_run
                    .as_ref()
                    .and_then(|l| l.started_at.parse::<DateTime<Utc>>().ok())
            });
        match base {
            Some(t) => Utc::now().signed_duration_since(t).num_seconds().max(0) as f64,
            None => 0.0,
        }
    }

    // -- Display helpers --------------------------------------------------

    /// Return a status indicator string for a step status.
    pub fn status_indicator(status: StepStatus) -> &'static str {
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
}

// ---------------------------------------------------------------------------
// Tail-buffer helpers
// ---------------------------------------------------------------------------

/// Append `line` to `tail`, evicting from the front when the buffer would
/// exceed [`TAIL_BUFFER_LINES`]. If the user has scrolled back (`*scroll > 0`)
/// the offset is bumped so the visible window stays anchored at the same
/// line — otherwise an arriving chunk would yank the view forward.
fn push_into_tail(tail: &mut VecDeque<String>, line: String, scroll: &mut usize) {
    tail.push_back(line);
    while tail.len() > TAIL_BUFFER_LINES {
        tail.pop_front();
        // The buffer shrunk by one from the front; if the user is parked
        // mid-buffer we need to consume the same one from `scroll` to keep
        // the visible window pinned.
        if *scroll > 0 {
            *scroll -= 1;
        }
    }
    if *scroll > 0 {
        // New chunk pushed in at the back; preserve the anchor by bumping
        // the offset so the same older window stays visible.
        let max_scroll = tail.len().saturating_sub(1);
        if *scroll < max_scroll {
            *scroll += 1;
        }
    }
}

/// Return the visible slice of a tail buffer (oldest-first), honoring the
/// scroll offset. With `scroll = 0` the slice ends at the newest line; with
/// `scroll = N` the slice ends N lines earlier. `visible` caps the slice
/// length; the `TAIL_VISIBLE_LINES` default is wired in by callers.
fn visible_window(tail: &VecDeque<String>, visible: usize, scroll: usize) -> Vec<String> {
    if tail.is_empty() || visible == 0 {
        return Vec::new();
    }
    let take = visible.min(tail.len());
    let scroll = scroll.min(tail.len().saturating_sub(1));
    let end = tail.len() - scroll;
    let start = end.saturating_sub(take);
    tail.iter().skip(start).take(end - start).cloned().collect()
}

const _: () = {
    // Sanity check: we expect the visible window default to be smaller than
    // the buffered total so users can scroll beyond the on-screen tail.
    assert!(TAIL_VISIBLE_LINES <= TAIL_BUFFER_LINES);
};

#[cfg(test)]
mod tests {
    use super::{AddPosition, InputMode, PlanDetailApp};
    use crate::config::Config;
    use crate::plan::{Plan, PlanStatus, Step, StepStatus};
    use crate::run_lock::LiveRun;
    use chrono::Utc;

    fn make_plan() -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "test-plan".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "feat/test".to_string(),
            description: "A test plan".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec!["cargo test".to_string()],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            review_enabled: None,
            max_review_corrections: None,
        }
    }

    fn make_steps(n: usize) -> Vec<Step> {
        (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                short_id: String::new(),
                plan_id: "p1".to_string(),
                // Use a0, a1, a2, ... so we have valid frac-index keys with
                // room between them (a0 < a1 admits a midpoint via key_between).
                sort_key: format!("a{i}"),
                title: format!("Step {}", i + 1),
                description: format!("Description {}", i + 1),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                status: if i == 0 {
                    StepStatus::Complete
                } else if i == 1 {
                    StepStatus::InProgress
                } else {
                    StepStatus::Pending
                },
                attempts: if i == 1 { 2 } else { 0 },
                max_retries: Some(3),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                model: None,
                skipped_reason: None,
                change_policy: crate::plan::ChangePolicy::Required,
                tags: vec![],
                review_enabled: None,
                review_status: None,
                corrects_step_id: None,
            })
            .collect()
    }

    #[test]
    fn test_app_creation() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan.clone(), steps.clone(), &Config::default());

        assert_eq!(app.plan.slug, "test-plan");
        assert_eq!(app.steps.len(), 3);
        assert_eq!(app.selected_index, 0);
        assert!(!app.should_pop);
        assert!(matches!(app.input_mode, InputMode::Normal));
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_navigate_down() {
        let plan = make_plan();
        let steps = make_steps(5);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        assert_eq!(app.selected_index, 0);
        app.navigate_down();
        assert_eq!(app.selected_index, 1);
        app.navigate_down();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn test_navigate_down_wraps() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.selected_index = 2;
        app.navigate_down();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_navigate_up() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.selected_index = 2;
        app.navigate_up();
        assert_eq!(app.selected_index, 1);
        app.navigate_up();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_navigate_up_wraps() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        assert_eq!(app.selected_index, 0);
        app.navigate_up();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn test_navigate_empty_steps() {
        let plan = make_plan();
        let mut app = PlanDetailApp::new(plan, vec![], &Config::default());

        app.navigate_down();
        assert_eq!(app.selected_index, 0);
        app.navigate_up();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_enter_add_mode_above() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode_above();
        assert_eq!(app.input_mode, InputMode::AddStep(AddPosition::Above));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_enter_add_mode_below() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode_below();
        assert_eq!(app.input_mode, InputMode::AddStep(AddPosition::Below));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_confirm_add_step_above() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.selected_index = 1;
        app.enter_add_mode_above();
        app.input_buffer = "New step title".to_string();
        let confirmed = app.confirm_add_step();

        assert_eq!(
            confirmed,
            Some((AddPosition::Above, "New step title".to_string()))
        );
        assert_eq!(app.input_mode, InputMode::Normal);
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_confirm_add_step_below() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode_below();
        app.input_buffer = "Another title".to_string();
        let confirmed = app.confirm_add_step();
        assert_eq!(
            confirmed,
            Some((AddPosition::Below, "Another title".to_string()))
        );
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_confirm_add_step_empty_title() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode_above();
        app.input_buffer = "   ".to_string();
        assert!(app.confirm_add_step().is_none());
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_confirm_add_step_in_normal_mode_returns_none() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        // No enter_add_mode_* call — confirm should bail.
        assert!(app.confirm_add_step().is_none());
    }

    #[test]
    fn test_cancel_add_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode_above();
        app.input_buffer = "Some text".to_string();
        app.cancel_input();
        assert_eq!(app.input_mode, InputMode::Normal);
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_skip_current_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        // Select the in_progress step via the outline cursor (the source of
        // truth the cursor-target helpers resolve through).
        app.outline.set_cursor(1);
        app.realign_selection_to_outline();
        let result = app.request_skip();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "s1"); // step index 1 = id "s1"
    }

    #[test]
    fn test_skip_complete_step_rejected() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        // Select the complete step via the outline cursor.
        app.outline.set_cursor(0);
        app.realign_selection_to_outline();
        let result = app.request_skip();
        assert!(result.is_none()); // Can't skip a completed step
    }

    #[test]
    fn test_request_pop() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        assert!(!app.should_pop);
        app.request_pop();
        assert!(app.should_pop);
    }

    #[test]
    fn test_update_step_status() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.update_step_status("s2", StepStatus::InProgress, 1);
        assert_eq!(app.steps[2].status, StepStatus::InProgress);
        assert_eq!(app.steps[2].attempts, 1);
    }

    #[test]
    fn test_update_step_status_unknown_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        // Should be a no-op
        app.update_step_status("unknown", StepStatus::Complete, 1);
        // No panic, no change
        assert_eq!(app.steps.len(), 3);
    }

    #[test]
    fn test_current_in_progress_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());

        let current = app.current_in_progress_step();
        assert!(current.is_some());
        let step = current.unwrap();
        assert_eq!(step.id, "s1");
        assert_eq!(step.status, StepStatus::InProgress);
    }

    #[test]
    fn test_no_in_progress_step() {
        let plan = make_plan();
        let mut steps = make_steps(3);
        steps[1].status = StepStatus::Pending;
        let app = PlanDetailApp::new(plan, steps, &Config::default());

        let current = app.current_in_progress_step();
        assert!(current.is_none());
    }

    #[test]
    fn test_insert_step_at_position() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        let new_step = Step {
            id: "s_new".to_string(),
            short_id: String::new(),
            plan_id: "p1".to_string(),
            sort_key: "a0V".to_string(),
            title: "Inserted step".to_string(),
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
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        };

        app.insert_step(new_step);
        assert_eq!(app.steps.len(), 4);

        // Verify still sorted by sort_key
        for i in 0..app.steps.len() - 1 {
            assert!(
                app.steps[i].sort_key < app.steps[i + 1].sort_key,
                "sort order broken at {}",
                i
            );
        }
    }

    #[test]
    fn test_elapsed_secs_no_live_run_returns_zero() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());

        assert_eq!(app.elapsed_secs(), 0.0);
    }

    #[test]
    fn test_elapsed_secs_uses_phase_started_at() {
        // (a) Construct an App, push a LiveRun whose phase_started_at is
        // 90 seconds in the past, and assert elapsed_secs ≈ 90 — derived
        // from the persisted timestamp, not a process-local Instant.
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        let mut live = make_live_run("test-plan", Some("s1"), Some(2));
        let started = Utc::now() - chrono::Duration::seconds(90);
        live.phase_started_at = Some(started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        app.update_live_run(Some(live));

        let elapsed = app.elapsed_secs();
        assert!(
            (89.0..=92.0).contains(&elapsed),
            "expected ~90s elapsed, got {elapsed}"
        );
    }

    #[test]
    fn test_elapsed_secs_survives_view_reentry() {
        // (b) Re-entry preservation: navigating from plan-detail to root
        // and back constructs a fresh PlanDetailApp. Two apps fed the same
        // LiveRun (same phase_started_at) must report the same elapsed
        // value — there is no per-instance Instant to reset.
        let mut live = make_live_run("test-plan", Some("s1"), Some(2));
        let started = Utc::now() - chrono::Duration::seconds(90);
        live.phase_started_at = Some(started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));

        let mut first = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        first.update_live_run(Some(live.clone()));
        let first_elapsed = first.elapsed_secs();
        assert!(
            (89.0..=92.0).contains(&first_elapsed),
            "first view should observe ~90s elapsed, got {first_elapsed}"
        );

        let mut second = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        second.update_live_run(Some(live));
        let second_elapsed = second.elapsed_secs();
        assert!(
            (89.0..=92.0).contains(&second_elapsed),
            "fresh view should observe the same ~90s elapsed, got {second_elapsed}"
        );
        // Crucially, the second instance must NOT have reset to ~0.
        assert!(
            second_elapsed > 60.0,
            "re-entry must preserve elapsed time across instances"
        );
    }

    #[test]
    fn test_elapsed_secs_falls_back_to_started_at() {
        // (c) When phase_started_at is None, fall back to started_at —
        // covers the window between run start and the first phase
        // transition (where the per-phase timestamp hasn't been written
        // yet).
        let mut live = make_live_run("test-plan", Some("s1"), Some(2));
        let started = Utc::now() - chrono::Duration::seconds(45);
        live.phase_started_at = None;
        live.started_at = started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.update_live_run(Some(live));

        let elapsed = app.elapsed_secs();
        assert!(
            (44.0..=47.0).contains(&elapsed),
            "expected ~45s elapsed via started_at fallback, got {elapsed}"
        );
    }

    #[test]
    fn test_elapsed_secs_clamps_negative_duration_to_zero() {
        // (d) Clock skew defense: a phase_started_at that is in the future
        // (perhaps because the runner host's clock is ahead of ours) must
        // not surface as a negative elapsed — the f64 cast would wrap and
        // the renderer would print nonsense.
        let mut live = make_live_run("test-plan", Some("s1"), Some(2));
        let future = Utc::now() + chrono::Duration::seconds(120);
        live.phase_started_at = Some(future.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));

        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.update_live_run(Some(live));

        assert_eq!(app.elapsed_secs(), 0.0);
    }

    #[test]
    fn test_status_indicator() {
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::Pending), "○");
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::InProgress), "▶");
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::Complete), "✔");
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::Failed), "✘");
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::Skipped), "⊘");
        assert_eq!(PlanDetailApp::status_indicator(StepStatus::Aborted), "⊘");
    }

    #[test]
    fn test_input_mode_variants() {
        let _normal = InputMode::Normal;
        let _add_above = InputMode::AddStep(AddPosition::Above);
        let _add_below = InputMode::AddStep(AddPosition::Below);
    }

    // -- Selection --------------------------------------------------------

    #[test]
    fn test_toggle_selection_uses_step_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.selected_index = 1;
        app.toggle_selection();
        assert!(app.selection.is_selected(&"s1".to_string()));
        assert_eq!(app.selection.len(), 1);

        // Toggling again clears it.
        app.toggle_selection();
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_toggle_selection_empty_steps_is_noop() {
        let plan = make_plan();
        let mut app = PlanDetailApp::new(plan, vec![], &Config::default());
        app.toggle_selection();
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_clear_selection() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.toggle_selection();
        app.navigate_down();
        app.toggle_selection();
        assert_eq!(app.selection.len(), 2);
        app.clear_selection();
        assert!(app.selection.is_empty());
    }

    #[test]
    fn test_escape_selection_clears_when_present() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.toggle_selection();
        let consumed = app.escape_selection();
        assert!(consumed, "esc with selection should be consumed");
        assert!(app.selection.is_empty());
        assert!(!app.should_pop, "esc must not pop the view");
    }

    #[test]
    fn test_escape_selection_noop_when_empty() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        let consumed = app.escape_selection();
        assert!(!consumed);
        assert!(!app.should_pop);
    }

    // -- Delete targets ---------------------------------------------------

    #[test]
    fn test_delete_targets_returns_cursor_when_no_selection() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.outline.set_cursor(2);
        app.realign_selection_to_outline();
        assert_eq!(app.delete_targets(), vec!["s2".to_string()]);
    }

    #[test]
    fn test_delete_targets_uses_selection_in_pick_order() {
        let plan = make_plan();
        let steps = make_steps(4);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2;
        app.toggle_selection();
        app.selected_index = 0;
        app.toggle_selection();
        app.selected_index = 3;
        app.toggle_selection();
        // Cursor on s3 should NOT short-circuit selection.
        assert_eq!(
            app.delete_targets(),
            vec!["s2".to_string(), "s0".to_string(), "s3".to_string()]
        );
    }

    #[test]
    fn test_delete_targets_empty_steps() {
        let plan = make_plan();
        let app = PlanDetailApp::new(plan, vec![], &Config::default());
        assert!(app.delete_targets().is_empty());
    }

    // -- Reset target -----------------------------------------------------

    #[test]
    fn test_reset_target_returns_cursor_step_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.outline.set_cursor(0);
        app.realign_selection_to_outline();
        assert_eq!(app.reset_target(), Some("s0".to_string()));
        app.outline.set_cursor(2);
        app.realign_selection_to_outline();
        assert_eq!(app.reset_target(), Some("s2".to_string()));
    }

    #[test]
    fn test_reset_target_empty_steps() {
        let plan = make_plan();
        let app = PlanDetailApp::new(plan, vec![], &Config::default());
        assert!(app.reset_target().is_none());
    }

    // -- Move targets -----------------------------------------------------

    #[test]
    fn test_move_up_target_at_top_returns_none() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());
        assert_eq!(app.selected_index, 0);
        assert!(app.move_up_target().is_none());
    }

    #[test]
    fn test_move_up_target_returns_step_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1;
        assert_eq!(app.move_up_target(), Some("s1".to_string()));
    }

    #[test]
    fn test_move_down_target_at_bottom_returns_none() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2;
        assert!(app.move_down_target().is_none());
    }

    #[test]
    fn test_move_down_target_returns_step_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1;
        assert_eq!(app.move_down_target(), Some("s1".to_string()));
    }

    #[test]
    fn test_move_targets_empty_steps() {
        let plan = make_plan();
        let app = PlanDetailApp::new(plan, vec![], &Config::default());
        assert!(app.move_up_target().is_none());
        assert!(app.move_down_target().is_none());
    }

    // -- Sort-key computation ---------------------------------------------

    #[test]
    fn test_compute_insert_above_sort_key_at_top() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());
        // selected_index=0, cursor on first step (sort_key="a0").
        let key = app
            .compute_insert_above_sort_key()
            .expect("insert above for cursor at top");
        // Must sort strictly before "a0".
        assert!(key.as_str() < "a0", "got {key}");
    }

    #[test]
    fn test_compute_insert_above_sort_key_in_middle() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1; // between a0 and a1
        let key = app.compute_insert_above_sort_key().unwrap();
        assert!(key.as_str() > "a0" && key.as_str() < "a1", "got {key}");
    }

    #[test]
    fn test_compute_insert_above_sort_key_empty_steps() {
        let plan = make_plan();
        let app = PlanDetailApp::new(plan, vec![], &Config::default());
        let key = app.compute_insert_above_sort_key().unwrap();
        assert_eq!(key, crate::frac_index::initial_key());
    }

    #[test]
    fn test_compute_append_below_sort_key_at_bottom() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2; // last step is "a2"
        let key = app.compute_append_below_sort_key().unwrap();
        assert!(key.as_str() > "a2", "got {key}");
    }

    #[test]
    fn test_compute_append_below_sort_key_in_middle() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 0; // between a0 and a1
        let key = app.compute_append_below_sort_key().unwrap();
        assert!(key.as_str() > "a0" && key.as_str() < "a1", "got {key}");
    }

    #[test]
    fn test_compute_append_below_sort_key_empty_steps() {
        let plan = make_plan();
        let app = PlanDetailApp::new(plan, vec![], &Config::default());
        let key = app.compute_append_below_sort_key().unwrap();
        assert_eq!(key, crate::frac_index::initial_key());
    }

    #[test]
    fn test_compute_move_up_sort_key_at_top_is_none() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());
        assert!(app.compute_move_up_sort_key().unwrap().is_none());
    }

    #[test]
    fn test_compute_move_up_sort_key_swaps_into_top_slot() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1;
        let key = app
            .compute_move_up_sort_key()
            .unwrap()
            .expect("move up should yield a key");
        // Should sort strictly before "a0" (the current top step).
        assert!(key.as_str() < "a0", "got {key}");
    }

    #[test]
    fn test_compute_move_up_sort_key_in_middle() {
        let plan = make_plan();
        let steps = make_steps(4);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2; // step a2 swaps with a1 → land between a0 and a1
        let key = app.compute_move_up_sort_key().unwrap().unwrap();
        assert!(key.as_str() > "a0" && key.as_str() < "a1", "got {key}");
    }

    #[test]
    fn test_compute_move_down_sort_key_at_bottom_is_none() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2;
        assert!(app.compute_move_down_sort_key().unwrap().is_none());
    }

    #[test]
    fn test_compute_move_down_sort_key_swaps_into_bottom_slot() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1; // a1 swaps with a2 → land after a2
        let key = app
            .compute_move_down_sort_key()
            .unwrap()
            .expect("move down should yield a key");
        assert!(key.as_str() > "a2", "got {key}");
    }

    #[test]
    fn test_compute_move_down_sort_key_in_middle() {
        let plan = make_plan();
        let steps = make_steps(4);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1; // a1 swaps with a2 → land between a2 and a3
        let key = app.compute_move_down_sort_key().unwrap().unwrap();
        assert!(key.as_str() > "a2" && key.as_str() < "a3", "got {key}");
    }

    // -- Refresh ----------------------------------------------------------

    #[test]
    fn test_refresh_steps_clamps_cursor_into_new_range() {
        let plan = make_plan();
        let steps = make_steps(5);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 4;
        app.refresh_steps(make_steps(2));
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn test_refresh_steps_resets_cursor_when_empty() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2;
        app.refresh_steps(vec![]);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_refresh_steps_clears_selection() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.toggle_selection();
        assert_eq!(app.selection.len(), 1);
        app.refresh_steps(make_steps(3));
        assert!(app.selection.is_empty());
    }

    // -- sync_steps_from_db -----------------------------------------------

    #[test]
    fn test_sync_steps_preserves_cursor_by_id() {
        // Cursor on s2; refresh shuffles position but the cursor follows
        // s2's new index instead of clamping to a numeric position.
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 2;

        let mut new_steps = make_steps(3);
        new_steps.reverse();
        let new_index_of_s2 = new_steps.iter().position(|s| s.id == "s2").unwrap();
        app.sync_steps_from_db(new_steps);
        assert_eq!(app.selected_index, new_index_of_s2);
    }

    #[test]
    fn test_sync_steps_preserves_selection_when_steps_survive() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.toggle_selection(); // selects s0
        app.navigate_down();
        app.toggle_selection(); // selects s1
        assert_eq!(app.selection.len(), 2);

        // Same set of steps from DB → selection retained.
        app.sync_steps_from_db(make_steps(3));
        assert_eq!(app.selection.len(), 2);
        assert!(app.selection.is_selected(&"s0".to_string()));
        assert!(app.selection.is_selected(&"s1".to_string()));
    }

    #[test]
    fn test_sync_steps_drops_selection_for_disappeared_step() {
        let plan = make_plan();
        let steps = make_steps(4);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 1;
        app.toggle_selection(); // s1
        app.selected_index = 3;
        app.toggle_selection(); // s3
        assert_eq!(app.selection.len(), 2);

        // Only s0..s2 remain — s3 disappeared.
        app.sync_steps_from_db(make_steps(3));
        assert_eq!(app.selection.len(), 1);
        assert!(app.selection.is_selected(&"s1".to_string()));
        assert!(!app.selection.is_selected(&"s3".to_string()));
    }

    #[test]
    fn test_sync_steps_clamps_cursor_when_target_disappears() {
        let plan = make_plan();
        let steps = make_steps(5);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.selected_index = 4;
        app.sync_steps_from_db(make_steps(2));
        assert_eq!(app.selected_index, 1);
    }

    // -- live_run snapshot ------------------------------------------------

    fn make_live_run(plan_slug: &str, step_id: Option<&str>, step_num: Option<i32>) -> LiveRun {
        LiveRun {
            project: "/proj".to_string(),
            pid: 1234,
            pid_start_token: None,
            plan_id: Some("p1".to_string()),
            plan_slug: Some(plan_slug.to_string()),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            step_id: step_id.map(|s| s.to_string()),
            step_num,
            attempt: Some(1),
            max_attempts: Some(4),
            phase: Some(crate::plan::Phase::Harness),
            phase_started_at: None,
            current_command: None,
            execution_log_id: None,
            child_pid: None,
            child_start_token: None,
            updated_at: None,
            source_branch: None,
            stash_sha: None,
            parent_tui_pid: None,
        }
    }

    #[test]
    fn test_update_live_run_marks_run_live_when_step_appears() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        assert!(!app.is_run_live());

        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        assert!(app.is_run_live());
        assert_eq!(app.live_step_num(), Some(2));
    }

    #[test]
    fn test_update_live_run_clears_run_state_when_run_ends() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        assert!(app.is_run_live());

        app.update_live_run(None);
        assert!(!app.is_run_live());
        assert_eq!(app.live_step_num(), None);
        assert_eq!(app.elapsed_secs(), 0.0);
    }

    #[test]
    fn test_update_live_run_reflects_new_phase_timestamp_on_step_change() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        let mut first = make_live_run("test-plan", Some("s1"), Some(2));
        let first_phase = Utc::now() - chrono::Duration::seconds(120);
        first.phase_started_at =
            Some(first_phase.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        app.update_live_run(Some(first));
        let elapsed_before = app.elapsed_secs();
        assert!(
            elapsed_before > 60.0,
            "first phase should report >60s elapsed, got {elapsed_before}"
        );

        // A new step arrives with a fresh phase_started_at — elapsed should
        // drop to roughly the new phase's age.
        let mut second = make_live_run("test-plan", Some("s2"), Some(3));
        let second_phase = Utc::now() - chrono::Duration::seconds(2);
        second.phase_started_at =
            Some(second_phase.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        app.update_live_run(Some(second));
        let elapsed_after = app.elapsed_secs();
        assert!(
            elapsed_after < 10.0,
            "fresh phase should reset elapsed to ~2s, got {elapsed_after}"
        );
        assert_eq!(app.live_step_num(), Some(3));
    }

    // -- NDJSON-stream-driven state (TUI-plan.md §13) ---------------------

    #[test]
    fn test_attach_subscription_marks_run_live() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        assert!(!app.is_run_live());
        app.attach_subscription();
        assert!(app.is_run_live());
        assert_eq!(app.current_phase(), None);
        assert!(app.harness_tail.is_empty());
        assert!(app.test_tail.is_empty());
    }

    #[test]
    fn set_preferred_run_mode_updates_r_binding_mode() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        assert_eq!(
            app.preferred_run_mode(),
            crate::tui::events::StreamMode::Run {
                current_branch: false,
                no_auto_stash: false,
            }
        );

        app.set_preferred_run_mode(crate::tui::events::StreamMode::Run {
            current_branch: true,
            no_auto_stash: true,
        });
        assert_eq!(
            app.preferred_run_mode(),
            crate::tui::events::StreamMode::Run {
                current_branch: true,
                no_auto_stash: true,
            }
        );

        app.set_preferred_run_mode(crate::tui::events::StreamMode::Resume);
        assert_eq!(
            app.preferred_run_mode(),
            crate::tui::events::StreamMode::Run {
                current_branch: true,
                no_auto_stash: true,
            }
        );
    }

    #[test]
    fn test_attach_subscription_resets_prior_tails_and_phase() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.subscribed = true;
        app.harness_tail.push_back("stale stdout".into());
        app.test_tail.push_back("stale tests".into());
        app.set_current_phase(crate::plan::Phase::Tests, Utc::now());
        app.harness_tail_scroll = 5;

        app.attach_subscription();
        assert!(app.harness_tail.is_empty());
        assert!(app.test_tail.is_empty());
        assert_eq!(app.harness_tail_scroll, 0);
        assert_eq!(app.test_tail_scroll, 0);
        assert_eq!(app.current_phase, None);
    }

    #[test]
    fn test_detach_subscription_clears_run_live_state() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.attach_subscription();
        app.note_step_started("s1");
        app.set_current_phase(crate::plan::Phase::Harness, Utc::now());
        assert!(app.is_run_live());
        app.detach_subscription();
        assert!(!app.is_run_live());
        assert_eq!(app.elapsed_secs(), 0.0);
        assert_eq!(app.current_phase, None);
        assert_eq!(app.subscribed_step_num, None);
    }

    #[test]
    fn test_push_harness_line_caps_at_buffer_lines() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        for i in 0..(super::TAIL_BUFFER_LINES + 50) {
            app.push_harness_line(format!("line {i}"));
        }
        assert_eq!(app.harness_tail.len(), super::TAIL_BUFFER_LINES);
        let oldest = app.harness_tail.front().unwrap();
        // Oldest 50 lines were evicted.
        assert_eq!(oldest, "line 50");
    }

    #[test]
    fn test_visible_harness_tail_returns_newest_window() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        for i in 0..30 {
            app.push_harness_line(format!("line {i}"));
        }
        let visible = app.visible_harness_tail(5);
        assert_eq!(
            visible,
            vec![
                "line 25".to_string(),
                "line 26".to_string(),
                "line 27".to_string(),
                "line 28".to_string(),
                "line 29".to_string(),
            ]
        );
    }

    #[test]
    fn test_scroll_tails_older_then_newer_round_trip() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        for i in 0..10 {
            app.push_harness_line(format!("h{i}"));
            app.push_test_line(format!("t{i}"));
        }
        // Scroll older: window shifts back by 1 line.
        app.scroll_tails_older();
        let visible = app.visible_harness_tail(3);
        assert_eq!(visible.last().map(String::as_str), Some("h8"));
        // Scrolling newer brings us back to the newest line.
        app.scroll_tails_newer();
        let visible = app.visible_harness_tail(3);
        assert_eq!(visible.last().map(String::as_str), Some("h9"));
    }

    #[test]
    fn test_scroll_tails_anchored_when_new_chunk_arrives() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        for i in 0..5 {
            app.push_harness_line(format!("h{i}"));
        }
        app.scroll_tails_older();
        app.scroll_tails_older();
        // Anchored 2 lines back from the newest.
        let before = app.visible_harness_tail(3);
        assert_eq!(before.last().map(String::as_str), Some("h2"));

        app.push_harness_line("h5".into());
        // The anchor should still point to "h2" — the new chunk did not
        // yank the visible window forward.
        let after = app.visible_harness_tail(3);
        assert_eq!(after.last().map(String::as_str), Some("h2"));
    }

    #[test]
    fn test_note_step_started_latches_step_num_from_step_list() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.note_step_started("s1");
        // s1 is the second step → 1-based num = 2.
        assert_eq!(app.subscribed_step_num, Some(2));
        assert_eq!(app.live_step_num(), Some(2));
    }

    #[test]
    fn test_note_step_started_unknown_id_clears_step_num() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.subscribed_step_num = Some(7);
        app.note_step_started("does-not-exist");
        assert_eq!(app.subscribed_step_num, None);
    }

    #[test]
    fn test_current_phase_prefers_subscription_over_db_snapshot() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        assert_eq!(app.current_phase(), Some(crate::plan::Phase::Harness));
        // A subscription-derived phase wins.
        app.set_current_phase(crate::plan::Phase::Tests, Utc::now());
        assert_eq!(app.current_phase(), Some(crate::plan::Phase::Tests));
    }

    #[test]
    fn test_has_tail_output_false_until_first_chunk() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        app.attach_subscription();
        assert!(!app.has_tail_output());
        app.push_harness_line("first".into());
        assert!(app.has_tail_output());
    }

    // -- Read-only attach lockdown (TUI-plan.md §13.2) -------------------

    #[test]
    fn test_read_only_default_is_editable() {
        let app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        assert!(!app.read_only.is_locked());
    }

    #[test]
    fn test_set_read_only_records_state() {
        use crate::tui::read_only::ReadOnly;
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        app.set_read_only(ReadOnly::Locked { pid: 4242 });
        assert!(app.read_only.is_locked());
        assert_eq!(app.read_only.pid(), Some(4242));
        app.set_read_only(ReadOnly::Editable);
        assert!(!app.read_only.is_locked());
    }

    // -- Palette (TUI-plan.md §9) ---------------------------------------

    #[test]
    fn palette_default_inactive() {
        let app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        assert!(!app.palette_active());
        assert!(app.palette_bar.is_none());
    }

    #[test]
    fn palette_open_records_prefix() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
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
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        app.open_palette(':');
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
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
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
        let mut app = PlanDetailApp::new(make_plan(), make_steps(1), &Config::default());
        app.open_palette('/');
        let bar = app.palette_bar.as_mut().unwrap();
        for c in "step skip 3".chars() {
            let _ = bar.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        let out = bar.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let input = match out {
            PaletteBarOutcome::Submit(s) => s,
            other => panic!("expected Submit, got {other:?}"),
        };
        assert_eq!(
            crate::tui::palette::parse(&input),
            Ok(PaletteCommand::StepSkip(Some(3)))
        );
    }

    // -- Mouse-drag split (step 26) ---------------------------------------

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
    fn split_drag_updates_split_pct() {
        // Down on the divider, drag right by ~10 columns, release.
        // Body width 100 with default split_pct 40 puts the divider at
        // column 40; dragging the cursor to column 60 should land at 60%.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
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
    fn split_drag_press_off_divider_does_not_arm() {
        // ±1 column tolerance: pressing far from the divider should not
        // arm a drag, so subsequent drag events leave split_pct alone.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.last_body_width = 100;

        // Divider is at column 40; press at column 10.
        app.handle_mouse(mouse_event(10, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_split);

        app.handle_mouse(mouse_event(70, 5, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 40, "drag without arming should not resize");
    }

    #[test]
    fn split_drag_clamps_to_20_and_80() {
        // Past column 0 still yields 20%; past column 80% still yields 80%.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
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
    fn keyboard_navigation_works_during_drag() {
        // j/k must continue to function regardless of dragging_split — the
        // drag flag only affects which mouse-drag events update the split.
        let mut app = PlanDetailApp::new(make_plan(), make_steps(5), &Config::default());
        app.dragging_split = true;

        assert_eq!(app.selected_index, 0);
        app.navigate_down();
        assert_eq!(app.selected_index, 1);
        app.navigate_down();
        assert_eq!(app.selected_index, 2);
        app.navigate_up();
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn handle_mouse_no_op_before_first_draw() {
        // Before the first frame `last_body_width` is zero; mouse events
        // must not panic and must not arm a drag (divider would be at 0).
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        assert_eq!(app.last_body_width, 0);

        app.handle_mouse(mouse_event(0, 0, MouseEventKind::Down(MouseButton::Left)));
        assert!(!app.dragging_split);
    }

    // -- Mouse: click-to-select / click-again-to-enter (step 25) ----------

    /// A bordered step-list area mirroring what `draw_step_list` records:
    /// the body sits below the 1-row top chrome (`y = 1`), and the
    /// `step_list` widget draws a `Borders::ALL` frame so content starts at
    /// `(x+1, y+1)`. With offset 0, row `y+1` is step 0, `y+2` is step 1, …
    fn list_app(n: usize) -> PlanDetailApp {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(n), &Config::default());
        // Wide enough that the divider (40% of 100) sits at col 40, far
        // from the rows we click in these tests.
        app.last_body_width = 100;
        app.step_list_area = ratatui::layout::Rect {
            x: 0,
            y: 1,
            width: 40,
            height: 20,
        };
        app
    }

    /// Park the outline cursor on visible row `idx` (the §12.1 cursor source
    /// of truth) the way the keyboard / `/focus` path does, then realign
    /// `selected_index`. The pre-overhaul mouse tests poked `selected_index`
    /// directly; after the §12.1 outline cutover the rendered highlight is
    /// driven by `outline.cursor()`, so tests must move the cursor through
    /// the outline. `make_steps` has empty `short_id` + ascending
    /// `sort_key`, so with no edges the outline order == construction order
    /// == `idx`.
    fn outline_cursor_to(app: &mut PlanDetailApp, idx: usize) {
        app.outline.set_cursor(idx);
        app.realign_selection_to_outline();
    }

    #[test]
    fn click_on_highlighted_step_enters_detail() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(4);
        outline_cursor_to(&mut app, 2);
        // Inner list row for index 2 is y = 1 (area.y) + 1 (border) + 2.
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(
            app.take_pending_open_step(),
            Some("s2".to_string()),
            "click on the highlighted row should request opening that step"
        );
        assert_eq!(app.selected_index, 2, "selection unchanged");
        assert_eq!(
            app.outline.selected_step_id().as_deref(),
            Some("s2"),
            "outline cursor unchanged on the re-click"
        );
        assert!(!app.dragging_split);
    }

    #[test]
    fn click_on_other_step_selects_only() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(4);
        outline_cursor_to(&mut app, 0);
        // Row for index 3: y = 1 + 1 + 3 = 5.
        app.handle_mouse(mouse_event(5, 5, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.selected_index, 3, "click moves the flat cursor");
        assert_eq!(
            app.outline.selected_step_id().as_deref(),
            Some("s3"),
            "click moves the OUTLINE cursor (drives the rendered highlight)"
        );
        assert!(
            app.take_pending_open_step().is_none(),
            "a non-highlighted click must not enter detail"
        );
    }

    #[test]
    fn click_accounts_for_scroll_offset() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(20);
        // Scrolled so the first visible item is *outline* row 5. The click
        // must hit-test against `outline.visible_rows()[offset + visible_row]`
        // (the rendered index space — §12.1), not `self.steps[idx]`; compute
        // the expected target from the outline so the assertion tracks the
        // real rendered order rather than a fragile hard-coded ordinal.
        let expected = app.outline.visible_rows()[5].step_id.clone();
        app.list_state.select(Some(7));
        *app.list_state.offset_mut() = 5;
        outline_cursor_to(&mut app, 7);
        // First inner row (y = 2) maps to offset 5 + 0 = outline row 5.
        app.handle_mouse(mouse_event(3, 2, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(
            app.outline.cursor(),
            5,
            "outline row→cursor must add the scroll offset"
        );
        assert_eq!(
            app.outline.selected_step_id(),
            Some(expected.clone()),
            "the offset-adjusted outline row drives the cursor"
        );
        assert_eq!(
            app.steps[app.selected_index].id, expected,
            "selected_index realigns onto the same step the outline resolved"
        );
        assert!(app.take_pending_open_step().is_none());
    }

    #[test]
    fn click_on_border_or_outside_is_ignored() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(4);
        outline_cursor_to(&mut app, 1);
        // The top border row (y == area.y == 1) is not a content row.
        app.handle_mouse(mouse_event(5, 1, MouseEventKind::Down(MouseButton::Left)));
        // A click well past the last step (only 4 steps) is also ignored.
        app.handle_mouse(mouse_event(5, 18, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.selected_index, 1, "out-of-list clicks change nothing");
        assert_eq!(
            app.outline.selected_step_id().as_deref(),
            Some("s1"),
            "out-of-list clicks must not move the outline cursor"
        );
        assert!(app.take_pending_open_step().is_none());
    }

    #[test]
    fn scroll_wheel_maps_to_k_and_j() {
        use crossterm::event::MouseEventKind;
        let mut app = list_app(5);
        assert_eq!(app.selected_index, 0);
        assert_eq!(app.outline.cursor(), 0);
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollDown));
        assert_eq!(app.selected_index, 1, "ScrollDown behaves like j");
        assert_eq!(app.outline.cursor(), 1, "scroll moves the OUTLINE cursor");
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollDown));
        assert_eq!(app.selected_index, 2);
        assert_eq!(app.outline.cursor(), 2);
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollUp));
        assert_eq!(app.selected_index, 1, "ScrollUp behaves like k");
        assert_eq!(app.outline.cursor(), 1);
    }

    #[test]
    fn divider_drag_takes_precedence_over_step_click() {
        // dragging_split must win the state machine: a Drag while armed
        // resizes the split even though the cursor is over the step list,
        // and never gets reinterpreted as a row click.
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(5);
        app.selected_index = 0;

        // Arm the drag on the divider (col 40 = 40% of 100).
        app.handle_mouse(mouse_event(40, 5, MouseEventKind::Down(MouseButton::Left)));
        assert!(app.dragging_split, "press on divider arms the drag");

        // Drag left into the step-list rows. It must resize, not select.
        app.handle_mouse(mouse_event(25, 4, MouseEventKind::Drag(MouseButton::Left)));
        assert_eq!(app.split_pct, 25, "armed drag keeps resizing over the list");
        assert_eq!(app.selected_index, 0, "drag must not move the cursor");
        assert!(app.take_pending_open_step().is_none());

        app.handle_mouse(mouse_event(25, 4, MouseEventKind::Up(MouseButton::Left)));
        assert!(!app.dragging_split, "release clears the drag flag");
    }

    #[test]
    fn step_click_before_first_draw_is_safe() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        // No draw yet: step_list_area is zero-sized.
        app.handle_mouse(mouse_event(2, 2, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.selected_index, 0);
        assert!(app.take_pending_open_step().is_none());
    }

    // -- Mouse on the dependency outline (docs/dag-redesign.md §12.1/§12.2) -
    //
    // These pin the §12.1 defect fix: the mouse path must hit-test /
    // navigate in `outline.visible_rows()`'s index space (topological,
    // focus-filtered), not the flat `self.steps` order, so click + scroll
    // behave correctly on a non-linear and a focused/re-rooted DAG.

    fn dag_step(id: &str, short_id: &str, sort_key: &str) -> Step {
        Step {
            id: id.to_string(),
            short_id: short_id.to_string(),
            plan_id: "p1".to_string(),
            sort_key: sort_key.to_string(),
            title: format!("step {short_id}"),
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
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        }
    }

    /// A diamond DAG (a → {b, c} → d) whose `self.steps` storage order is
    /// deliberately *scrambled* (`[d, b, a, c]`) so it differs from the
    /// topological `visible_rows()` order (`[a, b, c, d]`). Any hit-test
    /// that indexed `self.steps` would resolve the wrong step here.
    fn diamond_app() -> PlanDetailApp {
        let steps = vec![
            dag_step("ud", "dddd", "a3"),
            dag_step("ub", "bbbb", "a1"),
            dag_step("ua", "aaaa", "a0"),
            dag_step("uc", "cccc", "a2"),
        ];
        let mut app = PlanDetailApp::new(make_plan(), steps, &Config::default());
        app.last_body_width = 100;
        app.step_list_area = ratatui::layout::Rect {
            x: 0,
            y: 1,
            width: 40,
            height: 20,
        };
        let mut deps_of: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        deps_of.insert("ub".to_string(), vec!["ua".to_string()]);
        deps_of.insert("uc".to_string(), vec!["ua".to_string()]);
        deps_of.insert("ud".to_string(), vec!["ub".to_string(), "uc".to_string()]);
        app.sync_outline(deps_of, std::collections::HashSet::new());
        app
    }

    #[test]
    fn click_on_nonlinear_dag_resolves_outline_row_not_steps_index() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = diamond_app();
        // Topological visible order: [aaaa(ua), bbbb(ub), cccc(uc), dddd(ud)].
        let rows = app.outline.visible_rows();
        assert_eq!(
            rows.iter().map(|r| r.short_id.as_str()).collect::<Vec<_>>(),
            vec!["aaaa", "bbbb", "cccc", "dddd"],
        );
        // Click visible row k = 2 (cccc / "uc"). Inner row y = 1 + 1 + 2 = 4.
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::Down(MouseButton::Left)));
        // Outline cursor + selected_index + resolved step id all agree, and
        // they point at visible_rows()[2] = uc — NOT self.steps[2] (= "ua").
        assert_eq!(app.outline.cursor(), 2);
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("uc"));
        assert_eq!(
            app.selected_index, 3,
            "self.steps position of uc (scrambled order)"
        );
        assert_eq!(app.steps[app.selected_index].id, "uc");
        assert!(
            app.take_pending_open_step().is_none(),
            "first click only selects"
        );

        // Second click on the SAME visible row → open that exact step.
        app.handle_mouse(mouse_event(5, 4, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(
            app.take_pending_open_step(),
            Some("uc".to_string()),
            "repeat click on the selected outline row opens that step"
        );
        assert_eq!(app.outline.cursor(), 2, "cursor unchanged on the re-click");
    }

    #[test]
    fn click_on_focused_rerooted_outline_resolves_correct_step() {
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = diamond_app();
        // Focus on bbbb(ub): downstream cone = {bbbb, dddd}. The visible
        // rows shrink to [bbbb, dddd] — a different index space again.
        app.outline.set_cursor(1); // bbbb
        assert!(app.outline.focus_cursor());
        app.realign_selection_to_outline();
        let rows = app.outline.visible_rows();
        assert_eq!(
            rows.iter().map(|r| r.short_id.as_str()).collect::<Vec<_>>(),
            vec!["bbbb", "dddd"],
            "focus re-roots to the downstream cone"
        );

        // Click visible row k = 1 (dddd / "ud") within the focused outline.
        // Inner row y = 1 + 1 + 1 = 3.
        app.handle_mouse(mouse_event(5, 3, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.outline.cursor(), 1);
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("ud"));
        assert_eq!(app.steps[app.selected_index].id, "ud");
        assert!(app.take_pending_open_step().is_none());

        // Repeat click on that focused row opens dddd, not whatever the flat
        // index space would have resolved at row 1.
        app.handle_mouse(mouse_event(5, 3, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.take_pending_open_step(), Some("ud".to_string()));

        // A click below the focused cone (only 2 rows visible) is ignored —
        // it must not resolve into a hidden / unrelated step.
        app.handle_mouse(mouse_event(5, 6, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.outline.cursor(), 1, "out-of-cone click changes nothing");
        assert!(app.take_pending_open_step().is_none());
    }

    #[test]
    fn scroll_wheel_moves_outline_cursor_on_nonlinear_dag() {
        use crossterm::event::MouseEventKind;
        let mut app = diamond_app();
        assert_eq!(app.outline.cursor(), 0);
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("ua"));

        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollDown));
        assert_eq!(app.outline.cursor(), 1, "scroll moves the OUTLINE cursor");
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("ub"));
        // selected_index follows via realign — into the scrambled steps vec.
        assert_eq!(app.steps[app.selected_index].id, "ub");

        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollDown));
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("uc"));
        assert_eq!(app.steps[app.selected_index].id, "uc");

        app.handle_mouse(mouse_event(5, 4, MouseEventKind::ScrollUp));
        assert_eq!(
            app.outline.cursor(),
            1,
            "ScrollUp moves the outline cursor back"
        );
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("ub"));
        assert_eq!(app.steps[app.selected_index].id, "ub");
    }

    #[test]
    fn linear_no_edge_mouse_behavior_unchanged_regression() {
        // The degenerate linear / no-edge case must behave EXACTLY as before
        // the §12.1 cutover: outline order == construction order, so a click
        // on inner row k selects step `sk`, and a re-click opens it; scroll
        // walks the cursor 1:1. Pins "no regression for linear plans".
        use crossterm::event::{MouseButton, MouseEventKind};
        let mut app = list_app(5);
        // No edges installed: visible_rows() == construction order s0..s4.
        let rows = app.outline.visible_rows();
        assert_eq!(
            rows.iter().map(|r| r.step_id.as_str()).collect::<Vec<_>>(),
            vec!["s0", "s1", "s2", "s3", "s4"],
        );

        // Click row 3 (y = 1 + 1 + 3 = 5) selects s3 (cursor + flat agree).
        app.handle_mouse(mouse_event(5, 5, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.outline.cursor(), 3);
        assert_eq!(app.selected_index, 3);
        assert_eq!(app.outline.selected_step_id().as_deref(), Some("s3"));
        assert!(app.take_pending_open_step().is_none());
        // Re-click same row opens s3.
        app.handle_mouse(mouse_event(5, 5, MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(app.take_pending_open_step(), Some("s3".to_string()));

        // Scroll from s3: down → s4, up → s3 (1:1, identical to old j/k).
        app.handle_mouse(mouse_event(5, 5, MouseEventKind::ScrollDown));
        assert_eq!(app.selected_index, 4);
        assert_eq!(app.outline.cursor(), 4);
        app.handle_mouse(mouse_event(5, 5, MouseEventKind::ScrollUp));
        assert_eq!(app.selected_index, 3);
        assert_eq!(app.outline.cursor(), 3);
    }
}
