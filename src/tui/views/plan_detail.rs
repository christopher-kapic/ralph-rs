// Plan detail view state
//
// Manages the state tracked by the plan-detail view of the TUI: selected step,
// input mode, execution timer, multi-selection, and step list. This module is
// independent of rendering and input handling so that it can be unit-tested
// without a terminal.

use std::collections::VecDeque;
use std::time::Instant;

use ratatui::widgets::ListState;

use crate::config::Config;
use crate::frac_index::{self, FracIndexError};
use crate::plan::{Phase, Plan, Step, StepStatus};
use crate::run_lock::LiveRun;
use crate::tui::events::{TAIL_BUFFER_LINES, TAIL_VISIBLE_LINES};
use crate::tui::help::HelpState;
use crate::tui::read_only::ReadOnly;
use crate::tui::selection::Selection;
use crate::tui::toast::ToastQueue;

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

    /// Start time of the current in-progress step (for the live timer).
    pub step_start_time: Option<Instant>,

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

    /// Open (unanswered) `step_questions` rows for this plan, ordered oldest
    /// first per [`crate::storage::list_open_questions`]. Drives the §17
    /// banner pane and the `A` keybinding that pushes step-detail focused on
    /// the originating step. Refreshed by the dispatcher each poll tick.
    pub open_questions: Vec<crate::storage::OpenQuestion>,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// passing keys to the per-view input handler (TUI-plan.md §15).
    pub help: HelpState,
}

impl PlanDetailApp {
    /// Create a new PlanDetailApp with the given plan and steps.
    pub fn new(plan: Plan, steps: Vec<Step>, config: &Config) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        Self {
            plan,
            steps,
            selected_index: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            should_pop: false,
            step_start_time: None,
            list_state,
            default_max_retries: config.max_retries_per_step,
            selection: Selection::new(),
            toasts: ToastQueue::new(),
            live_run: None,
            subscribed: false,
            subscribed_step_num: None,
            current_phase: None,
            harness_tail: VecDeque::new(),
            test_tail: VecDeque::new(),
            harness_tail_scroll: 0,
            test_tail_scroll: 0,
            read_only: ReadOnly::Editable,
            open_questions: Vec::new(),
            help: HelpState::new(),
        }
    }

    /// Replace the cached open-question list (after a DB poll). Drives the
    /// §17 banner + the `A` keybinding's target. Empty input clears the list,
    /// hiding the banner.
    pub fn set_open_questions(&mut self, questions: Vec<crate::storage::OpenQuestion>) {
        self.open_questions = questions;
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
        if self.steps.is_empty() {
            return None;
        }
        let step = &self.steps[self.selected_index];
        match step.status {
            StepStatus::Pending
            | StepStatus::InProgress
            | StepStatus::Failed
            | StepStatus::Aborted => Some(step.id.clone()),
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
        } else if !self.steps.is_empty() {
            vec![self.steps[self.selected_index].id.clone()]
        } else {
            Vec::new()
        }
    }

    // -- Reset ------------------------------------------------------------

    /// Step ID to reset, or `None` when the step list is empty. Cursor-only
    /// (selection is ignored — reset is a single-step operation).
    pub fn reset_target(&self) -> Option<String> {
        if self.steps.is_empty() {
            None
        } else {
            Some(self.steps[self.selected_index].id.clone())
        }
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

    // -- Live-run snapshot ------------------------------------------------

    /// Update the cached live-run snapshot. When the in-progress step
    /// changes (or transitions in/out of the running state), the live timer
    /// is reset so "Elapsed" reflects the *current* step rather than the
    /// previous one.
    pub fn update_live_run(&mut self, live: Option<LiveRun>) {
        let prev_step = self.live_run.as_ref().and_then(|l| l.step_id.clone());
        let next_step = live.as_ref().and_then(|l| l.step_id.clone());
        self.live_run = live;
        if prev_step != next_step {
            if next_step.is_some() {
                self.start_step_timer();
            } else {
                self.stop_step_timer();
            }
        }
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

    /// True iff a runner is currently bound to this plan — either via an
    /// active TUI-spawned subscription (TUI-plan.md §13) or via a DB-poll
    /// snapshot of `run_locks` (read-only attach, §13.2).
    pub fn is_run_live(&self) -> bool {
        self.subscribed || self.live_run.is_some()
    }

    // -- NDJSON-stream driven state (TUI-plan.md §13) ---------------------

    /// Mark this view as bound to a TUI-spawned [`crate::tui::events::RunSubscription`].
    /// Resets the per-run state (phase, tails, step number) so a fresh run
    /// doesn't inherit stale chunks from a prior subscription.
    pub fn attach_subscription(&mut self) {
        self.subscribed = true;
        self.subscribed_step_num = None;
        self.current_phase = None;
        self.harness_tail.clear();
        self.test_tail.clear();
        self.harness_tail_scroll = 0;
        self.test_tail_scroll = 0;
        self.start_step_timer();
    }

    /// Release a previously-attached subscription. Called by the dispatcher
    /// when the channel disconnects (subprocess exited) or the user pops
    /// the view.
    pub fn detach_subscription(&mut self) {
        self.subscribed = false;
        self.subscribed_step_num = None;
        self.current_phase = None;
        self.stop_step_timer();
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

    /// Update the cached current phase (NDJSON `phase_changed` event).
    pub fn set_current_phase(&mut self, phase: Phase) {
        self.current_phase = Some(phase);
    }

    /// Record that a `step_started` event just arrived: bring the run-live
    /// state online if the subscription's first event preceded the DB-side
    /// row, latch the step number, and reset the elapsed timer for the
    /// freshly-started step.
    pub fn note_step_started(&mut self, step_id: &str) {
        self.subscribed = true;
        self.start_step_timer();
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

    /// Start the live timer for the current step.
    pub fn start_step_timer(&mut self) {
        self.step_start_time = Some(Instant::now());
    }

    /// Stop the live timer.
    pub fn stop_step_timer(&mut self) {
        self.step_start_time = None;
    }

    /// Get elapsed seconds since the step timer started (0.0 if not running).
    pub fn elapsed_secs(&self) -> f64 {
        self.step_start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
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

#[allow(dead_code)]
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
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
        }
    }

    fn make_steps(n: usize) -> Vec<Step> {
        (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
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

        // Select the in_progress step
        app.selected_index = 1;
        let result = app.request_skip();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "s1"); // step index 1 = id "s1"
    }

    #[test]
    fn test_skip_complete_step_rejected() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        // Select the complete step
        app.selected_index = 0;
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
    fn test_execution_timer() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        assert!(app.step_start_time.is_none());
        app.start_step_timer();
        assert!(app.step_start_time.is_some());

        let elapsed = app.elapsed_secs();
        assert!(elapsed >= 0.0);

        app.stop_step_timer();
        assert!(app.step_start_time.is_none());
    }

    #[test]
    fn test_elapsed_secs_no_timer() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = PlanDetailApp::new(plan, steps, &Config::default());

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
        app.selected_index = 2;
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
        app.selected_index = 0;
        assert_eq!(app.reset_target(), Some("s0".to_string()));
        app.selected_index = 2;
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
        }
    }

    #[test]
    fn test_update_live_run_starts_timer_when_step_appears() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        assert!(!app.is_run_live());
        assert!(app.step_start_time.is_none());

        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        assert!(app.is_run_live());
        assert!(app.step_start_time.is_some());
        assert_eq!(app.live_step_num(), Some(2));
    }

    #[test]
    fn test_update_live_run_stops_timer_when_run_ends() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        assert!(app.step_start_time.is_some());

        app.update_live_run(None);
        assert!(!app.is_run_live());
        assert!(app.step_start_time.is_none());
        assert_eq!(app.live_step_num(), None);
    }

    #[test]
    fn test_update_live_run_resets_timer_on_step_change() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());
        app.update_live_run(Some(make_live_run("test-plan", Some("s1"), Some(2))));
        let first_start = app.step_start_time.unwrap();

        // Pause briefly so Instant comparisons are unambiguous.
        std::thread::sleep(std::time::Duration::from_millis(2));
        app.update_live_run(Some(make_live_run("test-plan", Some("s2"), Some(3))));
        let second_start = app.step_start_time.unwrap();
        assert!(
            second_start > first_start,
            "timer should restart when in-progress step changes"
        );
        assert_eq!(app.live_step_num(), Some(3));
    }

    // -- NDJSON-stream-driven state (TUI-plan.md §13) ---------------------

    #[test]
    fn test_attach_subscription_marks_run_live_and_starts_timer() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        assert!(!app.is_run_live());
        assert!(app.step_start_time.is_none());
        app.attach_subscription();
        assert!(app.is_run_live());
        assert!(app.step_start_time.is_some());
        assert_eq!(app.current_phase(), None);
        assert!(app.harness_tail.is_empty());
        assert!(app.test_tail.is_empty());
    }

    #[test]
    fn test_attach_subscription_resets_prior_tails_and_phase() {
        let mut app = PlanDetailApp::new(make_plan(), make_steps(3), &Config::default());
        app.subscribed = true;
        app.harness_tail.push_back("stale stdout".into());
        app.test_tail.push_back("stale tests".into());
        app.set_current_phase(crate::plan::Phase::Tests);
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
        app.set_current_phase(crate::plan::Phase::Harness);
        assert!(app.is_run_live());
        app.detach_subscription();
        assert!(!app.is_run_live());
        assert!(app.step_start_time.is_none());
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
        app.set_current_phase(crate::plan::Phase::Tests);
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
}
