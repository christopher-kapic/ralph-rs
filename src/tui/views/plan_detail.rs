// Plan detail view state
//
// Manages the state tracked by the plan-detail view of the TUI: selected step,
// input mode, execution timer, multi-selection, and step list. This module is
// independent of rendering and input handling so that it can be unit-tested
// without a terminal.

use std::time::Instant;

use ratatui::widgets::ListState;

use crate::config::Config;
use crate::frac_index::{self, FracIndexError};
use crate::plan::{Plan, Step, StepStatus};
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
        }
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

#[cfg(test)]
mod tests {
    use super::{AddPosition, InputMode, PlanDetailApp};
    use crate::config::Config;
    use crate::plan::{Plan, PlanStatus, Step, StepStatus};
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
}
