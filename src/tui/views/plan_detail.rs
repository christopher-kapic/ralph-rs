// Plan detail view state
//
// Manages the state tracked by the plan-detail view of the TUI: selected step,
// input mode, execution timer, and step list. This module is independent of
// rendering and input handling so that it can be unit-tested without a
// terminal.

use std::time::Instant;

use ratatui::widgets::ListState;

use crate::config::Config;
use crate::plan::{Plan, Step, StepStatus};

// ---------------------------------------------------------------------------
// Input mode
// ---------------------------------------------------------------------------

/// Determines how keyboard input is interpreted.
pub enum InputMode {
    /// Normal navigation mode (j/k/a/s/q).
    Normal,
    /// Inline text input for adding a new step.
    AddStep,
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

    /// Switch to AddStep input mode.
    pub fn enter_add_mode(&mut self) {
        self.input_mode = InputMode::AddStep;
        self.input_buffer.clear();
    }

    /// Confirm the add-step input. Returns the trimmed title if non-empty,
    /// or `None` if the input was blank (cancelling the add).
    pub fn confirm_add_step(&mut self) -> Option<String> {
        let title = self.input_buffer.trim().to_string();
        self.input_buffer.clear();
        self.input_mode = InputMode::Normal;
        if title.is_empty() { None } else { Some(title) }
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
    use super::{InputMode, PlanDetailApp};
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
    fn test_enter_add_mode() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode();
        assert!(matches!(app.input_mode, InputMode::AddStep));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_confirm_add_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        // Select step 1 (in_progress) then add
        app.selected_index = 1;
        app.enter_add_mode();
        app.input_buffer = "New step title".to_string();
        let title = app.confirm_add_step();

        assert!(title.is_some());
        assert_eq!(title.unwrap(), "New step title");
        assert!(matches!(app.input_mode, InputMode::Normal));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_confirm_add_step_empty_title() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode();
        app.input_buffer = "   ".to_string();
        let title = app.confirm_add_step();
        assert!(title.is_none());
        assert!(matches!(app.input_mode, InputMode::Normal));
    }

    #[test]
    fn test_cancel_add_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = PlanDetailApp::new(plan, steps, &Config::default());

        app.enter_add_mode();
        app.input_buffer = "Some text".to_string();
        app.cancel_input();
        assert!(matches!(app.input_mode, InputMode::Normal));
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
        let _add = InputMode::AddStep;
        // Both variants are constructible without panic
    }
}
