// TUI application state
//
// Manages the state tracked by the interactive TUI: selected step, input mode,
// execution timer, and step list. This module is independent of rendering and
// input handling so that it can be unit-tested without a terminal.

use std::time::Instant;

use ratatui::widgets::ListState;

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

/// Core application state for the TUI.
pub struct App {
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

    /// Whether the user has requested a quit.
    pub should_quit: bool,

    /// Start time of the current in-progress step (for the live timer).
    pub step_start_time: Option<Instant>,

    /// Persistent list widget state so the viewport offset survives across frames.
    pub list_state: ListState,
}

impl App {
    /// Create a new App with the given plan and steps.
    pub fn new(plan: Plan, steps: Vec<Step>) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));
        Self {
            plan,
            steps,
            selected_index: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            should_quit: false,
            step_start_time: None,
            list_state,
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

    // -- Quit -------------------------------------------------------------

    /// Signal that the user wants to quit.
    pub fn request_quit(&mut self) {
        self.should_quit = true;
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
