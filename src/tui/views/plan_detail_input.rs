// Plan detail view input handling
//
// Maps crossterm key events to PlanDetailApp state mutations. Separates input
// interpretation from rendering and state so it can be tested without a
// terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::plan_detail::{AddPosition, InputMode, PlanDetailApp};

/// Result of handling a key event.
#[derive(Debug, PartialEq, Eq)]
pub enum InputAction {
    /// No special side-effect — the app state was updated in place.
    None,
    /// The user confirmed adding a step at `position` with the given title.
    AddStep(AddPosition, String),
    /// The user requested to skip the step with the given ID.
    SkipStep(String),
    /// The user requested to delete the listed step IDs (selection-aware,
    /// in selection pick order or the cursor's single-element list).
    Delete(Vec<String>),
    /// The user requested to reset the step with the given ID.
    Reset(String),
    /// The user requested to move the step with the given ID up one slot.
    MoveUp(String),
    /// The user requested to move the step with the given ID down one slot.
    MoveDown(String),
    /// The user requested to pop back to the plan list (`←`/`h`/`q` or
    /// Ctrl-C). TUI-plan.md §7.
    Pop,
    /// The user pressed `R` to spawn / resume the runner subprocess for this
    /// plan. The dispatcher checks for an existing live run before spawning.
    Run,
    /// The user pressed `S` to stop the live run via `ralph cancel` semantics
    /// (SIGTERM with timeout, fall back to SIGKILL).
    Stop,
}

/// Handle a key event and return the resulting action.
pub fn handle_key(app: &mut PlanDetailApp, key: KeyEvent) -> InputAction {
    match app.input_mode {
        InputMode::Normal => handle_normal_mode(app, key),
        InputMode::AddStep(_) => handle_add_mode(app, key),
    }
}

// ---------------------------------------------------------------------------
// Normal mode
// ---------------------------------------------------------------------------

fn handle_normal_mode(app: &mut PlanDetailApp, key: KeyEvent) -> InputAction {
    match key.code {
        // Navigation
        KeyCode::Char('j') | KeyCode::Down => {
            app.navigate_down();
            InputAction::None
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.navigate_up();
            InputAction::None
        }

        // Multi-select
        KeyCode::Char(' ') => {
            app.toggle_selection();
            InputAction::None
        }

        // Insert step above
        KeyCode::Char('i') => {
            app.enter_add_mode_above();
            InputAction::None
        }

        // Append step below
        KeyCode::Char('a') => {
            app.enter_add_mode_below();
            InputAction::None
        }

        // Delete step(s)
        KeyCode::Char('d') => {
            let targets = app.delete_targets();
            if targets.is_empty() {
                InputAction::None
            } else {
                InputAction::Delete(targets)
            }
        }

        // Reset step
        KeyCode::Char('r') => {
            if let Some(id) = app.reset_target() {
                InputAction::Reset(id)
            } else {
                InputAction::None
            }
        }

        // Move down (Shift-J → 'J')
        KeyCode::Char('J') => {
            if let Some(id) = app.move_down_target() {
                InputAction::MoveDown(id)
            } else {
                InputAction::None
            }
        }

        // Move up (Shift-K → 'K')
        KeyCode::Char('K') => {
            if let Some(id) = app.move_up_target() {
                InputAction::MoveUp(id)
            } else {
                InputAction::None
            }
        }

        // Skip current step
        KeyCode::Char('s') => {
            if let Some(step_id) = app.request_skip() {
                InputAction::SkipStep(step_id)
            } else {
                InputAction::None
            }
        }

        // Run / resume the plan (TUI-plan.md §7).
        KeyCode::Char('R') => InputAction::Run,

        // Stop the live run (TUI-plan.md §7).
        KeyCode::Char('S') => InputAction::Stop,

        // Esc clears selection if any; otherwise no-op (Esc does NOT pop the
        // view — `q`/`h`/`←` own that).
        KeyCode::Esc => {
            let _ = app.escape_selection();
            InputAction::None
        }

        // Pop back to plan list (TUI-plan.md §7).
        KeyCode::Char('q') | KeyCode::Char('h') | KeyCode::Left => {
            app.request_pop();
            InputAction::Pop
        }

        // Ctrl+C pops the plan-detail view (one level), matching the
        // archived-list view's pattern. The plan-list dispatcher owns
        // whole-TUI exit.
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.request_pop();
            InputAction::Pop
        }

        _ => InputAction::None,
    }
}

// ---------------------------------------------------------------------------
// AddStep mode
// ---------------------------------------------------------------------------

fn handle_add_mode(app: &mut PlanDetailApp, key: KeyEvent) -> InputAction {
    match key.code {
        // Confirm
        KeyCode::Enter => {
            if let Some((pos, title)) = app.confirm_add_step() {
                InputAction::AddStep(pos, title)
            } else {
                InputAction::None
            }
        }

        // Cancel
        KeyCode::Esc => {
            app.cancel_input();
            InputAction::None
        }

        // Backspace
        KeyCode::Backspace => {
            app.input_buffer.pop();
            InputAction::None
        }

        // Ctrl+C cancels the input and pops the view, mirroring
        // archived-list's escape behavior.
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.cancel_input();
            app.request_pop();
            InputAction::Pop
        }

        // Character input
        KeyCode::Char(c) => {
            app.input_buffer.push(c);
            InputAction::None
        }

        _ => InputAction::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::plan::{Plan, PlanStatus, Step, StepStatus};
    use chrono::Utc;

    fn make_app(n: usize) -> PlanDetailApp {
        let plan = Plan {
            id: "p1".to_string(),
            slug: "test".to_string(),
            project: "/tmp".to_string(),
            branch_name: "b".to_string(),
            description: "d".to_string(),
            status: PlanStatus::InProgress,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
            questions_enabled: false,
        };
        let steps: Vec<Step> = (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                plan_id: "p1".to_string(),
                sort_key: format!("a{i}"),
                title: format!("Step {}", i + 1),
                description: "Desc".to_string(),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                status: if i == 0 {
                    StepStatus::InProgress
                } else {
                    StepStatus::Pending
                },
                attempts: 0,
                max_retries: Some(3),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                model: None,
                skipped_reason: None,
                change_policy: crate::plan::ChangePolicy::Required,
                tags: vec![],
            })
            .collect();
        PlanDetailApp::new(plan, steps, &Config::default())
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- Normal mode tests --

    #[test]
    fn test_j_navigates_down() {
        let mut app = make_app(3);
        assert_eq!(app.selected_index, 0);
        let action = handle_key(&mut app, key(KeyCode::Char('j')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn test_k_navigates_up() {
        let mut app = make_app(3);
        app.selected_index = 2;
        let action = handle_key(&mut app, key(KeyCode::Char('k')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn test_down_arrow_navigates_down() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Down));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 1);
    }

    #[test]
    fn test_up_arrow_navigates_up() {
        let mut app = make_app(3);
        app.selected_index = 1;
        let action = handle_key(&mut app, key(KeyCode::Up));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_space_toggles_selection() {
        let mut app = make_app(3);
        app.selected_index = 1;
        let action = handle_key(&mut app, key(KeyCode::Char(' ')));
        assert_eq!(action, InputAction::None);
        assert!(app.selection.is_selected(&"s1".to_string()));
    }

    #[test]
    fn test_i_enters_add_mode_above() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('i')));
        assert_eq!(action, InputAction::None);
        assert_eq!(
            app.input_mode,
            InputMode::AddStep(AddPosition::Above)
        );
    }

    #[test]
    fn test_a_enters_add_mode_below() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('a')));
        assert_eq!(action, InputAction::None);
        assert_eq!(
            app.input_mode,
            InputMode::AddStep(AddPosition::Below)
        );
    }

    #[test]
    fn test_d_returns_delete_with_cursor_target() {
        let mut app = make_app(3);
        app.selected_index = 2;
        let action = handle_key(&mut app, key(KeyCode::Char('d')));
        assert_eq!(action, InputAction::Delete(vec!["s2".to_string()]));
    }

    #[test]
    fn test_d_returns_delete_with_selection() {
        let mut app = make_app(4);
        app.selected_index = 1;
        app.toggle_selection();
        app.selected_index = 3;
        app.toggle_selection();
        let action = handle_key(&mut app, key(KeyCode::Char('d')));
        assert_eq!(
            action,
            InputAction::Delete(vec!["s1".to_string(), "s3".to_string()])
        );
    }

    #[test]
    fn test_d_with_no_steps_is_noop() {
        let mut app = make_app(0);
        let action = handle_key(&mut app, key(KeyCode::Char('d')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_r_returns_reset_for_cursor_step() {
        let mut app = make_app(3);
        app.selected_index = 1;
        let action = handle_key(&mut app, key(KeyCode::Char('r')));
        assert_eq!(action, InputAction::Reset("s1".to_string()));
    }

    #[test]
    fn test_r_with_no_steps_is_noop() {
        let mut app = make_app(0);
        let action = handle_key(&mut app, key(KeyCode::Char('r')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_shift_j_returns_move_down() {
        let mut app = make_app(3);
        app.selected_index = 0;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::MoveDown("s0".to_string()));
    }

    #[test]
    fn test_shift_j_at_bottom_is_noop() {
        let mut app = make_app(3);
        app.selected_index = 2;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_shift_k_returns_move_up() {
        let mut app = make_app(3);
        app.selected_index = 2;
        let action = handle_key(&mut app, key(KeyCode::Char('K')));
        assert_eq!(action, InputAction::MoveUp("s2".to_string()));
    }

    #[test]
    fn test_shift_k_at_top_is_noop() {
        let mut app = make_app(3);
        app.selected_index = 0;
        let action = handle_key(&mut app, key(KeyCode::Char('K')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_s_skips_in_progress_step() {
        let mut app = make_app(3);
        app.selected_index = 0; // InProgress step
        let action = handle_key(&mut app, key(KeyCode::Char('s')));
        assert_eq!(action, InputAction::SkipStep("s0".to_string()));
    }

    #[test]
    fn test_shift_r_emits_run_action() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('R')));
        assert_eq!(action, InputAction::Run);
    }

    #[test]
    fn test_shift_s_emits_stop_action() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('S')));
        assert_eq!(action, InputAction::Stop);
    }

    #[test]
    fn test_q_pops() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('q')));
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
    }

    #[test]
    fn test_h_pops() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('h')));
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
    }

    #[test]
    fn test_left_arrow_pops() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Left));
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
    }

    #[test]
    fn test_ctrl_c_pops() {
        let mut app = make_app(3);
        let action = handle_key(
            &mut app,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
    }

    #[test]
    fn test_esc_clears_selection_without_popping() {
        let mut app = make_app(3);
        app.selected_index = 1;
        app.toggle_selection();
        let action = handle_key(&mut app, key(KeyCode::Esc));
        assert_eq!(action, InputAction::None);
        assert!(app.selection.is_empty());
        assert!(!app.should_pop);
    }

    #[test]
    fn test_esc_with_no_selection_is_noop() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Esc));
        assert_eq!(action, InputAction::None);
        assert!(!app.should_pop);
    }

    #[test]
    fn test_unknown_key_is_noop() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('x')));
        assert_eq!(action, InputAction::None);
    }

    // -- AddStep mode tests --

    #[test]
    fn test_add_mode_typing() {
        let mut app = make_app(3);
        app.enter_add_mode_above();

        handle_key(&mut app, key(KeyCode::Char('H')));
        handle_key(&mut app, key(KeyCode::Char('i')));
        assert_eq!(app.input_buffer, "Hi");
    }

    #[test]
    fn test_add_mode_backspace() {
        let mut app = make_app(3);
        app.enter_add_mode_above();
        app.input_buffer = "Hello".to_string();

        handle_key(&mut app, key(KeyCode::Backspace));
        assert_eq!(app.input_buffer, "Hell");
    }

    #[test]
    fn test_add_mode_enter_confirms_above() {
        let mut app = make_app(3);
        app.enter_add_mode_above();
        app.input_buffer = "New step".to_string();

        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(
            action,
            InputAction::AddStep(AddPosition::Above, "New step".to_string())
        );
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_add_mode_enter_confirms_below() {
        let mut app = make_app(3);
        app.enter_add_mode_below();
        app.input_buffer = "Tail step".to_string();

        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(
            action,
            InputAction::AddStep(AddPosition::Below, "Tail step".to_string())
        );
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_add_mode_enter_empty_is_noop() {
        let mut app = make_app(3);
        app.enter_add_mode_above();
        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_add_mode_esc_cancels() {
        let mut app = make_app(3);
        app.enter_add_mode_above();
        app.input_buffer = "partial".to_string();

        let action = handle_key(&mut app, key(KeyCode::Esc));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.input_mode, InputMode::Normal);
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_add_mode_ctrl_c_pops() {
        let mut app = make_app(3);
        app.enter_add_mode_above();
        app.input_buffer = "partial".to_string();

        let action = handle_key(
            &mut app,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
        assert_eq!(app.input_mode, InputMode::Normal);
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_add_mode_does_not_navigate() {
        let mut app = make_app(3);
        app.enter_add_mode_above();

        // j should be typed, not navigate
        handle_key(&mut app, key(KeyCode::Char('j')));
        assert_eq!(app.input_buffer, "j");
        assert_eq!(app.selected_index, 0);
    }
}
