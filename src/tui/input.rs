// TUI input handling
//
// Maps crossterm key events to App state mutations. Separates input
// interpretation from rendering and state so it can be tested without a
// terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{App, InputMode};

/// Result of handling a key event.
#[derive(Debug, PartialEq, Eq)]
pub enum InputAction {
    /// No special side-effect — the app state was updated in place.
    None,
    /// The user confirmed adding a step with the given title.
    AddStep(String),
    /// The user requested to skip the step with the given ID.
    SkipStep(String),
    /// The user requested to quit.
    Quit,
}

/// Handle a key event and return the resulting action.
pub fn handle_key(app: &mut App, key: KeyEvent) -> InputAction {
    match app.input_mode {
        InputMode::Normal => handle_normal_mode(app, key),
        InputMode::AddStep => handle_add_mode(app, key),
    }
}

// ---------------------------------------------------------------------------
// Normal mode
// ---------------------------------------------------------------------------

fn handle_normal_mode(app: &mut App, key: KeyEvent) -> InputAction {
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

        // Add step
        KeyCode::Char('a') => {
            app.enter_add_mode();
            InputAction::None
        }

        // Skip current step
        KeyCode::Char('s') => {
            if let Some(step_id) = app.request_skip() {
                InputAction::SkipStep(step_id)
            } else {
                InputAction::None
            }
        }

        // Quit
        KeyCode::Char('q') => {
            app.request_quit();
            InputAction::Quit
        }

        // Ctrl+C also triggers quit
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.request_quit();
            InputAction::Quit
        }

        _ => InputAction::None,
    }
}

// ---------------------------------------------------------------------------
// AddStep mode
// ---------------------------------------------------------------------------

fn handle_add_mode(app: &mut App, key: KeyEvent) -> InputAction {
    match key.code {
        // Confirm
        KeyCode::Enter => {
            if let Some(title) = app.confirm_add_step() {
                InputAction::AddStep(title)
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

        // Ctrl+C quits even in add mode
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.cancel_input();
            app.request_quit();
            InputAction::Quit
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

    fn make_app(n: usize) -> App {
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
        App::new(plan, steps, &Config::default())
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
    fn test_a_enters_add_mode() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('a')));
        assert_eq!(action, InputAction::None);
        assert!(matches!(app.input_mode, InputMode::AddStep));
    }

    #[test]
    fn test_s_skips_in_progress_step() {
        let mut app = make_app(3);
        app.selected_index = 0; // InProgress step
        let action = handle_key(&mut app, key(KeyCode::Char('s')));
        assert_eq!(action, InputAction::SkipStep("s0".to_string()));
    }

    #[test]
    fn test_q_quits() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('q')));
        assert_eq!(action, InputAction::Quit);
        assert!(app.should_quit);
    }

    #[test]
    fn test_ctrl_c_quits() {
        let mut app = make_app(3);
        let action = handle_key(
            &mut app,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert_eq!(action, InputAction::Quit);
        assert!(app.should_quit);
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
        app.enter_add_mode();

        handle_key(&mut app, key(KeyCode::Char('H')));
        handle_key(&mut app, key(KeyCode::Char('i')));
        assert_eq!(app.input_buffer, "Hi");
    }

    #[test]
    fn test_add_mode_backspace() {
        let mut app = make_app(3);
        app.enter_add_mode();
        app.input_buffer = "Hello".to_string();

        handle_key(&mut app, key(KeyCode::Backspace));
        assert_eq!(app.input_buffer, "Hell");
    }

    #[test]
    fn test_add_mode_enter_confirms() {
        let mut app = make_app(3);
        app.enter_add_mode();
        app.input_buffer = "New step".to_string();

        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(action, InputAction::AddStep("New step".to_string()));
        assert!(matches!(app.input_mode, InputMode::Normal));
    }

    #[test]
    fn test_add_mode_enter_empty_is_noop() {
        let mut app = make_app(3);
        app.enter_add_mode();
        // Empty input
        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(action, InputAction::None);
        assert!(matches!(app.input_mode, InputMode::Normal));
    }

    #[test]
    fn test_add_mode_esc_cancels() {
        let mut app = make_app(3);
        app.enter_add_mode();
        app.input_buffer = "partial".to_string();

        let action = handle_key(&mut app, key(KeyCode::Esc));
        assert_eq!(action, InputAction::None);
        assert!(matches!(app.input_mode, InputMode::Normal));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_add_mode_ctrl_c_quits() {
        let mut app = make_app(3);
        app.enter_add_mode();
        app.input_buffer = "partial".to_string();

        let action = handle_key(
            &mut app,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert_eq!(action, InputAction::Quit);
        assert!(app.should_quit);
        assert!(matches!(app.input_mode, InputMode::Normal));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_add_mode_does_not_navigate() {
        let mut app = make_app(3);
        app.enter_add_mode();

        // j should be typed, not navigate
        handle_key(&mut app, key(KeyCode::Char('j')));
        assert_eq!(app.input_buffer, "j");
        assert_eq!(app.selected_index, 0);
    }
}
