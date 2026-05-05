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
    /// The user pressed `D` to open the plan-dependencies sub-view
    /// (TUI-plan.md §1, step 33).
    OpenDependencies,
    /// The user pressed `H` to open the plan-hooks sub-view
    /// (TUI-plan.md §1).
    OpenHooks,
    /// The user pressed `A` to open step detail focused on the step that
    /// owns the oldest unanswered question (TUI-plan.md §17).
    OpenQuestion(String),
    /// The user pressed `enter` / `→` / `l` to open the step-detail view for
    /// the step under the cursor (TUI-plan.md §7).
    OpenStepDetail(String),
    /// The user pressed `Q` to flip the plan's `questions_enabled` column
    /// (TUI-plan.md §17 'Toggle surfaces').
    ToggleQuestionsEnabled,
    /// The user pressed `P` while a run is live to toggle the operator's
    /// graceful-pause request. The dispatcher flips `plans.pause_requested`
    /// — first press sets it (runner stops after the current step), second
    /// press clears it (cancels the request before the boundary fires).
    TogglePauseRequested,
}

/// True when J/K should scroll the live-run tails (TUI-plan.md §13) instead
/// of moving steps. Active whenever a subscription is wired AND the tails
/// have buffered output — outside of a live run we keep the original
/// step-move semantics so existing reorder-by-J/K muscle memory is intact.
fn tails_take_priority(app: &PlanDetailApp) -> bool {
    app.subscribed && app.has_tail_output()
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
    // §13.2 lockdown: when an external runner holds the lock, the edit
    // keybindings (i/a/d/r/s/R/Shift-J/Shift-K) are suppressed. Navigation,
    // S (cancel via the dispatcher), q/h/← (pop), Esc, and tail scrolling
    // remain active. We compute this once so each per-key arm only has to
    // reason about its own edit-vs-non-edit classification.
    let locked = app.read_only.is_locked();

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

        // Insert step above (edit — suppressed when locked).
        KeyCode::Char('i') if !locked => {
            app.enter_add_mode_above();
            InputAction::None
        }

        // Append step below (edit — suppressed when locked).
        KeyCode::Char('a') if !locked => {
            app.enter_add_mode_below();
            InputAction::None
        }

        // Delete step(s) (edit — suppressed when locked).
        KeyCode::Char('d') if !locked => {
            let targets = app.delete_targets();
            if targets.is_empty() {
                InputAction::None
            } else {
                InputAction::Delete(targets)
            }
        }

        // Reset step (edit — suppressed when locked).
        KeyCode::Char('r') if !locked => {
            if let Some(id) = app.reset_target() {
                InputAction::Reset(id)
            } else {
                InputAction::None
            }
        }

        // Shift-J: scroll the right-pane tails one line newer when a live
        // subscription is producing output (TUI-plan.md §13); otherwise
        // fall through to the original step-reorder semantics — but the
        // step-reorder branch is an edit and is suppressed when locked.
        KeyCode::Char('J') => {
            if tails_take_priority(app) {
                app.scroll_tails_newer();
                InputAction::None
            } else if !locked {
                if let Some(id) = app.move_down_target() {
                    InputAction::MoveDown(id)
                } else {
                    InputAction::None
                }
            } else {
                InputAction::None
            }
        }

        // Shift-K: same dual-mode as Shift-J but inverted direction.
        KeyCode::Char('K') => {
            if tails_take_priority(app) {
                app.scroll_tails_older();
                InputAction::None
            } else if !locked {
                if let Some(id) = app.move_up_target() {
                    InputAction::MoveUp(id)
                } else {
                    InputAction::None
                }
            } else {
                InputAction::None
            }
        }

        // Skip current step (edit — suppressed when locked).
        KeyCode::Char('s') if !locked => {
            if let Some(step_id) = app.request_skip() {
                InputAction::SkipStep(step_id)
            } else {
                InputAction::None
            }
        }

        // Run / resume the plan (TUI-plan.md §7). `R` is suppressed when
        // already locked because there's a run in progress.
        KeyCode::Char('R') if !locked => InputAction::Run,

        // Stop the live run (TUI-plan.md §7).
        KeyCode::Char('S') => InputAction::Stop,

        // Open the plan-dependencies sub-view (TUI-plan.md §1, step 33).
        KeyCode::Char('D') => InputAction::OpenDependencies,

        // Open the plan-hooks sub-view (TUI-plan.md §1).
        KeyCode::Char('H') => InputAction::OpenHooks,

        // Answer the oldest unanswered question for this plan (TUI-plan.md §17).
        // No-op when there are no open questions — the dispatcher checks the
        // returned step id is `Some`.
        KeyCode::Char('A') => match app.oldest_question_step_id() {
            Some(step_id) => InputAction::OpenQuestion(step_id),
            None => InputAction::None,
        },

        // Flip `plans.questions_enabled` for this plan (TUI-plan.md §17
        // 'Toggle surfaces'). Mirrors plan-list's Q binding. Edit — suppressed
        // when locked.
        KeyCode::Char('Q') if !locked => InputAction::ToggleQuestionsEnabled,

        // Graceful pause: toggle `plans.pause_requested` while a run is live.
        // The runner reads + clears the flag between step boundaries, so the
        // first press signals "stop after current step" and the second press
        // (before the boundary fires) cancels that request. Only meaningful
        // while a run is in flight, hence the `is_run_live()` gate.
        KeyCode::Char('P') if app.is_run_live() => InputAction::TogglePauseRequested,

        // Open step detail for the highlighted step (TUI-plan.md §7).
        // Read-only navigation, so allowed even while locked.
        KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
            if app.steps.is_empty() {
                InputAction::None
            } else {
                InputAction::OpenStepDetail(app.steps[app.selected_index].id.clone())
            }
        }

        // Esc precedence (TUI-plan.md §4): dismiss the current toast first
        // when one is showing; otherwise clear the selection. Esc does NOT
        // pop the view — `q`/`h`/`←` own that.
        KeyCode::Esc => {
            if !app.toasts.dismiss() {
                let _ = app.escape_selection();
            }
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
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
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
        assert_eq!(app.input_mode, InputMode::AddStep(AddPosition::Above));
    }

    #[test]
    fn test_a_enters_add_mode_below() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('a')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.input_mode, InputMode::AddStep(AddPosition::Below));
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
    fn test_shift_q_emits_toggle_questions_enabled() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('Q')));
        assert_eq!(action, InputAction::ToggleQuestionsEnabled);
    }

    #[test]
    fn test_shift_p_emits_toggle_pause_when_run_live() {
        // P is gated on `is_run_live()`, which is true once a subscription
        // is wired (TUI-spawned runner) or a LiveRun row is observed.
        let mut app = make_app(3);
        app.subscribed = true;
        assert!(app.is_run_live());
        let action = handle_key(&mut app, key(KeyCode::Char('P')));
        assert_eq!(action, InputAction::TogglePauseRequested);
    }

    #[test]
    fn test_shift_p_is_noop_when_no_run_live() {
        // No subscription, no live_run row → P should fall through silently.
        let mut app = make_app(3);
        assert!(!app.is_run_live());
        let action = handle_key(&mut app, key(KeyCode::Char('P')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_shift_q_toggle_questions() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('Q')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_shift_d_emits_open_dependencies_action() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('D')));
        assert_eq!(action, InputAction::OpenDependencies);
    }

    #[test]
    fn test_shift_h_emits_open_hooks_action() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('H')));
        assert_eq!(action, InputAction::OpenHooks);
    }

    #[test]
    fn test_enter_emits_open_step_detail_for_cursor() {
        let mut app = make_app(3);
        app.selected_index = 1;
        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(action, InputAction::OpenStepDetail("s1".to_string()));
    }

    #[test]
    fn test_right_arrow_emits_open_step_detail() {
        let mut app = make_app(3);
        app.selected_index = 2;
        let action = handle_key(&mut app, key(KeyCode::Right));
        assert_eq!(action, InputAction::OpenStepDetail("s2".to_string()));
    }

    #[test]
    fn test_l_emits_open_step_detail() {
        let mut app = make_app(3);
        let action = handle_key(&mut app, key(KeyCode::Char('l')));
        assert_eq!(action, InputAction::OpenStepDetail("s0".to_string()));
    }

    #[test]
    fn test_enter_with_no_steps_is_noop() {
        let mut app = make_app(0);
        let action = handle_key(&mut app, key(KeyCode::Enter));
        assert_eq!(action, InputAction::None);
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
    fn test_esc_dismisses_toast_first_and_preserves_selection() {
        // TUI-plan.md §4: Esc dismisses the current toast before falling
        // through to the view's own Esc behavior. The selection must remain
        // intact when the toast was the consumer.
        use crate::tui::toast::ToastKind;
        use std::time::Instant;

        let mut app = make_app(3);
        app.selected_index = 1;
        app.toggle_selection();
        assert!(!app.selection.is_empty());
        app.toasts
            .push("Saved.", ToastKind::Success, Instant::now());

        let action = handle_key(&mut app, key(KeyCode::Esc));

        assert_eq!(action, InputAction::None);
        assert!(app.toasts.is_empty(), "toast must be popped");
        assert!(
            !app.selection.is_empty(),
            "selection must be untouched when Esc consumed the toast"
        );
        assert!(!app.should_pop);
    }

    #[test]
    fn test_esc_with_no_toast_falls_through_to_clear_selection() {
        // Without a toast, Esc retains its existing §7 behavior: clear the
        // selection without popping the view.
        let mut app = make_app(3);
        app.selected_index = 1;
        app.toggle_selection();
        assert!(!app.selection.is_empty());
        assert!(app.toasts.is_empty());

        let action = handle_key(&mut app, key(KeyCode::Esc));

        assert_eq!(action, InputAction::None);
        assert!(app.selection.is_empty(), "selection must be cleared");
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

    // -- J/K dual semantics: scroll tails when subscription has output ---

    #[test]
    fn test_shift_j_scrolls_tails_when_subscription_has_output() {
        let mut app = make_app(3);
        app.attach_subscription();
        for i in 0..5 {
            app.push_harness_line(format!("h{i}"));
        }
        app.scroll_tails_older(); // anchor 1 back
        let before = app.harness_tail_scroll;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::None);
        // J = newer = scroll decrements.
        assert!(app.harness_tail_scroll < before);
    }

    #[test]
    fn test_shift_k_scrolls_tails_when_subscription_has_output() {
        let mut app = make_app(3);
        app.attach_subscription();
        for i in 0..5 {
            app.push_harness_line(format!("h{i}"));
        }
        let before = app.harness_tail_scroll;
        let action = handle_key(&mut app, key(KeyCode::Char('K')));
        assert_eq!(action, InputAction::None);
        // K = older = scroll increments.
        assert!(app.harness_tail_scroll > before);
    }

    #[test]
    fn test_shift_j_falls_through_to_move_step_when_no_subscription() {
        let mut app = make_app(3);
        // No subscription, no tails — preserve original step-move semantics.
        app.selected_index = 0;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::MoveDown("s0".to_string()));
    }

    #[test]
    fn test_shift_j_falls_through_when_subscription_has_no_output_yet() {
        let mut app = make_app(3);
        // Subscription attached but no chunks delivered: J still moves the step.
        app.attach_subscription();
        app.selected_index = 0;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::MoveDown("s0".to_string()));
    }

    // -- Read-only attach + edit lockdown (TUI-plan.md §13.2) -----------

    /// Drive the App into the read-only state used by the lockdown tests.
    fn lock_app(app: &mut PlanDetailApp) {
        app.set_read_only(crate::tui::read_only::ReadOnly::Locked { pid: 4242 });
    }

    #[test]
    fn test_locked_suppresses_i_add_above() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('i')));
        assert_eq!(action, InputAction::None);
        assert_eq!(
            app.input_mode,
            InputMode::Normal,
            "i must NOT enter add mode while locked"
        );
    }

    #[test]
    fn test_locked_suppresses_a_add_below() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('a')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn test_locked_suppresses_d_delete() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('d')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_r_reset() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('r')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_s_skip() {
        let mut app = make_app(3);
        app.selected_index = 0; // InProgress step would otherwise be skippable.
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('s')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_shift_r_run() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('R')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_shift_j_move_down_without_subscription() {
        // No subscription ⇒ J would otherwise be MoveDown — must be gated.
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_suppresses_shift_k_move_up_without_subscription() {
        let mut app = make_app(3);
        app.selected_index = 2;
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('K')));
        assert_eq!(action, InputAction::None);
    }

    #[test]
    fn test_locked_still_allows_navigation() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('j')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 1);
        let action = handle_key(&mut app, key(KeyCode::Char('k')));
        assert_eq!(action, InputAction::None);
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_locked_still_allows_shift_s_stop() {
        let mut app = make_app(3);
        lock_app(&mut app);
        // S is the user's escape hatch when locked — must remain enabled.
        let action = handle_key(&mut app, key(KeyCode::Char('S')));
        assert_eq!(action, InputAction::Stop);
    }

    #[test]
    fn test_locked_still_allows_q_pop() {
        let mut app = make_app(3);
        lock_app(&mut app);
        let action = handle_key(&mut app, key(KeyCode::Char('q')));
        assert_eq!(action, InputAction::Pop);
        assert!(app.should_pop);
    }

    #[test]
    fn test_locked_still_allows_shift_j_tail_scroll_when_subscription_has_output() {
        // J in the dual-mode tail-scroll branch is read-only navigation, so
        // it should keep working when the lockdown is engaged. (The lockdown
        // suppresses the *step-move* fallback only.)
        let mut app = make_app(3);
        app.attach_subscription();
        for i in 0..5 {
            app.push_harness_line(format!("h{i}"));
        }
        app.scroll_tails_older(); // anchor 1 back so J has somewhere to scroll
        lock_app(&mut app);
        let before = app.harness_tail_scroll;
        let action = handle_key(&mut app, key(KeyCode::Char('J')));
        assert_eq!(action, InputAction::None);
        assert!(
            app.harness_tail_scroll < before,
            "tail scroll should still work"
        );
    }

    // -- Help overlay (TUI-plan.md §15) ---------------------------------

    #[test]
    fn help_state_default_hidden() {
        let app = make_app(3);
        assert!(!app.help.is_visible());
    }

    #[test]
    fn help_intercepts_question_mark_and_esc() {
        let mut app = make_app(3);
        let q = key(KeyCode::Char('?'));
        assert_eq!(
            app.help.intercept_key(q),
            crate::tui::help::InterceptResult::Opened
        );
        assert!(app.help.is_visible());

        let esc = key(KeyCode::Esc);
        assert_eq!(
            app.help.intercept_key(esc),
            crate::tui::help::InterceptResult::Closed
        );
        assert!(!app.help.is_visible());
    }
}
