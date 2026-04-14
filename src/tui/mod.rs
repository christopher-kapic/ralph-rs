// TUI module
//
// Interactive terminal UI for plan execution, built on ratatui + crossterm.
// Provides step list navigation, live status, inline step insertion, and
// graceful shutdown — as the interactive counterpart to the non-interactive
// runner.

pub mod app;
pub mod input;
pub mod ui;

#[cfg(test)]
mod tests {
    use super::app::{App, InputMode};
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
            })
            .collect()
    }

    #[test]
    fn test_app_creation() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = App::new(plan.clone(), steps.clone());

        assert_eq!(app.plan.slug, "test-plan");
        assert_eq!(app.steps.len(), 3);
        assert_eq!(app.selected_index, 0);
        assert!(!app.should_quit);
        assert!(matches!(app.input_mode, InputMode::Normal));
    }

    #[test]
    fn test_navigate_down() {
        let plan = make_plan();
        let steps = make_steps(5);
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

        app.selected_index = 2;
        app.navigate_down();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_navigate_up() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

        assert_eq!(app.selected_index, 0);
        app.navigate_up();
        assert_eq!(app.selected_index, 2);
    }

    #[test]
    fn test_navigate_empty_steps() {
        let plan = make_plan();
        let mut app = App::new(plan, vec![]);

        app.navigate_down();
        assert_eq!(app.selected_index, 0);
        app.navigate_up();
        assert_eq!(app.selected_index, 0);
    }

    #[test]
    fn test_enter_add_mode() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

        app.enter_add_mode();
        assert!(matches!(app.input_mode, InputMode::AddStep));
        assert!(app.input_buffer.is_empty());
    }

    #[test]
    fn test_confirm_add_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

        // Select the complete step
        app.selected_index = 0;
        let result = app.request_skip();
        assert!(result.is_none()); // Can't skip a completed step
    }

    #[test]
    fn test_quit() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

        assert!(!app.should_quit);
        app.request_quit();
        assert!(app.should_quit);
    }

    #[test]
    fn test_update_step_status() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

        app.update_step_status("s2", StepStatus::InProgress, 1);
        assert_eq!(app.steps[2].status, StepStatus::InProgress);
        assert_eq!(app.steps[2].attempts, 1);
    }

    #[test]
    fn test_update_step_status_unknown_id() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

        // Should be a no-op
        app.update_step_status("unknown", StepStatus::Complete, 1);
        // No panic, no change
        assert_eq!(app.steps.len(), 3);
    }

    #[test]
    fn test_current_in_progress_step() {
        let plan = make_plan();
        let steps = make_steps(3);
        let app = App::new(plan, steps);

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
        let app = App::new(plan, steps);

        let current = app.current_in_progress_step();
        assert!(current.is_none());
    }

    #[test]
    fn test_insert_step_at_position() {
        let plan = make_plan();
        let steps = make_steps(3);
        let mut app = App::new(plan, steps);

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
        let mut app = App::new(plan, steps);

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
        let app = App::new(plan, steps);

        assert_eq!(app.elapsed_secs(), 0.0);
    }

    #[test]
    fn test_status_indicator() {
        assert_eq!(App::status_indicator(StepStatus::Complete), "  ");
        assert_eq!(App::status_indicator(StepStatus::InProgress), "  ");
        assert_eq!(App::status_indicator(StepStatus::Pending), "  ");
        assert_eq!(App::status_indicator(StepStatus::Failed), "  ");
        assert_eq!(App::status_indicator(StepStatus::Skipped), "  ");
        assert_eq!(App::status_indicator(StepStatus::Aborted), "  ");
    }

    #[test]
    fn test_input_mode_variants() {
        let _normal = InputMode::Normal;
        let _add = InputMode::AddStep;
        // Both variants are constructible without panic
    }
}
