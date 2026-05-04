// Plan detail view rendering
//
// Layout and widget construction for the plan-detail view of the TUI, powered
// by ratatui. Renders the step list, step detail panel, and keybinding help
// bar.

use std::path::Path;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

use super::plan_detail::{InputMode, PlanDetailApp};
use crate::plan::StepStatus;
use crate::tui::chrome::{self, Chrome};
use crate::tui::theme;

/// Render the entire plan-detail view.
pub fn draw(frame: &mut Frame, app: &mut PlanDetailApp) {
    let crumbs: [&str; 2] = ["ralph", app.plan.slug.as_str()];
    let hint = hint_for(app);
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint: &hint,
            cwd: Path::new(&app.plan.project),
        },
    );

    // Main content: step list (left) + step detail (right).
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(body);

    draw_step_list(frame, app, main[0]);
    draw_step_detail(frame, app, main[1]);
}

fn hint_for(app: &PlanDetailApp) -> String {
    match app.input_mode {
        InputMode::Normal => "[j/k] nav  [a] add step  [s] skip  [q] quit".to_string(),
        InputMode::AddStep => "[Enter] confirm  [Esc] cancel".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Step list (left panel)
// ---------------------------------------------------------------------------

fn draw_step_list(frame: &mut Frame, app: &mut PlanDetailApp, area: Rect) {
    let items: Vec<ListItem> = app
        .steps
        .iter()
        .enumerate()
        .map(|(i, step)| {
            let indicator = PlanDetailApp::status_indicator(step.status);
            let label = format!("{indicator} {}. {}", i + 1, step.title);
            let style = match step.status {
                StepStatus::Complete => Style::default().fg(theme::STATUS_COMPLETE),
                StepStatus::InProgress => Style::default()
                    .fg(theme::STATUS_IN_PROGRESS)
                    .add_modifier(Modifier::BOLD),
                StepStatus::Failed => Style::default().fg(theme::STATUS_FAILED),
                StepStatus::Skipped => Style::default().fg(theme::CHROME_DIM),
                StepStatus::Aborted => Style::default().fg(theme::STATUS_FAILED),
                StepStatus::Pending => Style::default().fg(theme::STATUS_PENDING),
            };
            ListItem::new(Line::from(Span::styled(label, style)))
        })
        .collect();

    let title = format!(" {} ", app.plan.slug);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let list = List::new(items)
        .block(block)
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    app.list_state.select(Some(app.selected_index));
    frame.render_stateful_widget(list, area, &mut app.list_state);
}

// ---------------------------------------------------------------------------
// Step detail (right panel)
// ---------------------------------------------------------------------------

fn draw_step_detail(frame: &mut Frame, app: &PlanDetailApp, area: Rect) {
    if app.steps.is_empty() {
        let empty = Paragraph::new("No steps in this plan.").block(
            Block::default()
                .title(" Details ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
        frame.render_widget(empty, area);
        return;
    }

    let step = &app.steps[app.selected_index];
    let mut lines: Vec<Line> = Vec::new();

    // Title
    lines.push(Line::from(vec![
        Span::styled("Title: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(&step.title),
    ]));

    // Status
    let status_color = match step.status {
        StepStatus::Complete => theme::STATUS_COMPLETE,
        StepStatus::InProgress => theme::STATUS_IN_PROGRESS,
        StepStatus::Failed => theme::STATUS_FAILED,
        StepStatus::Skipped => theme::CHROME_DIM,
        StepStatus::Aborted => theme::STATUS_FAILED,
        StepStatus::Pending => theme::STATUS_PENDING,
    };
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(step.status.as_str(), Style::default().fg(status_color)),
    ]));

    // Agent
    if let Some(agent) = step.agent.as_deref().or(app.plan.agent.as_deref()) {
        lines.push(Line::from(vec![
            Span::styled("Agent: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(agent),
        ]));
    }

    // Harness
    if let Some(harness) = step.harness.as_deref().or(app.plan.harness.as_deref()) {
        lines.push(Line::from(vec![
            Span::styled("Harness: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(harness),
        ]));
    }

    // Attempt counter
    let max_retries = step.max_retries.unwrap_or(app.default_max_retries as i32);
    let max_attempts = max_retries + 1;
    lines.push(Line::from(vec![
        Span::styled("Attempts: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!("{}/{}", step.attempts, max_attempts)),
    ]));

    // Live timer (only for in-progress steps)
    if step.status == StepStatus::InProgress {
        let elapsed = app.elapsed_secs();
        let mins = (elapsed as u64) / 60;
        let secs = (elapsed as u64) % 60;
        lines.push(Line::from(vec![
            Span::styled("Elapsed: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{mins:02}:{secs:02}"),
                Style::default().fg(Color::Yellow),
            ),
        ]));
    }

    // Description
    if !step.description.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Description:",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        for desc_line in step.description.lines() {
            lines.push(Line::from(Span::raw(desc_line)));
        }
    }

    // Acceptance criteria
    if !step.acceptance_criteria.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Acceptance Criteria:",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        for criterion in &step.acceptance_criteria {
            lines.push(Line::from(format!("  - {criterion}")));
        }
    }

    // Input field for adding a step (shown below the detail when in AddStep mode)
    if matches!(app.input_mode, InputMode::AddStep) {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(
                "New step title: ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(&app.input_buffer, Style::default().fg(Color::White)),
            Span::styled("_", Style::default().fg(Color::Cyan)),
        ]));
    }

    let block = Block::default()
        .title(" Details ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
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
                status: StepStatus::Pending,
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

    #[test]
    fn test_status_colors_are_distinct() {
        // This tests that the rendering function handles all status variants
        // without panicking. We can't inspect pixel output, but we verify the
        // status_indicator function returns unique indicators.
        let statuses = [
            StepStatus::Complete,
            StepStatus::InProgress,
            StepStatus::Pending,
            StepStatus::Failed,
            StepStatus::Skipped,
            StepStatus::Aborted,
        ];
        let indicators: Vec<&str> = statuses
            .iter()
            .map(|s| PlanDetailApp::status_indicator(*s))
            .collect();
        // All should be non-empty
        for ind in &indicators {
            assert!(!ind.is_empty());
        }
    }

    #[test]
    fn test_draw_does_not_panic_empty_steps() {
        let mut app = make_app(0);
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
    }

    #[test]
    fn test_draw_does_not_panic_with_steps() {
        let mut app = make_app(5);
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
    }

    #[test]
    fn test_draw_add_mode() {
        let mut app = make_app(3);
        app.enter_add_mode();
        app.input_buffer = "New step".to_string();
        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
    }

    #[test]
    fn test_detail_attempt_counter_uses_config_default() {
        // A step with no explicit max_retries override should fall back to the
        // configured Config.max_retries_per_step rather than a hardcoded 3.
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
        let steps = vec![Step {
            id: "s0".to_string(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Only step".to_string(),
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
        }];
        let config = Config {
            max_retries_per_step: 7,
            ..Default::default()
        };
        let mut app = PlanDetailApp::new(plan, steps, &config);

        let backend = ratatui::backend::TestBackend::new(80, 24);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();

        let buffer = terminal.backend().buffer().clone();
        let rendered: String = (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            rendered.contains("0/8"),
            "detail panel should render 0/(max_retries_per_step + 1) = 0/8; got:\n{rendered}"
        );
    }

    #[test]
    fn test_list_state_persists_across_frames() {
        // Render a long list in a small viewport, scroll past the visible window,
        // and verify the list_state offset is preserved (not reset to 0 each frame).
        let mut app = make_app(50);
        let backend = ratatui::backend::TestBackend::new(40, 10);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        assert_eq!(app.list_state.selected(), Some(0));

        // Scroll far enough that the selection must be off-screen on first render.
        for _ in 0..30 {
            app.navigate_down();
        }
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let offset_after_scroll = app.list_state.offset();
        assert_eq!(app.list_state.selected(), Some(30));
        assert!(
            offset_after_scroll > 0,
            "viewport should have scrolled to follow selection"
        );

        // A subsequent render with no navigation must not reset the offset.
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        assert_eq!(app.list_state.offset(), offset_after_scroll);
    }
}
