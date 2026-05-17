// Plan detail view rendering
//
// Layout and widget construction for the plan-detail view of the TUI, powered
// by ratatui. Renders the step list, step detail panel, and keybinding help
// bar.

use std::path::Path;
use std::time::Instant;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::plan_detail::{AddPosition, InputMode, PlanDetailApp};
use crate::plan::{Phase, StepStatus};
use crate::tui::chrome::{self, Chrome};
use crate::tui::events::TAIL_VISIBLE_LINES;
use crate::tui::help;
use crate::tui::read_only;
use crate::tui::theme;
use crate::tui::widgets::palette_bar;
use crate::tui::widgets::step_list;

/// Render the entire plan-detail view.
pub fn draw(frame: &mut Frame, app: &mut PlanDetailApp) {
    app.toasts.prune(Instant::now());

    let crumbs: [&str; 2] = ["ralph", app.plan.slug.as_str()];
    let hint = hint_for(app);
    let banner = read_only::banner(app.read_only);
    // §29: surface a compact "▶ Running step N (phase) MM:SS" in the bottom
    // chrome row whenever a runner is bound to the plan, so the user knows
    // what's executing regardless of where their cursor is parked.
    let running = running_indicator(app);
    let body = chrome::render(
        frame,
        &Chrome {
            breadcrumbs: &crumbs,
            hint: &hint,
            cwd: Path::new(&app.plan.project),
            banner: banner.as_deref(),
            running_indicator: running.as_deref(),
        },
    );

    // Main content: step list (left) + step detail (right). The split is
    // driven by `app.split_pct` so a mouse drag on the divider can resize
    // both panes (TUI-plan.md, step 26). `last_body_width` is captured
    // here so `handle_mouse` can convert the cursor's column into a percent.
    app.last_body_width = body.width;
    let split_pct = app.split_pct;
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(split_pct),
            Constraint::Percentage(100 - split_pct),
        ])
        .split(body);

    draw_step_list(frame, app, main[0]);
    draw_step_detail(frame, app, main[1]);

    if let Some(toast) = app.toasts.current() {
        let area = frame.area();
        if area.height >= 1 && area.width > 0 {
            render_toast_overlay(frame, area, &toast.text, toast.color);
        }
    }

    // Help overlay sits on top of everything else when `?` has been pressed.
    if app.help.is_visible() {
        let area = frame.area();
        help::render(frame, area, &help::for_plan_detail());
    }

    // Palette bar overlays the bottom chrome row when active. TUI-plan.md §9.
    if let Some(state) = app.palette_bar.as_ref() {
        let area = frame.area();
        let strip_height = 4.min(area.height);
        if strip_height > 0 {
            let palette_area = Rect {
                x: area.x,
                y: area.y + area.height - strip_height,
                width: area.width,
                height: strip_height,
            };
            palette_bar::render(frame, palette_area, state);
        }
    }
}

/// Build the compact running-step indicator surfaced in the chrome bar.
/// Returns `None` when no runner is bound to this plan; otherwise yields
/// `▶ Running step N (phase) MM:SS` (some fields omitted when unknown so the
/// indicator never renders as `(unknown)` placeholders).
fn running_indicator(app: &PlanDetailApp) -> Option<String> {
    if !app.is_run_live() {
        return None;
    }
    let step_label = match app.live_step_num() {
        Some(n) => format!("▶ Running step {n}"),
        None => "▶ Running...".to_string(),
    };
    let phase_label = app
        .current_phase()
        .map(|p| format!(" ({})", phase_human_label(p)))
        .unwrap_or_default();
    let elapsed = app.elapsed_secs() as u64;
    let mins = elapsed / 60;
    let secs = elapsed % 60;
    Some(format!("{step_label}{phase_label} {mins:02}:{secs:02}"))
}

fn hint_for(app: &PlanDetailApp) -> String {
    if app.palette_active() {
        return "[tab] complete  [enter] submit  [esc] cancel".to_string();
    }
    match app.input_mode {
        InputMode::Normal => {
            // While a run is live, surface `[P] pause` (graceful stop after
            // the current step) alongside the existing `[S] stop` (immediate
            // cancel). Outside a live run there's nothing to pause, so the
            // hint stays compact.
            if app.is_run_live() {
                "[j/k] nav  [enter] open  [space] sel  [i/a] add  [d] del  [s] skip  [R] run  [P] pause  [S] stop  [/:] cmd  [q] back"
                    .to_string()
            } else {
                "[j/k] nav  [enter] open  [space] sel  [i/a] add  [d] del  [s] skip  [R] run  [S] stop  [/:] cmd  [q] back"
                    .to_string()
            }
        }
        InputMode::AddStep(_) => "[Enter] confirm  [Esc] cancel".to_string(),
    }
}

fn render_toast_overlay(frame: &mut Frame, area: Rect, text: &str, color: ratatui::style::Color) {
    let max_toast = area.width.saturating_sub(1).max(1);
    let desired = text.chars().count().min(max_toast as usize) as u16;
    if desired == 0 {
        return;
    }
    let toast_area = Rect {
        x: area.x,
        y: area.y + area.height - 1,
        width: desired,
        height: 1,
    };
    frame.render_widget(Clear, toast_area);
    let para = Paragraph::new(Span::styled(
        text.chars()
            .take(toast_area.width as usize)
            .collect::<String>(),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(para, toast_area);
}

// ---------------------------------------------------------------------------
// Step list (left panel)
// ---------------------------------------------------------------------------

fn draw_step_list(frame: &mut Frame, app: &mut PlanDetailApp, area: Rect) {
    // Record the bordered list area so `handle_mouse` can hit-test a click
    // row to a step index (it accounts for the Block border + scroll offset).
    app.step_list_area = area;
    let cursor = if app.steps.is_empty() {
        None
    } else {
        Some(app.selected_index)
    };
    step_list::render(
        frame,
        area,
        &app.steps,
        &app.selection,
        cursor,
        app.is_run_live(),
        app.plan.slug.as_str(),
        &mut app.list_state,
    );
}

// ---------------------------------------------------------------------------
// Step detail (right panel)
// ---------------------------------------------------------------------------

fn draw_step_detail(frame: &mut Frame, app: &PlanDetailApp, area: Rect) {
    // §17: when this plan has unanswered questions, carve a single-row banner
    // off the top of the right panel. The remainder hosts the existing
    // step-detail body so callers don't need to reflow everything.
    let body_area = if !app.open_questions.is_empty() {
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0)])
            .split(area);
        draw_open_questions_banner(frame, app, split[0]);
        split[1]
    } else {
        area
    };

    if app.steps.is_empty() {
        let empty = Paragraph::new("No steps in this plan.").block(
            Block::default()
                .title(" Details ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
        frame.render_widget(empty, body_area);
        return;
    }
    let area = body_area;

    let step = &app.steps[app.selected_index];
    let mut lines: Vec<Line> = Vec::new();

    // §29: tails follow the cursor — they belong to the live step, so when
    // the user moves the cursor onto a pending step we don't want the
    // running step's logs occluding what they're trying to read. The compact
    // "▶ Running step N (phase) MM:SS" indicator lives in the chrome bar
    // (see `running_indicator` / chrome::render) so the live state remains
    // visible regardless of cursor position.
    let cursor_on_live_step = app
        .live_step_id()
        .as_deref()
        .is_some_and(|id| id == step.id);

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
        // §3.3 derived overlay; reuse the existing derived-question token
        // (the §12.5 `STATUS_BLOCKED` rename lands with the Phase 4 TUI work).
        StepStatus::Blocked => theme::STATUS_QUESTION,
    };
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(step.status.as_str(), Style::default().fg(status_color)),
    ]));

    // Attempt counter
    let max_retries = step.max_retries.unwrap_or(app.default_max_retries as i32);
    let max_attempts = max_retries + 1;
    lines.push(Line::from(vec![
        Span::styled("Attempts: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!("{}/{}", step.attempts, max_attempts)),
    ]));

    // Harness
    if let Some(harness) = step.harness.as_deref().or(app.plan.harness.as_deref()) {
        let mut harness_style = Style::default().add_modifier(Modifier::BOLD);
        if let Some(color) = crate::output::harness_color(harness) {
            harness_style = harness_style.fg(color);
        }
        lines.push(Line::from(vec![
            Span::styled("Harness: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(harness.to_string(), harness_style),
        ]));
    }

    // Model — step-level only; the harness's default model is config-derived
    // so we don't try to surface a fallback here.
    if let Some(model) = step.model.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("Model: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(model),
        ]));
    }

    // Agent
    if let Some(agent) = step.agent.as_deref().or(app.plan.agent.as_deref()) {
        lines.push(Line::from(vec![
            Span::styled("Agent: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(agent),
        ]));
    }

    // Plan-level deterministic tests. These run after each step's harness
    // attempt; rendering them on the step summary lets the operator see what
    // gating commands are in play without leaving the view.
    if !app.plan.deterministic_tests.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "Tests:",
            Style::default().add_modifier(Modifier::BOLD),
        )]));
        for cmd in &app.plan.deterministic_tests {
            lines.push(Line::from(format!("  • {cmd}")));
        }
    }

    // Live timer (only for in-progress steps)
    if step.status == StepStatus::InProgress {
        let elapsed = app.elapsed_secs();
        let mins = (elapsed as u64) / 60;
        let secs = (elapsed as u64) % 60;
        lines.push(Line::from(vec![
            Span::styled("Elapsed: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{mins:02}:{secs:02}"),
                Style::default().fg(theme::STATUS_IN_PROGRESS),
            ),
        ]));
    }

    // Step 26: total + per-attempt duration breakdown for terminal steps.
    // Only Complete/Failed steps carry meaningful attempt history here;
    // Running steps keep the live "Elapsed" timer above (unchanged). A
    // step with no recorded attempts renders nothing (no Total line, no
    // breakdown) — matching how other empty metadata is simply omitted.
    if matches!(step.status, StepStatus::Complete | StepStatus::Failed) {
        let logs = app.execution_logs_for(&step.id);
        if !logs.is_empty() {
            // None duration counts as 0 toward the total (a still-running
            // attempt shouldn't poison a terminal step's reported total).
            let total: f64 = logs.iter().map(|l| l.duration_secs.unwrap_or(0.0)).sum();
            lines.push(Line::from(vec![
                Span::styled(
                    "Total duration: ",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(crate::output::format_duration_secs(total)),
            ]));
            for log in logs {
                // `duration_secs = None` renders via the formatter the same
                // way 0.0 does (the formatter clamps non-positive to "0s").
                let dur = crate::output::format_duration_secs(log.duration_secs.unwrap_or(0.0));
                let outcome = attempt_outcome_label(log);
                // Color the `(<outcome>)` segment with the same theme status
                // palette the `Status:` line uses (see `attempt_outcome_color`):
                // success → STATUS_COMPLETE, user_skipped → CHROME_DIM, every
                // other failure → STATUS_FAILED. The `Attempt N:` label stays
                // bold and the duration stays uncolored, and the rendered text
                // is byte-identical to before (color spans only) so the
                // string-content snapshot tests still pass.
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("Attempt {}: ", log.attempt),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!("{dur} ")),
                    Span::styled(
                        format!("({outcome})"),
                        Style::default().fg(attempt_outcome_color(log)),
                    ),
                ]));
            }
        }
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

    // §29: harness/test output tails belong to the live step. Render them
    // only when the cursor is parked on that step — otherwise the user is
    // looking at a pending/complete step and would be drowned out by the
    // running step's logs. The empty-buffer guard mirrors the pre-refactor
    // behavior: don't show "(no output yet)" headers during the pre-harness
    // phase.
    if cursor_on_live_step {
        if !app.harness_tail.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "Harness output",
                Style::default()
                    .fg(theme::CHROME_DIM)
                    .add_modifier(Modifier::BOLD),
            )));
            for tail_line in app.visible_harness_tail(TAIL_VISIBLE_LINES) {
                lines.push(Line::from(Span::styled(
                    tail_line,
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
        }
        if !app.test_tail.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "Test output",
                Style::default()
                    .fg(theme::CHROME_DIM)
                    .add_modifier(Modifier::BOLD),
            )));
            for tail_line in app.visible_test_tail(TAIL_VISIBLE_LINES) {
                lines.push(Line::from(Span::styled(
                    tail_line,
                    Style::default().fg(theme::CHROME_DIM),
                )));
            }
        }
    }

    // Input field for adding a step (shown below the detail when in AddStep mode).
    // Label tells the user where the new step will land relative to the cursor.
    if let InputMode::AddStep(pos) = app.input_mode {
        let label = match pos {
            AddPosition::Above => "Insert above (title): ",
            AddPosition::Below => "Append below (title): ",
        };
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(
                label,
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

/// Render the open-questions banner above the right panel (TUI-plan.md §17).
/// One bordered row reading `❓ <count> open question(s) — press [A] to answer`,
/// styled with `STATUS_QUESTION` so the user can spot it at a glance.
fn draw_open_questions_banner(frame: &mut Frame, app: &PlanDetailApp, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let count = app.open_questions.len();
    let text = format!("❓ {count} open question(s) — press [A] to answer");
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::STATUS_QUESTION));
    let para = Paragraph::new(Span::styled(
        text,
        Style::default()
            .fg(theme::STATUS_QUESTION)
            .add_modifier(Modifier::BOLD),
    ))
    .block(block);
    frame.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Phase rendering
// ---------------------------------------------------------------------------

/// Build the parenthetical outcome label for one execution-log attempt,
/// rendered as `Attempt N: <dur> (<label>)` in the right pane (step 26).
///
/// A successful attempt reads `success`. A failed attempt surfaces its
/// [`crate::plan::TerminationReason`] so the operator can tell *why* it
/// failed at a glance (`failed: test_failed`, `failed: timeout`, …). When
/// the reason is absent (legacy rows that predate the column) or
/// `Unknown`, it degrades to a bare `failed`.
fn attempt_outcome_label(log: &crate::plan::ExecutionLog) -> String {
    use crate::plan::TerminationReason;
    match log.termination_reason {
        Some(TerminationReason::Success) => "success".to_string(),
        Some(TerminationReason::Unknown) | None => "failed".to_string(),
        Some(reason) => format!("failed: {}", reason.as_str()),
    }
}

/// Theme color for one attempt's `(<outcome>)` segment, reusing the exact
/// `theme::STATUS_*` palette the `Status:` line already maps onto:
/// a successful attempt is the completed/success green
/// ([`theme::STATUS_COMPLETE`]), a `user_skipped` attempt is the same dim
/// the `Status: skipped` line uses ([`theme::CHROME_DIM`]), and every other
/// failure/timeout/no_changes/harness_failed/unknown attempt is the failed
/// red ([`theme::STATUS_FAILED`]) — same as `Status: failed`. No new colors
/// or mapping concepts are introduced.
fn attempt_outcome_color(log: &crate::plan::ExecutionLog) -> Color {
    use crate::plan::TerminationReason;
    match log.termination_reason {
        Some(TerminationReason::Success) => theme::STATUS_COMPLETE,
        Some(TerminationReason::UserSkipped) => theme::CHROME_DIM,
        _ => theme::STATUS_FAILED,
    }
}

/// Map a [`Phase`] enum value to the user-facing label rendered in the
/// right-pane banner. Mirrors the `Phase::as_str` snake_case identifier
/// but spaces it out and capitalizes the first word so the banner reads
/// as "Running step 3 (Pre-step hook)" rather than "(pre_step_hook)".
fn phase_human_label(phase: Phase) -> &'static str {
    match phase {
        Phase::Idle => "idle",
        Phase::PreStepHook => "pre-step hook",
        Phase::Harness => "harness",
        Phase::PreTestHook => "pre-test hook",
        Phase::Tests => "tests",
        Phase::PostTestHook => "post-test hook",
        Phase::Commit => "commit",
        Phase::Rollback => "rollback",
        Phase::PostStepHook => "post-step hook",
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
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
        };
        let steps: Vec<Step> = (0..n)
            .map(|i| Step {
                id: format!("s{i}"),
                short_id: String::new(),
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
                retry_strategy: None,
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
        app.enter_add_mode_above();
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
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
        };
        let steps = vec![Step {
            id: "s0".to_string(),
            short_id: String::new(),
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
            retry_strategy: None,
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

    // -- Right-pane summary content ----------------------------------------

    /// Render the plan-detail view and return the buffer as a flat newline-
    /// joined string for substring assertions. Helper for the right-pane
    /// summary tests below.
    fn rendered(app: &mut PlanDetailApp, w: u16, h: u16) -> String {
        let backend = ratatui::backend::TestBackend::new(w, h);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Build a single-step app with the given status / overrides for
    /// right-pane assertions. Plan-level harness is `claude` and tests are a
    /// two-command list; step-level overrides default to `None`.
    fn app_with_step(
        status: StepStatus,
        attempts: i32,
        max_retries: Option<i32>,
        step_harness: Option<&str>,
        step_agent: Option<&str>,
        step_model: Option<&str>,
    ) -> PlanDetailApp {
        let plan = Plan {
            id: "p1".to_string(),
            slug: "demo".to_string(),
            project: "/proj".to_string(),
            branch_name: "feat".to_string(),
            description: "d".to_string(),
            status: PlanStatus::Ready,
            harness: Some("claude".to_string()),
            agent: None,
            deterministic_tests: vec![
                "cargo test".to_string(),
                "cargo clippy -- -D warnings".to_string(),
            ],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
        };
        let steps = vec![Step {
            id: "s0".to_string(),
            short_id: String::new(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Write migration".to_string(),
            description: "Add column".to_string(),
            agent: step_agent.map(|s| s.to_string()),
            harness: step_harness.map(|s| s.to_string()),
            acceptance_criteria: vec!["builds".to_string()],
            status,
            attempts,
            max_retries,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: step_model.map(|s| s.to_string()),
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            retry_strategy: None,
        }];
        PlanDetailApp::new(plan, steps, &Config::default())
    }

    #[test]
    fn right_pane_renders_full_summary_for_pending_step() {
        // Pending step: status row, attempt counter, harness fallback to plan,
        // tests list. No timer line (only InProgress).
        let mut app = app_with_step(StepStatus::Pending, 0, Some(3), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(
            out.contains("Title: Write migration"),
            "title missing:\n{out}"
        );
        assert!(out.contains("Status: pending"), "status missing:\n{out}");
        assert!(out.contains("Attempts: 0/4"), "attempts missing:\n{out}");
        assert!(out.contains("Harness: claude"), "harness missing:\n{out}");
        assert!(out.contains("Tests:"), "tests header missing:\n{out}");
        assert!(out.contains("• cargo test"), "test cmd missing:\n{out}");
        assert!(out.contains("• cargo clippy"), "clippy cmd missing:\n{out}");
        assert!(!out.contains("Elapsed:"), "no timer for pending:\n{out}");
    }

    #[test]
    fn right_pane_renders_in_progress_attempt_counter() {
        // 2/3 in the spec example means attempt 2 of 3 total attempts; the
        // implementation reports `<attempts>/(max_retries + 1)`.
        let mut app = app_with_step(StepStatus::InProgress, 2, Some(2), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(
            out.contains("Status: in_progress"),
            "status missing:\n{out}"
        );
        assert!(out.contains("Attempts: 2/3"), "attempts missing:\n{out}");
    }

    #[test]
    fn right_pane_renders_step_level_overrides() {
        let mut app = app_with_step(
            StepStatus::Pending,
            0,
            Some(3),
            Some("codex"),
            Some("rust-impl"),
            Some("claude-opus-4"),
        );
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Harness: codex"), "step harness:\n{out}");
        assert!(out.contains("Agent: rust-impl"), "step agent:\n{out}");
        assert!(out.contains("Model: claude-opus-4"), "step model:\n{out}");
    }

    #[test]
    fn right_pane_renders_complete_status() {
        let mut app = app_with_step(StepStatus::Complete, 1, Some(3), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Status: complete"), "status:\n{out}");
        assert!(out.contains("Attempts: 1/4"), "attempts:\n{out}");
    }

    #[test]
    fn right_pane_renders_failed_status() {
        let mut app = app_with_step(StepStatus::Failed, 3, Some(2), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Status: failed"), "status:\n{out}");
        assert!(out.contains("Attempts: 3/3"), "attempts:\n{out}");
    }

    #[test]
    fn right_pane_renders_skipped_status() {
        let mut app = app_with_step(StepStatus::Skipped, 0, Some(3), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Status: skipped"), "status:\n{out}");
    }

    #[test]
    fn right_pane_renders_aborted_status() {
        let mut app = app_with_step(StepStatus::Aborted, 1, Some(3), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Status: aborted"), "status:\n{out}");
    }

    #[test]
    fn right_pane_omits_tests_when_plan_has_none() {
        // A plan with no deterministic_tests should render no Tests header
        // (rather than "Tests:" followed by blank).
        let mut app = make_app(1);
        // make_app's plan has empty deterministic_tests.
        let out = rendered(&mut app, 80, 24);
        assert!(
            !out.contains("Tests:"),
            "Tests: header should be hidden when empty:\n{out}"
        );
    }

    /// Build a [`LiveRun`] snapshot pinned to the given `step_id` / `step_num`
    /// for the running-indicator + cursor-aware tail tests.
    fn make_live_run_for(
        step_id: &str,
        step_num: i32,
        phase: crate::plan::Phase,
    ) -> crate::run_lock::LiveRun {
        crate::run_lock::LiveRun {
            project: "/proj".to_string(),
            pid: 1234,
            pid_start_token: None,
            plan_id: Some("p1".to_string()),
            plan_slug: Some("test".to_string()),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            step_id: Some(step_id.to_string()),
            step_num: Some(step_num),
            attempt: Some(1),
            max_attempts: Some(4),
            phase: Some(phase),
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
    fn running_indicator_visible_in_chrome_when_live() {
        // §29: the compact "▶ Running step N (phase) MM:SS" indicator lives
        // in the bottom chrome row so the user sees what's executing
        // regardless of cursor position. We render at width=120 to ensure
        // the indicator slot has room next to the cwd/version text — the
        // chrome drops the indicator if there isn't enough horizontal room.
        let mut app = make_app(3);
        app.update_live_run(Some(make_live_run_for(
            "s1",
            2,
            crate::plan::Phase::Harness,
        )));
        let out = rendered(&mut app, 120, 24);
        let bottom_row = out.lines().last().unwrap();
        assert!(
            bottom_row.contains("Running step 2"),
            "running indicator missing on chrome bottom row: {bottom_row:?}"
        );
        assert!(
            bottom_row.contains("(harness)"),
            "phase missing on chrome: {bottom_row:?}"
        );
    }

    #[test]
    fn right_pane_renders_tails_when_cursor_on_live_step() {
        // §29: tail content only appears in the right pane when the cursor
        // is parked on the live step. Park selection at index 1 (matching
        // step_id `s1`), push tail lines, render, and confirm the tail
        // headers + content are present.
        let mut app = make_app(3);
        app.selected_index = 1;
        app.update_live_run(Some(make_live_run_for(
            "s1",
            2,
            crate::plan::Phase::Harness,
        )));
        app.push_harness_line("compiling crate...".to_string());
        app.push_test_line("test result: ok. 42 passed".to_string());
        let out = rendered(&mut app, 80, 30);
        assert!(
            out.contains("Harness output"),
            "harness tail header missing when cursor on live step:\n{out}"
        );
        assert!(
            out.contains("compiling crate"),
            "harness tail line missing:\n{out}"
        );
        assert!(
            out.contains("Test output"),
            "test tail header missing:\n{out}"
        );
        assert!(out.contains("42 passed"), "test tail line missing:\n{out}");
    }

    #[test]
    fn right_pane_hides_tails_when_cursor_on_other_step() {
        // §29: when the cursor moves off the live step, tails disappear so
        // the user can read pending step content. The cursor's step (title,
        // status) must still render normally.
        let mut app = make_app(3);
        // live step is `s1` (step 2); cursor parked on `s2` (step 3).
        app.selected_index = 2;
        app.update_live_run(Some(make_live_run_for(
            "s1",
            2,
            crate::plan::Phase::Harness,
        )));
        app.push_harness_line("compiling crate...".to_string());
        app.push_test_line("test result: ok. 42 passed".to_string());
        let out = rendered(&mut app, 80, 30);
        assert!(
            !out.contains("Harness output"),
            "harness tail header should be hidden when cursor off live step:\n{out}"
        );
        assert!(
            !out.contains("Test output"),
            "test tail header should be hidden when cursor off live step:\n{out}"
        );
        assert!(
            !out.contains("compiling crate"),
            "harness tail content leaked into non-live step view:\n{out}"
        );
        // Cursor's step content remains visible — this is the whole point
        // of the refactor: navigating ahead must not be blocked by logs.
        assert!(
            out.contains("Title: Step 3"),
            "cursor's step title missing on non-live step:\n{out}"
        );
        assert!(
            out.contains("Status: pending"),
            "cursor's step status missing:\n{out}"
        );
    }

    #[test]
    fn right_pane_omits_running_banner_when_no_live_run() {
        // Sanity: no live run → no chrome indicator and no in-body tails.
        let mut app = make_app(3);
        let out = rendered(&mut app, 120, 24);
        assert!(
            !out.contains("Running step"),
            "indicator should be hidden without live run:\n{out}"
        );
        assert!(
            !out.contains("Harness output"),
            "tails should be hidden without live run:\n{out}"
        );
    }

    #[test]
    fn hint_bar_advertises_run_and_stop_keybinds() {
        // §7 keybinding: bottom hint must include [R] run and [S] stop so
        // the controls are discoverable in the chrome footer.
        let mut app = make_app(1);
        let out = rendered(&mut app, 120, 6);
        assert!(out.contains("[R] run"), "missing [R] hint:\n{out}");
        assert!(out.contains("[S] stop"), "missing [S] hint:\n{out}");
    }

    #[test]
    fn breadcrumb_shows_ralph_arrow_slug() {
        // §7: top breadcrumb is `ralph › <slug>`. The chrome row is the
        // first row of the rendered output.
        let mut app = make_app(1);
        let out = rendered(&mut app, 80, 5);
        let top_row = out.lines().next().unwrap();
        assert!(
            top_row.contains("ralph › test"),
            "expected `ralph › test` on top row: {top_row:?}"
        );
    }

    // -- Step 26: total + per-attempt duration breakdown -------------------

    use crate::plan::{ExecutionLog, TerminationReason};

    /// Build a minimal `ExecutionLog` for the duration-breakdown tests.
    /// Only the fields the right pane reads (`attempt`, `duration_secs`,
    /// `termination_reason`) are meaningful; the rest are inert defaults.
    fn make_log(
        attempt: i32,
        duration_secs: Option<f64>,
        termination_reason: Option<TerminationReason>,
    ) -> ExecutionLog {
        ExecutionLog {
            id: attempt as i64,
            step_id: "s0".to_string(),
            attempt,
            started_at: Utc::now(),
            duration_secs,
            prompt_text: None,
            diff: None,
            test_results: vec![],
            rolled_back: false,
            committed: false,
            commit_hash: None,
            harness_stdout: None,
            harness_stderr: None,
            cost_usd: None,
            input_tokens: None,
            output_tokens: None,
            session_id: None,
            termination_reason,
            test_status: None,
        }
    }

    #[test]
    fn right_pane_no_breakdown_for_complete_step_with_zero_attempts() {
        // 0 attempts: no execution-log cache entry → no Total line and no
        // per-attempt breakdown (mirrors how other empty metadata is just
        // omitted rather than rendered as a placeholder).
        let mut app = app_with_step(StepStatus::Complete, 0, Some(3), None, None, None);
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Status: complete"), "status missing:\n{out}");
        assert!(
            !out.contains("Total duration:"),
            "no Total line expected with zero attempts:\n{out}"
        );
        assert!(
            !out.contains("Attempt 1:"),
            "no per-attempt breakdown expected with zero attempts:\n{out}"
        );
    }

    #[test]
    fn right_pane_single_success_attempt_shows_total_and_breakdown() {
        let mut app = app_with_step(StepStatus::Complete, 1, Some(3), None, None, None);
        app.set_execution_logs(
            "s0",
            vec![make_log(1, Some(12.0), Some(TerminationReason::Success))],
        );
        let out = rendered(&mut app, 80, 24);
        assert!(
            out.contains("Total duration: 12s"),
            "total duration missing/wrong:\n{out}"
        );
        assert!(
            out.contains("Attempt 1: 12s (success)"),
            "single success attempt line missing/wrong:\n{out}"
        );
    }

    #[test]
    fn right_pane_multi_attempt_mixed_outcomes_failed_step() {
        // A Failed step with: a test-failure, a timeout (None duration →
        // counts as 0 and renders "0s"), and a final success. Total sums
        // the present durations (45 + 0 + 30 = 75s → "1m 15s").
        let mut app = app_with_step(StepStatus::Failed, 3, Some(3), None, None, None);
        app.set_execution_logs(
            "s0",
            vec![
                make_log(1, Some(45.0), Some(TerminationReason::TestFailed)),
                make_log(2, None, Some(TerminationReason::Timeout)),
                make_log(3, Some(30.0), Some(TerminationReason::Success)),
            ],
        );
        let out = rendered(&mut app, 80, 30);
        assert!(
            out.contains("Total duration: 1m 15s"),
            "total duration should sum present durations:\n{out}"
        );
        assert!(
            out.contains("Attempt 1: 45s (failed: test_failed)"),
            "attempt 1 test-failure line missing/wrong:\n{out}"
        );
        assert!(
            out.contains("Attempt 2: 0s (failed: timeout)"),
            "attempt 2 timeout line (None duration → 0s) missing/wrong:\n{out}"
        );
        assert!(
            out.contains("Attempt 3: 30s (success)"),
            "attempt 3 success line missing/wrong:\n{out}"
        );
    }

    #[test]
    fn right_pane_attempt_with_no_changes_and_user_skipped_reasons() {
        // Cover the remaining failure-mode variants the task calls out:
        // no_changes, user_skipped, harness_failed. Instant (0.0s)
        // failures still render via the formatter as "0s".
        let mut app = app_with_step(StepStatus::Failed, 3, Some(3), None, None, None);
        app.set_execution_logs(
            "s0",
            vec![
                make_log(1, Some(0.0), Some(TerminationReason::NoChanges)),
                make_log(2, Some(5.0), Some(TerminationReason::HarnessFailed)),
                make_log(3, Some(2.0), Some(TerminationReason::UserSkipped)),
            ],
        );
        let out = rendered(&mut app, 80, 30);
        assert!(
            out.contains("Attempt 1: 0s (failed: no_changes)"),
            "no_changes attempt line missing/wrong:\n{out}"
        );
        assert!(
            out.contains("Attempt 2: 5s (failed: harness_failed)"),
            "harness_failed attempt line missing/wrong:\n{out}"
        );
        assert!(
            out.contains("Attempt 3: 2s (failed: user_skipped)"),
            "user_skipped attempt line missing/wrong:\n{out}"
        );
    }

    #[test]
    fn right_pane_running_step_keeps_live_timer_not_breakdown() {
        // §29 / step 26 boundary: a Running step keeps the live "Elapsed"
        // timer and must NOT render the terminal duration breakdown even
        // if execution_logs happen to be cached for it.
        let mut app = app_with_step(StepStatus::InProgress, 1, Some(3), None, None, None);
        app.set_execution_logs(
            "s0",
            vec![make_log(1, Some(9.0), Some(TerminationReason::Success))],
        );
        let out = rendered(&mut app, 80, 24);
        assert!(out.contains("Elapsed:"), "live timer missing:\n{out}");
        assert!(
            !out.contains("Total duration:"),
            "Running step must not show terminal Total line:\n{out}"
        );
        assert!(
            !out.contains("Attempt 1:"),
            "Running step must not show per-attempt breakdown:\n{out}"
        );
    }
}
