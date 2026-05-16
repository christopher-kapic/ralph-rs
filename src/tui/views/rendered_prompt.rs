// Rendered-prompt preview sub-view (prompt-overhaul, step 14).
//
// A read-only sub-view over the step-detail screen that shows the
// fully-assembled prompt exactly as the agent would receive it for the
// current step, as produced by [`crate::prompt::build_step_prompt`]. Entered
// from step_detail when the `StepPrompt` pane is focused and the user presses
// `l` / `→`.
//
// When the step has multiple `execution_logs` rows (one per attempt), `j`/`k`
// (and `↑`/`↓`) navigate between attempts; each attempt re-renders
// `build_step_prompt` with the retry context the executor would have built
// for that attempt (attempt 1 = no retry context; later attempts = retry
// context derived from the *previous* attempt's stored log). Zero
// `execution_logs` still works: it shows the attempt-1 prompt with no retry
// context.
//
// Like the other step-level sub-views (plan_hooks/step_hooks/step_tags) the
// module is split into a pure state machine (`RenderedPromptApp` +
// `handle_key`) and a separate renderer (`render`) so the navigation /
// assembly logic is unit-testable without a real terminal. There are no
// storage write-throughs — the dispatcher (`run_rendered_prompt_tui` in
// src/commands/run.rs) only builds the per-attempt prompts up front and pops
// when the state machine asks.

use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::plan::{AnsweredQuestion, ExecutionLog, Plan, RetryStrategy, Step, TerminationReason};
use crate::prompt::{self, Prompts, RetryContext};
use crate::tui::help::{self, HelpState};
use crate::tui::theme;

// ---------------------------------------------------------------------------
// Per-attempt model
// ---------------------------------------------------------------------------

/// One attempt's fully-assembled prompt plus the metadata needed for the
/// `Attempt N of M (started <relative-time>)` header line. `started_at` is
/// `None` for the synthetic attempt-1 entry shown when the step has zero
/// `execution_logs` rows (there is no log row to source a timestamp from).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptPrompt {
    /// 1-based attempt number (matches `execution_logs.attempt`).
    pub attempt: i32,
    /// When this attempt started, from `execution_logs.started_at`. `None`
    /// for the zero-logs synthetic attempt-1 entry.
    pub started_at: Option<DateTime<Utc>>,
    /// The fully-assembled prompt as `build_step_prompt` produced it for this
    /// attempt's context.
    pub prompt: String,
}

/// What [`RenderedPromptApp::handle_key`] returns each turn. The dispatcher
/// loop runs the side effect (pop the sub-view) and keeps looping on
/// `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed; no side effect required.
    Pending,
    /// User pressed `<esc>` / `h` / `←` / `q` / Ctrl-C — pop back to
    /// step-detail.
    Pop,
}

// ---------------------------------------------------------------------------
// Retry-context reconstruction
// ---------------------------------------------------------------------------

/// Reconstruct the [`RetryContext`] that the executor would have built for
/// attempt number `attempt` (1-based), given the chronological list of this
/// step's execution logs and the resolved attempt budget.
///
/// **Source of truth:** `src/executor.rs` (`execute_step`, ~lines 716-727 and
/// the `prev_diff` / `prev_test_output` / `prev_files_modified` assignments at
/// ~lines 1097-1099 and ~1551-1553). The executor builds attempt N's retry
/// context from the *previous* attempt's outcome:
///
/// - `previous_diff` = the previous attempt's diff
/// - `previous_test_output` = the previous attempt's test results joined with `\n`
/// - `files_modified` = the files changed during the previous attempt
///
/// Attempt 1 has no retry context (returns `None`).
///
/// Deviation from the executor that cannot be avoided here: the executor
/// sources `files_modified` from a *live* `git status` taken during the
/// previous attempt (`git::get_all_changed_files`). That state is not
/// persisted in `execution_logs`, so for a historical attempt we cannot
/// reproduce it exactly. We reconstruct the file list from the stored diff's
/// `diff --git a/<path> b/<path>` headers — the closest faithful
/// approximation available from persisted data. The other retry-context
/// fields (`previous_diff`, `previous_test_output`, `attempt`,
/// `max_attempts`) match the executor exactly *for the inputs given*.
///
/// Fidelity caveat: this is a "what would be sent now" preview. It
/// re-renders from *current* plan/project/step text and the *current*
/// answered-question set, so a historical attempt's preview reflects edits
/// made after that attempt ran — it is not the point-in-time prompt the
/// agent actually received. The exact prompt sent for each attempt is
/// persisted verbatim in `execution_logs.prompt_text`; this view
/// deliberately re-assembles instead, so layer edits are visible.
/// `strategy` is the step's resolved [`RetryStrategy`] (step > plan >
/// default `Keep`). It scopes the reconstruction exactly as the executor
/// does (Step 22): under `Rollback` the diff/files are included (the agent
/// no longer sees the reverted work); under `Keep` they are omitted (the
/// work is still on disk). `previous_failure_reason` is reconstructed from
/// the previous attempt's `termination_reason` under both strategies.
pub fn build_retry_context_for_attempt(
    attempt: i32,
    max_attempts: i32,
    logs: &[ExecutionLog],
    strategy: RetryStrategy,
) -> Option<RetryContext> {
    if attempt <= 1 {
        return None;
    }
    // The executor derives attempt N's retry context from attempt N-1's
    // recorded outcome. Logs are ordered by `attempt` ASC, so the previous
    // attempt is the row whose `attempt == attempt - 1`.
    let prev = logs.iter().find(|l| l.attempt == attempt - 1)?;

    let previous_test_output = if prev.test_results.is_empty() {
        None
    } else {
        // Mirrors `prev_test_output = Some(test_results.join("\n"))` /
        // `Some(test_output_summary)` in src/executor.rs.
        Some(prev.test_results.join("\n"))
    };

    // Strategy-scoped, mirroring src/executor.rs's RetryContext build:
    // Rollback re-sends the (now reverted) diff/files; Keep omits them
    // because the work is still on disk for the agent to `git diff`.
    let (previous_diff, files_modified) = match strategy {
        RetryStrategy::Rollback => (prev.diff.clone(), files_from_diff(prev.diff.as_deref())),
        RetryStrategy::Keep => (None, Vec::new()),
    };

    let previous_failure_reason = prev.termination_reason.map(|r| {
        match r {
            TerminationReason::TestFailed => "tests failed",
            TerminationReason::NoChanges => "no changes produced",
            TerminationReason::HarnessFailed => "harness exited non-zero",
            TerminationReason::HookFailed => "a lifecycle hook failed",
            TerminationReason::Timeout => "the harness timed out",
            other => other.as_str(),
        }
        .to_string()
    });

    Some(RetryContext {
        attempt,
        max_attempts,
        previous_diff,
        previous_test_output,
        files_modified,
        previous_failure_reason,
    })
}

/// Best-effort reconstruction of the previous attempt's modified-file list
/// from its stored unified diff. Parses `diff --git a/<old> b/<new>` headers
/// and returns the `b/` (post-image) path for each, deduplicated in first-seen
/// order. Returns an empty vec when there is no diff.
///
/// This is an approximation of the executor's live `git status` snapshot (see
/// [`build_retry_context_for_attempt`] for why an exact reproduction is not
/// possible from persisted data).
fn files_from_diff(diff: Option<&str>) -> Vec<String> {
    let Some(diff) = diff else {
        return Vec::new();
    };
    let mut files: Vec<String> = Vec::new();
    for line in diff.lines() {
        let Some(rest) = line.strip_prefix("diff --git ") else {
            continue;
        };
        // `rest` is `a/<old> b/<new>`. The post-image path is everything
        // after the last ` b/`. Paths with spaces are uncommon in this
        // codebase's diffs; the executor's git-status path can't disambiguate
        // them either, so a simple split is acceptable for a preview.
        let path = match rest.rsplit_once(" b/") {
            Some((_, b)) => b.to_string(),
            None => rest.to_string(),
        };
        if !files.contains(&path) {
            files.push(path);
        }
    }
    files
}

// ---------------------------------------------------------------------------
// Sub-view state
// ---------------------------------------------------------------------------

/// Sub-view state. Independent of rendering and crossterm input so attempt
/// navigation + scrolling can be unit-tested without a terminal.
pub struct RenderedPromptApp {
    /// Display slug of the parent plan, used in the title bar.
    pub plan_slug: String,
    /// Display label for the step (e.g. `#3 — Step title`), used in the
    /// title bar so the user always knows which step they're scoped to.
    pub step_label: String,
    /// One entry per attempt, in chronological order (oldest first). Always
    /// non-empty: with zero `execution_logs` it holds a single synthetic
    /// attempt-1 entry.
    pub attempts: Vec<AttemptPrompt>,
    /// Index into `attempts` for the attempt currently shown. Starts on the
    /// most recent attempt (`len - 1`) so the user lands on what the agent
    /// would receive on the next run.
    pub selected: usize,
    /// Vertical scroll offset (in lines) into the current attempt's prompt.
    /// Reset to 0 whenever the selected attempt changes.
    pub scroll: u16,
    /// Body viewport height recorded during the most recent `render`. Used to
    /// clamp `scroll` so the user can't scroll past the end. Zero before the
    /// first frame.
    pub last_body_height: u16,
    /// Line count of the current attempt's prompt recorded during the most
    /// recent `render`. Paired with `last_body_height` for scroll clamping.
    pub last_line_count: u16,
    /// Help-overlay state. `?` toggles visibility; while visible the per-view
    /// input handler is skipped (TUI-plan.md §15).
    pub help: HelpState,
}

impl RenderedPromptApp {
    /// Build the sub-view. `attempts` must be non-empty (the dispatcher
    /// guarantees at least the synthetic attempt-1 entry when the step has no
    /// execution logs). The cursor starts on the most recent attempt.
    pub fn new(plan_slug: String, step_label: String, attempts: Vec<AttemptPrompt>) -> Self {
        debug_assert!(
            !attempts.is_empty(),
            "RenderedPromptApp requires at least one attempt entry"
        );
        let selected = attempts.len().saturating_sub(1);
        Self {
            plan_slug,
            step_label,
            attempts,
            selected,
            scroll: 0,
            last_body_height: 0,
            last_line_count: 0,
            help: HelpState::new(),
        }
    }

    /// Assemble the full per-attempt prompt list for `step` by calling
    /// [`prompt::build_step_prompt`] once per attempt with that attempt's
    /// retry context. This is the single place the preview is produced — it
    /// passes the *real* plan/step/all_steps/agent/prompt-layers, so the
    /// output is byte-identical to what the executor would send for the same
    /// inputs.
    ///
    /// `logs` is the step's execution logs in chronological order (as
    /// returned by `storage::list_execution_logs_for_step`). When empty, a
    /// single attempt-1 entry with no retry context is produced.
    #[allow(clippy::too_many_arguments)]
    pub fn build_attempts(
        plan: &Plan,
        step: &Step,
        all_steps: &[Step],
        agent_name: Option<&str>,
        harness_supports_agent_file: bool,
        prompts: &Prompts,
        answered_questions: &[AnsweredQuestion],
        max_attempts: i32,
        logs: &[ExecutionLog],
    ) -> Vec<AttemptPrompt> {
        let mut out: Vec<AttemptPrompt> = Vec::new();

        if logs.is_empty() {
            // Zero execution logs → attempt 1, no retry context.
            let prompt = prompt::build_step_prompt(
                plan,
                step,
                all_steps,
                agent_name,
                None,
                harness_supports_agent_file,
                prompts,
                answered_questions,
            );
            out.push(AttemptPrompt {
                attempt: 1,
                started_at: None,
                prompt,
            });
            return out;
        }

        // Resolve the step's retry strategy once so each per-attempt preview
        // scopes the diff/files exactly as the executor would (Step 22).
        let strategy = step.effective_retry_strategy(plan);
        for log in logs {
            let retry = build_retry_context_for_attempt(log.attempt, max_attempts, logs, strategy);
            let prompt = prompt::build_step_prompt(
                plan,
                step,
                all_steps,
                agent_name,
                retry.as_ref(),
                harness_supports_agent_file,
                prompts,
                answered_questions,
            );
            out.push(AttemptPrompt {
                attempt: log.attempt,
                started_at: Some(log.started_at),
                prompt,
            });
        }
        out
    }

    /// The attempt currently shown. Always `Some` because `attempts` is
    /// non-empty by construction.
    pub fn current(&self) -> &AttemptPrompt {
        &self.attempts[self.selected.min(self.attempts.len() - 1)]
    }

    /// Whether there is more than one attempt to navigate between.
    pub fn has_multiple_attempts(&self) -> bool {
        self.attempts.len() > 1
    }

    /// Move to the next (newer) attempt, resetting scroll. No-op at the end.
    pub fn select_next_attempt(&mut self) {
        if self.selected + 1 < self.attempts.len() {
            self.selected += 1;
            self.scroll = 0;
        }
    }

    /// Move to the previous (older) attempt, resetting scroll. No-op at the
    /// start.
    pub fn select_prev_attempt(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
            self.scroll = 0;
        }
    }

    /// Maximum scroll offset given the last observed viewport. Clamps so the
    /// final line stays on screen.
    fn max_scroll(&self) -> u16 {
        self.last_line_count
            .saturating_sub(self.last_body_height.max(1))
    }

    /// Scroll the prompt body down one line, clamped to the end.
    pub fn scroll_down(&mut self) {
        let max = self.max_scroll();
        self.scroll = self.scroll.saturating_add(1).min(max);
    }

    /// Scroll the prompt body up one line.
    pub fn scroll_up(&mut self) {
        self.scroll = self.scroll.saturating_sub(1);
    }

    /// Page the prompt body down by roughly one viewport.
    pub fn page_down(&mut self) {
        let page = self.last_body_height.max(1);
        let max = self.max_scroll();
        self.scroll = self.scroll.saturating_add(page).min(max);
    }

    /// Page the prompt body up by roughly one viewport.
    pub fn page_up(&mut self) {
        let page = self.last_body_height.max(1);
        self.scroll = self.scroll.saturating_sub(page);
    }

    /// Jump to the top of the prompt body.
    pub fn scroll_to_top(&mut self) {
        self.scroll = 0;
    }

    /// Jump to the bottom of the prompt body.
    pub fn scroll_to_bottom(&mut self) {
        self.scroll = self.max_scroll();
    }

    /// Pure key handler. The dispatcher executes the side effect of
    /// [`Outcome::Pop`] and loops on [`Outcome::Pending`].
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // §15 help overlay: route `?` toggle / dismissal first. While the
        // overlay is up the per-view bindings are skipped.
        if self.help.intercept_key(key) != help::InterceptResult::Passthrough {
            return Outcome::Pending;
        }

        // Ctrl-C is the universal escape hatch (matches the convention in
        // step_hooks/step_tags).
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Outcome::Pop;
        }

        match key.code {
            // Pop back to step-detail.
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('h') | KeyCode::Left => Outcome::Pop,

            // Attempt navigation. j/k (and ↑/↓) move between attempts when
            // there is more than one; with a single attempt they fall
            // through to scrolling so the binding is never dead.
            KeyCode::Char('j') | KeyCode::Down => {
                if self.has_multiple_attempts() {
                    self.select_next_attempt();
                } else {
                    self.scroll_down();
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.has_multiple_attempts() {
                    self.select_prev_attempt();
                } else {
                    self.scroll_up();
                }
                Outcome::Pending
            }

            // Scrolling the (potentially long) prompt body.
            KeyCode::Char('J') => {
                self.scroll_down();
                Outcome::Pending
            }
            KeyCode::Char('K') => {
                self.scroll_up();
                Outcome::Pending
            }
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.page_down();
                Outcome::Pending
            }
            KeyCode::PageUp => {
                self.page_up();
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.scroll_to_top();
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.scroll_to_bottom();
                Outcome::Pending
            }

            _ => Outcome::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Relative-time helper
// ---------------------------------------------------------------------------

/// Render a coarse "<n><unit> ago" string for the header. Resolution steps:
/// `<60s` → `Ns ago`, `<60m` → `Nm ago`, `<24h` → `Nh ago`, else `Nd ago`.
/// A future or zero delta clamps to `just now`.
fn relative_time(started: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let secs = now.signed_duration_since(started).num_seconds();
    if secs <= 0 {
        return "just now".to_string();
    }
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Render the rendered-prompt preview over the parent step-detail surface.
/// Caller is expected to have drawn the background view immediately prior;
/// the bordered block + `Clear`-equivalent full-area fill keep the overlay
/// crisp.
pub fn render(frame: &mut Frame, area: Rect, app: &mut RenderedPromptApp) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let title = format!(" Rendered prompt — {} / {} ", app.plan_slug, app.step_label);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Layout: [header(1)] [body(rest)] [hint(1)]. The header carries the
    // `Attempt N of M (started <rel>)` line; it is only meaningful with at
    // least one real attempt timestamp, but we always reserve the row so the
    // body height is stable across attempts (keeps scroll math consistent).
    let header_h: u16 = 1;
    let hint_h: u16 = 1;
    let body_h = inner.height.saturating_sub(header_h + hint_h).max(1);

    let header_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: header_h,
    };
    let body_area = Rect {
        x: inner.x,
        y: inner.y + header_h,
        width: inner.width,
        height: body_h.min(inner.height.saturating_sub(header_h)),
    };
    let hint_area = Rect {
        x: inner.x,
        y: inner.y + inner.height.saturating_sub(hint_h),
        width: inner.width,
        height: hint_h,
    };

    let total = app.attempts.len();
    let current = app.current().clone();

    // -- Header ----------------------------------------------------------
    let header_text = {
        let mut s = format!("Attempt {} of {}", current.attempt, total);
        if let Some(started) = current.started_at {
            let rel = relative_time(started, Utc::now());
            s.push_str(&format!(" (started {rel})"));
        }
        s
    };
    let header = Paragraph::new(Line::from(Span::styled(
        header_text,
        Style::default()
            .fg(theme::SELECTION)
            .add_modifier(Modifier::BOLD),
    )));
    frame.render_widget(header, header_area);

    // -- Body ------------------------------------------------------------
    // Record the metrics scroll clamping depends on, then clamp the offset
    // *before* drawing so a stale (too-large) offset from a longer prior
    // attempt can't blank the pane.
    let line_count = current.prompt.lines().count().max(1) as u16;
    app.last_body_height = body_area.height;
    app.last_line_count = line_count;
    let max_scroll = line_count.saturating_sub(body_area.height.max(1));
    if app.scroll > max_scroll {
        app.scroll = max_scroll;
    }

    let body = Paragraph::new(current.prompt.clone())
        .wrap(Wrap { trim: false })
        .scroll((app.scroll, 0));
    frame.render_widget(body, body_area);

    // -- Hint ------------------------------------------------------------
    let hint = if app.has_multiple_attempts() {
        "[j/k] attempt  [J/K] scroll  [g/G] top/bottom  [h/←/esc] back  [?] help"
    } else {
        "[j/k] scroll  [g/G] top/bottom  [h/←/esc] back  [?] help"
    };
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            hint,
            Style::default().fg(theme::CHROME_DIM),
        ))),
        hint_area,
    );

    // -- Help overlay ----------------------------------------------------
    if app.help.is_visible() {
        help::render(frame, area, &help::for_rendered_prompt());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ChangePolicy, PlanStatus, StepStatus};
    use chrono::{Duration, Utc};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn make_plan() -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "test-plan".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "feat/test".to_string(),
            description: "Build a new feature for the project".to_string(),
            status: PlanStatus::InProgress,
            harness: None,
            agent: None,
            deterministic_tests: vec!["cargo test".to_string()],
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
        }
    }

    fn make_step() -> Step {
        Step {
            id: "s1".to_string(),
            short_id: String::new(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Implement harness spawning".to_string(),
            description: "Add harness.rs with spawn_harness() function".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec!["spawn_harness() works".to_string()],
            status: StepStatus::InProgress,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
            retry_strategy: None,
        }
    }

    fn make_log(attempt: i32, started: DateTime<Utc>) -> ExecutionLog {
        ExecutionLog {
            id: attempt as i64,
            step_id: "s1".to_string(),
            attempt,
            started_at: started,
            duration_secs: Some(12.0),
            prompt_text: Some("whatever".to_string()),
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
            termination_reason: None,
            test_status: None,
        }
    }

    fn prompts_for(plan: &Plan) -> Prompts {
        // Mirror the executor: the Plan layer is the plan's description.
        Prompts {
            global: None,
            project: None,
            plan: Some(plan.description.clone()),
        }
    }

    // -- retry-context reconstruction -----------------------------------

    #[test]
    fn attempt_one_has_no_retry_context() {
        assert!(build_retry_context_for_attempt(1, 4, &[], RetryStrategy::Keep).is_none());
        assert!(build_retry_context_for_attempt(1, 4, &[], RetryStrategy::Rollback).is_none());
    }

    #[test]
    fn later_attempt_pulls_previous_log_outcome_rollback() {
        // Under Rollback the reverted diff/files are re-sent so the agent
        // can learn from work it no longer sees on disk.
        let mut l1 = make_log(1, Utc::now());
        l1.diff = Some("diff --git a/src/foo.rs b/src/foo.rs\n@@ -1 +1 @@\n-old\n+new".to_string());
        l1.test_results = vec!["FAIL test_a".to_string(), "error: boom".to_string()];
        l1.termination_reason = Some(TerminationReason::TestFailed);
        let l2 = make_log(2, Utc::now());
        let logs = vec![l1, l2];

        let ctx = build_retry_context_for_attempt(2, 4, &logs, RetryStrategy::Rollback)
            .expect("retry ctx for attempt 2");
        assert_eq!(ctx.attempt, 2);
        assert_eq!(ctx.max_attempts, 4);
        assert_eq!(
            ctx.previous_diff.as_deref(),
            Some("diff --git a/src/foo.rs b/src/foo.rs\n@@ -1 +1 @@\n-old\n+new")
        );
        // test_results joined with `\n`, exactly like the executor.
        assert_eq!(
            ctx.previous_test_output.as_deref(),
            Some("FAIL test_a\nerror: boom")
        );
        // files_modified parsed from the diff's `diff --git ... b/<path>`.
        assert_eq!(ctx.files_modified, vec!["src/foo.rs".to_string()]);
        assert_eq!(ctx.previous_failure_reason.as_deref(), Some("tests failed"));
    }

    #[test]
    fn later_attempt_keep_omits_diff_and_files_keeps_reason_and_output() {
        // Step 22: under Keep the prior work is still on disk, so the preview
        // mirrors the executor by omitting diff/files but keeping the test
        // output and a reconstructed failure reason.
        let mut l1 = make_log(1, Utc::now());
        l1.diff = Some("diff --git a/src/foo.rs b/src/foo.rs\n@@ -1 +1 @@\n-old\n+new".to_string());
        l1.test_results = vec!["FAIL test_a".to_string()];
        l1.termination_reason = Some(TerminationReason::TestFailed);
        let l2 = make_log(2, Utc::now());
        let logs = vec![l1, l2];

        let ctx = build_retry_context_for_attempt(2, 4, &logs, RetryStrategy::Keep)
            .expect("retry ctx for attempt 2");
        assert_eq!(ctx.attempt, 2);
        assert!(
            ctx.previous_diff.is_none(),
            "Keep must not re-send the diff"
        );
        assert!(
            ctx.files_modified.is_empty(),
            "Keep must not re-send the file list"
        );
        assert_eq!(ctx.previous_test_output.as_deref(), Some("FAIL test_a"));
        assert_eq!(ctx.previous_failure_reason.as_deref(), Some("tests failed"));
    }

    #[test]
    fn files_from_diff_dedupes_and_handles_none() {
        assert!(files_from_diff(None).is_empty());
        let d = "diff --git a/a.rs b/a.rs\n@@\n+x\ndiff --git a/a.rs b/a.rs\n@@\n+y\n\
                 diff --git a/b.rs b/b.rs\n@@\n+z";
        assert_eq!(
            files_from_diff(Some(d)),
            vec!["a.rs".to_string(), "b.rs".to_string()]
        );
    }

    // -- assembly equals build_step_prompt ------------------------------

    #[test]
    fn build_attempts_zero_logs_matches_build_step_prompt_attempt_one() {
        let plan = make_plan();
        let step = make_step();
        let all = vec![step.clone()];
        let prompts = prompts_for(&plan);

        let attempts = RenderedPromptApp::build_attempts(
            &plan,
            &step,
            &all,
            None,
            true,
            &prompts,
            &[],
            4,
            &[],
        );
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].attempt, 1);
        assert!(attempts[0].started_at.is_none());

        // Byte-identical to a direct build_step_prompt call with no retry ctx.
        let expected =
            prompt::build_step_prompt(&plan, &step, &all, None, None, true, &prompts, &[]);
        assert_eq!(attempts[0].prompt, expected);
    }

    #[test]
    fn build_attempts_per_attempt_matches_build_step_prompt() {
        let plan = make_plan();
        let step = make_step();
        let all = vec![step.clone()];
        let prompts = prompts_for(&plan);

        let mut l1 = make_log(1, Utc::now());
        l1.diff = Some("diff --git a/src/x.rs b/src/x.rs\n+change".to_string());
        l1.test_results = vec!["FAIL".to_string()];
        let l2 = make_log(2, Utc::now());
        let logs = vec![l1, l2];

        let attempts = RenderedPromptApp::build_attempts(
            &plan,
            &step,
            &all,
            None,
            true,
            &prompts,
            &[],
            4,
            &logs,
        );
        assert_eq!(attempts.len(), 2);

        // Attempt 1: no retry context.
        let exp1 = prompt::build_step_prompt(&plan, &step, &all, None, None, true, &prompts, &[]);
        assert_eq!(attempts[0].prompt, exp1);
        assert!(!attempts[0].prompt.contains("# Retry Context"));

        // Attempt 2: retry context reconstructed from attempt 1's log.
        // `build_attempts` resolves the strategy via
        // `step.effective_retry_strategy(plan)`; make_step/make_plan both
        // leave it None → default `Keep`, so mirror that here.
        let ctx =
            build_retry_context_for_attempt(2, 4, &logs, step.effective_retry_strategy(&plan))
                .unwrap();
        let exp2 =
            prompt::build_step_prompt(&plan, &step, &all, None, Some(&ctx), true, &prompts, &[]);
        assert_eq!(attempts[1].prompt, exp2);
        assert!(attempts[1].prompt.contains("# Retry Context"));
        assert!(attempts[1].prompt.contains("attempt 2 of 4"));
    }

    // -- attempt navigation ---------------------------------------------

    #[test]
    fn navigation_cycles_attempts_chronologically() {
        let now = Utc::now();
        let attempts = vec![
            AttemptPrompt {
                attempt: 1,
                started_at: Some(now - Duration::seconds(300)),
                prompt: "P1".to_string(),
            },
            AttemptPrompt {
                attempt: 2,
                started_at: Some(now - Duration::seconds(120)),
                prompt: "P2".to_string(),
            },
            AttemptPrompt {
                attempt: 3,
                started_at: Some(now - Duration::seconds(10)),
                prompt: "P3".to_string(),
            },
        ];
        let mut app = RenderedPromptApp::new("slug".into(), "#1 — t".into(), attempts);

        // Starts on the most recent attempt (index len-1).
        assert_eq!(app.selected, 2);
        assert_eq!(app.current().attempt, 3);

        // k moves to the previous (older) attempt.
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.current().attempt, 2);
        assert_eq!(app.handle_key(key(KeyCode::Up)), Outcome::Pending);
        assert_eq!(app.current().attempt, 1);
        // Clamped at the oldest.
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.current().attempt, 1);

        // j moves to the next (newer) attempt.
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.current().attempt, 2);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.current().attempt, 3);
        // Clamped at the newest.
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.current().attempt, 3);
    }

    #[test]
    fn changing_attempt_resets_scroll() {
        let attempts = vec![
            AttemptPrompt {
                attempt: 1,
                started_at: Some(Utc::now()),
                prompt: "a\nb\nc\nd\ne".to_string(),
            },
            AttemptPrompt {
                attempt: 2,
                started_at: Some(Utc::now()),
                prompt: "x".to_string(),
            },
        ];
        let mut app = RenderedPromptApp::new("s".into(), "l".into(), attempts);
        app.last_body_height = 1;
        app.last_line_count = 5;
        app.scroll = 3;
        app.handle_key(key(KeyCode::Char('k'))); // move to attempt 1
        assert_eq!(app.scroll, 0, "scroll resets when attempt changes");
    }

    #[test]
    fn single_attempt_jk_scrolls_instead_of_navigating() {
        let attempts = vec![AttemptPrompt {
            attempt: 1,
            started_at: None,
            prompt: (0..50)
                .map(|i| format!("line {i}"))
                .collect::<Vec<_>>()
                .join("\n"),
        }];
        let mut app = RenderedPromptApp::new("s".into(), "l".into(), attempts);
        app.last_body_height = 10;
        app.last_line_count = 50;
        assert!(!app.has_multiple_attempts());
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.scroll, 1);
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.scroll, 0);
    }

    // -- pop bindings ----------------------------------------------------

    #[test]
    fn esc_h_left_q_ctrlc_pop() {
        let mk = || {
            RenderedPromptApp::new(
                "s".into(),
                "l".into(),
                vec![AttemptPrompt {
                    attempt: 1,
                    started_at: None,
                    prompt: "p".into(),
                }],
            )
        };
        assert_eq!(mk().handle_key(key(KeyCode::Esc)), Outcome::Pop);
        assert_eq!(mk().handle_key(key(KeyCode::Char('h'))), Outcome::Pop);
        assert_eq!(mk().handle_key(key(KeyCode::Left)), Outcome::Pop);
        assert_eq!(mk().handle_key(key(KeyCode::Char('q'))), Outcome::Pop);
        assert_eq!(
            mk().handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
    }

    #[test]
    fn help_overlay_swallows_keys() {
        let mut app = RenderedPromptApp::new(
            "s".into(),
            "l".into(),
            vec![AttemptPrompt {
                attempt: 1,
                started_at: None,
                prompt: "p".into(),
            }],
        );
        // `?` opens the overlay and is consumed.
        assert_eq!(app.handle_key(key(KeyCode::Char('?'))), Outcome::Pending);
        assert!(app.help.is_visible());
        // While visible, a normal pop key is swallowed (not a Pop).
        assert_eq!(app.handle_key(key(KeyCode::Char('h'))), Outcome::Pending);
        assert!(app.help.is_visible());
        // `?` again closes it.
        assert_eq!(app.handle_key(key(KeyCode::Char('?'))), Outcome::Pending);
        assert!(!app.help.is_visible());
    }

    // -- relative time ---------------------------------------------------

    #[test]
    fn relative_time_buckets() {
        let now = Utc::now();
        assert_eq!(relative_time(now, now), "just now");
        assert_eq!(relative_time(now - Duration::seconds(5), now), "5s ago");
        assert_eq!(relative_time(now - Duration::seconds(125), now), "2m ago");
        assert_eq!(relative_time(now - Duration::seconds(7200), now), "2h ago");
        assert_eq!(
            relative_time(now - Duration::seconds(180_000), now),
            "2d ago"
        );
        // Future timestamp clamps to "just now".
        assert_eq!(relative_time(now + Duration::seconds(30), now), "just now");
    }
}
