// Prompt generation

use crate::plan::{Plan, Step, StepStatus};

/// Context from a previous failed attempt, used when retrying a step.
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// Which attempt number this is (1-indexed, so attempt 2 means first retry).
    pub attempt: i32,
    /// Maximum number of attempts allowed.
    pub max_attempts: i32,
    /// The diff produced by the previous attempt (if any).
    pub previous_diff: Option<String>,
    /// Test output from the previous attempt (if tests were run).
    pub previous_test_output: Option<String>,
    /// Files that were modified in the previous attempt.
    pub files_modified: Vec<String>,
}

/// Summary of a completed prior step for context injection.
#[derive(Debug, Clone)]
pub struct PriorStepSummary {
    /// 1-based step number within the plan's full step list. Preserved from
    /// the plan (not the filtered prior-step slice) so skipped/aborted steps
    /// do not shift the numbering visible to the agent.
    pub number: usize,
    /// Step title.
    pub title: String,
    /// Step status (should be Complete or Skipped).
    pub status: StepStatus,
    /// Files changed in this step (if available).
    pub files_changed: Vec<String>,
    /// Brief description of what was done.
    pub description: String,
}

/// A single prefix/suffix pair contributed by one scope (global, project, or
/// plan). Fields are borrowed from their source of truth — config, DB row, or
/// plan column — so building a [`PromptWraps`] is allocation-free.
#[derive(Debug, Clone, Copy, Default)]
pub struct PromptWrap<'a> {
    pub prefix: Option<&'a str>,
    pub suffix: Option<&'a str>,
}

impl<'a> PromptWrap<'a> {
    /// Convenience constructor taking `Option<&String>` views, which is how
    /// `Plan` / `Config` / `ProjectSettings` expose their owned strings.
    pub fn from_opts(prefix: Option<&'a String>, suffix: Option<&'a String>) -> Self {
        Self {
            prefix: prefix.map(String::as_str),
            suffix: suffix.map(String::as_str),
        }
    }
}

/// All three wrap layers, outermost to innermost. Prefixes stack global →
/// project → plan at the top of the prompt; suffixes stack plan → project →
/// global at the bottom. Empty strings are treated as `None` so a scope can
/// be "set but blank" without contaminating the prompt.
#[derive(Debug, Clone, Copy, Default)]
pub struct PromptWraps<'a> {
    pub global: PromptWrap<'a>,
    pub project: PromptWrap<'a>,
    pub plan: PromptWrap<'a>,
}

impl<'a> PromptWraps<'a> {
    /// Iterator over prefix strings in the order they should appear at the
    /// top of the assembled prompt (outermost first).
    fn prefix_sections(&self) -> impl Iterator<Item = &'a str> {
        [self.global.prefix, self.project.prefix, self.plan.prefix]
            .into_iter()
            .filter_map(non_empty)
    }

    /// Iterator over suffix strings in the order they should appear at the
    /// bottom of the assembled prompt (innermost first, so global ends last).
    fn suffix_sections(&self) -> impl Iterator<Item = &'a str> {
        [self.plan.suffix, self.project.suffix, self.global.suffix]
            .into_iter()
            .filter_map(non_empty)
    }
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    s.filter(|v| !v.is_empty())
}

/// Build the full prompt for a step execution.
///
/// The prompt is assembled from 8 parts:
/// 1. Agent pointer (instructs the harness to fetch the agent profile itself)
/// 2. Retry context (if this is a retry attempt)
/// 3. Plan context (plan description and overall goal)
/// 4. Prior steps summary (what has been done so far)
/// 5. Step details (title and description of current step)
/// 6. Acceptance criteria (specific criteria the step must meet)
/// 7. Deterministic tests (test commands that must pass)
/// 8. Focus instruction (reminder to stay focused on just this step)
///
/// Then the global/project/plan prompt prefix/suffix layers are wrapped
/// around the joined sections: prefixes stack outermost→innermost at the
/// top, suffixes stack innermost→outermost at the bottom.
pub fn build_step_prompt(
    plan: &Plan,
    step: &Step,
    prior_steps: &[PriorStepSummary],
    agent_name: Option<&str>,
    retry_context: Option<&RetryContext>,
    harness_supports_agent_file: bool,
    wraps: &PromptWraps<'_>,
) -> String {
    let mut sections: Vec<String> = Vec::new();

    // 1. Agent pointer
    // For harnesses without native agent-file support, point the agent at
    // `ralph agents show <name>` rather than inlining the full file — the
    // agent can fetch it on demand and we save tokens in every prompt.
    // Native-support harnesses (e.g. claude --agent-file) already receive
    // the file by reference, so no pointer is needed.
    if !harness_supports_agent_file && let Some(name) = agent_name {
        sections.push(format_agent_pointer(name));
    }

    // 2. Retry context
    if let Some(retry) = retry_context {
        sections.push(format_retry_context(retry));
    }

    // 3. Plan context
    sections.push(format_plan_context(plan));

    // 4. Prior steps summary
    if !prior_steps.is_empty() {
        sections.push(format_prior_steps(prior_steps));
    }

    // 5. Step details
    sections.push(format_step_details(step));

    // 6. Acceptance criteria
    if !step.acceptance_criteria.is_empty() {
        sections.push(format_acceptance_criteria(&step.acceptance_criteria));
    }

    // 7. Deterministic tests
    if !plan.deterministic_tests.is_empty() {
        sections.push(format_deterministic_tests(&plan.deterministic_tests));
    }

    // 8. Focus instruction
    sections.push(format_focus_instruction(step));

    // Layer prefix/suffix wraps around the joined sections. Each wrap layer
    // is inserted as its own `\n\n`-separated section, matching the rest of
    // the prompt's delimiter so nothing looks glued on.
    let mut all = Vec::with_capacity(sections.len() + 6);
    all.extend(wraps.prefix_sections().map(str::to_string));
    all.extend(sections);
    all.extend(wraps.suffix_sections().map(str::to_string));

    all.join("\n\n")
}

// ---------------------------------------------------------------------------
// Section formatters
// ---------------------------------------------------------------------------

fn format_agent_pointer(name: &str) -> String {
    format!(
        "# Agent Profile\n\n\
         You are executing a ralph step. Before starting, run \
         `ralph agents show {name}` to read your assigned agent guidance."
    )
}

fn format_retry_context(ctx: &RetryContext) -> String {
    let mut parts = vec![format!(
        "# Retry Context\n\n\
         This is attempt {attempt} of {max} for this step. The previous attempt failed.",
        attempt = ctx.attempt,
        max = ctx.max_attempts,
    )];

    if !ctx.files_modified.is_empty() {
        parts.push(format!(
            "## Files Modified in Previous Attempt\n\n{}",
            ctx.files_modified
                .iter()
                .map(|f| format!("- {f}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }

    if let Some(diff) = &ctx.previous_diff {
        let truncated = truncate_text(diff, 200);
        parts.push(format!("## Previous Diff\n\n```diff\n{truncated}\n```"));
    }

    if let Some(test_output) = &ctx.previous_test_output {
        let truncated = truncate_text(test_output, 100);
        parts.push(format!("## Previous Test Output\n\n```\n{truncated}\n```"));
    }

    parts.join("\n\n")
}

fn format_plan_context(plan: &Plan) -> String {
    format!(
        "# Plan: {slug}\n\n\
         {description}\n\n\
         **Branch:** `{branch}`\n\
         **Project:** `{project}`",
        slug = plan.slug,
        description = plan.description,
        branch = plan.branch_name,
        project = plan.project,
    )
}

fn format_prior_steps(prior_steps: &[PriorStepSummary]) -> String {
    let mut lines = vec!["## Context from Prior Steps".to_string()];

    for step in prior_steps {
        let status_marker = match step.status {
            StepStatus::Complete => "completed",
            StepStatus::Skipped => "skipped",
            StepStatus::Failed => "failed",
            StepStatus::Aborted => "aborted",
            StepStatus::Pending => "pending",
            StepStatus::InProgress => "in-progress",
        };

        let mut step_line = format!(
            "\n**Step {} ({status_marker}): {title}**",
            step.number,
            title = step.title,
        );

        if !step.description.is_empty() {
            step_line.push_str(&format!("\n{}", step.description));
        }

        if !step.files_changed.is_empty() {
            step_line.push_str(&format!(
                "\n- Files changed: {}",
                step.files_changed.join(", ")
            ));
        }

        lines.push(step_line);
    }

    lines.join("\n")
}

fn format_step_details(step: &Step) -> String {
    format!(
        "# Step: {title}\n\n\
         {description}",
        title = step.title,
        description = step.description,
    )
}

fn format_acceptance_criteria(criteria: &[String]) -> String {
    let mut lines = vec!["## Acceptance Criteria".to_string()];
    for criterion in criteria {
        lines.push(format!("- {criterion}"));
    }
    lines.join("\n")
}

fn format_deterministic_tests(tests: &[String]) -> String {
    // Framing matters here: the harness should treat these as ralph-owned
    // post-harness validation, not as an imperative checklist to run eagerly
    // inside the session. Older wording ("All must pass") pushed agents into
    // test-loops that burned context before finishing the work — ralph
    // already re-runs the tests after the harness returns, so an in-session
    // pass doesn't substitute for ralph's check.
    let mut lines = vec![
        "## Post-harness validation".to_string(),
        String::new(),
        "After you return, ralph will run these commands as validation — you don't".to_string(),
        "need to run them yourself:".to_string(),
    ];
    for test in tests {
        lines.push(format!("\n```\n{test}\n```"));
    }
    lines.push(String::new());
    lines.push(
        "If you want to sanity-check your changes before returning, feel free — but".to_string(),
    );
    lines.push(
        "ralph will re-run them regardless, so a passing run inside your session".to_string(),
    );
    lines.push(
        "doesn't skip ralph's check. Prefer using the time to complete the work.".to_string(),
    );
    lines.join("\n")
}

fn format_focus_instruction(step: &Step) -> String {
    format!(
        "**Important:** Only modify files relevant to this step. Do not make unrelated changes.\n\
         Focus on: {title}",
        title = step.title,
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate text to a maximum number of lines, appending an elision marker
/// when truncated. Keeps the first `max_lines` because the top of a diff or
/// test output usually carries the most context — file headers, the first
/// failing assertion — and losing the tail is the cheaper choice.
fn truncate_text(text: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= max_lines {
        text.to_string()
    } else {
        let omitted = lines.len() - max_lines;
        let head = &lines[..max_lines];
        format!("{}\n... ({omitted} lines omitted) ...", head.join("\n"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Plan, PlanStatus};
    use chrono::Utc;

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
            deterministic_tests: vec![
                "cargo build".to_string(),
                "cargo test".to_string(),
                "cargo clippy -- -D warnings".to_string(),
            ],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
        }
    }

    fn make_step() -> Step {
        Step {
            id: "s1".to_string(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Implement harness spawning".to_string(),
            description: "Add harness.rs with spawn_harness() function".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![
                "spawn_harness() works correctly".to_string(),
                "Tests pass".to_string(),
            ],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
        }
    }

    #[test]
    fn test_build_step_prompt_all_sections() {
        let plan = make_plan();
        let step = make_step();
        let prior_steps = vec![PriorStepSummary {
            number: 1,
            title: "Set up project".to_string(),
            status: StepStatus::Complete,
            files_changed: vec!["Cargo.toml".to_string(), "src/main.rs".to_string()],
            description: "Initial project scaffolding".to_string(),
        }];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &prior_steps,
            None,
            None,
            true, // harness supports agent file natively
            &PromptWraps::default(),
        );

        // Should contain plan context
        assert!(prompt.contains("# Plan: test-plan"));
        assert!(prompt.contains("Build a new feature"));
        assert!(prompt.contains("feat/test"));

        // Should contain prior steps
        assert!(prompt.contains("Context from Prior Steps"));
        assert!(prompt.contains("Set up project"));
        assert!(prompt.contains("Cargo.toml"));

        // Should contain step details
        assert!(prompt.contains("# Step: Implement harness spawning"));
        assert!(prompt.contains("harness.rs"));

        // Should contain acceptance criteria
        assert!(prompt.contains("Acceptance Criteria"));
        assert!(prompt.contains("spawn_harness()"));

        // Should contain deterministic tests (framed as ralph-owned
        // post-harness validation, NOT as an imperative checklist).
        assert!(prompt.contains("Post-harness validation"));
        assert!(prompt.contains("ralph will run these"));
        assert!(prompt.contains("cargo build"));
        assert!(prompt.contains("cargo test"));
        assert!(prompt.contains("cargo clippy"));

        // Should contain focus instruction
        assert!(prompt.contains("Only modify files relevant"));
    }

    #[test]
    fn test_build_step_prompt_emits_pointer_for_non_native_harness() {
        let plan = make_plan();
        let step = make_step();

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            Some("senior-engineer"),
            None,
            false, // harness does NOT support agent file natively
            &PromptWraps::default(),
        );

        // A short pointer should be prepended telling the agent to run
        // `ralph agents show <name>` rather than inlining the full file.
        assert!(prompt.starts_with("# Agent Profile"));
        assert!(prompt.contains("ralph agents show senior-engineer"));
    }

    #[test]
    fn test_build_step_prompt_no_agent_pointer_when_native() {
        let plan = make_plan();
        let step = make_step();

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            Some("senior-engineer"),
            None,
            true, // harness supports agent file natively
            &PromptWraps::default(),
        );

        // Pointer section should NOT be in the prompt — the harness gets
        // the agent file by reference via its native flag/env var.
        assert!(!prompt.contains("# Agent Profile"));
        assert!(!prompt.contains("ralph agents show"));
    }

    #[test]
    fn test_build_step_prompt_no_agent_pointer_when_no_agent() {
        let plan = make_plan();
        let step = make_step();

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            None,
            None,
            false, // non-native, but no agent assigned
            &PromptWraps::default(),
        );

        assert!(!prompt.contains("# Agent Profile"));
    }

    #[test]
    fn test_build_step_prompt_with_retry_context() {
        let plan = make_plan();
        let step = make_step();
        let retry = RetryContext {
            attempt: 2,
            max_attempts: 3,
            previous_diff: Some("+added a line\n-removed a line".to_string()),
            previous_test_output: Some("error: test failed\nassert_eq failed".to_string()),
            files_modified: vec!["src/harness.rs".to_string(), "src/main.rs".to_string()],
        };

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            None,
            Some(&retry),
            true,
            &PromptWraps::default(),
        );

        assert!(prompt.contains("# Retry Context"));
        assert!(prompt.contains("attempt 2 of 3"));
        assert!(prompt.contains("src/harness.rs"));
        assert!(prompt.contains("src/main.rs"));
        assert!(prompt.contains("Previous Diff"));
        assert!(prompt.contains("+added a line"));
        assert!(prompt.contains("Previous Test Output"));
        assert!(prompt.contains("test failed"));
    }

    #[test]
    fn test_build_step_prompt_no_prior_steps() {
        let plan = make_plan();
        let step = make_step();

        let prompt =
            build_step_prompt(&plan, &step, &[], None, None, true, &PromptWraps::default());

        // Should not contain prior steps section
        assert!(!prompt.contains("Context from Prior Steps"));
    }

    #[test]
    fn test_build_step_prompt_no_acceptance_criteria() {
        let plan = make_plan();
        let mut step = make_step();
        step.acceptance_criteria = vec![];

        let prompt =
            build_step_prompt(&plan, &step, &[], None, None, true, &PromptWraps::default());

        assert!(!prompt.contains("Acceptance Criteria"));
    }

    #[test]
    fn test_build_step_prompt_no_tests() {
        let mut plan = make_plan();
        plan.deterministic_tests = vec![];
        let step = make_step();

        let prompt =
            build_step_prompt(&plan, &step, &[], None, None, true, &PromptWraps::default());

        assert!(!prompt.contains("Post-harness validation"));
    }

    #[test]
    fn test_deterministic_tests_framing_no_imperative() {
        // Belt-and-braces regression: the section must not revert to the
        // old imperative phrasing ("All must pass") which pushed harnesses
        // into test-loops inside the session. If a future edit drifts back
        // toward imperative language, this catches it.
        let plan = make_plan();
        let step = make_step();
        let prompt =
            build_step_prompt(&plan, &step, &[], None, None, true, &PromptWraps::default());
        assert!(
            !prompt.contains("All must pass"),
            "imperative wording re-introduced: prompt should frame tests as ralph-owned \
             post-harness validation, not a checklist the agent must run"
        );
        assert!(
            !prompt.contains("Deterministic Tests"),
            "old section heading re-introduced; expected `Post-harness validation`"
        );
    }

    #[test]
    fn test_truncate_text_short() {
        let text = "line 1\nline 2\nline 3";
        let result = truncate_text(text, 10);
        assert_eq!(result, text);
    }

    #[test]
    fn test_truncate_text_long_keeps_head() {
        let lines: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        let text = lines.join("\n");
        let result = truncate_text(&text, 5);

        assert!(result.contains("(15 lines omitted)"));
        // First five lines preserved in order.
        for i in 0..5 {
            assert!(
                result.contains(&format!("line {i}")),
                "head line {i} missing from {result}"
            );
        }
        // Lines beyond the head are elided.
        for i in 5..20 {
            assert!(
                !result.contains(&format!("line {i}")),
                "tail line {i} unexpectedly present in {result}"
            );
        }
        // Elision marker follows the retained head.
        let head_end = result.find("line 4").unwrap();
        let marker = result.find("lines omitted").unwrap();
        assert!(head_end < marker);
    }

    #[test]
    fn test_format_retry_context_minimal() {
        let ctx = RetryContext {
            attempt: 2,
            max_attempts: 3,
            previous_diff: None,
            previous_test_output: None,
            files_modified: vec![],
        };
        let result = format_retry_context(&ctx);
        assert!(result.contains("attempt 2 of 3"));
        assert!(!result.contains("Previous Diff"));
        assert!(!result.contains("Previous Test Output"));
        assert!(!result.contains("Files Modified"));
    }

    #[test]
    fn test_format_retry_context_full() {
        let ctx = RetryContext {
            attempt: 3,
            max_attempts: 5,
            previous_diff: Some("diff content".to_string()),
            previous_test_output: Some("test output".to_string()),
            files_modified: vec!["a.rs".to_string(), "b.rs".to_string()],
        };
        let result = format_retry_context(&ctx);
        assert!(result.contains("attempt 3 of 5"));
        assert!(result.contains("diff content"));
        assert!(result.contains("test output"));
        assert!(result.contains("a.rs"));
        assert!(result.contains("b.rs"));
    }

    #[test]
    fn test_format_prior_steps_multiple() {
        let steps = vec![
            PriorStepSummary {
                number: 3,
                title: "Step A".to_string(),
                status: StepStatus::Complete,
                files_changed: vec!["a.rs".to_string()],
                description: "Did A".to_string(),
            },
            PriorStepSummary {
                number: 7,
                title: "Step B".to_string(),
                status: StepStatus::Skipped,
                files_changed: vec![],
                description: "Skipped B".to_string(),
            },
        ];

        let result = format_prior_steps(&steps);
        // Numbers come from the plan, not the filtered slice.
        assert!(result.contains("Step 3 (completed): Step A"));
        assert!(result.contains("Step 7 (skipped): Step B"));
        assert!(!result.contains("Step 1 (completed)"));
        assert!(!result.contains("Step 2 (skipped)"));
        assert!(result.contains("a.rs"));
    }

    #[test]
    fn test_format_prior_steps_includes_failed_and_aborted() {
        let steps = vec![
            PriorStepSummary {
                number: 1,
                title: "Worked".to_string(),
                status: StepStatus::Complete,
                files_changed: vec!["ok.rs".to_string()],
                description: "Fine".to_string(),
            },
            PriorStepSummary {
                number: 2,
                title: "Broke things".to_string(),
                status: StepStatus::Failed,
                files_changed: vec!["bad.rs".to_string()],
                description: "Tests failed after edit".to_string(),
            },
            PriorStepSummary {
                number: 3,
                title: "User bailed".to_string(),
                status: StepStatus::Aborted,
                files_changed: vec![],
                description: "Ctrl+C mid-run".to_string(),
            },
        ];

        let result = format_prior_steps(&steps);
        // Completed step still labeled
        assert!(result.contains("Step 1 (completed): Worked"));
        // Failed step is present and tagged as failed
        assert!(result.contains("Step 2 (failed): Broke things"));
        assert!(result.contains("Tests failed after edit"));
        assert!(result.contains("bad.rs"));
        // Aborted step is present and tagged as aborted
        assert!(result.contains("Step 3 (aborted): User bailed"));
        assert!(result.contains("Ctrl+C mid-run"));
    }

    #[test]
    fn test_prompt_section_order() {
        let plan = make_plan();
        let step = make_step();
        let prior = vec![PriorStepSummary {
            number: 1,
            title: "Prior".to_string(),
            status: StepStatus::Complete,
            files_changed: vec![],
            description: "Done".to_string(),
        }];
        let retry = RetryContext {
            attempt: 2,
            max_attempts: 3,
            previous_diff: Some("diff".to_string()),
            previous_test_output: None,
            files_modified: vec![],
        };

        let prompt = build_step_prompt(
            &plan,
            &step,
            &prior,
            Some("senior-engineer"),
            Some(&retry),
            false,
            &PromptWraps::default(),
        );

        // Verify ordering: agent -> retry -> plan -> prior -> step -> criteria -> tests -> focus
        let agent_pos = prompt.find("# Agent Profile").unwrap();
        let retry_pos = prompt.find("# Retry Context").unwrap();
        let plan_pos = prompt.find("# Plan:").unwrap();
        let prior_pos = prompt.find("Context from Prior Steps").unwrap();
        let step_pos = prompt.find("# Step:").unwrap();
        let criteria_pos = prompt.find("Acceptance Criteria").unwrap();
        let tests_pos = prompt.find("Post-harness validation").unwrap();
        let focus_pos = prompt.find("Only modify files").unwrap();

        assert!(agent_pos < retry_pos);
        assert!(retry_pos < plan_pos);
        assert!(plan_pos < prior_pos);
        assert!(prior_pos < step_pos);
        assert!(step_pos < criteria_pos);
        assert!(criteria_pos < tests_pos);
        assert!(tests_pos < focus_pos);
    }

    #[test]
    fn test_wraps_layer_global_project_plan_order() {
        let plan = make_plan();
        let step = make_step();
        let global_pre = "GLOBAL-PRE".to_string();
        let global_suf = "GLOBAL-SUF".to_string();
        let project_pre = "PROJECT-PRE".to_string();
        let project_suf = "PROJECT-SUF".to_string();
        let plan_pre = "PLAN-PRE".to_string();
        let plan_suf = "PLAN-SUF".to_string();
        let wraps = PromptWraps {
            global: PromptWrap::from_opts(Some(&global_pre), Some(&global_suf)),
            project: PromptWrap::from_opts(Some(&project_pre), Some(&project_suf)),
            plan: PromptWrap::from_opts(Some(&plan_pre), Some(&plan_suf)),
        };

        let prompt = build_step_prompt(&plan, &step, &[], None, None, true, &wraps);

        // Prefixes stack outermost → innermost at the top.
        let g_pre = prompt.find("GLOBAL-PRE").unwrap();
        let p_pre = prompt.find("PROJECT-PRE").unwrap();
        let pl_pre = prompt.find("PLAN-PRE").unwrap();
        let plan_section = prompt.find("# Plan: test-plan").unwrap();
        assert!(g_pre < p_pre);
        assert!(p_pre < pl_pre);
        assert!(pl_pre < plan_section);

        // Suffixes stack innermost → outermost at the bottom.
        let focus = prompt.find("Only modify files").unwrap();
        let pl_suf = prompt.find("PLAN-SUF").unwrap();
        let p_suf = prompt.find("PROJECT-SUF").unwrap();
        let g_suf = prompt.find("GLOBAL-SUF").unwrap();
        assert!(focus < pl_suf);
        assert!(pl_suf < p_suf);
        assert!(p_suf < g_suf);

        // Global prefix is the very start; global suffix is the very end.
        assert!(prompt.starts_with("GLOBAL-PRE"));
        assert!(prompt.trim_end().ends_with("GLOBAL-SUF"));
    }

    #[test]
    fn test_wraps_skip_empty_and_none() {
        let plan = make_plan();
        let step = make_step();
        let blank = String::new();
        let plan_pre = "PLAN-PRE".to_string();
        let wraps = PromptWraps {
            // Empty strings are treated identically to None — they do not
            // contribute a section (no stray double-newline gap).
            global: PromptWrap::from_opts(Some(&blank), Some(&blank)),
            project: PromptWrap::default(),
            plan: PromptWrap::from_opts(Some(&plan_pre), None),
        };

        let prompt = build_step_prompt(&plan, &step, &[], None, None, true, &wraps);

        assert!(prompt.starts_with("PLAN-PRE"));
        assert!(
            !prompt.contains("\n\n\n"),
            "should not produce blank sections"
        );
        // No suffix contribution at all — focus instruction is the tail.
        assert!(
            prompt
                .trim_end()
                .ends_with(&format!("Focus on: {}", step.title))
        );
    }
}
