// Prompt generation

use crate::plan::{Plan, Step, StepStatus};

/// Default "how to introspect this plan" block prepended to every step's
/// prompt. Plans can override it via [`Plan::context_prepend`]; a `None`
/// override means "use this default verbatim", `Some(s)` means "use `s`
/// verbatim (no concatenation with this default)", and `Some("")` is an
/// explicit escape hatch meaning "no prepend at all".
///
/// This string is a user-facing contract — case, punctuation, and line
/// breaks are load-bearing and should not drift without a conscious bump.
pub const DEFAULT_CONTEXT_PREPEND: &str = "\
# Ralph context

You are executing one step of a multi-step plan managed by `ralph`, a
deterministic execution planner. Your step's title, description, and
acceptance criteria are below.

## Introspecting the plan

- `ralph status` — current plan state and progress
- `ralph step list` — all steps with status
- `ralph step show <num>` — full description of a specific step
- `ralph log --step <num>` — execution history (prompts sent, outputs)

## Adding follow-up steps

- `ralph step add --next \"title\" -d \"...\"` — insert immediately after current
- `ralph step add \"title\"` — append at end of plan

Do NOT use `--after <N>` during a run — positions shift as steps are added,
and inserting before the current step is a no-op for this execution.

---

";

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

/// Resolve the effective context-prepend text for a plan.
///
/// `None` -> [`DEFAULT_CONTEXT_PREPEND`]. `Some("")` -> `""` (power-user
/// escape hatch). `Some(s)` -> `s` verbatim, not concatenated with the
/// default. Callers that want to print the effective prepend (for example
/// `ralph plan prepend show`) should route through this helper so the
/// precedence stays in one place.
pub fn effective_context_prepend(plan: &Plan) -> &str {
    match plan.context_prepend.as_deref() {
        Some(s) => s,
        None => DEFAULT_CONTEXT_PREPEND,
    }
}

/// Build the full prompt for a step execution.
///
/// The prompt is assembled from these parts, in order:
/// 1. Context prepend — per-plan override or [`DEFAULT_CONTEXT_PREPEND`]
/// 2. Agent pointer (instructs the harness to fetch the agent profile itself)
/// 3. Retry context (if this is a retry attempt)
/// 4. Plan context (plan description and overall goal)
/// 5. Step details (title and description of current step)
/// 6. Acceptance criteria (specific criteria the step must meet)
/// 7. Plan step map — a compact titles-only list of ALL steps in the plan
///    with their current status, so the agent can see where it is in the
///    sequence without us paying O(n²) bytes for full prior descriptions
/// 8. Deterministic tests (test commands that will be run after)
/// 9. Focus instruction (reminder to stay focused on just this step)
///
/// Then the global/project/plan prompt prefix/suffix layers are wrapped
/// around the joined sections: prefixes stack outermost→innermost at the
/// top, suffixes stack innermost→outermost at the bottom.
///
/// `all_steps` is the full ordered list of steps in the plan (as returned by
/// `storage::list_steps`). `step` must be one of them — matched by `id`.
pub fn build_step_prompt(
    plan: &Plan,
    step: &Step,
    all_steps: &[Step],
    agent_name: Option<&str>,
    retry_context: Option<&RetryContext>,
    harness_supports_agent_file: bool,
    wraps: &PromptWraps<'_>,
) -> String {
    let mut sections: Vec<String> = Vec::new();

    // 1. Context prepend — plan override or system default. An empty override
    // is the explicit "no prepend" signal and contributes nothing.
    let prepend = effective_context_prepend(plan);
    if !prepend.is_empty() {
        // The constant includes a trailing `\n\n---\n\n` separator, but the
        // outer `sections.join("\n\n")` will also add one between sections.
        // Push the prepend with its trailing whitespace trimmed so we don't
        // end up with three blank lines between it and the next section.
        sections.push(prepend.trim_end().to_string());
    }

    // 2. Agent pointer
    // For harnesses without native agent-file support, point the agent at
    // `ralph agents show <name>` rather than inlining the full file — the
    // agent can fetch it on demand and we save tokens in every prompt.
    // Native-support harnesses (e.g. claude --agent-file) already receive
    // the file by reference, so no pointer is needed.
    if !harness_supports_agent_file && let Some(name) = agent_name {
        sections.push(format_agent_pointer(name));
    }

    // 3. Retry context
    if let Some(retry) = retry_context {
        sections.push(format_retry_context(retry));
    }

    // 4. Plan context
    sections.push(format_plan_context(plan));

    // 5. Step details (with 1-based position in the plan)
    let step_num = all_steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|i| i + 1)
        .unwrap_or(0);
    sections.push(format_step_details(step, step_num, all_steps.len()));

    // 6. Acceptance criteria
    if !step.acceptance_criteria.is_empty() {
        sections.push(format_acceptance_criteria(&step.acceptance_criteria));
    }

    // 7. Plan step map — titles-only listing of every step in the plan.
    // Strictly linear in plan size (~80 bytes/step) vs the old quadratic
    // prior-step descriptions dump.
    if !all_steps.is_empty() {
        sections.push(format_plan_step_map(all_steps, &step.id));
    }

    // 8. Deterministic tests
    if !plan.deterministic_tests.is_empty() {
        sections.push(format_deterministic_tests(&plan.deterministic_tests));
    }

    // 9. Focus instruction
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

fn format_step_details(step: &Step, step_num: usize, total: usize) -> String {
    format!(
        "## Your step (#{step_num} of {total}): {title}\n\n\
         {description}",
        title = step.title,
        description = step.description,
    )
}

fn format_acceptance_criteria(criteria: &[String]) -> String {
    let mut lines = vec!["### Acceptance criteria".to_string()];
    for criterion in criteria {
        lines.push(format!("- {criterion}"));
    }
    lines.join("\n")
}

/// Render the compact plan step map: every step as `#N. [STATUS] title`,
/// with the current step prefixed by `→` so the agent can locate itself.
/// Status labels are uppercase (COMPLETE, SKIPPED, PENDING, IN_PROGRESS,
/// FAILED, ABORTED) to stay visually consistent regardless of theme.
fn format_plan_step_map(all_steps: &[Step], current_step_id: &str) -> String {
    let mut lines = vec!["## Plan step map".to_string(), String::new()];
    for (idx, s) in all_steps.iter().enumerate() {
        let num = idx + 1;
        let status = status_label(s.status);
        let line = if s.id == current_step_id {
            format!("→ #{num}. [{status}] {title}", title = s.title)
        } else {
            format!("#{num}. [{status}] {title}", title = s.title)
        };
        lines.push(line);
    }
    lines.join("\n")
}

fn status_label(status: StepStatus) -> &'static str {
    match status {
        StepStatus::Complete => "COMPLETE",
        StepStatus::Skipped => "SKIPPED",
        StepStatus::Pending => "PENDING",
        StepStatus::InProgress => "IN_PROGRESS",
        StepStatus::Failed => "FAILED",
        StepStatus::Aborted => "ABORTED",
    }
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
    use crate::plan::{ChangePolicy, Plan, PlanStatus};
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
            context_prepend: None,
        }
    }

    fn make_step_with(id: &str, title: &str, status: StepStatus) -> Step {
        Step {
            id: id.to_string(),
            plan_id: "p1".to_string(),
            sort_key: id.to_string(),
            title: title.to_string(),
            description: format!("description for {title}"),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
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
            status: StepStatus::InProgress,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: ChangePolicy::Required,
            tags: vec![],
        }
    }

    #[test]
    fn test_build_step_prompt_all_sections() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true, // harness supports agent file natively
            &PromptWraps::default(),
        );

        // Should contain plan context
        assert!(prompt.contains("# Plan: test-plan"));
        assert!(prompt.contains("Build a new feature"));
        assert!(prompt.contains("feat/test"));

        // Should contain step details with numbered heading
        assert!(prompt.contains("## Your step (#1 of 1): Implement harness spawning"));
        assert!(prompt.contains("harness.rs"));

        // Should contain acceptance criteria
        assert!(prompt.contains("Acceptance criteria"));
        assert!(prompt.contains("spawn_harness()"));

        // Should contain plan step map, NOT the old "Context from Prior Steps"
        assert!(prompt.contains("## Plan step map"));
        assert!(!prompt.contains("Context from Prior Steps"));

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
    fn test_default_prepend_is_used_when_plan_override_is_none() {
        let plan = make_plan(); // context_prepend: None
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        // The DEFAULT_CONTEXT_PREPEND starts with "# Ralph context" and lists
        // introspection commands. Spot-check a few load-bearing markers.
        assert!(prompt.contains("# Ralph context"));
        assert!(prompt.contains("## Introspecting the plan"));
        assert!(prompt.contains("`ralph status`"));
        assert!(prompt.contains("Do NOT use `--after <N>` during a run"));
    }

    #[test]
    fn test_plan_override_replaces_default() {
        let mut plan = make_plan();
        plan.context_prepend = Some("# Custom prepend\n\nBe concise.".to_string());
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        // Custom text IS present …
        assert!(prompt.contains("# Custom prepend"));
        assert!(prompt.contains("Be concise."));
        // … and the default is NOT concatenated with it.
        assert!(
            !prompt.contains("# Ralph context"),
            "plan override must REPLACE the default, not append to it"
        );
        assert!(!prompt.contains("## Introspecting the plan"));
    }

    #[test]
    fn test_empty_string_override_yields_no_prepend() {
        let mut plan = make_plan();
        plan.context_prepend = Some(String::new());
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        // Neither the default nor any custom prepend is present.
        assert!(!prompt.contains("# Ralph context"));
        assert!(!prompt.contains("## Introspecting the plan"));
        // The prompt should start with the plan context, not a blank line.
        assert!(
            prompt.starts_with("# Plan:"),
            "empty override should leave plan context as the first section, got start: {:?}",
            &prompt[..prompt.len().min(80)]
        );
    }

    #[test]
    fn test_prompt_includes_step_titles_list_not_descriptions() {
        let plan = make_plan();
        // Three steps, all with non-empty descriptions; the second is the
        // current step.
        let s1 = make_step_with("s1", "Done thing", StepStatus::Complete);
        let s2 = make_step_with("s2", "Current thing", StepStatus::InProgress);
        let s3 = make_step_with("s3", "Future thing", StepStatus::Pending);
        let all_steps = vec![s1.clone(), s2.clone(), s3.clone()];

        let prompt = build_step_prompt(
            &plan,
            &s2,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        // Titles ARE present in the step map.
        assert!(prompt.contains("Done thing"));
        assert!(prompt.contains("Current thing"));
        assert!(prompt.contains("Future thing"));

        // Descriptions of OTHER steps are NOT present — only the current
        // step's description is allowed (via format_step_details). The step
        // description for s2 IS "description for Current thing" and should
        // appear, but s1's and s3's descriptions must not leak.
        assert!(
            !prompt.contains("description for Done thing"),
            "prior step description leaked into the prompt"
        );
        assert!(
            !prompt.contains("description for Future thing"),
            "future step description leaked into the prompt"
        );
        // Current step's own description is expected.
        assert!(prompt.contains("description for Current thing"));

        // Explicitly assert the removed section heading does not come back.
        assert!(!prompt.contains("Context from Prior Steps"));
    }

    #[test]
    fn test_current_step_marked_with_arrow() {
        let plan = make_plan();
        let s1 = make_step_with("s1", "Alpha", StepStatus::Complete);
        let s2 = make_step_with("s2", "Beta", StepStatus::InProgress);
        let s3 = make_step_with("s3", "Gamma", StepStatus::Pending);
        let all_steps = vec![s1.clone(), s2.clone(), s3.clone()];

        let prompt = build_step_prompt(
            &plan,
            &s2,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        // Only the current step line has the arrow prefix.
        assert!(prompt.contains("→ #2. [IN_PROGRESS] Beta"));
        // Other lines do NOT have the arrow.
        assert!(prompt.contains("#1. [COMPLETE] Alpha"));
        assert!(prompt.contains("#3. [PENDING] Gamma"));
        assert!(!prompt.contains("→ #1."));
        assert!(!prompt.contains("→ #3."));
    }

    #[test]
    fn test_build_step_prompt_emits_pointer_for_non_native_harness() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            Some("senior-engineer"),
            None,
            false, // harness does NOT support agent file natively
            &PromptWraps::default(),
        );

        // Pointer section should be present telling the agent to run
        // `ralph agents show <name>` rather than inlining the full file.
        assert!(prompt.contains("# Agent Profile"));
        assert!(prompt.contains("ralph agents show senior-engineer"));
    }

    #[test]
    fn test_build_step_prompt_no_agent_pointer_when_native() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
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
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
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
        let all_steps = vec![step.clone()];
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
            &all_steps,
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
    fn test_build_step_prompt_no_acceptance_criteria() {
        let plan = make_plan();
        let mut step = make_step();
        step.acceptance_criteria = vec![];
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

        assert!(!prompt.contains("Acceptance criteria"));
    }

    #[test]
    fn test_build_step_prompt_no_tests() {
        let mut plan = make_plan();
        plan.deterministic_tests = vec![];
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );

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
        let all_steps = vec![step.clone()];
        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &PromptWraps::default(),
        );
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
    fn test_prompt_section_order() {
        let plan = make_plan();
        let s1 = make_step_with("s1", "Prior", StepStatus::Complete);
        let s2 = make_step();
        let all_steps = vec![s1, s2.clone()];
        let retry = RetryContext {
            attempt: 2,
            max_attempts: 3,
            previous_diff: Some("diff".to_string()),
            previous_test_output: None,
            files_modified: vec![],
        };

        let prompt = build_step_prompt(
            &plan,
            &s2,
            &all_steps,
            Some("senior-engineer"),
            Some(&retry),
            false,
            &PromptWraps::default(),
        );

        // Verify ordering:
        // prepend -> agent -> retry -> plan -> step -> criteria -> step map -> tests -> focus
        let prepend_pos = prompt.find("# Ralph context").unwrap();
        let agent_pos = prompt.find("# Agent Profile").unwrap();
        let retry_pos = prompt.find("# Retry Context").unwrap();
        let plan_pos = prompt.find("# Plan:").unwrap();
        let step_pos = prompt.find("## Your step").unwrap();
        let criteria_pos = prompt.find("Acceptance criteria").unwrap();
        let map_pos = prompt.find("## Plan step map").unwrap();
        let tests_pos = prompt.find("Post-harness validation").unwrap();
        let focus_pos = prompt.find("Only modify files").unwrap();

        assert!(prepend_pos < agent_pos);
        assert!(agent_pos < retry_pos);
        assert!(retry_pos < plan_pos);
        assert!(plan_pos < step_pos);
        assert!(step_pos < criteria_pos);
        assert!(criteria_pos < map_pos);
        assert!(map_pos < tests_pos);
        assert!(tests_pos < focus_pos);
    }

    #[test]
    fn test_wraps_layer_global_project_plan_order() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
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

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &wraps);

        // Prefixes stack outermost → innermost at the top, ahead of the
        // prepend section.
        let g_pre = prompt.find("GLOBAL-PRE").unwrap();
        let p_pre = prompt.find("PROJECT-PRE").unwrap();
        let pl_pre = prompt.find("PLAN-PRE").unwrap();
        let prepend_pos = prompt.find("# Ralph context").unwrap();
        assert!(g_pre < p_pre);
        assert!(p_pre < pl_pre);
        assert!(pl_pre < prepend_pos);

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
        let all_steps = vec![step.clone()];
        let blank = String::new();
        let plan_pre = "PLAN-PRE".to_string();
        let wraps = PromptWraps {
            // Empty strings are treated identically to None — they do not
            // contribute a section (no stray double-newline gap).
            global: PromptWrap::from_opts(Some(&blank), Some(&blank)),
            project: PromptWrap::default(),
            plan: PromptWrap::from_opts(Some(&plan_pre), None),
        };

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &wraps);

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

    #[test]
    fn test_effective_context_prepend_returns_default_when_none() {
        let plan = make_plan();
        assert_eq!(effective_context_prepend(&plan), DEFAULT_CONTEXT_PREPEND);
    }

    #[test]
    fn test_effective_context_prepend_returns_override() {
        let mut plan = make_plan();
        plan.context_prepend = Some("custom".to_string());
        assert_eq!(effective_context_prepend(&plan), "custom");
    }

    #[test]
    fn test_effective_context_prepend_returns_empty_for_empty_override() {
        let mut plan = make_plan();
        plan.context_prepend = Some(String::new());
        assert_eq!(effective_context_prepend(&plan), "");
    }
}
