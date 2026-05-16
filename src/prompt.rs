// Prompt generation

use crate::plan::{AnsweredQuestion, Plan, Step, StepStatus};

/// Default "how to introspect this plan" block prepended to every step's
/// prompt. Injected verbatim — there is no per-plan override.
///
/// Trailing instruction appended to every step prompt when the plan has
/// `questions_enabled = true` (TUI-plan.md §17). Verbatim from the spec —
/// case, punctuation, and line breaks are load-bearing.
pub const QUESTION_ASK_INSTRUCTION: &str = "\
## Asking the user a question

This plan has questions enabled, so you may pause and ask the user for
clarification when you're genuinely blocked on a decision they need to
make.

Before asking, seriously consider whether the answer is already
recoverable from:
  - The plan description and step acceptance criteria above.
  - The codebase itself (read the relevant files).
  - A reasonable, conservative default that you can flag in a comment.

Most decisions belong in the plan, not the implementation. Questions
cost the user attention and break their flow.

That said: when you genuinely cannot proceed without input, ask. A good
question with suggested answers is far better than a wrong guess.

To ask, run:

    ralph question ask \"What should I do about X?\" \\
      -s \"option A: ...\" \\
      -s \"option B: ...\"

Suggestions are optional but appreciated — the user can always type a
custom answer. You may call `ralph question ask` multiple times in one
attempt. After your last call, exit normally (zero status). The plan
will pause; the user will answer in the TUI; your next attempt will
receive every answered question in the appended retry context.";

/// Seed source for the global prompt (`config.prompt`) — **init only**.
/// `ralph init` is the sole place this block is written from (see
/// `commands::seed_global_prompt`): a fresh or blank `config.prompt` is
/// filled with it, and `ralph init --restore-prompts` re-seeds it
/// unconditionally. `build_step_prompt` no longer injects it — the global
/// prompt layer (sourced from `config.prompt`) carries it at runtime, so
/// editing the global prompt fully customizes this block.
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

/// The three configurable layers of the four-layer prompt model
/// (Global → Project → Plan → Step), outermost to innermost. Empty strings
/// are treated as `None` so a layer can be "set but blank" without
/// contaminating the prompt.
///
/// Global and Project stack as plain prefix sections at the top of the
/// assembled prompt in global → project order; there is no suffix concept.
/// The Plan layer is the plan's description (not a separate column) and is
/// rendered by [`format_plan_context`] into the `# Plan: {slug}` block —
/// it is NOT a bare prefix section, so the description is emitted exactly
/// once with consistent slug/branch/project framing.
///
/// The Step layer is the step body itself (built by [`build_step_prompt`])
/// and is not represented here.
#[derive(Debug, Clone, Default)]
pub struct Prompts {
    pub global: Option<String>,
    pub project: Option<String>,
    pub plan: Option<String>,
}

impl Prompts {
    /// Iterator over the plain prefix layer strings (global → project) in
    /// the order they should appear at the top of the assembled prompt,
    /// skipping any layer that is unset or blank. The Plan layer is NOT
    /// included — it is rendered through [`format_plan_context`] in the
    /// body so the plan description is emitted exactly once.
    fn prefix_sections(&self) -> impl Iterator<Item = &str> {
        [self.global.as_deref(), self.project.as_deref()]
            .into_iter()
            .filter_map(non_empty)
    }
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    s.filter(|v| !v.is_empty())
}

/// Build the full prompt for a step execution.
///
/// The prompt is assembled from these parts, in order:
/// 1. Global prompt layer (`prompts.global`; sourced from `config.prompt`,
///    which `ralph init` seeds with [`DEFAULT_CONTEXT_PREPEND`])
/// 2. Project prompt layer (`prompts.project`)
/// 3. Plan prompt layer — the plan-context block, rendered by
///    [`format_plan_context`] from `prompts.plan` (the plan's description)
///    plus the plan's slug/branch/project framing. This is the **single**
///    place the plan description is emitted.
/// 4. Agent pointer (instructs the harness to fetch the agent profile itself)
/// 5. Retry context (if this is a retry attempt)
/// 6. Previously answered questions (only if `answered_questions` is non-empty)
/// 7. Step details (title and description of current step) + acceptance
///    criteria
/// 8. Plan step map — a compact titles-only list of ALL steps in the plan
///    with their current status, so the agent can see where it is in the
///    sequence without us paying O(n²) bytes for full prior descriptions
/// 9. Deterministic tests (test commands that will be run after)
/// 10. Focus instruction (reminder to stay focused on just this step)
/// 11. Question-ask instruction (only if `plan.questions_enabled`)
///
/// Assembly is pure prefix-stacking — there is no suffix stage and no
/// auto-injected context prepend (the global layer carries that block).
///
/// `all_steps` is the full ordered list of steps in the plan (as returned by
/// `storage::list_steps`). `step` must be one of them — matched by `id`.
///
/// `answered_questions` is the chronological list of Q&A pairs for this step
/// (from [`crate::storage::list_answered_questions_for_step`]). When non-empty
/// the prompt injects a "Previously answered questions" section between Plan
/// context and Step details so the harness sees the user's clarifications
/// verbatim before re-attacking the step.
#[allow(clippy::too_many_arguments)]
pub fn build_step_prompt(
    plan: &Plan,
    step: &Step,
    all_steps: &[Step],
    agent_name: Option<&str>,
    retry_context: Option<&RetryContext>,
    harness_supports_agent_file: bool,
    prompts: &Prompts,
    answered_questions: &[AnsweredQuestion],
) -> String {
    let mut sections: Vec<String> = Vec::new();

    // Plan layer — the plan-context block. This is the SINGLE place the plan
    // description is emitted: `prompts.plan` carries the description (the
    // executor wires it from `plan.description`), and `format_plan_context`
    // wraps it in the `# Plan: {slug}` header with branch/project framing.
    // We deliberately do NOT also stack `prompts.plan` as a bare prefix
    // section (see `Prompts::prefix_sections`) — doing so previously emitted
    // the description twice.
    sections.push(format_plan_context(plan, prompts.plan.as_deref()));

    // Agent pointer.
    // For harnesses without native agent-file support, point the agent at
    // `ralph agents show <name>` rather than inlining the full file — the
    // agent can fetch it on demand and we save tokens in every prompt.
    // Native-support harnesses (e.g. claude --agent-file) already receive
    // the file by reference, so no pointer is needed.
    if !harness_supports_agent_file && let Some(name) = agent_name {
        sections.push(format_agent_pointer(name));
    }

    // Retry context.
    if let Some(retry) = retry_context {
        sections.push(format_retry_context(retry));
    }

    // Previously answered questions — injected between plan context and
    // step details so the harness sees the user's clarifications before
    // re-reading the step description (TUI-plan.md §17 "Retry context after
    // answering"). Empty slice contributes nothing.
    if !answered_questions.is_empty() {
        sections.push(format_answered_questions(answered_questions));
    }

    // Step details (with 1-based position in the plan)
    let step_num = all_steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|i| i + 1)
        .unwrap_or(0);
    sections.push(format_step_details(step, step_num, all_steps.len()));

    // Acceptance criteria.
    if !step.acceptance_criteria.is_empty() {
        sections.push(format_acceptance_criteria(&step.acceptance_criteria));
    }

    // Plan step map — titles-only listing of every step in the plan.
    // Strictly linear in plan size (~80 bytes/step) vs the old quadratic
    // prior-step descriptions dump.
    if !all_steps.is_empty() {
        sections.push(format_plan_step_map(all_steps, &step.id));
    }

    // Deterministic tests.
    if !plan.deterministic_tests.is_empty() {
        sections.push(format_deterministic_tests(&plan.deterministic_tests));
    }

    // Focus instruction.
    sections.push(format_focus_instruction(step));

    // Question-ask instruction — appended at the very end (after the
    // focus instruction) when the plan opted into questions (TUI-plan.md §17
    // "Prompt injection (when enabled)").
    if plan.questions_enabled {
        sections.push(QUESTION_ASK_INSTRUCTION.to_string());
    }

    // Stack the global/project layers as prefix sections ahead of the joined
    // body. Each layer is inserted as its own `\n\n`-separated section,
    // matching the rest of the prompt's delimiter so nothing looks glued on.
    // The Plan layer is already the first body section (rendered via
    // `format_plan_context`), so it is not stacked here. There is no suffix
    // stage — assembly is pure prefix-stacking.
    let mut all = Vec::with_capacity(sections.len() + 2);
    all.extend(prompts.prefix_sections().map(str::to_string));
    all.extend(sections);

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

/// Render the Plan-layer block: `# Plan: {slug}` header, the plan
/// description, and branch/project framing.
///
/// **Single-source decision (Step 9):** the description is taken from
/// `plan_layer` — the configurable Plan prompt layer (`prompts.plan`, wired
/// by the executor from `plan.description`) — NOT from `plan.description`
/// directly, and the Plan layer is *not* also stacked as a bare prefix
/// section by `build_step_prompt`. This is why the description appears
/// exactly once. Chosen over "keep a header on a separate prefix layer"
/// because routing the description through this one formatter keeps a single
/// emission site and consistent slug/branch/project framing. `plan_layer`
/// being `None`/blank yields a header-only block (no empty body line).
fn format_plan_context(plan: &Plan, plan_layer: Option<&str>) -> String {
    let body = match non_empty(plan_layer) {
        Some(desc) => format!("{desc}\n\n"),
        None => String::new(),
    };
    format!(
        "# Plan: {slug}\n\n\
         {body}\
         **Branch:** `{branch}`\n\
         **Project:** `{project}`",
        slug = plan.slug,
        branch = plan.branch_name,
        project = plan.project,
    )
}

/// Render the "Previously answered questions" section. Each Q&A pair becomes
/// two markdown blockquote lines (`> Q: ...` / `> A: ...`) separated by a
/// blank line, in chronological order. Verbatim shape from TUI-plan.md §17.
fn format_answered_questions(answered: &[AnsweredQuestion]) -> String {
    let mut lines = vec![
        "## Previously answered questions".to_string(),
        String::new(),
    ];
    let last = answered.len().saturating_sub(1);
    for (i, qa) in answered.iter().enumerate() {
        lines.push(format!("> Q: {}", qa.question));
        lines.push(format!("> A: {}", qa.answer));
        if i != last {
            // Blank line between pairs to keep each blockquote distinct in
            // markdown rendering. The trailing pair has no separator.
            lines.push(String::new());
        }
    }
    lines.join("\n")
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
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
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

        // Mirror the executor: the Plan layer is the plan's description.
        let prompts = Prompts {
            global: None,
            project: None,
            plan: Some(plan.description.clone()),
        };
        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true, // harness supports agent file natively
            &prompts,
            &[],
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
    fn test_plan_description_emitted_exactly_once_executor_realistic() {
        // Carried-forward regression (Step 9): the executor wires the Plan
        // layer to `plan.description` verbatim. Previously `format_plan_context`
        // ALSO re-emitted `plan.description` independently, so the description
        // appeared TWICE in every real run. This test drives the
        // executor-realistic case — the Plan layer string IS `plan.description`
        // (not a synthetic "PLAN-LAYER" placeholder, which is exactly what
        // masked the bug) — and asserts it appears exactly once.
        let mut plan = make_plan();
        // A distinctive, unlikely-to-collide marker so a substring count is a
        // faithful occurrence count (won't accidentally match boilerplate).
        plan.description =
            "ZZZ_UNIQUE_PLAN_DESCRIPTION_MARKER: build the widget pipeline".to_string();
        let step = make_step();
        let all_steps = vec![step.clone()];

        // Exactly how src/executor.rs (~line 741) builds `Prompts`: the Plan
        // layer is `Some(plan.description.clone())`.
        let prompts = Prompts {
            global: config_like_global(),
            project: None,
            plan: Some(plan.description.clone()),
        };

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &prompts,
            &[],
        );

        assert_eq!(
            prompt.matches("ZZZ_UNIQUE_PLAN_DESCRIPTION_MARKER").count(),
            1,
            "plan description must appear EXACTLY ONCE (no Plan-layer + \
             format_plan_context double-emission). Prompt:\n{prompt}"
        );
        // And it must be inside the Plan-context block at the Plan-layer
        // position (after any global/project prefix, before the step body).
        let plan_hdr = prompt.find("# Plan: test-plan").unwrap();
        let desc_pos = prompt.find("ZZZ_UNIQUE_PLAN_DESCRIPTION_MARKER").unwrap();
        let step_pos = prompt.find("## Your step").unwrap();
        assert!(plan_hdr < desc_pos);
        assert!(desc_pos < step_pos);
    }

    /// Stand-in for a seeded `config.prompt` global layer so the regression
    /// test exercises a realistic multi-layer prompt (not just the Plan
    /// layer in isolation).
    fn config_like_global() -> Option<String> {
        Some(DEFAULT_CONTEXT_PREPEND.to_string())
    }

    #[test]
    fn test_default_prepend_not_auto_injected() {
        // Step 9 removed the context-prepend auto-injection stage. With no
        // Global layer set, NONE of DEFAULT_CONTEXT_PREPEND's load-bearing
        // markers may appear — the block now travels via `config.prompt`
        // (the Global layer), not the prompt builder.
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
            &Prompts::default(),
            &[],
        );

        assert!(!prompt.contains("# Ralph context"));
        assert!(!prompt.contains("## Introspecting the plan"));
        assert!(!prompt.contains("Do NOT use `--after <N>` during a run"));
    }

    #[test]
    fn test_default_prepend_flows_through_global_layer() {
        // When the Global layer carries DEFAULT_CONTEXT_PREPEND (as `ralph
        // init` seeds it into `config.prompt`), its markers DO appear, at the
        // very top of the prompt.
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let prompts = Prompts {
            global: Some(DEFAULT_CONTEXT_PREPEND.to_string()),
            project: None,
            plan: None,
        };

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &prompts,
            &[],
        );

        assert!(prompt.contains("# Ralph context"));
        assert!(prompt.contains("## Introspecting the plan"));
        assert!(prompt.contains("`ralph status`"));
        assert!(prompt.contains("Do NOT use `--after <N>` during a run"));
        assert!(prompt.starts_with("# Ralph context"));
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
            &Prompts::default(),
            &[],
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
        let mut plan = make_plan();
        plan.questions_enabled = true;
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
        let answered = vec![AnsweredQuestion {
            question: "Which DB?".to_string(),
            answer: "SQLite".to_string(),
        }];

        let prompts = Prompts {
            global: Some(DEFAULT_CONTEXT_PREPEND.to_string()),
            project: Some("PROJECT-LAYER".to_string()),
            plan: Some(plan.description.clone()),
        };

        let prompt = build_step_prompt(
            &plan,
            &s2,
            &all_steps,
            Some("senior-engineer"),
            Some(&retry),
            false,
            &prompts,
            &answered,
        );

        // Verify ordering:
        // global -> project -> plan -> agent -> retry -> answered_questions
        // -> step -> criteria -> step map -> tests -> focus -> ask-instruction
        let global_pos = prompt.find("# Ralph context").unwrap();
        let project_pos = prompt.find("PROJECT-LAYER").unwrap();
        let plan_pos = prompt.find("# Plan:").unwrap();
        let agent_pos = prompt.find("# Agent Profile").unwrap();
        let retry_pos = prompt.find("# Retry Context").unwrap();
        let answered_pos = prompt.find("## Previously answered questions").unwrap();
        let step_pos = prompt.find("## Your step").unwrap();
        let criteria_pos = prompt.find("Acceptance criteria").unwrap();
        let map_pos = prompt.find("## Plan step map").unwrap();
        let tests_pos = prompt.find("Post-harness validation").unwrap();
        let focus_pos = prompt.find("Only modify files").unwrap();
        let ask_pos = prompt.find("## Asking the user a question").unwrap();

        assert!(global_pos < project_pos);
        assert!(project_pos < plan_pos);
        assert!(plan_pos < agent_pos);
        assert!(agent_pos < retry_pos);
        assert!(retry_pos < answered_pos);
        assert!(answered_pos < step_pos);
        assert!(step_pos < criteria_pos);
        assert!(criteria_pos < map_pos);
        assert!(map_pos < tests_pos);
        assert!(tests_pos < focus_pos);
        assert!(focus_pos < ask_pos);
    }

    #[test]
    fn test_layers_stack_global_project_plan_at_top() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let prompts = Prompts {
            global: Some("GLOBAL-LAYER".to_string()),
            project: Some("PROJECT-LAYER".to_string()),
            plan: Some("PLAN-LAYER".to_string()),
        };

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &prompts, &[]);

        // Global and Project stack as plain prefix sections in
        // global → project order. The Plan layer is rendered through
        // `format_plan_context`, so the configured plan-layer text appears
        // inside the `# Plan:` block (immediately after the project prefix),
        // not as a separate bare prefix section.
        let g = prompt.find("GLOBAL-LAYER").unwrap();
        let p = prompt.find("PROJECT-LAYER").unwrap();
        let plan_hdr = prompt.find("# Plan: test-plan").unwrap();
        let pl = prompt.find("PLAN-LAYER").unwrap();
        assert!(g < p);
        assert!(p < plan_hdr);
        assert!(plan_hdr < pl, "plan-layer text must sit inside the # Plan block");

        // Global layer is the very start; the body's focus instruction is
        // the tail — nothing is appended after it.
        assert!(prompt.starts_with("GLOBAL-LAYER"));
        assert!(
            prompt
                .trim_end()
                .ends_with(&format!("Focus on: {}", step.title))
        );
        // The configured plan-layer string appears exactly once (no bare
        // prefix + format_plan_context double-emission).
        assert_eq!(prompt.matches("PLAN-LAYER").count(), 1);
        // None of the old suffix markers leak in.
        assert!(!prompt.contains("-SUF"));
    }

    #[test]
    fn test_layers_skip_empty_and_none() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let prompts = Prompts {
            // Empty strings are treated identically to None — they do not
            // contribute a section (no stray double-newline gap).
            global: Some(String::new()),
            project: None,
            plan: Some("PLAN-LAYER".to_string()),
        };

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &prompts, &[]);

        // Global is blank and Project is None, so the first section is the
        // Plan-context block. The configured plan-layer text lives inside it.
        assert!(prompt.starts_with("# Plan: test-plan"));
        assert!(prompt.contains("PLAN-LAYER"));
        assert!(
            !prompt.contains("\n\n\n"),
            "should not produce blank sections"
        );
        // Pure prefix-stacking — focus instruction is still the tail.
        assert!(
            prompt
                .trim_end()
                .ends_with(&format!("Focus on: {}", step.title))
        );
    }

    // ---- Question injection (TUI-plan.md §17) ----

    #[test]
    fn test_question_ask_instruction_appended_when_questions_enabled() {
        let mut plan = make_plan();
        plan.questions_enabled = true;
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &[],
        );

        // Header + a few load-bearing markers from the §17 spec text.
        assert!(prompt.contains("## Asking the user a question"));
        assert!(prompt.contains("This plan has questions enabled"));
        assert!(prompt.contains("ralph question ask"));
        assert!(prompt.contains("Most decisions belong in the plan"));

        // The ask-instruction sits AFTER the focus instruction so it's the
        // last body section before any suffix wraps.
        let focus_pos = prompt.find("Only modify files").unwrap();
        let ask_pos = prompt.find("## Asking the user a question").unwrap();
        assert!(focus_pos < ask_pos);
    }

    #[test]
    fn test_question_ask_instruction_absent_when_questions_disabled() {
        let plan = make_plan(); // questions_enabled defaults to false
        assert!(!plan.questions_enabled);
        let step = make_step();
        let all_steps = vec![step.clone()];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &[],
        );

        // No header, and no body text from the §17 ask block.
        assert!(!prompt.contains("## Asking the user a question"));
        assert!(!prompt.contains("This plan has questions enabled"));
        // The body text uses unique phrasing — make sure it's gone too.
        assert!(!prompt.contains("Most decisions belong in the plan"));
    }

    #[test]
    fn test_previously_answered_questions_section_renders_qa_pairs() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let answered = vec![
            AnsweredQuestion {
                question: "Should this use Postgres or SQLite?".to_string(),
                answer: "SQLite (already a dep)".to_string(),
            },
            AnsweredQuestion {
                question: "Pick a logging crate.".to_string(),
                answer: "tracing".to_string(),
            },
        ];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &answered,
        );

        // Section heading is present.
        assert!(prompt.contains("## Previously answered questions"));
        // Each Q&A pair renders as a `> Q:` / `> A:` blockquote pair.
        assert!(prompt.contains("> Q: Should this use Postgres or SQLite?"));
        assert!(prompt.contains("> A: SQLite (already a dep)"));
        assert!(prompt.contains("> Q: Pick a logging crate."));
        assert!(prompt.contains("> A: tracing"));

        // The section sits between Plan context and Step details.
        let plan_pos = prompt.find("# Plan:").unwrap();
        let answered_pos = prompt.find("## Previously answered questions").unwrap();
        let step_pos = prompt.find("## Your step").unwrap();
        assert!(plan_pos < answered_pos);
        assert!(answered_pos < step_pos);

        // Pairs render in the order supplied (chronological).
        let q1_pos = prompt.find("Postgres or SQLite").unwrap();
        let q2_pos = prompt.find("Pick a logging crate").unwrap();
        assert!(q1_pos < q2_pos);
    }

    #[test]
    fn test_previously_answered_questions_absent_when_empty() {
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
            &Prompts::default(),
            &[],
        );

        assert!(!prompt.contains("## Previously answered questions"));
        // No stray blockquote markers from this section either.
        assert!(!prompt.contains("> Q:"));
        assert!(!prompt.contains("> A:"));
    }

    #[test]
    fn test_question_features_independent() {
        // The "Previously answered questions" section is gated on the
        // `answered_questions` slice, NOT on `questions_enabled`. If a plan
        // had questions enabled, got answers, and then the user toggled the
        // flag off, we still want the harness to see the answers it received
        // on the prior attempt — otherwise the user's input is silently
        // dropped from the next retry. Conversely, enabling questions on a
        // fresh plan must not synthesize an empty section.
        let mut plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let answered = vec![AnsweredQuestion {
            question: "Q?".to_string(),
            answer: "A.".to_string(),
        }];

        // Case 1: questions disabled, but answers exist (toggled-off-after-
        // answering). Section IS rendered; ask-instruction is NOT.
        plan.questions_enabled = false;
        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &answered,
        );
        assert!(prompt.contains("## Previously answered questions"));
        assert!(!prompt.contains("## Asking the user a question"));

        // Case 2: questions enabled, but no answers yet. Ask-instruction IS
        // rendered; previously-answered section is NOT.
        plan.questions_enabled = true;
        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &[],
        );
        assert!(!prompt.contains("## Previously answered questions"));
        assert!(prompt.contains("## Asking the user a question"));
    }
}
