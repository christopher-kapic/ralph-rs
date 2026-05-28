// Prompt generation

use crate::plan::{Interruption, Plan, Step, StepStatus};

/// Trailing instruction appended to every step prompt (questions are always
/// enabled — there is no per-plan opt-out). Verbatim from the spec — case,
/// punctuation, and line breaks are load-bearing.
pub const QUESTION_ASK_INSTRUCTION: &str = "\
## Asking the user a question

You may pause and ask the user for clarification when you're genuinely
blocked on a decision they need to make.

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
///
/// Failed attempts leave the dirty tree on disk for the next attempt; this
/// context carries only the test output (and commit-hook output, if
/// applicable) plus the failure reason from the prior attempt.
///
/// Post test-then-commit (Phase A): the per-iteration commit happens only
/// after tests pass, so a failed attempt has no committed diff to feed back.
/// The `previous_diff` and `files_modified` fields are retained for
/// `RetryStrategy::Rollback`-style audit and external callers, but the
/// executor currently populates them lazily/never — the dirty tree is always
/// on disk for the agent to inspect via `git diff`. The retry-context render
/// is therefore typically just the failure reason + previous test output.
///
/// When a pre-commit hook rejects the commit (tests passed, commit refused),
/// the captured hook stderr is concatenated into `previous_test_output`
/// under a `[Commit hook output]` header so the next attempt's prompt
/// surfaces both the test output and the hook output in a single section.
///
/// `previous_failure_reason` is a short human-readable note (derived from
/// the prior attempt's
/// [`TerminationReason`](crate::plan::TerminationReason)) so the prompt
/// states *why* the last attempt failed even without a diff section.
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// Which attempt number this is (1-indexed, so attempt 2 means first retry).
    pub attempt: i32,
    /// Maximum number of attempts allowed.
    pub max_attempts: i32,
    /// The diff produced by the previous attempt. Typically `None` post
    /// test-then-commit: the dirty tree is on disk for the agent to inspect
    /// via `git diff`, so re-sending the same diff in the prompt would be
    /// redundant and confusing.
    pub previous_diff: Option<String>,
    /// Test output from the previous attempt (if tests were run). On
    /// commit-hook rejection, the hook stderr is concatenated here under a
    /// `[Commit hook output]` header so the prompt surfaces both signals.
    pub previous_test_output: Option<String>,
    /// Files that were modified in the previous attempt. Typically empty
    /// post test-then-commit (the changes are already on disk).
    pub files_modified: Vec<String>,
    /// Short human-readable reason the previous attempt failed (e.g. "tests
    /// failed", "harness exited non-zero", "no changes produced"). Always
    /// set on a real retry so the prompt — which omits the diff — still
    /// states what went wrong. `None` only when no reason was available.
    pub previous_failure_reason: Option<String>,
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
/// 6. Resolved interruptions — bounded (only if `resolved_interruptions` is
///    non-empty; the slice is the last *N*, newest-first, from the bounded
///    storage query)
/// 7. Step details (title and description of current step) + acceptance
///    criteria
/// 8. Plan step map — a compact titles-only list of ALL steps in the plan
///    with their current status, so the agent can see where it is in the
///    sequence without us paying O(n²) bytes for full prior descriptions
/// 9. Deterministic tests (test commands that will be run after)
/// 10. Focus instruction (reminder to stay focused on just this step)
/// 11. Question-ask instruction (always appended — questions are always on)
///
/// Assembly is pure prefix-stacking — there is no suffix stage and no
/// auto-injected context prepend (the global layer carries that block).
///
/// `all_steps` is the full ordered list of steps in the plan (as returned by
/// `storage::list_steps`). `step` must be one of them — matched by `id`.
///
/// `resolved_interruptions` is the **bounded** (last *N*, newest-first) list
/// of resolved interruptions for this step, from
/// [`crate::storage::list_resolved_interruptions_for_step`]. When non-empty
/// the prompt injects a "Resolved interruptions" section between Plan context
/// and Step details so the harness sees the human's clarifications/unblocks
/// verbatim before re-attacking the step. This section is the §8/§4 cutover:
/// it replaces the old unbounded "Previously answered questions" section and
/// is bounded in **both** entry count (the caller's `LIMIT`) **and**
/// per-field length (every body/resolution/comment is `truncate_text`'d),
/// closing the one pre-existing unbounded-context leak (docs/dag-redesign.md
/// §4). Callers must pass the result of the bounded query — there is no
/// unbounded slice anywhere in prompt assembly.
#[allow(clippy::too_many_arguments)]
pub fn build_step_prompt(
    plan: &Plan,
    step: &Step,
    all_steps: &[Step],
    agent_name: Option<&str>,
    retry_context: Option<&RetryContext>,
    harness_supports_agent_file: bool,
    prompts: &Prompts,
    resolved_interruptions: &[Interruption],
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

    // Resolved interruptions — injected between plan context and step
    // details so the harness sees the human's clarifications/unblocks before
    // re-reading the step description (docs/dag-redesign.md §8 item 1). The
    // slice is already bounded by the caller's `LIMIT`; each field is
    // additionally `truncate_text`'d inside the formatter, so this section is
    // doubly bounded (count + per-field). Empty slice contributes nothing.
    if !resolved_interruptions.is_empty() {
        sections.push(format_resolved_interruptions(resolved_interruptions));
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

    // Question-ask instruction — appended at the very end (after the focus
    // instruction). Questions are always enabled, so this always renders.
    sections.push(QUESTION_ASK_INSTRUCTION.to_string());

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
// Reviewer prompt (docs/dag-redesign.md §8, §9-inv-2, Decision 5)
// ---------------------------------------------------------------------------

/// The structured-verdict contract the reviewer harness MUST emit.
///
/// The reviewer is a *separate* nondeterministic harness invocation
/// (docs/dag-redesign.md §3.2/§9). Its only machine-consumed output is a
/// single verdict line, which `crate::review::parse_review_verdict` parses:
///
/// - `REVIEW PASS` — no real defect found in *this step's* implementation.
///   The step is `Complete` with `review_status = Passed`.
/// - `REVIEW FAIL — N issue(s)` — a real defect was found; the orchestrator
///   (sole DAG writer — §9-inv-3) inserts a corrective step and re-parents
///   dependents (§10). The em-dash and `N` are advisory; the parser keys off
///   the leading `REVIEW FAIL` token so a hyphen/spacing wobble can't flip a
///   FAIL into a silently-ignored line.
///
/// This text is embedded verbatim in the reviewer prompt so the contract is
/// stated *to the harness* exactly as the parser enforces it — the two must
/// never drift. Case, the two literal tokens, and the read-only mandate are
/// load-bearing.
pub const REVIEW_VERDICT_CONTRACT: &str = "\
## Your verdict (REQUIRED — exact format)

End your reply with EXACTLY ONE of these two lines, on its own line, as the
final line of your output:

    REVIEW PASS
    REVIEW FAIL — N issue(s)

(`N` is the count of real defects you found; the wording after `REVIEW FAIL`
is free text.) Emit nothing after the verdict line.";

/// Build the **read-only reviewer prompt** for one committed iteration.
///
/// This is deliberately a SEPARATE builder from [`build_step_prompt`] with
/// **no shared assembly** (docs/dag-redesign.md §8): the four-layer
/// Global/Project/Plan/Step stack, retry context, resolved-interruptions,
/// step map, deterministic-tests, and question-ask sections are all
/// irrelevant to a reviewer and would dilute the verdict. The reviewer sees
/// only:
///
/// 1. Plan + step context — **titles and acceptance criteria ONLY** (no plan
///    description body, no other steps' descriptions). The acceptance
///    criteria ARE the review rubric.
/// 2. The **single** commit diff for the reviewed iteration, supplied by the
///    caller as the verbatim output of `git show <sha>` (one commit, never a
///    cumulative `a..b` range and never a dependency's diff — Decision 5 /
///    §4: the reviewer prompt is O(1) in plan size).
/// 3. The §8 instruction (read-only; request a corrective step only on a
///    real defect in *this step's* implementation) + the verdict contract
///    ([`REVIEW_VERDICT_CONTRACT`]).
///
/// `commit_diff` MUST be exactly one commit's `git show` patch. The caller
/// (`crate::review`) is the single place that produces it via
/// `git::show_commit_diff`; passing a range diff here would violate the §9
/// hard invariant — the dedicated unit test asserts the assembled prompt
/// contains exactly one diff and zero dependency/cumulative diffs.
pub fn build_review_prompt(
    plan: &Plan,
    step: &Step,
    short_commit: &str,
    iteration: i32,
    commit_diff: &str,
) -> String {
    let mut sections: Vec<String> = Vec::new();

    // (1) Plan + step context — titles + acceptance criteria ONLY. We
    // intentionally do NOT emit `plan.description` (the Plan prompt layer)
    // or any other step's body: the review rubric is this step's acceptance
    // criteria, and pulling in surrounding context would both dilute the
    // verdict and risk re-introducing cross-step accumulation (§4).
    sections.push(format!(
        "# Review task\n\n\
         You are reviewing one committed iteration of a single step in the \
         `{slug}` plan. This is a **read-only code review** — see the rules \
         below.\n\n\
         **Plan:** {slug}\n\
         **Step:** {title}",
        slug = plan.slug,
        title = step.title,
    ));

    if step.acceptance_criteria.is_empty() {
        sections.push(
            "## Acceptance criteria\n\n\
             _This step declared no explicit acceptance criteria. Judge the \
             commit against the step title and whether the change is a \
             coherent, defect-free implementation of it._"
                .to_string(),
        );
    } else {
        let mut lines = vec![
            "## Acceptance criteria".to_string(),
            String::new(),
            "Review the commit against THESE criteria and nothing else:".to_string(),
            String::new(),
        ];
        for c in &step.acceptance_criteria {
            lines.push(format!("- {c}"));
        }
        sections.push(lines.join("\n"));
    }

    // (2) The SINGLE commit diff (O(1) — Decision 5). Fenced as ```diff so
    // the harness reads it as one patch; this is the verbatim
    // `git show <sha>` output for exactly the reviewed iteration's commit.
    sections.push(format!(
        "## Commit under review: `{short_commit}` ({sid}.{n})\n\n\
         This is the **entire** change introduced by this one commit — the \
         only thing you are reviewing. Do not request, fetch, or reason about \
         any other commit's diff.\n\n\
         ```diff\n{diff}\n```",
        short_commit = short_commit,
        sid = step.short_id,
        n = iteration,
        diff = commit_diff,
    ));

    // (3) The §8 read-only instruction + verdict contract.
    sections.push(format!(
        "## Review rules (READ-ONLY)\n\n\
         Review commit `{sid}.{n}` against this step's acceptance criteria.\n\n\
         - You are **read-only**. Do NOT modify, create, or delete any files. \
           Do NOT run commands that change the working tree or git state. The \
           commit is fixed history; your job is solely to judge it.\n\
         - Only if you find a **real defect in THIS step's implementation** \
           (a criterion genuinely unmet, a bug introduced by this commit, a \
           regression) should you fail the review. Style nits, hypothetical \
           future concerns, or pre-existing issues NOT introduced by this \
           commit are NOT grounds to fail.\n\
         - If you fail it, a **corrective step** will be inserted immediately \
           after this step, and everything that depended on this step will be \
           re-pointed at the correction. You do not create that step — you \
           only deliver the verdict; the orchestrator performs the change.\n\n\
         {contract}",
        sid = step.short_id,
        n = iteration,
        contract = REVIEW_VERDICT_CONTRACT,
    ));

    sections.join("\n\n")
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
    let mut header = format!(
        "# Retry Context\n\n\
         This is attempt {attempt} of {max} for this step. The previous attempt failed.",
        attempt = ctx.attempt,
        max = ctx.max_attempts,
    );
    if let Some(reason) = &ctx.previous_failure_reason {
        // Under `Keep` the diff section is omitted (the work is on disk), so
        // this line is the only thing telling the agent *what* failed —
        // keep it on the header so it's the first thing read.
        header.push_str(&format!("\n\nPrevious failure: {reason}."));
    }
    let mut parts = vec![header];

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
        // 200 lines × ~256 bytes-per-line headroom ≈ 50 KiB cap on the diff
        // pane — large but bounded so a single attempt's previous-diff
        // section can't blow the prompt up.
        let truncated = truncate_text(diff, 200, 50 * 1024);
        parts.push(format!("## Previous Diff\n\n```diff\n{truncated}\n```"));
    }

    if let Some(test_output) = &ctx.previous_test_output {
        // 100 lines × ~256 bytes headroom ≈ 25 KiB cap.
        let truncated = truncate_text(test_output, 100, 25 * 1024);
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

/// Max lines kept per interruption field (body / resolution / comment) when
/// rendering the "Resolved interruptions" section. The *count* of entries is
/// bounded upstream by [`crate::storage::list_resolved_interruptions_for_step`]
/// (its `LIMIT`); this is the **per-field** half of the §4 fix — the same
/// `truncate_text` helper used for the 200-line diff truncation, applied here
/// so a single pathologically long answer/blocker explanation cannot blow the
/// prompt up.
const RESOLVED_INTERRUPTION_FIELD_MAX_LINES: usize = 20;

/// Render the bounded "Resolved interruptions" section (docs/dag-redesign.md
/// §8 item 1). Each resolved interruption becomes a markdown blockquote
/// carrying its **kind**, **body**, the chosen **resolution**, and any human
/// **comment** — every free-text field run through [`truncate_text`] so the
/// section is bounded in per-field length (the entry *count* is already
/// bounded by the caller's `LIMIT`). The input is newest-first (as the
/// bounded query returns it); we render in that order so the freshest
/// clarification leads.
///
/// This replaces the pre-Phase-2 unbounded `format_answered_questions` —
/// there is no longer any unbounded vector anywhere in prompt assembly.
fn format_resolved_interruptions(resolved: &[Interruption]) -> String {
    let mut lines = vec!["## Resolved interruptions".to_string(), String::new()];
    let last = resolved.len().saturating_sub(1);
    for (i, intr) in resolved.iter().enumerate() {
        let body = truncate_text(
            &intr.body,
            RESOLVED_INTERRUPTION_FIELD_MAX_LINES,
            RESOLVED_INTERRUPTION_FIELD_MAX_BYTES,
        );
        lines.push(format!("> **{kind}**", kind = intr.kind.as_str()));
        // Each multi-line field is re-quoted line-by-line so the blockquote
        // stays well-formed even after truncation inserts its elision line.
        for bl in body.lines() {
            lines.push(format!("> {bl}"));
        }
        if let Some(resolution) = &intr.resolution {
            let r = truncate_text(
                resolution,
                RESOLVED_INTERRUPTION_FIELD_MAX_LINES,
                RESOLVED_INTERRUPTION_FIELD_MAX_BYTES,
            );
            lines.push(">".to_string());
            for (j, rl) in r.lines().enumerate() {
                lines.push(if j == 0 {
                    format!("> Resolution: {rl}")
                } else {
                    format!("> {rl}")
                });
            }
        }
        if let Some(comment) = &intr.comment {
            let c = truncate_text(
                comment,
                RESOLVED_INTERRUPTION_FIELD_MAX_LINES,
                RESOLVED_INTERRUPTION_FIELD_MAX_BYTES,
            );
            lines.push(">".to_string());
            for (j, cl) in c.lines().enumerate() {
                lines.push(if j == 0 {
                    format!("> Comment: {cl}")
                } else {
                    format!("> {cl}")
                });
            }
        }
        if i != last {
            // Blank line between entries to keep each blockquote distinct in
            // markdown rendering. The trailing entry has no separator.
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
        // `Blocked` is a derived overlay never stored on `steps.status`, so
        // the plan-step-map (built from stored statuses) won't normally see
        // it; label it explicitly for exhaustiveness if a derived value is
        // ever passed in.
        StepStatus::Blocked => "BLOCKED",
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

/// Per-field byte cap for the bounded "Resolved interruptions" section.
/// 8 KiB ≈ ~2k tokens — generous for a single body/resolution/comment while
/// keeping the total per-step prompt growth bounded even when a single
/// pathological line slips past the line cap (e.g. a multi-MB base64 blob
/// pasted into a resolution comment). Three fields × `MAX_RESOLVED_INTERRUPTIONS`
/// (capped upstream by the storage query) × this cap is the real upper bound
/// on the section's contribution to the prompt.
pub(crate) const RESOLVED_INTERRUPTION_FIELD_MAX_BYTES: usize = 8 * 1024;

/// Truncate text to both a maximum number of lines AND a maximum byte count,
/// appending an elision marker when truncated. Keeps the **head** (first
/// `max_lines` / first `max_bytes`) because the top of a diff or test output
/// usually carries the most context — file headers, the first failing
/// assertion — and losing the tail is the cheaper choice.
///
/// The byte cap closes the §4 prompt-growth hole that the pre-fix
/// line-only cap left open: a single line of arbitrarily large size
/// (multi-MB JSON dump, base64 blob, output-without-newlines) used to slip
/// straight through unmodified. The byte cap is enforced on the head slice
/// (or on the whole text on the no-line-cap-hit path) at a UTF-8 char
/// boundary so we never slice mid-codepoint.
fn truncate_text(text: &str, max_lines: usize, max_bytes: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let line_overflow = lines.len() > max_lines;
    let byte_overflow = text.len() > max_bytes;

    // Fast path — neither bound exceeded.
    if !line_overflow && !byte_overflow {
        return text.to_string();
    }

    // Decide the body to truncate by line bound first. If lines overflow,
    // take the first `max_lines`; otherwise keep the whole text. Then apply
    // the byte cap on whatever's left.
    let (line_body, omitted_lines) = if line_overflow {
        let head = &lines[..max_lines];
        (head.join("\n"), lines.len() - max_lines)
    } else {
        (text.to_string(), 0)
    };

    let original_bytes = text.len();
    let original_lines = lines.len();
    let body_bytes_truncated = line_body.len() > max_bytes;
    let body = if body_bytes_truncated {
        // Walk char boundaries to a cut <= max_bytes (UTF-8-safe).
        let mut cut = 0;
        for (i, _) in line_body.char_indices() {
            if i > max_bytes {
                break;
            }
            cut = i;
        }
        line_body[..cut].to_string()
    } else {
        line_body
    };

    // Build the elision marker. Mention whichever cap(s) tripped so a human
    // reading the prompt can tell why context was lost.
    let marker = if line_overflow && body_bytes_truncated {
        format!(
            "\n... [truncated; original was {original_lines} lines, {original_bytes} bytes; \
             {omitted_lines} lines omitted then byte-capped at {max_bytes}] ..."
        )
    } else if line_overflow {
        format!("\n... ({omitted_lines} lines omitted) ...")
    } else {
        format!(
            "\n... [truncated; original was {original_lines} lines, {original_bytes} bytes; \
             byte-capped at {max_bytes}] ..."
        )
    };

    format!("{body}{marker}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{ChangePolicy, InterruptionKind, InterruptionState, Plan, PlanStatus};
    use chrono::Utc;

    /// Build a resolved [`Interruption`] for prompt-rendering tests.
    fn resolved_intr(
        kind: InterruptionKind,
        body: &str,
        resolution: Option<&str>,
        comment: Option<&str>,
    ) -> Interruption {
        Interruption {
            id: "i-test".to_string(),
            step_id: "s1".to_string(),
            attempt: 1,
            kind,
            body: body.to_string(),
            options: vec![],
            resolution: resolution.map(str::to_string),
            comment: comment.map(str::to_string),
            state: InterruptionState::Resolved,
            asked_at: Utc::now(),
            resolved_at: Some(Utc::now()),
        }
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
            deterministic_tests: vec![
                "cargo build".to_string(),
                "cargo test".to_string(),
                "cargo clippy -- -D warnings".to_string(),
            ],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        }
    }

    fn make_step_with(id: &str, title: &str, status: StepStatus) -> Step {
        Step {
            id: id.to_string(),
            short_id: String::new(),
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
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
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
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
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

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &prompts, &[]);

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

        let prompt = build_step_prompt(&plan, &step, &all_steps, None, None, true, &prompts, &[]);

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
            previous_failure_reason: Some("tests failed".to_string()),
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
        let result = truncate_text(text, 10, 8 * 1024);
        assert_eq!(result, text);
    }

    #[test]
    fn test_truncate_text_long_keeps_head() {
        let lines: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        let text = lines.join("\n");
        let result = truncate_text(&text, 5, 8 * 1024);

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

    /// Fix #2: a single line of arbitrarily large size used to slip through
    /// the line-only cap unmodified — `lines().count() == 1` short-circuits
    /// the head-truncation path. The byte cap closes that hole. 100 KiB of
    /// one-line text must come back capped to roughly `max_bytes` (plus the
    /// elision marker).
    #[test]
    fn test_truncate_text_single_huge_line_byte_capped() {
        let max_bytes = 8 * 1024;
        let text: String = "x".repeat(100 * 1024);
        let result = truncate_text(&text, 100, max_bytes);

        // The text payload must NOT exceed max_bytes; the elision marker
        // adds a bounded suffix on top.
        assert!(
            result.starts_with(&"x".repeat(max_bytes)),
            "head must be the byte-capped prefix",
        );
        assert!(
            result.contains("truncated"),
            "elision marker must mark the truncation",
        );
        // The overall length is the cap plus the elision marker — call it
        // `cap + 512` to be safe.
        assert!(
            result.len() <= max_bytes + 512,
            "total length must be within cap + marker; got {} (cap {})",
            result.len(),
            max_bytes,
        );
    }

    /// Multi-byte UTF-8 inputs must never be sliced mid-codepoint. Feed a
    /// long stream of 4-byte chars and confirm the cap lands on a char
    /// boundary (the cut bytes are a valid UTF-8 string in the output).
    #[test]
    fn test_truncate_text_utf8_safe() {
        let max_bytes = 1000;
        let text: String = "\u{1F600}".repeat(2000); // grinning-face = 4 bytes each
        let result = truncate_text(&text, 100, max_bytes);

        // Output is still valid UTF-8 (we'd panic on a malformed slice in
        // the `format!` above; this assertion documents the invariant).
        assert!(!result.is_empty());
        // The leading run of chars is whole grinning-faces, not partial.
        let leading: String = result.chars().take_while(|c| *c == '\u{1F600}').collect();
        assert!(!leading.is_empty(), "the head must contain whole chars");
        assert_eq!(
            leading.len() % 4,
            0,
            "leading byte count must be a multiple of the 4-byte char width",
        );
    }

    /// Both bounds tripped → the elision marker calls both out.
    #[test]
    fn test_truncate_text_both_bounds_tripped() {
        let max_lines = 3;
        let max_bytes = 50;
        // 10 lines of 100 bytes each — exceeds both caps.
        let text: String = (0..10)
            .map(|i| format!("{:0>100}", i))
            .collect::<Vec<_>>()
            .join("\n");
        let result = truncate_text(&text, max_lines, max_bytes);
        assert!(result.contains("truncated"));
        assert!(result.contains("lines omitted") || result.contains("byte-capped"));
    }

    #[test]
    fn test_format_retry_context_minimal() {
        let ctx = RetryContext {
            attempt: 2,
            max_attempts: 3,
            previous_diff: None,
            previous_test_output: None,
            files_modified: vec![],
            previous_failure_reason: None,
        };
        let result = format_retry_context(&ctx);
        assert!(result.contains("attempt 2 of 3"));
        assert!(!result.contains("Previous Diff"));
        assert!(!result.contains("Previous Test Output"));
        assert!(!result.contains("Files Modified"));
        assert!(!result.contains("Previous failure:"));
    }

    #[test]
    fn test_format_retry_context_keep_scoped_no_diff_but_reason_and_output() {
        // Step 22: under `Keep` the executor passes diff=None / files=[] but
        // still sets previous_test_output + previous_failure_reason. The
        // formatter must convey attempt N/M, the failure reason, and the
        // test output, while OMITTING the diff/files sections entirely.
        let ctx = RetryContext {
            attempt: 2,
            max_attempts: 4,
            previous_diff: None,
            previous_test_output: Some("assertion failed: foo == bar".to_string()),
            files_modified: vec![],
            previous_failure_reason: Some("tests failed".to_string()),
        };
        let result = format_retry_context(&ctx);
        assert!(result.contains("attempt 2 of 4"));
        assert!(result.contains("Previous failure: tests failed."));
        assert!(result.contains("Previous Test Output"));
        assert!(result.contains("assertion failed: foo == bar"));
        // Diff/files sections are absent under Keep.
        assert!(!result.contains("Previous Diff"));
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
            previous_failure_reason: Some("tests failed".to_string()),
        };
        let result = format_retry_context(&ctx);
        assert!(result.contains("attempt 3 of 5"));
        assert!(result.contains("Previous failure: tests failed."));
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
            previous_failure_reason: Some("tests failed".to_string()),
        };
        let resolved = vec![resolved_intr(
            InterruptionKind::Question,
            "Which DB?",
            Some("SQLite"),
            None,
        )];

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
            &resolved,
        );

        // Verify ordering:
        // global -> project -> plan -> agent -> retry -> resolved-interruptions
        // -> step -> criteria -> step map -> tests -> focus -> ask-instruction
        let global_pos = prompt.find("# Ralph context").unwrap();
        let project_pos = prompt.find("PROJECT-LAYER").unwrap();
        let plan_pos = prompt.find("# Plan:").unwrap();
        let agent_pos = prompt.find("# Agent Profile").unwrap();
        let retry_pos = prompt.find("# Retry Context").unwrap();
        let answered_pos = prompt.find("## Resolved interruptions").unwrap();
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
        assert!(
            plan_hdr < pl,
            "plan-layer text must sit inside the # Plan block"
        );

        // Global layer is the very start; the always-on question-ask
        // instruction is the tail (it stacks after the focus instruction).
        assert!(prompt.starts_with("GLOBAL-LAYER"));
        let focus_pos = prompt.find(&format!("Focus on: {}", step.title)).unwrap();
        let ask_pos = prompt.find("## Asking the user a question").unwrap();
        assert!(focus_pos < ask_pos);
        assert!(prompt.contains("## Asking the user a question"));
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
        // Pure prefix-stacking — the always-on question-ask instruction is
        // the tail, stacked after the focus instruction.
        let focus_pos = prompt.find(&format!("Focus on: {}", step.title)).unwrap();
        let ask_pos = prompt.find("## Asking the user a question").unwrap();
        assert!(focus_pos < ask_pos);
    }

    // ---- Question injection (always on — no per-plan opt-out) ----

    #[test]
    fn test_question_ask_instruction_always_appended() {
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

        // Header + a few load-bearing markers from the ask block.
        assert!(prompt.contains("## Asking the user a question"));
        assert!(prompt.contains("ralph question ask"));
        assert!(prompt.contains("Most decisions belong in the plan"));

        // The ask-instruction sits AFTER the focus instruction so it's the
        // last body section before any suffix wraps.
        let focus_pos = prompt.find("Only modify files").unwrap();
        let ask_pos = prompt.find("## Asking the user a question").unwrap();
        assert!(focus_pos < ask_pos);
    }

    #[test]
    fn test_resolved_interruptions_section_renders_kind_body_resolution_comment() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        // Newest-first (as the bounded query returns it).
        let resolved = vec![
            resolved_intr(
                InterruptionKind::Blocker,
                "Needs sudo to install libfoo",
                Some("Installed by operator"),
                Some("ran apt-get install libfoo-dev"),
            ),
            resolved_intr(
                InterruptionKind::Question,
                "Should this use Postgres or SQLite?",
                Some("SQLite (already a dep)"),
                None,
            ),
        ];

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &resolved,
        );

        // Section heading present; old heading gone.
        assert!(prompt.contains("## Resolved interruptions"));
        assert!(!prompt.contains("## Previously answered questions"));

        // Kind, body, resolution, and comment all render.
        assert!(prompt.contains("> **blocker**"));
        assert!(prompt.contains("> Needs sudo to install libfoo"));
        assert!(prompt.contains("> Resolution: Installed by operator"));
        assert!(prompt.contains("> Comment: ran apt-get install libfoo-dev"));
        assert!(prompt.contains("> **question**"));
        assert!(prompt.contains("> Should this use Postgres or SQLite?"));
        assert!(prompt.contains("> Resolution: SQLite (already a dep)"));

        // Section sits between Plan context and Step details.
        let plan_pos = prompt.find("# Plan:").unwrap();
        let sec_pos = prompt.find("## Resolved interruptions").unwrap();
        let step_pos = prompt.find("## Your step").unwrap();
        assert!(plan_pos < sec_pos);
        assert!(sec_pos < step_pos);

        // Rendered in the order supplied (newest-first).
        let blocker_pos = prompt.find("> **blocker**").unwrap();
        let question_pos = prompt.find("> **question**").unwrap();
        assert!(blocker_pos < question_pos);
    }

    #[test]
    fn test_resolved_interruptions_absent_when_empty() {
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

        assert!(!prompt.contains("## Resolved interruptions"));
        // No stray section blockquote markers.
        assert!(!prompt.contains("> Resolution:"));
        assert!(!prompt.contains("> Comment:"));
    }

    #[test]
    fn test_resolved_interruptions_gated_only_on_the_slice() {
        // The "Resolved interruptions" section is gated solely on the
        // `resolved_interruptions` slice: present when there's a resolution,
        // absent when empty. The ask-instruction is always present (questions
        // are always enabled), independent of whether anything was resolved.
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];
        let resolved = vec![resolved_intr(
            InterruptionKind::Question,
            "Q?",
            Some("A."),
            None,
        )];

        // Case 1: a resolution exists. Section IS rendered; ask-instruction
        // is ALSO rendered.
        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &resolved,
        );
        assert!(prompt.contains("## Resolved interruptions"));
        assert!(prompt.contains("## Asking the user a question"));

        // Case 2: nothing resolved yet. Ask-instruction IS rendered; resolved
        // section is NOT.
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
        assert!(!prompt.contains("## Resolved interruptions"));
        assert!(prompt.contains("## Asking the user a question"));
    }

    // ---- Reviewer prompt (STEP 36; §8 / §9-inv-2 / Decision 5) ----

    #[test]
    fn test_build_review_prompt_basic_structure() {
        let plan = make_plan();
        let mut step = make_step();
        step.short_id = "abcd1234".to_string();
        step.acceptance_criteria = vec![
            "spawn_harness() works".to_string(),
            "Tests pass".to_string(),
        ];

        let diff = "diff --git a/src/x.rs b/src/x.rs\n\
                    @@ -1 +1 @@\n-old\n+new";
        let prompt = build_review_prompt(&plan, &step, "deadbee", 2, diff);

        // Plan + step titles, NOT the plan description body.
        assert!(prompt.contains("**Plan:** test-plan"));
        assert!(prompt.contains("Implement harness spawning"));
        assert!(
            !prompt.contains("Build a new feature for the project"),
            "reviewer prompt must NOT carry the plan description (§8: titles \
             + acceptance criteria only)"
        );

        // Acceptance criteria ARE the rubric.
        assert!(prompt.contains("## Acceptance criteria"));
        assert!(prompt.contains("- spawn_harness() works"));
        assert!(prompt.contains("- Tests pass"));

        // The single commit diff, framed with the <short_id>.<n> handle.
        assert!(prompt.contains("abcd1234.2"));
        assert!(prompt.contains("deadbee"));
        assert!(prompt.contains("+new"));

        // §8 read-only instruction + verdict contract.
        assert!(prompt.contains("READ-ONLY"));
        assert!(prompt.contains("Do NOT modify"));
        assert!(prompt.contains("corrective step"));
        assert!(prompt.contains("REVIEW PASS"));
        assert!(prompt.contains("REVIEW FAIL"));

        // NONE of build_step_prompt's sections leak in (separate assembly).
        assert!(!prompt.contains("## Plan step map"));
        assert!(!prompt.contains("Post-harness validation"));
        assert!(!prompt.contains("## Asking the user a question"));
        assert!(!prompt.contains("# Retry Context"));
    }

    /// HARD-INVARIANT PROOF (Decision 5 / §4 / §9): the reviewer prompt
    /// carries **exactly one** commit's diff and **zero** cumulative or
    /// dependency diffs. We feed a single-commit `git show`-shaped patch and
    /// assert there is exactly one diff fence and exactly one `diff --git`
    /// header — i.e. the prompt did not (and structurally cannot) splice in a
    /// prior step's / dependency's / range diff.
    #[test]
    fn test_review_prompt_is_o1_single_commit_diff_only() {
        let plan = make_plan();
        let mut step = make_step();
        step.short_id = "ff00ff00".to_string();

        // One commit's `git show --patch` output: a single `diff --git`
        // header. (A cumulative/range diff would contain several.)
        let one_commit_diff = "diff --git a/src/a.rs b/src/a.rs\n\
             index 111..222 100644\n\
             --- a/src/a.rs\n\
             +++ b/src/a.rs\n\
             @@ -1,2 +1,2 @@\n\
             -let x = 1;\n\
             +let x = 2;";

        let prompt = build_review_prompt(&plan, &step, "abc1234", 1, one_commit_diff);

        // Exactly one fenced ```diff block.
        assert_eq!(
            prompt.matches("```diff").count(),
            1,
            "reviewer prompt must contain exactly ONE diff block (O(1) — \
             Decision 5). Prompt:\n{prompt}"
        );
        // Exactly one `diff --git` header — proves no cumulative/dependency
        // diff was concatenated in (those carry one header per file/commit).
        assert_eq!(
            prompt.matches("diff --git").count(),
            1,
            "reviewer prompt must carry exactly one commit's diff, never a \
             cumulative or dependency diff (§9 hard invariant). Prompt:\n{prompt}"
        );
        // And the diff content is precisely what the caller supplied,
        // verbatim — the builder neither expands nor accumulates it.
        assert!(prompt.contains(one_commit_diff));
    }

    /// §4 fix proof: even when many resolved interruptions with very long
    /// fields are passed, the rendered section is bounded in BOTH
    /// dimensions — entry count (caller's slice, which in production is the
    /// bounded query) AND per-field length (`truncate_text` inside the
    /// formatter). This test feeds the formatter a deliberately oversized
    /// input and asserts the output is small.
    #[test]
    fn test_resolved_interruptions_section_is_bounded_in_count_and_per_field() {
        let plan = make_plan();
        let step = make_step();
        let all_steps = vec![step.clone()];

        // Production passes at most DEFAULT_RESOLVED_INTERRUPTION_LIMIT (5)
        // entries; emulate that bound here and make every field pathological
        // (5000 lines each).
        const N: usize = crate::storage::DEFAULT_RESOLVED_INTERRUPTION_LIMIT;
        let huge = (0..5000)
            .map(|i| format!("line-{i}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
            .collect::<Vec<_>>()
            .join("\n");
        let resolved: Vec<Interruption> = (0..N)
            .map(|_| resolved_intr(InterruptionKind::Question, &huge, Some(&huge), Some(&huge)))
            .collect();

        let prompt = build_step_prompt(
            &plan,
            &step,
            &all_steps,
            None,
            None,
            true,
            &Prompts::default(),
            &resolved,
        );

        // Isolate the rendered section (from its heading to the next `## `
        // heading) so we measure only this section's contribution.
        let start = prompt.find("## Resolved interruptions").unwrap();
        let rest = &prompt[start + "## Resolved interruptions".len()..];
        let end = rest
            .find("\n## ")
            .map(|p| start + p)
            .unwrap_or(prompt.len());
        let section = &prompt[start..end];

        // (1) Per-field truncation fired — the elision marker is present and
        // the raw 5000-line field did NOT survive intact.
        assert!(
            section.contains("lines omitted"),
            "each oversized field must be truncate_text'd"
        );
        assert!(
            !section.contains("line-4999-"),
            "the tail of a 5000-line field must be dropped"
        );

        // (2) Hard upper bound on the whole section's line count:
        // N entries × (1 kind line + 3 separators + 3 fields ×
        // (FIELD_MAX_LINES + 1 elision + 1 label)) + heading/blank/sep slack.
        // Use a generous but FINITE ceiling that does NOT scale with the
        // 5000-line input.
        let section_lines = section.lines().count();
        let ceiling = N * (3 * (RESOLVED_INTERRUPTION_FIELD_MAX_LINES + 4) + 8) + 8;
        assert!(
            section_lines <= ceiling,
            "section must be bounded: {section_lines} lines > ceiling {ceiling}"
        );
        // Sanity: the ceiling is far below the unbounded size (≈ N×3×5000).
        assert!(ceiling < N * 3 * 5000 / 10);
    }
}
