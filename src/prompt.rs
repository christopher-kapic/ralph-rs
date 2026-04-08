// Prompt generation
#![allow(dead_code)]

use std::path::Path;

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
    /// Step title.
    pub title: String,
    /// Step status (should be Complete or Skipped).
    pub status: StepStatus,
    /// Files changed in this step (if available).
    pub files_changed: Vec<String>,
    /// Brief description of what was done.
    pub description: String,
}

/// Build the full prompt for a step execution.
///
/// The prompt is assembled from 8 parts:
/// 1. Agent definition (from agent file content, if available)
/// 2. Retry context (if this is a retry attempt)
/// 3. Plan context (plan description and overall goal)
/// 4. Prior steps summary (what has been done so far)
/// 5. Step details (title and description of current step)
/// 6. Acceptance criteria (specific criteria the step must meet)
/// 7. Deterministic tests (test commands that must pass)
/// 8. Focus instruction (reminder to stay focused on just this step)
pub fn build_step_prompt(
    plan: &Plan,
    step: &Step,
    prior_steps: &[PriorStepSummary],
    agent_file_content: Option<&str>,
    retry_context: Option<&RetryContext>,
    harness_supports_agent_file: bool,
) -> String {
    let mut sections: Vec<String> = Vec::new();

    // 1. Agent definition
    // Only prepend agent content for harnesses without native agent file support.
    // Harnesses with native support (e.g., claude) receive the agent file via flag/env.
    if !harness_supports_agent_file && let Some(agent_content) = agent_file_content {
        sections.push(format_agent_definition(agent_content));
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

    sections.join("\n\n")
}

// ---------------------------------------------------------------------------
// Section formatters
// ---------------------------------------------------------------------------

fn format_agent_definition(content: &str) -> String {
    format!(
        "# Agent Definition\n\n\
         {content}"
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

    for (i, step) in prior_steps.iter().enumerate() {
        let status_marker = match step.status {
            StepStatus::Complete => "completed",
            StepStatus::Skipped => "skipped",
            _ => "other",
        };

        let mut step_line = format!(
            "\n**Step {} ({status_marker}): {title}**",
            i + 1,
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
    let mut lines = vec![
        "## Deterministic Tests".to_string(),
        String::new(),
        "The following commands will be run after your changes. All must pass:".to_string(),
    ];
    for test in tests {
        lines.push(format!("\n```\n{test}\n```"));
    }
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

/// Truncate text to a maximum number of lines, adding a note if truncated.
fn truncate_text(text: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= max_lines {
        text.to_string()
    } else {
        let omitted = lines.len() - max_lines;
        let tail = &lines[lines.len() - max_lines..];
        format!("... ({omitted} lines omitted) ...\n{}", tail.join("\n"))
    }
}

/// Read agent file content from a path, returning None if the file doesn't exist.
pub fn read_agent_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
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
            created_at: Utc::now(),
            updated_at: Utc::now(),
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
        }
    }

    #[test]
    fn test_build_step_prompt_all_sections() {
        let plan = make_plan();
        let step = make_step();
        let prior_steps = vec![PriorStepSummary {
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

        // Should contain deterministic tests
        assert!(prompt.contains("Deterministic Tests"));
        assert!(prompt.contains("cargo build"));
        assert!(prompt.contains("cargo test"));
        assert!(prompt.contains("cargo clippy"));

        // Should contain focus instruction
        assert!(prompt.contains("Only modify files relevant"));
    }

    #[test]
    fn test_build_step_prompt_with_agent_file_prepend() {
        let plan = make_plan();
        let step = make_step();
        let agent_content = "You are a senior engineer. Follow best practices.";

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            Some(agent_content),
            None,
            false, // harness does NOT support agent file natively
        );

        // Agent definition should be prepended
        assert!(prompt.starts_with("# Agent Definition"));
        assert!(prompt.contains("senior engineer"));
    }

    #[test]
    fn test_build_step_prompt_no_agent_prepend_when_native() {
        let plan = make_plan();
        let step = make_step();
        let agent_content = "You are a senior engineer.";

        let prompt = build_step_prompt(
            &plan,
            &step,
            &[],
            Some(agent_content),
            None,
            true, // harness supports agent file natively
        );

        // Agent definition should NOT be in the prompt
        assert!(!prompt.contains("# Agent Definition"));
        assert!(!prompt.contains("senior engineer"));
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

        let prompt = build_step_prompt(&plan, &step, &[], None, Some(&retry), true);

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

        let prompt = build_step_prompt(&plan, &step, &[], None, None, true);

        // Should not contain prior steps section
        assert!(!prompt.contains("Context from Prior Steps"));
    }

    #[test]
    fn test_build_step_prompt_no_acceptance_criteria() {
        let plan = make_plan();
        let mut step = make_step();
        step.acceptance_criteria = vec![];

        let prompt = build_step_prompt(&plan, &step, &[], None, None, true);

        assert!(!prompt.contains("Acceptance Criteria"));
    }

    #[test]
    fn test_build_step_prompt_no_tests() {
        let mut plan = make_plan();
        plan.deterministic_tests = vec![];
        let step = make_step();

        let prompt = build_step_prompt(&plan, &step, &[], None, None, true);

        assert!(!prompt.contains("Deterministic Tests"));
    }

    #[test]
    fn test_truncate_text_short() {
        let text = "line 1\nline 2\nline 3";
        let result = truncate_text(text, 10);
        assert_eq!(result, text);
    }

    #[test]
    fn test_truncate_text_long() {
        let lines: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        let text = lines.join("\n");
        let result = truncate_text(&text, 5);
        assert!(result.contains("(15 lines omitted)"));
        assert!(result.contains("line 19"));
        assert!(result.contains("line 15"));
        assert!(!result.contains("line 0"));
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
                title: "Step A".to_string(),
                status: StepStatus::Complete,
                files_changed: vec!["a.rs".to_string()],
                description: "Did A".to_string(),
            },
            PriorStepSummary {
                title: "Step B".to_string(),
                status: StepStatus::Skipped,
                files_changed: vec![],
                description: "Skipped B".to_string(),
            },
        ];

        let result = format_prior_steps(&steps);
        assert!(result.contains("Step 1 (completed): Step A"));
        assert!(result.contains("Step 2 (skipped): Step B"));
        assert!(result.contains("a.rs"));
    }

    #[test]
    fn test_read_agent_file_nonexistent() {
        let result = read_agent_file(Path::new("/nonexistent/path/agent.md"));
        assert!(result.is_none());
    }

    #[test]
    fn test_prompt_section_order() {
        let plan = make_plan();
        let step = make_step();
        let prior = vec![PriorStepSummary {
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

        let prompt =
            build_step_prompt(&plan, &step, &prior, Some("agent def"), Some(&retry), false);

        // Verify ordering: agent -> retry -> plan -> prior -> step -> criteria -> tests -> focus
        let agent_pos = prompt.find("# Agent Definition").unwrap();
        let retry_pos = prompt.find("# Retry Context").unwrap();
        let plan_pos = prompt.find("# Plan:").unwrap();
        let prior_pos = prompt.find("Context from Prior Steps").unwrap();
        let step_pos = prompt.find("# Step:").unwrap();
        let criteria_pos = prompt.find("Acceptance Criteria").unwrap();
        let tests_pos = prompt.find("Deterministic Tests").unwrap();
        let focus_pos = prompt.find("Only modify files").unwrap();

        assert!(agent_pos < retry_pos);
        assert!(retry_pos < plan_pos);
        assert!(plan_pos < prior_pos);
        assert!(prior_pos < step_pos);
        assert!(step_pos < criteria_pos);
        assert!(criteria_pos < tests_pos);
        assert!(tests_pos < focus_pos);
    }
}
