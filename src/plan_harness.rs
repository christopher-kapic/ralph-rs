// Interactive plan-harness: spawn a coding harness to create/update ralph-rs plans.
#![allow(dead_code)]

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::Config;
use crate::harness;

/// Embedded agent definition for the harness-plan agent.
///
/// This instructs the harness to investigate the codebase and create plans
/// by calling ralph-rs CLI commands.
const HARNESS_PLAN_AGENT: &str = r#"# ralph-rs Plan Agent

You are helping the user create or update a ralph-rs execution plan. ralph-rs is a deterministic
orchestrator for coding agent harnesses. Your job is to investigate the codebase and create a
structured plan with steps that can be executed by coding agents.

## Available Commands

Use these ralph-rs CLI commands to manage plans and steps:

### Plan Management
- `ralph-rs plan create <slug> --description "<desc>" [--branch <branch>] [--test "<cmd>"]`
- `ralph-rs plan list`
- `ralph-rs plan show <slug>`
- `ralph-rs plan approve <slug>`
- `ralph-rs plan delete <slug> --force`

### Step Management
- `ralph-rs step add "<title>" --plan <slug> [--description "<desc>"] [--after <n>]`
- `ralph-rs step list --plan <slug>`
- `ralph-rs step edit <n> --plan <slug> [--title "<title>"] [--description "<desc>"]`
- `ralph-rs step remove <n> --plan <slug> --force`
- `ralph-rs step move <n> --to <m> --plan <slug>`
- `ralph-rs step reset <n> --plan <slug>`

## Workflow

1. Investigate the project structure, code, and any existing plans.
2. Discuss the approach with the user if needed.
3. Create a plan with `ralph-rs plan create`.
4. Add steps with `ralph-rs step add`, each with a clear title and detailed description.
5. Include acceptance criteria and context in step descriptions.
6. Set deterministic test commands on the plan (e.g., `--test "cargo build" --test "cargo test"`).
7. Show the final plan with `ralph-rs plan show` for user review.
8. Approve the plan with `ralph-rs plan approve` when the user is satisfied.

## Guidelines

- Each step should be atomic and independently verifiable.
- Steps should be ordered so that earlier steps don't depend on later ones.
- Include enough context in each step description that an agent can execute it without
  seeing other steps.
- Deterministic tests should validate the overall project health after each step.
- Prefer smaller, focused steps over large monolithic ones.
"#;

/// Build the initial prompt for the plan-harness session.
fn build_initial_prompt(project: &str, description: Option<&str>) -> String {
    match description {
        Some(desc) => {
            format!("Create a ralph-rs plan for the project at {project}. Description: {desc}")
        }
        None => format!("Help me create or update a ralph-rs plan for the project at {project}."),
    }
}

/// Build harness arguments for interactive plan-harness mode.
///
/// Different harnesses receive the agent definition differently:
/// - **claude**: Gets `--system-prompt-file <temp_file>` pointing to the agent definition
/// - **goose**: Gets `GOOSE_SYSTEM_PROMPT_FILE_PATH` env var set to the agent file path
/// - **others** (codex, pi, opencode, copilot): Agent content is prepended to the prompt
fn build_plan_harness_args(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
    prompt: &str,
) -> Vec<String> {
    let harness_config = &config.harnesses[harness_name];
    let mut args = Vec::new();

    // For claude, add --system-prompt-file flag
    if harness_config.supports_agent_file
        && let Some(path) = agent_file_path
    {
        args.push("--system-prompt-file".to_string());
        args.push(path.to_string_lossy().to_string());
    }

    // Add the prompt. For harnesses without native agent file support,
    // prepend the agent content to the prompt.
    if !harness_config.supports_agent_file && agent_file_path.is_some() {
        // Prepend agent definition to the prompt
        let full_prompt = format!("{HARNESS_PLAN_AGENT}\n\n---\n\n{prompt}");
        args.push(full_prompt);
    } else {
        args.push(prompt.to_string());
    }

    args
}

/// Build environment variables for the plan-harness session.
fn build_plan_harness_env(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
) -> Vec<(String, String)> {
    let harness_config = &config.harnesses[harness_name];
    let mut env_vars = Vec::new();

    // Goose uses an env var for the system prompt file
    if let Some(ref env_name) = harness_config.agent_file_env
        && !harness_config.supports_agent_file
        && let Some(path) = agent_file_path
    {
        env_vars.push((env_name.clone(), path.to_string_lossy().to_string()));
    }

    env_vars
}

/// Run the interactive plan-harness: spawn a harness with the plan agent definition
/// and wait for it to exit.
///
/// Returns the harness exit code.
pub async fn run_plan_harness(
    config: &Config,
    harness_name: &str,
    project: &str,
    description: Option<&str>,
) -> Result<i32> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;

    // Write the agent definition to a temporary file.
    // This file lives for the duration of the harness process.
    let agent_temp_file = write_agent_temp_file()?;
    let agent_file_path = agent_temp_file.path();

    // Build the initial prompt
    let prompt = build_initial_prompt(project, description);

    // Build per-harness args and env
    let args = build_plan_harness_args(harness_name, config, Some(agent_file_path), &prompt);
    let env_vars = build_plan_harness_env(harness_name, config, Some(agent_file_path));

    // Spawn the harness interactively
    let cwd = std::path::Path::new(project);
    let mut child = harness::spawn_harness_interactive(harness_config, &args, &env_vars, cwd)
        .await
        .with_context(|| format!("Failed to spawn plan-harness '{harness_name}'"))?;

    // Wait for the harness to exit
    let status = child
        .wait()
        .await
        .context("Failed to wait for plan-harness process")?;

    // The temp file is cleaned up when agent_temp_file is dropped
    Ok(status.code().unwrap_or(1))
}

/// A temporary file that is cleaned up on drop.
pub struct TempAgentFile {
    path: PathBuf,
}

impl TempAgentFile {
    /// Returns the path to the temporary agent file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempAgentFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Write the embedded agent definition to a temporary file.
fn write_agent_temp_file() -> Result<TempAgentFile> {
    let file_name = format!("ralph-rs-plan-agent-{}.md", std::process::id());
    let path = std::env::temp_dir().join(file_name);

    let mut file = std::fs::File::create(&path).with_context(|| {
        format!(
            "Failed to create temporary agent file at {}",
            path.display()
        )
    })?;
    file.write_all(HARNESS_PLAN_AGENT.as_bytes())
        .context("Failed to write agent definition to temp file")?;
    file.flush().context("Failed to flush agent temp file")?;

    Ok(TempAgentFile { path })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_build_initial_prompt_with_description() {
        let prompt = build_initial_prompt("/tmp/project", Some("Add authentication"));
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("Add authentication"));
        assert!(prompt.starts_with("Create a ralph-rs plan"));
    }

    #[test]
    fn test_build_initial_prompt_without_description() {
        let prompt = build_initial_prompt("/tmp/project", None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.starts_with("Help me create or update"));
    }

    #[test]
    fn test_build_plan_harness_args_claude() {
        let config = Config::default();
        let agent_file = write_agent_temp_file().unwrap();
        let args =
            build_plan_harness_args("claude", &config, Some(agent_file.path()), "Create a plan");

        // Claude should get --system-prompt-file and the prompt as separate args
        assert!(args.contains(&"--system-prompt-file".to_string()));
        assert!(args.contains(&"Create a plan".to_string()));
        // Agent content should NOT be in the prompt
        assert!(!args.iter().any(|a| a.contains("ralph-rs Plan Agent")));
    }

    #[test]
    fn test_build_plan_harness_args_codex_prepends_agent() {
        let config = Config::default();
        let agent_file = write_agent_temp_file().unwrap();
        let args =
            build_plan_harness_args("codex", &config, Some(agent_file.path()), "Create a plan");

        // Codex doesn't support agent files, so agent content should be prepended
        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph-rs Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Create a plan")));
    }

    #[test]
    fn test_build_plan_harness_args_pi_prepends_agent() {
        let config = Config::default();
        let agent_file = write_agent_temp_file().unwrap();
        let args = build_plan_harness_args("pi", &config, Some(agent_file.path()), "Help me plan");

        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph-rs Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Help me plan")));
    }

    #[test]
    fn test_build_plan_harness_env_goose() {
        let config = Config::default();
        let agent_file = write_agent_temp_file().unwrap();
        let env = build_plan_harness_env("goose", &config, Some(agent_file.path()));

        // Goose config has agent_file_env: None in default config
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_env_goose_with_env() {
        let mut config = Config::default();
        // Simulate goose having the env var configured
        if let Some(goose) = config.harnesses.get_mut("goose") {
            goose.agent_file_env = Some("GOOSE_SYSTEM_PROMPT_FILE_PATH".to_string());
        }
        let agent_file = write_agent_temp_file().unwrap();
        let env = build_plan_harness_env("goose", &config, Some(agent_file.path()));

        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_SYSTEM_PROMPT_FILE_PATH");
    }

    #[test]
    fn test_build_plan_harness_env_claude_no_env() {
        let config = Config::default();
        let agent_file = write_agent_temp_file().unwrap();
        let env = build_plan_harness_env("claude", &config, Some(agent_file.path()));

        // Claude supports agent file natively, so env var should NOT be set
        assert!(env.is_empty());
    }

    #[test]
    fn test_write_agent_temp_file() {
        let agent_file = write_agent_temp_file().unwrap();
        let content = std::fs::read_to_string(agent_file.path()).unwrap();
        assert!(content.contains("ralph-rs Plan Agent"));
        assert!(content.contains("ralph-rs plan create"));
        assert!(content.contains("ralph-rs step add"));
    }

    #[test]
    fn test_harness_plan_agent_content() {
        // Verify the embedded agent definition has key sections
        assert!(HARNESS_PLAN_AGENT.contains("Plan Management"));
        assert!(HARNESS_PLAN_AGENT.contains("Step Management"));
        assert!(HARNESS_PLAN_AGENT.contains("Workflow"));
        assert!(HARNESS_PLAN_AGENT.contains("Guidelines"));
        assert!(HARNESS_PLAN_AGENT.contains("ralph-rs plan create"));
        assert!(HARNESS_PLAN_AGENT.contains("ralph-rs step add"));
        assert!(HARNESS_PLAN_AGENT.contains("ralph-rs plan approve"));
    }
}
