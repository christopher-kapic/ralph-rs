// Harness subprocess management
#![allow(dead_code)]

use std::path::Path;

use anyhow::{Context, Result};
use tokio::process::Command;

use crate::config::{Config, HarnessConfig};
use crate::plan::{Plan, Step};

/// Placeholder token in harness args that gets replaced with the actual prompt.
const PROMPT_PLACEHOLDER: &str = "{prompt}";

/// Placeholder token in harness args for the agent file path.
const AGENT_FILE_PLACEHOLDER: &str = "{agent_file}";

/// Resolve which harness name to use, following the precedence chain:
/// step.harness -> plan.harness -> config.default_harness
pub fn resolve_harness_name(step: &Step, plan: &Plan, config: &Config) -> String {
    step.harness
        .as_deref()
        .or(plan.harness.as_deref())
        .unwrap_or(&config.default_harness)
        .to_string()
}

/// Resolve a harness name to its configuration entry.
pub fn resolve_harness<'a>(
    step: &Step,
    plan: &Plan,
    config: &'a Config,
) -> Result<(&'a str, &'a HarnessConfig)> {
    let name = resolve_harness_name(step, plan, config);
    let harness_config = config.harnesses.get(&name).with_context(|| {
        format!(
            "Unknown harness '{name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;
    // Return a reference tied to the config lifetime
    let name_ref = config.harnesses.get_key_value(&name).unwrap().0.as_str();
    Ok((name_ref, harness_config))
}

/// Build the full argument list for a harness invocation.
///
/// This:
/// 1. Starts from the harness config's default args
/// 2. Replaces `{prompt}` placeholders with the actual prompt text
/// 3. Handles agent file injection based on harness type
/// 4. Appends JSON output args if supported
pub fn build_harness_args(
    harness_name: &str,
    harness_config: &HarnessConfig,
    prompt: &str,
    agent_file: Option<&Path>,
) -> Vec<String> {
    let mut args = harness_config.args.clone();

    // Replace {prompt} placeholders or append prompt at end
    let has_prompt_placeholder = args.iter().any(|a| a.contains(PROMPT_PLACEHOLDER));
    if has_prompt_placeholder {
        args = args
            .into_iter()
            .map(|a| a.replace(PROMPT_PLACEHOLDER, prompt))
            .collect();
    } else {
        // For most harnesses, the prompt is appended as the last arg
        args.push(prompt.to_string());
    }

    // Handle agent file injection based on harness type
    if let Some(agent_path) = agent_file {
        let agent_path_str = agent_path.to_string_lossy().to_string();
        inject_agent_file(harness_name, harness_config, &mut args, &agent_path_str);
    }

    // Remove {agent_file} placeholder and its surrounding flag if no agent file
    if agent_file.is_none() {
        remove_agent_file_args(&mut args);
    }

    // Append JSON output args if supported
    if harness_config.supports_json_output {
        args.extend(harness_config.json_output_args.clone());
    }

    args
}

/// Inject the agent file path into the args based on harness type.
///
/// - **claude**: Uses native `--agent-file` flag (supports_agent_file = true)
/// - **codex, pi, opencode, copilot**: Agent content is prepended to the prompt
///   (handled externally in prompt construction, not here)
/// - **goose**: Uses environment variable (handled via agent_file_env)
fn inject_agent_file(
    _harness_name: &str,
    harness_config: &HarnessConfig,
    args: &mut Vec<String>,
    agent_path: &str,
) {
    if harness_config.supports_agent_file {
        // Native flag support (e.g., claude --agent-file <path>)
        // Replace any {agent_file} placeholder, or add the flag
        let has_placeholder = args.iter().any(|a| a.contains(AGENT_FILE_PLACEHOLDER));
        if has_placeholder {
            *args = args
                .iter()
                .map(|a| a.replace(AGENT_FILE_PLACEHOLDER, agent_path))
                .collect();
        } else {
            args.push("--agent-file".to_string());
            args.push(agent_path.to_string());
        }
    }
    // For harnesses using agent_file_env (goose), environment variable is set
    // on the Command, not in args — handled in spawn_harness().
    // For harnesses without native support (codex, pi, opencode, copilot),
    // the agent file content is prepended to the prompt in prompt.rs.
}

/// Remove `{agent_file}` placeholder tokens and their associated flags from args.
///
/// This handles cases like `["--agent-file", "{agent_file}"]` where the placeholder
/// flag pair should be stripped when no agent file is specified.
pub fn remove_agent_file_args(args: &mut Vec<String>) {
    // Find indices to remove (the placeholder and its preceding flag)
    let mut indices_to_remove = Vec::new();
    for (i, arg) in args.iter().enumerate() {
        if arg.contains(AGENT_FILE_PLACEHOLDER) {
            indices_to_remove.push(i);
            // Also remove the preceding flag if it looks like a flag
            if i > 0 && args[i - 1].starts_with('-') {
                indices_to_remove.push(i - 1);
            }
        }
    }

    // Remove in reverse order to preserve indices
    indices_to_remove.sort_unstable();
    indices_to_remove.dedup();
    for &idx in indices_to_remove.iter().rev() {
        args.remove(idx);
    }
}

/// Build the environment variables for a harness invocation.
pub fn build_harness_env(
    harness_config: &HarnessConfig,
    agent_file: Option<&Path>,
) -> Vec<(String, String)> {
    let mut env_vars = Vec::new();

    // Set agent file env var if the harness uses one (e.g., goose)
    if let (Some(env_name), Some(agent_path)) = (&harness_config.agent_file_env, agent_file)
        && !harness_config.supports_agent_file
    {
        // Only use env var for harnesses that don't have native flag support
        env_vars.push((env_name.clone(), agent_path.to_string_lossy().to_string()));
    }

    env_vars
}

/// Spawn a harness process in non-interactive mode with piped stdout/stderr.
///
/// Returns a handle to the child process.
pub async fn spawn_harness(
    harness_config: &HarnessConfig,
    args: &[String],
    env_vars: &[(String, String)],
    cwd: &Path,
) -> Result<tokio::process::Child> {
    let mut cmd = Command::new(&harness_config.command);
    cmd.args(args)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    let child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn harness '{}'", harness_config.command))?;

    Ok(child)
}

/// Spawn a harness process in interactive mode with inherited stdio.
///
/// Used for `plan:harness` mode where the user interacts directly with the harness.
pub async fn spawn_harness_interactive(
    harness_config: &HarnessConfig,
    args: &[String],
    env_vars: &[(String, String)],
    cwd: &Path,
) -> Result<tokio::process::Child> {
    let mut cmd = Command::new(&harness_config.command);
    cmd.args(args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    let child = cmd.spawn().with_context(|| {
        format!(
            "Failed to spawn interactive harness '{}'",
            harness_config.command
        )
    })?;

    Ok(child)
}

/// Wait for a spawned harness process to complete and capture its output.
pub async fn wait_for_harness(child: tokio::process::Child) -> Result<HarnessOutput> {
    let output = child
        .wait_with_output()
        .await
        .context("Failed to wait for harness process")?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let exit_code = output.status.code();
    let success = output.status.success();

    Ok(HarnessOutput {
        stdout,
        stderr,
        exit_code,
        success,
    })
}

/// Output captured from a harness invocation.
#[derive(Debug, Clone)]
pub struct HarnessOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub success: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::plan::{Plan, PlanStatus, Step, StepStatus};
    use chrono::Utc;

    fn make_plan(harness: Option<&str>) -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "test-plan".to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "feat/test".to_string(),
            description: "A test plan".to_string(),
            status: PlanStatus::Ready,
            harness: harness.map(|s| s.to_string()),
            agent: None,
            deterministic_tests: vec!["cargo test".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_step(harness: Option<&str>) -> Step {
        Step {
            id: "s1".to_string(),
            plan_id: "p1".to_string(),
            sort_key: "a0".to_string(),
            title: "Step 1".to_string(),
            description: "First step".to_string(),
            agent: None,
            harness: harness.map(|s| s.to_string()),
            acceptance_criteria: vec!["tests pass".to_string()],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_resolve_harness_step_overrides_plan() {
        let config = Config::default();
        let plan = make_plan(Some("codex"));
        let step = make_step(Some("pi"));

        let name = resolve_harness_name(&step, &plan, &config);
        assert_eq!(name, "pi");
    }

    #[test]
    fn test_resolve_harness_plan_overrides_default() {
        let config = Config::default();
        let plan = make_plan(Some("codex"));
        let step = make_step(None);

        let name = resolve_harness_name(&step, &plan, &config);
        assert_eq!(name, "codex");
    }

    #[test]
    fn test_resolve_harness_falls_back_to_default() {
        let config = Config::default();
        let plan = make_plan(None);
        let step = make_step(None);

        let name = resolve_harness_name(&step, &plan, &config);
        assert_eq!(name, "claude");
    }

    #[test]
    fn test_resolve_harness_returns_config() {
        let config = Config::default();
        let plan = make_plan(None);
        let step = make_step(None);

        let (name, hc) = resolve_harness(&step, &plan, &config).unwrap();
        assert_eq!(name, "claude");
        assert_eq!(hc.command, "claude");
        assert!(hc.supports_agent_file);
    }

    #[test]
    fn test_resolve_harness_unknown_errors() {
        let config = Config::default();
        let plan = make_plan(Some("nonexistent"));
        let step = make_step(None);

        let result = resolve_harness(&step, &plan, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_harness_args_replaces_prompt_placeholder() {
        let hc = HarnessConfig {
            command: "test-harness".to_string(),
            args: vec!["-p".to_string(), "{prompt}".to_string()],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
        };

        let args = build_harness_args("test", &hc, "do the thing", None);
        assert_eq!(args, vec!["-p", "do the thing"]);
    }

    #[test]
    fn test_build_harness_args_appends_prompt_when_no_placeholder() {
        let hc = HarnessConfig {
            command: "codex".to_string(),
            args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
        };

        let args = build_harness_args("codex", &hc, "implement feature", None);
        assert_eq!(args, vec!["implement feature"]);
    }

    #[test]
    fn test_build_harness_args_appends_json_output() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let args = build_harness_args("claude", hc, "do stuff", None);
        // Should contain -p, prompt, and JSON output args
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"json".to_string()));
    }

    #[test]
    fn test_build_harness_args_no_json_when_unsupported() {
        let config = Config::default();
        let hc = &config.harnesses["pi"];

        let args = build_harness_args("pi", hc, "do stuff", None);
        assert!(!args.contains(&"--json".to_string()));
        assert!(!args.contains(&"--output-format".to_string()));
    }

    #[test]
    fn test_build_harness_args_claude_agent_file() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];
        let agent_path = Path::new("/home/user/.ralph2/agents/default.md");

        let args = build_harness_args("claude", hc, "do stuff", Some(agent_path));
        assert!(args.contains(&"--agent-file".to_string()));
        assert!(args.contains(&"/home/user/.ralph2/agents/default.md".to_string()));
    }

    #[test]
    fn test_build_harness_args_no_agent_file_no_flag() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let args = build_harness_args("claude", hc, "do stuff", None);
        assert!(!args.contains(&"--agent-file".to_string()));
    }

    #[test]
    fn test_remove_agent_file_args() {
        let mut args = vec![
            "-p".to_string(),
            "prompt".to_string(),
            "--agent-file".to_string(),
            "{agent_file}".to_string(),
        ];
        remove_agent_file_args(&mut args);
        assert_eq!(args, vec!["-p", "prompt"]);
    }

    #[test]
    fn test_remove_agent_file_args_no_placeholder() {
        let mut args = vec!["-p".to_string(), "prompt".to_string()];
        remove_agent_file_args(&mut args);
        assert_eq!(args, vec!["-p", "prompt"]);
    }

    #[test]
    fn test_build_harness_env_goose() {
        let hc = HarnessConfig {
            command: "goose".to_string(),
            args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: Some("GOOSE_AGENT_FILE".to_string()),
        };

        let agent_path = Path::new("/home/user/.ralph2/agents/default.md");
        let env = build_harness_env(&hc, Some(agent_path));
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_AGENT_FILE");
        assert_eq!(env[0].1, "/home/user/.ralph2/agents/default.md");
    }

    #[test]
    fn test_build_harness_env_no_agent() {
        let hc = HarnessConfig {
            command: "goose".to_string(),
            args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: Some("GOOSE_AGENT_FILE".to_string()),
        };

        let env = build_harness_env(&hc, None);
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_harness_env_claude_uses_flag_not_env() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];
        let agent_path = Path::new("/home/user/.ralph2/agents/default.md");

        // Claude has agent_file_env set but also supports_agent_file = true,
        // so the env var should NOT be set (flag is used instead).
        let env = build_harness_env(hc, Some(agent_path));
        assert!(env.is_empty());
    }

    #[test]
    fn test_resolve_harness_chain_all_levels() {
        let config = Config::default();

        // All None -> default
        let plan = make_plan(None);
        let step = make_step(None);
        assert_eq!(resolve_harness_name(&step, &plan, &config), "claude");

        // Plan set, step None -> plan
        let plan = make_plan(Some("pi"));
        let step = make_step(None);
        assert_eq!(resolve_harness_name(&step, &plan, &config), "pi");

        // Both set -> step wins
        let plan = make_plan(Some("pi"));
        let step = make_step(Some("opencode"));
        assert_eq!(resolve_harness_name(&step, &plan, &config), "opencode");
    }

    #[test]
    fn test_build_harness_args_replaces_prompt_within_arg() {
        let hc = HarnessConfig {
            command: "test".to_string(),
            args: vec!["--prompt={prompt}".to_string()],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
        };

        let args = build_harness_args("test", &hc, "hello world", None);
        assert_eq!(args, vec!["--prompt=hello world"]);
    }

    #[test]
    fn test_build_harness_args_agent_file_placeholder_replaced() {
        let hc = HarnessConfig {
            command: "claude".to_string(),
            args: vec![
                "-p".to_string(),
                "--agent-file".to_string(),
                "{agent_file}".to_string(),
            ],
            supports_agent_file: true,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
        };

        let agent_path = Path::new("/tmp/agent.md");
        let args = build_harness_args("claude", &hc, "do stuff", Some(agent_path));
        assert!(args.contains(&"/tmp/agent.md".to_string()));
        assert!(!args.iter().any(|a| a.contains("{agent_file}")));
    }
}
