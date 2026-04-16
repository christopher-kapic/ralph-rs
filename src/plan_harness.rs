// Interactive plan-harness: spawn a coding harness to create/update ralph-rs plans.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::Config;
use crate::harness;
use crate::hook_library::{self, Hook, Scope};

/// Base agent definition for the harness-plan agent.
///
/// The hook library section is appended at runtime by [`render_plan_agent`].
const HARNESS_PLAN_AGENT_BASE: &str = r#"# ralph Plan Agent

You are helping the user create or update a ralph execution plan. ralph is a deterministic
orchestrator for coding agent harnesses. Your job is to investigate the codebase and create a
structured plan with steps that can be executed by coding agents.

## Available Commands

Use these ralph CLI commands to manage plans and steps:

### Plan Management
- `ralph plan create <slug> --description "<desc>" [--branch <branch>] [--test "<cmd>"]`
- `ralph plan list`
- `ralph plan show <slug>`
- `ralph plan approve <slug>`
- `ralph plan delete <slug> --force`

### Step Management

Plan slug is a trailing positional argument on every step command and defaults
to the active plan when omitted.

- `ralph step add "<title>" <slug> [--description "<desc>"] [--after <n>]`
- `ralph step list <slug>`
- `ralph step edit <n> <slug> [--title "<title>"] [--description "<desc>"]`
- `ralph step remove <n> <slug> --force`
- `ralph step move <n> --to <m> <slug>`
- `ralph step reset <n> <slug>`

### Hook Attachment

ralph supports lifecycle hooks that run shell commands at specific points during step
execution (pre-step, post-step, pre-test, post-test). The user has a curated **hook library**
(see the "Available Hooks" section below). You attach hooks by name — you do NOT invent new
shell commands. If a hook you want doesn't exist in the library, tell the user and ask them
to create it with `ralph hooks add`.

- `ralph plan set-hook <slug> --lifecycle <l> --hook <name>` — attach a plan-wide hook
  (fires for every step in the plan). Use this for things like "review every completed step".
- `ralph step set-hook <n> <slug> --lifecycle <l> --hook <name>` — attach a hook to
  a specific step. Use this when only certain steps need review, linting, or extra checks.
- `ralph plan hooks <slug>` — show all hooks attached to a plan.

Hooks are most useful for post-step review: e.g., if a step is particularly risky or has
subtle acceptance criteria, attach a `post-step` hook that runs a review agent against the
diff. You should proactively suggest hooks when a step looks like it would benefit from
automated post-execution review.

## Workflow

1. Investigate the project structure, code, and any existing plans.
2. Discuss the approach with the user if needed.
3. Create a plan with `ralph plan create`.
4. Add steps with `ralph step add`, each with a clear title and detailed description.
5. Include acceptance criteria and context in step descriptions.
6. Set deterministic test commands on the plan (e.g., `--test "cargo build" --test "cargo test"`).
7. Consider which steps would benefit from post-step review hooks and attach them via
   `ralph step set-hook` or `ralph plan set-hook`. Only reference hooks that appear in the
   "Available Hooks" list below.
8. Show the final plan with `ralph plan show` for user review.
9. Approve the plan with `ralph plan approve` when the user is satisfied.

## Guidelines

- Each step should be atomic and independently verifiable.
- Steps should be ordered so that earlier steps don't depend on later ones.
- Include enough context in each step description that an agent can execute it without
  seeing other steps.
- Deterministic tests should validate the overall project health after each step.
- Prefer smaller, focused steps over large monolithic ones.
"#;

/// Render the plan agent definition, appending a list of hooks applicable
/// to the current project so the harness can reference them by name.
pub fn render_plan_agent(applicable_hooks: &[Hook]) -> String {
    let mut out = String::from(HARNESS_PLAN_AGENT_BASE);
    out.push_str("\n## Available Hooks\n\n");

    if applicable_hooks.is_empty() {
        out.push_str(
            "_No hooks are currently available for this project. \
            The user can add hooks with `ralph hooks add`, or import a bundle from a \
            teammate with `ralph hooks import <file>`._\n",
        );
        return out;
    }

    out.push_str(
        "These hooks are in the user's library and apply to this project. Attach them by \
         name — do not invent new ones.\n\n",
    );

    for hook in applicable_hooks {
        let scope = match &hook.scope {
            Scope::Global => "global".to_string(),
            Scope::Paths { paths } => {
                let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!("paths: {}", list.join(", "))
            }
        };
        out.push_str(&format!(
            "- **{}** ({}, {})",
            hook.name, hook.lifecycle, scope
        ));
        if !hook.description.is_empty() {
            out.push_str(&format!(" — {}", hook.description));
        }
        out.push('\n');
    }
    out
}

/// Build the initial prompt for the plan-harness session.
fn build_initial_prompt(project: &str, description: Option<&str>) -> String {
    match description {
        Some(desc) => {
            format!("Create a ralph plan for the project at {project}. Description: {desc}")
        }
        None => format!("Help me create or update a ralph plan for the project at {project}."),
    }
}

/// Build harness arguments for interactive plan-harness mode.
///
/// The argv is built from the per-harness `plan_args` template in
/// [`HarnessConfig`]. Two placeholders are supported:
/// - `{prompt}` — replaced with the initial prompt. If the harness has no
///   external mechanism for loading the agent definition (neither a
///   `{agent_file}` CLI flag nor an `agent_file_env` env var), the agent
///   definition is prepended so it still reaches the model via the single
///   prompt turn.
/// - `{agent_file}` — replaced with the absolute path to the agent
///   definition tempfile. Only meaningful when `supports_agent_file` is true.
///
/// If a harness's `plan_args` is empty (legacy user configs that predate
/// this field), this falls back to the pre-template behavior: claude gets
/// `--system-prompt-file <path>` followed by the prompt, and every other
/// harness receives just the (possibly agent-prepended) prompt as a single
/// positional argument.
fn build_plan_harness_args(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
    agent_content: &str,
    prompt: &str,
) -> Vec<String> {
    let harness_config = &config.harnesses[harness_name];

    // Goose-style harnesses load the system prompt from an env var that
    // fully replaces the default, so inlining the agent content into the
    // prompt would duplicate it and eat the context window. Only prepend
    // the agent definition when the harness has no external loading path
    // at all.
    let has_external_agent_loading =
        harness_config.supports_agent_file || harness_config.agent_file_env.is_some();
    let effective_prompt = if !has_external_agent_loading && agent_file_path.is_some() {
        format!("{agent_content}\n\n---\n\n{prompt}")
    } else {
        prompt.to_string()
    };

    // Legacy fallback for user configs that predate plan_args.
    if harness_config.plan_args.is_empty() {
        let mut args = Vec::new();
        if harness_config.supports_agent_file
            && let Some(path) = agent_file_path
        {
            args.push("--system-prompt-file".to_string());
            args.push(path.to_string_lossy().to_string());
        }
        args.push(effective_prompt);
        return args;
    }

    // Template path: substitute {prompt} and {agent_file} in place using
    // substring replacement so tokens like "--prompt={prompt}" and
    // "--agent-file={agent_file}" work, matching build_harness_args semantics.
    //
    // Resolve `{agent_file}` BEFORE `{prompt}` so a prompt that happens
    // to contain the literal string `{agent_file}` (e.g. a plan
    // description discussing the placeholder system) cannot collide
    // with the no-agent removal pass — see harness::build_harness_args
    // for the matching fix.
    let mut args: Vec<String> = harness_config.plan_args.clone();

    if let Some(path) = agent_file_path {
        let agent_file_str = path.to_string_lossy();
        for arg in args.iter_mut() {
            *arg = arg.replace("{agent_file}", &agent_file_str);
        }
    } else {
        // Mirror build_harness_args's no-agent-file behavior: strip any
        // `{agent_file}` placeholder tokens and the preceding flag they go with.
        harness::remove_agent_file_args(&mut args);
    }

    // Now that `{agent_file}` is resolved, substitute `{prompt}` into
    // every arg position.
    for arg in args.iter_mut() {
        *arg = arg.replace("{prompt}", &effective_prompt);
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

    // Build the plan agent content, injecting the list of hooks applicable
    // to the current project so the harness can reference them by name.
    let project_path = std::path::Path::new(project);
    let hooks = hook_library::load_all().unwrap_or_default();
    let applicable = hook_library::filter_by_project(hooks, project_path);
    let agent_content = render_plan_agent(&applicable);

    // Write the agent definition to a temporary file.
    // This file lives for the duration of the harness process.
    let agent_temp_file = write_agent_temp_file(&agent_content)?;
    let agent_file_path = agent_temp_file.path();

    // Build the initial prompt
    let prompt = build_initial_prompt(project, description);

    // Build per-harness args and env
    let args = build_plan_harness_args(
        harness_name,
        config,
        Some(agent_file_path),
        &agent_content,
        &prompt,
    );
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

/// Write the given agent definition content to a temporary file. The filename
/// combines the process id with a uuid to prevent collisions between
/// concurrent callers in the same process (notably parallel tests).
fn write_agent_temp_file(content: &str) -> Result<TempAgentFile> {
    let file_name = format!(
        "ralph-rs-plan-agent-{}-{}.md",
        std::process::id(),
        uuid::Uuid::new_v4(),
    );
    let path = std::env::temp_dir().join(file_name);

    let mut file = std::fs::File::create(&path).with_context(|| {
        format!(
            "Failed to create temporary agent file at {}",
            path.display()
        )
    })?;
    file.write_all(content.as_bytes())
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
        assert!(prompt.starts_with("Create a ralph plan"));
    }

    #[test]
    fn test_build_initial_prompt_without_description() {
        let prompt = build_initial_prompt("/tmp/project", None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.starts_with("Help me create or update"));
    }

    fn test_agent_content() -> String {
        render_plan_agent(&[])
    }

    #[test]
    fn test_build_plan_harness_args_claude() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        );

        // Claude should get --system-prompt-file and the prompt as separate args
        assert!(args.contains(&"--system-prompt-file".to_string()));
        assert!(args.contains(&"Create a plan".to_string()));
        // Agent content should NOT be in the prompt
        assert!(!args.iter().any(|a| a.contains("ralph Plan Agent")));
        // {agent_file} placeholder should have been substituted with the real path
        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert!(args.contains(&path_str));
        assert!(!args.iter().any(|a| a.contains("{agent_file}")));
    }

    #[test]
    fn test_build_plan_harness_args_prompt_containing_agent_file_token() {
        // Regression test mirroring harness::build_harness_args: if the
        // prompt text contains the literal string `{agent_file}` (e.g. a
        // plan description discussing the placeholder system itself), the
        // no-agent removal pass must run BEFORE prompt substitution so
        // the preceding flag (here, claude's `--system-prompt-file` or a
        // harness's `-p` / `--prompt`) is not stripped along with the
        // prompt arg.
        let config = Config::default();
        let agent_content = test_agent_content();
        let prompt_with_placeholder =
            "Plan that discusses the {agent_file} placeholder collision bug.";

        // No agent file: hits the remove_agent_file_args branch for
        // any harness whose plan_args contain a `{agent_file}` token.
        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            None,
            &agent_content,
            prompt_with_placeholder,
        );
        // Claude's plan_args include `--permission-mode bypassPermissions`
        // which must survive unconditionally — and the prompt itself must
        // appear verbatim with its literal `{agent_file}` intact.
        assert!(
            claude_args.iter().any(|a| a == "--permission-mode"),
            "--permission-mode was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == "bypassPermissions"),
            "bypassPermissions was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == prompt_with_placeholder),
            "prompt was stripped or mangled; got args: {claude_args:?}"
        );
        // No residual `{agent_file}` token in args (outside the prompt).
        // Every arg equal to just "{agent_file}" on its own would indicate
        // a failed removal; the prompt-as-a-whole containing the substring
        // is fine and expected.
        assert!(
            !claude_args.iter().any(|a| a == "{agent_file}"),
            "raw {{agent_file}} token leaked through; got args: {claude_args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_args_copilot_uses_interactive_flag() {
        // The whole point of the plan_args template: copilot's run-mode -p
        // is one-shot and rejects a seeded positional prompt, so plan-harness
        // mode must invoke it with -i instead.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "copilot",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan this",
        );

        assert!(
            args.contains(&"-i".to_string()),
            "copilot plan_args must use -i, got: {args:?}"
        );
        assert!(
            !args.contains(&"-p".to_string()),
            "copilot plan_args must NOT use -p (one-shot, non-interactive): {args:?}"
        );
        assert!(args.contains(&"--allow-all".to_string()));
        assert!(args.contains(&"--allow-all-paths".to_string()));
        // The prompt (with prepended agent content) should still be present.
        assert!(args.iter().any(|a| a.contains("Plan this")));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
    }

    #[test]
    fn test_build_plan_harness_args_legacy_empty_template() {
        // A user config that predates plan_args ships an empty Vec. The
        // builder must fall back to the pre-template behavior (claude gets
        // --system-prompt-file + prompt; everything else gets the prepended
        // prompt as a bare positional).
        let mut config = Config::default();
        for harness in config.harnesses.values_mut() {
            harness.plan_args.clear();
        }
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();

        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        );
        assert_eq!(claude_args[0], "--system-prompt-file");
        assert_eq!(claude_args[1], agent_file.path().to_string_lossy());
        assert_eq!(claude_args[2], "Plan");
        assert_eq!(claude_args.len(), 3);

        let codex_args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        );
        assert_eq!(codex_args.len(), 1);
        assert!(codex_args[0].contains("ralph Plan Agent"));
        assert!(codex_args[0].contains("Plan"));
    }

    #[test]
    fn test_build_plan_harness_args_codex_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        );

        // Codex doesn't support agent files, so agent content should be prepended
        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Create a plan")));
    }

    #[test]
    fn test_build_plan_harness_args_pi_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "pi",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        );

        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Help me plan")));
    }

    #[test]
    fn test_build_plan_harness_args_goose_does_not_inline_agent() {
        // Goose loads the agent definition via GOOSE_SYSTEM_PROMPT_FILE_PATH,
        // which fully replaces the default system prompt. If we ALSO inline
        // the definition into the -t prompt (as the old behavior did), goose
        // sees it twice: once as system, once as user. Pin the fix so any
        // future change to the prepend logic has to re-justify itself.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "goose",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        );

        assert!(
            args.iter().any(|a| a.contains("Help me plan")),
            "user prompt missing from argv: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.contains("ralph Plan Agent")),
            "agent definition was inlined into goose argv (double-load bug): {args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_env_goose() {
        // Goose's default config sets `agent_file_env` to
        // GOOSE_SYSTEM_PROMPT_FILE_PATH, so an agent file should be exported
        // as that env var to the subprocess.
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("goose", &config, Some(agent_file.path()));

        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_SYSTEM_PROMPT_FILE_PATH");
        assert_eq!(env[0].1, agent_file.path().to_string_lossy());
    }

    #[test]
    fn test_build_plan_harness_env_goose_no_agent_file() {
        // With no agent file, nothing should be exported even if the env
        // var is configured.
        let config = Config::default();
        let env = build_plan_harness_env("goose", &config, None);
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_env_claude_no_env() {
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("claude", &config, Some(agent_file.path()));

        // Claude supports agent file natively, so env var should NOT be set
        assert!(env.is_empty());
    }

    #[test]
    fn test_write_agent_temp_file() {
        let content = test_agent_content();
        let agent_file = write_agent_temp_file(&content).unwrap();
        let read = std::fs::read_to_string(agent_file.path()).unwrap();
        assert!(read.contains("ralph Plan Agent"));
        assert!(read.contains("ralph plan create"));
        assert!(read.contains("ralph step add"));
    }

    #[test]
    fn test_harness_plan_agent_content() {
        // Verify the rendered agent definition has key sections.
        let content = render_plan_agent(&[]);
        assert!(content.contains("Plan Management"));
        assert!(content.contains("Step Management"));
        assert!(content.contains("Hook Attachment"));
        assert!(content.contains("Workflow"));
        assert!(content.contains("Guidelines"));
        assert!(content.contains("ralph plan create"));
        assert!(content.contains("ralph step add"));
        assert!(content.contains("ralph plan approve"));
        assert!(content.contains("ralph step set-hook"));
        assert!(content.contains("Available Hooks"));
    }

    #[test]
    fn test_render_plan_agent_lists_hooks() {
        use crate::hook_library::{Hook, Lifecycle, Scope};
        let hooks = vec![
            Hook {
                name: "claude-review".to_string(),
                description: "Review with Claude".to_string(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Global,
                command: "claude -p 'review'".to_string(),
            },
            Hook {
                name: "rust-clippy".to_string(),
                description: String::new(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Paths {
                    paths: vec![std::path::PathBuf::from("/home/me/rust")],
                },
                command: "cargo clippy".to_string(),
            },
        ];
        let content = render_plan_agent(&hooks);
        assert!(content.contains("**claude-review**"));
        assert!(content.contains("Review with Claude"));
        assert!(content.contains("**rust-clippy**"));
        assert!(content.contains("/home/me/rust"));
    }

    #[test]
    fn test_render_plan_agent_no_hooks_message() {
        let content = render_plan_agent(&[]);
        assert!(content.contains("No hooks are currently available"));
        assert!(content.contains("ralph hooks add"));
    }

    /// Synthetic harness config builder used by the substring-replacement
    /// tests below. `HarnessConfig` doesn't derive `Default`, so we spell out
    /// all fields here to keep the tests independent of `Config::default()`.
    fn synthetic_harness(plan_args: Vec<String>, supports_agent_file: bool) -> Config {
        use crate::config::HarnessConfig;
        use std::collections::HashMap;
        let mut harnesses = HashMap::new();
        harnesses.insert(
            "synth".to_string(),
            HarnessConfig {
                command: "synth".to_string(),
                args: vec![],
                plan_args,
                supports_agent_file,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
                agent_file_args: vec![],
                model_args: vec![],
                default_model: None,
            },
        );
        Config {
            default_harness: "synth".to_string(),
            max_retries_per_step: 0,
            timeout_secs: 0,
            harnesses,
        }
    }

    #[test]
    fn test_plan_harness_args_substring_replacement() {
        // A plan_args template that embeds {prompt} inside a larger token
        // (e.g. `--prompt={prompt}`) must be substring-substituted, not
        // matched as a whole token — otherwise users copying run-mode
        // patterns into plan_args get the literal placeholder in argv.
        let config = synthetic_harness(vec!["--prompt={prompt}".to_string()], false);
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        );

        assert_eq!(args.len(), 1, "expected a single arg, got {args:?}");
        // Because supports_agent_file=false, the agent content is prepended
        // to the effective prompt — so the arg begins with "--prompt=" and
        // contains both the agent content and the original prompt.
        assert!(args[0].starts_with("--prompt="));
        assert!(args[0].contains("Create a plan"));
        assert!(
            !args[0].contains("{prompt}"),
            "literal placeholder leaked into argv: {args:?}"
        );
    }

    #[test]
    fn test_plan_harness_args_agent_file_substring() {
        // Same story for {agent_file}: a token like `--agent-file={agent_file}`
        // must substring-substitute to the real path, not leak the literal.
        let config = synthetic_harness(
            vec![
                "--agent-file={agent_file}".to_string(),
                "{prompt}".to_string(),
            ],
            true,
        );
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        );

        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert_eq!(args.len(), 2, "expected two args, got {args:?}");
        assert!(args[0].starts_with("--agent-file="));
        assert!(
            args[0].contains(&path_str),
            "expected path substring in first arg: {args:?}"
        );
        assert!(
            !args[0].contains("{agent_file}"),
            "literal {{agent_file}} leaked into argv: {args:?}"
        );
        assert_eq!(args[1], "Create a plan");
    }
}
