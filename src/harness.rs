// Harness subprocess management

use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::config::{Config, HarnessConfig, PromptInputMode};
use crate::plan::{Plan, Step};

/// Placeholder token in harness args that gets replaced with the actual prompt.
const PROMPT_PLACEHOLDER: &str = "{prompt}";

/// Placeholder token in harness args for the agent file path.
const AGENT_FILE_PLACEHOLDER: &str = "{agent_file}";

/// Placeholder token in `model_args` for the selected model identifier.
const MODEL_PLACEHOLDER: &str = "{model}";

/// Size at which `PromptInputMode::Argv` silently promotes to `TempFile`.
///
/// Linux caps a single argv string at `MAX_ARG_STRLEN` = 128 KB and
/// `execve` returns `E2BIG` past that. Half the kernel limit leaves
/// headroom for the other argv elements (flags, paths, model args) that
/// share the argv block, and for a few KB of env that gets counted against
/// the same limit on some kernels. Any prompt above this threshold spills
/// to a tempfile with a warning — the user's step still runs, it just
/// hands the harness a file path instead of the raw text.
pub(crate) const ARGV_SPILL_THRESHOLD_BYTES: usize = 64 * 1024;

/// How the prompt will actually be delivered to a specific invocation.
///
/// Returned by [`resolve_prompt_delivery`] so the spawn path knows whether
/// to attach a stdin pipe, keep a tempfile alive for the child's lifetime,
/// or do nothing (prompt is already baked into argv).
#[derive(Debug)]
pub enum PromptDelivery {
    /// Prompt already lives in `args` — nothing extra to do at spawn time.
    Argv,
    /// Prompt is piped to the child's stdin after spawn. The bytes are the
    /// prompt text; the spawn path must write them and close stdin.
    Stdin(Vec<u8>),
    /// Prompt has been written to a temp file whose path is already
    /// substituted into `args`. The `NamedTempFile` must be held alive
    /// until the child exits (drop triggers cleanup).
    TempFile(NamedTempFile),
}

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
/// 4. Forwards the model override (or config default) via model_args
/// 5. Appends JSON output args if supported
///
/// Model precedence: `model_override` (e.g. from `Step.model`) takes
/// priority over `harness_config.default_model`. If both are `None` the
/// harness is invoked without any model flag.
///
/// Note: this always inlines the prompt into argv and is preserved for
/// tests that expect that behavior. Production code should prefer
/// [`prepare_harness_invocation`], which honors
/// [`HarnessConfig::prompt_input`] and picks the safe delivery mode.
#[allow(dead_code)]
pub fn build_harness_args(
    harness_name: &str,
    harness_config: &HarnessConfig,
    prompt: &str,
    agent_file: Option<&Path>,
    model_override: Option<&str>,
) -> Vec<String> {
    let mut args = harness_config.args.clone();

    // Resolve `{agent_file}` placeholders BEFORE substituting `{prompt}`.
    // Otherwise, a prompt whose text contains the literal string
    // `{agent_file}` (e.g. a step description discussing the placeholder
    // system) would be scanned by `remove_agent_file_args` after
    // substitution, causing the prompt arg AND its preceding flag
    // (typically `-p`) to be stripped entirely.
    if let Some(agent_path) = agent_file {
        let agent_path_str = agent_path.to_string_lossy().to_string();
        inject_agent_file(harness_name, harness_config, &mut args, &agent_path_str);
    } else {
        // No agent file: strip any `{agent_file}` placeholder tokens and
        // the preceding flag they go with.
        remove_agent_file_args(&mut args);
    }

    // Replace {prompt} placeholders or append prompt at end. This must
    // happen AFTER agent-file resolution so the prompt text is opaque
    // to both passes.
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

    append_model_and_json_args(&mut args, harness_config, model_override);
    args
}

/// Append the optional model flag and JSON-output flags to an already-
/// assembled args vec. Factored out so both the legacy argv path and the
/// new [`prepare_harness_invocation`] resolver share the same tail logic.
fn append_model_and_json_args(
    args: &mut Vec<String>,
    harness_config: &HarnessConfig,
    model_override: Option<&str>,
) {
    // Forward an optional model selection via the harness's model_args
    // template. Precedence: explicit override (e.g. `Step.model`) first,
    // then the harness's config-level `default_model`. If both are None,
    // or if the harness has no model_args template, the model flag is
    // omitted entirely.
    let resolved_model = model_override.or(harness_config.default_model.as_deref());
    if let Some(model) = resolved_model
        && !harness_config.model_args.is_empty()
    {
        for arg in &harness_config.model_args {
            args.push(arg.replace(MODEL_PLACEHOLDER, model));
        }
    }

    // Append JSON output args if supported
    if harness_config.supports_json_output {
        args.extend(harness_config.json_output_args.clone());
    }
}

/// Prepare a harness invocation, honoring [`HarnessConfig::prompt_input`].
///
/// Returns the argv the spawn path should hand to `Command::args` plus a
/// [`PromptDelivery`] describing what side-channel (stdin pipe or temp
/// file) the spawn path must set up.
///
/// Behavior by mode:
/// - [`PromptInputMode::Stdin`]: the `{prompt}` placeholder is stripped
///   from `args` (and any preceding flag that looks dangling is *not*
///   removed — the harness's config is responsible for ensuring its
///   stdin-mode args are self-consistent, e.g. `claude -p -`). The
///   prompt bytes are returned in [`PromptDelivery::Stdin`] for the
///   spawn path to write to the child's stdin.
/// - [`PromptInputMode::TempFile`]: the prompt is written to a named
///   temp file; the file's path is substituted for `{prompt}` in
///   `args`. The tempfile handle is returned to hold it alive until
///   the child exits.
/// - [`PromptInputMode::Argv`]: same as [`build_harness_args`] unless
///   the prompt exceeds [`ARGV_SPILL_THRESHOLD_BYTES`], in which case
///   the invocation is transparently promoted to TempFile mode and a
///   `warn!`-style line is emitted on stderr.
pub fn prepare_harness_invocation(
    harness_name: &str,
    harness_config: &HarnessConfig,
    prompt: &str,
    agent_file: Option<&Path>,
    model_override: Option<&str>,
) -> Result<(Vec<String>, PromptDelivery)> {
    // Start with the raw args template and resolve agent-file placeholders
    // first — same ordering rationale as build_harness_args (so a prompt
    // whose text contains `{agent_file}` doesn't confuse the removal pass).
    let mut args = harness_config.args.clone();
    if let Some(agent_path) = agent_file {
        let agent_path_str = agent_path.to_string_lossy().to_string();
        inject_agent_file(harness_name, harness_config, &mut args, &agent_path_str);
    } else {
        remove_agent_file_args(&mut args);
    }

    // Decide the effective delivery mode. Argv auto-spills to TempFile
    // past the threshold so a retry-context-bloated prompt doesn't trip
    // E2BIG on the kernel.
    let mode = match harness_config.prompt_input {
        PromptInputMode::Argv if prompt.len() > ARGV_SPILL_THRESHOLD_BYTES => {
            eprintln!(
                "ralph: warning: prompt is {} bytes (>{} KB threshold); \
                 spilling to temp file to avoid E2BIG on argv-mode harness '{}'.",
                prompt.len(),
                ARGV_SPILL_THRESHOLD_BYTES / 1024,
                harness_name,
            );
            PromptInputMode::TempFile
        }
        other => other,
    };

    let delivery = match mode {
        PromptInputMode::Stdin => {
            // Strip any `{prompt}` placeholder tokens from args — in stdin
            // mode the harness reads its prompt from its stdin pipe and
            // the placeholder is dead config. If no placeholder is
            // present we leave args alone (the harness template already
            // assumes stdin, e.g. opencode `run` with no positional).
            args.retain(|a| !a.contains(PROMPT_PLACEHOLDER));
            PromptDelivery::Stdin(prompt.as_bytes().to_vec())
        }
        PromptInputMode::TempFile => {
            // Materialize the prompt to a NamedTempFile, then substitute
            // its path into args. If no `{prompt}` placeholder is
            // present, append the path as the trailing positional
            // (same convention as argv mode's append-when-no-placeholder
            // behavior).
            let mut tmp = NamedTempFile::new().context("failed to create prompt temp file")?;
            tmp.write_all(prompt.as_bytes())
                .context("failed to write prompt to temp file")?;
            tmp.flush().context("failed to flush prompt temp file")?;
            let path_str = tmp.path().to_string_lossy().to_string();
            let has_placeholder = args.iter().any(|a| a.contains(PROMPT_PLACEHOLDER));
            if has_placeholder {
                for a in args.iter_mut() {
                    if a.contains(PROMPT_PLACEHOLDER) {
                        *a = a.replace(PROMPT_PLACEHOLDER, &path_str);
                    }
                }
            } else {
                args.push(path_str);
            }
            PromptDelivery::TempFile(tmp)
        }
        PromptInputMode::Argv => {
            // Classic inline-into-argv path — unchanged semantics.
            let has_prompt_placeholder = args.iter().any(|a| a.contains(PROMPT_PLACEHOLDER));
            if has_prompt_placeholder {
                for a in args.iter_mut() {
                    if a.contains(PROMPT_PLACEHOLDER) {
                        *a = a.replace(PROMPT_PLACEHOLDER, prompt);
                    }
                }
            } else {
                args.push(prompt.to_string());
            }
            PromptDelivery::Argv
        }
    };

    append_model_and_json_args(&mut args, harness_config, model_override);

    Ok((args, delivery))
}

/// Inject the agent file path into the args based on harness type.
///
/// - **claude**: Uses native `--system-prompt-file` flag via `agent_file_args`
///   (supports_agent_file = true)
/// - **codex, pi, opencode, copilot**: Prompt carries a `ralph agents show`
///   pointer (see prompt.rs) — this function is a no-op for them.
/// - **goose**: Uses environment variable (handled via agent_file_env in
///   `build_harness_env`, not here)
fn inject_agent_file(
    _harness_name: &str,
    harness_config: &HarnessConfig,
    args: &mut Vec<String>,
    agent_path: &str,
) {
    if !harness_config.supports_agent_file {
        return;
    }

    // If the harness's args already contain an `{agent_file}` placeholder
    // (e.g. a user custom config embedding the flag inline), substitute
    // in place. Otherwise, apply the `agent_file_args` template.
    let has_inline_placeholder = args.iter().any(|a| a.contains(AGENT_FILE_PLACEHOLDER));
    if has_inline_placeholder {
        *args = args
            .iter()
            .map(|a| a.replace(AGENT_FILE_PLACEHOLDER, agent_path))
            .collect();
        return;
    }

    for arg in &harness_config.agent_file_args {
        args.push(arg.replace(AGENT_FILE_PLACEHOLDER, agent_path));
    }
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
///
/// On unix, the child is placed in a new process group with itself as leader
/// (`setpgid(0, 0)` via `Command::process_group(0)`). This lets ralph kill the
/// *entire* descendant tree on shutdown by signalling the negative pgid — so
/// grandchildren like `pnpm -> turbo -> next` don't survive as orphans when
/// the top-level harness exits. The tradeoff: a child in its own process
/// group no longer receives terminal-driven SIGINT automatically, so ralph
/// must forward it explicitly. `graceful_shutdown` in `executor.rs` already
/// does this (it sends SIGTERM to the process group) so the two pieces are
/// designed to work together.
///
/// Note the asymmetry with `spawn_harness_interactive` below: the planner
/// inherits stdio and expects terminal-driven Ctrl+C to pass straight to the
/// child, so it is intentionally left in ralph's own process group.
#[allow(dead_code)]
pub async fn spawn_harness(
    harness_config: &HarnessConfig,
    args: &[String],
    env_vars: &[(String, String)],
    cwd: &Path,
) -> Result<tokio::process::Child> {
    let (child, _tempfile) =
        spawn_harness_with_delivery(harness_config, args, env_vars, cwd, PromptDelivery::Argv)
            .await?;
    Ok(child)
}

/// Spawn a harness with an explicit [`PromptDelivery`] side-channel.
///
/// - [`PromptDelivery::Argv`]: stdin is `null`, nothing extra to do.
/// - [`PromptDelivery::Stdin`]: stdin is piped; after spawn we write the
///   prompt bytes and drop the stdin handle (which sends EOF to the
///   child). Failure to take stdin is surfaced as a hard error.
/// - [`PromptDelivery::TempFile`]: stdin is `null` — the prompt is in a
///   file whose path is already in `args`. The `NamedTempFile` is
///   returned to the caller, who must keep it alive until the child
///   exits (drop removes the file). Callers that don't care can let it
///   drop immediately by binding `_`.
pub async fn spawn_harness_with_delivery(
    harness_config: &HarnessConfig,
    args: &[String],
    env_vars: &[(String, String)],
    cwd: &Path,
    delivery: PromptDelivery,
) -> Result<(tokio::process::Child, Option<NamedTempFile>)> {
    let mut cmd = Command::new(&harness_config.command);
    cmd.args(args)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // Attach a stdin pipe only when we have prompt bytes to send;
    // otherwise close it so the child doesn't block on an empty TTY read.
    match &delivery {
        PromptDelivery::Stdin(_) => {
            cmd.stdin(std::process::Stdio::piped());
        }
        PromptDelivery::Argv | PromptDelivery::TempFile(_) => {
            cmd.stdin(std::process::Stdio::null());
        }
    }

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    // Put the child into its own process group so we can fan signals out to
    // grandchildren on shutdown. See the doc comment above for the full
    // rationale and the implication for SIGINT forwarding.
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn harness '{}'", harness_config.command))?;

    let (stdin_bytes, tempfile) = match delivery {
        PromptDelivery::Stdin(bytes) => (Some(bytes), None),
        PromptDelivery::TempFile(tmp) => (None, Some(tmp)),
        PromptDelivery::Argv => (None, None),
    };

    if let Some(bytes) = stdin_bytes {
        let mut stdin = child
            .stdin
            .take()
            .context("child process did not expose a stdin handle")?;
        // Tolerate BrokenPipe: harnesses that don't actually read stdin
        // (or exit early) close the pipe before we finish writing. Those
        // are legitimate scenarios — the child got enough to start work
        // (or intentionally ignored us) and must not be turned into a
        // step failure here. Any other IO error is a real problem (e.g.
        // ENOSPC) and bubbles up.
        if let Err(e) = stdin.write_all(&bytes).await
            && e.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(anyhow::Error::new(e).context("failed to write prompt to child stdin"));
        }
        if let Err(e) = stdin.shutdown().await
            && e.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(
                anyhow::Error::new(e).context("failed to close child stdin after writing prompt")
            );
        }
        drop(stdin);
    }

    Ok((child, tempfile))
}

/// Spawn a harness process in interactive mode with inherited stdio.
///
/// Used for `plan:harness` mode where the user interacts directly with the harness.
///
/// Unlike [`spawn_harness`], this deliberately does **not** move the child
/// into its own process group: the planner inherits stdio and relies on the
/// controlling terminal to forward SIGINT directly (Ctrl+C during planning
/// should behave like Ctrl+C in any other terminal program). Placing it in a
/// separate group would intercept that UX.
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
#[allow(dead_code)]
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
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            prompt_prefix: None,
            prompt_suffix: None,
            context_prepend: None,
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
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
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
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let args = build_harness_args("test", &hc, "do the thing", None, None);
        assert_eq!(args, vec!["-p", "do the thing"]);
    }

    #[test]
    fn test_build_harness_args_appends_prompt_when_no_placeholder() {
        let hc = HarnessConfig {
            command: "codex".to_string(),
            args: vec![],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let args = build_harness_args("codex", &hc, "implement feature", None, None);
        assert_eq!(args, vec!["implement feature"]);
    }

    #[test]
    fn test_build_harness_args_appends_json_output() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let args = build_harness_args("claude", hc, "do stuff", None, None);
        // Should contain -p, prompt, and JSON output args
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"json".to_string()));
    }

    #[test]
    fn test_build_harness_args_no_json_when_unsupported() {
        // All default harnesses now support JSON, so construct a synthetic
        // harness with `supports_json_output: false` to exercise the "no
        // JSON appended" branch in isolation.
        let hc = HarnessConfig {
            command: "fake".to_string(),
            args: vec!["--run".to_string(), "{prompt}".to_string()],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec!["--this-should-not-appear".to_string()],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let args = build_harness_args("fake", &hc, "do stuff", None, None);
        assert_eq!(args, vec!["--run".to_string(), "do stuff".to_string()]);
    }

    #[test]
    fn test_build_harness_args_pi_uses_mode_json() {
        let config = Config::default();
        let hc = &config.harnesses["pi"];

        let args = build_harness_args("pi", hc, "do stuff", None, None);
        // pi -p "do stuff" --mode json
        assert_eq!(
            args,
            vec!["-p", "do stuff", "--mode", "json"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_build_harness_args_opencode_uses_run_subcommand() {
        let config = Config::default();
        let hc = &config.harnesses["opencode"];

        let args = build_harness_args("opencode", hc, "do stuff", None, None);
        // opencode run "do stuff" --format json
        assert_eq!(
            args,
            vec!["run", "do stuff", "--format", "json"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_build_harness_args_claude_agent_file() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];
        let agent_path = Path::new("/home/user/.ralph2/agents/default.md");

        let args = build_harness_args("claude", hc, "do stuff", Some(agent_path), None);
        // Claude's real flag is --system-prompt-file (not --agent-file),
        // forwarded via the agent_file_args template.
        assert!(
            args.windows(2).any(|w| w[0] == "--system-prompt-file"
                && w[1] == "/home/user/.ralph2/agents/default.md"),
            "expected --system-prompt-file <path> pair in args, got: {args:?}"
        );
    }

    #[test]
    fn test_build_harness_args_no_agent_file_no_flag() {
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let args = build_harness_args("claude", hc, "do stuff", None, None);
        assert!(!args.contains(&"--system-prompt-file".to_string()));
    }

    #[test]
    fn test_build_harness_args_no_model_args_when_neither_set() {
        // Baseline for the new model precedence logic: when neither the
        // per-step override nor the harness's default_model is set, no
        // model flag should appear in the final args. Pi is a good fit
        // here because its real config has a real model_args template
        // (["--model", "{model}"]), so "no flag" must come from the
        // precedence check, not from an empty template.
        let config = Config::default();
        let hc = &config.harnesses["pi"];
        assert!(hc.default_model.is_none());
        assert!(!hc.model_args.is_empty());

        let args = build_harness_args("pi", hc, "q", None, None);
        assert!(
            !args.contains(&"--model".to_string()),
            "expected no --model flag when both override and default are None, got: {args:?}"
        );
    }

    #[test]
    fn test_build_harness_args_uses_default_model_when_no_override() {
        // default_model on the harness config should be forwarded when
        // the caller doesn't pass a per-step override.
        let mut hc = Config::default().harnesses["pi"].clone();
        hc.default_model = Some("sonnet-4.6".to_string());

        let args = build_harness_args("pi", &hc, "q", None, None);
        // pi's model_args is ["--model", "{model}"], so we expect the
        // substituted pair to land at the end of the args.
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--model" && w[1] == "sonnet-4.6")
        );
    }

    #[test]
    fn test_build_harness_args_override_beats_default_model() {
        // Per-step override must take priority over the harness default.
        let mut hc = Config::default().harnesses["pi"].clone();
        hc.default_model = Some("sonnet-4.6".to_string());

        let args = build_harness_args("pi", &hc, "q", None, Some("opus-4.6"));
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--model" && w[1] == "opus-4.6")
        );
        assert!(
            !args.iter().any(|a| a == "sonnet-4.6"),
            "default_model should have been overridden, got: {args:?}"
        );
    }

    #[test]
    fn test_build_harness_args_override_ignored_when_no_model_args() {
        // If the resolved harness has no `model_args` template, any
        // override is silently dropped — the harness literally has no
        // flag to forward it through. (Mirrors kctx-local's semantics.)
        let mut hc = Config::default().harnesses["pi"].clone();
        hc.model_args.clear();

        let args = build_harness_args("pi", &hc, "q", None, Some("opus-4.6"));
        assert!(
            !args.iter().any(|a| a == "opus-4.6"),
            "override should be silently ignored when model_args is empty, got: {args:?}"
        );
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
    fn test_build_harness_args_prompt_containing_agent_file_token_no_agent() {
        // Regression test for the `{agent_file}` / `{prompt}` collision:
        // if a prompt text happens to contain the literal string
        // `{agent_file}` (e.g. a step description discussing the
        // placeholder system itself), the prompt substitution must run
        // AFTER `remove_agent_file_args` so the removal pass does not
        // see the placeholder inside the substituted prompt and strip
        // the preceding `-p` flag along with it.
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let prompt_with_placeholder =
            "Fix the bug where {agent_file} collides with prompt substitution.";

        // No agent file — this is the path that invokes remove_agent_file_args.
        let args = build_harness_args("claude", hc, prompt_with_placeholder, None, None);

        // The `-p` flag and its surrounding default args must survive.
        assert!(
            args.iter().any(|a| a == "-p"),
            "-p flag was stripped; got args: {args:?}"
        );
        assert!(
            args.iter().any(|a| a == "--permission-mode"),
            "--permission-mode flag was stripped; got args: {args:?}"
        );
        // The prompt text must appear verbatim as an arg (including the
        // literal `{agent_file}` token — it is opaque user content, not
        // a placeholder for this harness to interpret).
        assert!(
            args.iter().any(|a| a == prompt_with_placeholder),
            "prompt was stripped or mangled; got args: {args:?}"
        );
    }

    #[test]
    fn test_build_harness_args_prompt_containing_agent_file_token_with_agent() {
        // Companion case: when an agent file IS provided, the prompt's
        // literal `{agent_file}` token must likewise be preserved — it's
        // not a placeholder for the prompt text to be interpolated into.
        let config = Config::default();
        let hc = &config.harnesses["claude"];

        let prompt_with_placeholder = "Step talks about {agent_file} placeholder.";
        let agent_path = Path::new("/tmp/agent.md");

        let args = build_harness_args(
            "claude",
            hc,
            prompt_with_placeholder,
            Some(agent_path),
            None,
        );

        // Agent file flag was injected…
        assert!(
            args.iter().any(|a| a == "--system-prompt-file"),
            "agent file flag missing; got args: {args:?}"
        );
        assert!(
            args.iter().any(|a| a == "/tmp/agent.md"),
            "agent path missing; got args: {args:?}"
        );
        // …and the prompt is present verbatim (with `{agent_file}` intact).
        assert!(
            args.iter().any(|a| a == prompt_with_placeholder),
            "prompt was mangled; got args: {args:?}"
        );
    }

    #[test]
    fn test_build_harness_env_goose() {
        let hc = HarnessConfig {
            command: "goose".to_string(),
            args: vec![],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: Some("GOOSE_SYSTEM_PROMPT_FILE_PATH".to_string()),
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let agent_path = Path::new("/home/user/.ralph2/agents/default.md");
        let env = build_harness_env(&hc, Some(agent_path));
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_SYSTEM_PROMPT_FILE_PATH");
        assert_eq!(env[0].1, "/home/user/.ralph2/agents/default.md");
    }

    #[test]
    fn test_build_harness_env_no_agent() {
        let hc = HarnessConfig {
            command: "goose".to_string(),
            args: vec![],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: Some("GOOSE_SYSTEM_PROMPT_FILE_PATH".to_string()),
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
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
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let args = build_harness_args("test", &hc, "hello world", None, None);
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
            plan_args: vec![],
            supports_agent_file: true,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: crate::config::PromptInputMode::Stdin,
            color: None,
        };

        let agent_path = Path::new("/tmp/agent.md");
        let args = build_harness_args("claude", &hc, "do stuff", Some(agent_path), None);
        assert!(args.contains(&"/tmp/agent.md".to_string()));
        assert!(!args.iter().any(|a| a.contains("{agent_file}")));
    }

    // -----------------------------------------------------------------
    // PromptInputMode / prepare_harness_invocation
    // -----------------------------------------------------------------

    fn hc_with_mode(mode: PromptInputMode, args: Vec<&str>) -> HarnessConfig {
        HarnessConfig {
            command: "test".to_string(),
            args: args.into_iter().map(String::from).collect(),
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: mode,
            color: None,
        }
    }

    #[test]
    fn test_prompt_input_mode_default_is_stdin() {
        // The `#[serde(default)]` annotation on HarnessConfig::prompt_input
        // must land on Stdin so older configs (which predate the field)
        // keep the safe, E2BIG-proof behavior by default.
        let mode = PromptInputMode::default();
        assert_eq!(mode, PromptInputMode::Stdin);

        // Also verify the field itself defaults to Stdin when a
        // HarnessConfig is deserialized without the key.
        let json = r#"{"command": "test"}"#;
        let hc: HarnessConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(hc.prompt_input, PromptInputMode::Stdin);
    }

    #[test]
    fn test_stdin_mode_strips_prompt_placeholder_and_returns_bytes() {
        // Stdin mode should NOT leave the prompt text in argv — it strips
        // the `{prompt}` placeholder and ships the bytes via the returned
        // PromptDelivery for the spawn path to pipe in.
        let hc = hc_with_mode(PromptInputMode::Stdin, vec!["-p", "-", "{prompt}"]);
        let (args, delivery) =
            prepare_harness_invocation("test", &hc, "hello world", None, None).unwrap();
        // placeholder gone
        assert!(!args.iter().any(|a| a.contains("{prompt}")));
        // raw prompt text NOT in argv
        assert!(!args.iter().any(|a| a == "hello world"));
        // surrounding flags preserved
        assert_eq!(args, vec!["-p".to_string(), "-".to_string()]);
        match delivery {
            PromptDelivery::Stdin(bytes) => assert_eq!(bytes, b"hello world"),
            other => panic!("expected Stdin delivery, got {other:?}"),
        }
    }

    #[test]
    fn test_argv_mode_under_threshold_uses_argv() {
        // Small prompts on an Argv-mode harness should stay inlined.
        let hc = hc_with_mode(PromptInputMode::Argv, vec!["-p", "{prompt}"]);
        let (args, delivery) =
            prepare_harness_invocation("test", &hc, "short prompt", None, None).unwrap();
        assert_eq!(args, vec!["-p".to_string(), "short prompt".to_string()]);
        assert!(matches!(delivery, PromptDelivery::Argv));
    }

    #[test]
    fn test_argv_mode_auto_spills_to_tempfile_above_threshold() {
        // A 100 KB prompt exceeds ARGV_SPILL_THRESHOLD_BYTES (64 KB), so
        // the invocation must silently promote to TempFile: the `{prompt}`
        // placeholder gets substituted with a file path (NOT the prompt
        // text) and a NamedTempFile is returned to be held alive.
        let hc = hc_with_mode(PromptInputMode::Argv, vec!["-p", "{prompt}"]);
        let big_prompt = "x".repeat(100 * 1024);
        let (args, delivery) =
            prepare_harness_invocation("test", &hc, &big_prompt, None, None).unwrap();

        // The huge prompt text must NOT appear in argv.
        assert!(
            !args.iter().any(|a| a.len() > ARGV_SPILL_THRESHOLD_BYTES),
            "large prompt leaked into argv: {args:?}"
        );
        // Argv should now hold a temp file path in the position that
        // used to carry `{prompt}`.
        let tmp = match delivery {
            PromptDelivery::TempFile(t) => t,
            other => panic!("expected spill to TempFile delivery, got {other:?}"),
        };
        let tmp_path_str = tmp.path().to_string_lossy().to_string();
        assert_eq!(args, vec!["-p".to_string(), tmp_path_str.clone()]);

        // Temp file contents must match the original prompt byte-for-byte.
        let contents = std::fs::read_to_string(tmp.path()).expect("read tempfile");
        assert_eq!(contents.len(), big_prompt.len());
        assert_eq!(contents, big_prompt);
    }

    #[test]
    fn test_tempfile_mode_writes_and_passes_path() {
        // Explicit TempFile mode (e.g. copilot) should behave the same
        // as the Argv spill path: `{prompt}` is replaced by the temp
        // file path and the contents match.
        let hc = hc_with_mode(
            PromptInputMode::TempFile,
            vec!["-p", "{prompt}", "--silent"],
        );
        let prompt = "prompt body for copilot";
        let (args, delivery) =
            prepare_harness_invocation("copilot", &hc, prompt, None, None).unwrap();

        assert!(!args.iter().any(|a| a.contains("{prompt}")));
        assert!(!args.iter().any(|a| a == prompt));
        assert_eq!(args[0], "-p");
        assert_eq!(args[2], "--silent");

        let tmp = match delivery {
            PromptDelivery::TempFile(t) => t,
            other => panic!("expected TempFile delivery, got {other:?}"),
        };
        assert_eq!(args[1], tmp.path().to_string_lossy());

        let contents = std::fs::read_to_string(tmp.path()).expect("read tempfile");
        assert_eq!(contents, prompt);
    }

    #[tokio::test]
    async fn test_stdin_mode_writes_prompt_to_stdin_and_closes() {
        // End-to-end: spawn `sh -c 'cat > $OUT'` with Stdin delivery and
        // verify the prompt lands in the target file byte-for-byte. Proves
        // both (a) stdin piping works and (b) closing stdin after the write
        // gets the child to see EOF and exit.
        let tmp_out = tempfile::NamedTempFile::new().unwrap();
        let out_path = tmp_out.path().to_path_buf();
        // Close the file handle so the child can freely overwrite it;
        // the NamedTempFile will clean up on drop via path.
        drop(tmp_out);

        let hc = HarnessConfig {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), format!("cat > {}", out_path.display())],
            plan_args: vec![],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
            agent_file_args: vec![],
            model_args: vec![],
            default_model: None,
            auth_env_vars: vec![],
            auth_probe_args: vec![],
            prompt_input: PromptInputMode::Stdin,
            color: None,
        };
        let prompt_bytes = b"line one\nline two\n".to_vec();
        let delivery = PromptDelivery::Stdin(prompt_bytes.clone());

        let cwd = std::env::temp_dir();
        let (child, _tmp) = spawn_harness_with_delivery(&hc, &hc.args, &[], &cwd, delivery)
            .await
            .unwrap();

        let output = child.wait_with_output().await.unwrap();
        assert!(
            output.status.success(),
            "sh exited non-zero: stderr={:?}",
            String::from_utf8_lossy(&output.stderr)
        );

        let written = std::fs::read(&out_path).expect("read output");
        assert_eq!(written, prompt_bytes);
        // Best-effort cleanup — ignore if the child already removed it.
        let _ = std::fs::remove_file(&out_path);
    }
}
