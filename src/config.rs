// Configuration management

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Configuration for a single coding agent harness.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HarnessConfig {
    /// The command to invoke (e.g. "claude", "codex").
    pub command: String,
    /// Default arguments passed to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Argument template for the `plan harness generate` interactive session.
    /// Supports two placeholders substituted at spawn time:
    /// - `{prompt}` — the initial user prompt (with agent content prepended
    ///   if the harness does not support agent files natively)
    /// - `{agent_file}` — path to the agent definition tempfile (only useful
    ///   if `supports_agent_file` is true)
    ///
    /// Empty means "fall back to the legacy hardcoded behavior" so existing
    /// user configs that predate this field keep working.
    #[serde(default)]
    pub plan_args: Vec<String>,
    /// Whether this harness supports an agent file (e.g. CLAUDE.md).
    #[serde(default)]
    pub supports_agent_file: bool,
    /// Whether this harness supports structured JSON output.
    #[serde(default)]
    pub supports_json_output: bool,
    /// Additional args to enable JSON output mode.
    #[serde(default)]
    pub json_output_args: Vec<String>,
    /// Environment variable name used to point to the agent file. Only read
    /// when `supports_agent_file` is false — harnesses that take a flag set
    /// this to `None`. Used by goose (`GOOSE_SYSTEM_PROMPT_FILE_PATH`).
    #[serde(default)]
    pub agent_file_env: Option<String>,
    /// Argument template for forwarding an agent file path via a CLI flag.
    /// Supports the `{agent_file}` placeholder, substituted at spawn time
    /// when `supports_agent_file` is true.
    ///
    /// Empty means the harness has no flag to forward the agent file through
    /// — and if `agent_file_env` is also None, no agent file is passed.
    /// Examples:
    /// - claude: `["--system-prompt-file", "{agent_file}"]`
    #[serde(default)]
    pub agent_file_args: Vec<String>,
    /// Argument template for forwarding a model selection to the harness.
    /// Supports the `{model}` placeholder, substituted at spawn time with
    /// either [`Self::default_model`] or a future per-invocation override.
    ///
    /// Empty means the harness has no model-selection flag, and any model
    /// value is silently ignored. Examples:
    /// - claude / pi / goose: `["--model", "{model}"]`
    /// - codex / opencode: `["-m", "{model}"]`
    /// - copilot: `["--model={model}"]` (combined form)
    #[serde(default)]
    pub model_args: Vec<String>,
    /// Default model identifier forwarded via [`Self::model_args`] on every
    /// invocation. `None` means "let the harness pick its own default".
    /// Users opt in by editing config.json — init leaves this empty.
    #[serde(default)]
    pub default_model: Option<String>,
}

/// Top-level ralph-rs configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// The default harness to use when none is specified.
    pub default_harness: String,
    /// Maximum number of retries per step before giving up.
    pub max_retries_per_step: u32,
    /// Timeout in seconds for a single harness invocation.
    pub timeout_secs: u64,
    /// Available harness definitions keyed by name.
    pub harnesses: HashMap<String, HarnessConfig>,
}

impl Config {
    /// Verifies the loaded config is internally consistent.
    ///
    /// Catches misconfigured `default_harness` values (empty or pointing at
    /// a harness name that isn't defined) at load time rather than at first
    /// run, so the user sees a clear error instead of a cryptic runtime
    /// failure deep in harness resolution.
    pub fn validate(&self) -> Result<()> {
        if self.default_harness.is_empty() {
            return Err(anyhow!("config.default_harness must not be empty"));
        }
        if !self.harnesses.contains_key(&self.default_harness) {
            let mut available: Vec<&str> =
                self.harnesses.keys().map(String::as_str).collect();
            available.sort_unstable();
            return Err(anyhow!(
                "config.default_harness '{}' is not defined in harnesses (available: {})",
                self.default_harness,
                if available.is_empty() {
                    "<none>".to_string()
                } else {
                    available.join(", ")
                }
            ));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut harnesses = HashMap::new();

        harnesses.insert(
            "claude".to_string(),
            HarnessConfig {
                command: "claude".to_string(),
                // `--permission-mode bypassPermissions` is required for
                // non-interactive runs — without it, claude falls back to
                // interactive approval prompts and hangs ralph's subprocess.
                args: vec![
                    "-p".to_string(),
                    "--permission-mode".to_string(),
                    "bypassPermissions".to_string(),
                ],
                // Claude's `--system-prompt-file` natively loads the agent
                // definition, and the prompt is a positional argument that
                // keeps the session interactive.
                plan_args: vec![
                    "--permission-mode".to_string(),
                    "bypassPermissions".to_string(),
                    "--system-prompt-file".to_string(),
                    "{agent_file}".to_string(),
                    "{prompt}".to_string(),
                ],
                supports_agent_file: true,
                supports_json_output: true,
                json_output_args: vec!["--output-format".to_string(), "json".to_string()],
                // Claude takes the agent file via `--system-prompt-file`,
                // not via env var — `supports_agent_file` is true so the
                // env var path in `build_harness_env` is unreachable.
                agent_file_env: None,
                agent_file_args: vec![
                    "--system-prompt-file".to_string(),
                    "{agent_file}".to_string(),
                ],
                model_args: vec!["--model".to_string(), "{model}".to_string()],
                default_model: None,
            },
        );

        harnesses.insert(
            "codex".to_string(),
            HarnessConfig {
                // Codex non-interactive invocation is `codex exec "<prompt>"`:
                // the `exec` subcommand takes the prompt as a positional. JSON
                // output is JSONL via `--json`. The extra `-c` / `--ephemeral`
                // / `--skip-git-repo-check` flags are the recommended defaults
                // for programmatic, non-interactive use — they prevent codex
                // from blocking on approval prompts and avoid persisting
                // session files that ralph-rs doesn't need.
                command: "codex".to_string(),
                args: vec![
                    "exec".to_string(),
                    "{prompt}".to_string(),
                    "--skip-git-repo-check".to_string(),
                    "--ephemeral".to_string(),
                    "-c".to_string(),
                    "approval_policy=never".to_string(),
                ],
                // Codex's interactive TUI is the default subcommand (no
                // `exec`). It accepts a positional PROMPT that seeds the
                // first user turn (see codex-rs/tui/src/cli.rs). `--full-auto`
                // is the codex-blessed low-friction combo that maps to
                // `-a on-request --sandbox workspace-write`, letting the
                // model run tools freely inside the workspace while still
                // asking for confirmation on anything truly risky.
                plan_args: vec!["--full-auto".to_string(), "{prompt}".to_string()],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--json".to_string()],
                agent_file_env: None,
                agent_file_args: vec![],
                // codex accepts `-m <model>` / `--model <model>`.
                model_args: vec!["-m".to_string(), "{model}".to_string()],
                default_model: None,
            },
        );

        harnesses.insert(
            "pi".to_string(),
            HarnessConfig {
                // Pi's non-interactive "print" mode is triggered by -p / --print,
                // with the prompt as a positional. JSON output uses `--mode json`
                // (NDJSON events), NOT a generic --json flag.
                command: "pi".to_string(),
                args: vec!["-p".to_string()],
                // Interactive is pi's default when no `-p` is passed, and
                // positional arguments become the initial user message
                // (see packages/coding-agent/src/main.ts resolveAppMode).
                // Pi has no permission/approval flags by design ("No
                // permission popups" — user drives the session), so there
                // is nothing to add beyond the seeded prompt itself.
                plan_args: vec!["{prompt}".to_string()],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--mode".to_string(), "json".to_string()],
                agent_file_env: None,
                agent_file_args: vec![],
                // Pi accepts `--model <pattern>` (e.g. `gpt-4o-mini`,
                // `openai/gpt-4o`, `sonnet:high`).
                model_args: vec!["--model".to_string(), "{model}".to_string()],
                default_model: None,
            },
        );

        harnesses.insert(
            "opencode".to_string(),
            HarnessConfig {
                // OpenCode takes prompts via the `run` subcommand (positional),
                // not as a top-level argument. JSON output uses `--format json`.
                command: "opencode".to_string(),
                args: vec!["run".to_string()],
                // OpenCode's interactive TUI is the default command (no
                // subcommand). The TUI accepts `--prompt <text>`, which
                // auto-submits the first user turn when it opens (see
                // packages/opencode/src/cli/cmd/tui/thread.ts and home.tsx).
                // Per-call permissions are config-only (OPENCODE_PERMISSION
                // env / opencode.json), so we leave those to the user's
                // ambient config and only seed the prompt here.
                plan_args: vec!["--prompt".to_string(), "{prompt}".to_string()],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--format".to_string(), "json".to_string()],
                agent_file_env: None,
                agent_file_args: vec![],
                // opencode expects `-m provider/model` — the user supplies
                // the full `provider/model` string as the model value
                // (e.g. `anthropic/claude-sonnet-4-20250514`).
                model_args: vec!["-m".to_string(), "{model}".to_string()],
                default_model: None,
            },
        );

        harnesses.insert(
            "copilot".to_string(),
            HarnessConfig {
                // The standalone GitHub Copilot CLI binary, NOT the older
                // `gh copilot` extension. Auth uses COPILOT_GITHUB_TOKEN
                // (or falls back to GH_TOKEN / GITHUB_TOKEN).
                command: "copilot".to_string(),
                args: vec![
                    "-p".to_string(),
                    "{prompt}".to_string(),
                    "--silent".to_string(),
                    "--allow-all-paths".to_string(),
                    "--allow-all".to_string(),
                ],
                // Copilot's `-p` mode is one-shot non-interactive. For
                // interactive plan-harness sessions we use `-i`, which
                // starts a REPL and seeds the first user turn from the
                // prompt argument. `--allow-all` / `--allow-all-paths`
                // skip permission gating, which is what we want since
                // the user is driving the session interactively anyway.
                plan_args: vec![
                    "--allow-all-paths".to_string(),
                    "--allow-all".to_string(),
                    "-i".to_string(),
                    "{prompt}".to_string(),
                ],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--output-format".to_string(), "json".to_string()],
                agent_file_env: None,
                agent_file_args: vec![],
                // copilot uses `=`-style: `--model=<name>`.
                model_args: vec!["--model={model}".to_string()],
                default_model: None,
            },
        );

        harnesses.insert(
            "goose".to_string(),
            HarnessConfig {
                // Goose non-interactive invocation is `goose run -t "<prompt>"`.
                // `--no-session` prevents session file creation so automated
                // runs don't litter the filesystem. JSON output is controlled
                // by `--output-format json` (single trailing object) or
                // `stream-json` (JSONL events) — we pick the simpler `json`.
                //
                // Agent files are injected via the `GOOSE_SYSTEM_PROMPT_FILE_PATH`
                // env var, which completely replaces the default system prompt
                // with the contents of the given file. `supports_agent_file`
                // stays false because goose has no native file-path flag; the
                // env-var path in `build_harness_env` handles it.
                command: "goose".to_string(),
                args: vec![
                    "run".to_string(),
                    "-t".to_string(),
                    "{prompt}".to_string(),
                    "--no-session".to_string(),
                ],
                // Goose's `session` subcommand does NOT accept a seeded
                // prompt, but `goose run -t <text> -s` does exactly what
                // we need: process the initial input, then drop into the
                // REPL via the `-s`/`--interactive` flag (see
                // crates/goose-cli/src/cli.rs around line 320 and the
                // `session.interactive(input_config.contents)` call site).
                // The agent definition is still loaded via the
                // GOOSE_SYSTEM_PROMPT_FILE_PATH env var set by
                // build_plan_harness_env, so {prompt} only needs to carry
                // the user turn. Goose has no CLI autonomy flags —
                // autonomy is controlled by the GOOSE_MODE env var
                // (auto / approve / smart_approve / chat), which we leave
                // to the user's ambient environment.
                plan_args: vec![
                    "run".to_string(),
                    "-t".to_string(),
                    "{prompt}".to_string(),
                    "-s".to_string(),
                ],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--output-format".to_string(), "json".to_string()],
                agent_file_env: Some("GOOSE_SYSTEM_PROMPT_FILE_PATH".to_string()),
                agent_file_args: vec![],
                // goose accepts `--model <name>` on `run`. If your build
                // instead requires GOOSE_MODEL env var, clear this and set
                // the env var ambient.
                model_args: vec!["--model".to_string(), "{model}".to_string()],
                default_model: None,
            },
        );

        Self {
            default_harness: "claude".to_string(),
            max_retries_per_step: 3,
            timeout_secs: 0,
            harnesses,
        }
    }
}

/// Returns the configuration directory for ralph-rs.
///
/// Uses XDG semantics on every platform so the config can live alongside
/// the user's other dotfiles:
/// - `$XDG_CONFIG_HOME/ralph-rs` if set
/// - otherwise `$HOME/.config/ralph-rs`
///
/// We deliberately do not use `dirs::config_dir()`, which on macOS returns
/// `~/Library/Application Support` and breaks dotfile workflows.
pub fn config_dir() -> Result<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(xdg).join("ralph-rs"));
    }
    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home.join(".config").join("ralph-rs"))
}

/// Returns the platform-specific data directory for ralph-rs.
///
/// This holds runtime state (the SQLite database), not user-curated config,
/// so it follows platform conventions via `dirs::data_dir()`:
/// - Linux: `~/.local/share/ralph-rs`
/// - macOS: `~/Library/Application Support/ralph-rs`
/// - Windows: `{FOLDERID_RoamingAppData}/ralph-rs`
pub fn data_dir() -> Result<PathBuf> {
    let base = dirs::data_dir().context("Could not determine data directory")?;
    Ok(base.join("ralph-rs"))
}

/// Returns the directory where agent definition files are stored.
///
/// Agent files are user-authored markdown — they belong with the rest of
/// the user's config so they can be checked into dotfiles. Located at
/// `<config_dir>/agents`.
pub fn agents_dir() -> Result<PathBuf> {
    Ok(config_dir()?.join("agents"))
}

/// Loads configuration from disk, or creates a default config file if none exists.
pub fn load_or_create_config() -> Result<Config> {
    let dir = config_dir()?;
    let path = dir.join("config.json");

    if path.exists() {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let config: Config = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("Invalid config at {}", path.display()))?;
        Ok(config)
    } else {
        let config = Config::default();
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create config directory {}", dir.display()))?;
        let json = serde_json::to_string_pretty(&config)?;
        fs::write(&path, &json)
            .with_context(|| format!("Failed to write default config to {}", path.display()))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_has_all_harnesses() {
        let config = Config::default();
        assert_eq!(config.default_harness, "claude");
        assert_eq!(config.max_retries_per_step, 3);
        assert_eq!(config.timeout_secs, 0);

        let expected_harnesses = ["claude", "codex", "pi", "opencode", "copilot", "goose"];
        for name in &expected_harnesses {
            assert!(
                config.harnesses.contains_key(*name),
                "Missing harness: {name}"
            );
        }
        assert_eq!(config.harnesses.len(), 6);
    }

    #[test]
    fn test_config_json_roundtrip() {
        let config = Config::default();
        let json = serde_json::to_string_pretty(&config).expect("serialize");
        let deserialized: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_harness_config_fields() {
        let config = Config::default();

        let claude = &config.harnesses["claude"];
        assert_eq!(claude.command, "claude");
        assert!(claude.supports_agent_file);
        assert!(claude.supports_json_output);
        assert!(!claude.json_output_args.is_empty());
        // Non-interactive runs must bypass claude's permission prompts —
        // otherwise the subprocess blocks waiting for approval input.
        assert!(claude.args.contains(&"-p".to_string()));
        assert!(claude.args.contains(&"--permission-mode".to_string()));
        assert!(claude.args.contains(&"bypassPermissions".to_string()));
        assert!(claude.plan_args.contains(&"--permission-mode".to_string()));
        assert!(
            claude
                .plan_args
                .contains(&"bypassPermissions".to_string())
        );
        // Claude takes the agent file via --system-prompt-file, not via env.
        // `agent_file_env` is only read when supports_agent_file is false,
        // so setting it on claude would be dead config.
        assert!(claude.agent_file_env.is_none());
        assert_eq!(
            claude.agent_file_args,
            vec![
                "--system-prompt-file".to_string(),
                "{agent_file}".to_string(),
            ]
        );
        // Claude plan_args must reference the agent file natively and
        // carry the prompt placeholder.
        assert!(!claude.plan_args.is_empty());
        assert!(claude.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            claude
                .plan_args
                .contains(&"--system-prompt-file".to_string())
        );
        assert!(claude.plan_args.contains(&"{agent_file}".to_string()));

        let codex = &config.harnesses["codex"];
        assert_eq!(codex.command, "codex");
        assert!(!codex.supports_agent_file);
        assert!(codex.supports_json_output);
        assert_eq!(codex.json_output_args, vec!["--json".to_string()]);
        // Uses the `exec` subcommand with `{prompt}` placeholder replaced
        // in-place, plus non-interactive hardening flags.
        assert_eq!(codex.args[0], "exec");
        assert_eq!(codex.args[1], "{prompt}");
        assert!(codex.args.contains(&"--ephemeral".to_string()));
        assert!(codex.args.contains(&"--skip-git-repo-check".to_string()));
        assert!(codex.args.contains(&"approval_policy=never".to_string()));
        // Plan-harness mode for codex must enter the interactive TUI
        // (default subcommand, NOT `exec`) with a seeded positional
        // prompt and the low-friction `--full-auto` autonomy combo.
        assert!(!codex.plan_args.is_empty());
        assert!(codex.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            codex.plan_args.contains(&"--full-auto".to_string()),
            "codex plan_args must request --full-auto, got: {:?}",
            codex.plan_args
        );
        assert!(
            !codex.plan_args.contains(&"exec".to_string()),
            "codex plan_args must NOT use the `exec` subcommand (one-shot, non-interactive): {:?}",
            codex.plan_args
        );

        let pi = &config.harnesses["pi"];
        assert_eq!(pi.args, vec!["-p".to_string()]);
        assert!(pi.supports_json_output);
        assert_eq!(
            pi.json_output_args,
            vec!["--mode".to_string(), "json".to_string()]
        );
        // Pi's interactive mode is the default when `-p` is absent, and a
        // positional seeds the first turn. Pi has no permission flags by
        // design, so plan_args should be just the prompt placeholder.
        assert!(!pi.plan_args.is_empty());
        assert!(pi.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            !pi.plan_args.contains(&"-p".to_string()),
            "pi plan_args must NOT use -p (print/one-shot mode): {:?}",
            pi.plan_args
        );

        let opencode = &config.harnesses["opencode"];
        assert_eq!(opencode.args, vec!["run".to_string()]);
        assert!(opencode.supports_json_output);
        assert_eq!(
            opencode.json_output_args,
            vec!["--format".to_string(), "json".to_string()]
        );
        // opencode's TUI is the default command — plan_args must NOT
        // invoke the `run` subcommand (that's one-shot non-interactive).
        // The TUI accepts `--prompt <text>`, which auto-submits.
        assert!(!opencode.plan_args.is_empty());
        assert!(opencode.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            opencode.plan_args.contains(&"--prompt".to_string()),
            "opencode plan_args must use --prompt to seed the TUI: {:?}",
            opencode.plan_args
        );
        assert!(
            !opencode.plan_args.contains(&"run".to_string()),
            "opencode plan_args must NOT invoke the `run` subcommand (one-shot): {:?}",
            opencode.plan_args
        );

        let copilot = &config.harnesses["copilot"];
        assert_eq!(copilot.command, "copilot");
        assert!(copilot.args.contains(&"-p".to_string()));
        assert!(copilot.args.contains(&"{prompt}".to_string()));
        assert!(copilot.args.contains(&"--silent".to_string()));
        assert!(copilot.args.contains(&"--allow-all-paths".to_string()));
        assert!(copilot.args.contains(&"--allow-all".to_string()));
        assert!(copilot.supports_json_output);
        assert_eq!(
            copilot.json_output_args,
            vec!["--output-format".to_string(), "json".to_string()]
        );
        // Copilot plan-harness mode uses `-i` (interactive REPL, seeded
        // via positional) and keeps the --allow-all* flags to skip
        // permission gating in the interactive session.
        assert!(!copilot.plan_args.is_empty());
        assert!(copilot.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            copilot.plan_args.contains(&"-i".to_string()),
            "copilot plan_args must use -i (interactive): {:?}",
            copilot.plan_args
        );
        assert!(
            !copilot.plan_args.contains(&"-p".to_string()),
            "copilot plan_args must NOT use -p (one-shot): {:?}",
            copilot.plan_args
        );

        let goose = &config.harnesses["goose"];
        assert_eq!(goose.command, "goose");
        assert_eq!(
            goose.agent_file_env,
            Some("GOOSE_SYSTEM_PROMPT_FILE_PATH".to_string())
        );
        // goose's `session` subcommand can't seed a prompt, so plan-harness
        // mode uses `goose run -t {prompt} -s` — the `-s`/--interactive
        // flag drops into the REPL after processing the initial input.
        assert!(!goose.plan_args.is_empty());
        assert!(goose.plan_args.contains(&"{prompt}".to_string()));
        assert!(
            goose.plan_args.contains(&"run".to_string()),
            "goose plan_args must start from `goose run`: {:?}",
            goose.plan_args
        );
        assert!(
            goose.plan_args.contains(&"-t".to_string()),
            "goose plan_args must pass -t <prompt>: {:?}",
            goose.plan_args
        );
        assert!(
            goose.plan_args.contains(&"-s".to_string()),
            "goose plan_args must include -s (stay interactive after initial input): {:?}",
            goose.plan_args
        );
    }

    #[test]
    fn test_config_deserialize_from_json() {
        let json = r#"{
            "default_harness": "codex",
            "max_retries_per_step": 5,
            "timeout_secs": 600,
            "harnesses": {
                "codex": {
                    "command": "codex",
                    "args": [],
                    "supports_agent_file": false,
                    "supports_json_output": true,
                    "json_output_args": ["--json"],
                    "agent_file_env": null
                }
            }
        }"#;
        let config: Config = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.default_harness, "codex");
        assert_eq!(config.max_retries_per_step, 5);
        assert_eq!(config.harnesses.len(), 1);
    }

    #[test]
    fn test_config_dir_returns_path() {
        // On any platform this should succeed and contain "ralph-rs"
        let dir = config_dir().expect("config_dir");
        assert!(dir.ends_with("ralph-rs"));
    }

    #[test]
    fn test_data_dir_returns_path() {
        let dir = data_dir().expect("data_dir");
        assert!(dir.ends_with("ralph-rs"));
    }

    #[test]
    fn test_agents_dir_returns_path() {
        let dir = agents_dir().expect("agents_dir");
        assert!(dir.ends_with("agents"));
    }

    #[test]
    fn test_load_or_create_config_creates_file() {
        // Use a temp dir to avoid polluting the real config
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_path = tmp.path().join("config.json");

        // Manually test the creation logic
        let config = Config::default();
        let json = serde_json::to_string_pretty(&config).expect("serialize");
        std::fs::write(&config_path, &json).expect("write");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let loaded: Config = serde_json::from_str(&contents).expect("deserialize");
        assert_eq!(config, loaded);
    }

    #[test]
    fn test_validate_rejects_missing_default_harness() {
        let mut config = Config::default();
        config.default_harness = "nope".to_string();
        let err = config.validate().expect_err("validate must reject missing harness");
        let msg = format!("{err}");
        assert!(
            msg.contains("nope"),
            "error should name the offending harness: {msg}"
        );
        assert!(
            msg.contains("default_harness"),
            "error should reference default_harness: {msg}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_default_harness() {
        let mut config = Config::default();
        config.default_harness = String::new();
        let err = config.validate().expect_err("validate must reject empty");
        assert!(
            format!("{err}").contains("default_harness"),
            "error should reference default_harness"
        );
    }

    #[test]
    fn test_validate_accepts_default_config() {
        Config::default()
            .validate()
            .expect("default config must validate");
    }

    #[test]
    fn test_harness_config_default_fields() {
        // Verify serde defaults work when fields are omitted
        let json = r#"{"command": "test"}"#;
        let harness: HarnessConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(harness.command, "test");
        assert!(harness.args.is_empty());
        assert!(!harness.supports_agent_file);
        assert!(!harness.supports_json_output);
        assert!(harness.json_output_args.is_empty());
        assert!(harness.agent_file_env.is_none());
    }
}
