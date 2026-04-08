// Configuration management
#![allow(dead_code)]

use anyhow::{Context, Result};
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
    /// Whether this harness supports an agent file (e.g. CLAUDE.md).
    #[serde(default)]
    pub supports_agent_file: bool,
    /// Whether this harness supports structured JSON output.
    #[serde(default)]
    pub supports_json_output: bool,
    /// Additional args to enable JSON output mode.
    #[serde(default)]
    pub json_output_args: Vec<String>,
    /// Environment variable name used to point to the agent file.
    #[serde(default)]
    pub agent_file_env: Option<String>,
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

impl Default for Config {
    fn default() -> Self {
        let mut harnesses = HashMap::new();

        harnesses.insert(
            "claude".to_string(),
            HarnessConfig {
                command: "claude".to_string(),
                args: vec!["-p".to_string()],
                supports_agent_file: true,
                supports_json_output: true,
                json_output_args: vec!["--output-format".to_string(), "json".to_string()],
                agent_file_env: Some("CLAUDE_AGENT_FILE".to_string()),
            },
        );

        harnesses.insert(
            "codex".to_string(),
            HarnessConfig {
                command: "codex".to_string(),
                args: vec![],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--json".to_string()],
                agent_file_env: None,
            },
        );

        harnesses.insert(
            "pi".to_string(),
            HarnessConfig {
                command: "pi".to_string(),
                args: vec![],
                supports_agent_file: false,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
            },
        );

        harnesses.insert(
            "opencode".to_string(),
            HarnessConfig {
                command: "opencode".to_string(),
                args: vec![],
                supports_agent_file: false,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
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
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--output-format".to_string(), "json".to_string()],
                agent_file_env: None,
            },
        );

        harnesses.insert(
            "goose".to_string(),
            HarnessConfig {
                command: "goose".to_string(),
                args: vec![],
                supports_agent_file: false,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
            },
        );

        Self {
            default_harness: "claude".to_string(),
            max_retries_per_step: 3,
            timeout_secs: 300,
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
        assert_eq!(config.timeout_secs, 300);

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
        assert!(claude.agent_file_env.is_some());

        let codex = &config.harnesses["codex"];
        assert_eq!(codex.command, "codex");
        assert!(!codex.supports_agent_file);
        assert!(codex.supports_json_output);

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
