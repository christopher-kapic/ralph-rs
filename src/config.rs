// Configuration management

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Accepts either `null`, a missing field, or `0` as "no timeout" so configs
/// written before `timeout_secs` became `Option<u64>` keep loading. Positive
/// integers become `Some(n)`.
fn deserialize_timeout_secs<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<u64>::deserialize(deserializer)?;
    Ok(match opt {
        Some(0) | None => None,
        Some(n) => Some(n),
    })
}

/// How the step prompt is delivered to the harness subprocess.
///
/// Linux caps a single argv string at `MAX_ARG_STRLEN` (128 KB) and
/// `execve` returns `E2BIG` when that limit is exceeded. For large prompts
/// (retry context accumulating, long plan contexts) ralph must avoid
/// passing the prompt as an argv element. Most harnesses accept prompts
/// on stdin; a few require a file path instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PromptInputMode {
    /// Pipe prompt to stdin. Most harnesses support this.
    #[default]
    Stdin,
    /// Pass prompt as an argv element. Only for harnesses without stdin support.
    /// WARNING: subject to 128 KB E2BIG limit; ralph auto-spills to temp file beyond that.
    Argv,
    /// Write prompt to a temp file, pass the file path as an argv element.
    TempFile,
}

impl std::fmt::Display for PromptInputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PromptInputMode::Stdin => "stdin",
            PromptInputMode::Argv => "argv",
            PromptInputMode::TempFile => "tempfile",
        };
        f.write_str(s)
    }
}

/// What to do when `PromptInputMode::Argv` is selected but the prompt
/// exceeds the kernel's argv ceiling (`MAX_ARG_STRLEN` = 128 KB on Linux).
///
/// Consulted only when the primary mode is `Argv` and the prompt is large
/// enough to risk `E2BIG`. The default — `SpillToTempFile` — preserves the
/// historical "transparently materialize a tempfile and swap its path into
/// `{prompt}`" behavior, which only works for harnesses that interpret the
/// substituted value as a file path. Most harnesses do NOT; their CLI reads
/// the value as literal prompt text.
///
/// `Error` is for harnesses that accept prompts only as inline argv text
/// with no file/stdin fallback. Spilling silently produces a broken
/// invocation (the harness sees a path string and treats it as the prompt),
/// so the safer behavior is to abort with a clear error. The canonical
/// example is the GitHub Copilot CLI: as of github/copilot-cli#1046 it has
/// no `--prompt-file`, no `@file` syntax, and no stdin support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArgvOverflowBehavior {
    /// Materialize the prompt to a `NamedTempFile` and substitute its path
    /// for `{prompt}` in `args`. The default. Correct only when the
    /// harness's CLI interprets the value as a file path, not as inline
    /// text. Verified for: none of the current built-ins (kept as default
    /// for backward compatibility — harnesses that need stricter behavior
    /// set `Error` or `SpillToStdin` explicitly).
    #[default]
    SpillToTempFile,
    /// Pipe the prompt to the child's stdin and strip the `{prompt}`
    /// placeholder from `args`. Valid only when the harness's CLI accepts
    /// prompts on stdin (typically via a trailing `-` positional or by
    /// having no positional prompt arg at all). None of the current
    /// built-ins use this — they declare `prompt_input: Stdin` directly
    /// instead — but it stays in the enum for harnesses that prefer argv
    /// for short prompts and accept stdin only as a fallback.
    SpillToStdin,
    /// Abort the invocation with a clear error. The harness's CLI accepts
    /// prompts only as inline argv text and has no working alternative
    /// delivery mode. Used by `copilot` per github/copilot-cli#1046.
    Error,
}

impl std::fmt::Display for ArgvOverflowBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ArgvOverflowBehavior::SpillToTempFile => "spill_to_temp_file",
            ArgvOverflowBehavior::SpillToStdin => "spill_to_stdin",
            ArgvOverflowBehavior::Error => "error",
        };
        f.write_str(s)
    }
}

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
    /// Environment variables that, when any one is set, signal that the
    /// harness is authenticated. Consulted by the preflight harness-auth
    /// check before falling back to [`Self::auth_probe_args`]. Empty means
    /// "no env-var-based auth check".
    #[serde(default)]
    pub auth_env_vars: Vec<String>,
    /// Argument vector for a non-interactive auth probe. When no
    /// [`Self::auth_env_vars`] matched, preflight runs
    /// `<command> <auth_probe_args...>` and treats a zero exit as
    /// authenticated. Empty means "no probe".
    #[serde(default)]
    pub auth_probe_args: Vec<String>,
    /// How the step prompt is delivered to the subprocess. Defaults to
    /// [`PromptInputMode::Stdin`] so large prompts don't trip `E2BIG` on
    /// Linux (128 KB argv cap). In `Stdin` mode the `{prompt}` placeholder
    /// is stripped from `args` at spawn time and the prompt text is piped
    /// to the child's stdin instead. In `TempFile` mode the placeholder is
    /// replaced with a temp file path. In `Argv` mode the prompt is passed
    /// inline; what happens past the 64 KB threshold is controlled by
    /// [`Self::argv_overflow`].
    #[serde(default)]
    pub prompt_input: PromptInputMode,
    /// Fallback delivery when `prompt_input == Argv` and the prompt
    /// exceeds the argv spill threshold. Defaults to
    /// [`ArgvOverflowBehavior::SpillToTempFile`] for backward compatibility
    /// (preserves the pre-existing auto-spill behavior), but for harnesses
    /// whose CLI cannot read a file path or stdin in place of the inline
    /// prompt, set this to [`ArgvOverflowBehavior::Error`] so ralph fails
    /// loudly instead of producing a silently-broken invocation.
    ///
    /// Ignored when `prompt_input != Argv`.
    #[serde(default)]
    pub argv_overflow: ArgvOverflowBehavior,
    /// Optional per-harness color override as `#RRGGBB` hex. Takes
    /// precedence over the hardcoded color map in `output::harness_color`.
    /// Validated at config load time; malformed values cause a hard error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

/// Default timeout in seconds for a single lifecycle hook invocation.
/// Applied when `Config::hook_timeout_secs` is missing from a user's config.
fn default_hook_timeout_secs() -> u64 {
    120
}

/// Default for `Config::auto_stash`. On by default so `ralph run` preserves
/// a dirty working tree via `git stash push --include-untracked` before
/// switching branches and pops it back at run end. Users who want to manage
/// their own stashing can set this to `false` (or pass `--no-auto-stash`
/// per-run).
fn default_auto_stash() -> bool {
    true
}

/// Default for `Config::min_free_disk_mb`. 1 GB is enough headroom for a
/// handful of Cargo artifacts before the next hook gets a chance to run and
/// prune; tuned low enough not to fire on small projects, high enough to
/// catch "disk is actually about to fill" before SQLite wedges.
fn default_min_free_disk_mb() -> u64 {
    1024
}

/// Default IANA timezone name used by the progress-header "started at"
/// stamp. UTC is safe on any platform and doesn't change semantics at DST
/// boundaries. Users opt in to a local zone via `ralph config set-timezone`.
pub fn default_display_timezone() -> String {
    "UTC".to_string()
}

/// Default for `Config::harness_chunk_max_bytes`. Caps the size of a single
/// `harness_chunk` / `test_chunk` NDJSON event payload so a runaway agent
/// printing a huge unbroken line can't blow up the TUI's buffer or a
/// downstream consumer. 4 KB is enough for any reasonable line of log
/// output and matches the spec in TUI-plan §13.1.
fn default_harness_chunk_max_bytes() -> usize {
    4096
}

/// Top-level ralph-rs configuration.
/// Global nondeterministic-review configuration (docs/dag-redesign.md §6).
///
/// Lives under the top-level `"review"` key of
/// `~/.config/ralph-rs/config.json`, e.g.
/// `"review": { "enabled": true, "harness": "codex", "model": "gpt-5-codex" }`.
///
/// Every field has a serde default and the whole block is
/// `#[serde(default)]` on [`Config`], so a config file that predates the
/// review feature (no `"review"` key at all) keeps loading unchanged and
/// resolves to "review off, no review harness/model".
///
/// - `enabled` is the **global default** in the precedence chain
///   step.review_enabled ?? plan.review_enabled ?? config.review.enabled
///   ?? false (resolved by [`effective_review_enabled`], mirroring
///   `RetryStrategy` step > plan > default precedence). It is `Option<bool>`
///   so "unset in config" (`None`) is distinguishable from an explicit
///   `false`; both fall through to `false` today but the distinction keeps
///   the precedence chain uniform with the per-plan / per-step columns.
/// - `harness` / `model` name the harness + model the reviewer subprocess
///   uses. They are *global config*, never plan/export data (a bundle stays
///   portable across machines whose review harness differs — §13.2). Empty
///   string = unconfigured; `ralph doctor` warns later when review is on
///   but no review harness is set (wired in a later Phase 3 step).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ReviewConfig {
    /// Global default for whether a step is reviewed. `None` = unset in
    /// config; resolves to `false` at the bottom of the precedence chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Harness name the reviewer subprocess uses. Empty = unconfigured.
    #[serde(default)]
    pub harness: String,
    /// Model the reviewer subprocess uses. Empty = harness default.
    #[serde(default)]
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// The default harness to use when none is specified.
    pub default_harness: String,
    /// Maximum number of retries per step before giving up.
    pub max_retries_per_step: u32,
    /// Timeout in seconds for a single harness invocation. `None` (or the
    /// legacy `0` value) disables the timeout.
    #[serde(default, deserialize_with = "deserialize_timeout_secs")]
    pub timeout_secs: Option<u64>,
    /// Timeout in seconds for a single lifecycle hook (pre/post-step,
    /// pre/post-test). `0` disables the timeout. Defaults to 120.
    #[serde(default = "default_hook_timeout_secs")]
    pub hook_timeout_secs: u64,
    /// When true (default), `ralph run` stashes any dirty working-tree
    /// state (tracked + untracked) before switching to the plan branch and
    /// pops the stash back at run end. When false, a dirty tree causes
    /// the run to bail so the user can manage it manually. The
    /// `--no-auto-stash` CLI flag forces this off for a single run.
    #[serde(default = "default_auto_stash")]
    pub auto_stash: bool,
    /// Global prompt — the outermost layer of the four-layer prompt model
    /// (Global → Project → Plan → Step), stacked at the top of every step
    /// prompt ahead of the project and plan layers. Seeded by `ralph init`
    /// with a pointer to the ralph CLI; editable via
    /// `ralph prompt set --scope global`. `None` means no global
    /// contribution.
    ///
    /// Configs written by a pre-collapse ralph carried separate
    /// `prompt_prefix` / `prompt_suffix` fields; those are migrated into this
    /// single field on load (see [`migrate_legacy_prompt_fields`]) and the
    /// file is rewritten in the new shape so subsequent loads are clean.
    #[serde(default)]
    pub prompt: Option<String>,
    /// Minimum free disk space (in MB) required before running a step.
    /// Default 1024 (1 GB). Set to 0 to disable the check.
    ///
    /// Gates `executor::execute_step` so a nearly-full filesystem terminates
    /// the attempt with a clear reason instead of letting SQLite hit
    /// SQLITE_FULL mid-write and corrupt ralph's own state.
    #[serde(default = "default_min_free_disk_mb")]
    pub min_free_disk_mb: u64,
    /// IANA timezone name used to format the "started at" stamp in the
    /// progress header. Defaults to `UTC`. Validated at load time via
    /// `chrono_tz::Tz::from_str` so typos fail loudly.
    #[serde(default = "default_display_timezone")]
    pub display_timezone: String,
    /// Maximum byte length of a single `harness_chunk` / `test_chunk` NDJSON
    /// event payload. Chunks larger than this are truncated before emission.
    /// Default 4096 (see [`default_harness_chunk_max_bytes`]). Per
    /// TUI-plan §13.1.
    #[serde(default = "default_harness_chunk_max_bytes")]
    pub harness_chunk_max_bytes: usize,
    /// Global nondeterministic-review configuration (§6). `#[serde(default)]`
    /// so a config file with no `"review"` key still loads, resolving to
    /// [`ReviewConfig::default`] (review off, no review harness/model). The
    /// effective per-step toggle is resolved by [`effective_review_enabled`].
    #[serde(default)]
    pub review: ReviewConfig,
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
            let mut available: Vec<&str> = self.harnesses.keys().map(String::as_str).collect();
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

        // Validate display_timezone against chrono-tz's known-name list. We
        // reject empty / unparseable IANA names at load time rather than
        // letting format_now_in_tz panic mid-run.
        use std::str::FromStr;
        chrono_tz::Tz::from_str(&self.display_timezone).map_err(|e| {
            anyhow!(
                "config.display_timezone '{}' is not a valid IANA timezone name: {e}",
                self.display_timezone
            )
        })?;

        // Validate per-harness color overrides. Failing loudly here beats
        // a silent fallback to the hardcoded map at render time.
        for (name, hc) in &self.harnesses {
            if let Some(hex) = &hc.color {
                crate::output::parse_hex_color(hex)
                    .map_err(|e| anyhow!("config.harnesses.{name}.color: {e}"))?;
            }
        }
        Ok(())
    }

    /// Write this config as pretty-printed JSON to the canonical config
    /// path (`<config_dir>/config.json`), atomically via tmp-file + rename.
    ///
    /// Used by `ralph config set-timezone` and any future mutator paths.
    /// Validates before writing so we never persist a broken config.
    pub fn save(&self) -> Result<()> {
        let dir = config_dir()?;
        self.save_at(&dir)
    }

    /// Path-explicit form of [`Self::save`] — writes `<dir>/config.json`
    /// atomically. Used by tests that round-trip against a tempdir; the TUI
    /// universal-prompt pane edit handoff also routes through here so
    /// non-default `XDG_CONFIG_HOME` setups stay correct.
    pub(crate) fn save_at(&self, dir: &Path) -> Result<()> {
        self.validate()?;
        fs::create_dir_all(dir)
            .with_context(|| format!("Failed to create config directory {}", dir.display()))?;
        let path = dir.join("config.json");
        write_config_atomic(dir, &path, self)
    }
}

/// Write `config` as pretty-printed JSON to `path` atomically: write to a
/// uniquely-named tmp file in `dir`, fsync it, then rename over `path`.
/// Rename is atomic on every supported filesystem, so observers either
/// see the old file or the new file — never a half-written one.
fn write_config_atomic(dir: &Path, path: &Path, config: &Config) -> Result<()> {
    let json = serde_json::to_string_pretty(config)?;

    use std::sync::atomic::{AtomicU64, Ordering};
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = dir.join(format!(
        "config.json.tmp-{}-{}-{:x}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed),
        nanos,
    ));

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .with_context(|| format!("Failed to create tmp file {}", tmp.display()))?;
    let write_result = f
        .write_all(json.as_bytes())
        .and_then(|_| f.sync_all())
        .with_context(|| format!("Failed to write tmp file {}", tmp.display()));
    drop(f);

    let result = write_result.and_then(|_| {
        fs::rename(&tmp, path)
            .with_context(|| format!("Failed to rename tmp into {}", path.display()))
    });

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
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
                //
                // `-p -` tells claude to read the prompt from stdin (the
                // trailing `-` is the positional PROMPT argument, a Unix
                // convention for "read from stdin"). In `Stdin` mode the
                // `{prompt}` placeholder is stripped at spawn time and the
                // prompt text is piped in instead.
                args: vec![
                    "-p".to_string(),
                    "-".to_string(),
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
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                // Claude accepts prompts on stdin via `-p -` (see args
                // above). Stdin mode bypasses the 128 KB argv cap.
                prompt_input: PromptInputMode::Stdin,
                // Stdin mode; argv_overflow is unused but set for clarity.
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
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
                //
                // `--sandbox workspace-write` lets codex modify the workspace
                // (the project directory). Without an explicit `--sandbox`,
                // codex defaults to read-only, which silently fails on any
                // implementation step and is too strict even for review steps
                // that need to write pager state to /tmp. Steps that need to
                // mutate state outside the workspace (e.g. a review step that
                // appends fix steps via `ralph step add`, which writes to the
                // ralph DB under `~/.local/share/ralph-rs/`) should select the
                // `codex-orchestrator` harness, which uses `danger-full-access`.
                command: "codex".to_string(),
                // `codex exec` reads the prompt from stdin when no positional
                // prompt argument is provided. In Stdin mode ralph strips the
                // `{prompt}` placeholder before spawn and pipes the prompt
                // text to the child's stdin instead, avoiding the 128 KB
                // argv cap.
                args: vec![
                    "exec".to_string(),
                    "{prompt}".to_string(),
                    "--skip-git-repo-check".to_string(),
                    "--ephemeral".to_string(),
                    "--sandbox".to_string(),
                    "workspace-write".to_string(),
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
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                // codex exec reads from stdin when no positional prompt is
                // present; the `{prompt}` placeholder is stripped in Stdin
                // mode and the prompt is piped in.
                prompt_input: PromptInputMode::Stdin,
                // Stdin mode; argv_overflow is unused but set for clarity.
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        harnesses.insert(
            "codex-orchestrator".to_string(),
            HarnessConfig {
                // Same as `codex` but with `--sandbox danger-full-access`. Use
                // for steps that need to mutate state outside the workspace —
                // most commonly, review steps that append follow-up steps via
                // `ralph step add` (the ralph SQLite DB lives under
                // `~/.local/share/ralph-rs/`, outside the workspace sandbox).
                // Implementation steps should stick with `codex` (workspace-write).
                command: "codex".to_string(),
                args: vec![
                    "exec".to_string(),
                    "{prompt}".to_string(),
                    "--skip-git-repo-check".to_string(),
                    "--ephemeral".to_string(),
                    "--sandbox".to_string(),
                    "danger-full-access".to_string(),
                    "-c".to_string(),
                    "approval_policy=never".to_string(),
                ],
                plan_args: vec!["--full-auto".to_string(), "{prompt}".to_string()],
                supports_agent_file: false,
                supports_json_output: true,
                json_output_args: vec!["--json".to_string()],
                agent_file_env: None,
                agent_file_args: vec![],
                model_args: vec!["-m".to_string(), "{model}".to_string()],
                default_model: None,
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                prompt_input: PromptInputMode::Stdin,
                // Stdin mode; argv_overflow is unused but set for clarity.
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
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
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                // Pi takes the prompt as a positional argv element. The
                // auto-spill guard in harness.rs promotes to TempFile when
                // the prompt exceeds 64 KB — preserved as the historical
                // default. Pi has no verified file-path or stdin fallback,
                // so the spilled path may be misinterpreted as a literal
                // prompt; revisit if pi gains an explicit file-input flag
                // or if users report large-prompt failures.
                prompt_input: PromptInputMode::Argv,
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
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
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                // opencode's `run` subcommand reads from stdin when no
                // positional prompt is supplied; Stdin mode strips the
                // `{prompt}` placeholder and pipes the prompt in.
                prompt_input: PromptInputMode::Stdin,
                // Stdin mode; argv_overflow is unused but set for clarity.
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
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
                // Standalone Copilot CLI accepts COPILOT_GITHUB_TOKEN,
                // GH_TOKEN, or GITHUB_TOKEN. Without one of these, `copilot`
                // requires an interactive `copilot login` device flow.
                auth_env_vars: vec![
                    "COPILOT_GITHUB_TOKEN".to_string(),
                    "GH_TOKEN".to_string(),
                    "GITHUB_TOKEN".to_string(),
                ],
                auth_probe_args: vec![],
                // Copilot CLI accepts prompts ONLY as inline argv text:
                // no `--prompt-file`, no `@file` syntax for `-p`, and no
                // stdin support as of github/copilot-cli#1046 (filed Jan
                // 2026, still open). Any other delivery mode silently
                // produces a broken invocation (the harness sees a path
                // string and treats it as the prompt). `argv_overflow:
                // Error` makes ralph fail loudly when the prompt is too
                // large for the kernel argv limit, instead of spilling
                // to a tempfile and feeding copilot a path-as-prompt.
                prompt_input: PromptInputMode::Argv,
                argv_overflow: ArgvOverflowBehavior::Error,
                color: None,
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
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                // goose's `-t` flag takes the prompt as an argv element.
                // The auto-spill guard in harness.rs promotes to TempFile
                // when the prompt exceeds 64 KB — preserved as the
                // historical default. Goose has no verified file-path
                // fallback, so the spilled path may be misinterpreted as
                // a literal prompt; revisit if users report large-prompt
                // failures.
                prompt_input: PromptInputMode::Argv,
                argv_overflow: ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );

        Self {
            default_harness: "claude".to_string(),
            max_retries_per_step: 3,
            timeout_secs: None,
            hook_timeout_secs: default_hook_timeout_secs(),
            auto_stash: default_auto_stash(),
            // The global prompt is seeded by `ralph init` (see
            // `commands::seed_global_prompt`), the canonical source of
            // ralph's built-in introspection block. A default config has no
            // prompt; init fills it with `prompt::DEFAULT_CONTEXT_PREPEND`.
            prompt: None,
            min_free_disk_mb: default_min_free_disk_mb(),
            display_timezone: default_display_timezone(),
            harness_chunk_max_bytes: default_harness_chunk_max_bytes(),
            // Review is off by default and carries no review harness/model
            // until the user configures one; the per-step effective value
            // resolves via `effective_review_enabled` (step > plan > this
            // global > false).
            review: ReviewConfig::default(),
            harnesses,
        }
    }
}

/// Resolve whether this step is nondeterministically reviewed.
///
/// Precedence is **step > plan > global > false**, the §6 spec table and a
/// direct analogue of [`crate::plan::Step::effective_retry_strategy`]
/// (step > plan > built-in default): a step-level override wins over a
/// plan-level default, which wins over the global `config.review.enabled`,
/// which finally falls through to `false` when nothing is set anywhere.
/// `None` at any level means "defer to the next level down".
///
/// ```text
/// step.review_enabled ?? plan.review_enabled ?? config.review.enabled ?? false
/// ```
///
/// Wired but not yet *consumed* by the runner in this batch — the review
/// pipeline that calls this lands in a later Phase 3 step. Disabled at any
/// level ⇒ the step is `Complete` straight from passing tests (§6).
#[allow(dead_code)] // consumed by the review pipeline in a later step
pub fn effective_review_enabled(
    step: &crate::plan::Step,
    plan: &crate::plan::Plan,
    config: &Config,
) -> bool {
    step.review_enabled
        .or(plan.review_enabled)
        .or(config.review.enabled)
        .unwrap_or(false)
}

/// True when this harness invokes `codex exec` — the non-interactive code
/// path that needs an explicit `--sandbox`. Shared by `harness_footguns` and
/// `harness_safety_summary` so the two surfaces can't drift on what counts
/// as "this is the codex case".
fn is_codex_exec_args(hc: &HarnessConfig) -> bool {
    hc.command == "codex" && hc.args.iter().any(|a| a == "exec")
}

/// True when this harness invokes `claude -p` — the non-interactive code
/// path that needs an explicit `--permission-mode`. See `is_codex_exec_args`
/// for why this is shared.
fn is_claude_print_args(hc: &HarnessConfig) -> bool {
    hc.command == "claude" && hc.args.iter().any(|a| a == "-p")
}

/// Look up the value following a flag in `args`. Returns `Some(value)` when
/// the flag appears and is followed by another arg; `None` when the flag is
/// absent or trails the vec. Linear scan — args lists are tiny (<10 items)
/// so a HashMap would be heavier than the lookup it replaces.
fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == flag {
            return iter.next().map(String::as_str);
        }
    }
    None
}

/// Inspect a harness's `args` for known foot-guns — invocation flags that
/// people commonly forget but which silently break `ralph run`. Returns one
/// short warning string per issue, suitable for surfacing through
/// `ralph harness list` / `show` and `ralph doctor`.
///
/// Currently flags:
/// - `codex` (or anything invoking the `codex exec` subcommand) without an
///   explicit `--sandbox`. Codex defaults to read-only with no sandbox flag,
///   so writes silently fail. The default `codex` config now ships with
///   `--sandbox workspace-write`; this catches users who copied an old
///   config or hand-edited the args.
/// - `claude` (the `-p` non-interactive path) without `--permission-mode`.
///   Without it, claude blocks on interactive approval prompts and hangs the
///   subprocess.
///
/// Heuristic-based: matches on `command` and arg presence. Conservative —
/// false positives are worse than missed warnings, so we only flag patterns
/// where the absent flag is clearly required, not "would be nicer".
pub fn harness_footguns(name: &str, hc: &HarnessConfig) -> Vec<String> {
    let mut issues = Vec::new();

    if is_codex_exec_args(hc) && !hc.args.iter().any(|a| a == "--sandbox") {
        issues.push(format!(
            "harness `{name}` invokes `codex exec` without `--sandbox`. \
             Codex defaults to read-only when no sandbox is specified, so \
             implementation steps will silently fail. Add `--sandbox \
             workspace-write` (or `danger-full-access` for steps that need \
             to write outside the workspace, e.g. review steps that call \
             `ralph step add`)."
        ));
    }

    if is_claude_print_args(hc) && !hc.args.iter().any(|a| a == "--permission-mode") {
        issues.push(format!(
            "harness `{name}` invokes `claude -p` without `--permission-mode`. \
             Claude will block on interactive approval prompts in non-interactive \
             mode and hang the subprocess. Add `--permission-mode bypassPermissions`."
        ));
    }

    issues
}

/// Underlying CLI binaries known to accept a `--model` / `-m` flag for
/// per-invocation model selection. Used by [`harness_compatibility_warnings`]
/// to flag harness configs that point at one of these commands but have an
/// empty `model_args`, in which case `ralph step add --model …` silently
/// drops the model override on the floor.
///
/// Matched on `HarnessConfig::command` (the actual binary name), not on
/// harness *name*, so a user who creates a custom harness called
/// `copilot-fast` pointing at `command: "copilot"` still gets the warning.
pub const MODEL_CAPABLE_COMMANDS: &[&str] =
    &["claude", "codex", "copilot", "goose", "opencode", "pi"];

/// Inspect a harness for known compatibility issues that won't show up in
/// `harness_footguns` but will silently break runs.
///
/// Currently flags:
/// - `copilot` with `prompt_input != Argv`: the Copilot CLI accepts prompts
///   only as inline argv text (no `--prompt-file`, no `@file` syntax, no
///   stdin per github/copilot-cli#1046). Stdin / TempFile mode produces a
///   broken invocation that copilot reads as a path-as-prompt.
/// - `copilot` with `argv_overflow != Error`: same root cause — the auto-
///   spill on large prompts would feed copilot a tempfile path; the
///   `Error` overflow mode aborts cleanly instead.
/// - Any harness whose `command` is in [`MODEL_CAPABLE_COMMANDS`] but whose
///   `model_args` is empty: `ralph step add --model X` will be silently
///   ignored.
pub fn harness_compatibility_warnings(name: &str, hc: &HarnessConfig) -> Vec<String> {
    let mut issues = Vec::new();

    if hc.command == "copilot" {
        if hc.prompt_input != PromptInputMode::Argv {
            issues.push(format!(
                "harness `{name}` uses command `copilot` with prompt_input \
                 = `{}`, but Copilot CLI accepts prompts ONLY as inline argv \
                 text (no --prompt-file, no stdin per github/copilot-cli#1046). \
                 Any other delivery silently produces a broken invocation \
                 where copilot reads a tempfile path as the literal prompt. \
                 Set prompt_input to `argv`.",
                hc.prompt_input
            ));
        }
        if hc.prompt_input == PromptInputMode::Argv
            && hc.argv_overflow != ArgvOverflowBehavior::Error
        {
            issues.push(format!(
                "harness `{name}` uses command `copilot` with argv_overflow \
                 = `{}`. Copilot has no working spill fallback (no \
                 --prompt-file / no stdin), so spilling on prompts >64 KB \
                 would feed copilot a path-as-prompt. Set argv_overflow to \
                 `error` so large prompts fail loudly instead.",
                hc.argv_overflow
            ));
        }
    }

    if MODEL_CAPABLE_COMMANDS.contains(&hc.command.as_str()) && hc.model_args.is_empty() {
        issues.push(format!(
            "harness `{name}` uses command `{}` which supports per-invocation \
             model selection, but `model_args` is empty — so `ralph step add \
             --model X` (or the plan-level model override) is silently dropped. \
             Add the appropriate template, e.g. `[\"--model\", \"{{model}}\"]` \
             (or `[\"--model={{model}}\"]` for copilot).",
            hc.command
        ));
    }

    issues
}

/// One-line summary of how the harness's args treat sandboxing / permissions,
/// for the `ralph harness list` table. Returns "ok" when neither known
/// permission flag is needed for the harness's command, or a short
/// human-readable description of the active mode (`workspace-write`,
/// `bypassPermissions`, etc.). Returns "no-sandbox!" or similar when a
/// known foot-gun is present, so the table itself surfaces the issue.
pub fn harness_safety_summary(hc: &HarnessConfig) -> String {
    if is_codex_exec_args(hc) {
        return flag_value(&hc.args, "--sandbox")
            .map(str::to_string)
            .unwrap_or_else(|| "no-sandbox!".to_string());
    }
    if is_claude_print_args(hc) {
        return flag_value(&hc.args, "--permission-mode")
            .map(str::to_string)
            .unwrap_or_else(|| "no-permission-mode!".to_string());
    }
    "—".to_string()
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
    load_or_create_config_at(&dir, &path)
}

/// Core logic for `load_or_create_config`, parameterized on paths so tests
/// can exercise concurrent callers. Writes the default to a unique tmp
/// file and uses `hard_link` to atomically publish it, closing the
/// TOCTOU gap: concurrent readers never observe a partially-written file,
/// and at most one writer wins the link race. Losers fall back to reading
/// the winner's file.
fn load_or_create_config_at(dir: &Path, path: &Path) -> Result<Config> {
    if path.exists() {
        return read_and_validate(path);
    }

    fs::create_dir_all(dir)
        .with_context(|| format!("Failed to create config directory {}", dir.display()))?;

    let default = Config::default();
    let json = serde_json::to_string_pretty(&default)?;

    use std::sync::atomic::{AtomicU64, Ordering};
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = dir.join(format!(
        "config.json.tmp-{}-{}-{:x}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed),
        nanos,
    ));

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .with_context(|| format!("Failed to create tmp file {}", tmp.display()))?;
    let write_result = f
        .write_all(json.as_bytes())
        .and_then(|_| f.sync_all())
        .with_context(|| format!("Failed to write tmp file {}", tmp.display()));
    drop(f);

    let result = write_result.and_then(|_| match fs::hard_link(&tmp, path) {
        Ok(()) => Ok(default),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => read_and_validate(path),
        Err(e) => Err(anyhow::Error::new(e))
            .with_context(|| format!("Failed to publish {}", path.display())),
    });

    let _ = fs::remove_file(&tmp);
    result
}

fn read_and_validate(path: &Path) -> Result<Config> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let mut raw: serde_json::Value = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse {}", path.display()))?;

    // Collapse a pre-overhaul `prompt_prefix` / `prompt_suffix` pair into the
    // single `prompt` field before deserialize. Track whether we rewrote the
    // shape so we can persist the cleaned config back to disk below.
    let migrated = migrate_legacy_prompt_fields(&mut raw);

    // Silently fill in missing fields at load time — the report variant is
    // for `ralph init` to surface what it persisted.
    let _ = layer_builtin_harness_defaults(&mut raw).with_context(|| {
        format!(
            "Failed to layer built-in harness defaults for {}",
            path.display()
        )
    })?;

    let config: Config = serde_json::from_value(raw)
        .with_context(|| format!("Failed to deserialize {}", path.display()))?;
    config
        .validate()
        .with_context(|| format!("Invalid config at {}", path.display()))?;

    // Rewrite the file in the new shape so subsequent loads don't re-run the
    // migration (and so the user's editor shows the collapsed field). Best
    // effort on the parent dir — a bare relative path with no parent still
    // loads correctly in-memory; we just skip the rewrite.
    if migrated && let Some(dir) = path.parent() {
        write_config_atomic(dir, path, &config)
            .with_context(|| format!("Failed to rewrite migrated config at {}", path.display()))?;
    }
    Ok(config)
}

/// The exact pre-overhaul `prompt_prefix` literal that `ralph init` seeded
/// for users who never customized the global prompt. Back then this short
/// one-liner was the *only* global-prefix contribution; the full
/// introspection block (`prompt::DEFAULT_CONTEXT_PREPEND`) was auto-injected
/// separately at runtime and never lived in the config file. After the
/// collapse + the removal of that runtime auto-injection, a verbatim copy of
/// this string in the migrated `prompt` field means "uncustomized legacy
/// default" — such a config must be re-seeded with the full block or the
/// migrating user silently loses ralph's entire introspection guidance.
///
/// This constant exists **only** for that back-compat detection. It must
/// stay byte-for-byte identical to the value of the (now-deleted)
/// `DEFAULT_GLOBAL_PROMPT_PREFIX` constant as of commit `6e7fb97^`
/// (`git show 6e7fb97^:src/config.rs`); do not "improve" the wording. The
/// legacy default had no companion `prompt_suffix` (it defaulted to `None`),
/// so there is no legacy-default suffix to match.
const LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX: &str = "You are running as part of a `ralph` plan. Run `ralph status` to see the active plan, or `ralph plan show <slug>` for full details. Plan-specific conventions may be defined in AGENTS.md or CLAUDE.md.";

/// Collapse the pre-overhaul global prompt fields (`prompt_prefix` /
/// `prompt_suffix`) into the single `prompt` field on a freshly-parsed
/// config `Value`. Returns `true` when the JSON object was changed, so the
/// caller can persist the cleaned shape back to disk.
///
/// When a new-shape `prompt` is already present it is left untouched (we
/// only strip the stale legacy keys). Otherwise the two legacy values are
/// concatenated with a blank line between them, skipping whichever side is
/// absent/null/empty, so no user customization is lost.
///
/// **Data-loss guard:** if the collapsed result is *byte-equal* to the
/// uncustomized legacy default ([`LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX`], with
/// no suffix — the only shape `ralph init` ever wrote for a default user),
/// it is treated as unseeded and replaced with the canonical
/// [`prompt::DEFAULT_CONTEXT_PREPEND`] block, so a migrating default user
/// ends up with the same effective content as a fresh install. The gate is
/// strict equality: anything the user customized (not byte-equal to the
/// legacy default) is preserved verbatim.
///
/// [`prompt::DEFAULT_CONTEXT_PREPEND`]: crate::prompt::DEFAULT_CONTEXT_PREPEND
fn migrate_legacy_prompt_fields(raw: &mut serde_json::Value) -> bool {
    let Some(root) = raw.as_object_mut() else {
        return false;
    };
    if !root.contains_key("prompt_prefix") && !root.contains_key("prompt_suffix") {
        return false;
    }

    if !root.contains_key("prompt") {
        let take = |root: &serde_json::Map<String, serde_json::Value>, key: &str| {
            root.get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .filter(|s| !s.is_empty())
        };
        let prefix = take(root, "prompt_prefix");
        let suffix = take(root, "prompt_suffix");
        let merged = match (prefix, suffix) {
            (Some(p), Some(s)) => Some(format!("{p}\n\n{s}")),
            (Some(p), None) => Some(p),
            (None, Some(s)) => Some(s),
            (None, None) => None,
        };
        if let Some(m) = merged {
            // Data-loss guard: an uncustomized legacy default (the short
            // one-liner prefix, no suffix) collapses to exactly
            // `LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX`. That value is non-blank,
            // so `seed_global_prompt` would skip re-seeding and the full
            // introspection block (no longer auto-injected at runtime) would
            // be permanently lost. Treat it as unseeded: substitute the
            // canonical block so a migrating default user matches a fresh
            // install. Strict byte equality — any customization is kept.
            let value = if m == LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX {
                crate::prompt::DEFAULT_CONTEXT_PREPEND.to_string()
            } else {
                m
            };
            root.insert("prompt".to_string(), serde_json::Value::String(value));
        }
    }

    root.remove("prompt_prefix");
    root.remove("prompt_suffix");
    true
}

/// Layer the built-in harness defaults underneath the user's on-disk
/// config so fields the user has not explicitly set come from
/// [`Config::default`] rather than from `serde(default)`'s zero-value
/// fallback.
///
/// Returns a list of `(harness_name, field_name)` pairs that were filled
/// in, so callers (e.g. `ralph init`) can report exactly what was added
/// before persisting the merged config back to disk. The load path
/// discards this list and silently applies the merge in-memory; init
/// uses it to print a one-line summary per added field.
///
/// This protects users from silent breakage when new fields are added to
/// `HarnessConfig` in code: a config written by an older ralph that
/// predates `model_args` or `argv_overflow` continues to work, because
/// missing keys are filled from the current built-in default at load time.
///
/// The merge is JSON-level on a `serde_json::Value`, which preserves the
/// crucial distinction between "key absent" and "key present with an
/// empty/null value" for recognized harness fields. User-set known keys,
/// including explicit `[]` arrays or `null` values, are never touched —
/// only known keys that don't appear in the user's object are filled in.
/// The config file is a closed schema: callers that persist the merged
/// result through [`Config`] do not preserve unknown JSON keys.
///
/// Only applied to harnesses whose *name* matches one of the built-ins
/// defined by [`Config::default`]. Custom-named harnesses are left alone
/// (no built-in default exists to layer against).
pub fn layer_builtin_harness_defaults(
    raw: &mut serde_json::Value,
) -> Result<Vec<(String, String)>> {
    let mut filled: Vec<(String, String)> = Vec::new();

    let Some(root) = raw.as_object_mut() else {
        return Ok(filled);
    };
    let Some(harnesses_value) = root.get_mut("harnesses") else {
        return Ok(filled);
    };
    let Some(user_harnesses) = harnesses_value.as_object_mut() else {
        return Ok(filled);
    };

    let defaults = Config::default();
    for (name, default_hc) in &defaults.harnesses {
        let Some(user_entry) = user_harnesses.get_mut(name) else {
            continue;
        };
        let Some(user_obj) = user_entry.as_object_mut() else {
            continue;
        };

        let default_value = serde_json::to_value(default_hc).with_context(|| {
            format!("failed to serialize built-in defaults for harness '{name}'")
        })?;
        let Some(default_obj) = default_value.as_object() else {
            continue;
        };
        for (k, v) in default_obj {
            if !user_obj.contains_key(k) {
                user_obj.insert(k.clone(), v.clone());
                filled.push((name.clone(), k.clone()));
            }
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_has_all_harnesses() {
        let config = Config::default();
        assert_eq!(config.default_harness, "claude");
        assert_eq!(config.max_retries_per_step, 3);
        assert_eq!(config.timeout_secs, None);
        // Auto-stash is default-on (real git stash + pop, not commit) so
        // `ralph run` preserves dirty working trees without the user needing
        // to opt in.
        assert!(config.auto_stash);

        let expected_harnesses = [
            "claude",
            "codex",
            "codex-orchestrator",
            "pi",
            "opencode",
            "copilot",
            "goose",
        ];
        for name in &expected_harnesses {
            assert!(
                config.harnesses.contains_key(*name),
                "Missing harness: {name}"
            );
        }
        assert_eq!(config.harnesses.len(), 7);
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
        assert!(claude.plan_args.contains(&"bypassPermissions".to_string()));
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
        // Default codex must run with workspace-write so steps can modify
        // files. Read-only (codex's default with no --sandbox) silently fails
        // on any implementation step.
        assert!(
            codex.args.contains(&"--sandbox".to_string()),
            "codex args must include --sandbox: {:?}",
            codex.args
        );
        assert!(
            codex.args.contains(&"workspace-write".to_string()),
            "codex args must use workspace-write sandbox: {:?}",
            codex.args
        );

        // codex-orchestrator is the danger-full-access variant for steps
        // that mutate state outside the workspace (e.g. review steps that
        // call `ralph step add`, which writes to the ralph DB outside cwd).
        let codex_orch = &config.harnesses["codex-orchestrator"];
        assert_eq!(codex_orch.command, "codex");
        assert_eq!(codex_orch.args[0], "exec");
        assert!(codex_orch.args.contains(&"--sandbox".to_string()));
        assert!(
            codex_orch.args.contains(&"danger-full-access".to_string()),
            "codex-orchestrator must use danger-full-access sandbox: {:?}",
            codex_orch.args
        );
        assert!(
            !codex_orch.args.contains(&"workspace-write".to_string()),
            "codex-orchestrator must not also pass workspace-write: {:?}",
            codex_orch.args
        );
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
    fn test_default_harnesses_have_no_footguns() {
        let config = Config::default();
        for (name, hc) in &config.harnesses {
            let issues = harness_footguns(name, hc);
            assert!(
                issues.is_empty(),
                "default harness `{name}` must not trigger footguns, got: {issues:?}"
            );
        }
    }

    #[test]
    fn test_footguns_flag_codex_without_sandbox() {
        let mut hc = Config::default().harnesses["codex"].clone();
        hc.args
            .retain(|a| a != "--sandbox" && a != "workspace-write");
        let issues = harness_footguns("codex", &hc);
        assert_eq!(
            issues.len(),
            1,
            "codex without --sandbox must produce exactly one footgun warning: {issues:?}"
        );
        assert!(
            issues[0].contains("--sandbox"),
            "warning should mention --sandbox: {issues:?}"
        );
    }

    /// Regression guard: codex-orchestrator ships `--sandbox danger-full-access`,
    /// which is a different value but still satisfies the "must have --sandbox"
    /// rule. If a future footgun rule mistakenly flags any non-`workspace-write`
    /// sandbox, this catches it. Pinning the orchestrator separately also
    /// protects against accidentally extending the rule to "must be
    /// workspace-write" — orchestrator deliberately needs broader access.
    #[test]
    fn test_footguns_codex_orchestrator_is_clean() {
        let hc = Config::default().harnesses["codex-orchestrator"].clone();
        let issues = harness_footguns("codex-orchestrator", &hc);
        assert!(
            issues.is_empty(),
            "codex-orchestrator must not trigger footgun warnings: {issues:?}"
        );
    }

    #[test]
    fn test_footguns_flag_claude_without_permission_mode() {
        let mut hc = Config::default().harnesses["claude"].clone();
        // Strip both the flag and its value so the heuristic sees no
        // --permission-mode at all.
        hc.args
            .retain(|a| a != "--permission-mode" && a != "bypassPermissions");
        let issues = harness_footguns("claude", &hc);
        assert_eq!(
            issues.len(),
            1,
            "claude -p without --permission-mode must warn: {issues:?}"
        );
        assert!(
            issues[0].contains("--permission-mode"),
            "warning should mention --permission-mode: {issues:?}"
        );
    }

    #[test]
    fn test_safety_summary_reports_sandbox_value() {
        let config = Config::default();
        assert_eq!(
            harness_safety_summary(&config.harnesses["codex"]),
            "workspace-write"
        );
        assert_eq!(
            harness_safety_summary(&config.harnesses["codex-orchestrator"]),
            "danger-full-access"
        );
        assert_eq!(
            harness_safety_summary(&config.harnesses["claude"]),
            "bypassPermissions"
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
    fn test_load_or_create_config_at_is_toctou_safe() {
        // L22: simultaneous callers must end with exactly one config file
        // and no errors — a naive exists()/write() pair lets the last
        // writer clobber an earlier writer's contents.
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("ralph-rs");
        let path = dir.join("config.json");

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|s| {
            for _ in 0..8 {
                let dir = dir.clone();
                let path = path.clone();
                let barrier = barrier.clone();
                s.spawn(move || {
                    barrier.wait();
                    load_or_create_config_at(&dir, &path)
                        .expect("concurrent load_or_create_config_at must not fail");
                });
            }
        });

        assert!(path.exists(), "config.json must exist after contention");
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .expect("read_dir")
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "exactly one file should exist, got: {:?}",
            entries.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );
        let contents = std::fs::read_to_string(&path).expect("read");
        let _: Config = serde_json::from_str(&contents).expect("parse");
    }

    #[test]
    fn test_validate_rejects_missing_default_harness() {
        let config = Config {
            default_harness: "nope".to_string(),
            ..Default::default()
        };
        let err = config
            .validate()
            .expect_err("validate must reject missing harness");
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
        let config = Config {
            default_harness: String::new(),
            ..Default::default()
        };
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
    fn test_auto_stash_defaults_to_true_when_missing_in_json() {
        // Configs written before the key existed must keep working and
        // default to the new stash-and-pop behavior (opt-out via
        // --no-auto-stash or `auto_stash: false`).
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "timeout_secs": 0,
            "harnesses": {
                "claude": {"command": "claude"}
            }
        }"#;
        let config: Config = serde_json::from_str(json).expect("deserialize");
        assert!(config.auto_stash);
    }

    #[test]
    fn test_timeout_secs_deserializes_zero_and_null_as_none() {
        // L23: legacy configs wrote `"timeout_secs": 0` to disable the
        // timeout. Keep that working alongside the new `null`/missing forms,
        // and preserve positive values as-is.
        let base = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "harnesses": {"claude": {"command": "claude"}}
        }"#;

        // Legacy zero — stays disabled.
        let legacy_zero = base.replacen(
            "\"max_retries_per_step\": 3",
            "\"max_retries_per_step\": 3, \"timeout_secs\": 0",
            1,
        );
        let c: Config = serde_json::from_str(&legacy_zero).expect("legacy zero");
        assert_eq!(c.timeout_secs, None);

        // Explicit null — disabled.
        let explicit_null = base.replacen(
            "\"max_retries_per_step\": 3",
            "\"max_retries_per_step\": 3, \"timeout_secs\": null",
            1,
        );
        let c: Config = serde_json::from_str(&explicit_null).expect("explicit null");
        assert_eq!(c.timeout_secs, None);

        // Missing entirely — disabled (serde default).
        let c: Config = serde_json::from_str(base).expect("missing field");
        assert_eq!(c.timeout_secs, None);

        // Positive value — preserved.
        let positive = base.replacen(
            "\"max_retries_per_step\": 3",
            "\"max_retries_per_step\": 3, \"timeout_secs\": 600",
            1,
        );
        let c: Config = serde_json::from_str(&positive).expect("positive");
        assert_eq!(c.timeout_secs, Some(600));

        // Round-trip Some(n) through JSON.
        let mut cfg = Config {
            timeout_secs: Some(42),
            ..Default::default()
        };
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.timeout_secs, Some(42));

        // Round-trip None through JSON.
        cfg.timeout_secs = None;
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.timeout_secs, None);
    }

    #[test]
    fn test_min_free_disk_mb_default_is_1024() {
        // Default should guard against SQLITE_FULL crashes at 1 GB.
        let config = Config::default();
        assert_eq!(config.min_free_disk_mb, 1024);

        // Missing from JSON should also default to 1024.
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "harnesses": {"claude": {"command": "claude"}}
        }"#;
        let loaded: Config = serde_json::from_str(json).expect("deserialize");
        assert_eq!(loaded.min_free_disk_mb, 1024);
    }

    #[test]
    fn test_min_free_disk_mb_round_trips() {
        let config = Config {
            min_free_disk_mb: 2048,
            ..Config::default()
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let back: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.min_free_disk_mb, 2048);
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
        assert!(harness.color.is_none());
    }

    // -- display_timezone ---------------------------------------------------

    #[test]
    fn test_default_timezone_is_utc() {
        let config = Config::default();
        assert_eq!(config.display_timezone, "UTC");
    }

    #[test]
    fn test_display_timezone_missing_in_json_defaults_to_utc() {
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "harnesses": {"claude": {"command": "claude"}}
        }"#;
        let c: Config = serde_json::from_str(json).expect("deserialize");
        assert_eq!(c.display_timezone, "UTC");
    }

    #[test]
    fn test_invalid_timezone_fails_to_load() {
        let config = Config {
            display_timezone: "Not/A_Real_Zone".to_string(),
            ..Default::default()
        };
        let err = config
            .validate()
            .expect_err("validate must reject invalid IANA timezone");
        let msg = format!("{err}");
        assert!(msg.contains("display_timezone"), "{msg}");
        assert!(msg.contains("Not/A_Real_Zone"), "{msg}");
    }

    #[test]
    fn test_valid_custom_timezone_passes() {
        let config = Config {
            display_timezone: "America/New_York".to_string(),
            ..Default::default()
        };
        config
            .validate()
            .expect("America/New_York is a valid IANA name");
    }

    // -- harness_chunk_max_bytes -------------------------------------------

    #[test]
    fn test_harness_chunk_max_bytes_default_is_4096() {
        let config = Config::default();
        assert_eq!(config.harness_chunk_max_bytes, 4096);

        // Missing from JSON should also default to 4096 (configs written
        // before this field existed must keep loading).
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "harnesses": {"claude": {"command": "claude"}}
        }"#;
        let loaded: Config = serde_json::from_str(json).expect("deserialize");
        assert_eq!(loaded.harness_chunk_max_bytes, 4096);
    }

    #[test]
    fn test_harness_chunk_max_bytes_round_trips() {
        let config = Config {
            harness_chunk_max_bytes: 8192,
            ..Config::default()
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let back: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.harness_chunk_max_bytes, 8192);
    }

    // -- harness color override --------------------------------------------

    #[test]
    fn test_harness_color_override_hex_parsed() {
        // Valid hex override passes validation.
        let mut config = Config::default();
        config.harnesses.get_mut("claude").unwrap().color = Some("#abcdef".to_string());
        config.validate().expect("valid hex override must pass");

        // Invalid hex override fails validation with a clear error.
        let mut config = Config::default();
        config.harnesses.get_mut("claude").unwrap().color = Some("not-hex".to_string());
        let err = config
            .validate()
            .expect_err("invalid hex must fail validation");
        let msg = format!("{err}");
        assert!(msg.contains("claude"), "{msg}");
        assert!(msg.contains("color"), "{msg}");
    }

    #[test]
    fn test_harness_color_json_roundtrip() {
        let mut config = Config::default();
        config.harnesses.get_mut("claude").unwrap().color = Some("#112233".to_string());
        let json = serde_json::to_string(&config).unwrap();
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(back.harnesses["claude"].color.as_deref(), Some("#112233"));
    }

    // -- layered harness defaults (migration / drift protection) ---------

    /// Simulates the precise on-disk shape that bit the user: copilot
    /// configured by an older ralph that predates `prompt_input` and
    /// `argv_overflow`. The layered load MUST fill them in so the
    /// resulting in-memory `HarnessConfig` matches the current built-in
    /// default for delivery semantics, otherwise the run silently
    /// produces a path-as-prompt invocation.
    #[test]
    fn test_layered_load_fills_missing_copilot_delivery_fields() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("config.json");
        // Hand-write a config WITHOUT prompt_input or argv_overflow for
        // copilot. Everything else uses defaults.
        let raw = r#"{
            "default_harness": "copilot",
            "max_retries_per_step": 3,
            "harnesses": {
                "copilot": {
                    "command": "copilot",
                    "args": ["-p", "{prompt}", "--silent"]
                }
            }
        }"#;
        fs::write(&path, raw).unwrap();

        let cfg = read_and_validate(&path).expect("read_and_validate");
        let copilot = cfg.harnesses.get("copilot").expect("copilot present");

        // The fields the user did not set must be filled from the
        // current built-in default — NOT the enum/serde fallback.
        assert_eq!(
            copilot.prompt_input,
            PromptInputMode::Argv,
            "missing prompt_input must default to current built-in (Argv), not enum default (Stdin)"
        );
        assert_eq!(
            copilot.argv_overflow,
            ArgvOverflowBehavior::Error,
            "missing argv_overflow must default to current built-in (Error)"
        );
        // model_args is the one the original agent request was about —
        // an older config without this field must still get the model
        // flag template wired up.
        assert_eq!(copilot.model_args, vec!["--model={model}".to_string()]);
        // The user's explicit args override must be preserved verbatim.
        assert_eq!(
            copilot.args,
            vec![
                "-p".to_string(),
                "{prompt}".to_string(),
                "--silent".to_string()
            ]
        );
    }

    #[test]
    fn test_layered_load_preserves_explicit_empty_array() {
        // Critical semantic: an explicit `[]` is "user opted out", not
        // "missing". The merger must NOT overwrite it with defaults.
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("config.json");
        let raw = r#"{
            "default_harness": "copilot",
            "max_retries_per_step": 3,
            "harnesses": {
                "copilot": {
                    "command": "copilot",
                    "args": ["-p", "{prompt}"],
                    "model_args": []
                }
            }
        }"#;
        fs::write(&path, raw).unwrap();

        let cfg = read_and_validate(&path).expect("read_and_validate");
        let copilot = cfg.harnesses.get("copilot").expect("copilot present");
        assert!(
            copilot.model_args.is_empty(),
            "explicit empty `model_args: []` must NOT be overwritten with defaults; got {:?}",
            copilot.model_args
        );
    }

    #[test]
    fn test_layered_load_leaves_custom_harnesses_untouched() {
        // A custom-named harness has no built-in to layer against — the
        // merger must not touch it, leaving missing fields to serde's
        // zero-value defaults.
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("config.json");
        let raw = r#"{
            "default_harness": "my-custom",
            "max_retries_per_step": 3,
            "harnesses": {
                "my-custom": {
                    "command": "echo",
                    "args": ["{prompt}"]
                }
            }
        }"#;
        fs::write(&path, raw).unwrap();

        let cfg = read_and_validate(&path).expect("read_and_validate");
        let custom = cfg.harnesses.get("my-custom").expect("present");
        // No layered fill → serde defaults apply: empty model_args,
        // Stdin prompt_input, etc.
        assert_eq!(custom.command, "echo");
        assert!(custom.model_args.is_empty());
        assert_eq!(custom.prompt_input, PromptInputMode::Stdin);
    }

    #[test]
    fn test_layer_function_reports_filled_fields() {
        // The merger returns a list of (harness, field) it added — used
        // by `ralph init` to print a one-line audit per addition. Sparse
        // copilot entry should report at least the missing delivery
        // fields and model_args.
        let mut raw: serde_json::Value = serde_json::from_str(
            r#"{
                "default_harness": "copilot",
                "max_retries_per_step": 3,
                "harnesses": {
                    "copilot": {
                        "command": "copilot"
                    }
                }
            }"#,
        )
        .unwrap();

        let filled = layer_builtin_harness_defaults(&mut raw).unwrap();
        let copilot_fields: Vec<&str> = filled
            .iter()
            .filter(|(h, _)| h == "copilot")
            .map(|(_, f)| f.as_str())
            .collect();

        for expected in &["prompt_input", "argv_overflow", "model_args", "args"] {
            assert!(
                copilot_fields.contains(expected),
                "expected `{expected}` in filled fields, got {copilot_fields:?}"
            );
        }
    }

    // -- compatibility warnings -------------------------------------------

    #[test]
    fn test_compat_warns_on_copilot_with_non_argv_prompt_input() {
        let mut hc = Config::default().harnesses["copilot"].clone();
        hc.prompt_input = PromptInputMode::TempFile;
        let issues = harness_compatibility_warnings("copilot", &hc);
        assert!(
            issues
                .iter()
                .any(|m| m.contains("Copilot CLI accepts prompts ONLY as inline argv")),
            "expected compat warning for copilot+tempfile, got: {issues:?}"
        );
    }

    #[test]
    fn test_compat_warns_on_copilot_with_spill_overflow() {
        // Argv + SpillToTempFile is wrong for copilot — the spill would
        // produce a path-as-prompt invocation.
        let mut hc = Config::default().harnesses["copilot"].clone();
        hc.argv_overflow = ArgvOverflowBehavior::SpillToTempFile;
        let issues = harness_compatibility_warnings("copilot", &hc);
        assert!(
            issues.iter().any(|m| m.contains("argv_overflow")),
            "expected argv_overflow compat warning, got: {issues:?}"
        );
    }

    #[test]
    fn test_compat_warns_on_missing_model_args() {
        // Any harness whose underlying CLI is in MODEL_CAPABLE_COMMANDS
        // and has empty model_args must warn — per-step `--model` is
        // silently dropped otherwise.
        for cmd in MODEL_CAPABLE_COMMANDS {
            let mut hc = HarnessConfig {
                command: (*cmd).to_string(),
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
                prompt_input: PromptInputMode::Argv,
                argv_overflow: ArgvOverflowBehavior::Error,
                color: None,
            };
            // For copilot, the prompt_input=Argv/Error combo above won't
            // trigger the copilot-specific warning; ensure only the
            // model_args warning fires by construction.
            if *cmd == "copilot" {
                hc.prompt_input = PromptInputMode::Argv;
                hc.argv_overflow = ArgvOverflowBehavior::Error;
            }
            let issues = harness_compatibility_warnings("test", &hc);
            assert!(
                issues
                    .iter()
                    .any(|m| m.contains("supports per-invocation model selection")),
                "command `{cmd}` with empty model_args must warn, got: {issues:?}"
            );
        }
    }

    #[test]
    fn test_compat_clean_on_default_config() {
        // The shipped Config::default() must produce zero compat warnings
        // — otherwise `ralph init` on a fresh machine would emit noise.
        for (name, hc) in &Config::default().harnesses {
            let issues = harness_compatibility_warnings(name, hc);
            assert!(
                issues.is_empty(),
                "default harness `{name}` must not trigger compat warnings, got: {issues:?}"
            );
        }
    }

    // -- save() round trip -------------------------------------------------

    #[test]
    fn test_save_round_trip() {
        // Exercise the atomic writer directly (not Config::save, which
        // uses the user's real config dir) — write a config to a tmp dir,
        // read it back, and verify equality.
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let path = dir.join("config.json");

        let config = Config {
            display_timezone: "America/New_York".to_string(),
            ..Config::default()
        };
        write_config_atomic(&dir, &path, &config).expect("write_config_atomic");

        let loaded = read_and_validate(&path).expect("read_and_validate");
        assert_eq!(loaded, config);
        assert_eq!(loaded.display_timezone, "America/New_York");
    }

    #[test]
    fn test_legacy_prompt_fields_migrate_and_rewrite_on_load() {
        // A config written by a pre-collapse ralph carries `prompt_prefix`
        // and `prompt_suffix`. On load they collapse into a single `prompt`
        // (joined by a blank line) and the file is rewritten without the old
        // keys so the next load is clean.
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_path_buf();
        let path = dir.join("config.json");

        let mut raw = serde_json::to_value(Config::default()).unwrap();
        let obj = raw.as_object_mut().unwrap();
        obj.remove("prompt");
        obj.insert("prompt_prefix".into(), serde_json::json!("PRE TEXT"));
        obj.insert("prompt_suffix".into(), serde_json::json!("SUF TEXT"));
        std::fs::write(&path, serde_json::to_string_pretty(&raw).unwrap()).unwrap();

        let loaded = read_and_validate(&path).expect("read_and_validate");
        assert_eq!(loaded.prompt.as_deref(), Some("PRE TEXT\n\nSUF TEXT"));

        // The on-disk file was rewritten in the collapsed shape.
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let on_disk = on_disk.as_object().unwrap();
        assert!(!on_disk.contains_key("prompt_prefix"));
        assert!(!on_disk.contains_key("prompt_suffix"));
        assert_eq!(
            on_disk.get("prompt").and_then(|v| v.as_str()),
            Some("PRE TEXT\n\nSUF TEXT")
        );

        // Second load is a no-op (no legacy keys left to migrate).
        let reloaded = read_and_validate(&path).expect("reload");
        assert_eq!(reloaded, loaded);
    }

    #[test]
    fn test_legacy_prompt_migration_skips_missing_sides() {
        // Only one of the two legacy fields present → `prompt` is exactly
        // that side, with no stray blank-line separator.
        let mut raw = serde_json::json!({ "prompt_prefix": "ONLY PREFIX" });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some("ONLY PREFIX")
        );
        assert!(raw.as_object().unwrap().get("prompt_prefix").is_none());

        // Both legacy keys present but null → no `prompt` synthesized, old
        // keys still stripped.
        let mut raw = serde_json::json!({
            "prompt_prefix": serde_json::Value::Null,
            "prompt_suffix": serde_json::Value::Null,
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert!(raw.get("prompt").is_none());
        assert!(raw.as_object().unwrap().get("prompt_suffix").is_none());

        // Neither legacy key present → not a migration.
        let mut raw = serde_json::json!({ "prompt": "already new" });
        assert!(!migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some("already new")
        );
    }

    #[test]
    fn test_legacy_prompt_migration_keeps_existing_new_field() {
        // If both the new `prompt` and a stale legacy key are present, the
        // new value wins and the legacy key is dropped.
        let mut raw = serde_json::json!({
            "prompt": "NEW WINS",
            "prompt_prefix": "STALE",
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(raw.get("prompt").and_then(|v| v.as_str()), Some("NEW WINS"));
        assert!(raw.as_object().unwrap().get("prompt_prefix").is_none());
    }

    #[test]
    fn test_legacy_default_prefix_migrates_to_context_prepend() {
        // A pre-overhaul DEFAULT user had `prompt_prefix` == the old short
        // one-liner and no suffix, while the full introspection block was
        // auto-injected at runtime (since removed). Migrating that config
        // must re-seed the canonical block, not collapse to the dead
        // one-liner — otherwise the introspection guidance is lost forever.
        let mut raw = serde_json::json!({
            "prompt_prefix": LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX,
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some(crate::prompt::DEFAULT_CONTEXT_PREPEND)
        );

        // Same when the legacy default prefix is present with an explicit
        // null/empty suffix (the only other shape `ralph init` ever wrote).
        let mut raw = serde_json::json!({
            "prompt_prefix": LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX,
            "prompt_suffix": serde_json::Value::Null,
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some(crate::prompt::DEFAULT_CONTEXT_PREPEND)
        );

        // And the legacy literal really does contain the substring the
        // doctor check keys on, proving the doctor check can't tell the
        // dead one-liner apart from the real block (the reason this guard
        // has to live in the migration path).
        assert!(LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX.contains("ralph status"));
    }

    #[test]
    fn test_legacy_customized_prefix_is_preserved_verbatim() {
        // A user who customized the global prefix (anything not byte-equal
        // to the legacy default) must keep their exact text — the data-loss
        // guard is gated strictly on equality so it never clobbers a
        // customization.
        let mut raw = serde_json::json!({
            "prompt_prefix": "My very own house style. Run `ralph status`.",
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some("My very own house style. Run `ralph status`.")
        );

        // Legacy default prefix PLUS a real custom suffix → not the
        // uncustomized-default shape, so it's preserved (joined) verbatim,
        // never re-seeded.
        let mut raw = serde_json::json!({
            "prompt_prefix": LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX,
            "prompt_suffix": "Always run the linter.",
        });
        assert!(migrate_legacy_prompt_fields(&mut raw));
        assert_eq!(
            raw.get("prompt").and_then(|v| v.as_str()),
            Some(
                format!("{LEGACY_DEFAULT_GLOBAL_PROMPT_PREFIX}\n\nAlways run the linter.").as_str()
            )
        );
    }

    // -----------------------------------------------------------------
    // Review config + effective_review_enabled (docs/dag-redesign.md §6)
    // -----------------------------------------------------------------

    #[test]
    fn test_review_config_absent_in_json_defaults_off() {
        // A config file written before the review feature has no `"review"`
        // key at all. `#[serde(default)]` must backfill `ReviewConfig`'s
        // default so existing configs keep loading unchanged: review off,
        // no review harness/model.
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "harnesses": {}
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.review, ReviewConfig::default());
        assert_eq!(cfg.review.enabled, None);
        assert_eq!(cfg.review.harness, "");
        assert_eq!(cfg.review.model, "");
    }

    #[test]
    fn test_review_config_round_trips() {
        // The §6 example block loads and serializes back faithfully.
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "review": { "enabled": true, "harness": "codex", "model": "gpt-5-codex" },
            "harnesses": {}
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.review.enabled, Some(true));
        assert_eq!(cfg.review.harness, "codex");
        assert_eq!(cfg.review.model, "gpt-5-codex");

        let round: Config =
            serde_json::from_str(&serde_json::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(round.review, cfg.review);
    }

    #[test]
    fn test_review_config_partial_block_uses_field_defaults() {
        // A `"review"` block that only sets `enabled` leaves harness/model
        // at their empty-string defaults (every field is `#[serde(default)]`).
        let json = r#"{
            "default_harness": "claude",
            "max_retries_per_step": 3,
            "review": { "enabled": false },
            "harnesses": {}
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.review.enabled, Some(false));
        assert_eq!(cfg.review.harness, "");
        assert_eq!(cfg.review.model, "");
    }

    /// Minimal `Plan` carrying only the `review_enabled` override under test;
    /// all other fields are inert defaults.
    fn review_plan(review_enabled: Option<bool>) -> crate::plan::Plan {
        let now = chrono::Utc::now();
        crate::plan::Plan {
            id: "p1".into(),
            slug: "p".into(),
            project: "/proj".into(),
            branch_name: "b".into(),
            description: "d".into(),
            status: crate::plan::PlanStatus::Planning,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: now,
            updated_at: now,
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled,
            squash_on_complete: false,
        }
    }

    /// Minimal `Step` carrying only the `review_enabled` override under test.
    fn review_step(review_enabled: Option<bool>) -> crate::plan::Step {
        let now = chrono::Utc::now();
        crate::plan::Step {
            id: "s1".into(),
            short_id: String::new(),
            plan_id: "p1".into(),
            sort_key: "a0".into(),
            title: "t".into(),
            description: "d".into(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: crate::plan::StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: now,
            updated_at: now,
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            retry_strategy: None,
            review_enabled,
            review_status: None,
            corrects_step_id: None,
        }
    }

    fn cfg_with_review(enabled: Option<bool>) -> Config {
        let mut c = Config::default();
        c.review.enabled = enabled;
        c
    }

    #[test]
    fn test_effective_review_enabled_precedence() {
        // Precedence is step > plan > config.review.enabled > false,
        // mirroring `Step::effective_retry_strategy` (step > plan >
        // default). Exercise EVERY combination of the three tri-state
        // levels (3^3 = 27) against the §6 chain
        // step ?? plan ?? global ?? false.
        for step in [None, Some(true), Some(false)] {
            for plan in [None, Some(true), Some(false)] {
                for global in [None, Some(true), Some(false)] {
                    let expected = step
                        .or(plan)
                        .or(global)
                        .unwrap_or(false);
                    let got = effective_review_enabled(
                        &review_step(step),
                        &review_plan(plan),
                        &cfg_with_review(global),
                    );
                    assert_eq!(
                        got, expected,
                        "step={step:?} plan={plan:?} global={global:?} \
                         must resolve to {expected} (step ?? plan ?? global ?? false)"
                    );
                }
            }
        }
    }

    #[test]
    fn test_effective_review_enabled_all_null_is_false() {
        // The spec's explicit bottom-of-chain case: nothing set anywhere
        // ⇒ review off.
        assert!(!effective_review_enabled(
            &review_step(None),
            &review_plan(None),
            &cfg_with_review(None),
        ));
    }

    #[test]
    fn test_effective_review_enabled_step_overrides_plan_and_global() {
        // A step `false` wins even when plan and global are both `true`.
        assert!(!effective_review_enabled(
            &review_step(Some(false)),
            &review_plan(Some(true)),
            &cfg_with_review(Some(true)),
        ));
        // A step `true` wins even when plan and global are both `false`.
        assert!(effective_review_enabled(
            &review_step(Some(true)),
            &review_plan(Some(false)),
            &cfg_with_review(Some(false)),
        ));
    }

    #[test]
    fn test_effective_review_enabled_plan_overrides_global() {
        // Step unset → plan decides over global.
        assert!(effective_review_enabled(
            &review_step(None),
            &review_plan(Some(true)),
            &cfg_with_review(Some(false)),
        ));
        assert!(!effective_review_enabled(
            &review_step(None),
            &review_plan(Some(false)),
            &cfg_with_review(Some(true)),
        ));
    }

    #[test]
    fn test_effective_review_enabled_falls_through_to_global() {
        // Step + plan unset → global default decides.
        assert!(effective_review_enabled(
            &review_step(None),
            &review_plan(None),
            &cfg_with_review(Some(true)),
        ));
        assert!(!effective_review_enabled(
            &review_step(None),
            &review_plan(None),
            &cfg_with_review(Some(false)),
        ));
    }
}
