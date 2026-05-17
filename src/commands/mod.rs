// Plan and step CLI command implementations
//
// This module is split into per-area submodules. Shared helpers live here;
// each submodule re-exports its public functions through this module.

mod agents;
pub mod config_cmd;
pub mod harness;
mod hooks;
pub mod interruption;
mod plan;
mod prompt;
pub mod question;
mod run;
mod step;

// Re-export all public command functions so callers can use `commands::*`.
pub use agents::*;
pub use hooks::*;
pub use plan::*;
pub use prompt::*;
pub use run::*;
pub use step::*;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::path::Path;

use crate::config;
use crate::db;
use crate::git;
use crate::output::{self, OutputContext};
use crate::plan::{Plan, Step};
use crate::preflight;
use crate::storage;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the project directory to a canonical absolute path string.
pub fn resolve_project(project: Option<&Path>) -> Result<String> {
    let dir = match project {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };
    let canonical = dir
        .canonicalize()
        .with_context(|| format!("Cannot resolve project path: {}", dir.display()))?;
    Ok(canonical.to_string_lossy().into_owned())
}

/// Resolve a plan from an optional slug: if provided, look it up; otherwise
/// find the active plan for the project. `include_complete` controls whether
/// completed plans count as "active" (useful for status/log).
pub fn resolve_plan(
    conn: &Connection,
    slug: Option<String>,
    project: &str,
    include_complete: bool,
) -> Result<Plan> {
    match slug {
        Some(s) if s.is_empty() => {
            bail!(
                "Plan slug cannot be empty. Specify a non-empty slug or omit the argument to use the active plan."
            )
        }
        Some(s) => storage::get_plan_by_slug(conn, &s, project)?
            .with_context(|| format!("Plan not found: {s}")),
        None => storage::find_active_plan(conn, project, include_complete)?
            .context("No active plan found. Specify a plan slug as a positional argument."),
    }
}

/// Resolve the plan to resume.
///
/// When `slug` is provided, behaves like [`resolve_plan`] (exact lookup).
/// When `slug` is absent, picks the resumable plan for the current git
/// branch:
///
/// 1. `storage::find_resumable_plans_for_branch(current_branch)` —
///    matches plans whose recorded `last_run_branch` equals the current
///    branch (or whose `branch_name` equals it AND `last_run_branch IS
///    NULL`, covering never-run plans).
///    * 1 hit: use it.
///    * 2+ hits: most recent (already DESC), warn on stderr listing the
///      others so the user knows to disambiguate next time.
///    * 0 hits: fall through.
/// 2. [`storage::find_resumable_plan`] — most-recent resumable plan in
///    the project regardless of branch, covering non-git workdirs,
///    detached HEAD, and branches that have never hosted a run.
///    Importantly, this includes `Aborted` plans (the runner accepts
///    them as resumable) — using `find_active_plan` here would silently
///    refuse to resume an aborted plan whose branch context was lost.
/// 3. Still nothing: error with both the branch and the resumable-plan hint.
///
/// The slug-collision defence is in step 1's SQL: `last_run_branch IS
/// NULL AND branch_name = ?` only fires when `last_run_branch` was never
/// written — so a plan whose last run physically executed on `master`
/// will NOT match a freshly-checked-out feature branch that happens to
/// share its slug as a name.
pub fn resolve_resume_plan(
    conn: &Connection,
    slug: Option<String>,
    project: &str,
    workdir: &Path,
) -> Result<Plan> {
    if let Some(s) = slug {
        if s.is_empty() {
            bail!(
                "Plan slug cannot be empty. Specify a non-empty slug or omit the argument to use the active plan."
            );
        }
        return storage::get_plan_by_slug(conn, &s, project)?
            .with_context(|| format!("Plan not found: {s}"));
    }

    // Branch-based resolution. `git::get_current_branch` shells out; if
    // it fails (no git, detached HEAD, …) we skip the branch hop and fall
    // straight to `find_resumable_plan` so non-git contexts still work.
    let branch_result = git::get_current_branch(workdir);
    if let Ok(branch) = branch_result.as_ref() {
        let candidates = storage::find_resumable_plans_for_branch(conn, project, branch)?;
        match candidates.len() {
            0 => { /* fall through to find_resumable_plan */ }
            1 => return Ok(candidates.into_iter().next().unwrap()),
            _ => {
                let chosen = candidates[0].clone();
                let other_slugs: Vec<&str> = candidates.iter().map(|p| p.slug.as_str()).collect();
                eprintln!(
                    "Multiple resumable plans on '{}': {}. Resuming '{}'. Pass a slug to disambiguate.",
                    branch,
                    other_slugs.join(", "),
                    chosen.slug,
                );
                return Ok(chosen);
            }
        }
    }

    if let Some(p) = storage::find_resumable_plan(conn, project)? {
        return Ok(p);
    }

    match branch_result {
        Ok(branch) => bail!(
            "No resumable plan found for branch '{branch}' or in this project. Specify a slug."
        ),
        Err(_) => bail!(
            "No resumable plan found in this project. Specify a plan slug as a positional argument."
        ),
    }
}

/// Resolve a step reference from the two shared selector forms.
///
/// A step can be named two ways on the CLI:
///
/// * the positional selector `<num|short_id>` (`step_sel`), and
/// * the `--step-id <uuid>` flag (`step_id`).
///
/// Exactly one must be `Some`; clap's `conflicts_with` guarantees they are
/// mutually exclusive and this function rejects the "neither" case.
///
/// ## Positional selector disambiguation (docs/dag-redesign.md §7)
///
/// Every `<num>` selector also accepts a step `short_id`. The rule is
/// deterministic, with the short-id branch requiring an *actual* match so
/// it can never shadow a number:
///
/// 1. If the token is **exactly [`storage::is_short_id_shaped`]-shaped**
///    (8 base-62 chars) **and equals the `short_id` of some step in this
///    plan**, it resolves as that step.
/// 2. Otherwise the token is parsed as a **1-based step number** (range
///    error if out of bounds, parse error if non-numeric).
///
/// Because branch 1 fires only on a concrete match, a purely numeric token
/// keeps its historical numeric meaning. An 8-digit numeric like
/// `"00000001"` is short-id-*shaped* but, absent a step whose short_id is
/// literally that string, falls through to the numeric branch and parses
/// as `1` — so linear-plan behavior stays byte-identical (minted short_ids
/// are random 8-char base-62 strings; a collision with a literal position
/// string is both astronomically unlikely and still resolves to the same
/// step the number would have).
///
/// Returns `(step, step_display_num)` where `step_display_num` is the
/// 1-based position in the plan's step list (used for user-facing messages).
pub fn resolve_step(
    conn: &Connection,
    plan_id: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
) -> Result<(Step, usize)> {
    let steps = storage::list_steps(conn, plan_id)?;

    match (step_sel, step_id) {
        (Some(tok), None) => {
            // 1. short_id form: correctly shaped AND an existing match in
            //    this plan. Requiring a hit means a coincidentally
            //    8-char-numeric token still parses as a number below.
            if storage::is_short_id_shaped(tok)
                && let Some(idx) = steps.iter().position(|s| s.short_id == tok)
            {
                return Ok((steps.into_iter().nth(idx).unwrap(), idx + 1));
            }
            // 2. numeric form (1-based).
            let num: usize = tok.parse().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid step selector '{tok}': expected a 1-based step number \
                     or an 8-character short id"
                )
            })?;
            if num == 0 || num > steps.len() {
                bail!(
                    "Step {} is out of range (plan has {} steps)",
                    num,
                    steps.len()
                );
            }
            Ok((steps.into_iter().nth(num - 1).unwrap(), num))
        }
        (None, Some(id)) => {
            let step = storage::get_step_by_id(conn, id)?
                .with_context(|| format!("Step not found with id: {id}"))?;
            // Ensure the step belongs to this plan.
            if step.plan_id != plan_id {
                bail!("Step {id} does not belong to this plan");
            }
            // Find the 1-based position for display.
            let pos = steps
                .iter()
                .position(|s| s.id == step.id)
                .map(|i| i + 1)
                .unwrap_or(0);
            Ok((step, pos))
        }
        (None, None) => {
            bail!("Provide either a step number/short id or --step-id");
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap conflicts_with, but guard anyway.
            bail!("Cannot specify both a step number/short id and --step-id");
        }
    }
}

// ---------------------------------------------------------------------------
// Init command
// ---------------------------------------------------------------------------

/// Options controlling the `init` command flow.
#[derive(Debug, Default, Clone)]
pub struct InitOptions {
    /// Skip interactive prompting. Used in CI / scripted setup.
    pub non_interactive: bool,
    /// Explicitly pre-select the default harness. Skips prompting.
    pub default_harness: Option<String>,
    /// Overwrite an existing config file. Without this, an existing config
    /// is preserved and init is a no-op for the config itself.
    pub force: bool,
    /// Re-seed the global prompt (`config.prompt`) with
    /// [`crate::prompt::DEFAULT_CONTEXT_PREPEND`] unconditionally, even when
    /// the user has customized it. Without this, the global prompt is only
    /// seeded when it is missing or blank.
    pub restore_prompts: bool,
}

pub fn cmd_init(opts: &InitOptions, out: &OutputContext) -> Result<()> {
    use std::fs;
    use std::io::IsTerminal;

    let icon = output::check_icon(out.color);

    // 1. Create directories (idempotent).
    let config_dir = config::config_dir()?;
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create config directory {}", config_dir.display()))?;
    eprintln!("{icon} Config directory: {}", config_dir.display());

    let agents_dir = config::agents_dir()?;
    fs::create_dir_all(&agents_dir)
        .with_context(|| format!("Failed to create agents directory {}", agents_dir.display()))?;
    eprintln!("{icon} Agents directory: {}", agents_dir.display());

    // 2. Build the default config so we can scan its harnesses regardless
    //    of whether we end up writing it to disk.
    let mut new_config = config::Config::default();

    // 3. Detect which harnesses are currently installed on PATH. We report
    //    this every run so `ralph init` doubles as a quick "what's available"
    //    check, even if we're not rewriting the config.
    let availability = detect_harnesses(&new_config);
    print_harness_availability(&availability, out);

    // 4. Decide whether to write a config file.
    let config_path = config_dir.join("config.json");
    let config_exists = config_path.exists();

    if config_exists && !opts.force {
        // Schema migration: layer built-in harness defaults under the
        // user's on-disk config and persist if any recognized fields were
        // added. Explicit values for known config keys are preserved;
        // unknown JSON keys are outside the closed config schema and are
        // not preserved when the typed Config is rewritten. `--force` is
        // reserved for the full default-config rewrite path below.
        migrate_existing_config(&config_path, out)?;
    } else {
        // Pick the default harness: explicit flag > interactive prompt >
        // first-available fallback > hard default ("claude").
        let chosen = choose_default_harness(opts, &availability)?;
        new_config.default_harness = chosen.clone();

        let json = serde_json::to_string_pretty(&new_config)?;
        fs::write(&config_path, &json)
            .with_context(|| format!("Failed to write config to {}", config_path.display()))?;

        let verb = if config_exists { "Rewrote" } else { "Wrote" };
        eprintln!(
            "{icon} {verb} config: {} (default harness: {chosen})",
            config_path.display()
        );
    }

    // 4b. Seed the global prompt. `ralph init` is the canonical source of
    //     ralph's built-in introspection block (`DEFAULT_CONTEXT_PREPEND`):
    //     a fresh or blank `config.prompt` is filled with it, and
    //     `--restore-prompts` re-seeds it unconditionally even over a
    //     user-customized value. An existing non-empty custom prompt is
    //     otherwise never touched. Persisted via the same atomic
    //     tmp-file + rename path as every other config mutation.
    seed_global_prompt(&config_dir, opts.restore_prompts, out)?;

    // 5. Initialize database (idempotent — `db::open` runs migrations).
    let _conn = db::open()?;
    let db_path = db::db_path()?;
    eprintln!("{icon} Database: {}", db_path.display());

    eprintln!();
    eprintln!("ralph initialized successfully.");

    // Hint about non-interactive mode when stdin isn't a TTY and we had to
    // silently fall back, so users notice why no prompt appeared.
    if !std::io::stdin().is_terminal() && !opts.non_interactive && opts.default_harness.is_none() {
        eprintln!();
        eprintln!(
            "  note: stdin is not a TTY — skipped interactive harness prompt. \
             Pass --default-harness <name> or edit {} to change the default.",
            config_path.display()
        );
    }

    Ok(())
}

/// Schema migration for an existing on-disk `config.json`: layer in any
/// recognized fields that are missing from built-in harnesses, persist the
/// merged typed config, and report each addition so the user can audit.
/// Never changes a recognized key the user has explicitly set, including
/// explicitly empty arrays or `null` values — only adds known keys that
/// aren't present.
///
/// Writes via the same tmp-file + rename atomic published in
/// `Config::save_at`, so concurrent readers never observe a partially
/// written file and a crash mid-write leaves the original intact.
fn migrate_existing_config(config_path: &Path, out: &OutputContext) -> Result<()> {
    use std::fs;

    let icon = output::check_icon(out.color);
    let warn = output::severity_icon("warning", out.color);

    let contents = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read {}", config_path.display()))?;
    let mut raw: serde_json::Value = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse {}", config_path.display()))?;

    let filled = config::layer_builtin_harness_defaults(&mut raw).with_context(|| {
        format!(
            "Failed to layer built-in harness defaults for {}",
            config_path.display()
        )
    })?;

    if filled.is_empty() {
        eprintln!(
            "{icon} Config up to date: {} (no missing built-in fields)",
            config_path.display()
        );
        return Ok(());
    }

    // Round-trip through Config to catch any post-merge validation failures
    // BEFORE persisting. If the user's existing config is broken in a way
    // that defeat the merge (e.g. invalid timezone), we want to surface
    // that without overwriting the file.
    let merged: config::Config = serde_json::from_value(raw.clone()).with_context(|| {
        format!(
            "Merged config failed to deserialize for {}",
            config_path.display()
        )
    })?;
    merged.validate().with_context(|| {
        format!(
            "Merged config failed validation for {}",
            config_path.display()
        )
    })?;

    // Persist via the canonical save path so we get the atomic
    // tmp-file + rename treatment, formatted identically to a fresh
    // `ralph init`.
    let dir = config_path
        .parent()
        .with_context(|| format!("Config path has no parent: {}", config_path.display()))?;
    merged.save_at(dir).with_context(|| {
        format!(
            "Failed to write merged config back to {}",
            config_path.display()
        )
    })?;

    // Group the report by harness so a config that's missing many fields
    // doesn't print one line per field.
    use std::collections::BTreeMap;
    let mut by_harness: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (harness, field) in &filled {
        by_harness
            .entry(harness.clone())
            .or_default()
            .push(field.clone());
    }

    eprintln!(
        "{warn} Migrated config: filled in {} missing field(s) for built-in harnesses.",
        filled.len()
    );
    for (harness, fields) in &by_harness {
        eprintln!("    {harness}: {}", fields.join(", "));
    }
    eprintln!("  Saved to: {}", config_path.display());

    Ok(())
}

/// Seed the global prompt (`config.prompt`) from
/// [`crate::prompt::DEFAULT_CONTEXT_PREPEND`].
///
/// `ralph init` is the canonical seed source for ralph's built-in
/// introspection block. The rule:
///
/// - `config.prompt` is `None` or empty/whitespace-only → seed it.
/// - `restore_prompts` is `true` → seed it unconditionally, overwriting any
///   existing customization.
/// - Otherwise → leave an existing non-empty custom prompt untouched.
///
/// When a write is needed it goes through `Config::save_at`, which uses the
/// same atomic tmp-file + rename as every other config mutation, so a
/// concurrent reader never observes a half-written file. A no-op (custom
/// prompt preserved, or already equal to the seed) does not rewrite the file.
fn seed_global_prompt(config_dir: &Path, restore_prompts: bool, out: &OutputContext) -> Result<()> {
    let icon = output::check_icon(out.color);

    // Load the config that `cmd_init` just created/migrated. This respects
    // the same `$XDG_CONFIG_HOME` that produced `config_dir`, so it reads the
    // file we wrote a few steps earlier.
    let mut cfg = config::load_or_create_config()
        .context("Failed to load config for global-prompt seeding")?;

    let is_blank = cfg
        .prompt
        .as_deref()
        .map(|p| p.trim().is_empty())
        .unwrap_or(true);

    if !restore_prompts && !is_blank {
        // Existing customization is preserved verbatim — never clobbered
        // without an explicit --restore-prompts.
        return Ok(());
    }

    let seed = crate::prompt::DEFAULT_CONTEXT_PREPEND.to_string();
    if cfg.prompt.as_deref() == Some(seed.as_str()) {
        // Already exactly the seed (e.g. a no-op re-run) — skip the write so
        // we don't churn the file's mtime.
        return Ok(());
    }

    cfg.prompt = Some(seed);
    cfg.save_at(config_dir)
        .context("Failed to persist seeded global prompt")?;

    if restore_prompts && !is_blank {
        eprintln!("{icon} Restored global prompt to ralph's built-in default.");
    } else {
        eprintln!("{icon} Seeded global prompt with ralph's built-in default.");
    }
    Ok(())
}

/// Probe each harness in the config for a binary on PATH. Returns pairs of
/// `(harness_name, installed)` sorted alphabetically for stable output.
fn detect_harnesses(config: &config::Config) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = config
        .harnesses
        .iter()
        .map(|(name, hc)| (name.clone(), preflight::is_binary_available(&hc.command)))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn print_harness_availability(availability: &[(String, bool)], out: &OutputContext) {
    let found: Vec<&str> = availability
        .iter()
        .filter_map(|(n, ok)| if *ok { Some(n.as_str()) } else { None })
        .collect();
    let missing: Vec<&str> = availability
        .iter()
        .filter_map(|(n, ok)| if !*ok { Some(n.as_str()) } else { None })
        .collect();

    let check = output::check_icon(out.color);
    let warn = output::severity_icon("warning", out.color);

    if found.is_empty() {
        eprintln!("{warn} No known harnesses found on PATH.");
    } else {
        eprintln!("{check} Harnesses found on PATH: {}", found.join(", "));
    }
    if !missing.is_empty() {
        eprintln!("  Not found: {}", missing.join(", "));
    }
}

/// Select which harness to record as the config default.
fn choose_default_harness(opts: &InitOptions, availability: &[(String, bool)]) -> Result<String> {
    use std::io::IsTerminal;

    // Explicit flag always wins and is validated against the known harness
    // list so typos fail loudly rather than writing a dead default.
    if let Some(name) = &opts.default_harness {
        let known: Vec<&str> = availability.iter().map(|(n, _)| n.as_str()).collect();
        if !known.contains(&name.as_str()) {
            bail!(
                "Unknown harness '{name}' passed to --default-harness. Known: {}",
                known.join(", ")
            );
        }
        return Ok(name.clone());
    }

    let installed: Vec<&str> = availability
        .iter()
        .filter_map(|(n, ok)| if *ok { Some(n.as_str()) } else { None })
        .collect();

    // Non-interactive or no TTY: pick the best available without asking.
    // Preference order: claude (historical default) > first installed >
    // fall back to "claude" even if missing, so the config is still valid
    // and the user can install claude later.
    if opts.non_interactive || !std::io::stdin().is_terminal() {
        if installed.contains(&"claude") {
            return Ok("claude".to_string());
        }
        if let Some(first) = installed.first() {
            return Ok((*first).to_string());
        }
        return Ok("claude".to_string());
    }

    // Interactive: prompt from the installed list. If nothing is installed,
    // fall back to claude and warn — the user might install it after init.
    if installed.is_empty() {
        eprintln!(
            "  No harnesses detected — defaulting to `claude`. \
             Install one (or edit config.json) before running plans."
        );
        return Ok("claude".to_string());
    }

    prompt_for_default(&installed)
}

/// Prompt the user to pick a default harness from the installed list.
/// Returns the chosen harness name. Re-prompts on invalid input up to 3x.
fn prompt_for_default(installed: &[&str]) -> Result<String> {
    use std::io::{BufRead, Write};

    // Suggest claude if it's present, otherwise the first entry.
    let suggested_idx = installed.iter().position(|n| *n == "claude").unwrap_or(0);

    eprintln!();
    eprintln!("Select a default harness:");
    for (i, name) in installed.iter().enumerate() {
        let marker = if i == suggested_idx { "*" } else { " " };
        eprintln!("  {marker} {}) {name}", i + 1);
    }

    let stdin = std::io::stdin();
    let mut handle = stdin.lock();
    let mut line = String::new();

    for _ in 0..3 {
        eprint!(
            "Choice [1-{}, default={}]: ",
            installed.len(),
            installed[suggested_idx]
        );
        std::io::stderr().flush().ok();

        line.clear();
        let n = handle
            .read_line(&mut line)
            .context("Failed to read from stdin")?;
        if n == 0 {
            // EOF — accept the suggestion silently.
            return Ok(installed[suggested_idx].to_string());
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(installed[suggested_idx].to_string());
        }

        // Accept either a 1-based number or a harness name.
        if let Ok(idx) = trimmed.parse::<usize>() {
            if idx >= 1 && idx <= installed.len() {
                return Ok(installed[idx - 1].to_string());
            }
        } else if installed.contains(&trimmed) {
            return Ok(trimmed.to_string());
        }

        eprintln!("  Invalid choice '{trimmed}'. Enter a number or harness name.");
    }

    bail!("No valid harness selection after 3 attempts");
}

// ---------------------------------------------------------------------------
// Doctor command
// ---------------------------------------------------------------------------

pub fn cmd_doctor(config: &config::Config, workdir: &Path, out: &OutputContext) -> Result<()> {
    println!("ralph doctor");
    println!();

    let checks = preflight::run_doctor_checks(config, workdir);

    let mut has_errors = false;
    for check in &checks {
        let severity_str = match check.severity {
            preflight::CheckSeverity::Pass => "pass",
            preflight::CheckSeverity::Warning => "warning",
            preflight::CheckSeverity::Error => {
                has_errors = true;
                "error"
            }
        };
        let icon = output::severity_icon(severity_str, out.color);
        println!("  {} {}: {}", icon, check.name, check.message);
    }

    println!();
    if has_errors {
        println!("Some checks failed. Please fix the issues above.");
    } else {
        println!("All checks passed.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::plan::{PlanStatus, StepStatus};

    use crate::output::{OutputContext, OutputFormat};

    fn setup() -> (Connection, String) {
        let conn = db::open_memory().expect("open_memory");
        let project = "/tmp/test-project".to_string();
        (conn, project)
    }

    // -- global-prompt seeding (`ralph init`) -----------------------------

    use std::sync::{Mutex, MutexGuard};

    /// Serialize tests that mutate `$XDG_CONFIG_HOME`. Same pattern as the
    /// config_cmd test module — a process-wide env var can't be raced.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct XdgGuard {
        _lock: MutexGuard<'static, ()>,
        prev: Option<std::ffi::OsString>,
    }
    impl Drop for XdgGuard {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => unsafe { std::env::set_var("XDG_CONFIG_HOME", v) },
                None => unsafe { std::env::remove_var("XDG_CONFIG_HOME") },
            }
        }
    }
    fn set_xdg(path: &Path) -> XdgGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("XDG_CONFIG_HOME");
        // SAFETY: guarded by ENV_LOCK for the duration of the returned guard.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", path) };
        XdgGuard { _lock: lock, prev }
    }

    #[test]
    fn test_seed_global_prompt_seeds_when_missing() {
        // Fresh config (Config::default has prompt: None) → seeded with the
        // built-in introspection block, which contains `ralph status`.
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());
        let config_dir = config::config_dir().expect("config_dir");

        // A default config has no prompt.
        let fresh = config::load_or_create_config().expect("load fresh");
        assert!(fresh.prompt.is_none(), "default config must have no prompt");

        seed_global_prompt(&config_dir, false, &test_out()).expect("seed ok");

        let reloaded = config::load_or_create_config().expect("reload");
        let prompt = reloaded.prompt.expect("prompt seeded");
        assert_eq!(prompt, crate::prompt::DEFAULT_CONTEXT_PREPEND);
        assert!(
            prompt.contains("ralph status"),
            "seeded prompt must contain the ralph-CLI hint"
        );
    }

    #[test]
    fn test_seed_global_prompt_seeds_when_blank() {
        // Whitespace-only `config.prompt` is treated as unset → seeded.
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());
        let config_dir = config::config_dir().expect("config_dir");

        let mut cfg = config::load_or_create_config().expect("load");
        cfg.prompt = Some("   \n\t  ".to_string());
        cfg.save_at(&config_dir).expect("save blank");

        seed_global_prompt(&config_dir, false, &test_out()).expect("seed ok");

        let reloaded = config::load_or_create_config().expect("reload");
        assert_eq!(
            reloaded.prompt.as_deref(),
            Some(crate::prompt::DEFAULT_CONTEXT_PREPEND)
        );
        assert!(reloaded.prompt.unwrap().contains("ralph status"));
    }

    #[test]
    fn test_seed_global_prompt_preserves_customization_without_flag() {
        // Re-running init WITHOUT --restore-prompts must never clobber a
        // user-customized prompt.
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());
        let config_dir = config::config_dir().expect("config_dir");

        let custom = "MY CUSTOM GLOBAL PROMPT — do not touch";
        let mut cfg = config::load_or_create_config().expect("load");
        cfg.prompt = Some(custom.to_string());
        cfg.save_at(&config_dir).expect("save custom");

        seed_global_prompt(&config_dir, false, &test_out()).expect("seed ok");

        let reloaded = config::load_or_create_config().expect("reload");
        assert_eq!(
            reloaded.prompt.as_deref(),
            Some(custom),
            "customization must be preserved without --restore-prompts"
        );
    }

    #[test]
    fn test_seed_global_prompt_restore_overwrites_customization() {
        // --restore-prompts re-seeds unconditionally, even over a
        // user-customized prompt.
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());
        let config_dir = config::config_dir().expect("config_dir");

        let custom = "MY CUSTOM GLOBAL PROMPT — should be replaced";
        let mut cfg = config::load_or_create_config().expect("load");
        cfg.prompt = Some(custom.to_string());
        cfg.save_at(&config_dir).expect("save custom");

        seed_global_prompt(&config_dir, true, &test_out()).expect("seed ok");

        let reloaded = config::load_or_create_config().expect("reload");
        let prompt = reloaded.prompt.expect("prompt present");
        assert_ne!(prompt, custom, "--restore-prompts must overwrite");
        assert_eq!(prompt, crate::prompt::DEFAULT_CONTEXT_PREPEND);
        assert!(prompt.contains("ralph status"));
    }

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    // -- init migration ---------------------------------------------------

    #[test]
    fn test_migrate_existing_config_fills_missing_copilot_fields() {
        // Simulates a config written by an older ralph that predates the
        // new fields. `migrate_existing_config` must (a) write the merged
        // result back atomically and (b) leave the user's explicit args
        // untouched.
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("config.json");

        let original = r#"{
            "default_harness": "copilot",
            "max_retries_per_step": 3,
            "harnesses": {
                "copilot": {
                    "command": "copilot",
                    "args": ["-p", "{prompt}", "--silent"]
                }
            }
        }"#;
        fs::write(&path, original).unwrap();

        migrate_existing_config(&path, &test_out()).expect("migration ok");

        // Re-read raw to confirm the merged keys are persisted on disk
        // (not just in-memory).
        let contents = fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        let copilot = &parsed["harnesses"]["copilot"];

        assert_eq!(
            copilot["prompt_input"].as_str(),
            Some("argv"),
            "prompt_input must be persisted as the built-in default"
        );
        assert_eq!(
            copilot["argv_overflow"].as_str(),
            Some("error"),
            "argv_overflow must be persisted as the built-in default"
        );
        assert!(
            copilot["model_args"].is_array(),
            "model_args must be persisted"
        );
        // The user's explicit args must NOT have been clobbered.
        assert_eq!(
            copilot["args"][0].as_str(),
            Some("-p"),
            "user args preserved"
        );
        assert_eq!(
            copilot["args"][2].as_str(),
            Some("--silent"),
            "user args preserved verbatim"
        );
    }

    #[test]
    fn test_migrate_existing_config_no_op_when_complete() {
        // If the on-disk config is already complete, migration must NOT
        // rewrite the file (idempotent — no spurious "modified" timestamps).
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("config.json");

        // Write a default config — every field is present.
        let cfg = crate::config::Config::default();
        let json = serde_json::to_string_pretty(&cfg).unwrap();
        fs::write(&path, &json).unwrap();

        let mtime_before = fs::metadata(&path).unwrap().modified().unwrap();
        // Small sleep so the FS timestamp would visibly change if a write
        // occurs — most filesystems have ms-level resolution.
        std::thread::sleep(std::time::Duration::from_millis(50));

        migrate_existing_config(&path, &test_out()).expect("migration ok");

        let mtime_after = fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "migration must not rewrite a complete config"
        );
    }

    #[test]
    fn test_plan_create_and_list() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            Some("A test plan"),
            Some("feat/test"),
            None,
            None,
            None,
            false,
            None,
            &["cargo build".to_string()],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(plan.slug, "my-plan");
        assert_eq!(plan.description, "A test plan");
        assert_eq!(plan.branch_name, "feat/test");
        assert_eq!(plan.deterministic_tests, vec!["cargo build"]);
        // No --retry-strategy given -> plan has no override (None).
        assert!(plan.retry_strategy.is_none());
    }

    #[test]
    fn test_plan_create_persists_retry_strategy() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "rs-plan",
            &project,
            None,
            None,
            None,
            None,
            Some(crate::plan::RetryStrategy::Rollback),
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "rs-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(
            plan.retry_strategy,
            Some(crate::plan::RetryStrategy::Rollback)
        );
    }

    #[test]
    fn test_plan_approve() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_approve(&conn, "my-plan", &project, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(plan.status, PlanStatus::Ready);
    }

    #[test]
    fn test_plan_approve_rejects_non_planning() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_approve(&conn, "my-plan", &project, &test_out()).unwrap();

        // Second approve should fail - plan is now ready, not planning
        let result = plan_approve(&conn, "my-plan", &project, &test_out());
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_delete_forced() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_delete(&conn, "my-plan", &project, true, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn test_step_add_and_list() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First step",
            Some("Do something"),
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Second step",
            Some("Do another thing"),
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].title, "First step");
        assert_eq!(steps[1].title, "Second step");
    }

    #[test]
    fn test_step_add_after() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Third",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        // Insert after position 1
        step_add(
            &conn,
            "my-plan",
            &project,
            "Second",
            None,
            Some(1),
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].title, "First");
        assert_eq!(steps[1].title, "Second");
        assert_eq!(steps[2].title, "Third");
    }

    #[test]
    fn test_step_add_with_criteria_and_max_retries() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        let criteria = vec!["Tests pass".to_string(), "No warnings".to_string()];
        step_add(
            &conn,
            "my-plan",
            &project,
            "Build it",
            None,
            None,
            None,
            None,
            None,
            &criteria,
            Some(5),
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].acceptance_criteria, criteria);
        assert_eq!(steps[0].max_retries, Some(5));
    }

    #[test]
    fn test_step_add_after_with_criteria() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        let criteria = vec!["Inserted check".to_string()];
        step_add(
            &conn,
            "my-plan",
            &project,
            "Inserted",
            None,
            Some(1),
            None,
            None,
            None,
            &criteria,
            Some(2),
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[1].title, "Inserted");
        assert_eq!(steps[1].acceptance_criteria, criteria);
        assert_eq!(steps[1].max_retries, Some(2));
    }

    #[test]
    fn test_step_remove_forced() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Second",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        step_remove(
            &conn,
            "my-plan",
            &project,
            Some("2"),
            None,
            true,
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].title, "First");
    }

    #[test]
    fn test_step_edit() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Old title",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        step_edit(
            &conn,
            "my-plan",
            &project,
            Some("1"),
            None,
            Some("New title"),
            Some("New desc"),
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            None,
            false,
            &[],
            false,
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "New title");
        assert_eq!(steps[0].description, "New desc");
    }

    #[test]
    fn test_step_reset() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Step",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        storage::update_step_status(&conn, &steps[0].id, StepStatus::Failed).unwrap();

        step_reset(
            &conn,
            "my-plan",
            &project,
            Some("1"),
            None,
            true,
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
        assert_eq!(steps[0].attempts, 0);
    }

    #[test]
    fn test_step_move() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "A",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "B",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "C",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        // Move step 3 (C) to position 1
        step_move(&conn, "my-plan", &project, Some("3"), None, 1, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "C");
        assert_eq!(steps[1].title, "A");
        assert_eq!(steps[2].title, "B");
    }

    #[test]
    fn test_step_move_to_end() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "A",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "B",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "C",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        // Move step 1 (A) to position 3
        step_move(&conn, "my-plan", &project, Some("1"), None, 3, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "B");
        assert_eq!(steps[1].title, "C");
        assert_eq!(steps[2].title, "A");
    }

    // -- plan dependency tests --

    #[test]
    fn test_plan_create_with_deps() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-b",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-c",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &["plan-a".to_string(), "plan-b".to_string()],
            &test_out(),
        )
        .unwrap();

        let c = storage::get_plan_by_slug(&conn, "plan-c", &project)
            .unwrap()
            .unwrap();
        let deps = storage::list_plan_dependencies(&conn, &c.id).unwrap();
        assert_eq!(deps.len(), 2);

        // Resolve the IDs back to slugs to confirm the correct plans were linked.
        let mut dep_slugs: Vec<String> = deps
            .iter()
            .map(|id| storage::get_plan_slug_by_id(&conn, id).unwrap().unwrap())
            .collect();
        dep_slugs.sort();
        assert_eq!(dep_slugs, vec!["plan-a".to_string(), "plan-b".to_string()]);
    }

    #[test]
    fn test_plan_create_with_missing_dep_errors() {
        let (conn, project) = setup();

        let result = plan_create(
            &conn,
            "plan-x",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &["nonexistent".to_string()],
            &test_out(),
        );
        assert!(result.is_err());

        // The plan should NOT have been created since we fail before insert.
        let p = storage::get_plan_by_slug(&conn, "plan-x", &project).unwrap();
        assert!(p.is_none());
    }

    #[test]
    fn test_plan_dependency_add_happy_path() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-b",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        plan_dependency_add(
            &conn,
            "plan-b",
            &project,
            &["plan-a".to_string()],
            &test_out(),
        )
        .unwrap();

        let b = storage::get_plan_by_slug(&conn, "plan-b", &project)
            .unwrap()
            .unwrap();
        let deps = storage::list_plan_dependencies(&conn, &b.id).unwrap();
        assert_eq!(deps.len(), 1);
    }

    #[test]
    fn test_plan_dependency_add_rejects_self_reference() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let result = plan_dependency_add(
            &conn,
            "plan-a",
            &project,
            &["plan-a".to_string()],
            &test_out(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_dependency_add_rejects_cycle() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-b",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        // a -> b is fine.
        plan_dependency_add(
            &conn,
            "plan-a",
            &project,
            &["plan-b".to_string()],
            &test_out(),
        )
        .unwrap();
        // b -> a would close a cycle and should error.
        let result = plan_dependency_add(
            &conn,
            "plan-b",
            &project,
            &["plan-a".to_string()],
            &test_out(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_dependency_remove() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-b",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &["plan-a".to_string()],
            &test_out(),
        )
        .unwrap();

        let b = storage::get_plan_by_slug(&conn, "plan-b", &project)
            .unwrap()
            .unwrap();
        assert_eq!(
            storage::list_plan_dependencies(&conn, &b.id).unwrap().len(),
            1
        );

        plan_dependency_remove(
            &conn,
            "plan-b",
            &project,
            &["plan-a".to_string()],
            &test_out(),
        )
        .unwrap();
        assert_eq!(
            storage::list_plan_dependencies(&conn, &b.id).unwrap().len(),
            0
        );
    }

    #[test]
    fn test_plan_dependency_list_resolves_both_directions() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "plan-a",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-b",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &["plan-a".to_string()],
            &test_out(),
        )
        .unwrap();
        plan_create(
            &conn,
            "plan-c",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &["plan-a".to_string()],
            &test_out(),
        )
        .unwrap();

        // plan-a has no deps but two dependents (b and c).
        let a = storage::get_plan_by_slug(&conn, "plan-a", &project)
            .unwrap()
            .unwrap();
        let a_deps = storage::list_plan_dependencies(&conn, &a.id).unwrap();
        let a_dependents = storage::list_dependent_plans(&conn, &a.id).unwrap();
        assert!(a_deps.is_empty());
        assert_eq!(a_dependents.len(), 2);

        // plan_dependency_list should run without error.
        plan_dependency_list(&conn, "plan-a", &project, &test_out()).unwrap();
        plan_dependency_list(&conn, "plan-b", &project, &test_out()).unwrap();
    }

    // -- step dependency add/remove/list (docs/dag-redesign.md §7) --

    #[test]
    fn test_step_dependency_add_remove_list_end_to_end() {
        let (conn, project) = setup();
        // `plan_with_steps` creates plan slug "sel" with `n` appended steps.
        let (_plan_id, sids) = plan_with_steps(&conn, &project, 3);

        // Add via a numeric selector (step 3) depending on a short_id
        // selector (step 1) — both forms must resolve through resolve_step.
        step_dependency_add(&conn, "sel", &project, "3", &[sids[0].clone()], &test_out()).unwrap();

        let s3 = resolve_step(&conn, &_plan_id, Some(sids[2].as_str()), None)
            .unwrap()
            .0;
        let s1 = resolve_step(&conn, &_plan_id, Some(sids[0].as_str()), None)
            .unwrap()
            .0;
        assert_eq!(
            storage::list_step_dependencies(&conn, &s3.id).unwrap(),
            vec![s1.id.clone()]
        );
        // Reverse edge is visible from the dependency's side.
        assert_eq!(
            storage::list_step_dependents(&conn, &s1.id).unwrap(),
            vec![s3.id.clone()]
        );

        // list runs without error in both human and JSON modes.
        step_dependency_list(&conn, "sel", &project, "3", &test_out()).unwrap();
        let mut json_out = test_out();
        json_out.format = OutputFormat::Json;
        step_dependency_list(&conn, "sel", &project, &sids[2], &json_out).unwrap();

        // Self-edge is rejected by the storage layer.
        assert!(
            step_dependency_add(&conn, "sel", &project, "1", &["1".to_string()], &test_out())
                .is_err()
        );

        // Remove drops the edge; a second remove is a harmless no-op.
        step_dependency_remove(
            &conn,
            "sel",
            &project,
            &sids[2],
            &[sids[0].clone()],
            &test_out(),
        )
        .unwrap();
        assert!(
            storage::list_step_dependencies(&conn, &s3.id)
                .unwrap()
                .is_empty()
        );
        step_dependency_remove(&conn, "sel", &project, "3", &["1".to_string()], &test_out())
            .unwrap();
    }

    #[test]
    fn test_step_out_of_range() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Step",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let result = step_remove(
            &conn,
            "my-plan",
            &project,
            Some("5"),
            None,
            true,
            &test_out(),
        );
        assert!(result.is_err());
    }

    // -- resolve_resume_plan --

    /// Initialize a throwaway git repo with a single commit so
    /// `git::get_current_branch` succeeds. Returns (TempDir, path,
    /// canonical-project-string, current-branch-name).
    fn git_repo_for_resume() -> (tempfile::TempDir, std::path::PathBuf, String, String) {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        for args in [
            vec!["init"],
            vec!["config", "user.email", "t@t.com"],
            vec!["config", "user.name", "t"],
        ] {
            std::process::Command::new("git")
                .args(&args)
                .current_dir(&dir)
                .output()
                .unwrap();
        }
        fs::write(dir.join("README.md"), "# hi").unwrap();
        std::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(&dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&dir)
            .output()
            .unwrap();
        let canonical = dir.canonicalize().unwrap().to_string_lossy().into_owned();
        let branch = crate::git::get_current_branch(&dir).unwrap();
        (tmp, dir, canonical, branch)
    }

    #[test]
    fn test_resolve_resume_plan_with_slug_uses_exact_lookup() {
        let (_tmp, dir, project, _branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        let plan =
            storage::create_plan(&conn, "exact", &project, "any", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Failed).unwrap();

        let p = resolve_resume_plan(&conn, Some("exact".to_string()), &project, &dir).unwrap();
        assert_eq!(p.slug, "exact");
    }

    #[test]
    fn test_resolve_resume_plan_no_slug_single_branch_match() {
        let (_tmp, dir, project, branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        let plan =
            storage::create_plan(&conn, "only", &project, "x", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &plan.id, PlanStatus::Failed).unwrap();
        storage::set_plan_last_run_branch(&conn, &plan.id, &branch).unwrap();

        let p = resolve_resume_plan(&conn, None, &project, &dir).unwrap();
        assert_eq!(p.slug, "only");
    }

    #[test]
    fn test_resolve_resume_plan_no_slug_picks_most_recent_when_ambiguous() {
        let (_tmp, dir, project, branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        let p1 = storage::create_plan(&conn, "older", &project, "x", "d", None, None, &[]).unwrap();
        let p2 = storage::create_plan(&conn, "newer", &project, "x", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &p1.id, PlanStatus::Failed).unwrap();
        storage::update_plan_status(&conn, &p2.id, PlanStatus::Failed).unwrap();
        storage::set_plan_last_run_branch(&conn, &p1.id, &branch).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        storage::set_plan_last_run_branch(&conn, &p2.id, &branch).unwrap();

        let p = resolve_resume_plan(&conn, None, &project, &dir).unwrap();
        assert_eq!(
            p.slug, "newer",
            "DESC by last_run_started_at picks the most recent"
        );
    }

    #[test]
    fn test_resolve_resume_plan_falls_back_to_resumable_plan_when_no_branch_match() {
        let (_tmp, dir, project, branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        // A plan whose last_run_branch is some OTHER branch — must not
        // match by branch.
        let other =
            storage::create_plan(&conn, "elsewhere", &project, "x", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &other.id, PlanStatus::Failed).unwrap();
        let bogus_branch = format!("not-{branch}");
        storage::set_plan_last_run_branch(&conn, &other.id, &bogus_branch).unwrap();

        // ...but find_resumable_plan still returns it (any in_progress /
        // ready / failed / aborted plan in the project counts).
        let p = resolve_resume_plan(&conn, None, &project, &dir).unwrap();
        assert_eq!(p.slug, "elsewhere");
    }

    /// Regression for finding 2: an aborted plan must resume even when
    /// branch inference misses (e.g. last_run_branch differs from the
    /// current branch, no branch ever recorded, etc.). Under the old
    /// `find_active_plan` fallback, Aborted was silently filtered out.
    #[test]
    fn test_resolve_resume_plan_falls_back_to_aborted_plan_when_no_branch_match() {
        let (_tmp, dir, project, branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        let p = storage::create_plan(&conn, "ab", &project, "x", "d", None, None, &[]).unwrap();
        storage::update_plan_status(&conn, &p.id, PlanStatus::Aborted).unwrap();
        // Last run executed on a branch that is NOT the current one, so
        // the branch-based resolver finds nothing.
        let other = format!("not-{branch}");
        storage::set_plan_last_run_branch(&conn, &p.id, &other).unwrap();

        let resolved = resolve_resume_plan(&conn, None, &project, &dir).unwrap();
        assert_eq!(
            resolved.slug, "ab",
            "Aborted plans must be resumable via the branch-miss fallback"
        );
    }

    #[test]
    fn test_resolve_resume_plan_errors_when_nothing_matches() {
        let (_tmp, dir, project, _branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        // No plans at all — neither branch resolution nor active-plan
        // fallback turns up a candidate.
        let err = resolve_resume_plan(&conn, None, &project, &dir).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("No resumable plan") || msg.contains("No active plan"),
            "expected branch-aware error message, got: {msg}"
        );
    }

    /// Slug-collision regression covering the spec's hard test:
    /// plan whose last_run_branch is recorded as 'master' must NOT match
    /// against a freshly-checked-out feature branch that happens to share
    /// its slug as its name.
    #[test]
    fn test_resolve_resume_plan_no_false_match_for_slug_named_branch() {
        let (_tmp, dir, project, current_branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();

        // Plan A: slug='deploy', branch_name='deploy'. Last run on
        // 'old-master' (some branch that is NOT the current one).
        let a = storage::create_plan(&conn, "deploy", &project, "deploy", "d", None, None, &[])
            .unwrap();
        storage::update_plan_status(&conn, &a.id, PlanStatus::Failed).unwrap();
        let unrelated = format!("unrelated-{current_branch}");
        storage::set_plan_last_run_branch(&conn, &a.id, &unrelated).unwrap();

        // Switch the workdir to a NEW branch named exactly 'deploy' (the
        // slug-collision shape).
        std::process::Command::new("git")
            .args(["checkout", "-b", "deploy"])
            .current_dir(&dir)
            .output()
            .unwrap();
        assert_eq!(crate::git::get_current_branch(&dir).unwrap(), "deploy");

        // Branch-based resolver must NOT match A. Active-plan fallback
        // does match A (it's the only Failed plan in the project), so the
        // resolver returns it via the fallback path — but this is fine,
        // because the *false-match defence* is the absence of a
        // branch-based hit. The user gets A only because A is the only
        // active plan in this project; if there were another active plan
        // somewhere, the active-plan resolver would pick whichever it
        // normally picks. The point of the test is: A.last_run_branch
        // 'unrelated-…' ≠ 'deploy', so the branch-based resolver never
        // false-matches.
        let candidates =
            storage::find_resumable_plans_for_branch(&conn, &project, "deploy").unwrap();
        assert!(
            candidates.is_empty(),
            "branch-based resolver MUST NOT match a plan whose last_run_branch is set to a different branch (got {:?})",
            candidates.iter().map(|p| &p.slug).collect::<Vec<_>>()
        );
    }

    /// Never-run plan (last_run_branch IS NULL) must still match by
    /// branch_name when the current branch equals it.
    #[test]
    fn test_resolve_resume_plan_never_run_uses_branch_name_fallback() {
        let (_tmp, dir, project, current_branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        // Create a plan whose branch_name matches the current branch and
        // mark it Ready so it's resumable. Don't call run, so
        // last_run_branch stays NULL.
        let p = storage::create_plan(
            &conn,
            "fresh",
            &project,
            &current_branch,
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::update_plan_status(&conn, &p.id, PlanStatus::Ready).unwrap();
        let reread = storage::get_plan_by_slug(&conn, "fresh", &project)
            .unwrap()
            .unwrap();
        assert!(reread.last_run_branch.is_none());

        let resolved = resolve_resume_plan(&conn, None, &project, &dir).unwrap();
        assert_eq!(resolved.slug, "fresh");
    }

    // -- resolve_step: numeric vs short_id selector (docs/dag-redesign.md §7) --

    /// Build a plan with `n` appended steps titled `Step {i}` (0-based).
    /// Returns `(plan_id, short_ids_in_order)`.
    fn plan_with_steps(conn: &Connection, project: &str, n: usize) -> (String, Vec<String>) {
        let plan = storage::create_plan(conn, "sel", project, "b", "d", None, None, &[]).unwrap();
        let mut sids = Vec::with_capacity(n);
        for i in 0..n {
            let (s, _) = storage::create_step(
                conn,
                &plan.id,
                &format!("Step {i}"),
                "",
                None,
                None,
                &[],
                None,
                None,
                None,
                None,
            )
            .unwrap();
            sids.push(s.short_id);
        }
        (plan.id, sids)
    }

    #[test]
    fn test_resolve_step_numeric_selector_still_works() {
        let (conn, project) = setup();
        let (plan_id, _sids) = plan_with_steps(&conn, &project, 3);

        let (step, pos) = resolve_step(&conn, &plan_id, Some("2"), None).unwrap();
        assert_eq!(pos, 2);
        // Titles are 0-based: position 2 is "Step 1".
        assert_eq!(step.title, "Step 1");
    }

    #[test]
    fn test_resolve_step_short_id_resolves_same_step_as_number() {
        let (conn, project) = setup();
        let (plan_id, sids) = plan_with_steps(&conn, &project, 3);

        // The 2nd step's short_id must resolve to exactly the same step and
        // display position as the numeric selector "2".
        let by_num = resolve_step(&conn, &plan_id, Some("2"), None).unwrap();
        let by_sid = resolve_step(&conn, &plan_id, Some(sids[1].as_str()), None).unwrap();
        assert_eq!(by_num.0.id, by_sid.0.id, "both forms resolve the same step");
        assert_eq!(by_num.1, by_sid.1, "both forms report the same position");
        assert_eq!(by_sid.0.short_id, sids[1]);
    }

    #[test]
    fn test_resolve_step_ambiguous_looking_but_numeric() {
        let (conn, project) = setup();
        let (plan_id, _sids) = plan_with_steps(&conn, &project, 3);

        // "00000001" is short_id-SHAPED (8 base-62 chars) but no step owns
        // it, so it falls through to the numeric branch and resolves step 1
        // — byte-identical to passing "1".
        let shaped = resolve_step(&conn, &plan_id, Some("00000001"), None).unwrap();
        let plain = resolve_step(&conn, &plan_id, Some("1"), None).unwrap();
        assert_eq!(shaped.0.id, plain.0.id);
        assert_eq!(shaped.1, 1);

        // A short numeric like "3" is never short-id-shaped → numeric.
        let (s3, p3) = resolve_step(&conn, &plan_id, Some("3"), None).unwrap();
        assert_eq!(p3, 3);
        assert_eq!(s3.title, "Step 2");
    }

    #[test]
    fn test_resolve_step_unknown_short_id_errors_cleanly() {
        let (conn, project) = setup();
        let (plan_id, _sids) = plan_with_steps(&conn, &project, 2);

        // 8-char base-62 token matching no short_id and not numeric: a
        // clean, actionable error — never a panic or silent fallback.
        let err = resolve_step(&conn, &plan_id, Some("zzzzABCD"), None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Invalid step selector") && err.contains("zzzzABCD"),
            "unknown short id must error cleanly, got: {err}"
        );

        // Out-of-range numeric still yields the historical range error.
        let oor = resolve_step(&conn, &plan_id, Some("9"), None)
            .unwrap_err()
            .to_string();
        assert!(oor.contains("out of range"), "got: {oor}");
    }

    #[test]
    fn test_resolve_step_short_id_scoped_to_plan() {
        // A short_id is plan-unique, not global: plan-A's short_id must
        // never resolve to plan-A's step when used against plan B. (It
        // either errors as a non-numeric token or, in the astronomically
        // unlikely all-digit case, resolves a *plan-B* position — never
        // crossing the plan boundary.)
        let (conn, project) = setup();
        let (_plan_a, a_sids) = plan_with_steps(&conn, &project, 1);
        let a_step = resolve_step(&conn, &_plan_a, Some(a_sids[0].as_str()), None)
            .unwrap()
            .0;
        let plan_b =
            storage::create_plan(&conn, "other", &project, "b", "d", None, None, &[]).unwrap();
        storage::create_step(
            &conn,
            &plan_b.id,
            "B0",
            "",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        match resolve_step(&conn, &plan_b.id, Some(a_sids[0].as_str()), None) {
            Err(_) => {} // non-numeric token → clean error (the common case).
            Ok((step, _)) => assert_ne!(
                step.id, a_step.id,
                "plan-A short id must never resolve to plan-A's step in plan B"
            ),
        }
    }
}
