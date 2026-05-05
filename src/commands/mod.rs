// Plan and step CLI command implementations
//
// This module is split into per-area submodules. Shared helpers live here;
// each submodule re-exports its public functions through this module.

mod agents;
pub mod config_cmd;
mod hooks;
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
        Err(_) => bail!("No resumable plan found in this project. Specify a plan slug as a positional argument."),
    }
}

/// Resolve a step reference: either a 1-based positional number within the
/// plan's step list, or a UUID string looked up via `storage::get_step_by_id`.
///
/// Exactly one of `step_num` / `step_id` must be `Some`; the caller (clap
/// `conflicts_with`) guarantees they are mutually exclusive, and this function
/// checks that at least one is present.
///
/// Returns `(step, step_display_num)` where `step_display_num` is the 1-based
/// position in the plan's step list (used for user-facing messages).
pub fn resolve_step(
    conn: &Connection,
    plan_id: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
) -> Result<(Step, usize)> {
    let steps = storage::list_steps(conn, plan_id)?;

    match (step_num, step_id) {
        (Some(num), None) => {
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
            bail!("Provide either a step number or --step-id");
        }
        (Some(_), Some(_)) => {
            // Should be prevented by clap conflicts_with, but guard anyway.
            bail!("Cannot specify both a step number and --step-id");
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
        eprintln!("{icon} Config exists: {}", config_path.display());
        eprintln!("  (use --force to regenerate)");
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

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
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
            &[],
            &test_out(),
        )
        .unwrap();

        step_remove(&conn, "my-plan", &project, Some(2), None, true, &test_out()).unwrap();

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
            &[],
            &test_out(),
        )
        .unwrap();

        step_edit(
            &conn,
            "my-plan",
            &project,
            Some(1),
            None,
            Some("New title"),
            Some("New desc"),
            None,
            None,
            None,
            &[],
            None,
            false,
            None,
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
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        storage::update_step_status(&conn, &steps[0].id, StepStatus::Failed).unwrap();

        step_reset(&conn, "my-plan", &project, Some(1), None, &test_out()).unwrap();

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
            &[],
            &test_out(),
        )
        .unwrap();

        // Move step 3 (C) to position 1
        step_move(&conn, "my-plan", &project, Some(3), None, 1, &test_out()).unwrap();

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
            &[],
            &test_out(),
        )
        .unwrap();

        // Move step 1 (A) to position 3
        step_move(&conn, "my-plan", &project, Some(1), None, 3, &test_out()).unwrap();

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
            &[],
            &test_out(),
        )
        .unwrap();

        let result = step_remove(&conn, "my-plan", &project, Some(5), None, true, &test_out());
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

        let p =
            resolve_resume_plan(&conn, Some("exact".to_string()), &project, &dir).unwrap();
        assert_eq!(p.slug, "exact");
    }

    #[test]
    fn test_resolve_resume_plan_no_slug_single_branch_match() {
        let (_tmp, dir, project, branch) = git_repo_for_resume();
        let conn = db::open_memory().unwrap();
        let plan = storage::create_plan(&conn, "only", &project, "x", "d", None, None, &[]).unwrap();
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
}
