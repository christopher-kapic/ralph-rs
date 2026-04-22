// Plan and step CLI command implementations
//
// This module is split into per-area submodules. Shared helpers live here;
// each submodule re-exports its public functions through this module.

mod agents;
pub mod config_cmd;
mod hooks;
mod plan;
mod prompt;
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
use crate::output::{self, OutputContext};
use crate::plan::Step;
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
}
