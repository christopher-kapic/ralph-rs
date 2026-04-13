// Plan and step CLI command implementations
//
// This module is split into per-area submodules. Shared helpers live here;
// each submodule re-exports its public functions through this module.

mod agents;
mod hooks;
mod plan;
mod run;
mod step;

// Re-export all public command functions so callers can use `commands::*`.
pub use agents::*;
pub use hooks::*;
pub use plan::*;
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

pub fn cmd_init(out: &OutputContext) -> Result<()> {
    use std::fs;

    let icon = output::check_icon(out.color);

    // Create config dir
    let config_dir = config::config_dir()?;
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create config directory {}", config_dir.display()))?;
    println!("{icon} Config directory: {}", config_dir.display());

    // Create agents dir
    let agents_dir = config::agents_dir()?;
    fs::create_dir_all(&agents_dir)
        .with_context(|| format!("Failed to create agents directory {}", agents_dir.display()))?;
    println!("{icon} Agents directory: {}", agents_dir.display());

    // Create default config file if it doesn't exist
    let config_path = config_dir.join("config.json");
    if !config_path.exists() {
        let default_config = config::Config::default();
        let json = serde_json::to_string_pretty(&default_config)?;
        fs::write(&config_path, &json)
            .with_context(|| format!("Failed to write config to {}", config_path.display()))?;
        println!("{icon} Default config: {}", config_path.display());
    } else {
        println!("{icon} Config exists: {}", config_path.display());
    }

    // Initialize database
    let _conn = db::open()?;
    let db_path = db::db_path()?;
    println!("{icon} Database: {}", db_path.display());

    println!();
    println!("ralph initialized successfully.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Doctor command
// ---------------------------------------------------------------------------

pub fn cmd_doctor(config: &config::Config, out: &OutputContext) -> Result<()> {
    println!("ralph doctor");
    println!();

    let checks = preflight::run_doctor_checks(config);

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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_approve(&conn, "my-plan", &project, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(plan.status, PlanStatus::Ready);
    }

    #[test]
    fn test_plan_approve_rejects_non_planning() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_approve(&conn, "my-plan", &project, &test_out()).unwrap();

        // Second approve should fail - plan is now ready, not planning
        let result = plan_approve(&conn, "my-plan", &project, &test_out());
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_delete_forced() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_delete(&conn, "my-plan", &project, true, &test_out()).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn test_step_add_and_list() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First step",
            Some("Do something"),
            None,
            None,
            None,
            &[],
            None,
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
            &[],
            None,
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "First", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "Third", None, None, None, None, &[], None, &test_out(),
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
            &[],
            None,
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
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
            &criteria,
            Some(5),
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "First", None, None, None, None, &[], None, &test_out(),
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
            &criteria,
            Some(2),
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "First", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "Second", None, None, None, None, &[], None, &test_out(),
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Old title",
            None,
            None,
            None,
            None,
            &[],
            None,
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "Step", None, None, None, None, &[], None, &test_out(),
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "A", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "B", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "C", None, None, None, None, &[], None, &test_out(),
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "A", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "B", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();
        step_add(
            &conn, "my-plan", &project, "C", None, None, None, None, &[], None, &test_out(),
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

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_create(&conn, "plan-b", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
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

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_create(&conn, "plan-b", &project, None, None, None, None, &[], &[], &test_out()).unwrap();

        plan_dependency_add(&conn, "plan-b", &project, &["plan-a".to_string()], &test_out()).unwrap();

        let b = storage::get_plan_by_slug(&conn, "plan-b", &project)
            .unwrap()
            .unwrap();
        let deps = storage::list_plan_dependencies(&conn, &b.id).unwrap();
        assert_eq!(deps.len(), 1);
    }

    #[test]
    fn test_plan_dependency_add_rejects_self_reference() {
        let (conn, project) = setup();

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();

        let result = plan_dependency_add(&conn, "plan-a", &project, &["plan-a".to_string()], &test_out());
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_dependency_add_rejects_cycle() {
        let (conn, project) = setup();

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        plan_create(&conn, "plan-b", &project, None, None, None, None, &[], &[], &test_out()).unwrap();

        // a -> b is fine.
        plan_dependency_add(&conn, "plan-a", &project, &["plan-b".to_string()], &test_out()).unwrap();
        // b -> a would close a cycle and should error.
        let result = plan_dependency_add(&conn, "plan-b", &project, &["plan-a".to_string()], &test_out());
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_dependency_remove() {
        let (conn, project) = setup();

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
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

        plan_dependency_remove(&conn, "plan-b", &project, &["plan-a".to_string()], &test_out()).unwrap();
        assert_eq!(
            storage::list_plan_dependencies(&conn, &b.id).unwrap().len(),
            0
        );
    }

    #[test]
    fn test_plan_dependency_list_resolves_both_directions() {
        let (conn, project) = setup();

        plan_create(&conn, "plan-a", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
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

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[], &[], &test_out()).unwrap();
        step_add(
            &conn, "my-plan", &project, "Step", None, None, None, None, &[], None, &test_out(),
        )
        .unwrap();

        let result = step_remove(&conn, "my-plan", &project, Some(5), None, true, &test_out());
        assert!(result.is_err());
    }
}
