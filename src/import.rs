// Plan import: deserialize portable JSON and create new plan + steps

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::Deserialize;
use std::path::Path;

use crate::storage;

// ---------------------------------------------------------------------------
// Import JSON schema (mirrors export but uses Deserialize)
// ---------------------------------------------------------------------------

/// Top-level imported plan structure.
#[derive(Debug, Clone, Deserialize)]
pub struct ImportedPlan {
    /// ralph-rs version that produced this export (informational).
    #[allow(dead_code)]
    pub ralph_rs_version: String,
    /// When the export was created (informational).
    #[allow(dead_code)]
    pub exported_at: String,
    /// Plan metadata.
    pub plan: ImportedPlanMeta,
    /// Ordered list of steps.
    pub steps: Vec<ImportedStep>,
}

/// Plan metadata from the portable JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct ImportedPlanMeta {
    pub slug: String,
    pub branch_name: String,
    pub description: String,
    pub harness: Option<String>,
    pub agent: Option<String>,
    #[serde(default)]
    pub deterministic_tests: Vec<String>,
    /// Slugs of plans this plan directly depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub plan_harness: Option<String>,
}

/// Step from the portable JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct ImportedStep {
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub agent: Option<String>,
    pub harness: Option<String>,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    pub max_retries: Option<i32>,
    #[serde(default)]
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// Import logic
// ---------------------------------------------------------------------------

/// Options for customizing the import.
pub struct ImportOptions<'a> {
    /// Override the slug from the JSON.
    pub slug: Option<&'a str>,
    /// Override the branch name from the JSON.
    pub branch: Option<&'a str>,
    /// Override the harness from the JSON.
    pub harness: Option<&'a str>,
    /// The project directory to bind the imported plan to.
    pub project: &'a str,
}

/// Read and parse a portable plan JSON file.
pub fn read_plan_file(path: &Path) -> Result<ImportedPlan> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Cannot read file: {}", path.display()))?;
    let imported: ImportedPlan = serde_json::from_str(&contents)
        .with_context(|| format!("Invalid plan JSON in: {}", path.display()))?;
    Ok(imported)
}

/// Import a plan from a parsed ImportedPlan into the database.
///
/// Creates a new plan with fresh UUIDs, status=ready, and all steps
/// set to pending with 0 attempts.
pub fn import_plan_from_data(
    conn: &Connection,
    data: &ImportedPlan,
    options: &ImportOptions<'_>,
) -> Result<String> {
    let slug = options.slug.unwrap_or(&data.plan.slug);
    let branch = options.branch.unwrap_or(&data.plan.branch_name);
    let harness = options.harness.or(data.plan.harness.as_deref());

    conn.execute_batch("BEGIN;")
        .context("Failed to begin import transaction")?;

    let result = import_plan_inner(conn, data, slug, branch, harness, options);

    match &result {
        Ok(_) => {
            conn.execute_batch("COMMIT;")
                .context("Failed to commit import transaction")?;
        }
        Err(_) => {
            let _ = conn.execute_batch("ROLLBACK;");
        }
    }

    result
}

fn import_plan_inner(
    conn: &Connection,
    data: &ImportedPlan,
    slug: &str,
    branch: &str,
    harness: Option<&str>,
    options: &ImportOptions<'_>,
) -> Result<String> {
    let plan = storage::create_plan(
        conn,
        slug,
        options.project,
        branch,
        &data.plan.description,
        harness,
        data.plan.agent.as_deref(),
        &data.plan.deterministic_tests,
    )
    .with_context(|| format!("Failed to create imported plan '{slug}'"))?;

    storage::update_plan_status(conn, &plan.id, crate::plan::PlanStatus::Ready)?;

    if data.plan.plan_harness.is_some() {
        storage::set_plan_harness_gen(conn, &plan.id, data.plan.plan_harness.as_deref())?;
    }

    for step_data in &data.steps {
        storage::create_step(
            conn,
            &plan.id,
            &step_data.title,
            &step_data.description,
            step_data.agent.as_deref(),
            step_data.harness.as_deref(),
            &step_data.acceptance_criteria,
            step_data.max_retries,
            step_data.model.as_deref(),
        )?;
    }

    for dep_slug in &data.plan.depends_on {
        match storage::get_plan_by_slug(conn, dep_slug, options.project)? {
            Some(dep) => {
                storage::add_plan_dependency(conn, &plan.id, &dep.id)?;
            }
            None => {
                eprintln!(
                    "warning: dependency '{}' of imported plan '{}' not found in project '{}'; skipping",
                    dep_slug, slug, options.project
                );
            }
        }
    }

    Ok(plan.id)
}

/// Import a plan from a JSON file. Full CLI entry point.
pub fn import_plan(
    conn: &Connection,
    file: &Path,
    project: &str,
    slug: Option<&str>,
    branch: Option<&str>,
    harness: Option<&str>,
) -> Result<()> {
    let data = read_plan_file(file)?;

    let options = ImportOptions {
        slug,
        branch,
        harness,
        project,
    };

    let effective_slug = slug.unwrap_or(&data.plan.slug);

    import_plan_from_data(conn, &data, &options)?;

    eprintln!(
        "Imported plan '{}' with {} steps (status: ready)",
        effective_slug,
        data.steps.len()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::export;
    use crate::plan::{PlanStatus, StepStatus};

    fn setup() -> Connection {
        db::open_memory().expect("open_memory")
    }

    #[test]
    fn test_import_from_json_string() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "imported-plan",
                "branch_name": "feat/import",
                "description": "A plan to import",
                "harness": "claude",
                "agent": "opus",
                "deterministic_tests": ["cargo test"]
            },
            "steps": [
                {
                    "title": "Step one",
                    "description": "First step",
                    "agent": null,
                    "harness": null,
                    "acceptance_criteria": ["tests pass"],
                    "max_retries": 3
                },
                {
                    "title": "Step two",
                    "description": "Second step",
                    "agent": "sonnet",
                    "harness": "codex",
                    "acceptance_criteria": [],
                    "max_retries": null
                }
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };

        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();

        // Verify plan
        let plan = storage::get_plan_by_slug(&conn, "imported-plan", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.id, plan_id);
        assert_eq!(plan.slug, "imported-plan");
        assert_eq!(plan.branch_name, "feat/import");
        assert_eq!(plan.description, "A plan to import");
        assert_eq!(plan.harness.as_deref(), Some("claude"));
        assert_eq!(plan.agent.as_deref(), Some("opus"));
        assert_eq!(plan.deterministic_tests, vec!["cargo test"]);
        assert_eq!(plan.status, PlanStatus::Ready);

        // Verify steps
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps.len(), 2);

        assert_eq!(steps[0].title, "Step one");
        assert_eq!(steps[0].description, "First step");
        assert!(steps[0].agent.is_none());
        assert!(steps[0].harness.is_none());
        assert_eq!(steps[0].acceptance_criteria, vec!["tests pass"]);
        assert_eq!(steps[0].max_retries, Some(3));
        assert_eq!(steps[0].status, StepStatus::Pending);
        assert_eq!(steps[0].attempts, 0);

        assert_eq!(steps[1].title, "Step two");
        assert_eq!(steps[1].agent.as_deref(), Some("sonnet"));
        assert_eq!(steps[1].harness.as_deref(), Some("codex"));
        assert_eq!(steps[1].status, StepStatus::Pending);
        assert_eq!(steps[1].attempts, 0);
    }

    #[test]
    fn test_import_with_slug_override() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "original-slug",
                "branch_name": "branch",
                "description": "desc"
            },
            "steps": []
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: Some("overridden-slug"),
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };

        import_plan_from_data(&conn, &data, &options).unwrap();

        // Should use overridden slug
        let plan = storage::get_plan_by_slug(&conn, "overridden-slug", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.slug, "overridden-slug");

        // Original slug should not exist
        let original = storage::get_plan_by_slug(&conn, "original-slug", "/tmp/proj").unwrap();
        assert!(original.is_none());
    }

    #[test]
    fn test_import_with_branch_override() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "branch-test",
                "branch_name": "original-branch",
                "description": "desc"
            },
            "steps": []
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: Some("new-branch"),
            harness: None,
            project: "/tmp/proj",
        };

        import_plan_from_data(&conn, &data, &options).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "branch-test", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.branch_name, "new-branch");
    }

    #[test]
    fn test_import_with_harness_override() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "harness-test",
                "branch_name": "branch",
                "description": "desc",
                "harness": "claude"
            },
            "steps": []
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: Some("codex"),
            project: "/tmp/proj",
        };

        import_plan_from_data(&conn, &data, &options).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "harness-test", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.harness.as_deref(), Some("codex"));
    }

    #[test]
    fn test_import_binds_to_project_directory() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "project-test",
                "branch_name": "branch",
                "description": "desc"
            },
            "steps": []
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/home/user/my-project",
        };

        import_plan_from_data(&conn, &data, &options).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "project-test", "/home/user/my-project")
            .unwrap()
            .unwrap();
        assert_eq!(plan.project, "/home/user/my-project");
    }

    #[test]
    fn test_import_creates_fresh_uuids() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "uuid-test",
                "branch_name": "branch",
                "description": "desc"
            },
            "steps": [
                {"title": "Step A", "description": "d"},
                {"title": "Step B", "description": "d"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();

        // Import twice to different slugs
        let options1 = ImportOptions {
            slug: Some("uuid-test-1"),
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };
        let id1 = import_plan_from_data(&conn, &data, &options1).unwrap();

        let options2 = ImportOptions {
            slug: Some("uuid-test-2"),
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };
        let id2 = import_plan_from_data(&conn, &data, &options2).unwrap();

        // Plans should have different IDs
        assert_ne!(id1, id2);

        // Steps should have different IDs
        let steps1 = storage::list_steps(&conn, &id1).unwrap();
        let steps2 = storage::list_steps(&conn, &id2).unwrap();
        assert_ne!(steps1[0].id, steps2[0].id);
        assert_ne!(steps1[1].id, steps2[1].id);
    }

    #[test]
    fn test_import_plan_status_ready_steps_pending() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "status-test",
                "branch_name": "branch",
                "description": "desc"
            },
            "steps": [
                {"title": "Step", "description": "d"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };

        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "status-test", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.status, PlanStatus::Ready);

        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
        assert_eq!(steps[0].attempts, 0);
    }

    #[test]
    fn test_roundtrip_export_import() {
        let conn = setup();

        // Create a plan with steps
        let original = storage::create_plan(
            &conn,
            "roundtrip",
            "/tmp/original",
            "feat/roundtrip",
            "Round trip test",
            Some("claude"),
            Some("opus"),
            &["cargo test".to_string(), "cargo clippy".to_string()],
        )
        .unwrap();

        storage::create_step(
            &conn,
            &original.id,
            "Setup",
            "Initial setup",
            Some("sonnet"),
            None,
            &["setup done".to_string()],
            Some(2),
            None,
        )
        .unwrap();

        storage::create_step(
            &conn,
            &original.id,
            "Implement",
            "Write the code",
            None,
            Some("codex"),
            &["code written".to_string(), "tests pass".to_string()],
            None,
            None,
        )
        .unwrap();

        // Mark a step as complete to ensure export doesn't carry status
        let orig_steps = storage::list_steps(&conn, &original.id).unwrap();
        storage::update_step_status(&conn, &orig_steps[0].id, StepStatus::Complete).unwrap();

        // Export
        let steps = storage::list_steps(&conn, &original.id).unwrap();
        let exported = export::build_exported_plan(&original, &steps, Vec::new());
        let json = serde_json::to_string_pretty(&exported).unwrap();

        // Import into a different project
        let imported_data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/imported",
        };

        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();

        // Verify imported plan matches original (except for internal fields)
        let imported_plan = storage::get_plan_by_slug(&conn, "roundtrip", "/tmp/imported")
            .unwrap()
            .unwrap();

        assert_ne!(imported_plan.id, original.id); // Fresh UUID
        assert_eq!(imported_plan.slug, original.slug);
        assert_eq!(imported_plan.branch_name, original.branch_name);
        assert_eq!(imported_plan.description, original.description);
        assert_eq!(imported_plan.harness, original.harness);
        assert_eq!(imported_plan.agent, original.agent);
        assert_eq!(
            imported_plan.deterministic_tests,
            original.deterministic_tests
        );
        assert_eq!(imported_plan.status, PlanStatus::Ready); // Not original status
        assert_eq!(imported_plan.project, "/tmp/imported"); // Bound to new project

        // Verify steps
        let imported_steps = storage::list_steps(&conn, &imported_id).unwrap();
        assert_eq!(imported_steps.len(), 2);

        // Step content should match
        assert_eq!(imported_steps[0].title, "Setup");
        assert_eq!(imported_steps[0].description, "Initial setup");
        assert_eq!(imported_steps[0].agent.as_deref(), Some("sonnet"));
        assert!(imported_steps[0].harness.is_none());
        assert_eq!(imported_steps[0].acceptance_criteria, vec!["setup done"]);
        assert_eq!(imported_steps[0].max_retries, Some(2));

        assert_eq!(imported_steps[1].title, "Implement");
        assert_eq!(imported_steps[1].description, "Write the code");
        assert!(imported_steps[1].agent.is_none());
        assert_eq!(imported_steps[1].harness.as_deref(), Some("codex"));
        assert_eq!(
            imported_steps[1].acceptance_criteria,
            vec!["code written", "tests pass"]
        );

        // All steps should be pending with 0 attempts regardless of original state
        for step in &imported_steps {
            assert_eq!(step.status, StepStatus::Pending);
            assert_eq!(step.attempts, 0);
        }

        // IDs should be fresh
        assert_ne!(imported_steps[0].id, orig_steps[0].id);
        assert_ne!(imported_steps[1].id, orig_steps[1].id);
    }

    #[test]
    fn test_import_from_file() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "file-import",
                "branch_name": "branch",
                "description": "From file"
            },
            "steps": [
                {"title": "Step", "description": "desc"}
            ]
        }"#;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("plan.json");
        std::fs::write(&file_path, json).unwrap();

        import_plan(&conn, &file_path, "/tmp/proj", None, None, None).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "file-import", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.slug, "file-import");
        assert_eq!(plan.description, "From file");
        assert_eq!(plan.status, PlanStatus::Ready);
    }

    #[test]
    fn test_import_file_not_found() {
        let conn = setup();
        let result = import_plan(
            &conn,
            Path::new("/nonexistent/plan.json"),
            "/tmp/proj",
            None,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_import_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("bad.json");
        std::fs::write(&file_path, "not valid json").unwrap();

        let result = read_plan_file(&file_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_roundtrip_with_dependencies() {
        let conn = setup();

        // A and B live together in the source project.
        let plan_a = storage::create_plan(
            &conn,
            "dep-a",
            "/tmp/src",
            "branch-a",
            "Plan A",
            None,
            None,
            &[],
        )
        .unwrap();

        let plan_b = storage::create_plan(
            &conn,
            "dep-b",
            "/tmp/src",
            "branch-b",
            "Plan B",
            None,
            None,
            &[],
        )
        .unwrap();

        // B depends on A.
        storage::add_plan_dependency(&conn, &plan_b.id, &plan_a.id).unwrap();

        // Build the export payload for B manually, resolving A's slug.
        let b_steps = storage::list_steps(&conn, &plan_b.id).unwrap();
        let exported_b = export::build_exported_plan(&plan_b, &b_steps, vec!["dep-a".to_string()]);
        assert_eq!(exported_b.plan.depends_on, vec!["dep-a".to_string()]);
        let json_b = serde_json::to_string_pretty(&exported_b).unwrap();

        // Import B into a fresh project that ALREADY contains A (import A
        // first, then B). Use a slug override for the imported B to avoid
        // colliding with any future projects.
        let plan_a_dest = storage::create_plan(
            &conn,
            "dep-a",
            "/tmp/dst",
            "branch-a",
            "Plan A copy",
            None,
            None,
            &[],
        )
        .unwrap();

        let imported_data: ImportedPlan = serde_json::from_str(&json_b).unwrap();
        let options = ImportOptions {
            slug: Some("dep-b2"),
            branch: None,
            harness: None,
            project: "/tmp/dst",
        };
        let b2_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();

        // Verify the imported B2's deps resolve to the destination A.
        let b2_deps = storage::list_plan_dependencies(&conn, &b2_id).unwrap();
        assert_eq!(b2_deps.len(), 1);
        assert_eq!(b2_deps[0], plan_a_dest.id);
    }

    #[test]
    fn test_import_with_missing_dep_warns_but_succeeds() {
        let conn = setup();

        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "needs-dep",
                "branch_name": "branch",
                "description": "desc",
                "depends_on": ["missing-plan"]
            },
            "steps": []
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };

        // Import should succeed despite the missing dependency.
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();

        // No dependency edge should have been created.
        let deps = storage::list_plan_dependencies(&conn, &plan_id).unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn test_import_rolls_back_on_failure() {
        let conn = setup();

        // Import a plan whose depends_on includes its own slug. The plan
        // and steps will be created inside the transaction, then
        // add_plan_dependency will bail on the self-cycle, triggering a
        // rollback. Afterward, no plan or steps should remain.
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "self-dep",
                "branch_name": "branch",
                "description": "will fail",
                "depends_on": ["self-dep"]
            },
            "steps": [
                {"title": "Step A", "description": "a"},
                {"title": "Step B", "description": "b"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/rollback",
        };

        let result = import_plan_from_data(&conn, &data, &options);
        assert!(result.is_err(), "import should fail on self-dependency cycle");

        let plan = storage::get_plan_by_slug(&conn, "self-dep", "/tmp/rollback").unwrap();
        assert!(plan.is_none(), "plan should not exist after rollback");
    }

    #[test]
    fn test_import_with_missing_optional_fields() {
        let conn = setup();

        // Minimal JSON with only required fields
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "minimal",
                "branch_name": "branch",
                "description": "desc"
            },
            "steps": [
                {"title": "Step"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
        };

        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "minimal", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert!(plan.harness.is_none());
        assert!(plan.agent.is_none());
        assert!(plan.deterministic_tests.is_empty());

        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps[0].title, "Step");
        assert_eq!(steps[0].description, ""); // Default empty
        assert!(steps[0].agent.is_none());
        assert!(steps[0].harness.is_none());
        assert!(steps[0].acceptance_criteria.is_empty());
        assert!(steps[0].max_retries.is_none());
    }
}
