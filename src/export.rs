// Plan export: serialize plan + steps to portable JSON

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::Connection;
use serde::Serialize;
use std::io::Write;
use std::path::Path;

use crate::plan::{Plan, Step};
use crate::storage;

// ---------------------------------------------------------------------------
// Portable JSON schema
// ---------------------------------------------------------------------------

/// Portable representation of a plan for export/import.
#[derive(Debug, Clone, Serialize)]
pub struct ExportedPlan {
    /// ralph-rs version that produced this export.
    pub ralph_rs_version: String,
    /// ISO 8601 timestamp of when the export was created.
    pub exported_at: String,
    /// Plan metadata (no ids, project, timestamps).
    pub plan: ExportedPlanMeta,
    /// Ordered list of steps (no ids, plan_id, timestamps, execution state).
    pub steps: Vec<ExportedStep>,
}

/// Plan metadata stripped of internal fields.
#[derive(Debug, Clone, Serialize)]
pub struct ExportedPlanMeta {
    pub slug: String,
    pub branch_name: String,
    pub description: String,
    pub harness: Option<String>,
    pub agent: Option<String>,
    pub deterministic_tests: Vec<String>,
    /// Slugs of plans this plan directly depends on (empty by default).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_harness: Option<String>,
}

/// Step stripped of internal fields.
#[derive(Debug, Clone, Serialize)]
pub struct ExportedStep {
    pub title: String,
    pub description: String,
    pub agent: Option<String>,
    pub harness: Option<String>,
    pub acceptance_criteria: Vec<String>,
    pub max_retries: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// Export logic
// ---------------------------------------------------------------------------

/// Build an ExportedPlan from a Plan and its steps.
///
/// `depends_on_slugs` is the caller-supplied list of slugs this plan
/// depends on (resolved by [`export_plan`] from the dependency graph).
pub fn build_exported_plan(
    plan: &Plan,
    steps: &[Step],
    depends_on_slugs: Vec<String>,
) -> ExportedPlan {
    let version = env!("CARGO_PKG_VERSION").to_string();
    let exported_at = Utc::now().to_rfc3339();

    let meta = ExportedPlanMeta {
        slug: plan.slug.clone(),
        branch_name: plan.branch_name.clone(),
        description: plan.description.clone(),
        harness: plan.harness.clone(),
        agent: plan.agent.clone(),
        deterministic_tests: plan.deterministic_tests.clone(),
        depends_on: depends_on_slugs,
        plan_harness: plan.plan_harness.clone(),
    };

    let exported_steps: Vec<ExportedStep> = steps
        .iter()
        .map(|s| ExportedStep {
            title: s.title.clone(),
            description: s.description.clone(),
            agent: s.agent.clone(),
            harness: s.harness.clone(),
            acceptance_criteria: s.acceptance_criteria.clone(),
            max_retries: s.max_retries,
            model: s.model.clone(),
        })
        .collect();

    ExportedPlan {
        ralph_rs_version: version,
        exported_at,
        plan: meta,
        steps: exported_steps,
    }
}

/// Export a plan by slug to JSON. Writes to a file or stdout.
pub fn export_plan(
    conn: &Connection,
    slug: &str,
    project: &str,
    output: Option<&Path>,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    // Resolve the plan's direct dependency IDs to slugs. Any dependency
    // we can't resolve (shouldn't happen in practice) is silently dropped.
    let dep_ids = storage::list_plan_dependencies(conn, &plan.id)?;
    let mut dep_slugs: Vec<String> = Vec::with_capacity(dep_ids.len());
    for id in &dep_ids {
        if let Some(s) = storage::get_plan_slug_by_id(conn, id)? {
            dep_slugs.push(s);
        }
    }
    dep_slugs.sort();

    let exported = build_exported_plan(&plan, &steps, dep_slugs);
    let json = serde_json::to_string_pretty(&exported)?;

    match output {
        Some(path) => {
            let mut file = std::fs::File::create(path)
                .with_context(|| format!("Cannot create file: {}", path.display()))?;
            file.write_all(json.as_bytes())?;
            file.write_all(b"\n")?;
            eprintln!("Exported plan '{}' to {}", slug, path.display());
        }
        None => {
            println!("{json}");
        }
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
    use crate::plan::StepStatus;

    fn setup() -> Connection {
        db::open_memory().expect("open_memory")
    }

    #[test]
    fn test_build_exported_plan_excludes_internal_fields() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "test-export",
            "/tmp/proj",
            "feat/export",
            "Export test plan",
            Some("claude"),
            Some("opus"),
            &["cargo test".to_string(), "cargo clippy".to_string()],
        )
        .unwrap();

        let (_s1, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step one",
            "First step desc",
            Some("sonnet"),
            None,
            &["tests pass".to_string()],
            Some(3),
            None,
        )
        .unwrap();

        let (_s2, _) = storage::create_step(
            &conn,
            &plan.id,
            "Step two",
            "Second step desc",
            None,
            Some("codex"),
            &[],
            None,
            None,
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let exported = build_exported_plan(&plan, &steps, Vec::new());

        // Check version and timestamp are present
        assert!(!exported.ralph_rs_version.is_empty());
        assert!(!exported.exported_at.is_empty());

        // Check plan metadata
        assert_eq!(exported.plan.slug, "test-export");
        assert_eq!(exported.plan.branch_name, "feat/export");
        assert_eq!(exported.plan.description, "Export test plan");
        assert_eq!(exported.plan.harness.as_deref(), Some("claude"));
        assert_eq!(exported.plan.agent.as_deref(), Some("opus"));
        assert_eq!(
            exported.plan.deterministic_tests,
            vec!["cargo test", "cargo clippy"]
        );

        // Check steps
        assert_eq!(exported.steps.len(), 2);

        assert_eq!(exported.steps[0].title, "Step one");
        assert_eq!(exported.steps[0].description, "First step desc");
        assert_eq!(exported.steps[0].agent.as_deref(), Some("sonnet"));
        assert!(exported.steps[0].harness.is_none());
        assert_eq!(exported.steps[0].acceptance_criteria, vec!["tests pass"]);
        assert_eq!(exported.steps[0].max_retries, Some(3));

        assert_eq!(exported.steps[1].title, "Step two");
        assert_eq!(exported.steps[1].harness.as_deref(), Some("codex"));
        assert!(exported.steps[1].acceptance_criteria.is_empty());
        assert!(exported.steps[1].max_retries.is_none());
    }

    #[test]
    fn test_exported_json_excludes_ids_and_timestamps() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "json-test",
            "/tmp/proj",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();

        storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let exported = build_exported_plan(&plan, &steps, Vec::new());
        let json = serde_json::to_string_pretty(&exported).unwrap();

        // The JSON should NOT contain internal fields
        assert!(!json.contains("\"id\""));
        assert!(!json.contains("\"plan_id\""));
        assert!(!json.contains("\"project\""));
        assert!(!json.contains("\"sort_key\""));
        assert!(!json.contains("\"status\""));
        assert!(!json.contains("\"attempts\""));
        assert!(!json.contains("\"created_at\""));
        assert!(!json.contains("\"updated_at\""));

        // It SHOULD contain exported fields
        assert!(json.contains("\"ralph_rs_version\""));
        assert!(json.contains("\"exported_at\""));
        assert!(json.contains("\"slug\""));
        assert!(json.contains("\"branch_name\""));
        assert!(json.contains("\"title\""));
    }

    #[test]
    fn test_exported_json_is_valid() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "valid-json",
            "/tmp/proj",
            "branch",
            "desc",
            Some("claude"),
            None,
            &["cargo test".to_string()],
        )
        .unwrap();

        storage::create_step(
            &conn,
            &plan.id,
            "Step A",
            "desc a",
            None,
            None,
            &["criterion".to_string()],
            Some(2),
            None,
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let exported = build_exported_plan(&plan, &steps, Vec::new());
        let json = serde_json::to_string(&exported).unwrap();

        // Should parse back as valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_object());
        assert!(parsed["plan"].is_object());
        assert!(parsed["steps"].is_array());
        assert_eq!(parsed["steps"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_export_plan_to_file() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "file-export",
            "/tmp/proj",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();

        storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("exported.json");

        export_plan(&conn, "file-export", "/tmp/proj", Some(&file_path)).unwrap();

        let contents = std::fs::read_to_string(&file_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["plan"]["slug"], "file-export");
    }

    #[test]
    fn test_export_plan_not_found() {
        let conn = setup();
        let result = export_plan(&conn, "nonexistent", "/tmp/proj", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_export_preserves_step_order() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "order-test",
            "/tmp/proj",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();

        // Steps are created in order and have ascending sort_keys
        storage::create_step(&conn, &plan.id, "Alpha", "d", None, None, &[], None, None).unwrap();
        storage::create_step(&conn, &plan.id, "Beta", "d", None, None, &[], None, None).unwrap();
        storage::create_step(&conn, &plan.id, "Gamma", "d", None, None, &[], None, None).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let exported = build_exported_plan(&plan, &steps, Vec::new());

        assert_eq!(exported.steps[0].title, "Alpha");
        assert_eq!(exported.steps[1].title, "Beta");
        assert_eq!(exported.steps[2].title, "Gamma");
    }

    #[test]
    fn test_export_resets_step_statuses() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn,
            "status-test",
            "/tmp/proj",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();

        let (step, _) =
            storage::create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None, None).unwrap();

        // Mark step as complete
        storage::update_step_status(&conn, &step.id, StepStatus::Complete).unwrap();

        // Export should NOT include status at all (the ExportedStep struct has no status field)
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let exported = build_exported_plan(&plan, &steps, Vec::new());
        let json = serde_json::to_string(&exported).unwrap();

        // The steps array shouldn't have "status" or "attempts" fields
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let step_obj = &parsed["steps"][0];
        assert!(step_obj.get("status").is_none());
        assert!(step_obj.get("attempts").is_none());
    }
}
