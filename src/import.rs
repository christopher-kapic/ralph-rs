// Plan import: deserialize portable JSON and create new plan + steps

use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;
use serde::Deserialize;
use std::path::Path;

use crate::storage;

/// Parse the leading numeric segment of a version string as the major version.
///
/// Returns `None` for empty or non-numeric leading segments.
fn major_version(v: &str) -> Option<u64> {
    v.split('.').next()?.parse().ok()
}

/// Compare the export's ralph-rs version against the running binary's version.
///
/// When the majors differ, emit a warning (non-strict) or return an error
/// (strict). When the exporter version is missing or unparseable, warn and
/// proceed regardless of strict.
fn check_import_version(exported_version: &str, strict: bool) -> Result<()> {
    let current = env!("CARGO_PKG_VERSION");
    let current_major = major_version(current);
    let exported_major = major_version(exported_version);

    match (current_major, exported_major) {
        (Some(c), Some(e)) if c != e => {
            let msg = format!(
                "export was produced by ralph-rs {exported_version} (major {e}), \
                 but this binary is {current} (major {c}); schema may be incompatible"
            );
            if strict {
                Err(anyhow!("{msg}"))
            } else {
                eprintln!("warning: {msg}");
                Ok(())
            }
        }
        (Some(_), None) => {
            eprintln!(
                "warning: export's ralph_rs_version '{exported_version}' is unparseable; \
                 proceeding without version compatibility check"
            );
            Ok(())
        }
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Import JSON schema (mirrors export but uses Deserialize)
// ---------------------------------------------------------------------------

/// Top-level imported plan structure.
#[derive(Debug, Clone, Deserialize)]
pub struct ImportedPlan {
    /// ralph-rs version that produced this export. Checked against the
    /// running binary's major version on import (see [`check_import_version`]).
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
    /// Optional plan-level retry-strategy override. Missing/absent field
    /// deserializes to `None` via serde(default) (no override — steps fall
    /// through to the global `keep` default), preserving backward
    /// compatibility with plan JSON written before V24.
    #[serde(default)]
    pub retry_strategy: Option<crate::plan::RetryStrategy>,
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
    /// Optional per-step change policy. Missing field defaults to
    /// [`crate::plan::ChangePolicy::Required`] via serde(default), preserving
    /// backward compatibility with plan JSON written before V12.
    #[serde(default)]
    pub change_policy: crate::plan::ChangePolicy,
    /// Free-form string tags attached to the step. Missing field defaults to
    /// an empty list via serde(default), preserving backward compatibility
    /// with plan JSON written before V13.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Optional step-level retry-strategy override. Missing/absent field
    /// deserializes to `None` via serde(default) (inherit plan/global),
    /// preserving backward compatibility with plan JSON written before
    /// V24.
    #[serde(default)]
    pub retry_strategy: Option<crate::plan::RetryStrategy>,
    /// Plan-unique portable edge handle (docs/dag-redesign.md §13.2/§13.3).
    /// Absent only in *true legacy* pre-DAG bundles (`#[serde(default)]` →
    /// `None`); present and **preserved verbatim** for every DAG-aware
    /// bundle — including a linear plan (whose steps now carry real chain
    /// edges) and a no-edge multi-root DAG. Never re-minted when present
    /// (that would break `short_id` stability and orphan `depends_on`
    /// references).
    #[serde(default)]
    pub short_id: Option<String>,
    /// `short_id`s of the steps this step directly depends on
    /// (docs/dag-redesign.md §13.2/§13.3). `#[serde(default)]` → empty when
    /// the step is a root (a linear plan's first step, or any root of a
    /// multi-root DAG) or in a true legacy bundle. Bundle classification is
    /// by `short_id` presence, **not** by whether `depends_on` is non-empty:
    /// a bundle that carries `short_id`s is DAG-aware and its edges (which
    /// may legitimately be empty for a no-edge multi-root DAG) are taken
    /// literally and validated before any DB write.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

// ---------------------------------------------------------------------------
// DAG validation (docs/dag-redesign.md §13.3)
// ---------------------------------------------------------------------------

/// How the importer should build the step graph for a bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BundleShape {
    /// **True legacy** pre-DAG bundle: *no* step carries a `short_id` and no
    /// step carries `depends_on`. Mint a fresh plan-unique `short_id` per
    /// step and synthesize a linear chain (step *k* depends on *k−1* by array
    /// order) — byte-identical to the V25 migration backfill, so import and
    /// migration produce the same DAG for the same linear input
    /// (docs/dag-redesign.md §13.3).
    Legacy,
    /// **DAG-aware** bundle: it carries `short_id`s (every modern export does,
    /// §13.2). Validate the §13.3 rules, preserve the bundle's `short_id`s
    /// verbatim, and wire *exactly* the explicit `depends_on` edges — which
    /// may legitimately be empty everywhere (a no-edge multi-root DAG stays
    /// no-edge; **no** chain is synthesized). A linear plan's bundle now
    /// carries its real chain edges, so this path reproduces it exactly.
    DagAware,
}

/// Classify a bundle by **`short_id` presence**, not by whether any
/// `depends_on` is non-empty (docs/dag-redesign.md §13.2/§13.3).
///
/// The old "DAG-aware ⇔ some non-empty `depends_on`" rule misclassified a
/// bundle that carried a `short_id` on every step but no serialized
/// `depends_on` (a multi-root no-edge DAG, or — under the now-removed
/// chain-suppression — a linear export) as legacy: its `short_id`s were
/// re-minted and its independent roots were fused into a synthetic linear
/// chain. The correct discriminator:
///
/// - **No `short_id` on any step** (and, defensively, no `depends_on`
///   anywhere — a true pre-DAG bundle never carries either) ⇒ [`Legacy`].
/// - **Otherwise** the bundle carries `short_id`s ⇒ [`DagAware`]: preserve
///   them and take its edge set literally (empty edges ⇒ a genuine no-edge
///   multi-root DAG, preserved as such).
///
/// A *partial* `short_id` set (some steps have one, some don't) is a
/// corrupt DAG-aware bundle, not legacy: it routes to [`DagAware`] where
/// rule 0 of [`validate_dag_aware_steps`] rejects it with a precise message
/// rather than silently re-minting and chain-fusing.
///
/// [`Legacy`]: BundleShape::Legacy
/// [`DagAware`]: BundleShape::DagAware
fn classify_bundle(steps: &[ImportedStep]) -> BundleShape {
    let any_short_id = steps.iter().any(|s| s.short_id.is_some());
    let any_depends_on = steps.iter().any(|s| !s.depends_on.is_empty());
    if !any_short_id && !any_depends_on {
        BundleShape::Legacy
    } else {
        BundleShape::DagAware
    }
}

/// In-memory analogue of [`storage::would_create_step_cycle`] (§6) applied
/// to the whole imported edge set, *before* any DB write.
///
/// Builds the edge set incrementally in a deterministic order (steps in
/// array order, each step's `depends_on` in listed order); before adding
/// each edge `s -> d` it asks the same question
/// [`storage::would_create_step_cycle`] asks — "is `s` already reachable
/// from `d`?" — over the edges added so far, using the identical
/// stack/visited DFS. A self-edge (`s == d`) is a cycle, mirroring that
/// function's early return. Returns the offending `(s, d)` pair on the
/// first edge that would close a cycle.
fn find_imported_cycle(steps: &[ImportedStep]) -> Option<(String, String)> {
    use std::collections::{HashMap, HashSet};

    // short_id -> already-added dependency short_ids.
    let mut built: HashMap<&str, Vec<&str>> = HashMap::new();

    // Identical stack/visited DFS to `storage::would_create_step_cycle`:
    // is `target` reachable from `from` over the edges added so far?
    fn reachable(built: &HashMap<&str, Vec<&str>>, from: &str, target: &str) -> bool {
        let mut stack: Vec<&str> = vec![from];
        let mut visited: HashSet<&str> = HashSet::new();
        while let Some(cur) = stack.pop() {
            if !visited.insert(cur) {
                continue;
            }
            if cur == target {
                return true;
            }
            if let Some(deps) = built.get(cur) {
                for d in deps {
                    if !visited.contains(d) {
                        stack.push(d);
                    }
                }
            }
        }
        false
    }

    for step in steps {
        let Some(s) = step.short_id.as_deref() else {
            continue;
        };
        for d in &step.depends_on {
            let d = d.as_str();
            if s == d || reachable(&built, d, s) {
                return Some((s.to_string(), d.to_string()));
            }
            built.entry(s).or_default().push(d);
        }
    }
    None
}

/// Validate a DAG-aware bundle's edge set *before any DB write*
/// (docs/dag-redesign.md §13.3). Enforced in order:
///
///  0. every step carries a `short_id` (the portable edge handle — a
///     DAG-aware exporter always emits one for every step; a missing one
///     is a corrupt bundle that cannot be wired deterministically);
///  1. no dangling edge — every `depends_on` entry resolves to a
///     `short_id` present in the same bundle;
///  2. `short_id`s are unique within the bundle;
///  3. the edge set is acyclic ([`find_imported_cycle`]);
///  4. ≥1 root (a step with no `depends_on`).
///
/// Any failure returns an `Err` naming the offending `short_id` and the
/// violated rule. The caller runs this before `create_plan`, so a
/// rejected bundle writes no partial plan (imports are also transactional
/// as a backstop).
fn validate_dag_aware_steps(steps: &[ImportedStep]) -> Result<()> {
    use std::collections::HashSet;

    // Rule 0 + 2: every step has a short_id, and they are unique.
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, step) in steps.iter().enumerate() {
        let sid = step.short_id.as_deref().ok_or_else(|| {
            anyhow!(
                "DAG-aware import: step #{} ('{}') has no short_id; \
                 a branched bundle must carry a short_id for every step",
                i + 1,
                step.title
            )
        })?;
        if !seen.insert(sid) {
            return Err(anyhow!(
                "DAG-aware import: duplicate short_id '{sid}' in the bundle; \
                 short_ids must be unique within a plan"
            ));
        }
    }

    // Rule 1: no dangling edge.
    for step in steps {
        let sid = step.short_id.as_deref().expect("rule 0 ensured Some");
        for dep in &step.depends_on {
            if !seen.contains(dep.as_str()) {
                return Err(anyhow!(
                    "DAG-aware import: step '{sid}' depends on '{dep}', \
                     which is not a short_id present in the bundle (dangling edge)"
                ));
            }
        }
    }

    // Rule 3: acyclic.
    if let Some((s, d)) = find_imported_cycle(steps) {
        if s == d {
            return Err(anyhow!(
                "DAG-aware import: step '{s}' depends on itself (cycle)"
            ));
        }
        return Err(anyhow!(
            "DAG-aware import: edge '{s}' -> '{d}' closes a dependency cycle"
        ));
    }

    // Rule 4: at least one root.
    if !steps.iter().any(|s| s.depends_on.is_empty()) {
        return Err(anyhow!(
            "DAG-aware import: no root step (every step has at least one \
             dependency); a DAG must have ≥1 root"
        ));
    }

    Ok(())
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
    /// When true, major-version mismatch in `ralph_rs_version` is a hard
    /// error; when false, it only warns.
    pub strict: bool,
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
    check_import_version(&data.ralph_rs_version, options.strict)?;

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
    // Decide the step-graph shape *before any DB write* so a malformed
    // DAG-aware bundle aborts without leaving a partial plan
    // (docs/dag-redesign.md §13.3). A true legacy bundle takes the
    // linear-chain backfill path; any bundle carrying `short_id`s is
    // DAG-aware and validated here (its edges, possibly empty for a
    // no-edge multi-root DAG, are taken literally).
    let shape = classify_bundle(&data.steps);
    let dag_aware = shape == BundleShape::DagAware;
    if dag_aware {
        validate_dag_aware_steps(&data.steps)?;
    }

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

    // Restore the plan-level retry-strategy override only when the import
    // carried one. `None` is the column default, so skipping the write
    // keeps an unset plan unset (round-trip: None stays None).
    if let Some(rs) = data.plan.retry_strategy {
        storage::set_plan_retry_strategy(conn, &plan.id, Some(rs))?;
    }

    // First pass: create every step (in array order — the deterministic
    // scheduler tie-break seed). On the DAG-aware path the bundle's
    // `short_id`s are preserved verbatim (create_step mints a throwaway id
    // that we immediately overwrite); on the linear-chain backfill path
    // the minted id is kept, exactly as the V25 migration mints fresh ids
    // for existing linear plans (docs/dag-redesign.md §13.3). `created_ids`
    // is parallel to `data.steps`.
    let mut created_ids: Vec<String> = Vec::with_capacity(data.steps.len());
    let mut short_to_id: std::collections::HashMap<&str, String> =
        std::collections::HashMap::new();
    for step_data in &data.steps {
        let tags_arg: Option<&[String]> = if step_data.tags.is_empty() {
            None
        } else {
            Some(&step_data.tags)
        };
        let (step, _pos) = storage::create_step(
            conn,
            &plan.id,
            &step_data.title,
            &step_data.description,
            step_data.agent.as_deref(),
            step_data.harness.as_deref(),
            &step_data.acceptance_criteria,
            step_data.max_retries,
            step_data.model.as_deref(),
            Some(step_data.change_policy),
            tags_arg,
        )?;
        // Same rule for the step-level override: write only when present
        // so an unset imported step stays unset (round-trip preserved).
        if let Some(rs) = step_data.retry_strategy {
            storage::set_step_retry_strategy(conn, &step.id, Some(rs))?;
        }
        if dag_aware {
            // validate_dag_aware_steps guaranteed Some + uniqueness.
            let sid = step_data.short_id.as_deref().expect("validated Some");
            storage::set_step_short_id(conn, &step.id, sid)?;
            short_to_id.insert(sid, step.id.clone());
        }
        created_ids.push(step.id);
    }

    // Second pass: wire dependency edges.
    if dag_aware {
        // Preserve the bundle's explicit edges, resolved short_id -> id.
        // The edge set was already validated acyclic/non-dangling above;
        // add_step_dependency re-checks defensively (§6) — harmless and
        // consistent with every other edge-mutation path.
        for (step_data, step_id) in data.steps.iter().zip(&created_ids) {
            for dep in &step_data.depends_on {
                let dep_id = short_to_id
                    .get(dep.as_str())
                    .expect("rule 1 ensured every dep resolves");
                storage::add_step_dependency(conn, step_id, dep_id)?;
            }
        }
    } else {
        // Linear-chain backfill: step k depends on step k-1 by array
        // order — byte-identical to the V25 migration backfill, so import
        // and migration produce the same DAG for the same linear input
        // (docs/dag-redesign.md §13.3).
        for window in created_ids.windows(2) {
            storage::add_step_dependency(conn, &window[1], &window[0])?;
        }
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
    strict: bool,
) -> Result<()> {
    let data = read_plan_file(file)?;

    let options = ImportOptions {
        slug,
        branch,
        harness,
        project,
        strict,
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
            strict: false,
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
            strict: false,
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
            strict: false,
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
            strict: false,
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
            strict: false,
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
            strict: false,
        };
        let id1 = import_plan_from_data(&conn, &data, &options1).unwrap();

        let options2 = ImportOptions {
            slug: Some("uuid-test-2"),
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
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
            strict: false,
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
            None,
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
            None,
            None,
        )
        .unwrap();

        // Mark a step as complete to ensure export doesn't carry status
        let orig_steps = storage::list_steps(&conn, &original.id).unwrap();
        storage::update_step_status(&conn, &orig_steps[0].id, StepStatus::Complete).unwrap();

        // Export
        let steps = storage::list_steps(&conn, &original.id).unwrap();
        let exported = export::build_exported_plan(&original, &steps, Vec::new(), &[]);
        let json = serde_json::to_string_pretty(&exported).unwrap();

        // Import into a different project
        let imported_data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/imported",
            strict: false,
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

        import_plan(&conn, &file_path, "/tmp/proj", None, None, None, false).unwrap();

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
            false,
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
        let exported_b = export::build_exported_plan(&plan_b, &b_steps, vec!["dep-a".to_string()], &[]);
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
            strict: false,
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
            strict: false,
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
            strict: false,
        };

        let result = import_plan_from_data(&conn, &data, &options);
        assert!(
            result.is_err(),
            "import should fail on self-dependency cycle"
        );

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
            strict: false,
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

    #[test]
    fn test_major_version_helper() {
        assert_eq!(major_version("0.1.10"), Some(0));
        assert_eq!(major_version("1.2.3"), Some(1));
        assert_eq!(major_version("42"), Some(42));
        assert_eq!(major_version(""), None);
        assert_eq!(major_version("not-a-version"), None);
        assert_eq!(major_version("1.x"), Some(1));
    }

    #[test]
    fn test_check_import_version_same_major_ok() {
        // Current binary is the pkg version; feed the same major.
        let current = env!("CARGO_PKG_VERSION");
        let same_major = major_version(current).unwrap().to_string() + ".999.999";
        assert!(check_import_version(&same_major, false).is_ok());
        assert!(check_import_version(&same_major, true).is_ok());
    }

    #[test]
    fn test_check_import_version_future_major_warns_not_strict() {
        // Bump major arbitrarily high; non-strict should warn and succeed.
        let future = format!(
            "{}.0.0",
            major_version(env!("CARGO_PKG_VERSION")).unwrap() + 42
        );
        assert!(check_import_version(&future, false).is_ok());
    }

    #[test]
    fn test_check_import_version_future_major_errors_when_strict() {
        let future = format!(
            "{}.0.0",
            major_version(env!("CARGO_PKG_VERSION")).unwrap() + 42
        );
        let err = check_import_version(&future, true).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("major"), "error should mention major: {msg}");
    }

    #[test]
    fn test_import_with_future_major_version_succeeds_non_strict() {
        let conn = setup();
        // Construct JSON with a far-future major version.
        let future_major = major_version(env!("CARGO_PKG_VERSION")).unwrap() + 42;
        let json = format!(
            r#"{{
                "ralph_rs_version": "{future_major}.0.0",
                "exported_at": "2025-01-01T00:00:00Z",
                "plan": {{
                    "slug": "future-ver",
                    "branch_name": "branch",
                    "description": "desc"
                }},
                "steps": []
            }}"#
        );

        let data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };

        // Non-strict: should succeed (with a warning printed to stderr).
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "future-ver", "/tmp/proj")
            .unwrap()
            .unwrap();
        assert_eq!(plan.id, plan_id);
    }

    #[test]
    fn test_import_with_future_major_version_errors_strict() {
        let conn = setup();
        let future_major = major_version(env!("CARGO_PKG_VERSION")).unwrap() + 42;
        let json = format!(
            r#"{{
                "ralph_rs_version": "{future_major}.0.0",
                "exported_at": "2025-01-01T00:00:00Z",
                "plan": {{
                    "slug": "future-ver-strict",
                    "branch_name": "branch",
                    "description": "desc"
                }},
                "steps": [
                    {{"title": "Step", "description": "d"}}
                ]
            }}"#
        );

        let data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: true,
        };

        let result = import_plan_from_data(&conn, &data, &options);
        assert!(result.is_err(), "strict import should reject future major");

        // Plan should not have been created.
        let plan = storage::get_plan_by_slug(&conn, "future-ver-strict", "/tmp/proj").unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn test_import_defaults_change_policy_to_required_when_missing() {
        let conn = setup();
        // Old-style plan JSON with no change_policy on any step.
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "old-plan",
                "branch_name": "branch",
                "description": "legacy"
            },
            "steps": [
                {"title": "Implement", "description": "d"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };

        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].change_policy, crate::plan::ChangePolicy::Required);
    }

    #[test]
    fn test_roundtrip_preserves_mixed_change_policies() {
        let conn = setup();

        // Build a source plan with one Required step and one Optional step.
        let original = storage::create_plan(
            &conn,
            "mix-policy",
            "/tmp/src",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::create_step(
            &conn,
            &original.id,
            "Implement",
            "d",
            None,
            None,
            &[],
            None,
            None,
            Some(crate::plan::ChangePolicy::Required),
            None,
        )
        .unwrap();
        storage::create_step(
            &conn,
            &original.id,
            "Review",
            "d",
            None,
            None,
            &[],
            None,
            None,
            Some(crate::plan::ChangePolicy::Optional),
            None,
        )
        .unwrap();

        // Export through the real export pipeline.
        let steps = storage::list_steps(&conn, &original.id).unwrap();
        let exported = export::build_exported_plan(&original, &steps, Vec::new(), &[]);
        let json = serde_json::to_string_pretty(&exported).unwrap();
        // The exported JSON must mention both policy values literally.
        assert!(json.contains("\"required\""));
        assert!(json.contains("\"optional\""));

        // Import into a fresh project.
        let imported_data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();
        let imported_steps = storage::list_steps(&conn, &imported_id).unwrap();
        assert_eq!(imported_steps.len(), 2);
        assert_eq!(
            imported_steps[0].change_policy,
            crate::plan::ChangePolicy::Required
        );
        assert_eq!(
            imported_steps[1].change_policy,
            crate::plan::ChangePolicy::Optional
        );
    }

    #[test]
    fn test_import_with_unparseable_version_proceeds() {
        let conn = setup();
        let json = r#"{
            "ralph_rs_version": "unreleased",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "unparseable-ver",
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
            project: "/tmp/proj",
            strict: true,
        };

        // Even in strict mode, an unparseable version only warns; the
        // import proceeds because there's nothing concrete to compare.
        assert!(import_plan_from_data(&conn, &data, &options).is_ok());
    }

    #[test]
    fn test_import_missing_tags_defaults_to_empty() {
        // Pre-V13 export JSON doesn't include a `tags` field. The import path
        // must fall back to an empty list via serde(default) and not reject
        // the payload.
        let conn = setup();
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "no-tags",
                "branch_name": "b",
                "description": "legacy export"
            },
            "steps": [
                {"title": "legacy step", "description": "d"}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        assert!(data.steps[0].tags.is_empty());

        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].tags.is_empty());
    }

    #[test]
    fn test_import_preserves_explicit_tags() {
        let conn = setup();
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {
                "slug": "tagged-import",
                "branch_name": "b",
                "description": "desc"
            },
            "steps": [
                {"title": "Tagged", "description": "d", "tags": ["FIX", "REGRESSION"]}
            ]
        }"#;

        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(
            steps[0].tags,
            vec!["FIX".to_string(), "REGRESSION".to_string()]
        );
    }

    /// Round-trip `retry_strategy` through export -> JSON -> import for all
    /// three states: plan-set, step-set, and unset. The value (including
    /// `None`) must survive the round-trip unchanged (Step 23).
    #[test]
    fn test_roundtrip_preserves_retry_strategy_all_states() {
        use crate::plan::RetryStrategy;
        let conn = setup();

        let original = storage::create_plan(
            &conn,
            "rs-plan",
            "/tmp/src",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        // Plan-level override = rollback.
        storage::set_plan_retry_strategy(&conn, &original.id, Some(RetryStrategy::Rollback))
            .unwrap();

        // step 1: explicit step-level keep override.
        let (s1, _) = storage::create_step(
            &conn,
            &original.id,
            "explicit",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        storage::set_step_retry_strategy(&conn, &s1.id, Some(RetryStrategy::Keep)).unwrap();
        // step 2: no step-level override (unset -> None).
        storage::create_step(
            &conn,
            &original.id,
            "inherits",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Re-fetch so the in-memory Plan reflects the post-create
        // set_plan_retry_strategy write (the original handle is stale).
        let original = storage::get_plan_by_id(&conn, &original.id).unwrap();
        let steps = storage::list_steps(&conn, &original.id).unwrap();
        let exported = export::build_exported_plan(&original, &steps, Vec::new(), &[]);
        let json = serde_json::to_string_pretty(&exported).unwrap();

        // Plan override + the explicit step override are present; the unset
        // step omits the field entirely (skip_serializing_if).
        assert!(json.contains("\"retry_strategy\": \"rollback\""));
        assert!(json.contains("\"retry_strategy\": \"keep\""));

        // Import into a fresh project and verify all three states survived.
        let imported_data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();
        let imported_plan = storage::get_plan_by_id(&conn, &imported_id).unwrap();
        let imported_steps = storage::list_steps(&conn, &imported_id).unwrap();

        assert_eq!(imported_plan.retry_strategy, Some(RetryStrategy::Rollback));
        assert_eq!(imported_steps[0].retry_strategy, Some(RetryStrategy::Keep));
        assert!(
            imported_steps[1].retry_strategy.is_none(),
            "unset step-level override must round-trip as None"
        );
    }

    /// An unset plan-level override exports without the `retry_strategy`
    /// key at all (pre-V24 JSON shape preserved) and re-imports as `None`.
    #[test]
    fn test_roundtrip_unset_plan_retry_strategy_omitted_and_none() {
        let conn = setup();
        let original = storage::create_plan(
            &conn,
            "no-rs",
            "/tmp/src",
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        storage::create_step(
            &conn,
            &original.id,
            "s",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &original.id).unwrap();
        let exported = export::build_exported_plan(&original, &steps, Vec::new(), &[]);
        let json = serde_json::to_string_pretty(&exported).unwrap();
        assert!(
            !json.contains("retry_strategy"),
            "an all-unset plan must not emit retry_strategy at all; got:\n{json}"
        );

        let imported_data: ImportedPlan = serde_json::from_str(&json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();
        let imported_plan = storage::get_plan_by_id(&conn, &imported_id).unwrap();
        let imported_steps = storage::list_steps(&conn, &imported_id).unwrap();
        assert!(imported_plan.retry_strategy.is_none());
        assert!(imported_steps[0].retry_strategy.is_none());
    }

    // -----------------------------------------------------------------
    // DAG redesign §13.3: legacy backfill + DAG-aware validation
    // -----------------------------------------------------------------

    /// The plan's step edge set as a sorted set of `(step_short_id,
    /// dep_short_id)` pairs — a stable, UUID-independent fingerprint of
    /// the DAG used by the round-trip assertions.
    fn edge_set(
        conn: &Connection,
        plan_id: &str,
    ) -> std::collections::BTreeSet<(String, String)> {
        let steps = storage::list_steps(conn, plan_id).unwrap();
        let by_id: std::collections::HashMap<String, String> = steps
            .iter()
            .map(|s| (s.id.clone(), s.short_id.clone()))
            .collect();
        let mut out = std::collections::BTreeSet::new();
        for s in &steps {
            for dep in storage::list_step_dependencies(conn, &s.id).unwrap() {
                out.insert((s.short_id.clone(), by_id[&dep].clone()));
            }
        }
        out
    }

    /// A legacy bundle (no `short_id`, no per-step `depends_on`) backfills
    /// to the same linear-chain DAG the V25 migration produces: every step
    /// gets a minted 8-char `short_id` and step *k* depends on step *k-1*
    /// by array order, with step 0 the sole root.
    #[test]
    fn test_legacy_bundle_backfills_linear_chain_like_v25() {
        let conn = setup();
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {"slug": "legacy", "branch_name": "b", "description": "d"},
            "steps": [
                {"title": "S0", "description": "d"},
                {"title": "S1", "description": "d"},
                {"title": "S2", "description": "d"}
            ]
        }"#;
        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        assert_eq!(steps.len(), 3);
        // short_ids minted (8-char base-62) and plan-unique.
        for s in &steps {
            assert!(
                storage::is_short_id_shaped(&s.short_id),
                "minted short_id must be 8-char base-62: {:?}",
                s.short_id
            );
        }
        // Linear chain: S1->S0, S2->S1; S0 the sole root.
        assert!(storage::list_step_dependencies(&conn, &steps[0].id)
            .unwrap()
            .is_empty());
        assert_eq!(
            storage::list_step_dependencies(&conn, &steps[1].id).unwrap(),
            vec![steps[0].id.clone()]
        );
        assert_eq!(
            storage::list_step_dependencies(&conn, &steps[2].id).unwrap(),
            vec![steps[1].id.clone()]
        );
    }

    /// A DAG-aware (branched) bundle preserves the bundle's `short_id`s
    /// verbatim (never re-minted) and reproduces its explicit edges.
    #[test]
    fn test_dag_aware_bundle_preserves_short_ids_and_edges() {
        let conn = setup();
        let json = r#"{
            "ralph_rs_version": "0.1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "plan": {"slug": "branched", "branch_name": "b", "description": "d"},
            "steps": [
                {"title": "A", "description": "d", "short_id": "aaaaaaaa"},
                {"title": "B", "description": "d", "short_id": "bbbbbbbb", "depends_on": ["aaaaaaaa"]},
                {"title": "C", "description": "d", "short_id": "cccccccc", "depends_on": ["aaaaaaaa", "bbbbbbbb"]}
            ]
        }"#;
        let data: ImportedPlan = serde_json::from_str(json).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/proj",
            strict: false,
        };
        let plan_id = import_plan_from_data(&conn, &data, &options).unwrap();
        let steps = storage::list_steps(&conn, &plan_id).unwrap();
        // short_ids preserved verbatim, in array order.
        assert_eq!(steps[0].short_id, "aaaaaaaa");
        assert_eq!(steps[1].short_id, "bbbbbbbb");
        assert_eq!(steps[2].short_id, "cccccccc");
        // Exactly the bundle's edges.
        let mut expected = std::collections::BTreeSet::new();
        expected.insert(("bbbbbbbb".to_string(), "aaaaaaaa".to_string()));
        expected.insert(("cccccccc".to_string(), "aaaaaaaa".to_string()));
        expected.insert(("cccccccc".to_string(), "bbbbbbbb".to_string()));
        assert_eq!(edge_set(&conn, &plan_id), expected);
    }

    /// Round-trip guarantee (§13.3): exporting then importing a genuinely
    /// branched plan reproduces the same DAG — identical edges, roots,
    /// step order, and `short_id`s.
    #[test]
    fn test_roundtrip_branched_plan_reproduces_dag() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn, "diamond", "/tmp/src", "b", "d", None, None, &[],
        )
        .unwrap();
        let mk = |t: &str| {
            storage::create_step(
                &conn, &plan.id, t, "d", None, None, &[], None, None, None, None,
            )
            .unwrap()
            .0
        };
        let a = mk("A");
        let b = mk("B");
        let c = mk("C");
        let d = mk("D");
        // Diamond: B->A, C->A, D->B, D->C.
        storage::add_step_dependency(&conn, &b.id, &a.id).unwrap();
        storage::add_step_dependency(&conn, &c.id, &a.id).unwrap();
        storage::add_step_dependency(&conn, &d.id, &b.id).unwrap();
        storage::add_step_dependency(&conn, &d.id, &c.id).unwrap();

        let orig_edges = edge_set(&conn, &plan.id);

        // Export through the real pipeline (chain suppression decided
        // inside export_plan), then re-import into a fresh project.
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("diamond.json");
        export::export_plan(&conn, "diamond", "/tmp/src", Some(&file_path)).unwrap();
        let imported_data = read_plan_file(&file_path).unwrap();
        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();

        let imported_steps = storage::list_steps(&conn, &imported_id).unwrap();
        // Step order preserved.
        let titles: Vec<&str> = imported_steps.iter().map(|s| s.title.as_str()).collect();
        assert_eq!(titles, vec!["A", "B", "C", "D"]);
        // short_ids preserved.
        assert_eq!(imported_steps[0].short_id, a.short_id);
        assert_eq!(imported_steps[1].short_id, b.short_id);
        assert_eq!(imported_steps[2].short_id, c.short_id);
        assert_eq!(imported_steps[3].short_id, d.short_id);
        // Same edges and same single root.
        assert_eq!(edge_set(&conn, &imported_id), orig_edges);
        let roots: Vec<&str> = imported_steps
            .iter()
            .filter(|s| storage::list_step_dependencies(&conn, &s.id).unwrap().is_empty())
            .map(|s| s.short_id.as_str())
            .collect();
        assert_eq!(roots, vec![a.short_id.as_str()]);
    }

    /// §13.3 defect fix: a multi-root **no-edge** plan must round-trip with
    /// **no `step_dependencies`** and its `short_id`s **preserved** — it must
    /// NOT be misclassified as legacy (which would re-mint short_ids and fuse
    /// the independent roots into a synthetic linear chain).
    #[test]
    fn test_roundtrip_multi_root_no_edge_plan_stays_no_edge() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn, "multiroot", "/tmp/src", "b", "d", None, None, &[],
        )
        .unwrap();
        let mk = |t: &str| {
            storage::create_step(
                &conn, &plan.id, t, "d", None, None, &[], None, None, None, None,
            )
            .unwrap()
            .0
        };
        // Three independent roots, zero edges.
        let r0 = mk("R0");
        let r1 = mk("R1");
        let r2 = mk("R2");

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("multiroot.json");
        export::export_plan(&conn, "multiroot", "/tmp/src", Some(&file_path)).unwrap();
        let imported_data = read_plan_file(&file_path).unwrap();

        // Bundle: short_id on every step, depends_on absent everywhere.
        assert!(
            imported_data.steps.iter().all(|s| s.short_id.is_some()),
            "every step carries a short_id"
        );
        assert!(
            imported_data.steps.iter().all(|s| s.depends_on.is_empty()),
            "a no-edge DAG carries no depends_on"
        );
        // It is DAG-aware (carries short_ids) — NOT legacy.
        assert_eq!(classify_bundle(&imported_data.steps), BundleShape::DagAware);

        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();
        let steps = storage::list_steps(&conn, &imported_id).unwrap();
        let titles: Vec<&str> = steps.iter().map(|s| s.title.as_str()).collect();
        assert_eq!(titles, vec!["R0", "R1", "R2"]);

        // short_ids preserved verbatim — NOT re-minted.
        assert_eq!(steps[0].short_id, r0.short_id);
        assert_eq!(steps[1].short_id, r1.short_id);
        assert_eq!(steps[2].short_id, r2.short_id);

        // Zero step_dependencies: every step is still an independent root.
        assert!(
            edge_set(&conn, &imported_id).is_empty(),
            "no synthetic linear chain may be created for a no-edge DAG"
        );
        for s in &steps {
            assert!(
                storage::list_step_dependencies(&conn, &s.id)
                    .unwrap()
                    .is_empty(),
                "step {} must remain a root",
                s.title
            );
        }
    }

    /// Unit-level guard on the classification defect: a bundle that carries
    /// `short_id` on every step but no `depends_on` (a no-edge multi-root
    /// DAG, or a chain-suppressed-style linear export) is **DAG-aware**, not
    /// legacy; only a bundle with no `short_id` and no `depends_on` anywhere
    /// is legacy.
    #[test]
    fn test_classify_bundle_short_id_presence_not_depends_on() {
        // No short_id, no depends_on → true legacy.
        let legacy = serde_json::from_str::<ImportedPlan>(
            r#"{"ralph_rs_version":"0.1.0","exported_at":"x",
                "plan":{"slug":"s","branch_name":"b","description":"d"},
                "steps":[{"title":"A","description":"d"},
                         {"title":"B","description":"d"}]}"#,
        )
        .unwrap();
        assert_eq!(classify_bundle(&legacy.steps), BundleShape::Legacy);

        // short_id on every step, NO depends_on anywhere → DAG-aware
        // (this is the case the old `any non-empty depends_on` rule got
        // wrong by treating it as legacy).
        let no_edge = serde_json::from_str::<ImportedPlan>(
            r#"{"ralph_rs_version":"0.1.0","exported_at":"x",
                "plan":{"slug":"s","branch_name":"b","description":"d"},
                "steps":[{"title":"A","description":"d","short_id":"aaaaaaaa"},
                         {"title":"B","description":"d","short_id":"bbbbbbbb"}]}"#,
        )
        .unwrap();
        assert_eq!(classify_bundle(&no_edge.steps), BundleShape::DagAware);

        // Explicit edges → DAG-aware (unchanged behavior).
        let branched = serde_json::from_str::<ImportedPlan>(
            r#"{"ralph_rs_version":"0.1.0","exported_at":"x",
                "plan":{"slug":"s","branch_name":"b","description":"d"},
                "steps":[{"title":"A","description":"d","short_id":"aaaaaaaa"},
                         {"title":"B","description":"d","short_id":"bbbbbbbb",
                          "depends_on":["aaaaaaaa"]}]}"#,
        )
        .unwrap();
        assert_eq!(classify_bundle(&branched.steps), BundleShape::DagAware);
    }

    /// Round-trip for a *linear* plan (§13.3, defect fix): the export now
    /// emits the real chain edges (no suppression), so re-import takes the
    /// DAG-aware path — the exact chain is reproduced **and** every
    /// `short_id` is preserved verbatim (no re-minting). This is the
    /// `short_id`-stability guarantee the prior chain-suppression broke.
    #[test]
    fn test_roundtrip_linear_plan_preserves_short_ids_and_exact_chain() {
        let conn = setup();
        let plan = storage::create_plan(
            &conn, "linear", "/tmp/src", "b", "d", None, None, &[],
        )
        .unwrap();
        let mk = |t: &str| {
            storage::create_step(
                &conn, &plan.id, t, "d", None, None, &[], None, None, None, None,
            )
            .unwrap()
            .0
        };
        let s0 = mk("S0");
        let s1 = mk("S1");
        let s2 = mk("S2");
        storage::add_step_dependency(&conn, &s1.id, &s0.id).unwrap();
        storage::add_step_dependency(&conn, &s2.id, &s1.id).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("linear.json");
        export::export_plan(&conn, "linear", "/tmp/src", Some(&file_path)).unwrap();
        // The chain is now emitted: the root omits depends_on, the rest
        // carry their single predecessor edge.
        let imported_data = read_plan_file(&file_path).unwrap();
        assert!(
            imported_data.steps[0].depends_on.is_empty(),
            "S0 is the root"
        );
        assert_eq!(
            imported_data.steps[1].depends_on,
            vec![s0.short_id.clone()]
        );
        assert_eq!(
            imported_data.steps[2].depends_on,
            vec![s1.short_id.clone()]
        );
        // It is classified DAG-aware (carries short_ids), not legacy.
        assert_eq!(classify_bundle(&imported_data.steps), BundleShape::DagAware);

        let options = ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/dst",
            strict: false,
        };
        let imported_id = import_plan_from_data(&conn, &imported_data, &options).unwrap();
        let steps = storage::list_steps(&conn, &imported_id).unwrap();
        let titles: Vec<&str> = steps.iter().map(|s| s.title.as_str()).collect();
        assert_eq!(titles, vec!["S0", "S1", "S2"]);
        // short_ids preserved verbatim — NOT re-minted.
        assert_eq!(steps[0].short_id, s0.short_id);
        assert_eq!(steps[1].short_id, s1.short_id);
        assert_eq!(steps[2].short_id, s2.short_id);
        // Exact chain reproduced: S0 root, S1→S0, S2→S1.
        assert!(storage::list_step_dependencies(&conn, &steps[0].id)
            .unwrap()
            .is_empty());
        assert_eq!(
            storage::list_step_dependencies(&conn, &steps[1].id).unwrap(),
            vec![steps[0].id.clone()]
        );
        assert_eq!(
            storage::list_step_dependencies(&conn, &steps[2].id).unwrap(),
            vec![steps[1].id.clone()]
        );
    }

    /// Helper: a DAG-aware bundle JSON with caller-supplied step entries.
    fn dag_bundle(steps_json: &str) -> ImportedPlan {
        let json = format!(
            r#"{{
                "ralph_rs_version": "0.1.0",
                "exported_at": "2025-01-01T00:00:00Z",
                "plan": {{"slug": "bad", "branch_name": "b", "description": "d"}},
                "steps": [{steps_json}]
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    fn opts() -> ImportOptions<'static> {
        ImportOptions {
            slug: None,
            branch: None,
            harness: None,
            project: "/tmp/bad",
            strict: false,
        }
    }

    /// Rule 1: a `depends_on` entry that resolves to no in-bundle
    /// `short_id` aborts the import; no partial plan is written.
    #[test]
    fn test_import_rejects_dangling_edge() {
        let conn = setup();
        let data = dag_bundle(
            r#"{"title": "A", "short_id": "aaaaaaaa"},
               {"title": "B", "short_id": "bbbbbbbb", "depends_on": ["zzzzzzzz"]}"#,
        );
        let err = import_plan_from_data(&conn, &data, &opts()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("zzzzzzzz"), "message must name the bad id: {msg}");
        assert!(msg.contains("dangling"), "message must cite the rule: {msg}");
        assert!(storage::get_plan_by_slug(&conn, "bad", "/tmp/bad")
            .unwrap()
            .is_none());
    }

    /// Rule 2: duplicate `short_id`s within the bundle abort the import.
    #[test]
    fn test_import_rejects_duplicate_short_id() {
        let conn = setup();
        let data = dag_bundle(
            r#"{"title": "A", "short_id": "samesame"},
               {"title": "B", "short_id": "samesame"},
               {"title": "C", "short_id": "cccccccc", "depends_on": ["samesame"]}"#,
        );
        let err = import_plan_from_data(&conn, &data, &opts()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate short_id"), "got: {msg}");
        assert!(msg.contains("samesame"), "got: {msg}");
        assert!(storage::get_plan_by_slug(&conn, "bad", "/tmp/bad")
            .unwrap()
            .is_none());
    }

    /// Rule 0: a DAG-aware bundle with a step missing its `short_id`
    /// aborts (a branched exporter always emits one for every step).
    #[test]
    fn test_import_rejects_missing_short_id_in_dag_bundle() {
        let conn = setup();
        let data = dag_bundle(
            r#"{"title": "A"},
               {"title": "B", "short_id": "bbbbbbbb", "depends_on": ["aaaaaaaa"]}"#,
        );
        let err = import_plan_from_data(&conn, &data, &opts()).unwrap_err();
        assert!(
            err.to_string().contains("no short_id"),
            "got: {err}"
        );
        assert!(storage::get_plan_by_slug(&conn, "bad", "/tmp/bad")
            .unwrap()
            .is_none());
    }

    /// Rule 3: a dependency cycle aborts the import.
    #[test]
    fn test_import_rejects_cycle() {
        let conn = setup();
        let data = dag_bundle(
            r#"{"title": "A", "short_id": "aaaaaaaa", "depends_on": ["bbbbbbbb"]},
               {"title": "B", "short_id": "bbbbbbbb", "depends_on": ["aaaaaaaa"]}"#,
        );
        let err = import_plan_from_data(&conn, &data, &opts()).unwrap_err();
        assert!(err.to_string().contains("cycle"), "got: {err}");
        assert!(storage::get_plan_by_slug(&conn, "bad", "/tmp/bad")
            .unwrap()
            .is_none());
    }

    /// Rule 3: a self-edge is reported as a cycle (mirrors
    /// `would_create_step_cycle`'s self-edge early return).
    #[test]
    fn test_import_rejects_self_edge() {
        let conn = setup();
        let data = dag_bundle(
            r#"{"title": "A", "short_id": "aaaaaaaa", "depends_on": ["aaaaaaaa"]},
               {"title": "B", "short_id": "bbbbbbbb"}"#,
        );
        let err = import_plan_from_data(&conn, &data, &opts()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("itself"), "got: {msg}");
        assert!(msg.contains("aaaaaaaa"), "got: {msg}");
        assert!(storage::get_plan_by_slug(&conn, "bad", "/tmp/bad")
            .unwrap()
            .is_none());
    }

    /// Rule 4 (`≥1 root`) is a defensive backstop: once rules 1–3 hold it
    /// is unreachable, because a finite graph whose edges are all internal
    /// and acyclic always has a source. The only way to make every step
    /// have a dependency without a dangling edge is to introduce a cycle,
    /// which rule 3 catches first. This asserts that ordered behavior — a
    /// rootless (mutually-dependent) graph is rejected by the validator.
    #[test]
    fn test_validate_rejects_rootless_dag() {
        let steps = vec![
            ImportedStep {
                title: "A".into(),
                description: String::new(),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                max_retries: None,
                model: None,
                change_policy: crate::plan::ChangePolicy::default(),
                tags: vec![],
                retry_strategy: None,
                short_id: Some("aaaaaaaa".into()),
                depends_on: vec!["bbbbbbbb".into()],
            },
            ImportedStep {
                title: "B".into(),
                description: String::new(),
                agent: None,
                harness: None,
                acceptance_criteria: vec![],
                max_retries: None,
                model: None,
                change_policy: crate::plan::ChangePolicy::default(),
                tags: vec![],
                retry_strategy: None,
                short_id: Some("bbbbbbbb".into()),
                depends_on: vec!["aaaaaaaa".into()],
            },
        ];
        let err = validate_dag_aware_steps(&steps).unwrap_err();
        assert!(err.to_string().contains("cycle"));
    }
}
