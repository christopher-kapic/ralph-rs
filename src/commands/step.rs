// Step CLI command implementations (CRUD, move, hooks)

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::Read;

use crate::frac_index;
use crate::hook_library::{self, Lifecycle};
use crate::import::ImportedStep;
use crate::output::{self, OutputContext, OutputFormat};
use crate::storage;

use super::resolve_step;

// ---------------------------------------------------------------------------
// Step commands
// ---------------------------------------------------------------------------

pub fn step_list(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    if out.format == OutputFormat::Json {
        let summaries: Vec<output::StepSummary> =
            steps.iter().map(output::StepSummary::from).collect();
        println!("{}", serde_json::to_string(&summaries)?);
        return Ok(());
    }

    if steps.is_empty() {
        eprintln!("No steps in plan '{}'.", plan_slug);
        return Ok(());
    }

    eprintln!(
        "Steps for {} ({} total):",
        output::bold(plan_slug, out.color),
        steps.len()
    );
    for (i, step) in steps.iter().enumerate() {
        println!(
            "  {:>3}. {} {}  [{}]",
            i + 1,
            output::status_icon(step.status, out.color),
            output::bold(&step.title, out.color),
            output::colored_status(step.status, out.color),
        );
        if !step.description.is_empty() {
            println!("       {}", step.description);
        }
        if step.attempts > 0 {
            println!("       attempts: {}", step.attempts);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn step_add(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    title: &str,
    description: Option<&str>,
    after: Option<usize>,
    agent: Option<&str>,
    harness: Option<&str>,
    criteria: &[String],
    max_retries: Option<i32>,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let desc = description.unwrap_or("");

    let (step, pos) = if let Some(after_pos) = after {
        // Insert after a specific position using fractional indexing
        let steps = storage::list_steps(conn, &plan.id)?;
        if after_pos > steps.len() {
            bail!(
                "Position {} is out of range (plan has {} steps)",
                after_pos,
                steps.len()
            );
        }

        let sort_key = if after_pos == 0 {
            // Insert before the first step
            if steps.is_empty() {
                frac_index::initial_key()
            } else {
                let first_key = &steps[0].sort_key;
                if first_key.as_str() > "0" {
                    frac_index::key_between("0", first_key)
                } else {
                    "00".to_string()
                }
            }
        } else if after_pos == steps.len() {
            // Append at end
            frac_index::key_after(&steps[steps.len() - 1].sort_key)
        } else {
            // Insert between after_pos-1 and after_pos
            let before = &steps[after_pos - 1].sort_key;
            let after_key = &steps[after_pos].sort_key;
            frac_index::key_between(before, after_key)
        };

        storage::create_step_at(
            conn,
            &plan.id,
            &sort_key,
            title,
            desc,
            agent,
            harness,
            criteria,
            max_retries,
        )?
    } else {
        // Append at the end (default)
        storage::create_step(
            conn,
            &plan.id,
            title,
            desc,
            agent,
            harness,
            criteria,
            max_retries,
        )?
    };

    eprintln!(
        "{} Added step #{}: {}",
        output::check_icon(out.color),
        pos,
        output::bold(&step.title, out.color),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Bulk step add via JSON (stdin or file)
// ---------------------------------------------------------------------------

/// Parse the bulk-import payload. Accepts either a JSON array of step objects
/// or a single object; the latter is wrapped into a 1-element Vec. Each object
/// must at minimum provide `title`; all other fields default via serde.
///
/// Kept as a free function so the unit tests can exercise it without touching
/// stdin or the filesystem.
pub(crate) fn parse_bulk_steps(raw: &str) -> Result<Vec<ImportedStep>> {
    // Try array first, fall back to single object.
    if let Ok(arr) = serde_json::from_str::<Vec<ImportedStep>>(raw) {
        return Ok(arr);
    }
    let single: ImportedStep = serde_json::from_str(raw)
        .context("Invalid --import-json payload: expected a JSON array of step objects or a single step object (each must have a `title` field)")?;
    Ok(vec![single])
}

/// Bulk-add steps from a JSON source. `source` is either `-` (stdin) or a
/// filesystem path. All inserts happen inside a single DB transaction so
/// the batch is atomic: any failure rolls the whole batch back.
pub fn step_add_bulk(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    source: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    // Read the JSON payload from stdin or a file.
    let raw = if source == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("Failed to read --import-json payload from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("Failed to read --import-json file: {source}"))?
    };

    let steps = parse_bulk_steps(&raw)?;

    if steps.is_empty() {
        bail!("--import-json payload contained no steps");
    }

    // Validate each step up front so we fail before touching the database.
    for (i, s) in steps.iter().enumerate() {
        if s.title.trim().is_empty() {
            bail!("Step #{} is missing a non-empty `title`", i + 1);
        }
    }

    // Insert atomically inside a transaction. On any error, roll back.
    conn.execute_batch("BEGIN;")
        .context("Failed to begin bulk-import transaction")?;

    let mut inserted: Vec<(crate::plan::Step, usize)> = Vec::with_capacity(steps.len());
    let insert_result: Result<()> = (|| {
        for s in &steps {
            let (step, pos) = storage::create_step(
                conn,
                &plan.id,
                &s.title,
                &s.description,
                s.agent.as_deref(),
                s.harness.as_deref(),
                &s.acceptance_criteria,
                s.max_retries,
            )?;
            inserted.push((step, pos));
        }
        Ok(())
    })();

    if let Err(e) = insert_result {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(e).context("Bulk step insert failed; rolled back (no steps added)");
    }

    conn.execute_batch("COMMIT;")
        .context("Failed to commit bulk-import transaction")?;

    // Emit results.
    if out.format == OutputFormat::Json {
        let summaries: Vec<output::StepSummary> = inserted
            .iter()
            .map(|(s, _)| output::StepSummary::from(s))
            .collect();
        println!("{}", serde_json::to_string(&summaries)?);
    } else {
        for (step, pos) in &inserted {
            eprintln!(
                "{} Added step #{}: {}",
                output::check_icon(out.color),
                pos,
                output::bold(&step.title, out.color),
            );
        }
    }

    Ok(())
}

pub fn step_remove(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    force: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    if !force {
        let prompt = format!("Remove step #{} '{}'?", display_num, step.title);
        if !output::confirm(&prompt)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_step(conn, &step.id)?;
    eprintln!(
        "{} Removed step #{}: {}",
        output::check_icon(out.color),
        display_num,
        step.title
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn step_edit(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    title: Option<&str>,
    description: Option<&str>,
    agent: Option<&str>,
    harness: Option<&str>,
    criteria: &[String],
    max_retries: Option<i32>,
    clear_max_retries: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    if title.is_none()
        && description.is_none()
        && agent.is_none()
        && harness.is_none()
        && criteria.is_empty()
        && max_retries.is_none()
        && !clear_max_retries
    {
        bail!(
            "Nothing to edit: provide at least one of --title, --description, --agent, --harness, --criteria, --max-retries, or --clear-max-retries"
        );
    }

    // We only pass non-None fields to the update function for fields the
    // user explicitly changed. The "None means don't change" rule applies
    // for agent/harness when the user passed the flag; empty string means
    // "clear".
    let agent_update = agent.map(|a| if a.is_empty() { None } else { Some(a) });

    let harness_update = harness.map(|h| if h.is_empty() { None } else { Some(h) });

    // For max_retries: Some(N) means set to N, clear_max_retries means
    // set to NULL (use plan default), None means don't change.
    let retries_update: Option<Option<i32>> = if clear_max_retries {
        Some(None) // Set to NULL
    } else {
        max_retries.map(Some) // Set to specific value
    };

    storage::update_step_fields_ext(
        conn,
        &step.id,
        title,
        description,
        agent_update,
        harness_update,
        if criteria.is_empty() {
            None
        } else {
            Some(criteria)
        },
        retries_update,
    )?;

    eprintln!(
        "{} Updated step #{}: {}",
        output::check_icon(out.color),
        display_num,
        title.unwrap_or(&step.title)
    );
    Ok(())
}

pub fn step_reset(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;
    storage::reset_step(conn, &step.id)?;
    eprintln!(
        "{} Reset step #{} '{}' to pending (0 attempts)",
        output::check_icon(out.color),
        display_num,
        step.title
    );
    Ok(())
}

pub fn step_move(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    to: usize,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    // Resolve the step and its current 1-based position.
    let (step, display_num) = if let Some(id) = step_id {
        let s = storage::get_step_by_id(conn, id)?
            .with_context(|| format!("Step not found with id: {id}"))?;
        if s.plan_id != plan.id {
            bail!("Step {id} does not belong to this plan");
        }
        let pos = steps
            .iter()
            .position(|x| x.id == s.id)
            .map(|i| i + 1)
            .unwrap_or(0);
        (s, pos)
    } else if let Some(num) = step_num {
        if num == 0 || num > steps.len() {
            bail!(
                "Step {} is out of range (plan has {} steps)",
                num,
                steps.len()
            );
        }
        (steps[num - 1].clone(), num)
    } else {
        bail!("Provide either a step number or --step-id");
    };

    if to == 0 || to > steps.len() {
        bail!(
            "Target position {} is out of range (plan has {} steps)",
            to,
            steps.len()
        );
    }
    if display_num == to {
        eprintln!("Step is already at position {}.", to);
        return Ok(());
    }

    // Calculate the new sort_key for the target position.
    // We need a key that places the step at position `to` (1-based)
    // after removing it from its current position.
    let target_idx = to - 1; // 0-based target index

    // Build a list of sort keys excluding the step being moved
    let other_keys: Vec<&str> = steps
        .iter()
        .filter(|s| s.id != step.id)
        .map(|s| s.sort_key.as_str())
        .collect();

    let new_sort_key = if target_idx == 0 {
        // Move to first position: need a key before the first remaining step
        if other_keys.is_empty() {
            frac_index::initial_key()
        } else {
            let first = other_keys[0];
            // Use "0" as a synthetic lower bound; it sorts before any key
            // starting with a digit > '0' or a letter.
            if first > "0" {
                frac_index::key_between("0", first)
            } else {
                // Extremely unlikely: first key is "0". Prepend with shorter key.
                "00".to_string()
            }
        }
    } else if target_idx >= other_keys.len() {
        // Move to last position
        frac_index::key_after(other_keys[other_keys.len() - 1])
    } else {
        // Move between two existing steps
        let before = other_keys[target_idx - 1];
        let after_key = other_keys[target_idx];
        frac_index::key_between(before, after_key)
    };

    storage::update_step_sort_key(conn, &step.id, &new_sort_key)?;
    eprintln!(
        "{} Moved step '{}' to position {}",
        output::check_icon(out.color),
        step.title,
        to
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Step hook attachment commands
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn cmd_step_set_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    // Warn if the hook isn't in the library (user can still attach — it will
    // be warn-and-skipped at run time until they import it).
    if hook_library::try_load(hook_name)?.is_none() {
        eprintln!(
            "Warning: hook '{hook_name}' is not in the local library. It will be skipped at run time until imported."
        );
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    storage::attach_hook_to_step(conn, &plan.id, &step.id, lifecycle.as_str(), hook_name)?;
    eprintln!("Attached hook '{hook_name}' to step {display_num} of '{plan_slug}' at {lifecycle}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn cmd_step_unset_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: Option<usize>,
    step_id: Option<&str>,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    let removed = storage::detach_hook(
        conn,
        &plan.id,
        Some(&step.id),
        lifecycle.as_str(),
        hook_name,
    )?;
    if removed == 0 {
        bail!("No hook '{hook_name}' attached to step {display_num} at {lifecycle}");
    }
    eprintln!("Detached hook '{hook_name}' from step {display_num} of '{plan_slug}'");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::plan_create;
    use crate::db;
    use crate::output::OutputFormat;

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    fn setup_with_plan() -> (Connection, String) {
        let conn = db::open_memory().expect("open_memory");
        let project = "/tmp/bulk-test".to_string();
        plan_create(
            &conn,
            "bulk-plan",
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
        (conn, project)
    }

    #[test]
    fn test_parse_bulk_steps_array() {
        let json = r#"[
            {"title": "a", "description": "first"},
            {"title": "b", "acceptance_criteria": ["passes"], "max_retries": 5}
        ]"#;
        let parsed = parse_bulk_steps(json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title, "a");
        assert_eq!(parsed[1].max_retries, Some(5));
    }

    #[test]
    fn test_parse_bulk_steps_single_object() {
        let json = r#"{"title": "lonely"}"#;
        let parsed = parse_bulk_steps(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].title, "lonely");
    }

    #[test]
    fn test_parse_bulk_steps_invalid_rejected() {
        // Missing `title`.
        let json = r#"[{"description": "no title"}]"#;
        assert!(parse_bulk_steps(json).is_err());
    }

    #[test]
    fn test_step_add_bulk_from_file_inserts_array() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("steps.json");
        let json = r#"[
            {
                "title": "Write tests",
                "description": "Cover the happy path",
                "acceptance_criteria": ["tests pass"],
                "max_retries": 2
            },
            {
                "title": "Implement feature",
                "agent": "claude-code",
                "harness": "claude-code"
            }
        ]"#;
        std::fs::write(&file, json).unwrap();

        step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 2, "both steps should have been inserted");
        assert_eq!(steps[0].title, "Write tests");
        assert_eq!(steps[0].description, "Cover the happy path");
        assert_eq!(steps[0].acceptance_criteria, vec!["tests pass".to_string()]);
        assert_eq!(steps[0].max_retries, Some(2));
        assert_eq!(steps[1].title, "Implement feature");
        assert_eq!(steps[1].agent.as_deref(), Some("claude-code"));
        assert_eq!(steps[1].harness.as_deref(), Some("claude-code"));
    }

    #[test]
    fn test_step_add_bulk_from_file_single_object() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("one.json");
        std::fs::write(&file, r#"{"title": "just one"}"#).unwrap();

        step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].title, "just one");
    }

    #[test]
    fn test_step_add_bulk_empty_title_fails_atomically() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bad.json");
        // Second entry has an empty title — the whole batch must fail and
        // leave no steps in the DB.
        let json = r#"[
            {"title": "ok"},
            {"title": ""}
        ]"#;
        std::fs::write(&file, json).unwrap();

        let result = step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        );
        assert!(result.is_err());

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(steps.is_empty(), "no steps should have been inserted");
    }
}
