// Step CLI command implementations (CRUD, move, hooks)

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::Read;

use crate::config::Config;
use crate::frac_index;
use crate::hook_library::{self, Lifecycle};
use crate::import::ImportedStep;
use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{ChangePolicy, Step, StepStatus};
use crate::storage;

use super::resolve_step;

// ---------------------------------------------------------------------------
// Step commands
// ---------------------------------------------------------------------------

/// Normalize user-supplied tags from a single CLI invocation.
///
/// Trims whitespace from each value, rejects empty/whitespace-only entries,
/// and rejects exact duplicates within the same invocation. Case is preserved
/// as the user typed it. Returns the normalized list ready to store.
pub(crate) fn normalize_tag_inputs(raw: &[String]) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    for t in raw {
        let trimmed = t.trim();
        if trimmed.is_empty() {
            bail!("Tag values cannot be empty or whitespace-only");
        }
        if out.iter().any(|existing| existing == trimmed) {
            bail!("Duplicate tag '{trimmed}' in this invocation");
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

/// Render a step's tags for plain-text output (e.g. `[FIX][REGRESSION]`).
///
/// Returns an empty string when the step has no tags so list rendering stays
/// unchanged for pre-V13 data and steps that never opted in.
pub(crate) fn render_tags_inline(step: &Step) -> String {
    if step.tags.is_empty() {
        return String::new();
    }
    let mut s = String::new();
    for t in &step.tags {
        s.push('[');
        s.push_str(t);
        s.push(']');
    }
    s
}

pub fn step_list(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    config: &Config,
    filter_tags: &[String],
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let all_steps = storage::list_steps(conn, &plan.id)?;

    // AND-filter: step must carry every requested tag (case-sensitive exact
    // match). No tags requested → no filtering, preserving the legacy shape.
    let steps: Vec<Step> = if filter_tags.is_empty() {
        all_steps
    } else {
        all_steps
            .into_iter()
            .filter(|s| filter_tags.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect()
    };

    if out.format == OutputFormat::Json {
        let summaries: Vec<output::StepSummary> =
            steps.iter().map(output::StepSummary::from).collect();
        println!("{}", serde_json::to_string(&summaries)?);
        return Ok(());
    }

    if steps.is_empty() {
        if filter_tags.is_empty() {
            eprintln!("No steps in plan '{}'.", plan_slug);
        } else {
            eprintln!(
                "No steps in plan '{}' matching tags {:?}.",
                plan_slug, filter_tags
            );
        }
        return Ok(());
    }

    eprintln!(
        "Steps for {} ({} total):",
        output::bold(plan_slug, out.color),
        steps.len()
    );
    for (i, step) in steps.iter().enumerate() {
        let policy_tag = if step.change_policy == ChangePolicy::Optional {
            " [optional]"
        } else {
            ""
        };
        let tags_inline = render_tags_inline(step);
        let tags_prefix = if tags_inline.is_empty() {
            String::new()
        } else {
            format!("{tags_inline} ")
        };
        let budget_tag = render_budget_tag(step, config);
        println!(
            "  {:>3}. {} {}{}{}  [{}]{}",
            i + 1,
            output::status_icon(step.status, out.color),
            tags_prefix,
            output::bold(&step.title, out.color),
            policy_tag,
            output::colored_status(step.status, out.color),
            budget_tag,
        );
        if !step.description.is_empty() {
            println!("       {}", step.description);
        }
    }

    Ok(())
}

/// Render the `(attempts: N/M)` tag shown at end of a step-list line.
///
/// Returns an empty string for the "noisy for the common case" rule:
/// a step that is still Pending with zero attempts and no custom
/// `max_retries` doesn't need the budget cluttering every row. As soon as the
/// step has been attempted (or failed/aborted/etc.) or the user explicitly
/// bound `max_retries`, the tag renders.
pub(crate) fn render_budget_tag(step: &Step, config: &Config) -> String {
    let show =
        step.attempts > 0 || step.status != StepStatus::Pending || step.max_retries.is_some();
    if !show {
        return String::new();
    }
    // Match executor.rs: max_attempts = max_retries.unwrap_or(default) + 1.
    let max_retries = step
        .max_retries
        .unwrap_or(config.max_retries_per_step as i32);
    let max_attempts = max_retries + 1;
    format!(" (attempts: {}/{})", step.attempts, max_attempts)
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
    model: Option<&str>,
    criteria: &[String],
    max_retries: Option<i32>,
    change_policy: Option<ChangePolicy>,
    tags: &[String],
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let desc = description.unwrap_or("");
    let normalized_tags = normalize_tag_inputs(tags)?;
    let tags_arg: Option<&[String]> = if normalized_tags.is_empty() {
        None
    } else {
        Some(&normalized_tags)
    };

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
                    frac_index::key_between("0", first_key)?
                } else {
                    "00".to_string()
                }
            }
        } else if after_pos == steps.len() {
            // Append at end
            frac_index::key_after(&steps[steps.len() - 1].sort_key)?
        } else {
            // Insert between after_pos-1 and after_pos
            let before = &steps[after_pos - 1].sort_key;
            let after_key = &steps[after_pos].sort_key;
            frac_index::key_between(before, after_key)?
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
            model,
            change_policy,
            tags_arg,
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
            model,
            change_policy,
            tags_arg,
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
            let tags_arg: Option<&[String]> = if s.tags.is_empty() {
                None
            } else {
                Some(&s.tags)
            };
            let (step, pos) = storage::create_step(
                conn,
                &plan.id,
                &s.title,
                &s.description,
                s.agent.as_deref(),
                s.harness.as_deref(),
                &s.acceptance_criteria,
                s.max_retries,
                s.model.as_deref(),
                Some(s.change_policy),
                tags_arg,
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
    model: Option<&str>,
    criteria: &[String],
    max_retries: Option<i32>,
    clear_max_retries: bool,
    change_policy: Option<ChangePolicy>,
    tags: &[String],
    clear_tags: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    if title.is_none()
        && description.is_none()
        && agent.is_none()
        && harness.is_none()
        && model.is_none()
        && criteria.is_empty()
        && max_retries.is_none()
        && !clear_max_retries
        && change_policy.is_none()
        && tags.is_empty()
        && !clear_tags
    {
        bail!(
            "Nothing to edit: provide at least one of --title, --description, --agent, --harness, --model, --criteria, --max-retries, --clear-max-retries, --change-policy, --tag, or --clear-tags"
        );
    }

    // We only pass non-None fields to the update function for fields the
    // user explicitly changed. The "None means don't change" rule applies
    // for agent/harness/model when the user passed the flag; empty string means
    // "clear".
    let agent_update = agent.map(|a| if a.is_empty() { None } else { Some(a) });

    let harness_update = harness.map(|h| if h.is_empty() { None } else { Some(h) });

    let model_update = model.map(|m| if m.is_empty() { None } else { Some(m) });

    // For max_retries: Some(N) means set to N, clear_max_retries means
    // set to NULL (use plan default), None means don't change.
    let retries_update: Option<Option<i32>> = if clear_max_retries {
        Some(None) // Set to NULL
    } else {
        max_retries.map(Some) // Set to specific value
    };

    // Tags: `--clear-tags` substitutes an empty list, any `--tag` invocation
    // replaces the existing list wholesale after normalization, otherwise
    // don't change the stored tags.
    let normalized_tags = if tags.is_empty() {
        Vec::new()
    } else {
        normalize_tag_inputs(tags)?
    };
    let tags_update: Option<&[String]> = if clear_tags {
        Some(&[])
    } else if !tags.is_empty() {
        Some(&normalized_tags)
    } else {
        None
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
        model_update,
        change_policy,
        tags_update,
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
                frac_index::key_between("0", first)?
            } else {
                // Extremely unlikely: first key is "0". Prepend with shorter key.
                "00".to_string()
            }
        }
    } else if target_idx >= other_keys.len() {
        // Move to last position
        frac_index::key_after(other_keys[other_keys.len() - 1])?
    } else {
        // Move between two existing steps
        let before = other_keys[target_idx - 1];
        let after_key = other_keys[target_idx];
        frac_index::key_between(before, after_key)?
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
    println!("Attached hook '{hook_name}' to step {display_num} of '{plan_slug}' at {lifecycle}");
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
    println!("Detached hook '{hook_name}' from step {display_num} of '{plan_slug}'");
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

    // -- step-list attempt budget ------------------------------------------

    fn default_config() -> Config {
        Config {
            max_retries_per_step: 3, // explicit default budget for the test
            ..Config::default()
        }
    }

    #[test]
    fn test_step_list_shows_attempts_budget_when_relevant() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "With custom retries",
            None,
            None,
            None,
            None,
            None,
            &[],
            Some(3),
            None,
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        // Simulate attempts=2 on that step.
        conn.execute(
            "UPDATE steps SET attempts = 2 WHERE id = ?1",
            rusqlite::params![steps[0].id],
        )
        .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        // Inspect the budget tag directly — no need to capture stdout for a
        // format contract that's fully rendered by render_budget_tag.
        let tag = render_budget_tag(&steps[0], &default_config());
        assert_eq!(tag, " (attempts: 2/4)", "tag was: {tag:?}");
    }

    #[test]
    fn test_step_list_omits_budget_for_pending_default_steps() {
        let (conn, project) = setup_with_plan();
        // No max_retries override, no attempts yet, Pending.
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "Plain pending",
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
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let tag = render_budget_tag(&steps[0], &default_config());
        assert_eq!(
            tag, "",
            "pending default-retry step should not render the budget tag; got {tag:?}"
        );
    }

    #[test]
    fn test_step_list_shows_budget_after_attempts_even_without_override() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "No override",
            None,
            None,
            None,
            None,
            None,
            &[],
            None, // no max_retries override — falls back to config default.
            None,
            &[],
            &test_out(),
        )
        .unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        conn.execute(
            "UPDATE steps SET attempts = 1 WHERE id = ?1",
            rusqlite::params![steps[0].id],
        )
        .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        let tag = render_budget_tag(&steps[0], &default_config());
        // Default config has max_retries_per_step=3 → max_attempts=4.
        assert_eq!(tag, " (attempts: 1/4)");
    }

    // -- Tag tests ---------------------------------------------------------

    /// Helper: invoke `step_add` with a minimum set of args plus user-provided tags.
    fn add_with_tags(conn: &Connection, project: &str, title: &str, tags: &[String]) {
        step_add(
            conn,
            "bulk-plan",
            project,
            title,
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            tags,
            &test_out(),
        )
        .unwrap();
    }

    #[test]
    fn test_step_add_with_tags() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "REGRESSION".to_string()];
        add_with_tags(&conn, &project, "tagged", &tags);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].tags, tags);
    }

    #[test]
    fn test_step_add_rejects_empty_tag() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "  ".to_string()];
        let err = step_add(
            &conn,
            "bulk-plan",
            &project,
            "t",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            &tags,
            &test_out(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_step_add_rejects_duplicate_tag_in_same_invocation() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "FIX".to_string()];
        let err = step_add(
            &conn,
            "bulk-plan",
            &project,
            "t",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            &tags,
            &test_out(),
        )
        .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn test_step_edit_replaces_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(
            &conn,
            &project,
            "t",
            &["INITIAL".to_string(), "OTHER".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Replace with a brand-new set.
        let new_tags = vec!["REVIEW".to_string()];
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
            None,
            &new_tags,
            false,
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].tags, new_tags);
    }

    #[test]
    fn test_step_edit_clear_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "t", &["FIX".to_string()]);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            false,
            None,
            &[],
            true, // clear_tags
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(steps[0].tags.is_empty());
    }

    #[test]
    fn test_step_edit_no_tag_flag_leaves_tags_unchanged() {
        let (conn, project) = setup_with_plan();
        let original = vec!["KEEP".to_string()];
        add_with_tags(&conn, &project, "t", &original);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Edit just the title — tags should be unchanged.
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some(1),
            None,
            Some("new title"),
            None,
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

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "new title");
        assert_eq!(steps[0].tags, original);
    }

    #[test]
    fn test_step_list_filter_by_tag() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "A", &["FIX".to_string()]);
        add_with_tags(&conn, &project, "B", &["REVIEW".to_string()]);
        add_with_tags(
            &conn,
            &project,
            "C",
            &["FIX".to_string(), "URGENT".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // No filter -> all three.
        let all = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(all.len(), 3);

        // Single-tag filter picks the two steps that carry FIX.
        let filter = ["FIX".to_string()];
        let filtered: Vec<&Step> = all
            .iter()
            .filter(|s| filter.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect();
        assert_eq!(filtered.len(), 2);
        let titles: Vec<&str> = filtered.iter().map(|s| s.title.as_str()).collect();
        assert!(titles.contains(&"A"));
        assert!(titles.contains(&"C"));
    }

    #[test]
    fn test_step_list_filter_requires_all_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "A", &["FIX".to_string()]);
        add_with_tags(
            &conn,
            &project,
            "B",
            &["FIX".to_string(), "URGENT".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Demand BOTH `FIX` and `URGENT` -> only B matches.
        let filter = ["FIX".to_string(), "URGENT".to_string()];
        let all = storage::list_steps(&conn, &plan.id).unwrap();
        let filtered: Vec<&Step> = all
            .iter()
            .filter(|s| filter.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].title, "B");
    }

    #[test]
    fn test_render_tags_inline() {
        let (conn, project) = setup_with_plan();
        add_with_tags(
            &conn,
            &project,
            "tagged",
            &["FIX".to_string(), "REGRESSION".to_string()],
        );
        add_with_tags(&conn, &project, "untagged", &[]);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        assert_eq!(render_tags_inline(&steps[0]), "[FIX][REGRESSION]");
        assert_eq!(render_tags_inline(&steps[1]), "");
    }
}
