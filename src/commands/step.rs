// Step CLI command implementations (CRUD, move, hooks)

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::{self, Write};

use crate::frac_index;
use crate::hook_library::{self, Lifecycle};
use crate::output::{self, OutputContext, OutputFormat};
use crate::storage;

use super::resolve_step;

// ---------------------------------------------------------------------------
// Step commands
// ---------------------------------------------------------------------------

pub fn step_list(conn: &Connection, plan_slug: &str, project: &str, out: &OutputContext) -> Result<()> {
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
        println!("No steps in plan '{}'.", plan_slug);
        return Ok(());
    }

    println!(
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

    println!(
        "{} Added step #{}: {}",
        output::check_icon(out.color),
        pos,
        output::bold(&step.title, out.color),
    );
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
        print!("Remove step #{} '{}'? [y/N] ", display_num, step.title);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_step(conn, &step.id)?;
    println!(
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
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_num, step_id)?;

    if title.is_none() && description.is_none() {
        bail!("Nothing to edit: provide --title and/or --description");
    }

    storage::update_step_fields(conn, &step.id, title, description)?;
    println!(
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
    println!(
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
        println!("Step is already at position {}.", to);
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
    println!(
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
    println!(
        "Attached hook '{hook_name}' to step {display_num} of '{plan_slug}' at {lifecycle}"
    );
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

    let removed = storage::detach_hook(conn, &plan.id, Some(&step.id), lifecycle.as_str(), hook_name)?;
    if removed == 0 {
        bail!("No hook '{hook_name}' attached to step {display_num} at {lifecycle}");
    }
    println!("Detached hook '{hook_name}' from step {display_num} of '{plan_slug}'");
    Ok(())
}
