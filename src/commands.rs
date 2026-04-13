// Plan and step CLI command implementations

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::{self, Write};
use std::path::Path;

use crate::config;
use crate::db;
use crate::frac_index;
use crate::hook_library::{self, Hook, HookBundle, Lifecycle, Scope};
use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{ExecutionLog, PlanStatus, StepStatus};
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

use crate::plan::Step;

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
// Plan commands
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn plan_create(
    conn: &Connection,
    slug: &str,
    project: &str,
    description: Option<&str>,
    branch: Option<&str>,
    harness: Option<&str>,
    agent: Option<&str>,
    tests: &[String],
    depends_on: &[String],
    out: &OutputContext,
) -> Result<()> {
    let desc = description.unwrap_or(slug);
    let branch_name = branch.unwrap_or(slug);

    // Resolve dependency slugs to plan IDs BEFORE creating the plan so we
    // fail fast if any are missing. We must look them up in the same
    // project.
    let mut resolved_deps: Vec<(String, String)> = Vec::with_capacity(depends_on.len());
    for dep_slug in depends_on {
        let dep = storage::get_plan_by_slug(conn, dep_slug, project)?
            .with_context(|| format!("Dependency plan not found: {dep_slug}"))?;
        resolved_deps.push((dep_slug.clone(), dep.id));
    }

    let plan = storage::create_plan(
        conn,
        slug,
        project,
        branch_name,
        desc,
        harness,
        agent,
        tests,
    )?;

    // Attach each resolved dependency. Self-references and cycles are
    // rejected by the storage layer (the new plan has no deps yet, so a
    // cycle is impossible, but self-reference is guarded anyway).
    for (dep_slug, dep_id) in &resolved_deps {
        storage::add_plan_dependency(conn, &plan.id, dep_id)
            .with_context(|| format!("Failed to add dependency on '{dep_slug}'"))?;
    }

    println!(
        "{} Created plan: {}",
        output::check_icon(out.color),
        output::bold(&plan.slug, out.color),
    );
    if !tests.is_empty() {
        println!("  Tests: {}", tests.join(", "));
    }
    if !resolved_deps.is_empty() {
        let slugs: Vec<&str> = resolved_deps.iter().map(|(s, _)| s.as_str()).collect();
        println!("  Depends on: {}", slugs.join(", "));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Plan dependency commands
// ---------------------------------------------------------------------------

/// Add one or more plan dependency edges to `slug`.
pub fn plan_dependency_add(
    conn: &Connection,
    slug: &str,
    project: &str,
    depends_on_slugs: &[String],
    out: &OutputContext,
) -> Result<()> {
    if depends_on_slugs.is_empty() {
        bail!("At least one --depends-on slug is required");
    }

    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    for dep_slug in depends_on_slugs {
        let dep = storage::get_plan_by_slug(conn, dep_slug, project)?
            .with_context(|| format!("Dependency plan not found: {dep_slug}"))?;
        storage::add_plan_dependency(conn, &plan.id, &dep.id)?;
        println!(
            "{} Added dependency: {} -> {}",
            output::check_icon(out.color),
            slug,
            dep_slug
        );
    }

    Ok(())
}

/// Remove one or more plan dependency edges from `slug`.
pub fn plan_dependency_remove(
    conn: &Connection,
    slug: &str,
    project: &str,
    depends_on_slugs: &[String],
    out: &OutputContext,
) -> Result<()> {
    if depends_on_slugs.is_empty() {
        bail!("At least one --depends-on slug is required");
    }

    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    for dep_slug in depends_on_slugs {
        let dep = storage::get_plan_by_slug(conn, dep_slug, project)?
            .with_context(|| format!("Dependency plan not found: {dep_slug}"))?;
        storage::remove_plan_dependency(conn, &plan.id, &dep.id)?;
        println!(
            "{} Removed dependency: {} -> {}",
            output::check_icon(out.color),
            slug,
            dep_slug
        );
    }

    Ok(())
}

/// Print the direct dependencies and dependents of `slug`.
pub fn plan_dependency_list(conn: &Connection, slug: &str, project: &str, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    let dep_ids = storage::list_plan_dependencies(conn, &plan.id)?;
    let dependent_ids = storage::list_dependent_plans(conn, &plan.id)?;

    let mut dep_slugs: Vec<String> = Vec::with_capacity(dep_ids.len());
    for id in &dep_ids {
        if let Some(s) = storage::get_plan_slug_by_id(conn, id)? {
            dep_slugs.push(s);
        }
    }
    dep_slugs.sort();

    let mut dependent_slugs: Vec<String> = Vec::with_capacity(dependent_ids.len());
    for id in &dependent_ids {
        if let Some(s) = storage::get_plan_slug_by_id(conn, id)? {
            dependent_slugs.push(s);
        }
    }
    dependent_slugs.sort();

    if out.format == OutputFormat::Json {
        let summary = output::DependencyListSummary {
            slug: slug.to_string(),
            depends_on: dep_slugs,
            depended_on_by: dependent_slugs,
        };
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    println!("{}", output::bold(slug, out.color));
    println!("  depends on:");
    if dep_slugs.is_empty() {
        println!("    (none)");
    } else {
        for s in &dep_slugs {
            println!("    - {s}");
        }
    }
    println!("  depended on by:");
    if dependent_slugs.is_empty() {
        println!("    (none)");
    } else {
        for s in &dependent_slugs {
            println!("    - {s}");
        }
    }

    Ok(())
}

pub fn plan_list(
    conn: &Connection,
    project: &str,
    all: bool,
    status: Option<PlanStatus>,
    show_archived: bool,
    out: &OutputContext,
) -> Result<()> {
    let plans = storage::list_plans(conn, project, all)?;

    // Filter by status if provided, otherwise hide archived unless --archived
    let plans: Vec<_> = if let Some(target) = status {
        plans.into_iter().filter(|p| p.status == target).collect()
    } else if !show_archived {
        plans
            .into_iter()
            .filter(|p| p.status != PlanStatus::Archived)
            .collect()
    } else {
        plans
    };

    if out.format == OutputFormat::Json {
        let summaries: Vec<output::PlanSummary> =
            plans.iter().map(output::PlanSummary::from).collect();
        println!("{}", serde_json::to_string(&summaries)?);
        return Ok(());
    }

    if plans.is_empty() {
        println!("No plans found.");
        return Ok(());
    }

    for plan in &plans {
        println!(
            "  {} {}  {}  [{}]",
            output::plan_status_icon(plan.status, out.color),
            output::bold(&plan.slug, out.color),
            plan.description,
            output::colored_plan_status(plan.status, out.color),
        );
        if all {
            println!("    project: {}", plan.project);
        }
    }

    Ok(())
}

pub fn plan_show(conn: &Connection, slug: &str, project: &str, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    if out.format == OutputFormat::Json {
        let summary = output::PlanShowSummary {
            plan: output::PlanSummary::from(&plan),
            steps: steps.iter().map(output::StepSummary::from).collect(),
        };
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    println!(
        "{}  {}",
        output::bold(&plan.slug, out.color),
        output::colored_plan_status(plan.status, out.color),
    );
    println!("  Description: {}", plan.description);
    println!("  Branch:      {}", plan.branch_name);
    println!("  Project:     {}", plan.project);
    if let Some(ref h) = plan.harness {
        println!("  Harness:     {h}");
    }
    if let Some(ref a) = plan.agent {
        println!("  Agent:       {a}");
    }
    if !plan.deterministic_tests.is_empty() {
        println!("  Tests:");
        for t in &plan.deterministic_tests {
            println!("    - {t}");
        }
    }
    println!(
        "  Created:     {}",
        plan.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );

    if !steps.is_empty() {
        println!();
        println!("  Steps:");
        for (i, step) in steps.iter().enumerate() {
            println!(
                "    {:>3}. {} {} [{}]",
                i + 1,
                output::status_icon(step.status, out.color),
                step.title,
                output::colored_status(step.status, out.color),
            );
        }
    }

    Ok(())
}

pub fn plan_approve(conn: &Connection, slug: &str, project: &str, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if plan.status != PlanStatus::Planning {
        bail!(
            "Plan '{}' is in status '{}', can only approve plans in 'planning' status",
            slug,
            plan.status
        );
    }

    storage::update_plan_status(conn, &plan.id, PlanStatus::Ready)?;
    println!(
        "{} Plan '{}' approved and ready for execution",
        output::check_icon(out.color),
        slug
    );
    Ok(())
}

pub fn plan_archive(conn: &Connection, slug: &str, project: &str, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    match plan.status {
        PlanStatus::Complete | PlanStatus::Failed | PlanStatus::Aborted => {}
        _ => bail!(
            "Plan '{}' is in status '{}'; only complete, failed, or aborted plans can be archived",
            slug,
            plan.status
        ),
    }

    storage::update_plan_status(conn, &plan.id, PlanStatus::Archived)?;
    println!(
        "{} Archived plan '{}'",
        output::plan_status_icon(PlanStatus::Archived, out.color),
        slug
    );
    Ok(())
}

pub fn plan_unarchive(conn: &Connection, slug: &str, project: &str, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if plan.status != PlanStatus::Archived {
        bail!(
            "Plan '{}' is not archived (status: '{}')",
            slug,
            plan.status
        );
    }

    // Restore to complete — the most neutral terminal state.
    storage::update_plan_status(conn, &plan.id, PlanStatus::Complete)?;
    println!(
        "{} Unarchived plan '{}' (status: complete)",
        output::check_icon(out.color),
        slug
    );
    Ok(())
}

pub fn plan_delete(conn: &Connection, slug: &str, project: &str, force: bool, out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if !force {
        print!("Delete plan '{}' and all its steps/logs? [y/N] ", slug);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_plan(conn, &plan.id)?;
    println!(
        "{} Deleted plan '{}'",
        output::check_icon(out.color),
        slug
    );
    Ok(())
}

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

    let step = if let Some(after_pos) = after {
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

    // Determine the position
    let steps = storage::list_steps(conn, &plan.id)?;
    let pos = steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|i| i + 1)
        .unwrap_or(0);

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
// Status command
// ---------------------------------------------------------------------------

pub fn cmd_status(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    verbose: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        // Find the most recent active plan, including completed plans so that
        // running `status` right after a plan finishes still shows it.
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format == OutputFormat::Json {
                    println!("null");
                } else {
                    eprintln!("No active plan found. Specify a plan slug as a positional argument.");
                }
                return Ok(());
            }
        }
    };

    let steps = storage::list_steps(conn, &plan.id)?;

    let total = steps.len();
    let complete = steps.iter().filter(|s| s.status == StepStatus::Complete).count();
    let failed = steps.iter().filter(|s| s.status == StepStatus::Failed).count();
    let skipped = steps.iter().filter(|s| s.status == StepStatus::Skipped).count();
    let pending = steps.iter().filter(|s| s.status == StepStatus::Pending).count();
    let in_progress = steps.iter().filter(|s| s.status == StepStatus::InProgress).count();

    if out.format == OutputFormat::Json {
        let summary = output::StatusSummary {
            slug: plan.slug.clone(),
            status: plan.status,
            branch_name: plan.branch_name.clone(),
            steps: output::StepCounts {
                total,
                complete,
                failed,
                skipped,
                pending,
                in_progress,
            },
        };
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    println!(
        "{}  {}",
        output::bold(&plan.slug, out.color),
        output::colored_plan_status(plan.status, out.color),
    );
    println!("  Branch: {}", plan.branch_name);

    if steps.is_empty() {
        println!("  No steps.");
        return Ok(());
    }

    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        complete, total, failed, skipped, pending, in_progress
    );

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            println!(
                "  {:>3}. {} {} [{}] (attempts: {})",
                i + 1,
                output::status_icon(step.status, out.color),
                step.title,
                output::colored_status(step.status, out.color),
                step.attempts,
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Log command
// ---------------------------------------------------------------------------

/// Controls how harness stdout/stderr is displayed in log output.
///
/// - `Hidden` — don't show output (default when no flags given).
/// - `Truncated(n)` — show up to `n` lines per stream.
/// - `Full` — show everything, no truncation.
pub enum LogOutputMode {
    Hidden,
    Truncated(usize),
    Full,
}

pub fn cmd_log(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    step_num: Option<usize>,
    limit: Option<usize>,
    output_mode: &LogOutputMode,
    out: &OutputContext,
) -> Result<()> {
    // Resolve plan
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format != OutputFormat::Json {
                    eprintln!("No plan found. Specify a plan slug as a positional argument.");
                }
                return Ok(());
            }
        }
    };

    if let Some(step_idx) = step_num {
        // Show logs for a specific step
        let steps = storage::list_steps(conn, &plan.id)?;
        if step_idx == 0 || step_idx > steps.len() {
            anyhow::bail!(
                "Step {} is out of range (plan has {} steps)",
                step_idx,
                steps.len()
            );
        }
        let step = &steps[step_idx - 1];
        let logs = storage::list_execution_logs_for_step(conn, &step.id)?;

        if out.format == OutputFormat::Json {
            for log in &logs {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode));
            }
            return Ok(());
        }

        eprintln!(
            "Logs for step #{} '{}' ({} attempts):",
            step_idx,
            step.title,
            logs.len()
        );
        eprintln!();

        for log in &logs {
            print_log_entry(&step.title, log, output_mode, out.color);
        }
    } else {
        // Show all logs for the plan
        let entries = storage::list_execution_logs_for_plan(conn, &plan.id, limit)?;

        if out.format == OutputFormat::Json {
            for (_, log) in &entries {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode));
            }
            return Ok(());
        }

        if entries.is_empty() {
            eprintln!("No execution logs for plan '{}'.", plan.slug);
            return Ok(());
        }

        eprintln!(
            "Execution logs for plan '{}' ({} entries):",
            plan.slug,
            entries.len()
        );
        eprintln!();

        for (step_title, log) in &entries {
            print_log_entry(step_title, log, output_mode, out.color);
        }
    }

    Ok(())
}

fn print_log_entry(
    step_title: &str,
    log: &ExecutionLog,
    output_mode: &LogOutputMode,
    color: bool,
) {
    let icon = output::log_status_icon(log.committed, log.rolled_back, color);

    let duration_str = log
        .duration_secs
        .map(|d| format!("{:.1}s", d))
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {} [attempt {}] {} ({}) {}",
        icon,
        log.attempt,
        step_title,
        duration_str,
        log.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
    );

    if let Some(ref hash) = log.commit_hash {
        println!("    commit: {}", &hash[..hash.len().min(8)]);
    }

    if !log.test_results.is_empty() {
        println!("    tests: {}", log.test_results.join(", "));
    }

    if let Some(cost) = log.cost_usd {
        let tokens = match (log.input_tokens, log.output_tokens) {
            (Some(i), Some(o)) => format!(" ({i} in / {o} out tokens)"),
            _ => String::new(),
        };
        println!("    cost: ${:.4}{}", cost, tokens);
    }

    if !matches!(output_mode, LogOutputMode::Hidden) {
        let take_n = match output_mode {
            LogOutputMode::Truncated(n) => Some(*n),
            _ => None,
        };
        if let Some(ref stdout) = log.harness_stdout
            && !stdout.is_empty()
        {
            println!("    --- stdout ---");
            let lines_iter = stdout.lines();
            let lines: Box<dyn Iterator<Item = &str>> = match take_n {
                Some(n) => Box::new(lines_iter.take(n)),
                None => Box::new(lines_iter),
            };
            for line in lines {
                println!("    {line}");
            }
        }
        if let Some(ref stderr) = log.harness_stderr
            && !stderr.is_empty()
        {
            println!("    --- stderr ---");
            let lines_iter = stderr.lines();
            let lines: Box<dyn Iterator<Item = &str>> = match take_n {
                Some(n) => Box::new(lines_iter.take(n)),
                None => Box::new(lines_iter),
            };
            for line in lines {
                println!("    {line}");
            }
        }
    }

    println!();
}

// ---------------------------------------------------------------------------
// Agents commands
// ---------------------------------------------------------------------------

pub fn cmd_agents_list(out: &OutputContext) -> Result<()> {
    let agents_dir = config::agents_dir()?;

    if !agents_dir.exists() {
        if out.format == OutputFormat::Json {
            println!("[]");
        } else {
            println!("Agents directory not found: {}", agents_dir.display());
            println!("Run `ralph init` to create it.");
        }
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(&agents_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "md"))
        .collect();

    entries.sort_by_key(|e| e.file_name());

    if out.format == OutputFormat::Json {
        let infos: Vec<output::AgentInfo> = entries
            .iter()
            .map(|entry| {
                let name = entry
                    .file_name()
                    .to_string_lossy()
                    .trim_end_matches(".md")
                    .to_string();
                let size = entry.metadata().ok().map(|m| m.len()).unwrap_or(0);
                output::AgentInfo {
                    name,
                    size_bytes: size,
                }
            })
            .collect();
        println!("{}", serde_json::to_string(&infos)?);
        return Ok(());
    }

    let mut found = false;
    for entry in &entries {
        let name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".md")
            .to_string();
        let metadata = entry.metadata().ok();
        let size = metadata.map(|m| m.len()).unwrap_or(0);
        println!("  {} ({} bytes)", name, size);
        found = true;
    }

    if !found {
        println!("No agent files found in {}", agents_dir.display());
    }

    Ok(())
}

pub fn cmd_agents_show(name: &str, _out: &OutputContext) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

pub fn cmd_agents_create(name: &str, file: Option<&std::path::Path>, _out: &OutputContext) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    std::fs::create_dir_all(&agents_dir)?;
    let path = agents_dir.join(format!("{name}.md"));

    if path.exists() {
        anyhow::bail!("Agent file already exists: {}", path.display());
    }

    let contents = if let Some(src) = file {
        std::fs::read_to_string(src).with_context(|| format!("Failed to read {}", src.display()))?
    } else {
        format!("# {name}\n\nAgent instructions go here.\n")
    };

    std::fs::write(&path, &contents)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    println!("Created agent file: {}", path.display());
    Ok(())
}

pub fn cmd_agents_delete(name: &str, _out: &OutputContext) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    std::fs::remove_file(&path).with_context(|| format!("Failed to delete {}", path.display()))?;
    println!("Deleted agent file: {name}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Hooks commands
// ---------------------------------------------------------------------------

pub fn cmd_hooks_list(project: &str, all: bool, out: &OutputContext) -> Result<()> {
    let hooks = hook_library::load_all()?;

    let filtered: Vec<Hook> = if all {
        hooks
    } else {
        hook_library::filter_by_project(hooks, Path::new(project))
    };

    if out.format == OutputFormat::Json {
        let infos: Vec<output::HookInfo> = filtered
            .iter()
            .map(|h| {
                let scope_str = match &h.scope {
                    Scope::Global => "global".to_string(),
                    Scope::Paths { paths } => {
                        let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                        format!("paths: {}", list.join(", "))
                    }
                };
                output::HookInfo {
                    name: h.name.clone(),
                    lifecycle: h.lifecycle.to_string(),
                    scope: scope_str,
                    description: h.description.clone(),
                }
            })
            .collect();
        println!("{}", serde_json::to_string(&infos)?);
        return Ok(());
    }

    if filtered.is_empty() {
        if all {
            println!(
                "No hooks found in {}",
                hook_library::hooks_dir()?.display()
            );
        } else {
            println!(
                "No hooks applicable to {project}. Use `ralph hooks list --all` to see all hooks."
            );
        }
        return Ok(());
    }

    for hook in &filtered {
        let scope_str = match &hook.scope {
            Scope::Global => "global".to_string(),
            Scope::Paths { paths } => {
                let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!("paths: {}", list.join(", "))
            }
        };
        let desc = if hook.description.is_empty() {
            String::new()
        } else {
            format!(" — {}", hook.description)
        };
        println!(
            "  {name:<24} [{lifecycle}] ({scope}){desc}",
            name = hook.name,
            lifecycle = hook.lifecycle,
            scope = scope_str,
        );
    }

    Ok(())
}

pub fn cmd_hooks_show(name: &str, _out: &OutputContext) -> Result<()> {
    let path = hook_library::hooks_dir()?.join(format!("{name}.md"));
    if !path.exists() {
        bail!("Hook not found: {name}");
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

pub fn cmd_hooks_add(
    name: &str,
    lifecycle: Lifecycle,
    command: &str,
    description: Option<&str>,
    scope_paths: &[std::path::PathBuf],
    force: bool,
    _out: &OutputContext,
) -> Result<()> {
    let scope = if scope_paths.is_empty() {
        Scope::Global
    } else {
        for p in scope_paths {
            if !p.is_absolute() {
                bail!(
                    "Scope path '{}' must be absolute (no '~' expansion)",
                    p.display()
                );
            }
        }
        Scope::Paths {
            paths: scope_paths.to_vec(),
        }
    };

    let hook = Hook {
        name: name.to_string(),
        description: description.unwrap_or("").to_string(),
        lifecycle,
        scope,
        command: command.to_string(),
    };

    let path = hook_library::save(&hook, force)?;
    println!("Created hook '{name}' at {}", path.display());
    Ok(())
}

pub fn cmd_hooks_remove(name: &str, _out: &OutputContext) -> Result<()> {
    hook_library::delete(name)?;
    println!("Deleted hook '{name}'");
    Ok(())
}

pub fn cmd_hooks_export(
    project: &str,
    output: Option<&Path>,
    all: bool,
    path: Option<&Path>,
    _out: &OutputContext,
) -> Result<()> {
    let hooks = hook_library::load_all()?;

    let filtered: Vec<Hook> = if all {
        hooks
    } else {
        let scope_path = path.map(|p| p.to_path_buf()).unwrap_or_else(|| {
            std::path::PathBuf::from(project)
        });
        hook_library::filter_by_project(hooks, &scope_path)
    };

    let bundle = HookBundle::new(filtered);
    let json = serde_json::to_string_pretty(&bundle)?;

    match output {
        Some(p) => {
            std::fs::write(p, format!("{json}\n"))
                .with_context(|| format!("Failed to write {}", p.display()))?;
            eprintln!(
                "Exported {} hook(s) to {}",
                bundle.hooks.len(),
                p.display()
            );
        }
        None => println!("{json}"),
    }
    Ok(())
}

pub fn cmd_hooks_import(file: &Path, force: bool, _out: &OutputContext) -> Result<()> {
    let contents = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read bundle {}", file.display()))?;
    let bundle: HookBundle = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse bundle {}", file.display()))?;

    if bundle.hooks.is_empty() {
        println!("Bundle contains no hooks.");
        return Ok(());
    }

    let mut imported = 0usize;
    let mut skipped = 0usize;

    for hook in &bundle.hooks {
        // Check for collisions first (default: error).
        let existed = hook_library::try_load(&hook.name)?.is_some();
        if existed && !force {
            eprintln!(
                "Error: hook '{}' already exists. Re-run with --force to overwrite.",
                hook.name
            );
            skipped += 1;
            continue;
        }
        hook_library::save(hook, true)?;
        imported += 1;
    }

    println!(
        "Imported {imported} hook(s), skipped {skipped}.{}",
        if skipped > 0 && !force {
            " Use --force to overwrite existing hooks."
        } else {
            ""
        }
    );
    if skipped > 0 && !force {
        bail!("{skipped} hook(s) skipped due to collisions");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Step/plan hook attachment commands
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

pub fn cmd_plan_set_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    if hook_library::try_load(hook_name)?.is_none() {
        eprintln!(
            "Warning: hook '{hook_name}' is not in the local library. It will be skipped at run time until imported."
        );
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    storage::attach_hook_to_plan(conn, &plan.id, lifecycle.as_str(), hook_name)?;
    println!("Attached plan-wide hook '{hook_name}' to '{plan_slug}' at {lifecycle}");
    Ok(())
}

pub fn cmd_plan_unset_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    let removed = storage::detach_hook(conn, &plan.id, None, lifecycle.as_str(), hook_name)?;
    if removed == 0 {
        bail!("No plan-wide hook '{hook_name}' attached to '{plan_slug}' at {lifecycle}");
    }
    println!("Detached plan-wide hook '{hook_name}' from '{plan_slug}'");
    Ok(())
}

pub fn cmd_plan_hooks(conn: &Connection, plan_slug: &str, project: &str, _out: &OutputContext) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    let rows = storage::list_all_hooks_for_plan(conn, &plan.id)?;

    if rows.is_empty() {
        println!("No hooks attached to plan '{plan_slug}'.");
        return Ok(());
    }

    let steps = storage::list_steps(conn, &plan.id)?;
    let step_num = |sid: &str| -> Option<usize> {
        steps.iter().position(|s| s.id == sid).map(|i| i + 1)
    };

    println!("Hooks attached to plan '{plan_slug}':");
    for row in &rows {
        let target = match &row.step_id {
            None => "plan-wide".to_string(),
            Some(sid) => match step_num(sid) {
                Some(n) => format!("step {n}"),
                None => format!("step <unknown id {sid}>"),
            },
        };
        println!(
            "  {target:<12} [{lifecycle:<9}] {hook}",
            target = target,
            lifecycle = row.lifecycle,
            hook = row.hook_name,
        );
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
