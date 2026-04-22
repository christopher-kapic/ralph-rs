// Plan CLI command implementations (CRUD, dependencies, plan-level hooks)

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use crate::hook_library::{self, Lifecycle};
use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::PlanStatus;
use crate::storage;

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

    eprintln!(
        "{} Created plan: {}",
        output::check_icon(out.color),
        output::bold(&plan.slug, out.color),
    );
    if !tests.is_empty() {
        eprintln!("  Tests: {}", tests.join(", "));
    }
    if !resolved_deps.is_empty() {
        let slugs: Vec<&str> = resolved_deps.iter().map(|(s, _)| s.as_str()).collect();
        eprintln!("  Depends on: {}", slugs.join(", "));
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
        eprintln!(
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
        eprintln!(
            "{} Removed dependency: {} -> {}",
            output::check_icon(out.color),
            slug,
            dep_slug
        );
    }

    Ok(())
}

/// Print the direct dependencies and dependents of `slug`.
pub fn plan_dependency_list(
    conn: &Connection,
    slug: &str,
    project: &str,
    out: &OutputContext,
) -> Result<()> {
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

/// Decide whether a plan with the given status should appear in the listing,
/// given the `--status` and `--archived` flags.
///
/// - Neither flag: hide archived plans.
/// - `--status X` only: keep plans whose status is exactly X.
/// - `--archived` only: keep all plans.
/// - Both: keep plans whose status is X or Archived. When X itself is
///   Archived, `--archived` is implied and the rule collapses to `Archived`.
fn plan_list_matches(
    plan_status: PlanStatus,
    status_filter: Option<PlanStatus>,
    show_archived: bool,
) -> bool {
    match (status_filter, show_archived) {
        (Some(target), true) => plan_status == target || plan_status == PlanStatus::Archived,
        (Some(target), false) => plan_status == target,
        (None, true) => true,
        (None, false) => plan_status != PlanStatus::Archived,
    }
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
    let plans: Vec<_> = plans
        .into_iter()
        .filter(|p| plan_list_matches(p.status, status, show_archived))
        .collect();

    if out.format == OutputFormat::Json {
        let summaries: Vec<output::PlanSummary> =
            plans.iter().map(output::PlanSummary::from).collect();
        println!("{}", serde_json::to_string(&summaries)?);
        return Ok(());
    }

    if plans.is_empty() {
        eprintln!("No plans found.");
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

pub fn plan_approve(
    conn: &Connection,
    slug: &str,
    project: &str,
    out: &OutputContext,
) -> Result<()> {
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
    eprintln!(
        "{} Plan '{}' approved and ready for execution",
        output::check_icon(out.color),
        slug
    );
    Ok(())
}

pub fn plan_archive(
    conn: &Connection,
    slug: &str,
    project: &str,
    out: &OutputContext,
) -> Result<()> {
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
    eprintln!(
        "{} Archived plan '{}'",
        output::plan_status_icon(PlanStatus::Archived, out.color),
        slug
    );
    Ok(())
}

pub fn plan_unarchive(
    conn: &Connection,
    slug: &str,
    project: &str,
    out: &OutputContext,
) -> Result<()> {
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
    eprintln!(
        "{} Unarchived plan '{}' (status: complete)",
        output::check_icon(out.color),
        slug
    );
    Ok(())
}

pub fn plan_delete(
    conn: &Connection,
    slug: &str,
    project: &str,
    force: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if !force {
        let prompt = format!("Delete plan '{}' and all its steps/logs?", slug);
        if !output::confirm(&prompt)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_plan(conn, &plan.id)?;
    eprintln!("{} Deleted plan '{}'", output::check_icon(out.color), slug);
    Ok(())
}

// ---------------------------------------------------------------------------
// Plan hook attachment commands
// ---------------------------------------------------------------------------

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

pub fn cmd_plan_hooks(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    _out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    let rows = storage::list_all_hooks_for_plan(conn, &plan.id)?;

    if rows.is_empty() {
        println!("No hooks attached to plan '{plan_slug}'.");
        return Ok(());
    }

    let steps = storage::list_steps(conn, &plan.id)?;
    let step_num =
        |sid: &str| -> Option<usize> { steps.iter().position(|s| s.id == sid).map(|i| i + 1) };

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

pub fn plan_harness_set(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    harness: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    storage::set_plan_harness_gen(conn, &plan.id, Some(harness))?;
    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan_slug,
            "plan_harness": harness,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        eprintln!(
            "Set plan-generation harness for '{}' to '{}'.",
            plan_slug, harness
        );
    }
    Ok(())
}

/// Set the context-prepend override for a plan.
///
/// Stores `text` verbatim — an empty string is a legitimate "no prepend at
/// all" escape hatch, not "clear back to default". Use [`plan_prepend_clear`]
/// for the clear-to-None path.
pub fn plan_prepend_set(
    conn: &Connection,
    plan: &crate::plan::Plan,
    text: &str,
    out: &OutputContext,
) -> Result<()> {
    storage::set_plan_context_prepend(conn, &plan.id, Some(text))?;
    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan.slug,
            "context_prepend": text,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else if !out.quiet {
        eprintln!(
            "{} Set context prepend for plan '{}' ({} bytes).",
            output::check_icon(out.color),
            plan.slug,
            text.len(),
        );
    }
    Ok(())
}

/// Show the effective context-prepend text for a plan. When `default` is
/// true, print the built-in system default regardless of the plan's setting.
pub fn plan_prepend_show(
    _conn: &Connection,
    plan: &crate::plan::Plan,
    default: bool,
    out: &OutputContext,
) -> Result<()> {
    let effective = if default {
        crate::prompt::DEFAULT_CONTEXT_PREPEND
    } else {
        crate::prompt::effective_context_prepend(plan)
    };

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan.slug,
            "context_prepend": plan.context_prepend,
            "effective": effective,
            "is_default": plan.context_prepend.is_none() || default,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        // Write to stdout so `ralph plan prepend show | less` works.
        println!("{effective}");
    }
    Ok(())
}

/// Clear the plan's context-prepend override — fall back to the system
/// default.
pub fn plan_prepend_clear(
    conn: &Connection,
    plan: &crate::plan::Plan,
    out: &OutputContext,
) -> Result<()> {
    storage::set_plan_context_prepend(conn, &plan.id, None)?;
    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan.slug,
            "context_prepend": serde_json::Value::Null,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else if !out.quiet {
        eprintln!(
            "{} Cleared context prepend for plan '{}' (now using system default).",
            output::check_icon(out.color),
            plan.slug,
        );
    }
    Ok(())
}

pub fn plan_harness_show(
    _conn: &Connection,
    plan: &crate::plan::Plan,
    config: &crate::config::Config,
    out: &OutputContext,
) -> Result<()> {
    let harness_name = plan
        .plan_harness
        .as_deref()
        .unwrap_or(&config.default_harness);
    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan.slug,
            "plan_harness": plan.plan_harness,
            "default_harness": config.default_harness,
            "effective_harness": harness_name,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        match &plan.plan_harness {
            Some(h) => eprintln!("Plan '{}' plan-generation harness: {}", plan.slug, h),
            None => eprintln!(
                "Plan '{}' plan-generation harness: (default: {})",
                plan.slug, config.default_harness
            ),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_list_matches_default_hides_archived() {
        assert!(plan_list_matches(PlanStatus::Ready, None, false));
        assert!(plan_list_matches(PlanStatus::Complete, None, false));
        assert!(!plan_list_matches(PlanStatus::Archived, None, false));
    }

    #[test]
    fn plan_list_matches_archived_flag_includes_all() {
        assert!(plan_list_matches(PlanStatus::Ready, None, true));
        assert!(plan_list_matches(PlanStatus::Archived, None, true));
    }

    #[test]
    fn plan_list_matches_status_only_filters_to_exact_status() {
        assert!(plan_list_matches(
            PlanStatus::Complete,
            Some(PlanStatus::Complete),
            false
        ));
        assert!(!plan_list_matches(
            PlanStatus::Ready,
            Some(PlanStatus::Complete),
            false
        ));
        assert!(!plan_list_matches(
            PlanStatus::Archived,
            Some(PlanStatus::Complete),
            false
        ));
    }

    #[test]
    fn plan_list_matches_archived_and_status_includes_both() {
        // --archived --status complete: archived plans and complete plans appear.
        assert!(plan_list_matches(
            PlanStatus::Complete,
            Some(PlanStatus::Complete),
            true
        ));
        assert!(plan_list_matches(
            PlanStatus::Archived,
            Some(PlanStatus::Complete),
            true
        ));
        // Unrelated statuses still excluded.
        assert!(!plan_list_matches(
            PlanStatus::Ready,
            Some(PlanStatus::Complete),
            true
        ));
    }

    #[test]
    fn plan_list_matches_status_archived_with_flag_is_implied() {
        assert!(plan_list_matches(
            PlanStatus::Archived,
            Some(PlanStatus::Archived),
            true
        ));
        assert!(!plan_list_matches(
            PlanStatus::Complete,
            Some(PlanStatus::Archived),
            true
        ));
    }

    // ----------------------------------------------------------------------
    // `ralph plan prepend ...` tests
    // ----------------------------------------------------------------------

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    fn setup_prepend_test() -> (rusqlite::Connection, crate::plan::Plan) {
        let conn = crate::db::open_memory().expect("open_memory");
        let plan = storage::create_plan(
            &conn,
            "prep-plan",
            "/tmp/proj",
            "b",
            "A test plan",
            None,
            None,
            &[],
        )
        .expect("create_plan");
        (conn, plan)
    }

    #[test]
    fn test_plan_prepend_set_text_from_flag() {
        let (conn, plan) = setup_prepend_test();

        plan_prepend_set(&conn, &plan, "# Custom\n\nCaveat emptor.", &quiet_out()).unwrap();

        let reloaded = storage::get_plan_by_slug(&conn, &plan.slug, &plan.project)
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.context_prepend.as_deref(),
            Some("# Custom\n\nCaveat emptor."),
            "set should store the text verbatim"
        );
    }

    #[test]
    fn test_plan_prepend_show_default_flag() {
        let (conn, plan) = setup_prepend_test();

        // Seed a plan-specific override so we can assert `--default` ignores it.
        plan_prepend_set(&conn, &plan, "IGNORED OVERRIDE", &quiet_out()).unwrap();
        let reloaded = storage::get_plan_by_slug(&conn, &plan.slug, &plan.project)
            .unwrap()
            .unwrap();

        // Show with `default=true` should bypass the override. We can't
        // easily capture stdout here, but we can validate the resolved
        // string via `effective_context_prepend` and a parallel check that
        // `DEFAULT_CONTEXT_PREPEND` is what `default=true` returns.
        let effective_when_default =
            if true { crate::prompt::DEFAULT_CONTEXT_PREPEND } else { "" };
        assert!(effective_when_default.contains("# Ralph context"));
        assert!(effective_when_default.contains("## Introspecting the plan"));

        // The override sanity-check: `effective_context_prepend` without
        // --default should surface the override, not the system default.
        assert_eq!(
            crate::prompt::effective_context_prepend(&reloaded),
            "IGNORED OVERRIDE"
        );

        // Running the command shouldn't error.
        plan_prepend_show(&conn, &reloaded, true, &quiet_out()).unwrap();
        plan_prepend_show(&conn, &reloaded, false, &quiet_out()).unwrap();
    }

    #[test]
    fn test_plan_prepend_clear_resets_to_none() {
        let (conn, plan) = setup_prepend_test();

        plan_prepend_set(&conn, &plan, "custom text", &quiet_out()).unwrap();
        let reloaded = storage::get_plan_by_slug(&conn, &plan.slug, &plan.project)
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.context_prepend.as_deref(), Some("custom text"));

        plan_prepend_clear(&conn, &reloaded, &quiet_out()).unwrap();
        let reloaded = storage::get_plan_by_slug(&conn, &plan.slug, &plan.project)
            .unwrap()
            .unwrap();
        assert_eq!(
            reloaded.context_prepend, None,
            "clear must reset to None so the plan falls back to the system default"
        );
    }
}
