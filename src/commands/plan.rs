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

/// The user-supplied inputs to [`plan_create`]: the new plan's `slug` /
/// `project`, optional `description` / `branch` / default `harness` / `agent`,
/// the optional review recursion cap, the deterministic `tests`, and the
/// `depends_on` plan slugs. `conn` and the `out` sink stay separate.
pub struct PlanCreateArgs<'a> {
    pub slug: &'a str,
    pub project: &'a str,
    pub description: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub harness: Option<&'a str>,
    pub agent: Option<&'a str>,
    pub max_review_corrections: Option<i32>,
    pub tests: &'a [String],
    pub depends_on: &'a [String],
}

pub fn plan_create(conn: &Connection, args: PlanCreateArgs<'_>, out: &OutputContext) -> Result<()> {
    let PlanCreateArgs {
        slug,
        project,
        description,
        branch,
        harness,
        agent,
        max_review_corrections,
        tests,
        depends_on,
    } = args;
    let desc = description.unwrap_or(slug);
    let branch_name = branch.unwrap_or(slug);

    // Validate inputs BEFORE any DB write so an invalid slug/branch fails
    // fast with nothing persisted (previously the bad branch was only
    // discovered later, at `runner::setup_branch`, after a junk plan row
    // already existed). Slug rules stay deliberately simple — reject only
    // empty/blank; we do NOT impose git-ref syntax on slugs, only on the
    // resolved branch name.
    if slug.trim().is_empty() {
        bail!("invalid plan slug: slug is empty or whitespace-only");
    }
    crate::git::check_ref_format(branch_name)?;
    if let Some(cap) = max_review_corrections
        && cap < 0
    {
        bail!("--max-review-corrections must be non-negative, got {cap}");
    }

    // Resolve dependency slugs to plan IDs BEFORE creating the plan so we
    // fail fast if any are missing. We must look them up in the same
    // project.
    let mut resolved_deps: Vec<(String, String)> = Vec::with_capacity(depends_on.len());
    for dep_slug in depends_on {
        let dep = storage::get_plan_by_slug(conn, dep_slug, project)?
            .with_context(|| format!("Dependency plan not found: {dep_slug}"))?;
        resolved_deps.push((dep_slug.clone(), dep.id));
    }

    // Create the plan and apply every config setter + dependency wiring
    // atomically (mirrors `step_add` / `step_add_bulk`). If any setter or a
    // late dependency error fails, the whole transaction rolls back so no
    // partially-configured plan row is left persisted. All storage functions
    // below take the tx connection directly and open no nested transaction of
    // their own, so this is safe.
    let plan = crate::db::with_tx(conn, |conn| {
        let plan = storage::create_plan(
            conn,
            storage::NewPlan {
                slug,
                project,
                branch_name,
                description: desc,
                harness,
                agent,
                deterministic_tests: tests,
            },
        )?;

        // Persist the per-plan review recursion cap only when explicitly given.
        // `None` is the column default (NULL → built-in
        // `review::DEFAULT_MAX_REVIEW_CORRECTIONS`), so skipping the write keeps
        // the common case identical (mirrors how `plan_harness` is only set
        // when present).
        if let Some(cap) = max_review_corrections {
            storage::set_plan_max_review_corrections(conn, &plan.id, Some(cap))?;
        }

        // Attach each resolved dependency. Self-references and cycles are
        // rejected by the storage layer (the new plan has no deps yet, so a
        // cycle is impossible, but self-reference is guarded anyway).
        for (dep_slug, dep_id) in &resolved_deps {
            storage::add_plan_dependency(conn, &plan.id, dep_id)
                .with_context(|| format!("Failed to add dependency on '{dep_slug}'"))?;
        }

        Ok(plan)
    })?;

    out.status(format!(
        "{} Created plan: {}",
        output::check_icon(out.color),
        output::bold(&plan.slug, out.color),
    ));
    if !tests.is_empty() {
        out.status(format!("  Tests: {}", tests.join(", ")));
    }
    if !resolved_deps.is_empty() {
        let slugs: Vec<&str> = resolved_deps.iter().map(|(s, _)| s.as_str()).collect();
        out.status(format!("  Depends on: {}", slugs.join(", ")));
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
        out.status(format!(
            "{} Added dependency: {} -> {}",
            output::check_icon(out.color),
            slug,
            dep_slug
        ));
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
        out.status(format!(
            "{} Removed dependency: {} -> {}",
            output::check_icon(out.color),
            slug,
            dep_slug
        ));
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
        out.status("No plans found.");
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
    out.status(format!(
        "{} Plan '{}' approved and ready for execution",
        output::check_icon(out.color),
        slug
    ));
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
    out.status(format!(
        "{} Archived plan '{}'",
        output::plan_status_icon(PlanStatus::Archived, out.color),
        slug
    ));
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
    out.status(format!(
        "{} Unarchived plan '{}' (status: complete)",
        output::check_icon(out.color),
        slug
    ));
    Ok(())
}

pub fn plan_delete(
    conn: &Connection,
    slug: &str,
    project: &str,
    force: bool,
    non_interactive: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if !force {
        if non_interactive {
            bail!("confirmation required to delete plan '{slug}'; pass --force to confirm");
        }
        let prompt = format!("Delete plan '{}' and all its steps/logs?", slug);
        if !output::confirm(&prompt)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_plan(conn, &plan.id)?;
    out.status(format!(
        "{} Deleted plan '{}'",
        output::check_icon(out.color),
        slug
    ));
    Ok(())
}

// ---------------------------------------------------------------------------
// Plan hook attachment commands
// ---------------------------------------------------------------------------

/// Set the plan-level `review_enabled` override (docs/dag-redesign.md
/// §6/§7). `enabled` writes `Some(true)`/`Some(false)` to the nullable
/// `plans.review_enabled` column — an explicit per-plan on/off that wins
/// over the global `config.review.enabled` and is itself overridden by a
/// per-step `--review` (precedence step > plan > config > false, resolved
/// by [`crate::config::effective_review_enabled`]).
///
/// Intentional asymmetry with `ralph step edit --review on|off|inherit`:
/// the plan-level command only takes `on`/`off` (`OnOffState`), so there is
/// no value that clears the override back to NULL (inherit-from-global). This
/// is deliberate — the documented surface is `ralph plan review <on|off>`,
/// and adding an `inherit` arm would mean a new tri-state value enum + a
/// `bool` → `Option<bool>` signature change here, expanding the CLI surface.
/// The underlying `set_plan_review_enabled` already accepts `None`, so a
/// future `inherit` is a small follow-up if the asymmetry proves annoying.
pub fn cmd_plan_review(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    enabled: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    storage::set_plan_review_enabled(conn, &plan.id, Some(enabled))?;

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "plan": plan_slug,
            "review_enabled": enabled,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        let verb = if enabled { "enabled" } else { "disabled" };
        out.status(format!("Review {verb} for plan '{plan_slug}'."));
    }
    Ok(())
}

pub fn cmd_plan_set_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    lifecycle: Lifecycle,
    hook_name: &str,
    out: &OutputContext,
) -> Result<()> {
    if hook_library::try_load(hook_name)?.is_none() {
        eprintln!(
            "Warning: hook '{hook_name}' is not in the local library. It will be skipped at run time until imported."
        );
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    storage::attach_hook_to_plan(conn, &plan.id, lifecycle.as_str(), hook_name)?;
    out.status(format!(
        "Attached plan-wide hook '{hook_name}' to '{plan_slug}' at {lifecycle}"
    ));
    Ok(())
}

pub fn cmd_plan_unset_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    lifecycle: Lifecycle,
    hook_name: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;
    let removed = storage::detach_hook(conn, &plan.id, None, lifecycle.as_str(), hook_name)?;
    if removed == 0 {
        bail!("No plan-wide hook '{hook_name}' attached to '{plan_slug}' at {lifecycle}");
    }
    out.status(format!(
        "Detached plan-wide hook '{hook_name}' from '{plan_slug}'"
    ));
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
        out.status(format!(
            "Set plan-generation harness for '{}' to '{}'.",
            plan_slug, harness
        ));
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
            Some(h) => out.status(format!(
                "Plan '{}' plan-generation harness: {}",
                plan.slug, h
            )),
            None => out.status(format!(
                "Plan '{}' plan-generation harness: (default: {})",
                plan.slug, config.default_harness
            )),
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

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    // ----------------------------------------------------------------------
    // `ralph plan review on|off` tests (STEP 42 / docs/dag-redesign.md §7)
    // ----------------------------------------------------------------------

    #[test]
    fn test_cmd_plan_review_toggles_on_then_off_persists() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/r-toggle";
        let plan = storage::create_plan(
            &conn,
            storage::NewPlan {
                slug: "rp",
                project,
                branch_name: "br",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        // New plans default to review_enabled = NULL (inherit global).
        assert_eq!(plan.review_enabled, None, "default is inherit (NULL)");

        cmd_plan_review(&conn, "rp", project, true, &quiet_out()).unwrap();
        let on = storage::get_plan_by_slug(&conn, "rp", project)
            .unwrap()
            .unwrap();
        assert_eq!(
            on.review_enabled,
            Some(true),
            "after `on`, the plan-scope override is Some(true)"
        );

        cmd_plan_review(&conn, "rp", project, false, &quiet_out()).unwrap();
        let off = storage::get_plan_by_slug(&conn, "rp", project)
            .unwrap()
            .unwrap();
        assert_eq!(
            off.review_enabled,
            Some(false),
            "after `off`, the plan-scope override is Some(false)"
        );
    }

    #[test]
    fn test_cmd_plan_review_unknown_slug_errors() {
        let conn = crate::db::open_memory().expect("open_memory");
        let err = cmd_plan_review(&conn, "nope", "/tmp/r-noplan", true, &quiet_out())
            .expect_err("missing plan must error");
        assert!(err.to_string().contains("Plan not found: nope"));
    }

    /// End-to-end precedence resolution through the actual scope setters:
    /// step ?? plan ?? config ?? false (docs/dag-redesign.md §6).
    #[test]
    fn test_review_scope_precedence_end_to_end() {
        use crate::config::{Config, effective_review_enabled};
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/r-prec";
        let plan = storage::create_plan(
            &conn,
            storage::NewPlan {
                slug: "pp",
                project,
                branch_name: "br",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "S",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: None,
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        let cfg_on = {
            let mut c = Config::default();
            c.review.enabled = Some(true);
            c
        };
        let cfg_off = Config::default(); // review.enabled = None

        // All inherit + config None ⇒ false.
        let p = storage::get_plan_by_slug(&conn, "pp", project)
            .unwrap()
            .unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert!(!effective_review_enabled(&s, &p, &cfg_off));

        // config ON, plan/step inherit ⇒ true (falls through to config).
        assert!(effective_review_enabled(&s, &p, &cfg_on));

        // plan OFF beats config ON.
        cmd_plan_review(&conn, "pp", project, false, &quiet_out()).unwrap();
        let p = storage::get_plan_by_slug(&conn, "pp", project)
            .unwrap()
            .unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert!(!effective_review_enabled(&s, &p, &cfg_on));

        // step ON beats plan OFF (and config).
        storage::set_step_review_enabled(&conn, &step.id, Some(true)).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert!(effective_review_enabled(&s, &p, &cfg_on));
        assert!(
            effective_review_enabled(&s, &p, &cfg_off),
            "step ON wins even when config is unset"
        );

        // step cleared back to inherit ⇒ falls to plan OFF.
        storage::set_step_review_enabled(&conn, &step.id, None).unwrap();
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert!(!effective_review_enabled(&s, &p, &cfg_on));
    }

    // ----------------------------------------------------------------------
    // FINDING 5: branch/slug validated up front, before any DB write.
    // ----------------------------------------------------------------------

    #[test]
    fn test_plan_create_invalid_branch_errors_and_writes_no_plan() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/pc-badbranch";

        let err = plan_create(
            &conn,
            PlanCreateArgs {
                slug: "myslug",
                project,
                description: None,
                branch: Some("feat/bad..branch"),
                harness: None,
                agent: None,
                max_review_corrections: None,
                tests: &[],
                depends_on: &[],
            },
            &quiet_out(),
        )
        .expect_err("an invalid branch name must fail fast");
        assert!(
            err.to_string().contains("invalid branch name"),
            "error must cite the branch rule: {err}"
        );

        // Nothing was persisted: no plan row exists for the slug.
        assert!(
            storage::get_plan_by_slug(&conn, "myslug", project)
                .unwrap()
                .is_none(),
            "no plan row may be written when the branch is invalid"
        );
    }

    #[test]
    fn test_plan_create_blank_slug_errors_and_writes_no_plan() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/pc-blankslug";

        let err = plan_create(
            &conn,
            PlanCreateArgs {
                slug: "   ",
                project,
                description: None,
                branch: None,
                harness: None,
                agent: None,
                max_review_corrections: None,
                tests: &[],
                depends_on: &[],
            },
            &quiet_out(),
        )
        .expect_err("a blank slug must fail fast");
        assert!(
            err.to_string().contains("invalid plan slug"),
            "error must cite the slug rule: {err}"
        );
        // The (blank) slug branch defaults from slug, so creation aborts
        // before any write — verify the plan list stayed empty.
        let plans = storage::list_plans(&conn, project, false).unwrap();
        assert!(plans.is_empty(), "no plan row may be written: {plans:?}");
    }

    #[test]
    fn test_plan_create_negative_max_review_corrections_errors_and_writes_no_plan() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/pc-negative-review-cap";

        let err = plan_create(
            &conn,
            PlanCreateArgs {
                slug: "bad-cap",
                project,
                description: None,
                branch: None,
                harness: None,
                agent: None,
                max_review_corrections: Some(-1),
                tests: &[],
                depends_on: &[],
            },
            &quiet_out(),
        )
        .expect_err("a negative review correction cap must fail fast");
        assert!(
            err.to_string().contains("must be non-negative"),
            "error must cite the invalid cap: {err}"
        );
        assert!(
            storage::get_plan_by_slug(&conn, "bad-cap", project)
                .unwrap()
                .is_none(),
            "no plan row may be written when the cap is invalid"
        );
    }

    #[test]
    fn test_plan_create_valid_branch_succeeds() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/pc-okbranch";
        plan_create(
            &conn,
            PlanCreateArgs {
                slug: "good-slug",
                project,
                description: Some("a description"),
                branch: Some("feat/ok"),
                harness: None,
                agent: None,
                max_review_corrections: None,
                tests: &[],
                depends_on: &[],
            },
            &quiet_out(),
        )
        .expect("a valid branch must create the plan");
        let plan = storage::get_plan_by_slug(&conn, "good-slug", project)
            .unwrap()
            .expect("plan row must exist");
        assert_eq!(plan.branch_name, "feat/ok");
    }

    // ----------------------------------------------------------------------
    // Transactional `plan_create`: a failure raised *after* the plan row is
    // inserted (inside the `db::with_tx` block) must roll the whole thing
    // back so no partially-configured plan row survives.
    //
    // The injectable late failure used here: passing the *same* dependency
    // slug twice. `plan_create` resolves each dep slug independently (no
    // dedup), so both copies clear the pre-tx existence check. Inside the
    // tx, `create_plan` inserts the plan row and the first
    // `add_plan_dependency` succeeds; the second INSERT then violates
    // `plan_dependencies`'s `PRIMARY KEY (plan_id, depends_on_plan_id)`. That
    // error propagates out of the `with_tx` closure, the transaction is
    // dropped without commit, and the plan row must NOT persist.
    // ----------------------------------------------------------------------

    #[test]
    fn test_plan_create_rolls_back_plan_row_on_late_dependency_failure() {
        let conn = crate::db::open_memory().expect("open_memory");
        let project = "/tmp/pc-rollback";

        // A pre-existing dependency plan that the new plan will depend on.
        storage::create_plan(
            &conn,
            storage::NewPlan {
                slug: "dep-a",
                project,
                branch_name: "dep-a",
                description: "dep",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();

        // Trigger a late failure by listing the same dependency twice: the
        // second `add_plan_dependency` INSERT hits the PRIMARY KEY constraint
        // *after* the plan row has already been inserted in the same tx.
        let err = plan_create(
            &conn,
            PlanCreateArgs {
                slug: "rolled-back",
                project,
                description: Some("desc"),
                branch: Some("feat/rb"),
                harness: None,
                agent: None,
                max_review_corrections: None,
                tests: &[],
                depends_on: &["dep-a".to_string(), "dep-a".to_string()],
            },
            &quiet_out(),
        )
        .expect_err("a duplicate dependency must fail inside the transaction");
        // The failure must come from the dependency-wiring step, not the
        // pre-tx validation (which would not exercise the rollback path).
        assert!(
            err.to_string()
                .contains("Failed to add dependency on 'dep-a'"),
            "failure must originate from the in-tx dependency wiring: {err}"
        );

        // Full rollback: the plan row inserted earlier in the same tx must
        // NOT have persisted.
        assert!(
            storage::get_plan_by_slug(&conn, "rolled-back", project)
                .unwrap()
                .is_none(),
            "the plan row must be rolled back when a later in-tx step fails"
        );

        // And only the original dependency plan remains.
        let slugs: Vec<String> = storage::list_plans(&conn, project, true)
            .unwrap()
            .into_iter()
            .map(|p| p.slug)
            .collect();
        assert_eq!(
            slugs,
            vec!["dep-a".to_string()],
            "no partial plan should leak: {slugs:?}"
        );
    }
}
