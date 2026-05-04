// Palette command dispatcher (TUI-plan.md §9).
//
// Bridges parsed [`PaletteCommand`] values to a testable [`PaletteAction`]
// describing the side effect the dispatcher loop should run. The split lets
// us exercise every command end-to-end through the parser without forking a
// subprocess, opening a confirm dialog, or touching the database.
//
// Step #30 wires the following commands through to real actions:
// `/run <branch>`, `/plan harness [<name>]`, `/plan show [<slug>]`,
// `/plan archive [<slug>]`, `/plan unarchive <slug>`, `/plan delete <slug>`,
// `/plan approve [<slug>]`. Other recognized commands resolve to
// [`PaletteAction::Deferred`]; their wiring lands in step #31 onward.

use crate::plan::PlanStatus;
use crate::tui::palette::{ParseError, PaletteCommand};
use crate::tui::run_dialog::RunTarget;
use crate::tui::toast::ToastKind;

// ---------------------------------------------------------------------------
// Per-plan view of what the dispatcher needs to know
// ---------------------------------------------------------------------------

/// Lightweight projection of [`crate::plan::Plan`] used by the dispatcher.
///
/// Carries just enough to resolve named-slug arguments to plan IDs, branch
/// names, and statuses. Built from the surrounding view's plan list (e.g.
/// `PlanListApp.tiles`, `ArchivedListApp.tiles`, or the open plan in
/// plan-detail) by the caller before invoking [`dispatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanRef {
    pub id: String,
    pub slug: String,
    pub branch_name: String,
    pub status: PlanStatus,
}

/// Read-only context the pure dispatcher reads while resolving a command.
///
/// Slices borrow from the calling view's state so we don't have to clone the
/// plan list for every keystroke. `focused_slug` is the slug of the plan
/// under the cursor (plan-list) or the open plan (plan-detail) — used to
/// resolve commands that take an optional `[<slug>]`.
pub struct PaletteContext<'a> {
    /// Default harness name from `Config.default_harness`, used by
    /// `/plan harness` when no harness is supplied.
    pub default_harness: &'a str,
    /// Slug to substitute for `[<slug>]` when omitted. `None` means the
    /// surrounding view has no inferable target (e.g. archived list with
    /// nothing under the cursor).
    pub focused_slug: Option<&'a str>,
    /// Selection-aware run targets resolved by the caller (selection in
    /// pick order, or just the cursor's plan when no selection). Empty
    /// means the surrounding view has nothing to run.
    pub run_targets: &'a [RunTarget],
    /// All non-archived plans visible to the surrounding view, used to
    /// resolve a named slug for `/plan show|archive|delete|approve|harness`.
    pub plans: &'a [PlanRef],
    /// Archived plans, used to resolve a named slug for `/plan unarchive`.
    pub archived: &'a [PlanRef],
}

// ---------------------------------------------------------------------------
// Action enum — what the dispatcher loop should do next
// ---------------------------------------------------------------------------

/// One of these is returned for every dispatched command. The caller in the
/// TUI event loop matches on the variant and runs the corresponding side
/// effect (open a dialog, spawn a subprocess, push a view, or toast).
///
/// Variants for commands that step #30 doesn't wire collapse into
/// [`PaletteAction::Deferred`] so future steps can light them up incrementally
/// without breaking existing tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteAction {
    /// Nothing to do (e.g. blank input).
    None,
    /// Show a toast in the surrounding view's queue.
    Toast {
        message: String,
        kind: ToastKind,
    },
    /// `/run` — open the branch-choice dialog seeded with `default_branch`
    /// for `plan_count` plans against `targets`.
    OpenRunDialog {
        default_branch: String,
        plan_count: usize,
        targets: Vec<RunTarget>,
    },
    /// `/run <branch>` — short-circuit the dialog. Caller must check whether
    /// `branch` exists, prompt to create it if not, switch to it, and then
    /// spawn one runner per target with `--current-branch`.
    ///
    /// `force_current_branch` is true whenever there is more than one target
    /// (TUI-plan.md §9.1: multi-plan runs always pass `--current-branch`).
    RunOnBranch {
        branch: String,
        targets: Vec<RunTarget>,
        force_current_branch: bool,
    },
    /// `/plan harness [<name>]` — invoke the existing
    /// [`crate::plan_harness::run_plan_harness`] flow with the named harness
    /// (or the default), optionally targeting an existing plan.
    SpawnPlanHarness {
        harness: String,
        slug: Option<String>,
    },
    /// `/plan show [<slug>]` — push the plan-detail view for `slug`.
    PushPlanDetail { slug: String },
    /// `/plan archive [<slug>]` — open a yes/no confirm that, on yes, runs
    /// `update_plan_status(plan_id, Archived)` and refreshes the surrounding
    /// view. Mirrors the `d` keybinding on the plan-list tile.
    OpenConfirmArchive { plan_id: String, slug: String },
    /// `/plan unarchive <slug>` — flip status back to `Ready` and refresh
    /// the surrounding view. No confirm — mirrors the archived-list `enter`
    /// keybinding.
    Unarchive { plan_id: String, slug: String },
    /// `/plan delete <slug>` — open a yes/no confirm that, on yes, runs
    /// `delete_plan(plan_id)`. Mirrors the archived-list `d` keybinding.
    OpenConfirmDelete { plan_id: String, slug: String },
    /// `/plan approve [<slug>]` — flip `Planning` → `Ready` (toast otherwise).
    /// No confirm — mirrors the plan-list `A` keybinding.
    Approve { plan_id: String, slug: String },
    /// Recognized command whose dispatch lands in a later tui-v1 step. Carries
    /// the canonical label so the loop can render a placeholder toast.
    Deferred(&'static str),
}

// ---------------------------------------------------------------------------
// Pure dispatcher
// ---------------------------------------------------------------------------

/// Map a parsed [`PaletteCommand`] to a [`PaletteAction`] without performing
/// any side effects. Pure so tests can drive every command path through the
/// parser without a TUI, DB, or subprocess.
pub fn dispatch(cmd: &PaletteCommand, ctx: &PaletteContext<'_>) -> PaletteAction {
    match cmd {
        // -- /run [<branch>] ----------------------------------------------
        PaletteCommand::Run(None) => dispatch_run_dialog(ctx),
        PaletteCommand::Run(Some(branch)) => dispatch_run_branch(branch, ctx),

        // -- /plan harness [<name>] ---------------------------------------
        PaletteCommand::PlanHarness(name) => dispatch_plan_harness(name.as_deref(), ctx),

        // -- /plan show [<slug>] ------------------------------------------
        PaletteCommand::PlanShow(slug) => dispatch_plan_show(slug.as_deref(), ctx),

        // -- /plan archive [<slug>] ---------------------------------------
        PaletteCommand::PlanArchive(slug) => dispatch_plan_archive(slug.as_deref(), ctx),

        // -- /plan unarchive <slug> ---------------------------------------
        PaletteCommand::PlanUnarchive(slug) => dispatch_plan_unarchive(slug, ctx),

        // -- /plan delete <slug> ------------------------------------------
        PaletteCommand::PlanDelete(slug) => dispatch_plan_delete(slug, ctx),

        // -- /plan approve [<slug>] ---------------------------------------
        PaletteCommand::PlanApprove(slug) => dispatch_plan_approve(slug.as_deref(), ctx),

        // Everything else is wired in steps 31–32 (per the tui-v1 plan).
        // Keep the label so the dispatcher loop can render a stable
        // "<verb> not yet implemented" toast.
        other => PaletteAction::Deferred(other.label()),
    }
}

/// Map a [`ParseError`] to an action so the dispatcher loop can render a
/// toast for the user without diverging from `dispatch`'s shape. Empty
/// inputs become `None` (silent close); everything else becomes an
/// `Error`-kind toast describing the failure.
pub fn dispatch_parse_error(err: &ParseError) -> PaletteAction {
    match err {
        ParseError::Empty => PaletteAction::None,
        ParseError::Unknown(verb) => PaletteAction::Toast {
            message: format!("Unknown command: {verb}"),
            kind: ToastKind::Error,
        },
        ParseError::MissingArgument { command, arg } => PaletteAction::Toast {
            message: format!("{command} requires {arg}"),
            kind: ToastKind::Error,
        },
        ParseError::InvalidArgument {
            command,
            arg,
            reason,
        } => PaletteAction::Toast {
            message: format!("{command}: bad argument `{arg}` ({reason})"),
            kind: ToastKind::Error,
        },
    }
}

// ---------------------------------------------------------------------------
// Per-command resolvers
// ---------------------------------------------------------------------------

fn dispatch_run_dialog(ctx: &PaletteContext<'_>) -> PaletteAction {
    if ctx.run_targets.is_empty() {
        return PaletteAction::Toast {
            message: "No plan to run.".to_string(),
            kind: ToastKind::Info,
        };
    }
    // The dialog seed branch is the first target's plan branch — the same
    // rule the keybinding uses when there's no selection.
    let default_branch = ctx.run_targets[0].default_branch.clone();
    PaletteAction::OpenRunDialog {
        default_branch,
        plan_count: ctx.run_targets.len(),
        targets: ctx.run_targets.to_vec(),
    }
}

fn dispatch_run_branch(branch: &str, ctx: &PaletteContext<'_>) -> PaletteAction {
    if ctx.run_targets.is_empty() {
        return PaletteAction::Toast {
            message: "No plan to run.".to_string(),
            kind: ToastKind::Info,
        };
    }
    PaletteAction::RunOnBranch {
        branch: branch.to_string(),
        targets: ctx.run_targets.to_vec(),
        // Multi-plan runs always force --current-branch (TUI-plan.md §9.1).
        force_current_branch: ctx.run_targets.len() > 1,
    }
}

fn dispatch_plan_harness(name: Option<&str>, ctx: &PaletteContext<'_>) -> PaletteAction {
    let harness = name
        .map(str::to_string)
        .unwrap_or_else(|| ctx.default_harness.to_string());
    // Optional slug binding: when the user is sitting on a plan, target it;
    // otherwise let the harness create a new plan from scratch.
    let slug = ctx.focused_slug.map(str::to_string);
    PaletteAction::SpawnPlanHarness { harness, slug }
}

fn dispatch_plan_show(slug: Option<&str>, ctx: &PaletteContext<'_>) -> PaletteAction {
    match resolve_slug(slug, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::PushPlanDetail { slug: target.slug },
        ResolvedSlug::Missing => PaletteAction::Toast {
            message: "No plan selected.".to_string(),
            kind: ToastKind::Info,
        },
        ResolvedSlug::Unknown(name) => unknown_plan_toast(&name),
    }
}

fn dispatch_plan_archive(slug: Option<&str>, ctx: &PaletteContext<'_>) -> PaletteAction {
    match resolve_slug(slug, ctx) {
        ResolvedSlug::Some(target) => {
            if target.status == PlanStatus::Archived {
                PaletteAction::Toast {
                    message: format!("Plan `{}` is already archived.", target.slug),
                    kind: ToastKind::Info,
                }
            } else {
                PaletteAction::OpenConfirmArchive {
                    plan_id: target.id,
                    slug: target.slug,
                }
            }
        }
        ResolvedSlug::Missing => PaletteAction::Toast {
            message: "No plan selected.".to_string(),
            kind: ToastKind::Info,
        },
        ResolvedSlug::Unknown(name) => unknown_plan_toast(&name),
    }
}

fn dispatch_plan_unarchive(slug: &str, ctx: &PaletteContext<'_>) -> PaletteAction {
    if let Some(plan) = find_by_slug(ctx.archived, slug) {
        return PaletteAction::Unarchive {
            plan_id: plan.id.clone(),
            slug: plan.slug.clone(),
        };
    }
    // Fallback: maybe the user spelled a real plan that just isn't archived.
    // Surface a friendlier message in that case.
    if find_by_slug(ctx.plans, slug).is_some() {
        return PaletteAction::Toast {
            message: format!("Plan `{slug}` is not archived."),
            kind: ToastKind::Info,
        };
    }
    unknown_plan_toast(slug)
}

fn dispatch_plan_delete(slug: &str, ctx: &PaletteContext<'_>) -> PaletteAction {
    if let Some(plan) = find_by_slug(ctx.plans, slug).or_else(|| find_by_slug(ctx.archived, slug)) {
        PaletteAction::OpenConfirmDelete {
            plan_id: plan.id.clone(),
            slug: plan.slug.clone(),
        }
    } else {
        unknown_plan_toast(slug)
    }
}

fn dispatch_plan_approve(slug: Option<&str>, ctx: &PaletteContext<'_>) -> PaletteAction {
    match resolve_slug(slug, ctx) {
        ResolvedSlug::Some(target) => {
            if target.status == PlanStatus::Planning {
                PaletteAction::Approve {
                    plan_id: target.id,
                    slug: target.slug,
                }
            } else {
                // Mirror the `A` keybinding's info toast for non-Planning
                // plans (TUI-plan.md §5).
                PaletteAction::Toast {
                    message: format!(
                        "Plan is in {} status; nothing to approve.",
                        target.status
                    ),
                    kind: ToastKind::Info,
                }
            }
        }
        ResolvedSlug::Missing => PaletteAction::Toast {
            message: "No plan selected.".to_string(),
            kind: ToastKind::Info,
        },
        ResolvedSlug::Unknown(name) => unknown_plan_toast(&name),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

enum ResolvedSlug {
    /// Found a matching plan (slug given or focused slug looked up).
    Some(PlanRef),
    /// No slug given and the surrounding view has no focus.
    Missing,
    /// Slug given but no plan with that slug is visible to the dispatcher.
    Unknown(String),
}

fn resolve_slug(explicit: Option<&str>, ctx: &PaletteContext<'_>) -> ResolvedSlug {
    if let Some(name) = explicit {
        // Explicit lookup hits both active and archived so commands like
        // `/plan show` work on archived plans too.
        if let Some(plan) = find_by_slug(ctx.plans, name) {
            return ResolvedSlug::Some(plan.clone());
        }
        if let Some(plan) = find_by_slug(ctx.archived, name) {
            return ResolvedSlug::Some(plan.clone());
        }
        return ResolvedSlug::Unknown(name.to_string());
    }
    let Some(focus) = ctx.focused_slug else {
        return ResolvedSlug::Missing;
    };
    if let Some(plan) = find_by_slug(ctx.plans, focus) {
        return ResolvedSlug::Some(plan.clone());
    }
    if let Some(plan) = find_by_slug(ctx.archived, focus) {
        return ResolvedSlug::Some(plan.clone());
    }
    // Focus pointing at a slug not in our pools is a programming error in
    // the caller; surface as missing rather than panicking.
    ResolvedSlug::Missing
}

fn find_by_slug<'a>(pool: &'a [PlanRef], slug: &str) -> Option<&'a PlanRef> {
    pool.iter().find(|p| p.slug == slug)
}

fn unknown_plan_toast(slug: &str) -> PaletteAction {
    PaletteAction::Toast {
        message: format!("Unknown plan: {slug}"),
        kind: ToastKind::Error,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanStatus;
    use crate::tui::palette::parse;
    use crate::tui::run_dialog::RunTarget;

    fn plan_ref(slug: &str, status: PlanStatus) -> PlanRef {
        PlanRef {
            id: format!("id-{slug}"),
            slug: slug.to_string(),
            branch_name: format!("branch-{slug}"),
            status,
        }
    }

    fn target(slug: &str, branch: &str) -> RunTarget {
        RunTarget {
            slug: slug.to_string(),
            default_branch: branch.to_string(),
        }
    }

    /// Helper for tests that need a populated context. Each field can be
    /// overridden by mutating the returned struct directly.
    struct Ctx {
        default_harness: String,
        focused_slug: Option<String>,
        run_targets: Vec<RunTarget>,
        plans: Vec<PlanRef>,
        archived: Vec<PlanRef>,
    }

    impl Ctx {
        fn new() -> Self {
            Self {
                default_harness: "claude".to_string(),
                focused_slug: None,
                run_targets: vec![],
                plans: vec![],
                archived: vec![],
            }
        }
        fn as_ctx(&self) -> PaletteContext<'_> {
            PaletteContext {
                default_harness: &self.default_harness,
                focused_slug: self.focused_slug.as_deref(),
                run_targets: &self.run_targets,
                plans: &self.plans,
                archived: &self.archived,
            }
        }
    }

    fn dispatch_str(input: &str, c: &Ctx) -> PaletteAction {
        let cmd = parse(input).expect("parse should succeed");
        dispatch(&cmd, &c.as_ctx())
    }

    // -- /run --------------------------------------------------------------

    #[test]
    fn run_with_no_targets_toasts() {
        let c = Ctx::new();
        let action = dispatch_str("/run", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "No plan to run.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn run_with_single_target_opens_dialog_with_plan_branch() {
        let mut c = Ctx::new();
        c.run_targets = vec![target("alpha", "feature-x")];
        let action = dispatch_str("/run", &c);
        assert_eq!(
            action,
            PaletteAction::OpenRunDialog {
                default_branch: "feature-x".to_string(),
                plan_count: 1,
                targets: vec![target("alpha", "feature-x")],
            }
        );
    }

    #[test]
    fn run_with_multi_target_uses_first_branch_as_default() {
        let mut c = Ctx::new();
        c.run_targets = vec![
            target("alpha", "feat-a"),
            target("beta", "feat-b"),
        ];
        let action = dispatch_str("/run", &c);
        match action {
            PaletteAction::OpenRunDialog {
                default_branch,
                plan_count,
                targets,
            } => {
                assert_eq!(default_branch, "feat-a");
                assert_eq!(plan_count, 2);
                assert_eq!(targets.len(), 2);
            }
            other => panic!("unexpected action {other:?}"),
        }
    }

    // -- /run <branch> -----------------------------------------------------

    #[test]
    fn run_branch_with_no_targets_toasts() {
        let c = Ctx::new();
        let action = dispatch_str("/run feature-x", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "No plan to run.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn run_branch_single_plan_does_not_force_current_branch() {
        let mut c = Ctx::new();
        c.run_targets = vec![target("alpha", "main")];
        let action = dispatch_str("/run hotfix", &c);
        assert_eq!(
            action,
            PaletteAction::RunOnBranch {
                branch: "hotfix".to_string(),
                targets: vec![target("alpha", "main")],
                force_current_branch: false,
            }
        );
    }

    #[test]
    fn run_branch_multi_plan_forces_current_branch() {
        let mut c = Ctx::new();
        c.run_targets = vec![target("a", "ba"), target("b", "bb")];
        let action = dispatch_str("/run integration", &c);
        match action {
            PaletteAction::RunOnBranch {
                branch,
                targets,
                force_current_branch,
            } => {
                assert_eq!(branch, "integration");
                assert_eq!(targets.len(), 2);
                assert!(force_current_branch);
            }
            other => panic!("unexpected action {other:?}"),
        }
    }

    // -- /plan harness -----------------------------------------------------

    #[test]
    fn plan_harness_no_name_uses_default() {
        let c = Ctx::new();
        let action = dispatch_str("/plan harness", &c);
        assert_eq!(
            action,
            PaletteAction::SpawnPlanHarness {
                harness: "claude".to_string(),
                slug: None,
            }
        );
    }

    #[test]
    fn plan_harness_named_overrides_default() {
        let c = Ctx::new();
        let action = dispatch_str("/plan harness codex", &c);
        assert_eq!(
            action,
            PaletteAction::SpawnPlanHarness {
                harness: "codex".to_string(),
                slug: None,
            }
        );
    }

    #[test]
    fn plan_harness_inherits_focused_slug() {
        let mut c = Ctx::new();
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/plan harness", &c);
        assert_eq!(
            action,
            PaletteAction::SpawnPlanHarness {
                harness: "claude".to_string(),
                slug: Some("alpha".to_string()),
            }
        );
    }

    // -- /plan show -------------------------------------------------------

    #[test]
    fn plan_show_named_pushes_detail() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan show alpha", &c);
        assert_eq!(
            action,
            PaletteAction::PushPlanDetail {
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_show_unknown_slug_toasts() {
        let c = Ctx::new();
        let action = dispatch_str("/plan show ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    #[test]
    fn plan_show_omitted_uses_focused_slug() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("focused", PlanStatus::Ready)];
        c.focused_slug = Some("focused".to_string());
        let action = dispatch_str("/plan show", &c);
        assert_eq!(
            action,
            PaletteAction::PushPlanDetail {
                slug: "focused".to_string(),
            }
        );
    }

    #[test]
    fn plan_show_omitted_with_no_focus_toasts_info() {
        let c = Ctx::new();
        let action = dispatch_str("/plan show", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "No plan selected.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn plan_show_resolves_archived_slug() {
        // Archived plans are addressable via /plan show <slug>.
        let mut c = Ctx::new();
        c.archived = vec![plan_ref("frozen", PlanStatus::Archived)];
        let action = dispatch_str("/plan show frozen", &c);
        assert_eq!(
            action,
            PaletteAction::PushPlanDetail {
                slug: "frozen".to_string(),
            }
        );
    }

    // -- /plan archive ----------------------------------------------------

    #[test]
    fn plan_archive_named_opens_confirm() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan archive alpha", &c);
        assert_eq!(
            action,
            PaletteAction::OpenConfirmArchive {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_archive_already_archived_toasts() {
        let mut c = Ctx::new();
        c.archived = vec![plan_ref("alpha", PlanStatus::Archived)];
        let action = dispatch_str("/plan archive alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Plan `alpha` is already archived.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn plan_archive_unknown_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/plan archive ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    #[test]
    fn plan_archive_omitted_uses_focus() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/plan archive", &c);
        assert_eq!(
            action,
            PaletteAction::OpenConfirmArchive {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    // -- /plan unarchive --------------------------------------------------

    #[test]
    fn plan_unarchive_named_unarchives() {
        let mut c = Ctx::new();
        c.archived = vec![plan_ref("alpha", PlanStatus::Archived)];
        let action = dispatch_str("/plan unarchive alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Unarchive {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_unarchive_active_plan_toasts_info() {
        // The slug exists but isn't archived — that's a UX miss, not an
        // unknown plan.
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan unarchive alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Plan `alpha` is not archived.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn plan_unarchive_unknown_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/plan unarchive ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    // -- /plan delete -----------------------------------------------------

    #[test]
    fn plan_delete_active_opens_confirm() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan delete alpha", &c);
        assert_eq!(
            action,
            PaletteAction::OpenConfirmDelete {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_delete_archived_also_opens_confirm() {
        // Delete works regardless of archived state — mirrors the archived-
        // list `d` keybinding.
        let mut c = Ctx::new();
        c.archived = vec![plan_ref("alpha", PlanStatus::Archived)];
        let action = dispatch_str("/plan delete alpha", &c);
        assert_eq!(
            action,
            PaletteAction::OpenConfirmDelete {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_delete_unknown_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/plan delete ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    // -- /plan approve ----------------------------------------------------

    #[test]
    fn plan_approve_planning_flips_to_ready() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Planning)];
        let action = dispatch_str("/plan approve alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Approve {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    #[test]
    fn plan_approve_non_planning_toasts_info() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::InProgress)];
        let action = dispatch_str("/plan approve alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Plan is in in_progress status; nothing to approve.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn plan_approve_unknown_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/plan approve ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    #[test]
    fn plan_approve_omitted_uses_focus() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Planning)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/plan approve", &c);
        assert_eq!(
            action,
            PaletteAction::Approve {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
            }
        );
    }

    // -- Deferred passthrough ---------------------------------------------

    #[test]
    fn unwired_commands_become_deferred_with_label() {
        // Step 30 doesn't wire /step add — it should fall through to
        // Deferred so step 31 can pick it up.
        let c = Ctx::new();
        let action = dispatch_str("/step add hello world", &c);
        assert_eq!(action, PaletteAction::Deferred("/step add"));
    }

    // -- Parse-error mapping ----------------------------------------------

    #[test]
    fn parse_error_empty_maps_to_none() {
        assert_eq!(
            dispatch_parse_error(&ParseError::Empty),
            PaletteAction::None
        );
    }

    #[test]
    fn parse_error_unknown_maps_to_error_toast() {
        assert_eq!(
            dispatch_parse_error(&ParseError::Unknown("foobar".to_string())),
            PaletteAction::Toast {
                message: "Unknown command: foobar".to_string(),
                kind: ToastKind::Error,
            }
        );
    }

    #[test]
    fn parse_error_missing_arg_maps_to_error_toast() {
        assert_eq!(
            dispatch_parse_error(&ParseError::MissingArgument {
                command: "/plan delete",
                arg: "<slug>",
            }),
            PaletteAction::Toast {
                message: "/plan delete requires <slug>".to_string(),
                kind: ToastKind::Error,
            }
        );
    }

    #[test]
    fn parse_error_invalid_arg_maps_to_error_toast() {
        assert_eq!(
            dispatch_parse_error(&ParseError::InvalidArgument {
                command: "/step skip",
                arg: "foo".to_string(),
                reason: "expected step number",
            }),
            PaletteAction::Toast {
                message: "/step skip: bad argument `foo` (expected step number)".to_string(),
                kind: ToastKind::Error,
            }
        );
    }
}
