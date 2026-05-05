// Palette command dispatcher (TUI-plan.md §9).
//
// Bridges parsed [`PaletteCommand`] values to a testable [`PaletteAction`]
// describing the side effect the dispatcher loop should run. The split lets
// us exercise every command end-to-end through the parser without forking a
// subprocess, opening a confirm dialog, or touching the database.
//
// Step #30 wired `/run [<branch>]`, `/plan harness [<name>]`,
// `/plan show [<slug>]`, `/plan archive [<slug>]`, `/plan unarchive <slug>`,
// `/plan delete <slug>`, `/plan approve [<slug>]`.
//
// Step #31 wires the v1-deferred routes per TUI-plan.md §9:
// * `/plan questions on|off [<slug>]` — flips `plans.questions_enabled` via
//   the surrounding view's storage helper.
// * `/step add <title>` — appends a new step to the focused plan.
// * `/step skip [<num>]` — mirrors the `s` keybinding's `runner::skip_step`.
// * `/step move <num> --to <m>` — re-keys a step into a new position.
//
// The remaining v1-deferred commands route to `PaletteAction::ComingSoon`
// with the actual sub-view step number from the tui-v1 plan map (43 — the
// help overlay is the last surface still pending).
//
// Step #32 wires `/cancel`, `/export`, `/import`, `/quit`, `/help`:
// * `/cancel` — mirrors the `S` keybinding's "stop the live run" action.
//   The pure dispatcher always emits `CancelRun`; the consuming view checks
//   whether there's an actual live run before forwarding the signal.
// * `/export <slug> [-o <path>]` — resolves the slug against the visible
//   plan list and emits an `Export` action. When `-o` is omitted the
//   consumer writes to `<slug>.ralph.json` in the cwd (TUI-plan.md §9).
// * `/import <path>` — emits an `Import` action; the consuming view reads
//   the file and prompts for a fresh slug if it conflicts with an existing
//   plan.
// * `/quit` (and `/q`) — emits `Quit`; the consuming view exits the TUI.
// * `/help` — surfaces a `ComingSoon` action targeting step 43 (the help
//   overlay) so the dispatcher loop can toast a placeholder until then.

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

/// Lightweight projection of the focused step inside step-detail, used by
/// `/step set-hook|unset-hook` to resolve the per-step sub-view target.
///
/// Only views that actually have a focused step (currently step-detail) set
/// this; other views leave it `None` and `/step set-hook` falls through to a
/// "Open a step first…" toast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FocusedStep {
    /// `steps.id` for the focused step.
    pub id: String,
    /// Display label for the step (e.g. `#3 — Step title`), forwarded into
    /// the step-hook sub-view's title bar so the user always knows which
    /// step they're scoped to.
    pub label: String,
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
    /// Step under the cursor (step-detail only). Used to resolve
    /// `/step set-hook|unset-hook`. Other views leave this `None`.
    pub focused_step: Option<&'a FocusedStep>,
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
/// Commands whose sub-views haven't landed yet collapse into
/// [`PaletteAction::ComingSoon`] with the target tui-v1 step number, so the
/// dispatcher loop can render a uniform "coming soon" toast until the
/// real implementation lands.
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
    /// `/plan questions on|off [<slug>]` — flip `plans.questions_enabled` via
    /// `storage::set_plan_questions_enabled`. Mirrors the `Q` keybinding on
    /// the plan-list tile.
    SetQuestionsEnabled {
        plan_id: String,
        slug: String,
        enabled: bool,
    },
    /// `/step add <title>` — append a new step to the focused plan via
    /// `storage::create_step`. The caller defaults the rest of the column
    /// values (no agent, no harness override, empty criteria, etc.) — the
    /// step-detail view is the right place to fill those in afterwards.
    AddStep {
        plan_id: String,
        slug: String,
        title: String,
    },
    /// `/step skip [<num>]` — mirrors the `s` keybinding via `runner::skip_step`.
    /// `step_num` is 1-based; `None` means "skip the current step" (whichever
    /// step `runner::skip_step`'s default selection lands on).
    SkipStep {
        plan_id: String,
        slug: String,
        step_num: Option<u32>,
    },
    /// `/step move <num> --to <m>` — re-key the 1-based step at position
    /// `from` to land at position `to`. The caller resolves a fractional sort
    /// key between the new neighbours via `frac_index::key_between` and
    /// writes it with `storage::update_step_sort_key`.
    MoveStep {
        plan_id: String,
        slug: String,
        from: u32,
        to: u32,
    },
    /// `/cancel` — request that the consuming view stop the live run.
    /// Mirrors the `S` keybinding (TUI-plan.md §9). The pure dispatcher
    /// emits this unconditionally; the view checks whether there is a
    /// live run before forwarding the signal and toasts otherwise.
    CancelRun,
    /// `/export <slug> [-o <path>]` — write the resolved plan's portable
    /// JSON to `output` (or `<slug>.ralph.json` in cwd when `output` is
    /// `None`). The slug has already been resolved against the visible
    /// plan list, so the consumer can call [`crate::export::export_plan`]
    /// directly without re-validating.
    Export {
        slug: String,
        output: Option<String>,
    },
    /// `/import <path>` — read `path` and create a new plan. The consumer
    /// is responsible for calling [`crate::import::read_plan_file`] and
    /// prompting for a fresh slug if the imported slug collides with an
    /// existing plan in the project.
    Import { path: String },
    /// `/quit` or `/q` — exit the TUI cleanly.
    Quit,
    /// `/plan dependency add|remove|list [<slug>]` — push the
    /// plan-dependencies sub-view for `plan_id` (TUI-plan.md §1, step 33).
    /// All three subcommands route here because the sub-view itself owns
    /// add/remove via `a`/`d` keybindings; the verb in the palette is just
    /// the entry door. The orchestrator resolves `slug` (explicit if given,
    /// else focus) and the consuming view loads dep / candidate snapshots
    /// from storage on entry.
    OpenPlanDependencies { plan_id: String, slug: String },
    /// `/plan set-hook|unset-hook|hooks [<slug>]` — push the plan-hooks
    /// sub-view for `plan_id` (TUI-plan.md §1, step 34). The sub-view owns
    /// add/remove via `a`/`d` keybindings and a two-step lifecycle/hook
    /// picker; the verb in the palette is just the entry door. The
    /// orchestrator resolves `slug` (explicit if given, else focus) and the
    /// consuming view loads the attachment / library snapshots from storage
    /// and the hook library on entry.
    OpenPlanHooks { plan_id: String, slug: String },
    /// `/step set-hook|unset-hook` — push the step-hooks sub-view scoped to
    /// the focused step (TUI-plan.md §1, step 35). The sub-view owns
    /// add/remove via `a`/`d` keybindings and the same two-step
    /// lifecycle/hook picker as `OpenPlanHooks`. Resolved against
    /// `focused_slug` + `focused_step`; both must be present.
    OpenStepHooks {
        plan_id: String,
        step_id: String,
        plan_slug: String,
        step_label: String,
    },
    /// `/step edit --tags` — push the step-tags sub-view scoped to the
    /// focused step (TUI-plan.md §1, step 36). The sub-view owns add
    /// (`i`, then a text-input modal) and remove (`d`) interactively, so
    /// the verb in the palette is just the entry door. Resolved against
    /// `focused_slug` and `focused_step`; both must be present.
    OpenStepTags {
        step_id: String,
        plan_slug: String,
        step_label: String,
    },
    /// Recognized command stubbed for a later tui-v1 step (43 — help
    /// overlay). Caller renders a `Coming soon — landing in step <N>`
    /// info toast.
    ComingSoon {
        label: &'static str,
        target_step: u32,
    },
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

        // -- /plan questions on|off [<slug>] ------------------------------
        PaletteCommand::PlanQuestionsOn(slug) => {
            dispatch_plan_questions(slug.as_deref(), true, ctx)
        }
        PaletteCommand::PlanQuestionsOff(slug) => {
            dispatch_plan_questions(slug.as_deref(), false, ctx)
        }

        // -- /step add <title> --------------------------------------------
        PaletteCommand::StepAdd(title) => dispatch_step_add(title, ctx),

        // -- /step skip [<num>] -------------------------------------------
        PaletteCommand::StepSkip(num) => dispatch_step_skip(*num, ctx),

        // -- /step move <num> --to <m> ------------------------------------
        PaletteCommand::StepMove { num, to } => dispatch_step_move(*num, *to, ctx),

        // -- /plan dependency add|remove|list -----------------------------
        // All three subcommands push the same sub-view (step 33). The
        // sub-view owns add (`a`) / remove (`d`) interactively, so the
        // verb in the palette is just the entry door.
        PaletteCommand::PlanDependencyAdd
        | PaletteCommand::PlanDependencyRemove
        | PaletteCommand::PlanDependencyList => dispatch_plan_dependencies(ctx),

        // -- /plan set-hook|unset-hook|hooks ------------------------------
        // All three subcommands push the same sub-view (step 34). The
        // sub-view owns add (`a`) / remove (`d`) interactively, so the
        // verb in the palette is just the entry door.
        PaletteCommand::PlanSetHook | PaletteCommand::PlanUnsetHook | PaletteCommand::PlanHooks => {
            dispatch_plan_hooks(ctx)
        }

        // -- /step set-hook|unset-hook -------------------------------------
        // Both subcommands push the same sub-view (step 35). The sub-view
        // owns add (`a`) / remove (`d`) interactively, so the verb in the
        // palette is just the entry door.
        PaletteCommand::StepSetHook | PaletteCommand::StepUnsetHook => dispatch_step_hooks(ctx),

        // -- /step edit --tags --------------------------------------------
        // Pushes the step-tags sub-view (step 36). The sub-view owns add
        // (`i`) / remove (`d`) interactively, so the palette verb is just
        // the entry door.
        PaletteCommand::StepEditTags => dispatch_step_tags(ctx),

        // -- /cancel ------------------------------------------------------
        PaletteCommand::Cancel => PaletteAction::CancelRun,

        // -- /export <slug> [-o <path>] -----------------------------------
        PaletteCommand::Export { slug, output } => {
            dispatch_export(slug, output.as_deref(), ctx)
        }

        // -- /import <path> -----------------------------------------------
        PaletteCommand::Import(path) => PaletteAction::Import {
            path: path.clone(),
        },

        // -- /quit / /q ---------------------------------------------------
        PaletteCommand::Quit => PaletteAction::Quit,

        // -- /help (overlay lands in step 43) -----------------------------
        PaletteCommand::Help => PaletteAction::ComingSoon {
            label: cmd.label(),
            target_step: 43,
        },
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

fn dispatch_plan_questions(
    slug: Option<&str>,
    enabled: bool,
    ctx: &PaletteContext<'_>,
) -> PaletteAction {
    match resolve_slug(slug, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::SetQuestionsEnabled {
            plan_id: target.id,
            slug: target.slug,
            enabled,
        },
        ResolvedSlug::Missing => PaletteAction::Toast {
            message: "No plan selected.".to_string(),
            kind: ToastKind::Info,
        },
        ResolvedSlug::Unknown(name) => unknown_plan_toast(&name),
    }
}

fn dispatch_step_add(title: &str, ctx: &PaletteContext<'_>) -> PaletteAction {
    let title = title.trim();
    if title.is_empty() {
        return PaletteAction::Toast {
            message: "/step add requires a non-empty title.".to_string(),
            kind: ToastKind::Error,
        };
    }
    match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::AddStep {
            plan_id: target.id,
            slug: target.slug,
            title: title.to_string(),
        },
        // Plan-list and archived-list don't have a single open plan, but the
        // focus pointer is enough — every concrete view that hosts the
        // palette (plan-detail, step-detail) sets `focused_slug` to the open
        // plan. Without a focused plan we can't pick a target.
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => PaletteAction::Toast {
            message: "Open a plan first to add a step.".to_string(),
            kind: ToastKind::Info,
        },
    }
}

fn dispatch_step_skip(num: Option<u32>, ctx: &PaletteContext<'_>) -> PaletteAction {
    match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::SkipStep {
            plan_id: target.id,
            slug: target.slug,
            step_num: num,
        },
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => PaletteAction::Toast {
            message: "Open a plan first to skip a step.".to_string(),
            kind: ToastKind::Info,
        },
    }
}

fn dispatch_export(
    slug: &str,
    output: Option<&str>,
    ctx: &PaletteContext<'_>,
) -> PaletteAction {
    // /export looks up the slug against both active and archived plans.
    // Exporting an archived plan is intentional — the JSON snapshot is the
    // user's escape hatch before a `/plan delete`.
    if find_by_slug(ctx.plans, slug)
        .or_else(|| find_by_slug(ctx.archived, slug))
        .is_some()
    {
        PaletteAction::Export {
            slug: slug.to_string(),
            output: output.map(str::to_string),
        }
    } else {
        unknown_plan_toast(slug)
    }
}

fn dispatch_plan_dependencies(ctx: &PaletteContext<'_>) -> PaletteAction {
    // The sub-view always opens against the focused plan; the palette verb
    // doesn't take an explicit slug. Plan-list / archived-list don't have a
    // single "open" plan, but the focus pointer always identifies one when
    // the user is sitting on a plan tile.
    match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::OpenPlanDependencies {
            plan_id: target.id,
            slug: target.slug,
        },
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => PaletteAction::Toast {
            message: "Open a plan first to edit dependencies.".to_string(),
            kind: ToastKind::Info,
        },
    }
}

fn dispatch_plan_hooks(ctx: &PaletteContext<'_>) -> PaletteAction {
    // Same shape as `dispatch_plan_dependencies` — the sub-view is the verb;
    // the palette command merely opens it against the focused plan.
    match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::OpenPlanHooks {
            plan_id: target.id,
            slug: target.slug,
        },
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => PaletteAction::Toast {
            message: "Open a plan first to edit hooks.".to_string(),
            kind: ToastKind::Info,
        },
    }
}

fn dispatch_step_hooks(ctx: &PaletteContext<'_>) -> PaletteAction {
    // `/step set-hook|unset-hook` resolves against both `focused_slug` (the
    // parent plan) and `focused_step` (the highlighted step). Only step-detail
    // sets the latter today; other views collapse to a toast.
    let target = match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => target,
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => {
            return PaletteAction::Toast {
                message: "Open a step first to edit hooks.".to_string(),
                kind: ToastKind::Info,
            };
        }
    };
    let Some(step) = ctx.focused_step else {
        return PaletteAction::Toast {
            message: "Open a step first to edit hooks.".to_string(),
            kind: ToastKind::Info,
        };
    };
    PaletteAction::OpenStepHooks {
        plan_id: target.id,
        step_id: step.id.clone(),
        plan_slug: target.slug,
        step_label: step.label.clone(),
    }
}

fn dispatch_step_tags(ctx: &PaletteContext<'_>) -> PaletteAction {
    // Same shape as `dispatch_step_hooks` — `/step edit --tags` resolves
    // against both `focused_slug` (the parent plan, used only for the title
    // bar) and `focused_step` (the step whose tags are being edited).
    let target = match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => target,
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => {
            return PaletteAction::Toast {
                message: "Open a step first to edit tags.".to_string(),
                kind: ToastKind::Info,
            };
        }
    };
    let Some(step) = ctx.focused_step else {
        return PaletteAction::Toast {
            message: "Open a step first to edit tags.".to_string(),
            kind: ToastKind::Info,
        };
    };
    PaletteAction::OpenStepTags {
        step_id: step.id.clone(),
        plan_slug: target.slug,
        step_label: step.label.clone(),
    }
}

fn dispatch_step_move(from: u32, to: u32, ctx: &PaletteContext<'_>) -> PaletteAction {
    if from == 0 || to == 0 {
        return PaletteAction::Toast {
            message: "/step move: step numbers are 1-based.".to_string(),
            kind: ToastKind::Error,
        };
    }
    if from == to {
        return PaletteAction::Toast {
            message: format!("/step move: step {from} is already at position {to}."),
            kind: ToastKind::Info,
        };
    }
    match resolve_slug(None, ctx) {
        ResolvedSlug::Some(target) => PaletteAction::MoveStep {
            plan_id: target.id,
            slug: target.slug,
            from,
            to,
        },
        ResolvedSlug::Missing | ResolvedSlug::Unknown(_) => PaletteAction::Toast {
            message: "Open a plan first to move a step.".to_string(),
            kind: ToastKind::Info,
        },
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
        focused_step: Option<FocusedStep>,
        run_targets: Vec<RunTarget>,
        plans: Vec<PlanRef>,
        archived: Vec<PlanRef>,
    }

    impl Ctx {
        fn new() -> Self {
            Self {
                default_harness: "claude".to_string(),
                focused_slug: None,
                focused_step: None,
                run_targets: vec![],
                plans: vec![],
                archived: vec![],
            }
        }
        fn as_ctx(&self) -> PaletteContext<'_> {
            PaletteContext {
                default_harness: &self.default_harness,
                focused_slug: self.focused_slug.as_deref(),
                focused_step: self.focused_step.as_ref(),
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

    // -- /cancel ----------------------------------------------------------

    #[test]
    fn cancel_emits_cancelrun_unconditionally() {
        // The pure dispatcher doesn't know about live runs — the consuming
        // view checks for one and toasts otherwise. Verify the action shape
        // for both an empty context and one with a focused plan.
        let c = Ctx::new();
        assert_eq!(dispatch_str("/cancel", &c), PaletteAction::CancelRun);

        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::InProgress)];
        c.focused_slug = Some("alpha".to_string());
        assert_eq!(dispatch_str("/cancel", &c), PaletteAction::CancelRun);
    }

    // -- /quit ------------------------------------------------------------

    #[test]
    fn quit_emits_quit_action() {
        let c = Ctx::new();
        assert_eq!(dispatch_str("/quit", &c), PaletteAction::Quit);
        // Alias /q parses to the same command.
        assert_eq!(dispatch_str("/q", &c), PaletteAction::Quit);
    }

    // -- /help ------------------------------------------------------------

    #[test]
    fn help_routes_to_step_43() {
        // Help overlay itself lands in step 43; until then the dispatcher
        // returns ComingSoon so the loop can toast a placeholder.
        let c = Ctx::new();
        assert_eq!(
            dispatch_str("/help", &c),
            PaletteAction::ComingSoon {
                label: "/help",
                target_step: 43,
            }
        );
    }

    // -- /export ----------------------------------------------------------

    #[test]
    fn export_named_active_plan_emits_export() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/export alpha", &c);
        assert_eq!(
            action,
            PaletteAction::Export {
                slug: "alpha".to_string(),
                output: None,
            }
        );
    }

    #[test]
    fn export_with_explicit_output_path() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/export alpha -o /tmp/out.json", &c);
        assert_eq!(
            action,
            PaletteAction::Export {
                slug: "alpha".to_string(),
                output: Some("/tmp/out.json".to_string()),
            }
        );
    }

    #[test]
    fn export_resolves_archived_plan() {
        // Archived plans are exportable — their JSON snapshot is the user's
        // escape hatch before a /plan delete.
        let mut c = Ctx::new();
        c.archived = vec![plan_ref("frozen", PlanStatus::Archived)];
        let action = dispatch_str("/export frozen", &c);
        assert_eq!(
            action,
            PaletteAction::Export {
                slug: "frozen".to_string(),
                output: None,
            }
        );
    }

    #[test]
    fn export_unknown_slug_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/export ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    // -- /import ----------------------------------------------------------

    #[test]
    fn import_emits_import_action_with_path() {
        // The pure dispatcher just forwards the path. The consuming view
        // reads the file and prompts for a fresh slug on collision.
        let c = Ctx::new();
        let action = dispatch_str("/import /tmp/plan.json", &c);
        assert_eq!(
            action,
            PaletteAction::Import {
                path: "/tmp/plan.json".to_string(),
            }
        );
    }

    // -- /plan questions on|off -------------------------------------------

    #[test]
    fn plan_questions_on_named_flips_flag() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan questions on alpha", &c);
        assert_eq!(
            action,
            PaletteAction::SetQuestionsEnabled {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                enabled: true,
            }
        );
    }

    #[test]
    fn plan_questions_off_named_flips_flag() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        let action = dispatch_str("/plan questions off alpha", &c);
        assert_eq!(
            action,
            PaletteAction::SetQuestionsEnabled {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                enabled: false,
            }
        );
    }

    #[test]
    fn plan_questions_on_omitted_uses_focus() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/plan questions on", &c);
        assert_eq!(
            action,
            PaletteAction::SetQuestionsEnabled {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                enabled: true,
            }
        );
    }

    #[test]
    fn plan_questions_unknown_slug_toasts_error() {
        let c = Ctx::new();
        let action = dispatch_str("/plan questions on ghost", &c);
        assert_eq!(action, unknown_plan_toast("ghost"));
    }

    #[test]
    fn plan_questions_omitted_with_no_focus_toasts_info() {
        let c = Ctx::new();
        let action = dispatch_str("/plan questions off", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "No plan selected.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    // -- /step add --------------------------------------------------------

    #[test]
    fn step_add_with_focus_returns_addstep() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step add do the thing", &c);
        assert_eq!(
            action,
            PaletteAction::AddStep {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                title: "do the thing".to_string(),
            }
        );
    }

    #[test]
    fn step_add_without_focus_toasts_info() {
        let c = Ctx::new();
        let action = dispatch_str("/step add hello", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a plan first to add a step.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn step_add_with_whitespace_title_toasts_error() {
        // The parser preserves multi-word titles by re-joining tokens, so a
        // pure-whitespace title can't actually reach the dispatcher through
        // `parse()` (split_whitespace drops it). Build the command directly
        // to verify the dispatcher's defensive guard.
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let cmd = PaletteCommand::StepAdd("   ".to_string());
        let action = dispatch(&cmd, &c.as_ctx());
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "/step add requires a non-empty title.".to_string(),
                kind: ToastKind::Error,
            }
        );
    }

    // -- /step skip -------------------------------------------------------

    #[test]
    fn step_skip_with_focus_no_num_returns_skipstep_none() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::InProgress)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step skip", &c);
        assert_eq!(
            action,
            PaletteAction::SkipStep {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                step_num: None,
            }
        );
    }

    #[test]
    fn step_skip_with_explicit_num() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step skip 3", &c);
        assert_eq!(
            action,
            PaletteAction::SkipStep {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                step_num: Some(3),
            }
        );
    }

    #[test]
    fn step_skip_without_focus_toasts_info() {
        let c = Ctx::new();
        let action = dispatch_str("/step skip", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a plan first to skip a step.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    // -- /step move -------------------------------------------------------

    #[test]
    fn step_move_returns_movestep() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step move 3 --to 5", &c);
        assert_eq!(
            action,
            PaletteAction::MoveStep {
                plan_id: "id-alpha".to_string(),
                slug: "alpha".to_string(),
                from: 3,
                to: 5,
            }
        );
    }

    #[test]
    fn step_move_without_focus_toasts_info() {
        let c = Ctx::new();
        let action = dispatch_str("/step move 3 --to 5", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a plan first to move a step.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn step_move_same_position_toasts_info() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step move 3 --to 3", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "/step move: step 3 is already at position 3.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn step_move_zero_indexed_args_rejected() {
        // Defensive: parser accepts 0 as a u32, but step numbers are 1-based.
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch(
            &PaletteCommand::StepMove { num: 0, to: 5 },
            &c.as_ctx(),
        );
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "/step move: step numbers are 1-based.".to_string(),
                kind: ToastKind::Error,
            }
        );
    }

    // -- /plan dependency routes to OpenPlanDependencies (step 33) --------

    #[test]
    fn plan_dependency_subcommands_open_sub_view_with_focused_plan() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        for input in [
            "/plan dependency add",
            "/plan dependency remove",
            "/plan dependency list",
        ] {
            match dispatch_str(input, &c) {
                PaletteAction::OpenPlanDependencies { plan_id, slug } => {
                    assert_eq!(plan_id, "id-alpha");
                    assert_eq!(slug, "alpha");
                }
                other => panic!("expected OpenPlanDependencies for {input}, got {other:?}"),
            }
        }
    }

    #[test]
    fn plan_dependency_subcommands_toast_when_no_focused_plan() {
        let c = Ctx::new();
        let action = dispatch_str("/plan dependency add", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a plan first to edit dependencies.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    // -- /plan set-hook|unset-hook|hooks routes to OpenPlanHooks (step 34) --

    #[test]
    fn plan_hook_subcommands_route_to_open_plan_hooks() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        for input in ["/plan set-hook", "/plan unset-hook", "/plan hooks"] {
            match dispatch_str(input, &c) {
                PaletteAction::OpenPlanHooks { plan_id, slug } => {
                    assert_eq!(plan_id, "id-alpha");
                    assert_eq!(slug, "alpha");
                }
                other => panic!("expected OpenPlanHooks for {input}, got {other:?}"),
            }
        }
    }

    #[test]
    fn plan_hook_subcommands_toast_when_no_focused_plan() {
        let c = Ctx::new();
        let action = dispatch_str("/plan set-hook", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a plan first to edit hooks.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    // -- /step set-hook|unset-hook routes to OpenStepHooks (step 35) -----

    #[test]
    fn step_hook_subcommands_route_to_open_step_hooks() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        c.focused_step = Some(FocusedStep {
            id: "step-1".to_string(),
            label: "#3 — Build the thing".to_string(),
        });
        for input in ["/step set-hook", "/step unset-hook"] {
            match dispatch_str(input, &c) {
                PaletteAction::OpenStepHooks {
                    plan_id,
                    step_id,
                    plan_slug,
                    step_label,
                } => {
                    assert_eq!(plan_id, "id-alpha");
                    assert_eq!(step_id, "step-1");
                    assert_eq!(plan_slug, "alpha");
                    assert_eq!(step_label, "#3 — Build the thing");
                }
                other => panic!("expected OpenStepHooks for {input}, got {other:?}"),
            }
        }
    }

    #[test]
    fn step_hook_subcommands_toast_when_no_focused_step() {
        // Plan focused but no step focused (e.g. invoked from plan-detail).
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step set-hook", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a step first to edit hooks.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn step_hook_subcommands_toast_when_no_focused_plan() {
        // Step focused without a plan focus is a programming error in the
        // caller, but the dispatcher still falls back to a toast rather
        // than panicking.
        let mut c = Ctx::new();
        c.focused_step = Some(FocusedStep {
            id: "step-1".to_string(),
            label: "#1 — Step".to_string(),
        });
        let action = dispatch_str("/step set-hook", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a step first to edit hooks.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    // -- /step edit --tags routes to OpenStepTags (step 36) --------------

    #[test]
    fn step_edit_tags_routes_to_open_step_tags() {
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        c.focused_step = Some(FocusedStep {
            id: "step-1".to_string(),
            label: "#3 — Build the thing".to_string(),
        });
        match dispatch_str("/step edit --tags", &c) {
            PaletteAction::OpenStepTags {
                step_id,
                plan_slug,
                step_label,
            } => {
                assert_eq!(step_id, "step-1");
                assert_eq!(plan_slug, "alpha");
                assert_eq!(step_label, "#3 — Build the thing");
            }
            other => panic!("expected OpenStepTags, got {other:?}"),
        }
    }

    #[test]
    fn step_edit_tags_toasts_when_no_focused_step() {
        // Plan focused but no step focused (e.g. invoked from plan-detail).
        let mut c = Ctx::new();
        c.plans = vec![plan_ref("alpha", PlanStatus::Ready)];
        c.focused_slug = Some("alpha".to_string());
        let action = dispatch_str("/step edit --tags", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a step first to edit tags.".to_string(),
                kind: ToastKind::Info,
            }
        );
    }

    #[test]
    fn step_edit_tags_toasts_when_no_focused_plan() {
        // Step focused without a plan focus is a programming error in the
        // caller, but the dispatcher still falls back to a toast rather
        // than panicking.
        let mut c = Ctx::new();
        c.focused_step = Some(FocusedStep {
            id: "step-1".to_string(),
            label: "#1 — Step".to_string(),
        });
        let action = dispatch_str("/step edit --tags", &c);
        assert_eq!(
            action,
            PaletteAction::Toast {
                message: "Open a step first to edit tags.".to_string(),
                kind: ToastKind::Info,
            }
        );
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
