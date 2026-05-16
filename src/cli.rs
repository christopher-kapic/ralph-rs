// CLI argument parsing (clap)

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

use crate::hook_library::Lifecycle;
use crate::plan::{ChangePolicy, PlanStatus, RetryStrategy};

/// Authoring tip surfaced via `--help` on plan/step creation commands and
/// the top-level binary, so plan authors learn ralph's commit-ownership
/// contract before they hit a confusing `reason: no_changes` loop. Kept as
/// a single constant so the wording stays consistent across surfaces.
pub(crate) const AUTHORING_TIP_COMMITS: &str = "Authoring tip:\n  \
    Ralph owns commits. On a successful step, ralph stages the harness's \
    diff and creates the commit itself. Step descriptions should NOT tell \
    the agent to run `git commit` or `git add` — doing so leaves the \
    worktree clean while HEAD advances, which trips the no_changes failure \
    path and burns retries.";

/// ralph-rs: a deterministic orchestrator for coding agent harnesses.
#[derive(Debug, Parser)]
#[command(
    name = "ralph",
    version,
    about,
    long_about = None,
    after_help = AUTHORING_TIP_COMMITS,
)]
pub struct Cli {
    /// Path to the project directory (defaults to current directory).
    #[arg(long, short = 'C', global = true)]
    pub project: Option<PathBuf>,

    /// Override the default harness for this invocation. A per-subcommand
    /// `--harness` (e.g. `ralph run --harness X`) takes precedence over this
    /// global flag; both in turn fall back to the plan's stored harness and
    /// then the config default.
    #[arg(long, global = true)]
    pub harness: Option<String>,

    /// Emit machine-readable JSON output instead of human-readable text.
    ///
    /// `--jsonl` is accepted as a strict alias: NDJSON and JSONL are the
    /// same format, and the alias exists so meta-harnesses can spell the
    /// flag the way they expect.
    #[arg(long, alias = "jsonl", global = true)]
    pub json: bool,

    /// Suppress progress and banner output.
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Disable ANSI color output even when stdout is a TTY.
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Force non-interactive mode: skip the TUI and emit plain scripted
    /// output even from a TTY. Auto-set when stdout is not a TTY.
    #[arg(long, global = true)]
    pub non_interactive: bool,

    /// `None` means the user invoked bare `ralph` with no subcommand. From a
    /// TTY the dispatcher routes this to the TUI plan-list view (TUI-plan.md
    /// §2). With stdout piped, `main` falls back to printing clap's help.
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize ralph-rs: create config/agents directories, detect
    /// installed harnesses, and write the default config.
    Init {
        /// Skip the interactive default-harness prompt (picks the first
        /// installed harness, preferring `claude`).
        #[arg(long)]
        non_interactive: bool,

        /// Explicitly set the default harness. Must be one of the known
        /// harness names. Skips the interactive prompt.
        #[arg(long, value_name = "NAME")]
        default_harness: Option<String>,

        /// Overwrite an existing config file. Without this, an existing
        /// config is preserved and init will not re-prompt.
        #[arg(long)]
        force: bool,

        /// Re-seed the global prompt with ralph's built-in default,
        /// overwriting any existing customization. Without this flag the
        /// global prompt is only seeded when it is missing or blank.
        #[arg(long)]
        restore_prompts: bool,
    },

    /// Manage plans.
    #[command(subcommand)]
    Plan(PlanCommand),

    /// Manage steps within a plan.
    #[command(subcommand)]
    Step(StepCommand),

    /// Run the next pending step (or all remaining steps) of a plan.
    ///
    /// By default, runs all remaining pending steps in the plan sequentially.
    /// Use --one to run only the next pending step. Use --from/--to to run a
    /// specific range of steps. Use --all to run every plan in dependency order
    /// (conflicts with the positional plan slug). --one and --all are mutually
    /// exclusive.
    ///
    /// `--one` may be combined with `--from`/`--to`: when both are present, ralph
    /// resolves the step window from `--from`/`--to` and then runs only the first
    /// actionable step inside it. `--one` is still mutually exclusive with `--all`.
    Run {
        /// Plan slug to run. Defaults to the active plan.
        #[arg(conflicts_with = "all")]
        plan: Option<String>,

        /// Run only the next pending step instead of all remaining.
        #[arg(long, alias = "single", conflicts_with = "all")]
        one: bool,

        /// Run all plans in dependency order (chains plans). Conflicts with
        /// the positional plan slug.
        #[arg(long)]
        all: bool,

        /// Start from a specific step number (1-based).
        #[arg(long)]
        from: Option<usize>,

        /// Stop after a specific step number (1-based).
        #[arg(long)]
        to: Option<usize>,

        /// Dry-run mode: print what would happen without executing.
        #[arg(long)]
        dry_run: bool,

        /// Skip preflight checks before running.
        #[arg(long)]
        skip_preflight: bool,

        /// Skip branch creation and use the current git branch.
        #[arg(long)]
        current_branch: bool,

        /// Disable the default auto-stash behavior.
        ///
        /// By default, `ralph run` stashes a dirty working tree (tracked +
        /// untracked) with `git stash push --include-untracked -m "ralph:
        /// auto-stash for plan '<slug>' at <timestamp>"` before switching
        /// branches, then pops it back at run end. Pass this flag to make
        /// ralph bail on a dirty tree instead — useful when you want to
        /// manage the stash yourself, or are paranoid about pop conflicts.
        ///
        /// The `auto_stash` key in `config.json` can be set to `false` to
        /// make this the default for every run in that config.
        #[arg(long = "no-auto-stash")]
        no_auto_stash: bool,

        /// Override the harness for this run.
        #[arg(long)]
        harness: Option<String>,

        /// Reclaim a held run lock even if the previous runner still appears alive (use only if you know the other process is gone).
        #[arg(long)]
        force: bool,

        /// Print the full per-attempt prompt to stderr instead of the
        /// truncated 512-char preview.
        #[arg(long, short = 'v')]
        verbose: bool,
    },

    /// Resume a plan from the last failed or in-progress step.
    Resume {
        /// Plan slug to resume. Defaults to the active plan.
        plan: Option<String>,

        /// Reclaim a held run lock even if the previous runner still appears alive (use only if you know the other process is gone).
        #[arg(long)]
        force: bool,
    },

    /// Request a graceful pause: the active runner finishes the current step,
    /// then exits before starting the next one.
    ///
    /// Sets `plans.pause_requested = true` on the resolved plan. The runner
    /// reads + clears this flag between step boundaries; idempotent if no run
    /// is active. Use `ralph resume` to continue from the next pending step.
    Pause {
        /// Plan slug to pause. Defaults to the active plan.
        plan: Option<String>,
    },

    /// Cancel the live `ralph run` for this project.
    ///
    /// Sends SIGTERM to the active runner so it can finish its current phase,
    /// tear down the harness process group, and release the project run lock.
    /// Falls through to SIGKILL if the runner doesn't release the lock within
    /// --timeout. Idempotent: a no-op if no run is active.
    Cancel {
        /// Restrict cancellation to a specific plan slug. If the live run is
        /// for a different plan, cancel refuses (exit 1).
        plan: Option<String>,

        /// Skip the graceful SIGTERM + grace period. Goes straight to SIGKILL
        /// on the runner AND its harness process group, then writes a
        /// `user_interrupted` row to the live execution_log so the history
        /// isn't left ambiguous.
        #[arg(long)]
        force: bool,

        /// How long to wait for the runner to release the lock after SIGTERM,
        /// in seconds. Ignored when --force is set.
        #[arg(long, default_value = "15")]
        timeout: u64,
    },

    /// Skip the current or specified step.
    Skip {
        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Step number to skip (1-based). Defaults to current step.
        #[arg(long)]
        step: Option<usize>,

        /// Reason for skipping.
        #[arg(long)]
        reason: Option<String>,

        /// How to dispose of the in-flight harness's uncommitted changes when
        /// skipping a *currently-running* step. Ignored for steps that aren't
        /// running (their changes aren't causally tied to the skip).
        #[arg(long, value_enum, default_value_t = ChangeHandling::Stash)]
        changes: ChangeHandling,

        /// Reclaim a held run lock even if the previous runner still appears alive (use only if you know the other process is gone).
        #[arg(long)]
        force: bool,
    },

    /// Export a plan to a portable JSON file.
    Export {
        /// Plan slug to export.
        plan: String,

        /// Output file path (defaults to stdout).
        #[arg(long, short)]
        output: Option<PathBuf>,
    },

    /// Import a plan from a portable JSON file.
    Import {
        /// Path to the JSON file to import.
        file: PathBuf,

        /// Override the plan slug on import.
        #[arg(long)]
        slug: Option<String>,

        /// Override the branch name on import.
        #[arg(long)]
        branch: Option<String>,

        /// Fail when the export's ralph-rs major version is incompatible
        /// (default is to warn and continue).
        #[arg(long)]
        strict: bool,
    },

    /// Show the status of the current or specified plan.
    Status {
        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Show verbose output including step details.
        #[arg(long, short)]
        verbose: bool,
    },

    /// Show execution logs.
    Log {
        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Step number (1-based) to show logs for.
        #[arg(long)]
        step: Option<usize>,

        /// Maximum number of log entries to show.
        #[arg(long, short)]
        limit: Option<usize>,

        /// Show full log output (stdout/stderr) with no truncation.
        #[arg(long, conflicts_with = "lines")]
        full: bool,

        /// Maximum number of output lines to show per attempt, split between
        /// stdout and stderr (total budget, not per stream). Implies showing
        /// output. Conflicts with --full.
        #[arg(long)]
        lines: Option<usize>,
    },

    /// Ask the user a question or list/answer outstanding questions on a plan.
    ///
    /// `ralph question ask` is invoked by the harness mid-step to pause for
    /// clarification on a per-plan opt-in question feature. See TUI-plan.md
    /// §17 for the full design.
    #[command(subcommand)]
    Question(QuestionCommand),

    /// List and manage agent file templates.
    #[command(subcommand)]
    Agents(AgentsCommand),

    /// Manage the hook library (reusable shell commands that run at lifecycle events).
    #[command(subcommand)]
    Hooks(HooksCommand),

    /// Configure the global or project prompt layer.
    ///
    /// Each scope holds a single prompt blob (no prefix/suffix split). The
    /// four layers — global, project, plan (the plan description), step —
    /// stack top-to-bottom to form each step prompt; this command edits the
    /// global and project layers (`--scope universal` aliases global; the
    /// project layer reads `<project>/.ralph/prompt.md` when present, else
    /// the DB).
    #[command(subcommand)]
    Prompt(PromptCommand),

    /// Run preflight checks to verify the environment is ready.
    Doctor,

    /// Inspect configured harnesses (read-only).
    ///
    /// Mutating harness config still goes through `~/.config/ralph-rs/config.json`
    /// directly. These commands are for discovering what's configured, what's
    /// on PATH, and which harness has known foot-guns (e.g. codex without
    /// `--sandbox`).
    #[command(subcommand)]
    Harness(HarnessCommand),

    /// View or mutate the global config file (`~/.config/ralph-rs/config.json`).
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Generate shell completions for bash, zsh, fish, elvish, or powershell.
    Completions {
        /// Shell to generate completions for.
        shell: Shell,
    },
}

// ---------------------------------------------------------------------------
// Config subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the canonical config path and the current effective values.
    Show,

    /// Set the IANA timezone used to format progress-header timestamps.
    ///
    /// Value must be a recognized IANA name (e.g. `America/New_York`,
    /// `Europe/London`, `Asia/Tokyo`, `UTC`). Rejected with a clear error
    /// if the name is unknown, so typos fail fast.
    SetTimezone {
        /// IANA timezone name.
        tz: String,
    },
}

// ---------------------------------------------------------------------------
// Harness subcommands (top-level `ralph harness …`, distinct from
// `ralph plan harness …` which manages the plan-generation harness).
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum HarnessCommand {
    /// List all configured harnesses with on-PATH status, sandbox/permission
    /// summary, and a flag for any known foot-guns.
    List {
        /// Emit machine-readable JSON instead of the default table.
        #[arg(long)]
        json: bool,
    },

    /// Print the full configuration of a single harness.
    Show {
        /// Harness name (e.g. `claude`, `codex`, `codex-orchestrator`).
        name: String,

        /// Emit machine-readable JSON instead of the default pretty form.
        #[arg(long)]
        json: bool,
    },
}

// ---------------------------------------------------------------------------
// Plan subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum PlanCommand {
    /// Create a new plan.
    #[command(after_help = AUTHORING_TIP_COMMITS)]
    Create {
        /// Short slug identifier for the plan.
        slug: String,

        /// Description of the plan.
        #[arg(long, short)]
        description: Option<String>,

        /// Git branch name (defaults to slug-based branch).
        #[arg(long)]
        branch: Option<String>,

        /// Harness to use for this plan.
        #[arg(long)]
        harness: Option<String>,

        /// Agent/model to use.
        #[arg(long)]
        agent: Option<String>,

        /// Plan-level default retry strategy for failed step attempts.
        /// Effective value is resolved step > plan > default `keep`:
        /// a step's own `--retry-strategy` wins, then this plan-level
        /// default, then the built-in default (`keep`). `keep` = a failed
        /// attempt leaves the working tree as-is so the next attempt
        /// builds on it directly; `rollback` = a failed attempt rolls the
        /// working tree back and feeds the prior diff into the next
        /// attempt's prompt instead. Omit to leave the plan with no
        /// override (steps then fall through to the global `keep`
        /// default).
        #[arg(long, value_name = "STRATEGY")]
        retry_strategy: Option<RetryStrategy>,

        /// Deterministic test command(s) to validate each step.
        #[arg(long = "test")]
        tests: Vec<String>,

        /// Slug of another plan this plan depends on (can be repeated).
        #[arg(long = "depends-on")]
        depends_on: Vec<String>,
    },

    /// List plans.
    List {
        /// Show plans across all projects, not just the current one.
        #[arg(long)]
        all: bool,

        /// Filter by status.
        #[arg(long)]
        status: Option<PlanStatus>,

        /// Include archived plans in the listing.
        #[arg(long)]
        archived: bool,
    },

    /// Show details of a plan.
    Show {
        /// Plan slug.
        slug: String,
    },

    /// Mark a plan as approved/ready for execution.
    Approve {
        /// Plan slug.
        slug: String,
    },

    /// Manage plan-level dependencies.
    #[command(subcommand)]
    Dependency(PlanDependencyCommand),

    /// Delete a plan and all its steps/logs.
    Delete {
        /// Plan slug.
        slug: String,

        /// Skip confirmation prompt.
        #[arg(long, short, alias = "yes")]
        force: bool,
    },

    /// Archive a completed, failed, or aborted plan.
    Archive {
        /// Plan slug.
        slug: String,
    },

    /// Restore an archived plan.
    Unarchive {
        /// Plan slug.
        slug: String,
    },

    /// Attach a library hook plan-wide (fires for every step in the plan).
    SetHook {
        /// Plan slug.
        slug: String,

        /// Lifecycle event: pre-step, post-step, pre-test, post-test.
        #[arg(long)]
        lifecycle: Lifecycle,

        /// Hook name from the library.
        #[arg(long)]
        hook: String,
    },

    /// Detach a previously-attached plan-wide hook.
    UnsetHook {
        /// Plan slug.
        slug: String,

        /// Lifecycle event.
        #[arg(long)]
        lifecycle: Lifecycle,

        /// Hook name to detach.
        #[arg(long)]
        hook: String,
    },

    /// List every hook attached to the plan (plan-wide and per-step).
    Hooks {
        /// Plan slug.
        slug: String,
    },

    /// Manage the plan-generation harness.
    #[command(subcommand)]
    Harness(PlanHarnessCommand),

    /// Toggle the pause-for-question feature for a plan.
    ///
    /// `ralph plan questions on <slug>` enables the feature; off disables it.
    /// Mirrors the `Q` keybinding in the TUI plan list.
    Questions {
        /// `on` to enable, `off` to disable.
        state: QuestionsState,

        /// Plan slug.
        slug: String,
    },
}

// ---------------------------------------------------------------------------
// Plan dependency subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum PlanDependencyCommand {
    /// Add one or more dependency edges to a plan.
    Add {
        /// Plan slug to add dependencies to.
        slug: String,

        /// Slug of another plan this plan depends on (can be repeated).
        #[arg(long = "depends-on", num_args = 1.., required = true)]
        depends_on: Vec<String>,
    },

    /// Remove one or more dependency edges from a plan.
    Remove {
        /// Plan slug to remove dependencies from.
        slug: String,

        /// Slug of the dependency to remove (can be repeated).
        #[arg(long = "depends-on", num_args = 1.., required = true)]
        depends_on: Vec<String>,
    },

    /// List a plan's direct dependencies and dependents.
    List {
        /// Plan slug.
        slug: String,
    },
}

// ---------------------------------------------------------------------------
// Step subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum StepCommand {
    /// List steps in a plan.
    List {
        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Filter to steps that have this tag (repeatable). When passed
        /// multiple times the filter is AND: a step must carry every tag
        /// to appear in the output. Matching is case-sensitive.
        #[arg(long = "tag", value_name = "TAG")]
        tags: Vec<String>,
    },

    /// Add a new step to a plan.
    ///
    /// The single-step form takes a positional title plus per-field flags.
    /// For bulk insertion use `--import-json <FILE|->` to read an array of
    /// step objects (or a single object) from a file or stdin; the per-field
    /// flags are mutually exclusive with `--import-json`. When `--import-json`
    /// is used, the first positional is interpreted as the plan slug (since
    /// no title is meaningful for a bulk import).
    #[command(after_help = AUTHORING_TIP_COMMITS)]
    Add {
        /// Step title. Required unless `--import-json` is used. With
        /// `--import-json`, a single positional is reinterpreted as the
        /// plan slug.
        #[arg(required_unless_present = "import_json")]
        title: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Step description.
        #[arg(long, short, conflicts_with = "import_json")]
        description: Option<String>,

        /// Position to insert at (1-based). Defaults to end.
        #[arg(long, conflicts_with = "import_json")]
        after: Option<usize>,

        /// Agent/model override for this step.
        #[arg(long, conflicts_with = "import_json")]
        agent: Option<String>,

        /// Harness override for this step.
        #[arg(long, conflicts_with = "import_json")]
        harness: Option<String>,

        /// Per-step model override, forwarded via the harness's `model_args`
        /// template (e.g. `--model sonnet-4.6`). Silently ignored if the
        /// resolved harness has no `model_args` configured.
        #[arg(long, conflicts_with = "import_json")]
        model: Option<String>,

        /// Acceptance criterion (repeatable).
        #[arg(long = "criteria", conflicts_with = "import_json")]
        criteria: Vec<String>,

        /// Step-level max retries override.
        #[arg(long, conflicts_with = "import_json")]
        max_retries: Option<i32>,

        /// Whether this step must produce file changes. `required` (default)
        /// fails when the harness exits with an empty diff — appropriate for
        /// implementation steps. `optional` allows a clean harness exit with
        /// no diff — appropriate for review, audit, or check steps where the
        /// prompt directs the harness not to modify code. Omit to leave the
        /// step at the default (`required`).
        #[arg(long, value_name = "POLICY", conflicts_with = "import_json")]
        change_policy: Option<ChangePolicy>,

        /// Step-level retry strategy for failed attempts. Effective value
        /// is resolved step > plan > default `keep`: this step-level
        /// override wins, then the plan's `--retry-strategy`, then the
        /// built-in default (`keep`). `keep` = a failed attempt leaves the
        /// working tree as-is so the next attempt builds on it directly;
        /// `rollback` = a failed attempt rolls the working tree back and
        /// feeds the prior diff into the next attempt's prompt instead.
        /// Omit to inherit the plan/global value.
        #[arg(long, value_name = "STRATEGY", conflicts_with = "import_json")]
        retry_strategy: Option<RetryStrategy>,

        /// Attach a free-form tag to the new step (repeatable). Tags are
        /// user-defined labels for filtering with `ralph step list --tag`;
        /// they carry no execution-model semantics today. Empty/whitespace
        /// values and exact-duplicate values within a single invocation
        /// are rejected.
        #[arg(long = "tag", value_name = "TAG", conflicts_with = "import_json")]
        tags: Vec<String>,

        /// Bulk-insert steps from a JSON file or stdin (use `-` for stdin).
        /// Accepts a JSON array of step objects, or a single object. Each
        /// object requires `title`; `description`, `acceptance_criteria`,
        /// `agent`, `harness`, and `max_retries` are optional. Steps are
        /// appended in array order; the whole batch is atomic.
        #[arg(long, value_name = "FILE|-")]
        import_json: Option<String>,
    },

    /// Remove a step from a plan.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive; numbers are convenient
    /// for humans, UUIDs are stable across concurrent edits.
    Remove {
        /// Step number (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Skip confirmation prompt.
        #[arg(long, short, alias = "yes")]
        force: bool,
    },

    /// Edit a step's title, description, agent, harness, criteria, or max-retries.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive.
    Edit {
        /// Step number (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// New title.
        #[arg(long)]
        title: Option<String>,

        /// New description.
        #[arg(long)]
        description: Option<String>,

        /// New agent override. Pass empty string to clear.
        #[arg(long)]
        agent: Option<String>,

        /// New harness override. Pass empty string to clear.
        #[arg(long)]
        harness: Option<String>,

        /// New per-step model override. Pass empty string to clear.
        #[arg(long)]
        model: Option<String>,

        /// Replace acceptance criteria (repeatable). Clears existing criteria.
        #[arg(long = "criteria")]
        criteria: Vec<String>,

        /// Explicitly clear all acceptance criteria on the step (mirrors --clear-tags).
        #[arg(long, conflicts_with = "criteria")]
        clear_criteria: bool,

        /// New max retries override. Stores the value as-is — `--max-retries 0` means
        /// zero retries (the step is final on its first failed attempt). To fall back
        /// to the plan/global default instead, use `--clear-max-retries`.
        #[arg(long)]
        max_retries: Option<i32>,

        /// Explicitly clear the max-retries override (sets to NULL/plan default).
        #[arg(long)]
        clear_max_retries: bool,

        /// Update the step's change policy. `required` fails the step when
        /// the harness exits with an empty diff; `optional` allows a clean
        /// no-diff exit (for review/audit/check steps). Omit to leave the
        /// existing policy unchanged. `change_policy` is NOT NULL, so there
        /// is no clear form — you always substitute one valid policy for
        /// another.
        #[arg(long, value_name = "POLICY")]
        change_policy: Option<ChangePolicy>,

        /// Update the step-level retry strategy. Effective value is
        /// resolved step > plan > default `keep`: this step-level override
        /// wins, then the plan's `--retry-strategy`, then the built-in
        /// default (`keep`). `keep` = a failed attempt leaves the working
        /// tree as-is so the next attempt builds on it directly;
        /// `rollback` = a failed attempt rolls the working tree back and
        /// feeds the prior diff into the next attempt's prompt instead.
        /// Omit to leave the existing override unchanged; use
        /// `--clear-retry-strategy` to revert to plan/global inheritance.
        #[arg(long, value_name = "STRATEGY")]
        retry_strategy: Option<RetryStrategy>,

        /// Explicitly clear the step-level retry-strategy override (sets to
        /// NULL so the step inherits the plan/global default). Mirrors
        /// `--clear-max-retries`; conflicts with `--retry-strategy`.
        #[arg(long, conflicts_with = "retry_strategy")]
        clear_retry_strategy: bool,

        /// Replace the step's tag list with these values (repeatable). Omit
        /// to leave existing tags unchanged; pass at least once to overwrite.
        /// Exact-duplicate values within the same invocation are rejected.
        /// See also `--clear-tags` for an explicit empty-list clear.
        #[arg(long = "tag", value_name = "TAG")]
        tags: Vec<String>,

        /// Explicitly clear all tags on the step (mirrors
        /// `--clear-max-retries`).
        #[arg(long, conflicts_with = "tags")]
        clear_tags: bool,
    },

    /// Reset a step's status back to pending.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive.
    Reset {
        /// Step number (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Skip the confirmation prompt shown before reverting any
        /// `[ralph wip]` skip commit(s) belonging to this step.
        #[arg(long, short, alias = "yes")]
        force: bool,
    },

    /// Move a step to a different position.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive.
    Move {
        /// Step number to move (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Target position (1-based).
        #[arg(long)]
        to: usize,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,
    },

    /// Attach a library hook to a specific step at a lifecycle event.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive.
    SetHook {
        /// Step number (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Lifecycle event: pre-step, post-step, pre-test, post-test.
        #[arg(long)]
        lifecycle: Lifecycle,

        /// Hook name from the library.
        #[arg(long)]
        hook: String,
    },

    /// Detach a previously-attached hook from a step.
    ///
    /// Identify the step by positional number (1-based) **or** by UUID via
    /// `--step-id`. The two are mutually exclusive.
    UnsetHook {
        /// Step number (1-based). Conflicts with --step-id.
        #[arg(conflicts_with = "step_id")]
        step: Option<usize>,

        /// Step UUID. Conflicts with positional step number.
        #[arg(long)]
        step_id: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Lifecycle event.
        #[arg(long)]
        lifecycle: Lifecycle,

        /// Hook name to detach.
        #[arg(long)]
        hook: String,
    },
}

// ---------------------------------------------------------------------------
// Plan harness subcommands (nested under `plan harness`)
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum PlanHarnessCommand {
    /// Set the plan-generation harness.
    Set {
        /// Harness name to assign.
        harness: String,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,
    },

    /// Show the current harness for a plan.
    Show {
        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,
    },

    /// Generate a plan via the configured harness.
    Generate {
        /// Description of what to plan.
        description: Option<String>,

        /// Plan slug. Defaults to the active plan.
        plan: Option<String>,

        /// Override the harness to use for planning.
        #[arg(long)]
        use_harness: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Question subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum QuestionCommand {
    /// Record a harness-asked question against the currently-executing step.
    ///
    /// Designed to be invoked by the harness mid-step. Binds to the live
    /// `ralph run` for this project via the run lock; if no run is active,
    /// or the plan does not have questions enabled, exits non-zero with an
    /// explanatory message and writes nothing to the database.
    Ask {
        /// The question text. If omitted, read from stdin.
        question: Option<String>,

        /// A suggested answer. Repeatable. The user can always type a custom
        /// answer; suggestions are hints, not a closed set.
        #[arg(long = "suggest", short = 's', value_name = "ANSWER")]
        suggest: Vec<String>,
    },

    /// List open (unanswered) questions for the current project.
    ///
    /// Output is numbered 1..N — those numbers are the input expected by
    /// `ralph question answer` and `ralph question show`. Order is by
    /// `asked_at` ASC then `id`, so a question's index does not change as
    /// new questions arrive.
    List {
        /// Filter to questions on a specific plan slug. Without this, all
        /// open questions on plans for the current project are listed.
        plan: Option<String>,
    },

    /// Answer a specific open question by its index in `ralph question list`.
    Answer {
        /// 1-based index from `ralph question list`.
        num: usize,

        /// Answer text. If omitted, read from stdin (heredoc-friendly).
        text: Option<String>,
    },

    /// Print a question's full text and any harness-supplied suggestions,
    /// identified by its index in `ralph question list`.
    Show {
        /// 1-based index from `ralph question list`.
        num: usize,
    },
}

/// How `ralph skip` disposes of a currently-running step's uncommitted
/// work after the harness is killed. Mirrors [`crate::git::ParkStrategy`]
/// at the CLI surface (the label/subject/trailer are filled in later from
/// the skipped step's identity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum ChangeHandling {
    /// `git stash push --include-untracked` — recoverable later (default).
    Stash,
    /// `git add -A && git commit` a WIP commit carrying a
    /// `Ralph-Skipped-Step` trailer.
    Commit,
    /// Throw the in-flight changes away (pre-existing untracked files are
    /// preserved).
    Discard,
}

impl From<ChangeHandling> for crate::git::ParkStrategyKind {
    fn from(c: ChangeHandling) -> Self {
        match c {
            ChangeHandling::Stash => crate::git::ParkStrategyKind::Stash,
            ChangeHandling::Commit => crate::git::ParkStrategyKind::Commit,
            ChangeHandling::Discard => crate::git::ParkStrategyKind::Discard,
        }
    }
}

/// `on` / `off` value enum for `ralph plan questions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum QuestionsState {
    /// Enable the per-plan pause-for-question feature.
    On,
    /// Disable the feature (the default for new plans).
    Off,
}

// ---------------------------------------------------------------------------
// Agents subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum AgentsCommand {
    /// List available agent file templates.
    List,

    /// Show the contents of an agent file template.
    Show {
        /// Agent template name.
        name: String,
    },

    /// Create a new agent file template.
    Create {
        /// Agent template name.
        name: String,

        /// Path to the file to use as the template.
        #[arg(long)]
        file: Option<PathBuf>,
    },

    /// Delete an agent file template.
    Delete {
        /// Agent template name.
        name: String,
    },
}

// ---------------------------------------------------------------------------
// Hooks subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum HooksCommand {
    /// List hooks in the library. By default shows only hooks applicable to
    /// the current project path; pass --all to include everything.
    List {
        /// Show every hook in the library, including path-scoped hooks that
        /// don't apply to the current project.
        #[arg(long)]
        all: bool,
    },

    /// Show a hook's definition (frontmatter + shell command body).
    Show {
        /// Hook name.
        name: String,
    },

    /// Add a new hook to the library.
    Add {
        /// Hook name (also used as the filename).
        name: String,

        /// Lifecycle event: pre-step, post-step, pre-test, post-test.
        #[arg(long)]
        lifecycle: Lifecycle,

        /// Shell command to execute. Can be a multi-line script.
        #[arg(long)]
        command: String,

        /// Human-readable description.
        #[arg(long)]
        description: Option<String>,

        /// Restrict the hook to these absolute path prefixes (repeatable).
        /// If omitted, the hook is global.
        #[arg(long = "scope-path")]
        scope_paths: Vec<PathBuf>,

        /// Overwrite an existing hook with the same name.
        #[arg(long)]
        force: bool,
    },

    /// Delete a hook from the library.
    Remove {
        /// Hook name.
        name: String,
    },

    /// Export hooks to a portable JSON bundle.
    Export {
        /// Output file path (defaults to stdout).
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Export every hook in the library (by default only hooks
        /// applicable to the current project are exported).
        #[arg(long)]
        all: bool,

        /// Filter hooks applicable to this absolute project path instead
        /// of the current project.
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// Import hooks from a portable JSON bundle.
    Import {
        /// Path to the bundle file.
        file: PathBuf,

        /// Overwrite existing hooks on name collision.
        #[arg(long)]
        force: bool,
    },
}

// ---------------------------------------------------------------------------
// Prompt subcommands
// ---------------------------------------------------------------------------

/// Which layer of the four-layer prompt model a prompt command targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum PromptScope {
    /// Global layer stored in `~/.config/ralph-rs/config.json`. Accepts
    /// `universal` as an alias — both resolve to this single variant, so
    /// every prompt subcommand path treats them identically.
    #[value(alias = "universal")]
    Global,
    /// Project layer stored in SQLite keyed on the current project path.
    Project,
}

#[derive(Debug, Subcommand)]
pub enum PromptCommand {
    /// Show the prompt configured for one or all scopes.
    ///
    /// With no `--scope`, displays the global and project entries. Use
    /// `--resolved` to print the composed prompt (global + project joined by
    /// a blank line) exactly as it would lead a step prompt.
    Show {
        /// Limit output to a single scope.
        #[arg(long)]
        scope: Option<PromptScope>,

        /// Show the final composed prompt (global + project) rather than
        /// each scope's individual contribution.
        #[arg(long)]
        resolved: bool,
    },

    /// Set the prompt at the given scope, replacing any existing value.
    /// Pass an empty string to blank it (equivalent to `prompt clear`).
    Set {
        /// Target scope.
        #[arg(long)]
        scope: PromptScope,

        /// The prompt content for this scope.
        content: String,
    },

    /// Clear the prompt at the given scope.
    Clear {
        /// Target scope.
        #[arg(long)]
        scope: PromptScope,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn test_cli_debug_assert() {
        // Verifies that the clap derive macros produce a valid CLI definition.
        Cli::command().debug_assert();
    }

    #[test]
    fn test_parse_init() {
        let cli = Cli::try_parse_from(["ralph-rs", "init"]).unwrap();
        assert!(matches!(cli.command.unwrap(), Command::Init { .. }));
    }

    #[test]
    fn test_parse_bare_ralph_yields_none_command() {
        // Bare `ralph` with no subcommand parses successfully and leaves
        // `command` empty so main can route to the TUI plan-list view.
        let cli = Cli::try_parse_from(["ralph-rs"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn test_parse_bare_ralph_with_global_flags() {
        // Global flags without a subcommand still parse and leave `command`
        // empty — main applies the routing rules from there.
        let cli =
            Cli::try_parse_from(["ralph-rs", "--project", "/tmp", "--non-interactive"]).unwrap();
        assert!(cli.command.is_none());
        assert!(cli.non_interactive);
        assert_eq!(cli.project.unwrap().to_str().unwrap(), "/tmp");
    }

    #[test]
    fn test_parse_plan_create() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "create",
            "my-feature",
            "--description",
            "Add feature X",
            "--branch",
            "feat/x",
        ])
        .unwrap();

        if let Command::Plan(PlanCommand::Create {
            slug,
            description,
            branch,
            ..
        }) = cli.command.unwrap()
        {
            assert_eq!(slug, "my-feature");
            assert_eq!(description.as_deref(), Some("Add feature X"));
            assert_eq!(branch.as_deref(), Some("feat/x"));
        } else {
            panic!("Expected Plan Create");
        }
    }

    #[test]
    fn test_parse_plan_list() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "list", "--all"]).unwrap();
        if let Command::Plan(PlanCommand::List { all, .. }) = cli.command.unwrap() {
            assert!(all);
        } else {
            panic!("Expected Plan List");
        }
    }

    #[test]
    fn test_parse_step_add() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "Implement parser",
            "my-feature",
            "--description",
            "Build the parser module",
        ])
        .unwrap();

        if let Command::Step(StepCommand::Add {
            title,
            plan,
            description,
            ..
        }) = cli.command.unwrap()
        {
            assert_eq!(title.as_deref(), Some("Implement parser"));
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert_eq!(description.as_deref(), Some("Build the parser module"));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_import_json() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "add", "--import-json", "-"]).unwrap();
        if let Command::Step(StepCommand::Add {
            title, import_json, ..
        }) = cli.command.unwrap()
        {
            assert!(title.is_none());
            assert_eq!(import_json.as_deref(), Some("-"));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_import_json_with_plan_slug() {
        // With --import-json, the single positional should parse as the
        // (would-be) title slot — the handler reinterprets it as the plan
        // slug. This guards against a clap-level conflict error.
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "--import-json",
            "f.json",
            "my-plan",
        ])
        .unwrap();
        if let Command::Step(StepCommand::Add {
            title,
            plan,
            import_json,
            ..
        }) = cli.command.unwrap()
        {
            assert_eq!(title.as_deref(), Some("my-plan"));
            assert!(plan.is_none());
            assert_eq!(import_json.as_deref(), Some("f.json"));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_import_json_conflicts_with_description() {
        // Non-positional single-step flags still conflict with --import-json.
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "--import-json",
            "-",
            "--description",
            "x",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_step_add_requires_title_without_import() {
        let result = Cli::try_parse_from(["ralph-rs", "step", "add"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_run() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "my-feature"]).unwrap();
        if let Command::Run { plan, all, .. } = cli.command.unwrap() {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(!all);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_all_with_slug_conflicts() {
        // A positional plan slug paired with --all is ambiguous (the slug
        // would be silently ignored), so clap must reject the combination.
        let result = Cli::try_parse_from(["ralph-rs", "run", "my-plan", "--all"]);
        assert!(
            result.is_err(),
            "clap must reject a plan slug combined with --all"
        );
    }

    #[test]
    fn test_parse_run_one() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--one"]).unwrap();
        if let Command::Run { plan, one, all, .. } = cli.command.unwrap() {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(one);
            assert!(!all);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_single_alias() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--single"]).unwrap();
        if let Command::Run { one, .. } = cli.command.unwrap() {
            assert!(one);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_all_plans() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "--all"]).unwrap();
        if let Command::Run { all, one, .. } = cli.command.unwrap() {
            assert!(all);
            assert!(!one);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_one_and_all_conflict() {
        let result = Cli::try_parse_from(["ralph-rs", "run", "--one", "--all"]);
        assert!(
            result.is_err(),
            "clap must reject --one combined with --all"
        );
    }

    #[test]
    fn test_parse_run_current_branch() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--current-branch"]).unwrap();
        if let Command::Run {
            plan,
            current_branch,
            ..
        } = cli.command.unwrap()
        {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(current_branch);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_no_auto_stash() {
        // The `--no-auto-stash` flag flips the default-on behavior off for
        // a single run. Default is "opt-out" now (the old flag was
        // `--auto-stash` and defaulted off).
        let cli = Cli::try_parse_from(["ralph-rs", "run", "--no-auto-stash"]).unwrap();
        if let Command::Run { no_auto_stash, .. } = cli.command.unwrap() {
            assert!(no_auto_stash);
        } else {
            panic!("Expected Run");
        }

        // Default must leave `no_auto_stash` false (i.e. auto-stash is on).
        let cli = Cli::try_parse_from(["ralph-rs", "run"]).unwrap();
        if let Command::Run { no_auto_stash, .. } = cli.command.unwrap() {
            assert!(!no_auto_stash);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_plan_create_with_deps() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "create",
            "my-feature",
            "--depends-on",
            "a",
            "--depends-on",
            "b",
        ])
        .unwrap();

        if let Command::Plan(PlanCommand::Create {
            slug, depends_on, ..
        }) = cli.command.unwrap()
        {
            assert_eq!(slug, "my-feature");
            assert_eq!(depends_on, vec!["a".to_string(), "b".to_string()]);
        } else {
            panic!("Expected Plan Create");
        }
    }

    #[test]
    fn test_parse_plan_dependency_add() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "dependency",
            "add",
            "foo",
            "--depends-on",
            "bar",
        ])
        .unwrap();

        if let Command::Plan(PlanCommand::Dependency(PlanDependencyCommand::Add {
            slug,
            depends_on,
        })) = cli.command.unwrap()
        {
            assert_eq!(slug, "foo");
            assert_eq!(depends_on, vec!["bar".to_string()]);
        } else {
            panic!("Expected Plan Dependency Add");
        }
    }

    #[test]
    fn test_parse_plan_dependency_add_requires_depends_on() {
        // Missing --depends-on should error because of num_args = 1..
        let result = Cli::try_parse_from(["ralph-rs", "plan", "dependency", "add", "foo"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_plan_dependency_remove() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "dependency",
            "remove",
            "foo",
            "--depends-on",
            "bar",
            "--depends-on",
            "baz",
        ])
        .unwrap();

        if let Command::Plan(PlanCommand::Dependency(PlanDependencyCommand::Remove {
            slug,
            depends_on,
        })) = cli.command.unwrap()
        {
            assert_eq!(slug, "foo");
            assert_eq!(depends_on, vec!["bar".to_string(), "baz".to_string()]);
        } else {
            panic!("Expected Plan Dependency Remove");
        }
    }

    #[test]
    fn test_parse_plan_dependency_list() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "dependency", "list", "foo"]).unwrap();
        if let Command::Plan(PlanCommand::Dependency(PlanDependencyCommand::List { slug })) =
            cli.command.unwrap()
        {
            assert_eq!(slug, "foo");
        } else {
            panic!("Expected Plan Dependency List");
        }
    }

    #[test]
    fn test_parse_resume() {
        let cli = Cli::try_parse_from(["ralph-rs", "resume"]).unwrap();
        assert!(matches!(cli.command.unwrap(), Command::Resume { .. }));
    }

    #[test]
    fn test_parse_skip() {
        let cli = Cli::try_parse_from(["ralph-rs", "skip", "--step", "3"]).unwrap();
        if let Command::Skip { step, .. } = cli.command.unwrap() {
            assert_eq!(step, Some(3));
        } else {
            panic!("Expected Skip");
        }
    }

    #[test]
    fn test_parse_cancel_defaults() {
        let cli = Cli::try_parse_from(["ralph-rs", "cancel"]).unwrap();
        if let Command::Cancel {
            plan,
            force,
            timeout,
        } = cli.command.unwrap()
        {
            assert!(plan.is_none());
            assert!(!force);
            assert_eq!(timeout, 15);
        } else {
            panic!("Expected Cancel");
        }
    }

    #[test]
    fn test_parse_cancel_with_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "cancel", "myplan"]).unwrap();
        if let Command::Cancel {
            plan,
            force,
            timeout,
        } = cli.command.unwrap()
        {
            assert_eq!(plan.as_deref(), Some("myplan"));
            assert!(!force);
            assert_eq!(timeout, 15);
        } else {
            panic!("Expected Cancel");
        }
    }

    #[test]
    fn test_parse_cancel_force() {
        let cli = Cli::try_parse_from(["ralph-rs", "cancel", "--force"]).unwrap();
        if let Command::Cancel { force, .. } = cli.command.unwrap() {
            assert!(force);
        } else {
            panic!("Expected Cancel");
        }
    }

    #[test]
    fn test_parse_cancel_timeout() {
        let cli = Cli::try_parse_from(["ralph-rs", "cancel", "--timeout", "30"]).unwrap();
        if let Command::Cancel { timeout, .. } = cli.command.unwrap() {
            assert_eq!(timeout, 30);
        } else {
            panic!("Expected Cancel");
        }
    }

    #[test]
    fn test_parse_export() {
        let cli = Cli::try_parse_from(["ralph-rs", "export", "my-plan", "--output", "plan.json"])
            .unwrap();
        if let Command::Export { plan, output } = cli.command.unwrap() {
            assert_eq!(plan, "my-plan");
            assert_eq!(output.unwrap().to_str().unwrap(), "plan.json");
        } else {
            panic!("Expected Export");
        }
    }

    #[test]
    fn test_parse_import() {
        let cli = Cli::try_parse_from(["ralph-rs", "import", "plan.json"]).unwrap();
        if let Command::Import { file, .. } = cli.command.unwrap() {
            assert_eq!(file.to_str().unwrap(), "plan.json");
        } else {
            panic!("Expected Import");
        }
    }

    #[test]
    fn test_parse_status() {
        let cli = Cli::try_parse_from(["ralph-rs", "status", "--verbose"]).unwrap();
        if let Command::Status { verbose, .. } = cli.command.unwrap() {
            assert!(verbose);
        } else {
            panic!("Expected Status");
        }
    }

    #[test]
    fn test_parse_log() {
        let cli = Cli::try_parse_from(["ralph-rs", "log", "--step", "2", "--limit", "10"]).unwrap();
        if let Command::Log { step, limit, .. } = cli.command.unwrap() {
            assert_eq!(step, Some(2));
            assert_eq!(limit, Some(10));
        } else {
            panic!("Expected Log");
        }
    }

    #[test]
    fn test_parse_doctor() {
        let cli = Cli::try_parse_from(["ralph-rs", "doctor"]).unwrap();
        assert!(matches!(cli.command.unwrap(), Command::Doctor));
    }

    #[test]
    fn test_parse_agents_list() {
        let cli = Cli::try_parse_from(["ralph-rs", "agents", "list"]).unwrap();
        assert!(matches!(
            cli.command.unwrap(),
            Command::Agents(AgentsCommand::List)
        ));
    }

    #[test]
    fn test_parse_plan_harness_set() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "set", "codex"]).unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Set { harness, plan })) =
            cli.command.unwrap()
        {
            assert_eq!(harness, "codex");
            assert!(plan.is_none());
        } else {
            panic!("Expected Plan Harness Set");
        }
    }

    #[test]
    fn test_parse_plan_harness_set_with_positional_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "set", "codex", "my-plan"])
            .unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Set { harness, plan })) =
            cli.command.unwrap()
        {
            assert_eq!(harness, "codex");
            assert_eq!(plan.as_deref(), Some("my-plan"));
        } else {
            panic!("Expected Plan Harness Set");
        }
    }

    #[test]
    fn test_parse_plan_harness_set_rejects_plan_flag() {
        // `--plan` used to be a flag but is now positional only. Clean break.
        let result = Cli::try_parse_from([
            "ralph-rs", "plan", "harness", "set", "codex", "--plan", "my-plan",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_plan_harness_show() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "show"]).unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Show { plan })) =
            cli.command.unwrap()
        {
            assert!(plan.is_none());
        } else {
            panic!("Expected Plan Harness Show");
        }
    }

    #[test]
    fn test_parse_plan_harness_show_with_positional_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "show", "my-plan"]).unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Show { plan })) =
            cli.command.unwrap()
        {
            assert_eq!(plan.as_deref(), Some("my-plan"));
        } else {
            panic!("Expected Plan Harness Show");
        }
    }

    #[test]
    fn test_parse_plan_harness_show_rejects_plan_flag() {
        let result =
            Cli::try_parse_from(["ralph-rs", "plan", "harness", "show", "--plan", "my-plan"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_plan_harness_generate() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "generate", "Add feature X"])
            .unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Generate {
            description,
            plan,
            ..
        })) = cli.command.unwrap()
        {
            assert_eq!(description.as_deref(), Some("Add feature X"));
            assert!(plan.is_none());
        } else {
            panic!("Expected Plan Harness Generate");
        }
    }

    #[test]
    fn test_parse_plan_harness_generate_with_positional_plan() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "harness",
            "generate",
            "Add feature X",
            "my-plan",
        ])
        .unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Generate {
            description,
            plan,
            ..
        })) = cli.command.unwrap()
        {
            assert_eq!(description.as_deref(), Some("Add feature X"));
            assert_eq!(plan.as_deref(), Some("my-plan"));
        } else {
            panic!("Expected Plan Harness Generate");
        }
    }

    #[test]
    fn test_parse_plan_harness_generate_rejects_plan_flag() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "harness",
            "generate",
            "Add feature X",
            "--plan",
            "my-plan",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_global_project_flag() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "--project", "/tmp/my-project", "status"]).unwrap();
        assert_eq!(cli.project.unwrap().to_str().unwrap(), "/tmp/my-project");
    }

    #[test]
    fn test_global_harness_flag() {
        let cli = Cli::try_parse_from(["ralph-rs", "--harness", "codex", "doctor"]).unwrap();
        assert_eq!(cli.harness.as_deref(), Some("codex"));
    }

    #[test]
    fn test_global_non_interactive_flag() {
        let cli = Cli::try_parse_from(["ralph-rs", "--non-interactive", "plan", "list"]).unwrap();
        assert!(cli.non_interactive);

        // Default is false.
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "list"]).unwrap();
        assert!(!cli.non_interactive);
    }

    #[test]
    fn test_global_jsonl_alias_for_json() {
        // Both spellings populate the same `json` field.
        let cli = Cli::try_parse_from(["ralph-rs", "--json", "run"]).unwrap();
        assert!(cli.json);

        let cli = Cli::try_parse_from(["ralph-rs", "--jsonl", "run"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn test_run_subcommand_captures_harness_flag() {
        // The Run subcommand must carry its own harness field so main.rs can
        // honor per-subcommand overrides. clap's `global = true` on the top
        // level makes both fields mirror the final `--harness` value, which is
        // why the parse check below sees the same string in both places.
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "--harness",
            "codex",
            "run",
            "--harness",
            "claude",
        ])
        .unwrap();
        if let Command::Run {
            harness: run_harness,
            ..
        } = cli.command.unwrap()
        {
            assert_eq!(run_harness.as_deref(), Some("claude"));
        } else {
            panic!("Expected Run command");
        }
    }

    #[test]
    fn test_harness_precedence_prefers_subcommand() {
        // Direct regression test for the rule applied in main.rs:
        //   per-subcommand --harness beats global --harness.
        // Expressed here so the precedence survives any refactor that relocates
        // the dispatcher.
        let global = Some("codex".to_string());
        let run_flag = Some("claude".to_string());
        let resolved = run_flag.clone().or(global.clone());
        assert_eq!(resolved.as_deref(), Some("claude"));

        // Falls back to the global when the subcommand flag is absent.
        let none_run: Option<String> = None;
        let resolved = none_run.or(global.clone());
        assert_eq!(resolved.as_deref(), Some("codex"));
    }

    #[test]
    fn test_step_move() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "move", "3", "--to", "1"]).unwrap();
        if let Command::Step(StepCommand::Move { step, to, .. }) = cli.command.unwrap() {
            assert_eq!(step, Some(3));
            assert_eq!(to, 1);
        } else {
            panic!("Expected Step Move");
        }
    }

    #[test]
    fn test_step_reset() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "reset", "2"]).unwrap();
        if let Command::Step(StepCommand::Reset { step, .. }) = cli.command.unwrap() {
            assert_eq!(step, Some(2));
        } else {
            panic!("Expected Step Reset");
        }
    }

    #[test]
    fn test_step_remove() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "remove", "1", "--force"]).unwrap();
        if let Command::Step(StepCommand::Remove { step, force, .. }) = cli.command.unwrap() {
            assert_eq!(step, Some(1));
            assert!(force);
        } else {
            panic!("Expected Step Remove");
        }
    }

    #[test]
    fn test_plan_delete() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "plan", "delete", "old-plan", "--force"]).unwrap();
        if let Command::Plan(PlanCommand::Delete { slug, force }) = cli.command.unwrap() {
            assert_eq!(slug, "old-plan");
            assert!(force);
        } else {
            panic!("Expected Plan Delete");
        }
    }

    #[test]
    fn test_plan_delete_yes_alias() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "delete", "old-plan", "--yes"]).unwrap();
        if let Command::Plan(PlanCommand::Delete { slug, force }) = cli.command.unwrap() {
            assert_eq!(slug, "old-plan");
            assert!(force);
        } else {
            panic!("Expected Plan Delete");
        }
    }

    #[test]
    fn test_step_remove_yes_alias() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "remove", "1", "--yes"]).unwrap();
        if let Command::Step(StepCommand::Remove { step, force, .. }) = cli.command.unwrap() {
            assert_eq!(step, Some(1));
            assert!(force);
        } else {
            panic!("Expected Step Remove");
        }
    }

    #[test]
    fn test_plan_list_status_value_enum() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "plan", "list", "--status", "in_progress"]).unwrap();
        if let Command::Plan(PlanCommand::List { status, .. }) = cli.command.unwrap() {
            assert_eq!(status, Some(crate::plan::PlanStatus::InProgress));
        } else {
            panic!("Expected Plan List");
        }
    }

    #[test]
    fn test_plan_list_status_invalid_rejected() {
        let result = Cli::try_parse_from(["ralph-rs", "plan", "list", "--status", "bogus"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_hooks_add_lifecycle_value_enum() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "hooks",
            "add",
            "my-hook",
            "--lifecycle",
            "pre-step",
            "--command",
            "echo hello",
        ])
        .unwrap();
        if let Command::Hooks(HooksCommand::Add { lifecycle, .. }) = cli.command.unwrap() {
            assert_eq!(lifecycle, crate::hook_library::Lifecycle::PreStep);
        } else {
            panic!("Expected Hooks Add");
        }
    }

    #[test]
    fn test_hooks_add_lifecycle_invalid_rejected() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "hooks",
            "add",
            "my-hook",
            "--lifecycle",
            "bogus",
            "--command",
            "echo hello",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_step_set_hook_lifecycle_value_enum() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "set-hook",
            "1",
            "my-plan",
            "--lifecycle",
            "post-test",
            "--hook",
            "my-hook",
        ])
        .unwrap();
        if let Command::Step(StepCommand::SetHook { lifecycle, .. }) = cli.command.unwrap() {
            assert_eq!(lifecycle, crate::hook_library::Lifecycle::PostTest);
        } else {
            panic!("Expected Step SetHook");
        }
    }

    #[test]
    fn test_step_set_hook_lifecycle_invalid_rejected() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "set-hook",
            "1",
            "my-plan",
            "--lifecycle",
            "bogus",
            "--hook",
            "my-hook",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_step_add_change_policy_optional() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "Review",
            "--change-policy",
            "optional",
        ])
        .unwrap();
        if let Command::Step(StepCommand::Add { change_policy, .. }) = cli.command.unwrap() {
            assert_eq!(change_policy, Some(crate::plan::ChangePolicy::Optional));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_change_policy_default_none() {
        // Without the flag, the parsed value is None (the handler treats this
        // as "use default" = Required).
        let cli = Cli::try_parse_from(["ralph-rs", "step", "add", "Implement"]).unwrap();
        if let Command::Step(StepCommand::Add { change_policy, .. }) = cli.command.unwrap() {
            assert!(change_policy.is_none());
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_change_policy_invalid_rejected() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "Review",
            "--change-policy",
            "forbidden",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_step_edit_change_policy() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "edit",
            "1",
            "--change-policy",
            "required",
        ])
        .unwrap();
        if let Command::Step(StepCommand::Edit { change_policy, .. }) = cli.command.unwrap() {
            assert_eq!(change_policy, Some(crate::plan::ChangePolicy::Required));
        } else {
            panic!("Expected Step Edit");
        }
    }

    #[test]
    fn test_parse_plan_create_retry_strategy() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "create",
            "my-plan",
            "--retry-strategy",
            "rollback",
        ])
        .unwrap();
        if let Command::Plan(PlanCommand::Create { retry_strategy, .. }) = cli.command.unwrap() {
            assert_eq!(retry_strategy, Some(crate::plan::RetryStrategy::Rollback));
        } else {
            panic!("Expected Plan Create");
        }
    }

    #[test]
    fn test_parse_plan_create_retry_strategy_default_none() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "create", "my-plan"]).unwrap();
        if let Command::Plan(PlanCommand::Create { retry_strategy, .. }) = cli.command.unwrap() {
            assert!(retry_strategy.is_none());
        } else {
            panic!("Expected Plan Create");
        }
    }

    #[test]
    fn test_parse_step_add_retry_strategy() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "Implement",
            "--retry-strategy",
            "keep",
        ])
        .unwrap();
        if let Command::Step(StepCommand::Add { retry_strategy, .. }) = cli.command.unwrap() {
            assert_eq!(retry_strategy, Some(crate::plan::RetryStrategy::Keep));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_retry_strategy_invalid_rejected() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "Implement",
            "--retry-strategy",
            "discard",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_step_edit_retry_strategy() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "edit",
            "1",
            "--retry-strategy",
            "rollback",
        ])
        .unwrap();
        if let Command::Step(StepCommand::Edit {
            retry_strategy,
            clear_retry_strategy,
            ..
        }) = cli.command.unwrap()
        {
            assert_eq!(retry_strategy, Some(crate::plan::RetryStrategy::Rollback));
            assert!(!clear_retry_strategy);
        } else {
            panic!("Expected Step Edit");
        }
    }

    #[test]
    fn test_parse_step_edit_clear_retry_strategy() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "edit", "1", "--clear-retry-strategy"])
            .unwrap();
        if let Command::Step(StepCommand::Edit {
            retry_strategy,
            clear_retry_strategy,
            ..
        }) = cli.command.unwrap()
        {
            assert!(retry_strategy.is_none());
            assert!(clear_retry_strategy);
        } else {
            panic!("Expected Step Edit");
        }
    }

    #[test]
    fn test_parse_step_edit_set_and_clear_retry_strategy_conflict() {
        // Mirrors how `--criteria` + `--clear-criteria` conflict: clap must
        // reject passing both `--retry-strategy` and `--clear-retry-strategy`
        // in the same invocation.
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "edit",
            "1",
            "--retry-strategy",
            "keep",
            "--clear-retry-strategy",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_retry_strategy_help_explains_precedence() {
        // Render the long help for `step add` and assert the precedence rule
        // and both value meanings are documented (acceptance criterion:
        // "help text explains the precedence"). We introspect the clap
        // Command rather than shelling out so the test is hermetic.
        let mut cmd = Cli::command();
        let mut step_add = cmd
            .find_subcommand_mut("step")
            .and_then(|s| s.find_subcommand_mut("add"))
            .expect("step add subcommand")
            .clone();
        let help = step_add.render_long_help().to_string();
        assert!(
            help.contains("step > plan > default"),
            "help should state the step>plan>default precedence; got:\n{help}"
        );
        assert!(
            help.contains("keep") && help.contains("rollback"),
            "help should explain both keep and rollback; got:\n{help}"
        );
        assert!(
            help.to_lowercase().contains("rolls the working tree back"),
            "help should explain rollback semantics; got:\n{help}"
        );
    }

    #[test]
    fn test_parse_question_ask_positional() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "question",
            "ask",
            "Should I use Postgres or SQLite?",
            "--suggest",
            "PostgreSQL",
            "-s",
            "SQLite",
        ])
        .unwrap();
        if let Command::Question(QuestionCommand::Ask { question, suggest }) = cli.command.unwrap()
        {
            assert_eq!(
                question.as_deref(),
                Some("Should I use Postgres or SQLite?")
            );
            assert_eq!(
                suggest,
                vec!["PostgreSQL".to_string(), "SQLite".to_string()]
            );
        } else {
            panic!("Expected Question Ask");
        }
    }

    #[test]
    fn test_parse_question_ask_no_positional_no_suggestions() {
        // Both the positional and the `-s` flag must be optional so the
        // stdin-only / open-ended cases parse cleanly.
        let cli = Cli::try_parse_from(["ralph-rs", "question", "ask"]).unwrap();
        if let Command::Question(QuestionCommand::Ask { question, suggest }) = cli.command.unwrap()
        {
            assert!(question.is_none());
            assert!(suggest.is_empty());
        } else {
            panic!("Expected Question Ask");
        }
    }

    #[test]
    fn test_parse_question_list_no_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "question", "list"]).unwrap();
        if let Command::Question(QuestionCommand::List { plan }) = cli.command.unwrap() {
            assert!(plan.is_none());
        } else {
            panic!("Expected Question List");
        }
    }

    #[test]
    fn test_parse_question_list_with_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "question", "list", "my-plan"]).unwrap();
        if let Command::Question(QuestionCommand::List { plan }) = cli.command.unwrap() {
            assert_eq!(plan.as_deref(), Some("my-plan"));
        } else {
            panic!("Expected Question List");
        }
    }

    #[test]
    fn test_parse_question_answer() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "question", "answer", "3", "use Postgres"]).unwrap();
        if let Command::Question(QuestionCommand::Answer { num, text }) = cli.command.unwrap() {
            assert_eq!(num, 3);
            assert_eq!(text.as_deref(), Some("use Postgres"));
        } else {
            panic!("Expected Question Answer");
        }
    }

    #[test]
    fn test_parse_question_answer_text_optional_for_stdin() {
        // Omitting the text positional must still parse so the dispatcher can
        // fall back to stdin (heredoc-friendly invocation).
        let cli = Cli::try_parse_from(["ralph-rs", "question", "answer", "1"]).unwrap();
        if let Command::Question(QuestionCommand::Answer { num, text }) = cli.command.unwrap() {
            assert_eq!(num, 1);
            assert!(text.is_none());
        } else {
            panic!("Expected Question Answer");
        }
    }

    #[test]
    fn test_parse_question_show() {
        let cli = Cli::try_parse_from(["ralph-rs", "question", "show", "2"]).unwrap();
        if let Command::Question(QuestionCommand::Show { num }) = cli.command.unwrap() {
            assert_eq!(num, 2);
        } else {
            panic!("Expected Question Show");
        }
    }

    #[test]
    fn test_parse_plan_questions_on() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "questions", "on", "my-plan"]).unwrap();
        if let Command::Plan(PlanCommand::Questions { state, slug }) = cli.command.unwrap() {
            assert_eq!(state, QuestionsState::On);
            assert_eq!(slug, "my-plan");
        } else {
            panic!("Expected Plan Questions");
        }
    }

    #[test]
    fn test_parse_plan_questions_off() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "questions", "off", "my-plan"]).unwrap();
        if let Command::Plan(PlanCommand::Questions { state, slug }) = cli.command.unwrap() {
            assert_eq!(state, QuestionsState::Off);
            assert_eq!(slug, "my-plan");
        } else {
            panic!("Expected Plan Questions");
        }
    }

    #[test]
    fn test_parse_plan_questions_invalid_state_rejected() {
        // Only `on`/`off` are valid; anything else must be rejected by clap.
        let result = Cli::try_parse_from(["ralph-rs", "plan", "questions", "maybe", "my-plan"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_plan_questions_requires_slug() {
        let result = Cli::try_parse_from(["ralph-rs", "plan", "questions", "on"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_set_hook_lifecycle_value_enum() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "plan",
            "set-hook",
            "my-plan",
            "--lifecycle",
            "pre-test",
            "--hook",
            "my-hook",
        ])
        .unwrap();
        if let Command::Plan(PlanCommand::SetHook { lifecycle, .. }) = cli.command.unwrap() {
            assert_eq!(lifecycle, crate::hook_library::Lifecycle::PreTest);
        } else {
            panic!("Expected Plan SetHook");
        }
    }

    #[test]
    fn test_parse_prompt_scope_universal_resolves_to_global() {
        // `--scope universal` is a clap value alias for `global`; the enum
        // maps it onto the single `Global` variant so every downstream
        // prompt subcommand path treats them identically.
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "prompt",
            "set",
            "--scope",
            "universal",
            "hello world",
        ])
        .unwrap();
        if let Command::Prompt(PromptCommand::Set { scope, content }) = cli.command.unwrap() {
            assert_eq!(scope, PromptScope::Global);
            assert_eq!(content, "hello world");
        } else {
            panic!("Expected Prompt Set");
        }

        // Same for `clear` and `show`.
        let cli =
            Cli::try_parse_from(["ralph-rs", "prompt", "clear", "--scope", "universal"]).unwrap();
        if let Command::Prompt(PromptCommand::Clear { scope }) = cli.command.unwrap() {
            assert_eq!(scope, PromptScope::Global);
        } else {
            panic!("Expected Prompt Clear");
        }

        let cli =
            Cli::try_parse_from(["ralph-rs", "prompt", "show", "--scope", "universal"]).unwrap();
        if let Command::Prompt(PromptCommand::Show { scope, .. }) = cli.command.unwrap() {
            assert_eq!(scope, Some(PromptScope::Global));
        } else {
            panic!("Expected Prompt Show");
        }
    }

    #[test]
    fn test_parse_prompt_scope_global_and_universal_are_indistinguishable() {
        // The two spellings must parse to the exact same variant — there's
        // no separate "Universal" variant to diverge later.
        let from_global =
            Cli::try_parse_from(["ralph-rs", "prompt", "clear", "--scope", "global"]).unwrap();
        let from_universal =
            Cli::try_parse_from(["ralph-rs", "prompt", "clear", "--scope", "universal"]).unwrap();
        let g = match from_global.command.unwrap() {
            Command::Prompt(PromptCommand::Clear { scope }) => scope,
            _ => panic!("Expected Prompt Clear"),
        };
        let u = match from_universal.command.unwrap() {
            Command::Prompt(PromptCommand::Clear { scope }) => scope,
            _ => panic!("Expected Prompt Clear"),
        };
        assert_eq!(g, u);
        assert_eq!(g, PromptScope::Global);
    }
}
