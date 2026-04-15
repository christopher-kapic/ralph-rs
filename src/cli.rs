// CLI argument parsing (clap)

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

use crate::hook_library::Lifecycle;
use crate::plan::PlanStatus;

/// ralph-rs: a deterministic orchestrator for coding agent harnesses.
#[derive(Debug, Parser)]
#[command(name = "ralph", version, about, long_about = None)]
pub struct Cli {
    /// Path to the project directory (defaults to current directory).
    #[arg(long, short = 'C', global = true)]
    pub project: Option<PathBuf>,

    /// Override the default harness for this invocation.
    #[arg(long, global = true)]
    pub harness: Option<String>,

    /// Emit machine-readable JSON output instead of human-readable text.
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress progress and banner output.
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Disable ANSI color output even when stdout is a TTY.
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize ralph-rs: create config/agents directories, detect
    /// installed harnesses, and write the default config.
    Init {
        /// Slug for the initial plan.
        #[arg(long)]
        slug: Option<String>,

        /// Git branch name to use.
        #[arg(long)]
        branch: Option<String>,

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
    /// (ignores the plan slug). Precedence: --all > --one > --from/--to > default.
    Run {
        /// Plan slug to run. Defaults to the active plan.
        plan: Option<String>,

        /// Run only the next pending step instead of all remaining.
        #[arg(long, alias = "single")]
        one: bool,

        /// Run all plans in dependency order (chains plans). Plan slug
        /// is ignored when set.
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

        /// Override the harness for this run.
        #[arg(long)]
        harness: Option<String>,

        /// Reclaim a held run lock even if the previous runner still appears alive (use only if you know the other process is gone).
        #[arg(long)]
        force: bool,
    },

    /// Resume a plan from the last failed or in-progress step.
    Resume {
        /// Plan slug to resume. Defaults to the active plan.
        plan: Option<String>,
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

        /// Maximum number of stdout/stderr lines to show per attempt
        /// (default: 50). Implies showing output. Conflicts with --full.
        #[arg(long)]
        lines: Option<usize>,
    },

    /// List and manage agent file templates.
    #[command(subcommand)]
    Agents(AgentsCommand),

    /// Manage the hook library (reusable shell commands that run at lifecycle events).
    #[command(subcommand)]
    Hooks(HooksCommand),

    /// Run preflight checks to verify the environment is ready.
    Doctor,

    /// Generate shell completions for bash, zsh, fish, elvish, or powershell.
    Completions {
        /// Shell to generate completions for.
        shell: Shell,
    },
}

// ---------------------------------------------------------------------------
// Plan subcommands
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
pub enum PlanCommand {
    /// Create a new plan.
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
        #[arg(long = "depends-on", num_args = 1..)]
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
    },

    /// Add a new step to a plan.
    ///
    /// The single-step form takes a positional title plus per-field flags.
    /// For bulk insertion use `--import-json <FILE|->` to read an array of
    /// step objects (or a single object) from a file or stdin; the positional
    /// title and per-field flags are mutually exclusive with `--import-json`.
    Add {
        /// Step title. Required unless `--import-json` is used.
        #[arg(
            required_unless_present = "import_json",
            conflicts_with = "import_json"
        )]
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

        /// New max retries override. Pass 0 to clear (sets to plan/global default).
        #[arg(long)]
        max_retries: Option<i32>,

        /// Explicitly clear the max-retries override (sets to NULL/plan default).
        #[arg(long)]
        clear_max_retries: bool,
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
        assert!(matches!(cli.command, Command::Init { .. }));
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
        }) = cli.command
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
        if let Command::Plan(PlanCommand::List { all, .. }) = cli.command {
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
        }) = cli.command
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
        }) = cli.command
        {
            assert!(title.is_none());
            assert_eq!(import_json.as_deref(), Some("-"));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_step_add_import_json_conflicts_with_title() {
        let result = Cli::try_parse_from([
            "ralph-rs",
            "step",
            "add",
            "some title",
            "--import-json",
            "-",
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
        let cli = Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--all"]).unwrap();
        if let Command::Run { plan, all, .. } = cli.command {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(all);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_one() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--one"]).unwrap();
        if let Command::Run { plan, one, all, .. } = cli.command {
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
        if let Command::Run { one, .. } = cli.command {
            assert!(one);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_all_plans() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "--all"]).unwrap();
        if let Command::Run { all, one, .. } = cli.command {
            assert!(all);
            assert!(!one);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_current_branch() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "run", "my-feature", "--current-branch"]).unwrap();
        if let Command::Run {
            plan,
            current_branch,
            ..
        } = cli.command
        {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(current_branch);
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
        }) = cli.command
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
        })) = cli.command
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
        })) = cli.command
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
            cli.command
        {
            assert_eq!(slug, "foo");
        } else {
            panic!("Expected Plan Dependency List");
        }
    }

    #[test]
    fn test_parse_resume() {
        let cli = Cli::try_parse_from(["ralph-rs", "resume"]).unwrap();
        assert!(matches!(cli.command, Command::Resume { .. }));
    }

    #[test]
    fn test_parse_skip() {
        let cli = Cli::try_parse_from(["ralph-rs", "skip", "--step", "3"]).unwrap();
        if let Command::Skip { step, .. } = cli.command {
            assert_eq!(step, Some(3));
        } else {
            panic!("Expected Skip");
        }
    }

    #[test]
    fn test_parse_export() {
        let cli = Cli::try_parse_from(["ralph-rs", "export", "my-plan", "--output", "plan.json"])
            .unwrap();
        if let Command::Export { plan, output } = cli.command {
            assert_eq!(plan, "my-plan");
            assert_eq!(output.unwrap().to_str().unwrap(), "plan.json");
        } else {
            panic!("Expected Export");
        }
    }

    #[test]
    fn test_parse_import() {
        let cli = Cli::try_parse_from(["ralph-rs", "import", "plan.json"]).unwrap();
        if let Command::Import { file, .. } = cli.command {
            assert_eq!(file.to_str().unwrap(), "plan.json");
        } else {
            panic!("Expected Import");
        }
    }

    #[test]
    fn test_parse_status() {
        let cli = Cli::try_parse_from(["ralph-rs", "status", "--verbose"]).unwrap();
        if let Command::Status { verbose, .. } = cli.command {
            assert!(verbose);
        } else {
            panic!("Expected Status");
        }
    }

    #[test]
    fn test_parse_log() {
        let cli = Cli::try_parse_from(["ralph-rs", "log", "--step", "2", "--limit", "10"]).unwrap();
        if let Command::Log { step, limit, .. } = cli.command {
            assert_eq!(step, Some(2));
            assert_eq!(limit, Some(10));
        } else {
            panic!("Expected Log");
        }
    }

    #[test]
    fn test_parse_doctor() {
        let cli = Cli::try_parse_from(["ralph-rs", "doctor"]).unwrap();
        assert!(matches!(cli.command, Command::Doctor));
    }

    #[test]
    fn test_parse_agents_list() {
        let cli = Cli::try_parse_from(["ralph-rs", "agents", "list"]).unwrap();
        assert!(matches!(cli.command, Command::Agents(AgentsCommand::List)));
    }

    #[test]
    fn test_parse_plan_harness_set() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "set", "codex"]).unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Set { harness, plan })) =
            cli.command
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
            cli.command
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
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Show { plan })) = cli.command
        {
            assert!(plan.is_none());
        } else {
            panic!("Expected Plan Harness Show");
        }
    }

    #[test]
    fn test_parse_plan_harness_show_with_positional_plan() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "harness", "show", "my-plan"]).unwrap();
        if let Command::Plan(PlanCommand::Harness(PlanHarnessCommand::Show { plan })) = cli.command
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
        })) = cli.command
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
        })) = cli.command
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
    fn test_step_move() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "move", "3", "--to", "1"]).unwrap();
        if let Command::Step(StepCommand::Move { step, to, .. }) = cli.command {
            assert_eq!(step, Some(3));
            assert_eq!(to, 1);
        } else {
            panic!("Expected Step Move");
        }
    }

    #[test]
    fn test_step_reset() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "reset", "2"]).unwrap();
        if let Command::Step(StepCommand::Reset { step, .. }) = cli.command {
            assert_eq!(step, Some(2));
        } else {
            panic!("Expected Step Reset");
        }
    }

    #[test]
    fn test_step_remove() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "remove", "1", "--force"]).unwrap();
        if let Command::Step(StepCommand::Remove { step, force, .. }) = cli.command {
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
        if let Command::Plan(PlanCommand::Delete { slug, force }) = cli.command {
            assert_eq!(slug, "old-plan");
            assert!(force);
        } else {
            panic!("Expected Plan Delete");
        }
    }

    #[test]
    fn test_plan_delete_yes_alias() {
        let cli = Cli::try_parse_from(["ralph-rs", "plan", "delete", "old-plan", "--yes"]).unwrap();
        if let Command::Plan(PlanCommand::Delete { slug, force }) = cli.command {
            assert_eq!(slug, "old-plan");
            assert!(force);
        } else {
            panic!("Expected Plan Delete");
        }
    }

    #[test]
    fn test_step_remove_yes_alias() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "remove", "1", "--yes"]).unwrap();
        if let Command::Step(StepCommand::Remove { step, force, .. }) = cli.command {
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
        if let Command::Plan(PlanCommand::List { status, .. }) = cli.command {
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
        if let Command::Hooks(HooksCommand::Add { lifecycle, .. }) = cli.command {
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
        if let Command::Step(StepCommand::SetHook { lifecycle, .. }) = cli.command {
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
        if let Command::Plan(PlanCommand::SetHook { lifecycle, .. }) = cli.command {
            assert_eq!(lifecycle, crate::hook_library::Lifecycle::PreTest);
        } else {
            panic!("Expected Plan SetHook");
        }
    }
}
