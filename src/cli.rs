// CLI argument parsing (clap)
#![allow(dead_code)]

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

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

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize ralph-rs in the current project directory.
    Init {
        /// Slug for the initial plan.
        #[arg(long)]
        slug: Option<String>,

        /// Git branch name to use.
        #[arg(long)]
        branch: Option<String>,
    },

    /// Manage plans.
    #[command(subcommand)]
    Plan(PlanCommand),

    /// Manage steps within a plan.
    #[command(subcommand)]
    Step(StepCommand),

    /// Run the next pending step (or all remaining steps) of a plan.
    Run {
        /// Plan slug to run.
        #[arg(long)]
        plan: Option<String>,

        /// Run only the next pending step instead of all remaining.
        #[arg(long)]
        step: bool,

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
    },

    /// Resume a plan from the last failed or in-progress step.
    Resume {
        /// Plan slug to resume.
        #[arg(long)]
        plan: Option<String>,
    },

    /// Skip the current or specified step.
    Skip {
        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// Step number to skip (1-based). Defaults to current step.
        #[arg(long)]
        step: Option<usize>,

        /// Reason for skipping.
        #[arg(long)]
        reason: Option<String>,
    },

    /// Manage plan-level harness configuration.
    PlanHarness(PlanHarnessArgs),

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
        #[arg(long)]
        plan: Option<String>,

        /// Show verbose output including step details.
        #[arg(long, short)]
        verbose: bool,
    },

    /// Show execution logs.
    Log {
        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// Step number (1-based) to show logs for.
        #[arg(long)]
        step: Option<usize>,

        /// Maximum number of log entries to show.
        #[arg(long, short)]
        limit: Option<usize>,

        /// Show full log output (stdout/stderr).
        #[arg(long)]
        full: bool,
    },

    /// List and manage agent file templates.
    #[command(subcommand)]
    Agents(AgentsCommand),

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
        status: Option<String>,

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
        #[arg(long, short)]
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
        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,
    },

    /// Add a new step to a plan.
    Add {
        /// Step title.
        title: String,

        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// Step description.
        #[arg(long, short)]
        description: Option<String>,

        /// Position to insert at (1-based). Defaults to end.
        #[arg(long)]
        after: Option<usize>,

        /// Agent/model override for this step.
        #[arg(long)]
        agent: Option<String>,

        /// Harness override for this step.
        #[arg(long)]
        harness: Option<String>,
    },

    /// Remove a step from a plan.
    Remove {
        /// Step number (1-based).
        step: usize,

        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// Skip confirmation prompt.
        #[arg(long, short)]
        force: bool,
    },

    /// Edit a step's title or description.
    Edit {
        /// Step number (1-based).
        step: usize,

        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// New title.
        #[arg(long)]
        title: Option<String>,

        /// New description.
        #[arg(long)]
        description: Option<String>,
    },

    /// Reset a step's status back to pending.
    Reset {
        /// Step number (1-based).
        step: usize,

        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,
    },

    /// Move a step to a different position.
    Move {
        /// Step number to move (1-based).
        step: usize,

        /// Target position (1-based).
        #[arg(long)]
        to: usize,

        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Plan-harness subcommand
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
pub struct PlanHarnessArgs {
    /// Description of what to plan. If omitted, the harness will ask interactively.
    #[arg(long, short)]
    pub description: Option<String>,

    /// Override the harness to use for planning.
    #[arg(long)]
    pub use_harness: Option<String>,

    #[command(subcommand)]
    pub command: Option<PlanHarnessCommand>,
}

#[derive(Debug, Subcommand)]
pub enum PlanHarnessCommand {
    /// Set the harness for a plan.
    Set {
        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,

        /// Harness name to assign.
        harness: String,
    },

    /// Show the current harness for a plan.
    Show {
        /// Plan slug.
        #[arg(long)]
        plan: Option<String>,
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
            "--plan",
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
            assert_eq!(title, "Implement parser");
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert_eq!(description.as_deref(), Some("Build the parser module"));
        } else {
            panic!("Expected Step Add");
        }
    }

    #[test]
    fn test_parse_run() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "run", "--plan", "my-feature", "--all"]).unwrap();
        if let Command::Run { plan, all, .. } = cli.command {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(all);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_step() {
        let cli =
            Cli::try_parse_from(["ralph-rs", "run", "--plan", "my-feature", "--step"]).unwrap();
        if let Command::Run {
            plan, step, all, ..
        } = cli.command
        {
            assert_eq!(plan.as_deref(), Some("my-feature"));
            assert!(step);
            assert!(!all);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_all_plans() {
        let cli = Cli::try_parse_from(["ralph-rs", "run", "--all"]).unwrap();
        if let Command::Run { all, step, .. } = cli.command {
            assert!(all);
            assert!(!step);
        } else {
            panic!("Expected Run");
        }
    }

    #[test]
    fn test_parse_run_current_branch() {
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "run",
            "--plan",
            "my-feature",
            "--current-branch",
        ])
        .unwrap();
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
        let cli = Cli::try_parse_from(["ralph-rs", "plan-harness", "set", "codex"]).unwrap();
        if let Command::PlanHarness(PlanHarnessArgs {
            command: Some(PlanHarnessCommand::Set { harness, .. }),
            ..
        }) = cli.command
        {
            assert_eq!(harness, "codex");
        } else {
            panic!("Expected PlanHarness Set");
        }
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
            assert_eq!(step, 3);
            assert_eq!(to, 1);
        } else {
            panic!("Expected Step Move");
        }
    }

    #[test]
    fn test_step_reset() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "reset", "2"]).unwrap();
        if let Command::Step(StepCommand::Reset { step, .. }) = cli.command {
            assert_eq!(step, 2);
        } else {
            panic!("Expected Step Reset");
        }
    }

    #[test]
    fn test_step_remove() {
        let cli = Cli::try_parse_from(["ralph-rs", "step", "remove", "1", "--force"]).unwrap();
        if let Command::Step(StepCommand::Remove { step, force, .. }) = cli.command {
            assert_eq!(step, 1);
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
}
