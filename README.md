<p align="center"><img src="header.png" alt="ralph — Deterministic execution planner for coding agent harnesses" width="100%" /></p>

# ralph-rs

A deterministic orchestrator for coding agent harnesses. Takes step-based plans and executes them through AI coding agents (Claude, Codex, OpenCode, Copilot, Goose, Pi) with retry loops, test validation, and git integration.

## What it does

- **Plan management**: Create, edit, and execute step-based plans for AI coding agents
- **Multi-harness support**: Works with Claude Code, Codex, OpenCode, Copilot, Goose, Pi, and more
- **Deterministic execution**: Subprocess orchestration with test validation, git commits, and rollback on failure
- **Retry with context**: Failed attempts inject diffs and test output into retry prompts
- **Interactive TUI**: Ratatui-based terminal UI with vim keybindings for live plan execution
- **Plan portability**: Export/import plans as JSON for harness comparison and reuse

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/christopher-kapic/ralph-rs/master/scripts/install.sh | bash
```

To install a specific version or to a custom directory:

```bash
# Specific version
curl -fsSL https://raw.githubusercontent.com/christopher-kapic/ralph-rs/master/scripts/install.sh | bash -s v0.2.0

# Custom directory
curl -fsSL https://raw.githubusercontent.com/christopher-kapic/ralph-rs/master/scripts/install.sh | INSTALL_DIR=~/.local/bin bash
```

Or build from source:

```bash
cargo install --path .
```

## Quick Start

```bash
# Initialize config and database
ralph init

# Create a plan
ralph plan create "Add user authentication" --test "cargo build" --test "cargo test"

# Add steps
ralph step add auth --title "Add user model" --description "Create User struct with id, email, password_hash fields" --criteria "User struct exists in src/models/user.rs" --criteria "Tests pass"
ralph step add auth --title "Add API endpoints" --description "Create login/register endpoints" --criteria "/api/login returns 200 with valid credentials"

# Approve and run
ralph approve auth
ralph run auth
```

## Usage

### Plan Management

```bash
ralph plan create <description>        # Create a new plan
  [--test <cmd>]...                       #   Repeatable: deterministic test commands
  [--harness <h>]                         #   Plan-level harness override
  [--agent <name>]                        #   Plan-level agent definition
  [--branch <name>]                       #   Custom branch name

ralph plan list [--all]                # List plans
ralph plan show <slug>                 # Show plan details
ralph plan approve <slug>              # Approve plan (planning -> ready)
ralph plan delete <slug>               # Delete a plan
```

### Step Management

```bash
ralph step list <slug>                 # List steps in a plan
ralph step add <slug>                  # Add a step
  --title <t> --description <d>
  [--criteria <c>]...                     #   Acceptance criteria
  [--agent <name>]                        #   Step-level agent override
  [--after <num>]                         #   Insert after step number

ralph step remove <slug> <num>         # Remove step by position
ralph step edit <slug> <num>           # Edit step fields
ralph step reset <slug> <num>          # Reset step to pending
ralph step move <slug> <from> <to>     # Reorder step
```

### Execution

```bash
ralph run [<slug>]                     # Run plan (interactive TUI)
ralph run [<slug>] --noninteractive    # Run plan (stdout progress)
ralph run [<slug>] --current-branch    # Run on current branch
ralph resume [<slug>]                  # Resume from last failed step
ralph skip [<slug>]                    # Skip failed step, continue
```

### Planning with a Harness

```bash
ralph plan:harness [<harness>] [<description>]   # Delegate planning to an AI harness
```

### Portability

```bash
ralph export <slug> [-o <file>]        # Export plan to JSON
ralph import <file>                    # Import plan from JSON
  [--project <path>]
  [--slug <name>]
  [--harness <h>]
```

### Utilities

```bash
ralph status [<slug>]                  # Show execution status
ralph log <slug> [<step-num>]          # Show execution logs
ralph agents                           # List agent definitions
ralph doctor                           # Check config, DB, harness availability
```

## Harness Comparison

Export a plan and run it with different harnesses to compare results:

```bash
# Create and export a plan
ralph plan:harness claude "Add user auth"
ralph export auth

# Import into separate project copies with different harnesses
ralph import auth.json --project ~/myapp-claude --slug auth-claude --harness claude
ralph import auth.json --project ~/myapp-codex --slug auth-codex --harness codex

# Run both
cd ~/myapp-claude && ralph run auth-claude --noninteractive &
cd ~/myapp-codex  && ralph run auth-codex  --noninteractive &
```

## Configuration

Config lives at `~/.config/ralph-rs/config.json` (Linux/macOS) with harness definitions, default harness, retry settings, and timeout configuration.

Agent definitions are markdown files in `~/.config/ralph-rs/agents/*.md`.

## Lifecycle Hooks

Ralph supports shell-based lifecycle hooks at four points during step execution: `pre-step`, `post-step`, `pre-test`, and `post-test`. Hooks are defined once in a reusable library at `~/.config/ralph-rs/hooks/*.md` (nothing in your working directory) and then attached to plans or individual steps via CLI:

```bash
ralph hooks add my-review --lifecycle post-step --command "claude -p 'review this'"
ralph plan set-hook my-feature --lifecycle post-step --hook my-review       # every step
ralph step set-hook 3 --plan my-feature --lifecycle post-step --hook my-review  # one step
ralph hooks export -o bundle.json        # share with teammates
ralph hooks import bundle.json
```

Hooks can be `global` or path-scoped to specific project prefixes. When you run `ralph plan:harness`, the plan agent is told which hooks are available and can attach them to steps it thinks deserve review.

For the full model (library layout, scope rules, sharing, worked examples for Claude Code / Codex / clippy), see [docs/review-hooks.md](docs/review-hooks.md).

## License

MIT
