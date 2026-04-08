<p align="center"><img src="header.png" alt="ralph-rs — Deterministic execution planner for coding agent harnesses" width="100%" /></p>

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
curl -fsSL https://raw.githubusercontent.com/device-ai/ralph-rs/master/scripts/install.sh | bash
```

To install a specific version or to a custom directory:

```bash
# Specific version
curl -fsSL https://raw.githubusercontent.com/device-ai/ralph-rs/master/scripts/install.sh | bash -s v0.2.0

# Custom directory
curl -fsSL https://raw.githubusercontent.com/device-ai/ralph-rs/master/scripts/install.sh | INSTALL_DIR=~/.local/bin bash
```

Or build from source:

```bash
cargo install --path .
```

## Quick Start

```bash
# Initialize config and database
ralph-rs init

# Create a plan
ralph-rs plan create "Add user authentication" --test "cargo build" --test "cargo test"

# Add steps
ralph-rs step add auth --title "Add user model" --description "Create User struct with id, email, password_hash fields" --criteria "User struct exists in src/models/user.rs" --criteria "Tests pass"
ralph-rs step add auth --title "Add API endpoints" --description "Create login/register endpoints" --criteria "/api/login returns 200 with valid credentials"

# Approve and run
ralph-rs approve auth
ralph-rs run auth
```

## Usage

### Plan Management

```bash
ralph-rs plan create <description>        # Create a new plan
  [--test <cmd>]...                       #   Repeatable: deterministic test commands
  [--harness <h>]                         #   Plan-level harness override
  [--agent <name>]                        #   Plan-level agent definition
  [--branch <name>]                       #   Custom branch name

ralph-rs plan list [--all]                # List plans
ralph-rs plan show <slug>                 # Show plan details
ralph-rs plan approve <slug>              # Approve plan (planning -> ready)
ralph-rs plan delete <slug>               # Delete a plan
```

### Step Management

```bash
ralph-rs step list <slug>                 # List steps in a plan
ralph-rs step add <slug>                  # Add a step
  --title <t> --description <d>
  [--criteria <c>]...                     #   Acceptance criteria
  [--agent <name>]                        #   Step-level agent override
  [--after <num>]                         #   Insert after step number

ralph-rs step remove <slug> <num>         # Remove step by position
ralph-rs step edit <slug> <num>           # Edit step fields
ralph-rs step reset <slug> <num>          # Reset step to pending
ralph-rs step move <slug> <from> <to>     # Reorder step
```

### Execution

```bash
ralph-rs run [<slug>]                     # Run plan (interactive TUI)
ralph-rs run [<slug>] --noninteractive    # Run plan (stdout progress)
ralph-rs run [<slug>] --current-branch    # Run on current branch
ralph-rs resume [<slug>]                  # Resume from last failed step
ralph-rs skip [<slug>]                    # Skip failed step, continue
```

### Planning with a Harness

```bash
ralph-rs plan:harness [<harness>] [<description>]   # Delegate planning to an AI harness
```

### Portability

```bash
ralph-rs export <slug> [-o <file>]        # Export plan to JSON
ralph-rs import <file>                    # Import plan from JSON
  [--project <path>]
  [--slug <name>]
  [--harness <h>]
```

### Utilities

```bash
ralph-rs status [<slug>]                  # Show execution status
ralph-rs log <slug> [<step-num>]          # Show execution logs
ralph-rs agents                           # List agent definitions
ralph-rs doctor                           # Check config, DB, harness availability
```

## Harness Comparison

Export a plan and run it with different harnesses to compare results:

```bash
# Create and export a plan
ralph-rs plan:harness claude "Add user auth"
ralph-rs export auth

# Import into separate project copies with different harnesses
ralph-rs import auth.json --project ~/myapp-claude --slug auth-claude --harness claude
ralph-rs import auth.json --project ~/myapp-codex --slug auth-codex --harness codex

# Run both
cd ~/myapp-claude && ralph-rs run auth-claude --noninteractive &
cd ~/myapp-codex  && ralph-rs run auth-codex  --noninteractive &
```

## Configuration

Config lives at `~/.config/ralph-rs/config.json` (Linux/macOS) with harness definitions, default harness, retry settings, and timeout configuration.

Agent definitions are markdown files in `~/.config/ralph-rs/agents/*.md`.

## License

MIT
