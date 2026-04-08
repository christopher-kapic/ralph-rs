---
name: create-ralph
description: Create a ralph (a deterministic execution plan with steps) for a coding task. Use when the user wants to plan and break down a feature, bug fix, or refactor into executable steps that ralph-rs can run through a coding agent harness.
argument-hint: [description of what to build]
allowed-tools: Read Grep Glob Bash Agent
---

You are helping the user create a **ralph** — a structured, deterministic execution plan for ralph-rs. A ralph is a plan with ordered steps that ralph-rs will execute sequentially through a coding agent harness (Claude, Codex, etc.), validating each step with deterministic tests before moving on.

## Your workflow

1. **Understand the task**: Read `$ARGUMENTS`. If the description is vague, ask clarifying questions. If it's clear, proceed.

2. **Investigate the codebase**: Before planning, explore the relevant parts of the codebase to understand the current state — file structure, existing patterns, dependencies, and test infrastructure. This is critical for writing steps that are specific and actionable.

3. **Identify deterministic tests**: Figure out what shell commands can validate success. Common examples:
   - `cargo test` / `npm test` / `pytest`
   - `cargo clippy -- -D warnings`
   - `cargo build`
   - Custom test scripts
   The user may also specify tests — ask if unsure.

4. **Design the plan**: Break the task into sequential steps where:
   - Each step is a discrete, independently testable unit of work
   - Steps build on each other (earlier steps create foundations for later ones)
   - Each step is small enough for a coding agent to complete in one pass
   - Step descriptions are specific: mention exact files, functions, structs, and patterns to follow
   - Include acceptance criteria in each step description so the agent knows when it's done

5. **Present the plan to the user**: Show the plan with all steps before creating anything. Include:
   - Plan slug and description
   - Test command(s)
   - Each step with its title and description
   Wait for the user to approve or request changes.

6. **Create the plan using ralph-rs CLI**: Once approved, run the commands:

```bash
# Create the plan
ralph-rs plan create <slug> \
  --description "<plan description>" \
  --test "<test command 1>" \
  --test "<test command 2>"

# Add steps in order
ralph-rs step add "<Step 1 title>" \
  --plan <slug> \
  --description "<detailed description with acceptance criteria>"

ralph-rs step add "<Step 2 title>" \
  --plan <slug> \
  --description "<detailed description with acceptance criteria>"

# ... continue for all steps

# Approve the plan so it's ready to run
ralph-rs plan approve <slug>
```

## Guidelines for good steps

- **Be specific**: "Add a `UserService` struct in `src/services/user.rs` with methods `create`, `get_by_id`, and `delete`" is better than "Add user service"
- **Reference existing patterns**: "Follow the pattern used in `src/services/auth.rs`" helps the agent produce consistent code
- **Include acceptance criteria**: "The struct should derive `Debug`, `Clone`, and `Serialize`. Tests in `src/services/user_test.rs` should cover all three methods."
- **Keep steps focused**: One concern per step. Don't combine "add the model" and "add the API endpoint" into one step.
- **Order dependencies correctly**: Create types before using them, add modules before importing them
- **3-8 steps is typical**: Fewer than 3 usually means the steps are too coarse; more than 8 means they might be too granular

## Important

- Always run `ralph-rs init` first if this is a new project that hasn't been initialized
- Make sure the project directory is a git repo with a clean working tree before running
- The plan slug should be short, descriptive, and use hyphens (e.g., `add-auth`, `fix-parser-bug`, `refactor-db-layer`)
