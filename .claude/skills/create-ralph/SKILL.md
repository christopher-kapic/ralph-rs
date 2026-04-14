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

6. **Create the plan using the `ralph` CLI**: Once approved, run the commands:

```bash
# Create the plan
ralph plan create <slug> \
  --description "<plan description>" \
  --test "<test command 1>" \
  --test "<test command 2>"

# Add steps in order. The plan slug is a positional argument after the
# step title (or omit it to use the active plan).
ralph step add "<Step 1 title>" <slug> \
  --description "<detailed description with acceptance criteria>"

ralph step add "<Step 2 title>" <slug> \
  --description "<detailed description with acceptance criteria>"

# ... continue for all steps

# Approve the plan so it's ready to run
ralph plan approve <slug>
```

## Guidelines for good steps

- **Be specific**: "Add a `UserService` struct in `src/services/user.rs` with methods `create`, `get_by_id`, and `delete`" is better than "Add user service"
- **Reference existing patterns**: "Follow the pattern used in `src/services/auth.rs`" helps the agent produce consistent code
- **Include acceptance criteria**: "The struct should derive `Debug`, `Clone`, and `Serialize`. Tests in `src/services/user_test.rs` should cover all three methods."
- **Keep steps focused**: One concern per step. Don't combine "add the model" and "add the API endpoint" into one step.
- **Order dependencies correctly**: Create types before using them, add modules before importing them
- **Size the plan to the scope, not a default**: Plans can reasonably range from ~3 steps (focused bugfix) to ~300 steps (greenfield service or multi-week refactor). Pick granularity based on the work, not a target count. Rough bands: bugfix 3–5, small feature 5–15, medium feature 15–40, large refactor or greenfield 40–300. Don't compress a big task into a handful of mega-steps — you lose the checkpointing, retry, and rollback that ralph gives you per step. Don't inflate a small task into many trivial steps either. Whatever the size, every step must still be atomic and independently verifiable.

## Important

- Always run `ralph init` first if this is a new project that hasn't been initialized (it will detect installed harnesses and prompt for a default; pass `--non-interactive` in scripted contexts)
- Make sure the project directory is a git repo with a clean working tree before running
- The plan slug should be short, descriptive, and use hyphens (e.g., `add-auth`, `fix-parser-bug`, `refactor-db-layer`)
