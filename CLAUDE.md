# ralph-rs — Deterministic Execution Planner

A Rust CLI that orchestrates coding agent harnesses (Claude Code, Codex, OpenCode, Copilot, Goose, Pi) through step-based plans with test validation, git integration, and retry loops.

## Design Spec

The full design spec is in `ralph-rs-plan.md` at the project root. Read it before making changes.

## Tech Stack

- **Language:** Rust (edition 2024)
- **CLI:** clap v4 with derive macros
- **Database:** rusqlite with bundled feature (zero system deps)
- **Async:** tokio (subprocess management, signal handling, TUI)
- **TUI:** ratatui + crossterm (vim keybindings)
- **Serialization:** serde + serde_json, chrono (timestamps)
- **Platform dirs:** dirs crate (XDG-compliant)
- **Error handling:** anyhow
- **IDs:** uuid v4, fractional indexing for step ordering

## Project Structure

```
src/
  main.rs              — Entry point, clap CLI dispatch
  cli.rs               — Clap command/arg definitions
  commands/
    mod.rs             — Re-exports, shared helpers (resolve_project/step), init, doctor
    plan.rs            — Plan CRUD, dependency, plan-level hook commands
    step.rs            — Step CRUD, move, step-level hook commands
    run.rs             — Status and log commands
    agents.rs          — Agent file CRUD commands
    hooks.rs           — Hook library CRUD, export/import commands
  config.rs            — Config loading (~/.config/ralph-rs/config.json)
  db.rs                — SQLite connection, migrations
  plan.rs              — Plan/Step data models and enums
  storage.rs           — High-level CRUD operations
  runner.rs            — Plan-level orchestrator (step iteration, status transitions)
  executor.rs          — Single-step executor (harness spawn, test, git)
  harness.rs           — Harness subprocess management
  prompt.rs            — Prompt assembly (agent def, retry context, plan context, etc.)
  test_runner.rs       — Deterministic test execution (shell commands)
  git.rs               — Git CLI wrappers (branch, commit, diff, rollback)
  signal.rs            — Two-stage Ctrl+C handling (graceful then forceful)
  preflight.rs         — Pre-run environment validation
  frac_index.rs        — Base-62 fractional indexing for O(1) step reordering
  plan_harness.rs      — AI harness invocation for plan generation
  export.rs            — Plan export to portable JSON
  import.rs            — Plan import from JSON with override options
  output.rs            — Output formatting
  tui/
    mod.rs             — TUI module entry + tests
    app.rs             — App state (plan, steps, selection)
    ui.rs              — Ratatui rendering (layout, colors)
    input.rs           — Vim keybinding input handling
```

## Key Design Decisions

- **Deterministic-only:** No built-in LLM; plans created manually or via harness delegation
- **Multi-harness:** Pluggable harness support with different integration patterns
- **Git-integrated:** All steps are git commits; branches per plan; rollback on failure
- **SQLite storage** at platform-appropriate data dir
- **JSON config** at `~/.config/ralph-rs/config.json`
- **Signal-aware:** Two-stage Ctrl+C (graceful then forceful) via tokio watch channels
- **Fractional indexing:** O(1) step insertion without full reindex

## Querying Dependencies with kctx

This project's coding agents have access to **kctx** — a dependency knowledge service. Use the `mcp__kctx__query_dependency` and `mcp__kctx__list_dependencies` MCP tools to ask usage questions about external libraries.

## Build & Test

```bash
cargo build
cargo test
```

## Related Projects

- **kctx-local** (sibling at `../kctx-local/`) — Local-first Q&A CLI for codebases. Uses same Rust patterns.
- **mcp2cli-rs** (at `../../mcp2cli/mcp2cli-rs/`) — Universal CLI adapter for MCP, OpenAPI, GraphQL.
