# ralph-rs — Deterministic Execution Planner

A Rust CLI that orchestrates coding agent harnesses (Claude Code, Codex, OpenCode, Copilot, Goose, Pi) through step-based plans with test validation, git integration, and retry loops.

## Design Spec

The full design spec is in `ralph-rs-plan.md` at the project root. **Note:** that document was written before implementation and some sections (CLI surface, module structure, defaults) have drifted from the current code. This file is the authoritative reference for the project's current state.

## Tech Stack

- **Language:** Rust (edition 2024)
- **CLI:** clap v4 with derive macros + clap_complete for shell completions
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
  main.rs              — Entry point, clap CLI dispatch, resolve_plan helper
  cli.rs               — Clap command/arg definitions (ValueEnum for Lifecycle, PlanStatus)
  config.rs            — JSON config loading (~/.config/ralph-rs/config.json), harness definitions
  db.rs                — SQLite connection, migrations (V1–V5)
  plan.rs              — Plan/Step/ExecutionLog data models and enums
  frac_index.rs        — Base-62 fractional indexing for O(1) step reordering
  storage.rs           — High-level CRUD operations (plans, steps, dependencies, hooks, locks)
  harness.rs           — Harness resolution, subprocess spawning, output parsing
  prompt.rs            — Prompt construction (agent def, retry context, plan context, hooks)
  executor.rs          — Single-step execution (spawn harness → test → commit/rollback)
  runner.rs            — Plan-level orchestrator (step iteration, status transitions, --all)
  run_lock.rs          — Per-project run lock to prevent concurrent runs
  signal.rs            — Two-stage Ctrl+C handling (graceful then forceful)
  test_runner.rs       — Deterministic test execution (shell commands)
  git.rs               — Git CLI wrappers (branch, commit, diff, rollback)
  hook_library.rs      — Hook library management (read/write hook markdown files)
  hooks.rs             — Hook execution engine (lifecycle hooks at pre/post-step, pre/post-test)
  plan_harness.rs      — AI harness invocation for plan generation (interactive)
  export.rs            — Plan export to portable JSON
  import.rs            — Plan import from JSON with override options
  preflight.rs         — Pre-run environment validation (harness auth, git dirty state, etc.)
  output.rs            — Output formatting (JSON, plain, color detection, NDJSON events)
  commands/
    mod.rs             — Re-exports, shared helpers (resolve_project/step, init, doctor, confirm)
    plan.rs            — Plan CRUD, dependency, plan-level hook, plan harness set/show commands
    step.rs            — Step CRUD, move, edit (with agent/harness/criteria/max-retries), step-level hooks
    run.rs             — Status and log commands
    agents.rs          — Agent file CRUD commands
    hooks.rs           — Hook library CRUD, export/import commands
  tui/
    mod.rs             — TUI module entry + tests
    app.rs             — App state (plan, steps, selection, input mode)
    ui.rs              — Ratatui rendering (layout, colors)
    input.rs           — Vim keybinding input handling (j/k, a, s, q, Ctrl+C)
```

## Key Design Decisions

- **Deterministic-only:** No built-in LLM; plans created manually or via harness delegation
- **Multi-harness:** Pluggable harness support with different integration patterns (native agent file, env var, prompt injection)
- **Git-integrated:** All steps are git commits; branches per plan; rollback on failure
- **SQLite storage** at platform-appropriate data dir (`~/.local/share/ralph-rs/ralph.db` on Linux)
- **JSON config** at `~/.config/ralph-rs/config.json` (XDG semantics on all platforms)
- **Signal-aware:** Two-stage Ctrl+C (graceful then forceful) via tokio watch channels
- **Fractional indexing:** O(1) step insertion without full reindex
- **Run locks:** SQLite-based per-project lock prevents concurrent `ralph run` invocations; `--force` to recover stale locks
- **Hook system:** Reusable hooks in `~/.config/ralph-rs/hooks/*.md` with scope, export/import, and lifecycle attachment
- **NDJSON output:** `--json` flag streams structured events during runs; `--quiet` suppresses progress; `--no-color` and `NO_COLOR` respected
- **Shell completions:** `ralph completions <shell>` generates bash/zsh/fish/elvish/powershell

## CLI Surface

```
ralph init [--non-interactive] [--default-harness <name>] [--force]
ralph plan create <slug> [-d <desc>] [--test <cmd>]... [--harness <h>] [--agent <name>] [--branch <name>] [--depends-on <slug>]...
ralph plan list [--all] [--status <status>] [--archived]
ralph plan show <slug>
ralph plan approve <slug>
ralph plan delete <slug> [--force/-y]
ralph plan archive <slug>
ralph plan unarchive <slug>
ralph plan set-hook <slug> --lifecycle <lifecycle> --hook <name>
ralph plan unset-hook <slug> --lifecycle <lifecycle> --hook <name>
ralph plan hooks <slug>
ralph plan dependency add <slug> --depends-on <slug>...
ralph plan dependency remove <slug> --depends-on <slug>...
ralph plan dependency list <slug>
ralph plan harness set <harness> [<slug>]
ralph plan harness show [<slug>]
ralph plan harness generate [<description>] [<slug>] [--use-harness <h>]

ralph step list [<slug>]
ralph step add <title> [<slug>] [-d <desc>] [--after <num>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--max-retries <n>] [--import-json <FILE|->]
ralph step remove <num>|--step-id <uuid> [<slug>] [--force/-y]
ralph step edit <num>|--step-id <uuid> [<slug>] [--title <t>] [--description <d>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--max-retries <n>] [--clear-max-retries]
ralph step reset <num>|--step-id <uuid> [<slug>]
ralph step move <num>|--step-id <uuid> --to <n> [<slug>]
ralph step set-hook <num>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>
ralph step unset-hook <num>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>

ralph run [<slug>] [--one/--single] [--all] [--from <n>] [--to <m>] [--dry-run] [--skip-preflight] [--current-branch] [--harness <h>] [--force]
ralph resume [<slug>]
ralph skip [<slug>] [--step <n>] [--reason <reason>]

ralph export <slug> [-o <file>]
ralph import <file> [--slug <name>] [--branch <name>] [--harness <h>]

ralph status [<slug>] [--verbose/-v]
ralph log [<slug>] [--step <n>] [--limit <n>] [--full|--lines <n>]

ralph agents list|show|create|delete
ralph hooks list|show|add|remove|export|import
ralph doctor
ralph completions <shell>
```

Global flags: `--project <path>` (`-C`), `--harness <name>`, `--json`, `--quiet`, `--no-color`

## Build & Test

```bash
cargo build
cargo test
cargo clippy -- -D warnings
```

## Related Projects

- **kctx-local** (sibling at `../kctx-local/`) — Local-first Q&A CLI for codebases. Uses same Rust patterns.
- **mcp2cli-rs** (at `../../mcp2cli/mcp2cli-rs/`) — Universal CLI adapter for MCP, OpenAPI, GraphQL.