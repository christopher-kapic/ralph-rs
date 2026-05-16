# ralph-rs — Deterministic Execution Planner

A Rust CLI that orchestrates coding agent harnesses (Claude Code, Codex, OpenCode, Copilot, Goose, Pi) through step-based plans with test validation, git integration, and retry loops.

## Design Spec

The TUI design spec is `TUI-plan.md` at the project root. **Note:** that document was written before implementation. Its prompt-layer model (§8/§11), questions storage (§15), and build-phase list still describe the *pre-overhaul* shape (per-plan `context_prepend`, global/project prefix-suffix pairs, `questions_enabled DEFAULT 0`); the prompt-overhaul branch superseded those — see "Prompt model" and "Key Design Decisions" below for the current four-layer model, retry strategy, and skip behavior. The narrative sections that the overhaul touched have been reconciled in `TUI-plan.md`, but the older keybinding tables and ASCII mocks were left as historical design notes. This file is the authoritative reference for the project's current state.

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
  db.rs                — SQLite connection, migrations (V1–V24)
  plan.rs              — Plan/Step/ExecutionLog models, enums (incl. RetryStrategy {Keep, Rollback})
  frac_index.rs        — Base-62 fractional indexing for O(1) step reordering
  storage.rs           — High-level CRUD operations (plans, steps, dependencies, hooks, locks, project prompt)
  harness.rs           — Harness resolution, subprocess spawning, output parsing
  prompt.rs            — Prompt construction (four-layer `Prompts`, retry context, plan context, hooks); DEFAULT_CONTEXT_PREPEND global-prompt seed
  executor.rs          — Single-step execution (spawn harness → test → commit; retry honors RetryStrategy; skip parks WIP)
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
    plan.rs            — Plan CRUD, dependency, plan-level hook, plan harness set/show, retry-strategy commands
    step.rs            — Step CRUD, move, edit (agent/harness/criteria/max-retries/retry-strategy), step-level hooks
    run.rs             — Status, log (incl. WIP-skip commits), skip (`--changes`) commands
    prompt.rs          — `ralph prompt set/clear/show` (global/project scope; `.ralph/prompt.md`-aware)
    question.rs        — `ralph question ask/list/answer` (per-plan pause-for-clarification)
    agents.rs          — Agent file CRUD commands
    hooks.rs           — Hook library CRUD, export/import commands
    harness.rs         — Read-only harness inspection (`ralph harness list/show`)
  tui/
    mod.rs             — TUI module entry
    view.rs            — `View` enum (PlanList, ArchivedList, PlanDetail, StepDetail)
    chrome.rs          — Persistent top breadcrumb + bottom hint/cwd/version bar
    theme.rs           — Color tokens (truecolor `Color::Rgb` constants)
    toast.rs           — Transient bottom-row message bar with TTL
    dialog.rs          — Confirm-dialog primitive (yes/no over a background view)
    choice.rs          — Generic single-select dialog primitive (vertical j/k/↑/↓ list, Enter/Esc)
    editor.rs          — `$EDITOR` handoff (round-trip text through a tempfile)
    events.rs          — NDJSON `RunEvent` subscription wiring (TUI → runner subprocess)
    help.rs            — `?` help overlay (per-view binding model + render)
    palette.rs         — `/` / `:` slash-command parser + tab completion
    palette_dispatch.rs — Maps parsed palette commands to per-view actions
    read_only.rs       — Read-only attach state when an external runner holds the lock
    run_dialog.rs      — `/run` branch-choice dialog (consumes `choice.rs`) + naming phase
    skip_dialog.rs     — `s` skip change-handling dialog (Stash/Commit/Discard via `choice.rs`; Esc = cancel-restart, no retry budget)
    selection.rs       — Multi-selection state (with `[N]` badge ordering)
    views/
      plan_list.rs     — Landing screen: tile per plan, sort by recency
      archived_list.rs — Same layout as plan_list but for archived plans
      plan_detail.rs   — Plan-detail view state
      plan_detail_input.rs — Pure key handler returning `InputAction`s
      plan_detail_ui.rs — Plan-detail rendering (step list + right pane)
      step_detail.rs   — Step-detail pane stack (four layers: Global/Project/Plan/Step prompts, etc.)
      step_detail_picker.rs — Bottom-row pickers (harness/model/agent/change_policy)
      rendered_prompt.rs — Read-only fully-assembled-prompt preview (`l`/`→` from StepPrompt pane; per-attempt nav)
      create_plan.rs   — Inline create-plan modal (slug → description → tests)
      answer_modal.rs  — `❓` answer modal + post-answer resume modal
      plan_dependencies.rs — Plan-dependency sub-view (List + Picker modes)
      plan_hooks.rs    — Plan-hook attachment sub-view
      step_hooks.rs    — Step-hook attachment sub-view
      step_tags.rs     — Step tag editor sub-view
```

## TUI architecture

The TUI is **multi-view** (plan list / archived list / plan detail /
step detail) with sub-views pushed on top for plan dependencies, plan
hooks, step hooks, step tags, and the rendered-prompt preview. Each view
is a self-contained `App` struct with pure state-machine methods, plus a
separate render function and a per-view input handler — splitting these
three lets us unit-test state transitions without spinning up a real
terminal.

The step-detail screen exposes the **four user-facing prompt layers** as
panes (`GlobalPrompt` / `ProjectPrompt` / `PlanPrompt` / `StepPrompt`) —
the pre-overhaul `PlanContextPrepend` / `PlanPrefix` / `PlanSuffix` panes
are gone. From the `StepPrompt` pane, `l`/`→` pushes the
**`RenderedPromptView`** sub-view (`src/tui/views/rendered_prompt.rs`): a
read-only preview of the fully-assembled prompt exactly as
`prompt::build_step_prompt` produces it, with `j`/`k` navigating between
per-attempt renders (each attempt re-assembled with the retry context the
executor would have built for it).

Mouse is supported in the list views: in plan_list / archived_list /
plan_detail's step list, a click selects the row, a second click on the
already-selected row enters it, and the scroll wheel moves the cursor.
The TUI still enables mouse capture (Shift-click bypasses it for native
text selection).

The dispatchers live in `src/commands/run.rs` (`run_plan_list_tui`,
`run_archived_list_tui`, `run_plan_detail_tui`, `run_step_detail_tui`,
`run_plan_dependencies_tui`, `run_rendered_prompt_tui`). They own the
alternate-screen / raw-mode session, the crossterm event loop, and any
DB/storage write-throughs.
Sub-view state machines expose a pure `handle_key(KeyEvent) -> Outcome`
method; the dispatcher executes the side effect and loops on `Pending`.

Routing into the TUI is conditional: `ralph` (no subcommand) and
`ralph run` with no non-default flags drop into the TUI. **Any
non-default flag** (`--one`, `--all`, `--harness`, `--json`, …) keeps
today's non-interactive behavior so scripts don't regress. The
`--non-interactive` flag and a non-TTY stdout both force the
non-interactive path.

Runtime communication between the TUI and a TUI-spawned runner is
NDJSON over the runner's stdout (same stream as `--json` / `--jsonl`).
See [docs/ndjson-events.md](docs/ndjson-events.md) for the schema.

The help overlay (`?`) toggles a centered modal listing the bindings of
the current view, grouped by category. Per-view binding models live in
`src/tui/help.rs`; each view's `App` carries a `HelpState` field whose
`intercept_key` is consulted before the view's normal input handler so
view bindings don't fire under the overlay.

## Key Design Decisions

- **Deterministic-only:** No built-in LLM; plans created manually or via harness delegation
- **Multi-harness:** Pluggable harness support with different integration patterns (native agent file, env var, prompt injection)
- **Git-integrated:** All steps are git commits; branches per plan
- **Retry strategy:** `RetryStrategy {Keep, Rollback}`, precedence step > plan > default `Keep`. `Keep` (the default) carries the dirty tree forward between failed attempts (the prior diff is on disk; the retry context omits it); `Rollback` reverts the tree before each retry and feeds the rolled-back diff into the next prompt
- **SQLite storage** at platform-appropriate data dir (`~/.local/share/ralph-rs/ralph.db` on Linux)
- **JSON config** at `~/.config/ralph-rs/config.json` (XDG semantics on all platforms)
- **Signal-aware:** Two-stage Ctrl+C (graceful then forceful) via tokio watch channels
- **Fractional indexing:** O(1) step insertion without full reindex
- **Run locks:** SQLite-based per-project lock prevents concurrent `ralph run` invocations; `--force` to recover stale locks
- **Hook system:** Reusable hooks in `~/.config/ralph-rs/hooks/*.md` with scope, export/import, and lifecycle attachment
- **NDJSON output:** `--json` flag streams structured events during runs; `--quiet` suppresses progress; `--no-color` and `NO_COLOR` respected. Includes an `attempt_cancelled` event (TUI skip-dialog Esc/cancel)
- **Skip overhaul:** `ralph skip --changes <stash|commit|discard>` (default `stash`) and a TUI Choice<T> skip dialog (Stash/Commit/Discard; Esc-cancel restarts the attempt consuming no retry budget) decide what happens to the killed harness's in-flight work. `commit` writes a `[ralph wip]` commit carrying a `Ralph-Skipped-Step: <id>` git trailer; `ralph log` surfaces those commits and `ralph step reset` reverts them (confirm / `--force`). A cross-process **skip bridge** (`plans.skip_requested_step_id` / `plans.skip_changes`, migration V23) lets the TUI/CLI skip a step running inside a separate spawned-runner process
- **Shell completions:** `ralph completions <shell>` generates bash/zsh/fish/elvish/powershell

## Prompt model

Four layers, assembled outermost → innermost by `prompt::build_step_prompt`
(`Prompts` struct in `src/prompt.rs`):

1. **Global** — `config.prompt` in `~/.config/ralph-rs/config.json`. Seeded
   with `DEFAULT_CONTEXT_PREPEND` (the ralph-CLI introspection hints) at
   `ralph init`; `ralph init --restore-prompts` re-seeds it unconditionally
   (overwriting customization); uncustomized legacy configs are reseeded on
   migration. `build_step_prompt` no longer auto-injects the prepend — the
   Global layer carries it, so editing the global prompt fully customizes it.
2. **Project** — `<project>/.ralph/prompt.md` (a file, if present) **wins
   over** the `project_settings.prompt` DB column. `ralph prompt
   set/clear/show --scope project` is file-vs-DB aware.
3. **Plan** — the plan's `description`, rendered once into the `# Plan:
   {slug}` context block. There is **no** per-plan prefix/suffix and no
   per-plan `context_prepend` (legacy columns dropped in migrations V21/V22).
4. **Step** — the step body (title / description / acceptance criteria).

There is no suffix concept; layers stack as prefix sections only.
`--scope universal` is a clap alias for `--scope global`. `ralph doctor`
emits a non-fatal warning when the global prompt lacks the ralph-CLI
hints, pointing the user at `ralph init --restore-prompts`.

## CLI Surface

```
ralph init [--non-interactive] [--default-harness <name>] [--force] [--restore-prompts]
ralph plan create <slug> [-d <desc>] [--test <cmd>]... [--harness <h>] [--agent <name>] [--branch <name>] [--depends-on <slug>]... [--retry-strategy <keep|rollback>]
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
ralph plan questions <on|off> [<slug>]
ralph plan harness set <harness> [<slug>]
ralph plan harness show [<slug>]
ralph plan harness generate [<description>] [<slug>] [--use-harness <h>]

ralph step list [<slug>]
ralph step add <title> [<slug>] [-d <desc>] [--after <num>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--max-retries <n>] [--retry-strategy <keep|rollback>] [--import-json <FILE|->]
ralph step remove <num>|--step-id <uuid> [<slug>] [--force/-y]
ralph step edit <num>|--step-id <uuid> [<slug>] [--title <t>] [--description <d>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--clear-criteria] [--max-retries <n>] [--clear-max-retries] [--retry-strategy <keep|rollback>] [--clear-retry-strategy]
ralph step reset <num>|--step-id <uuid> [<slug>]
ralph step move <num>|--step-id <uuid> --to <n> [<slug>]
ralph step set-hook <num>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>
ralph step unset-hook <num>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>

ralph run [<slug>] [--one/--single] [--all] [--from <n>] [--to <m>] [--dry-run] [--skip-preflight] [--current-branch] [--auto-stash] [--harness <h>] [--force]
ralph resume [<slug>]
ralph skip [<slug>] [--step <n>] [--reason <reason>] [--changes <stash|commit|discard>] [--force]
ralph step reset <num>|--step-id <uuid> [<slug>] [--force/-y]

ralph export <slug> [-o <file>]
ralph import <file> [--slug <name>] [--branch <name>] [--strict]

ralph status [<slug>] [--verbose/-v]
ralph log [<slug>] [--step <n>] [--limit <n>] [--full|--lines <n>]

ralph prompt show [--scope <global|project|universal>] [--resolved]
ralph prompt set --scope <global|project|universal> <content>
ralph prompt clear --scope <global|project|universal>

ralph question ask [<text>] [--suggest/-s <answer>]...
ralph question list [<slug>]
ralph question answer <num> [<text>]
ralph question show <num>

ralph agents list|show|create|delete
ralph hooks list|show|add|remove|export|import
ralph harness list [--json]
ralph harness show <name> [--json]
ralph doctor
ralph completions <shell>
```

Global flags: `--project <path>` (`-C`), `--harness <name>`, `--json`, `--quiet`, `--no-color`

## Plan-generation prompt parity

There are **two** documents that teach an AI agent how to author a ralph plan,
and they must stay in lockstep:

- `.claude/skills/create-ralph/SKILL.md` — the slash-command skill, used when a
  user runs `/create-ralph` inside Claude Code.
- `HARNESS_PLAN_AGENT_BASE` in `src/plan_harness.rs` — the system prompt sent
  to a coding harness spawned by `ralph plan harness generate`.

Both teach the same workflow, anti-patterns, and CLI surface. If you change
one, change the other in the same PR. Drift means the same user gets
materially worse plans depending on which entry point they use.

The harness prompt should not reference Claude-Code-specific things
(`$ARGUMENTS`, `allowed-tools`, frontmatter); the skill should not duplicate
the runtime hook-library injection that `render_plan_agent` does. Everything
else — preflight, recommended shape, authoring (`--import-json` warning),
review steps, anti-patterns, CLI flags — should match in substance.

## Build & Test

```bash
cargo build
cargo test
cargo clippy -- -D warnings
```

**Test footgun — ETXTBSY on freshly-written scripts:** Tests that write a shell script to a tempdir and then `Command::new(script).status()` it can intermittently fail in CI with `Text file busy (os error 26)`. Cause: cargo runs tests in parallel; another thread's spawned child can inherit a writable fd to the script across its fork→exec window, and Linux refuses `execve()` while any process holds the file open for write. **Fix:** invoke via `/bin/sh <path>` instead of exec'ing the script directly — `sh` opens it as a regular file and sidesteps the kernel's writer-check. See `sh_editor()` in `src/tui/editor.rs` for the pattern.

## Related Projects

- **kctx-local** (sibling at `../kctx-local/`) — Local-first Q&A CLI for codebases. Uses same Rust patterns.
- **mcp2cli-rs** (at `../../mcp2cli/mcp2cli-rs/`) — Universal CLI adapter for MCP, OpenAPI, GraphQL.