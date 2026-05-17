# ralph-rs — Deterministic Dependencies & Validation, Dynamic Scheduling, Optional Nondeterministic Review

A Rust CLI that orchestrates coding agent harnesses (Claude Code, Codex, OpenCode, Copilot, Goose, Pi) through **dependency-DAG plans** with test validation, git integration, retry loops, and an optional built-in step-by-step review pipeline.

**Determinism framing (post-DAG-redesign, §11).** ralph is no longer a flat
"Deterministic Execution Planner." The reproducibility promise is now
**per-step**: same inputs → same step behavior, and the scheduler's choice
among runnable steps is a deterministic `(topological depth, sort_key,
short_id)` tie-break — so a linear plan (and any plan, given identical human
answers) runs in the authored order, no script regressions. *Added* on top:
**dynamic scheduling** (when a branch blocks on a human, the order the
remaining branches run depends on human-answer timing) and **first-class
nondeterministic review** (a separate harness audits steps). The wall-clock
interleave of concurrently-running reviews is explicitly **not** part of the
reproducibility guarantee (§14.4).

## Design Spec

The DAG-redesign design document is `docs/dag-redesign.md`. It is the
authoritative spec for the dependency-DAG model, the interruption system,
the built-in review pipeline, the scheduler, and the TUI outline/inbox. The
"Important deliberate deviations" below record where the shipped code
intentionally differs from that draft.

The pre-DAG TUI design spec is `TUI-plan.md` at the project root. **Note:**
that document was written before implementation. Its prompt-layer model
(§8/§11), questions storage (§15), and build-phase list still describe the
*pre-overhaul* shape (per-plan `context_prepend`, global/project
prefix-suffix pairs, `questions_enabled DEFAULT 0`); the prompt-overhaul
branch superseded those, and the DAG redesign superseded the flat
step-list/question model on top of that — see "Prompt model", "Key Design
Decisions", and "DAG redesign — shipped shape" below for the current state.
The narrative sections that the overhauls touched have been reconciled in
`TUI-plan.md`, but the older keybinding tables and ASCII mocks were left as
historical design notes. This file is the authoritative reference for the
project's current state.

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
  db.rs                — SQLite connection, migrations (V1–V30)
  plan.rs              — Plan/Step/ExecutionLog models, enums (RetryStrategy {Keep, Rollback}; StepStatus incl. derived Blocked overlay; PlanStatus incl. derived Interrupted; ReviewStatus; Interruption domain model)
  frac_index.rs        — Base-62 fractional indexing for O(1) step reordering
  storage.rs           — High-level CRUD (plans, steps, step_dependencies + cycle check, short_id mint, interruptions CRUD, corrective-step request bridge, hooks, locks, project prompt)
  harness.rs           — Harness resolution, subprocess spawning, output parsing
  prompt.rs            — Step prompt construction (four-layer `Prompts`, bounded "Resolved interruptions" section, retry context, plan context, hooks); DEFAULT_CONTEXT_PREPEND global-prompt seed
  review.rs            — Built-in nondeterministic review pipeline: separate O(1) read-only reviewer prompt, spawnable detached review subprocess, orchestrator-only `finalize_review`, corrective-step request drain + re-parent, recursion-cap escalation
  executor.rs          — Single-step execution (spawn harness → per-iteration commit → test; retry honors RetryStrategy; skip parks WIP)
  runner.rs            — Plan-level orchestrator: the topological **scheduler** (runnable-set + `(depth, sort_key, short_id)` tie-break), impl semaphore=1, detached-review JoinSet drained at scheduler ticks, sole DB writer, status transitions, --all
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
    run.rs             — Status, log (incl. WIP-skip + per-iteration commits w/ git-note verdict), skip (`--changes`) commands; TUI dispatchers (`run_inbox_tui`, …)
    prompt.rs          — `ralph prompt set/clear/show` (global/project scope; `.ralph/prompt.md`-aware)
    question.rs        — `ralph question ask --priority` / `ralph block` (harness raises an interruption); legacy `question list/answer` thin aliases over `interruption`
    interruption.rs    — `ralph interruption list/show/resolve` (human-side resolve of questions + blockers)
    config_cmd.rs      — `ralph config show/set-timezone`, `ralph config review set` (global review block)
    agents.rs          — Agent file CRUD commands
    hooks.rs           — Hook library CRUD, export/import commands
    harness.rs         — Read-only harness inspection (`ralph harness list/show`)
  tui/
    mod.rs             — TUI module entry
    view.rs            — `View` enum (PlanList, ArchivedList, PlanDetail, StepDetail, **Inbox**)
    outline.rs         — Pure DAG-outline projection (topological depth indent, join `deps:` by short_id, `↳ corrects` marker); shares the runner's `step_schedule_cmp` so outline order == execution order; `z`/`Z` focus (downstream-dependents cone) is a pure view transform
    chrome.rs          — Persistent top breadcrumb (incl. focus path) + bottom hint/cwd/version bar
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
      plan_detail.rs   — Plan-detail view state (drives the DAG outline; `z`/`Z` focus, `I` to inbox)
      plan_detail_input.rs — Pure key handler returning `InputAction`s
      plan_detail_ui.rs — Plan-detail rendering (right pane + the DAG outline via `outline_view.rs`)
      outline_view.rs  — DAG outline render that **replaces the flat step list** in plan_detail (depth indent, `deps:`, `↳ corrects <short_id>`, derived `Blocked` overlay, review badges); pure state machine + render split; mouse path resolves through `outline.visible_rows()`
      inbox.rs         — `View::Inbox` cross-branch interruptions inbox state (open questions + blockers; run-through auto-advance; resolved items kept dimmed)
      inbox_ui.rs      — Inbox rendering
      step_detail.rs   — Step-detail pane stack (four layers: Global/Project/Plan/Step prompts, etc.)
      step_detail_picker.rs — Bottom-row pickers (harness/model/agent/change_policy)
      rendered_prompt.rs — Read-only fully-assembled-prompt preview (`l`/`→` from StepPrompt pane; per-attempt nav)
      create_plan.rs   — Inline create-plan modal (slug → description → tests)
      answer_modal.rs  — Legacy `❓` answer modal + post-answer resume modal (kept intact) **plus** the new `InterruptionModal` (ranked proposed answers with the agent's #1 pre-selected + freeform escape hatch + optional comment; blocker variant = resolve / resolve-with-comment, no options; deliberately no "let the agent decide" shortcut)
      plan_dependencies.rs — Plan-dependency sub-view (List + Picker modes)
      plan_hooks.rs    — Plan-hook attachment sub-view
      step_hooks.rs    — Step-hook attachment sub-view
      step_tags.rs     — Step tag editor sub-view
```

## TUI architecture

The TUI is **multi-view** (plan list / archived list / plan detail /
step detail / **inbox**) with sub-views pushed on top for plan
dependencies, plan hooks, step hooks, step tags, and the rendered-prompt
preview. Each view is a self-contained `App` struct with pure
state-machine methods, plus a separate render function and a per-view
input handler — splitting these three lets us unit-test state
transitions without spinning up a real terminal.

**DAG outline (replaces the flat step list, §12.1).** Plan-detail no
longer renders a flat `self.steps` list; it renders the **DAG outline**
(`src/tui/outline.rs` projection + `views/outline_view.rs` render):
topologically ordered, indented by depth, each join step listing its
dependencies inline (`deps: …`) by short_id, reviewer-inserted steps
marked `↳ corrects <short_id>`. The outline shares the runner's
`step_schedule_cmp`, so the drawn order is exactly the execution order.
`Blocked` is a **derived overlay** (an open interruption), rendered like
the derived `Interrupted` plan status — never persisted. Both the
keyboard and the mouse path resolve through `outline.visible_rows()`
(click selects / second-click enters / scroll moves the outline cursor)
so they share one index space on a non-linear or focused DAG.

**Focus / re-root (§12.2).** `z` on a step re-roots the outline at that
step's **downstream dependents cone** (only it and what flows out of it;
upstream context lives in the breadcrumb chrome); `Z`/`Esc` pops back
toward the true root(s). Focus nests and is a **pure view transform** —
no DB writes, no scheduler effect; scheduling still spans the whole DAG.

**Interruptions inbox (`View::Inbox`, §12.3).** A cross-branch list of
every open question/blocker, decoupled from DAG navigation, reachable
from anywhere via **`I`** (Shift-i; lowercase `i` is a pre-existing
"insert/create" binding so the inbox deliberately uses `I`) with an
open-count badge. Submitting an answer auto-advances to the next open
interruption (run-through; `Esc` exits); resolved items stay dimmed for
context. The ranked-answer UI is the new `InterruptionModal` in
`answer_modal.rs` (the legacy `AnswerModal` is kept intact alongside
it). Palette adds `/inbox` and `/focus`.

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
`run_plan_dependencies_tui`, `run_rendered_prompt_tui`, `run_inbox_tui`).
They own the alternate-screen / raw-mode session, the crossterm event
loop, and any DB/storage write-throughs.
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
- **NDJSON output:** `--json` flag streams structured events during runs; `--quiet` suppresses progress; `--no-color` and `NO_COLOR` respected. The DAG redesign adds `review_started`, `review_finished`, `corrective_step_requested`, `corrective_step_inserted`, `review_loop_escalated`, `paused_by_user`, and `summary` (alongside the existing `attempt_cancelled`); see `docs/ndjson-events.md`. **There is no `interruption_raised`/`interruption_resolved` NDJSON event** — interruptions surface via DB state (the derived `Blocked` overlay / the `run_locks` bridge), not a dedicated event variant
- **Skip overhaul:** `ralph skip --changes <stash|commit|discard>` (default `stash`) and a TUI Choice<T> skip dialog (Stash/Commit/Discard; Esc-cancel restarts the attempt consuming no retry budget) decide what happens to the killed harness's in-flight work. `commit` writes a `[ralph wip]` commit carrying a `Ralph-Skipped-Step: <id>` git trailer; `ralph log` surfaces those commits and `ralph step reset` reverts them (confirm / `--force`). A cross-process **skip bridge** (`plans.skip_requested_step_id` / `plans.skip_changes`, migration V23) lets the TUI/CLI skip a step running inside a separate spawned-runner process
- **Shell completions:** `ralph completions <shell>` generates bash/zsh/fish/elvish/powershell

### DAG redesign — shipped shape (deliberate deviations from `docs/dag-redesign.md`)

- **Plan = dependency DAG of steps.** Every step has a stable plan-unique
  8-char `short_id` (the user-facing handle; the internal UUID is
  unchanged) and `step_dependencies` edges (a structural clone of
  `plan_dependencies`, with `would_create_step_cycle`). Roots = steps with
  no deps. The V25 backfill turns every existing linear plan into a
  degenerate chain DAG that executes identically. Import mirrors the same
  backfill for legacy bundles (classification is by `short_id` presence).
- **Topological scheduler + deterministic tie-break.** A single dynamic
  scheduler (in `runner.rs`) replaces the linear iterator. It computes the
  runnable set (every dep `Complete` and its review returned; not
  `Blocked`; not terminal) and picks by `(topological depth, sort_key,
  short_id)` (`step_schedule_cmp`). With no edges every depth is 0 and
  this is byte-identical to the old "earliest actionable by sort_key" —
  linear plans don't regress.
- **Unified interruptions + `run_locks` cross-process bridge.** Questions
  and blockers are one entity/table/state-machine (`interruptions`, V26,
  supersedes `step_questions`). A harness raises one via `ralph question
  ask --priority` / `ralph block`; it **binds to the live run via the
  `run_locks` table** (`get_live_run` reads `run_locks.step_id`) — there
  is **no `RALPH_STEP_ID` env var** for this (the `RALPH_STEP_ID` env var
  exists only for lifecycle hooks, unrelated). An open interruption
  consumes **no retry budget**; the scheduler moves to another branch.
  `PlanStatus::Question` became the broader **derived** `Interrupted`;
  `StepStatus::Blocked` is a **derived overlay** (never persisted, clears
  on resolution).
- **Built-in review pipeline.** Off by default; effective =
  `step.review_enabled ?? plan.review_enabled ?? config.review.enabled ??
  false` (V27 nullable columns; precedence step > plan > global, mirrors
  `RetryStrategy`). The reviewer prompt is **separately assembled** (not
  `build_step_prompt`), O(1): plan/step context + a **single** `git show
  <sha>` diff (Decision 5 — no dependency diffs). Concurrency model:
  reviews run as a **detached task** (a tokio `JoinSet`); the
  orchestrator's single scheduler loop is the **sole DB writer** and
  drains finished reviews at scheduler ticks; an **implementation
  semaphore of 1** serializes implement+test+commit; the reviewer runs
  **read-only in a throwaway, isolated `git worktree`** pinned at the
  reviewed SHA (RAII `git::ReviewWorktree`, `Drop` cleanup) so it
  physically cannot touch the live tree (ancestry guard kept as
  defense-in-depth).
- **No `StepStatus::AwaitingReview` variant.** A review-gated step stays
  `InProgress` — gating is **structural** (the re-parented edge to the
  corrective step / deps-not-satisfied), per §3.3/§10, not status-based.
  A separate per-step `review_status` (`Pending | InFlight | Passed |
  Failed | Skipped | Disabled`) tracks the verdict.
- **Per-iteration commits + git-note verdict.** A commit happens **per
  iteration, before the deterministic test**: subject `ralph
  <short_id>.<n> - <title>` + trailers `Ralph-Plan` / `Ralph-Step` /
  `Ralph-Iteration` / `Ralph-Review: pending`. The review verdict is
  recorded as a **git note on `refs/notes/ralph-review`**, **not** by
  amending the commit (`git::annotate_review_verdict`) — history/tree-safe
  under concurrency. `ralph log` groups iteration commits by short_id and
  surfaces the note verdict; `ralph step reset` reverts a step's iteration
  commits via trailers. `RetryStrategy` reinterpreted: `Keep` accumulates
  iteration commits; `Rollback` git-reverts the prior iteration and feeds
  the rolled-back diff into the next prompt.
- **Corrective re-parenting + recursion cap (§10).** A failed review
  *requests* (never performs) a corrective step via an NDJSON event +
  the V29 `corrective_step_requests` bridge row. The orchestrator (sole
  writer) drains it: inserts `A′` (`corrects_step_id = A`, `A′ depends_on
  A`), **re-parents** every former dependent of `A` onto `A′`, then `A`
  → `Complete` with `review_status = Failed`. The
  review→correction→review chain is bounded by a per-plan
  `max_review_corrections` (V30, default `DEFAULT_MAX_REVIEW_CORRECTIONS`
  = 3); exceeding it raises a `kind=blocker` interruption ("review loop —
  needs human") instead of spawning forever.
- **§9 concurrency invariants** (hard): (1) one implementation slot
  (semaphore=1); (2) reviews strictly read-only w.r.t. the working tree
  (isolated worktree at a fixed SHA); (3) single DAG writer (only the
  orchestrator mutates the DAG; reviewers only *request*); (4)
  cross-process interruption bridge via `run_locks` (reviews never take
  the run-lock).
- **§14.1 resolved:** every per-iteration commit is **kept by default**
  (full audit trail); opt-in per-plan `--squash-on-complete` (V28)
  collapses a step's iteration commits into one when it reaches
  `Complete`. **§14.4:** scheduler reproducibility is timing-independent
  given identical human inputs; the wall-clock interleave of concurrent
  reviews is **not** part of the guarantee.
- **Export/import** carry the DAG: `ExportedStep` gains `short_id`
  (always emitted) and `depends_on: Vec<short_id>`; plan+step
  `review_enabled`, `squash_on_complete`, and `max_review_corrections`
  round-trip via the `retry_strategy` `skip_serializing_if` pattern.
  Runtime state (interruptions, `review_status`, attempts, iteration
  commits, `corrects_step_id` provenance) is not exported. Import
  validates the imported edge set (no dangling edges, unique short_ids,
  acyclic, ≥1 root) before any write; `--strict` rejects a review-on
  bundle when the target machine has no review harness.
- **Schema/version:** migrations now run through **V30**; `Cargo.toml`
  is **0.1.21**.

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
   per-plan `context_prepend` (legacy per-plan columns dropped in
   migration V21; the project-scope prefix/suffix pair was collapsed into
   `project_settings.prompt` in V22).
4. **Step** — the step body (title / description / acceptance criteria).

There is no suffix concept; layers stack as prefix sections only.
`--scope universal` is a clap alias for `--scope global`. `ralph doctor`
emits a non-fatal warning when the global prompt lacks the ralph-CLI
hints, pointing the user at `ralph init --restore-prompts`; it also warns
non-fatally when `review_enabled` is set but no/invalid review harness is
configured.

**DAG-redesign prompt deltas:**

- The old unbounded **"Previously answered questions"** section became
  **"Resolved interruptions"** and is now **bounded** (closes the §4
  context-growth leak): the last *N* resolved interruptions for the step,
  each body/resolution/comment run through `truncate_text`. The chosen
  answer/resolution **and** the human comment both flow into this bounded
  injection.
- The **reviewer prompt** is a *separate*, independently-assembled prompt
  (not `build_step_prompt`, `review::build_review_prompt`): plan/step
  context + the **single** `git show <sha>` diff — **O(1) in plan size**.
  No dependency diffs are ever injected (**Decision 5** preserved — the
  DAG is a scheduling/eligibility construct, not a prompt-context-growth
  construct).

## CLI Surface

```
ralph init [--non-interactive] [--default-harness <name>] [--force] [--restore-prompts]
ralph plan create <slug> [-d <desc>] [--test <cmd>]... [--harness <h>] [--agent <name>] [--branch <name>] [--depends-on <slug>]... [--retry-strategy <keep|rollback>] [--squash-on-complete] [--max-review-corrections <n>]
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
ralph plan review <on|off> <slug>            # per-plan review toggle (precedence step > plan > config > false)
ralph plan harness set <harness> [<slug>]
ralph plan harness show [<slug>]
ralph plan harness generate [<description>] [<slug>] [--use-harness <h>]

# Every <num> step selector ALSO accepts an 8-char short_id (DAG handle).
ralph step list [<slug>]
ralph step add <title> [<slug>] [-d <desc>] [--after <num>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--max-retries <n>] [--retry-strategy <keep|rollback>] [--depends-on <short_id|num>]... [--import-json <FILE|->]
ralph step remove <num|short_id>|--step-id <uuid> [<slug>] [--force/-y]
ralph step edit <num|short_id>|--step-id <uuid> [<slug>] [--title <t>] [--description <d>] [--agent <name>] [--harness <h>] [--criteria <c>]... [--clear-criteria] [--max-retries <n>] [--clear-max-retries] [--retry-strategy <keep|rollback>] [--clear-retry-strategy] [--review <on|off|inherit>]
ralph step reset <num|short_id>|--step-id <uuid> [<slug>] [--force/-y]
ralph step move <num|short_id>|--step-id <uuid> --to <n> [<slug>]
ralph step set-hook <num|short_id>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>
ralph step unset-hook <num|short_id>|--step-id <uuid> [<slug>] --lifecycle <lifecycle> --hook <name>
ralph step dependency add <num|short_id> --depends-on <short_id|num>...
ralph step dependency remove <num|short_id> --depends-on <short_id|num>...
ralph step dependency list <num|short_id>

ralph run [<slug>] [--one/--single] [--all] [--from <n>] [--to <m>] [--dry-run] [--skip-preflight] [--current-branch] [--auto-stash] [--harness <h>] [--force]
ralph resume [<slug>]
ralph skip [<slug>] [--step <n>] [--reason <reason>] [--changes <stash|commit|discard>] [--force]

# Harness raises an interruption mid-step (binds to the live run via the
# run_locks table; consumes NO retry budget); human resolves it CLI-side.
ralph question ask [<text>] [--suggest/-s <answer>]... [--priority <n>]...
ralph block [<text>]
ralph interruption list [<slug>]
ralph interruption show <id|index>
ralph interruption resolve <id|index> [--option <k>] [--answer <text>] [--comment <text>]
ralph question list [<slug>]                  # legacy thin alias over `interruption` (one release)
ralph question answer <num> [<text>]          # legacy thin alias

ralph export <slug> [-o <file>]
ralph import <file> [--slug <name>] [--branch <name>] [--strict]

ralph status [<slug>] [--verbose/-v]
ralph log [<slug>] [--step <n>] [--limit <n>] [--full|--lines <n>]

ralph prompt show [--scope <global|project|universal>] [--resolved]
ralph prompt set --scope <global|project|universal> <content>
ralph prompt clear --scope <global|project|universal>

ralph config show
ralph config set-timezone <tz>
ralph config review set [--harness <h>] [--model <m>] [--enabled <bool>]

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