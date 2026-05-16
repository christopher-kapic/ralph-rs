# ralph-rs Interactive TUI — Design Plan

Status: design draft, not yet implemented. This document captures the target UX
for ralph's full interactive mode and the related changes to the
non-interactive `ralph run` surface. The existing TUI in `src/tui/` is
single-plan and step-list-only; this plan supersedes it.

---

## 1. Goals & non-goals

**Goals**

- Make `ralph` (no subcommand) and `ralph run` (with no flags) drop the user
  into a vim-flavored TUI modeled on lazygit: discoverable to arrow-key
  users, fast for vim users.
- Surface the most common plan-level operations (run, archive, create,
  navigate, edit prompts, answer questions) inside the TUI. Less common
  operations remain CLI-only in v1 — see the v1 non-goals below.
- Let users navigate the prompt hierarchy (universal → project → plan → step
  + appended retry context) and edit any layer in `$EDITOR`.
- Keep deterministic, scriptable output untouched: when stdout is not a TTY,
  `--non-interactive` is passed, **or any non-default flag is passed to
  `ralph run`**, behavior matches today's runner. Add `--jsonl` as an alias
  of `--json` for meta-harnesses (§10).

**v1 non-goals (deferred, not abandoned)**

These are deliberately **left out of the v1 TUI** to keep scope coherent.
They all have working CLI surfaces today; v2 of the TUI absorbs them.

- Plan dependencies (`plan dependency add/remove/list`).
- Plan hook attachment (`plan set-hook/unset-hook/hooks`).
- Step hook attachment.
- Step tag editor (`step edit --tags ...`).
- Full mouse support (keyboard-first, like lazygit). **Prompt-overhaul
  update:** the list views now support click-to-select / click-again-to-
  enter and scroll-wheel cursor movement; everything else stays keyboard.
- Replacing scriptable commands (`plan show --json`, `status --json`, etc.) —
  the TUI is additive.
- Any LLM functionality inside the TUI itself; the only AI invocations are
  the existing harness / plan-harness subprocess paths.

**v1-included CLI parity items**

These exist in the CLI today and ARE absorbed into the v1 TUI because
they're cheap to implement as keybindings or palette commands:

- Plan approve (`A` keybinding on plan tile + `/plan approve`).
- Step move (Shift-`J` / Shift-`K` in plan detail to nudge selected
  step down/up; `/step move <num> --to <m>` in palette).
- Step reset (`r` on a step in plan detail).
- Prompt editing (Global / Project / Plan panes in step detail; the
  prompt-overhaul collapsed the old per-plan `context_prepend` and
  prefix/suffix panes into one blob per scope).
- Per-step `change_policy` toggle (4th cell in step-detail's bottom row,
  Required ↔ Optional).
- Per-step `model` / `agent` / `harness` overrides (existing bottom-row
  cells).
- Plan harness selection for AI-assisted plan generation (`/plan harness`
  in palette → existing `plan harness generate`).

> **Note (revised after planning):** the ralph plan generated from this
> document (`tui-v1`) pulls the v1 non-goals back in as steps 33–36 (plan
> dependencies sub-view, plan hook attachment sub-view, step hook
> attachment sub-view, step tag editor sub-view). The "v1 non-goals"
> framing above is preserved as design intent; the implementation plan
> covers everything.

---

## 2. Mode matrix for `ralph run`

| Invocation                              | Behavior                                                                                     |
| --------------------------------------- | -------------------------------------------------------------------------------------------- |
| `ralph` (no args)                       | Launches TUI at the **plan list** view.                                                      |
| `ralph run`                             | Launches TUI at the **plan detail** view of the active plan; auto-starts the run.            |
| `ralph run <slug>`                      | Same, for a specific plan slug.                                                              |
| `ralph run` with **any non-default flag** (`--one`, `--all`, `--from`, `--to`, `--dry-run`, `--skip-preflight`, `--no-auto-stash`, `--current-branch`, `--harness`, `--force`, `--verbose`) | **Stays non-interactive.** Today's runner. Zero behavioral regression for scripts. |
| `ralph run --non-interactive`           | Forces non-interactive even with no other flags.                                             |
| `ralph run --jsonl`                     | Alias for `--json`; implies `--non-interactive`. See §10.                                    |

Auto-detection: if stdout is not a TTY, the TUI is skipped
(`--non-interactive` is implied). An explicit `--non-interactive` flag is
added for the case where the user wants to force scripted output from a TTY
(e.g., piping into `tee`).

A new `--tui` flag is **not** added; the default for TTY + bare invocation is
interactive.

The "any flag drops to non-interactive" rule is deliberate: it preserves
every existing scripted use of `ralph run` exactly as it works today.
Interactivity is reserved for the bare invocation, which today is rare and
opens space for the TUI to claim it without a regression.

---

## 3. Information architecture

Four screens, one modal layer:

```
                      ┌──────────────────────────────┐
   `ralph`           ─┤ 1. Plan list                  │
                      │   ↓ enter                     │
                      ├──────────────────────────────┤
                      │ 2. Archived plan list         │  (entered from "Archived" tile)
                      └──────────────────────────────┘

   `ralph run`       ─►┌──────────────────────────────┐
                       │ 3. Plan detail (step list)    │
                       │   ↓ enter                     │
                       ├──────────────────────────────┤
                       │ 4. Step detail (prompt focus) │
                       └──────────────────────────────┘
```

Modal layer (orthogonal to view): command palette (`:`/`/`), confirm dialogs,
toast bar, and the `c`-triggered editor handoff.

---

## 4. Persistent chrome

Drawn on every screen.

- **Bottom-right corner**: `"<cwd>  ralph v<version>"`. `<cwd>` is the project
  the TUI is operating against (resolved via `--project` or `pwd`), shortened
  with `~` substitution. Truncates from the left with `…` if the line would
  exceed terminal width.
- **Bottom bar (left)**: hint line that changes per view ("[j/k] nav  [enter]
  open  [space] select  [d] archive  [/ or :] cmd  [q] quit").
- **Bottom bar (center)**: toast slot for transient messages (errors, "step
  added", "no $EDITOR set", etc.). 3-second timeout, dismissible with `<esc>`.
- **Top bar**: breadcrumb of the current view (e.g.
  `ralph › my-plan › step 3: "Add migration"`).

> **Mouse capture & native selection.** The TUI enables mouse capture so views
> can route `Event::Mouse` to per-view drag handlers; this suppresses the
> terminal's native click-drag text selection. Most terminals let users hold
> **Shift** while clicking/dragging to bypass program mouse capture and select
> text natively (and use the system clipboard).

---

## 5. View 1 — Plan list (`ralph` with no args)

### Layout

Two-panel horizontal split, mirroring plan-detail (§7):

- **Left pane** (~40% width): scrollable column of plan tiles.
- **Right pane** (~60% width): preview of the **highlighted plan's** step
  list — identical compact rows to plan-detail's left sidebar
  (`<num> <glyph> <title>`), rendered via a shared `step_list_widget`
  extracted from `plan_detail_ui`. Read-only on this screen; pressing
  `enter`/`→`/`l` pushes plan-detail where the same widget becomes the
  primary navigation column.

When the cursor is on the **Archived tile**, the right pane is blank
(no placeholder text — leaving it empty avoids visual noise).

### Plan tile

Width fills the available column. Height: 6 rows including borders.

```
┌───────────────────────────────────────────┐
│ Plan Name                                 │
│                                           │
│ Ran/Created Mon DD at HH:MM AM/PM         │
│                                           │
│ ● 3/7                                     │
└───────────────────────────────────────────┘
```

- Title line: `plan.slug`. Truncate with `…` if too wide.
- Timestamp line: literally `"Ran <date>"` if any execution log exists for the
  plan, else `"Created <date>"`. Format: `Mon DD at HH:MM AM/PM` rendered in
  `Config.display_timezone`.
- Status line: a colored dot (`●`) followed by `"<completed>/<total>"`.
- Border style:
  - Default: dimmed gray.
  - Highlighted (cursor): solid `#f7d135`.
  - Selected (multi-select): solid `#56d0d9` + small `[N]` selection-order
    badge in the top-right corner of the box.
  - Highlighted **and** selected: `#56d0d9` border with `#f7d135`-tinted title
    line (so the user can see both states without ambiguity).
- Corner badges:
  - **Top-right**: `[N]` selection-order badge when multi-selected (above).
  - **Top-left**: small `?` glyph when `plan.questions_enabled = true`.
    Absence means the toggle is off. Distinct from the purple status
    dot in §17, which signals an *unanswered* question is currently
    blocking the plan — both can render simultaneously.

### Sort order

Most recent first. Sort key: `MAX(execution_log.started_at) per plan`, fall
back to `plan.created_at` when no logs exist. Recompute on every refresh.
Excludes archived plans.

### Status dot legend

| Plan state                               | Color    | Hex / ratatui token |
| ---------------------------------------- | -------- | -------------------- |
| `complete`                               | green    | `#34d058`            |
| `in_progress`                            | yellow   | `#f7d135`            |
| `ready` / `planning` (never run)         | blue     | `#3b82f6`            |
| `failed` / `aborted`                     | red      | `#ef4444`            |
| `question` (harness paused for input)    | purple   | `#a855f7`            |

`PlanStatus::Question` is added in V16 alongside the questions feature
(§17). It's a *derived* status: a plan reports `Question` whenever any
unanswered `step_questions` row exists for one of its steps, regardless of
the underlying `plans.status` column. Un-shadows automatically when the
user answers.

### Archived tile

If any archived plans exist, render a single condensed tile **at the bottom
of the list** with a red border:

```
┌───────────────────────────────────────────┐
│ Archived (12)                             │
│ Press → / l / enter to view               │
└───────────────────────────────────────────┘
```

Entering this tile pushes the **archived plan list** view (§6).

### Keybindings (Plan list)

| Key                              | Action                                                                                      |
| -------------------------------- | ------------------------------------------------------------------------------------------- |
| `j` / `↓`                        | Next plan                                                                                   |
| `k` / `↑`                        | Previous plan                                                                               |
| `g` / `G`                        | Top / bottom (vim-style)                                                                    |
| `enter` / `→` / `l`              | Open highlighted plan (push **plan detail** view)                                           |
| `space`                          | Toggle selection on highlighted plan; selection order preserved (1, 2, 3, …)                |
| `d`                              | Archive (selection wins; if no selection, target the cursor)                                |
| `i` / `a`                        | Create new plan (push inline prompt → `plan create` flow)                                   |
| `A`                              | Approve highlighted plan (`plans.status` → `ready`)                                         |
| `Q`                              | Toggle `questions_enabled` on highlighted plan                                              |
| `/` or `:`                       | Open command palette (§9)                                                                   |
| `r`                              | Refresh from DB (also auto-refreshed on focus return)                                       |
| `?`                              | Help overlay listing all bindings                                                           |
| `q` / `<esc>` (when no selection)| Quit TUI                                                                                    |
| `<esc>` (when selection exists)  | Clear all selections                                                                        |

### Create-plan flow (`i` / `a`)

A modal prompts for slug, then description, then test commands (each `<enter>`
moves to next field, `<tab>` switches focus). On submit, the plan is created
with the global default harness, and the cursor jumps to it in the list.

`<esc>` cancels at any field. This intentionally does **not** open the plan
harness — that's reserved for `/plan harness` (see §9).

---

## 6. View 2 — Archived plan list

Identical layout to plan list (§5), but the source set is
`PlanStatus = Archived`. Header reads `Archived plans`.

### Keybindings (Archived list)

Same as plan list with these overrides:

| Key                  | Action                                                                                  |
| -------------------- | --------------------------------------------------------------------------------------- |
| `enter` / `→` / `l`  | Unarchive selection (or highlighted if no selection); cursor follows the unarchived plan back to the main list when the user returns there |
| `d`                  | **Permanently delete** (selection-aware, with confirm dialog) — destructive             |
| `←` / `h` / `q`      | Pop back to main plan list                                                              |
| `space`              | Multi-select                                                                            |

The destructive nature of `d` here is opposite to the main list (where `d` is
archive). Confirm dialog is mandatory; copy: `Permanently delete <N> plan(s)?
This cannot be undone. [y/N]`.

---

## 7. View 3 — Plan detail (step list)

Entered via `enter` on a plan tile, or by `ralph run`.

### Layout

```
┌─ steps ──────────┐┌─ step detail (read-only preview) ──────────────┐
│ 1 ✔ Add schema   ││  Step 2: Write migration                        │
│ 2 ▶ Write migrat ││                                                  │
│ 3 ○ Wire FK      ││  Status: in_progress (attempt 2/3)               │
│ 4 ○ Add tests    ││  Harness: claude (sonnet-4-6)                    │
│ 5 ○ Update docs  ││  Tests: cargo test, cargo clippy -- -D warnings  │
│                  ││                                                  │
│ < archived: ...  ││                                                  │
└──────────────────┘└──────────────────────────────────────────────────┘
[ persistent chrome ]
```

Left sidebar = step list. Right pane = read-only summary of the highlighted
step (title, status, attempt count, harness, model, agent, tests). Pressing
`enter` swaps to the editable step-detail view (§8).

### Step list tile

One row per step, not boxed (compact list, like lazygit's file list):

```
  3 ▶ Write migration   2/3 retries
```

- `<num>` (1-based position), then status glyph, then title.
- Status glyphs reuse `App::status_indicator`: `○ ▶ ✔ ✘ ⊘`.
- Highlight bar `#f7d135` for cursor.
- Selection: `#56d0d9` background, `[N]` badge after the title.
- The "next-up / currently-running" step (first non-complete, non-skipped)
  is highlighted by default on entry.

### Live execution

When a run is active for this plan (run lock present, runner alive), the right
pane shows live state instead of the static summary:

- Current phase (from `run_locks.phase`: harness / tests / commit / …)
- Step elapsed time (live timer, reusing `step_start_time`)
- Tail of harness stdout (last ~20 lines, scrollable with `J`/`K`)
- Tail of test output

The TUI subscribes to NDJSON events from the runner via a tokio channel —
same producer used by `--json` today. If the run was started outside this
TUI process (separate `ralph run` invocation in another terminal), the TUI
attaches **read-only**: it polls DB rows and the run lock to display
status, but cannot stream harness stdout (no IPC channel). A toast warns:
`Run attached read-only — started outside this TUI.` The user can still
navigate but `R` is disabled while the external runner holds the lock; `S`
(cancel) still works because it goes through `ralph cancel` semantics. See
§13.2 for the full edit lockdown.

### Keybindings (Plan detail)

| Key                  | Action                                                                       |
| -------------------- | ---------------------------------------------------------------------------- |
| `j` / `↓`, `k` / `↑` | Navigate steps                                                               |
| `enter` / `→` / `l`  | Open step detail (§8)                                                        |
| `←` / `h` / `q`      | Back to plan list                                                            |
| `space`              | Multi-select steps                                                           |
| `i`                  | Insert new step **above** highlighted step (inline title prompt)             |
| `a`                  | Append new step **below** highlighted step                                   |
| `d`                  | Delete step (selection-aware, with confirm dialog)                           |
| `r`                  | Reset highlighted step (status → `pending`, attempts → 0)                    |
| `Shift-J`            | Move highlighted (or selected) step **down** one position                    |
| `Shift-K`            | Move highlighted (or selected) step **up** one position                      |
| `s`                  | Skip step. If it left uncommitted work, opens the Stash/Commit/Discard skip dialog (Esc = cancel, restarts the attempt with no retry-budget cost) |
| `R`                  | Run / resume this plan (no-op if already running)                            |
| `S`                  | Stop the live run (sends SIGTERM via `ralph cancel` semantics)               |
| `/` or `:`           | Command palette                                                              |
| `?`                  | Help overlay                                                                 |

---

## 8. View 4 — Step detail

Entered via `enter` on a step. The screen swaps to a stacked main-pane layout
with the step list collapsed to a thin gutter on the left (or hidden — see
§18 Q5):

> **Prompt-overhaul update.** The prompt model collapsed to four layers
> (Global / Project / Plan / Step). The panes below replace the original
> Universal/Project/Plan-context-prepend/Plan-prompt stack: there is one
> content blob per scope, no prefix/suffix split, and no per-plan
> `context_prepend` pane. The `l`/`→` key on the Step-prompt pane pushes a
> read-only **rendered-prompt preview** sub-view (the fully-assembled
> prompt, navigable per attempt).

```
┌─ Global (universal) prompt ────────────────────────────────────────┐
│ <config.prompt>  (seeded with DEFAULT_CONTEXT_PREPEND at init)      │
└─────────────────────────────────────────────────────────────────────┘
┌─ Project prompt ───────────────────────────────────────────────────┐
│ <.ralph/prompt.md  if present, else project_settings.prompt>       │
└─────────────────────────────────────────────────────────────────────┘
┌─ Plan prompt ──────────────────────────────────────────────────────┐
│ <plan.description>  (rendered into the `# Plan: {slug}` block)      │
└─────────────────────────────────────────────────────────────────────┘
┌─ Step prompt ──────────────────────────────────────────────────────┐  ← initial focus
│ <step.title>                                                       │
│ <step.description>                                                 │
│ Acceptance: <criteria>                                             │
└─────────────────────────────────────────────────────────────────────┘
┌─ Appended (attempt 2/3) ──────────────────────────────────────────┐
│  ◀ < retry context: previous diff, test output, modified files >  ▶│
└─────────────────────────────────────────────────────────────────────┘
┌─ Tests ────────────────────────────────────────────────────────────┐
│ • cargo test                                                       │
│ • cargo clippy -- -D warnings                                      │
└─────────────────────────────────────────────────────────────────────┘
┌─ Harness  │  Model            │  Agent     │  Change policy ───────┐
│ claude    │  claude-sonnet-4-6│  rust-impl │  required             │
└─────────────────────────────────────────────────────────────────────┘
```

Vertical navigation (`j`/`k` or `↑`/`↓`) moves between **panes**, not within
text. Initial focus is the **Step prompt** pane.

### Pane semantics

| Pane                  | Source of truth (read)                               | `c` writes to                                                  |
| --------------------- | ---------------------------------------------------- | -------------------------------------------------------------- |
| Global (universal)    | `Config.prompt` (seeded with `DEFAULT_CONTEXT_PREPEND`) | `~/.config/ralph-rs/config.json` (plain serde round-trip)    |
| Project prompt        | `<project>/.ralph/prompt.md` if present, else `project_settings.prompt` | the active source (file or `project_settings`)|
| Plan prompt           | `plan.description` (rendered into the `# Plan:` block) | `plans.description`                                            |
| Step prompt           | `step.title`, `description`, `acceptance_criteria`   | `steps` row                                                    |
| Appended (per-attempt)| `execution_log` rows for the step                    | **read-only**; `h`/`l` paginate by attempt                     |
| Tests                 | `plan.deterministic_tests`                           | `plans.deterministic_tests`                                    |
| Harness/Model/Agent/Change policy | `step` columns with plan/config fallback | Inline picker dialog, then `steps` row                         |

### Editing — `c`

Pressing `c` on any editable pane:

1. Resolves the pane's current text (the literal column value, not the
   composed prompt).
2. Spawns `$EDITOR <tempfile>` after suspending the TUI (same crossterm
   leave/enter dance lazygit uses).
3. On editor exit, reads the file and writes back to the source of truth.
4. Shows a toast: `Saved.` or `No changes.`.

If `$EDITOR` is unset and `$VISUAL` is also unset, show a red toast:
`No $EDITOR set — set one in your shell to edit prompts in-place.`

The Universal-prompt pane edits a JSON file via plain serde round-trip
through `Config::save`. Hand-written comments / custom key ordering in
`config.json` are not preserved (see §18 Q3).

### Appended-prompt navigation

The Appended pane shows the **most recent** execution log's retry context by
default. Inside this pane, `h` / `←` and `l` / `→` paginate through prior
attempts (oldest → newest). `h` / `←` from the leftmost attempt **exits the
step-detail view back to the step list** (which is the spec's "back to plan
sidebar" behavior).

`h` / `←` on any pane other than Appended pops the view (per §18 Q6).

### Bottom-row inline editors (Harness / Model / Agent / Change policy)

`c` on the bottom row opens a single-list picker scoped to whichever
sub-cell is focused (use `h`/`l` to move within the row, then `c`):

- **Harness**: enumerated from `Config.harnesses` keys.
- **Model**: enumerated from the harness's known model list, plus a
  free-text option.
- **Agent**: enumerated from `~/.config/ralph-rs/agents/*.md`.
- **Change policy**: two-item picker (Required / Optional).

Selection writes to the `steps` row. `<esc>` cancels.

### Keybindings (Step detail)

| Key                | Action                                                                |
| ------------------ | --------------------------------------------------------------------- |
| `k` / `↑`          | Move focus up one pane                                                |
| `j` / `↓`          | Move focus down one pane                                              |
| `h` / `←`          | (Appended pane) prev attempt; from leftmost, pop view. (Other panes) pop view. |
| `l` / `→`          | (Appended pane only) next attempt                                     |
| `c`                | Edit current pane (in `$EDITOR` for text panes; picker for bottom row)|
| `z`                | Toggle zen mode (full sidebar ↔ thin gutter)                          |
| `←` / `q`          | Pop to plan detail                                                    |
| `/` or `:`         | Command palette                                                       |

---

## 9. Command palette (`/` and `:`)

Both keys open the same input bar at the bottom. Triggered exactly like
lazygit's prompt; submit with `<enter>`, cancel with `<esc>`.

### Tab completion

- `<tab>` cycles through commands matching the current input.
- After a recognized command, `<tab>` cycles through valid arguments
  (harness names, plan slugs, branch names from `git branch --list`).
- Completion uses `clap`'s shell-completion data structure as a source of
  truth — same surface as `ralph completions`.

### Commands

Initial set (extensible; missing commands fall back to "unknown command"):

| Palette command                  | Behavior                                                                                  |
| -------------------------------- | ----------------------------------------------------------------------------------------- |
| `/run`                           | See §9.1 below                                                                            |
| `/run <branch>`                  | Run with `--current-branch` on `<branch>`; if branch doesn't exist, prompt to create it   |
| `/plan harness`                  | Open default plan harness in plan-creation mode (existing `plan harness generate`) — **NOT IMPLEMENTED**: the palette consumer toasts a "use the CLI for now" hint because the harness flow is interactive and not yet plumbed through the TUI subprocess hand-off. |
| `/plan harness <name>`           | As above with explicit harness — **NOT IMPLEMENTED** (same reason)                         |
| `/plan show [<slug>]`            | Push read-only plan summary view                                                          |
| `/plan archive [<slug>]`         | Equivalent to pressing `d` on the slug                                                    |
| `/plan unarchive <slug>`         | Move from archived list back                                                              |
| `/plan delete <slug>`            | Confirm + permanent delete                                                                |
| `/plan approve [<slug>]`         | Equivalent to `A` keybinding                                                              |
| `/plan questions on\|off [<slug>]` | Toggle `questions_enabled`                                                              |
| `/plan dependency add\|remove\|list` | Routes to plan dependencies sub-view                                                  |
| `/plan set-hook\|unset-hook\|hooks`  | Routes to plan hook attachment sub-view                                               |
| `/step add <title>`              | In plan detail: append step                                                               |
| `/step skip [<num>]`             | Equivalent to `s`                                                                         |
| `/step move <num> --to <m>`      | Equivalent to Shift-`J`/`K`                                                               |
| `/step set-hook\|unset-hook`     | Routes to step hook attachment sub-view                                                   |
| `/step edit --tags`              | Routes to step tag editor sub-view                                                        |
| `/cancel`                        | `ralph cancel` for the live run                                                           |
| `/export <slug>`                 | Write to `<slug>.ralph.json` in cwd                                                       |
| `/import <path>`                 | Read JSON, prompt for slug if conflict — **PARTIALLY IMPLEMENTED**: imports succeed but a slug collision surfaces as an error toast instead of an inline rename prompt. |
| `/quit` / `/q`                   | Exit TUI                                                                                  |
| `/help`                          | Open help overlay — **NOT IMPLEMENTED** from the palette: `?` opens the overlay, but `/help` currently surfaces a "coming soon" info toast routed through `PaletteAction::ComingSoon`. The overlay itself is fully wired per §15. |

`:` and `/` are interchangeable; both submit through the same parser. (No
distinction between vim-style `:wq` and claude-style `/foo` — pick whichever
is muscle memory.)

### 9.1 The `/run` flow

Executed against:

- The **selection** if any plans are selected (in selection order).
- Otherwise, the **highlighted** plan.

If the target is a single plan:

- Open a 3-button dialog: `Use current branch  [Enter] | New branch [n] | Cancel [esc]`.
- "New branch" prompts for a name, defaulting to `plan.branch_name`.
- Runs the plan with `--current-branch` if "current" picked, or with branch
  switch if "new" picked.

If multiple plans are selected:

- `--current-branch` is **forced** (per spec).
- Same dialog, but "Use current branch" runs in cwd's branch; "New branch"
  creates one branch and runs all selected plans on it sequentially.

`/run <branch>` short-circuits the dialog: `--current-branch` mode on
`<branch>`, switching to it first (creating if it doesn't exist; confirm
prompt before creating).

When the run starts, the TUI auto-pushes the **plan detail** view of the
first plan being run; subsequent plans (in multi-select mode) push as they
start.

---

## 10. Non-interactive mode & `--jsonl`

### `--non-interactive`

Global flag on the root `Cli`, alongside `--json`, `--quiet`, `--no-color`.
Forces no TUI, plain output, no ANSI color (overrides `--no-color` default
behavior to default-on). Auto-set when stdout is not a TTY.

### `--jsonl`

`--jsonl` is a **strict alias** for the existing `--json` flag. NDJSON
(newline-delimited JSON) and JSONL are the same format — one self-contained
JSON object per line — and `ralph run --json` already emits exactly that
stream. `--jsonl` exists so meta-harnesses can spell the flag the way they
expect; no behavior diverges between the two spellings.

When either is passed with `ralph run`:

- Implies `--non-interactive`
- One JSON object per line on stdout (NDJSON / JSONL)
- Final stdout line is a `summary` event with overall plan outcome
- Stderr remains free-text for human debugging
- Exit code matches today's runner (`0` success, non-zero on failure)

### Event schema

See §13.1 for the full event schema (existing events plus the new
`harness_chunk`, `test_chunk`, `phase_changed`, and `summary` events).
Documented in `docs/ndjson-events.md` (separate doc).

---

## 11. Prompt-layer model (recap)

> **Prompt-overhaul update.** The model collapsed to a strict **four-layer**
> shape (Global / Project / Plan / Step). One content blob per scope — no
> prefix/suffix split, no per-plan `context_prepend`. The legacy per-plan
> columns were dropped (migrations V21/V22).

The step-detail view exposes three editable prompt scopes (Global / Project
/ Plan) plus the read-only Step and Appended scopes. They map onto storage:

```
                ┌───────────────┐
Global          │ config.json   │  config.prompt  (seeded w/ DEFAULT_CONTEXT_PREPEND
(universal)     └───────────────┘                   at `ralph init`)
                ┌───────────────┐
Project         │ project_settings │  <project>/.ralph/prompt.md  (file wins)
                └───────────────┘                   else project_settings.prompt
                ┌───────────────┐
Plan            │ plans row     │  description  (rendered into `# Plan: {slug}` once)
                └───────────────┘
                ┌───────────────┐
Step            │ steps row     │  title, description, acceptance_criteria
                └───────────────┘
                ┌───────────────┐
Appended        │ execution_log │  retry context (per attempt; read-only)
                └───────────────┘
```

The CLI surface is `ralph prompt show|set|clear --scope <global|project>`
(`universal` is an alias for `global`); `ralph prompt show --resolved`
prints the composed global+project lead. `ralph init --restore-prompts`
re-seeds the global prompt; `ralph doctor` warns (non-fatal) if the global
prompt is missing the ralph-CLI hints.

The step-detail view does **not** show the composed prompt inline, but
`l`/`→` on the Step-prompt pane pushes a read-only **rendered-prompt
preview** sub-view that runs `prompt::build_step_prompt` exactly as the
harness would receive it, with per-attempt navigation.

---

## 12. Color palette (centralized)

Add a `tui::theme` module so colors are defined once.

| Token                | Hex       | Use                                              |
| -------------------- | --------- | ------------------------------------------------ |
| `cursor`             | `#f7d135` | Highlight border / cursor row                    |
| `selection`          | `#56d0d9` | Multi-select border                              |
| `status.complete`    | `#34d058` | Plan dot + step glyph when complete              |
| `status.in_progress` | `#f7d135` | Plan dot + step glyph when running               |
| `status.pending`     | `#3b82f6` | Plan dot when never run                          |
| `status.failed`      | `#ef4444` | Plan dot + archived tile border                  |
| `status.question`    | `#a855f7` | Plan dot when paused for question (§17)          |
| `chrome.dim`         | gray      | Default tile borders, idle bottom bar text       |
| `toast.error`        | `#ef4444` | Error toasts                                     |
| `toast.info`         | `#3b82f6` | Info toasts                                      |
| `toast.success`      | `#34d058` | "Saved." toasts                                  |

Truecolor is assumed; fall back to the nearest 256-color match if
`COLORTERM` is not `truecolor` / `24bit`.

---

## 13. State management & data flow

- TUI owns one `App` struct per view, swapped via a stack (`Vec<View>`).
- Single `tokio::task` polls the DB at 250ms intervals when a run is live,
  500ms otherwise; pushes deltas into the App via a watch channel.
- Runs **spawned by the TUI** stream events directly via the existing
  `OutputFormat::Ndjson` plumbing; the TUI consumes those events instead of
  parsing stdout. See §13.1 for the event-schema additions needed to
  support live tails.
- Runs spawned **outside** the TUI (separate `ralph run` invocation in
  another terminal) are detected via the `run_locks` table; the TUI
  attaches **read-only**: it polls `execution_logs` rows for status but
  cannot stream stdout (no IPC channel). See §13.2 for the read-only edit
  lockdown.
- The run lock interaction is unchanged: only one runner per project;
  TUI-started runs acquire the lock the same way the CLI does today.

### 13.1 NDJSON event schema additions

Today's `output::emit_ndjson` covers only durable lifecycle events:
`step_started`, `step_finished`, `plan_complete`, `stale_steps_swept`,
`plan_grew`, `prompt_prepared`. The TUI's live-status pane needs streaming
I/O too, so the schema gains the following events. All are additive and
backward compatible — meta-harnesses can ignore unknown event types per
the existing convention:

- `harness_chunk { stream: "stdout" | "stderr", text, seq }` — emitted as
  the harness writes output. `text` is line-buffered (one emit per
  newline); `seq` is a monotonic counter so the TUI can reorder if
  needed. Truncated past `Config.harness_chunk_max_bytes` (default 4096)
  per chunk.
- `test_chunk { test_index, stream, text, seq }` — same shape, scoped to
  the deterministic-test phase. `test_index` indexes into
  `plan.deterministic_tests`.
- `phase_changed { phase }` — emitted on every transition recorded into
  `run_locks.phase` (existing field). Lets the TUI redraw the phase
  indicator without polling.
- `summary { plan_status, steps_complete, steps_total, duration_secs,
  cost_usd?, started_at, ended_at }` — the new final event for `ralph
  run`, replacing the role of `plan_complete` for human-readable summary
  consumers. `plan_complete` is **kept** for one release as a compat
  shim (still emitted alongside `summary`) so meta-harnesses pinned to
  it don't break.

Schema lives in `docs/ndjson-events.md` (separate doc).

### 13.2 Read-only attach: edit lockdown

When the TUI is attached to a run it didn't spawn, **all edit keys are
disabled** until the external runner releases the lock. Specifically:

- Disabled in plan list: `i`/`a`/`A` (create), `d` (archive), `Q` (toggle
  questions).
- Disabled in plan detail: `i`/`a` (add step), `d` (delete step), `r`
  (reset), `s` (skip), `R` (run), Shift-`J`/`K` (move).
- Disabled in step detail: `c` on every editable pane (universal /
  project / plan / step / tests / bottom row), `a` (answer question).
- Still enabled: navigation, viewing, `S` (cancel — this goes through
  `ralph cancel` semantics and is the user's escape hatch), `q` (quit
  TUI), `?` (help).

A persistent banner reads: `🔒 Read-only — run in progress (PID <n>).
[S] cancel  [q] quit`.

When the external runner releases the lock, the banner disappears and
edit keys come back. There is no edit-staging or queued-edit mechanism;
if the user wants to make changes, they cancel the run or wait.

---

## 14. Editor handoff

`crossterm::execute!(stdout, LeaveAlternateScreen, DisableMouseCapture)` →
spawn `$EDITOR` (or `$VISUAL` if `$EDITOR` unset) inheriting stdio →
re-enter alternate screen on exit. This is the standard ratatui pattern.

Tempfile lives in `~/.local/share/ralph-rs/tmp/<scope>-<id>-<rand>.md`,
deleted on successful save. On editor non-zero exit, abort save and toast.

---

## 15. Module / file plan

```
src/tui/
  mod.rs            (existing; rewrite to multi-view)
  app.rs            (existing; gut and rebuild around `View` stack)
  view.rs           (NEW) View enum: PlanList | ArchivedList | PlanDetail | StepDetail
  views/
    plan_list.rs    (NEW)
    archived_list.rs(NEW)
    plan_detail.rs  (refactor of today's app.rs)
    step_detail.rs  (NEW)
    plan_dependencies.rs (NEW; sub-view)
    plan_hooks.rs   (NEW; sub-view)
    step_hooks.rs   (NEW; sub-view)
    step_tags.rs    (NEW; sub-view)
  ui.rs             (existing; split into per-view renderers)
  input.rs          (existing; add command palette + per-view dispatch)
  palette.rs        (NEW) Slash/colon command parser + tab completion
  theme.rs          (NEW) Color tokens
  toast.rs          (NEW) Transient message bar
  dialog.rs         (NEW) Confirm dialog primitive
  editor.rs         (NEW) $EDITOR handoff
  events.rs         (NEW) Event subscription wiring
  chrome.rs         (NEW) Persistent top/bottom chrome
src/cli.rs          Add `--non-interactive`, `--jsonl` flags; allow no-subcommand entry; `Question` subcommand
src/commands/question.rs (NEW)
src/main.rs         Route no-subcommand `ralph` → TUI; route `ralph run` → TUI unless any non-default flag
```

The existing single-plan TUI tests (`src/tui/mod.rs` `tests`) move into
`views/plan_detail.rs` tests.

---

## 16. Build phases (rough; not contractual)

A possible ordering for ralph plans built from this doc:

1. Multi-view skeleton: `View` enum, view stack, persistent chrome (cwd /
   version), command palette parser stub.
2. Plan list view (read-only) — render tiles, status dots, sort order.
3. Archived list view + `d` archive flow + selection logic + `A` approve.
4. Plan detail view — refactor existing TUI into a view; preserve current
   tests. Include `r` reset, Shift-`J`/`K` move, confirmable `d` delete.
5. Step detail view — pane navigation only (no editing yet); includes
   the four prompt-layer panes (Global/Project/Plan/Step) and the
   `change_policy` cell.
6. `c` editor handoff for each text pane.
7. Bottom-row pickers (harness / model / agent / change policy).
8. NDJSON event-schema additions: `harness_chunk`, `test_chunk`,
   `phase_changed`, `summary`. Bumps the events doc.
9. Command palette commands: `/run`, `/run <branch>`, `/plan harness`,
   the `/step` and `/plan` slash variants.
10. Live run integration (subscribe to NDJSON when TUI spawns the run);
    read-only attach + edit lockdown when an external runner holds the
    lock.
11. Question feature: V16 migration (`questions_enabled`,
    `step_questions`, `PausedForQuestion` termination reason); `Q`
    toggle; `ralph question ask` CLI binding via run lock; runner
    integration; answer modal + resume prompt.
12. `--non-interactive` flag + `--jsonl` alias + auto-detection on
    non-TTY + "any flag drops to non-interactive" rule for `ralph run`.
13. Help overlay, toast polish, docs.

The implemented ralph plan (`tui-v1`) re-orders these so the foundations
(CLI flags, V16 migration, NDJSON event expansion) come first, before any
TUI refactor. See the plan steps for the contractual order.

---

## 17. Question state (per-plan, opt-in)

Resolution of the original Q2: questions are a **per-plan opt-in feature**
that lets the harness pause execution and ask the user for clarification.
Designed primarily as an escape hatch — the expectation is that decisions
are resolved during planning, not during implementation — but useful when
the plan turns out to be ambiguous mid-step.

### Toggling questions on/off

**Only the user can enable or disable questions for a plan.** The harness
cannot toggle the flag itself. This is intentional: if the harness could
turn questions on, it would, every time. Keeping the toggle user-only
preserves the "questions are an explicit invitation" invariant.

Toggle surfaces:

- TUI plan list / plan detail: `Q` on the plan tile flips
  `questions_enabled` (toast: `Questions enabled.` / `Questions disabled.`).
- TUI command palette: `/plan questions on` / `off`.
- CLI: `ralph plan questions on|off <slug>`.

Default for new plans: **off**.

### CLI surface (designed for the harness)

The harness calls `ralph question ask` to ask a question. The shape was
chosen to match how an LLM agent naturally writes shell commands:

```
ralph question ask [OPTIONS] [<QUESTION>]

ARGUMENTS
  <QUESTION>     The question text. If omitted, read from stdin.

OPTIONS
  -s, --suggest <ANSWER>     A suggested answer. Repeatable.
                             (User can always type a custom answer.)
  -h, --help                 Print help.
```

### Binding model

`ralph question ask` binds the question to the currently-executing step
by reading the project's **run lock**. The lock already records
`step_id` and the current attempt; no env vars or harness-side context
plumbing is needed.

Resolution order:

1. Find the run lock for the current project (cwd, or `--project`).
2. If the lock exists and is held by a live runner, read `step_id` and
   the current `execution_logs.attempt` for that step. Write the
   `step_questions` row with those values.
3. If no lock exists, exit non-zero with: `ralph question ask: no
   active ralph run found for this project. This command only works
   while a step is being executed by `ralph run`.`

Examples:

```bash
# Most common case — one-line question, two suggestions
ralph question ask "Should this use Postgres or SQLite?" \
  -s "PostgreSQL with diesel" \
  -s "SQLite (already a dep)"

# Long-form question via stdin (heredoc-friendly)
ralph question ask -s "Yes" -s "No" <<'EOF'
The plan says "add caching." Ambiguous whether this means:
(a) in-process LRU via the `lru` crate, or
(b) a separate Redis instance.

(a) ships faster; (b) matches our infra patterns.
EOF

# No suggestions at all — open-ended
ralph question ask "What should the new endpoint be named?"

# Multiple questions in a single attempt — just call again
ralph question ask "Pick a logging crate" -s "tracing" -s "log+env_logger"
```

The harness can call `ralph question ask` any number of times in one
attempt. Each call writes one `step_questions` row and exits 0. After the
harness exits, the runner detects rows where `answer IS NULL` and pauses.

### Behavior when questions are disabled

If the harness runs `ralph question ask` on a plan with
`questions_enabled = false`, the command exits **non-zero** with the
following message on stderr (no DB write, nothing visible to the user):

```
ralph question ask: questions are not enabled for this plan.

Continue with the work as best you can given the information you have.
Document any assumption you make in a comment near the relevant code so
the user can review and adjust. A reasonable guess that's clearly
flagged is preferable to halting; do not retry this command.

(If the user wants to enable questions, they can press `Q` on this plan
in the ralph TUI, or run `ralph plan questions on <slug>`.)
```

The non-zero exit is deliberate so the harness's own error handling
notices, but the message body is encouraging: "do your best, flag
assumptions, don't loop." This avoids a pathological case where the
harness keeps retrying `ralph question ask` because it didn't understand
why it failed.

### Prompt injection (when enabled)

When `questions_enabled = true`, `prompt::build_step_prompt` appends this
block at the **very end** of every step prompt, after the existing focus
instruction. Tone: encouraging real questions, discouraging frivolous
ones, without being adversarial.

```
## Asking the user a question

This plan has questions enabled, so you may pause and ask the user for
clarification when you're genuinely blocked on a decision they need to
make.

Before asking, seriously consider whether the answer is already
recoverable from:
  - The plan description and step acceptance criteria above.
  - The codebase itself (read the relevant files).
  - A reasonable, conservative default that you can flag in a comment.

Most decisions belong in the plan, not the implementation. Questions
cost the user attention and break their flow.

That said: when you genuinely cannot proceed without input, ask. A good
question with suggested answers is far better than a wrong guess.

To ask, run:

    ralph question ask "What should I do about X?" \
      -s "option A: ..." \
      -s "option B: ..."

Suggestions are optional but appreciated — the user can always type a
custom answer. You may call `ralph question ask` multiple times in one
attempt. After your last call, exit normally (zero status). The plan
will pause; the user will answer in the TUI; your next attempt will
receive every answered question in the appended retry context.
```

### Storage

- Migration V16:
  - `ALTER TABLE plans ADD COLUMN questions_enabled INTEGER NOT NULL DEFAULT 0;`
    (the column DEFAULT is unchanged so pre-existing rows stay opted-out.
    **Prompt-overhaul update:** `ralph plan create` now writes
    `questions_enabled = true` for *newly created* plans — the app-level
    default flipped to on; no migration touched existing rows.)
  - New table:
    ```sql
    CREATE TABLE step_questions (
      id TEXT PRIMARY KEY,
      step_id TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
      attempt INTEGER NOT NULL,
      question TEXT NOT NULL,
      suggestions TEXT NOT NULL DEFAULT '[]',  -- JSON array of strings
      answer TEXT,
      asked_at TEXT NOT NULL,
      answered_at TEXT
    );
    CREATE INDEX idx_step_questions_step ON step_questions(step_id);
    CREATE INDEX idx_step_questions_unanswered ON step_questions(answer)
      WHERE answer IS NULL;
    ```
- New `PlanStatus::Question` enum variant. The status is **derived**, not
  stored: a plan with any unanswered `step_questions` row reports
  `Question` regardless of the underlying `plans.status` column. This
  avoids two-source-of-truth bugs where the row exists but the column
  drifted. The `plans.status` column keeps tracking the underlying
  lifecycle (`in_progress`, `ready`, etc.) so when the user answers,
  status simply un-shadows.
- New `TerminationReason::PausedForQuestion` enum variant for the
  `execution_logs.termination_reason` column.

### Runner integration

After the harness exits, the runner:

1. Reads `step_questions` rows where `step_id = current` AND
   `attempt = current_attempt` AND `answer IS NULL`.
2. If any exist:
   - **Skip test execution.** A pause is not a failed test, so testing
     is meaningless until the user clarifies.
   - **Skip commit.** Roll back any diff the harness produced.
   - Record the `execution_logs` row with
     `termination_reason = paused_for_question`. The label distinguishes
     "paused" history from real failures.
   - **The pause counts as one attempt.** `step.attempts` ticks normally
     (single counter shared with regular failures). This keeps the data
     model simple. The harness has natural incentive not to keep asking
     when retries are running low; if a harness asks on its last attempt
     and gets answered, the user can `ralph step edit --max-retries +1`
     to give more budget. See design note below.
   - Release the run lock cleanly and exit. The plan's effective status
     is now `Question` (derived from the unanswered row).
3. If no unanswered questions exist, the runner proceeds normally
   (tests, commit, retry-or-success). The harness exiting without
   asking — even on a question-enabled plan — is the happy path.

**Design note: single retry counter.** An earlier draft separated
"execution attempt id" from "retry budget consumed" so question pauses
wouldn't tick the budget. The complexity of two counters (with
divergent IDs in `execution_logs.attempt` vs `steps.attempts`) wasn't
worth the marginal benefit. Single counter, single source of truth,
explicit user action to extend the budget when needed.

### TUI surface

- **Plan list**: purple dot for plans with unanswered questions. Tile
  shows a one-line teaser of the oldest unanswered question (truncated).
- **Plan detail**: a banner pane above the right panel when unanswered
  questions exist: `❓ <count> open question(s) — press [A] to answer`.
- **Step detail**: an additional pane between **Step prompt** and
  **Appended**, titled `Open question(s)`, listing each unanswered
  question with its suggestions. Keys in this pane:
  - `a` — answer the focused question. Opens a modal:
    ```
    ❓ Should this use Postgres or SQLite?

      [1] PostgreSQL with diesel
      [2] SQLite (already a dep)
      [c] Custom answer (opens $EDITOR)
      [esc] Cancel
    ```
    Pressing a number selects that suggestion verbatim. `c` opens
    `$EDITOR` for a free-form answer. The user is never forced into a
    suggestion.
  - `j` / `k` — move between questions if multiple are open.
- **Resume prompt**: when the user answers the **last** open question
  for a plan (anywhere — plan-detail banner, step-detail pane, or CLI),
  a modal pops:
  ```
  All questions answered.
  Resume implementation now?  [Y/n]
  ```
  - `Y` / `<enter>` — kick off `ralph run` on this plan, reusing the
    branch mode of the previous run (current-branch if the prior run
    was current-branch; otherwise the plan's normal branch flow).
  - `n` / `<esc>` — leave the plan unpaused; user can `R` later.
  This is the explicit "user-driven resume" decision: never auto-run,
  but make it one keystroke when the user is ready.

### CLI surface for answering (mirror of the TUI)

For users who answer outside the TUI:

```
ralph question list [<plan-slug>]
  # Lists open questions across plans (or one plan), with index numbers.

ralph question answer <num> <answer>
  # Answers a specific question by index from `ralph question list`.

ralph question show <num>
  # Prints the full question text + suggestions.
```

Answering via CLI does NOT trigger the auto-resume prompt (CLI is
non-interactive). The user runs `ralph run` explicitly.

### Retry context after answering

On the next attempt of a step that had questions answered, the prompt
gains a new section between **Plan context** and **Step details**:

```
## Previously answered questions

> Q: Should this use Postgres or SQLite?
> A: SQLite (already a dep)

> Q: Pick a logging crate.
> A: tracing
```

This means the harness sees its own questions and the user's answers
verbatim — no paraphrasing — so it can pick up where it left off.

---

## 18. Resolved decisions

- **Q1 (archive `d`):** Selection wins; if no selection, target the cursor.
  Drafted text in §5/§6 already matches this.
- **Q8 (`--json` vs `--jsonl`):** `--json` already emits NDJSON during
  `ralph run` (one self-contained JSON object per line). `--jsonl` is
  a **strict alias** for `--json` — added so meta-harnesses can spell it
  the way they expect. No rename, no behavior change for current consumers.
- **Q9 (active run):** TUI attaches read-only when a run is already live
  for the plan. The CLI's `ralph run` keeps its current "refuse with error"
  behavior outside the TUI; inside the TUI, the right pane shows live
  status (DB-polled, no harness stdout — see §13) and a toast: `Run
  attached read-only — started outside this TUI.`
- **Q10 (`/plan harness`):** Confirmed — opens the AI-assisted plan
  generation flow (`ralph plan harness generate`), not just a harness
  picker for the next plan.
- **Q4 (step delete):** Confirm dialog before deleting steps in the plan-
  detail view. Selection-aware (so deleting 5 selected steps shows one
  dialog with the count, not five dialogs).
- **Q3 (global prompt save):** Plain serde round-trip — read
  `config.json`, mutate the single `prompt` field, write back via
  `serde_json::to_string_pretty`. Hand-written comments / custom key
  ordering are not preserved; this is acceptable since `config.json` is
  ralph-generated and editing happens through the TUI surface.
- **Q5 (step list in step detail):** Two modes, toggled with `z`.
  - **Default**: full step-list sidebar identical to plan-detail's left
    panel (titles + status glyphs + selection markers). Width: same as
    plan-detail (~25 cols).
  - **Zen mode**: thin gutter (~4 cols) with just step number + status
    glyph. Triggered by `z`, persists for the duration of the step-detail
    visit (resets on pop).
  - **Auto-zen**: if `term_width < 100`, the TUI forces zen mode and
    disables the `z` toggle (with a toast on first hit:
    `Terminal too narrow — zen mode forced.`). When the terminal is
    resized above the threshold, the user can `z` back to full sidebar.
- **Q6 (`h`/`←` from non-Appended panes):** Pop view to plan-detail. The
  Appended pane intercepts `h` first when there's a previous attempt to
  show; once exhausted (leftmost attempt), it falls through to the same
  pop behavior.
- **Q7 (`--non-interactive` scope):** Global flag on the root `Cli`,
  alongside `--json`, `--quiet`, `--no-color`. No-op on subcommands that
  are already non-interactive; meaningful on `ralph run` and bare `ralph`.

---

End of plan.
