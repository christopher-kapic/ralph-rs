---
name: create-ralph
description: Create a ralph (a deterministic execution plan with steps) for a coding task. Use when the user wants to plan and break down a feature, bug fix, or refactor into executable steps that ralph-rs can run through a coding agent harness.
argument-hint: [description of what to build]
allowed-tools: Read Grep Glob Bash Agent
---

You are helping the user create a **ralph** — a structured, deterministic execution plan for ralph-rs. A ralph is a plan with ordered steps that ralph-rs will execute sequentially through a coding agent harness (Claude, Codex, etc.), validating each step with deterministic tests before moving on.

## When to use a ralph (and when not to)

Plan **per feature, not per task**. If the work fits in one focused session, don't reach for ralph — just do it. Ralph earns its complexity when:

- The work spans more than a single coherent session of edits.
- You want each step independently verified by tests before the next one starts.
- You want a review pass interleaved with implementation passes.
- You want per-step retry on failure (and, opt-in via `--retry-strategy rollback`, a clean-tree rollback between attempts).

A bugfix that's "find the line, change three characters, run tests" is not a ralph. A multi-phase refactor with verification gates is.

## Preflight (before generating any plan)

Run these checks first. If any fail, fix them before authoring the plan:

1. **`command -v ralph`** — the CLI is installed.
2. **`ralph doctor`** — environment is healthy. The doctor surfaces foot-guns (codex without `--sandbox`, claude without `--permission-mode`) as warnings; **address those before generating a plan that depends on them**.
3. **If any step will use `codex` as the harness**, verify its sandbox via `ralph harness show codex`. Default `codex` (`workspace-write`) is correct for implementation and most review steps. Use the `codex-orchestrator` harness (`danger-full-access`) for steps that need to mutate state outside the workspace — most commonly review steps that append follow-up steps via `ralph step add`, since ralph's DB lives outside the workspace sandbox.
4. **The cwd is a git repo** on a branch the user is okay deriving from.

## Workflow

1. **Understand the task**: Read `$ARGUMENTS`. If vague, ask clarifying questions.

2. **Investigate the codebase**: Read the relevant code, existing patterns, test infrastructure, and any `AGENTS.md` / `CLAUDE.md` conventions. Steps must be specific to the actual codebase, not generic.

3. **Identify deterministic tests**: Shell commands that validate success. Examples: `cargo test`, `cargo clippy -- -D warnings`, `npm test`, `pytest`, custom scripts. Ask if unsure.

4. **Design the plan** using the recommended shape below.

5. **Present the plan to the user before creating anything.** Show plan slug, description, test command(s), and each step's title and description. Wait for approval.

6. **Create the plan** with the commands in *Authoring* below.

## Express step independence (the DAG payoff)

Ralph executes a plan as a **dependency DAG**, not a flat list. When one
branch blocks on a human (a question or a blocker the agent can't clear),
**independent branches keep running** and the human batch-answers on their
own schedule. **The payoff scales with the DAG's width — a purely linear
plan gets *zero* benefit** (its single branch blocking = the whole plan
blocked, exactly like before).

So, when the work genuinely allows it, **make independence explicit**:

- Split work into branches that *don't* depend on each other and declare
  only the edges that are real. **Every step's place in the DAG is
  explicit** — on a non-empty plan `ralph step add` *requires* one of:
  - `--after <S>` — the new step depends on `S` (a new branch off `S`);
  - `--before <S>` — insert before `S` (the new step takes over `S`'s
    incoming edges; `S` then depends only on it);
  - `--depends-on <S>...` — depend on several prior steps (a fan-in/join);
  - `--root` — a deliberate independent root (no dependencies).

  The first step of an empty plan is the implied root. `--after`/`--before`
  are **dependency edges, not list position** — there is no positional
  insert (that ambiguity silently produced edge-less DAGs). In
  `--import-json`, the DAG is carried per-object via `short_id` +
  `depends_on` (see below). Wire only true ordering constraints; a `--root`
  / no-`depends_on` step runs as soon as the scheduler reaches it.
- Don't manufacture a dependency just because two steps touch the same
  area — only add an edge when step B genuinely needs step A's output.
- **But don't over-fragment into deep independent branches that all
  re-converge at one late join step.** History is linear (one git branch
  per plan), so a deep side branch's commits stack on top of whatever else
  committed meanwhile; a late join then has to absorb all of that at once.
  Prefer **shallow, wide independence** (several short parallel branches)
  over **deep, narrow independence** (long chains that join late) — it
  limits how much in-flight work is entangled when something blocks.
- A linear plan is still perfectly valid (and behaves exactly as before —
  deterministic, no regressions). Independence is *soft pressure*, not a
  mandate: only express it when the task actually decomposes that way.

## Recommended plan shape

Default to: **build → verify → review → fix-as-needed**, repeated per phase.

- **Build steps** (default `change_policy=required`): the agent writes code. Ralph fails the step if no diff is produced. Where build steps are independent, wire only the real `--depends-on` edges so they can interleave.
- **Verify step** (a deterministic test like `cargo test`): catches "code compiled but the page is broken in the browser" failures that reviewers can't replace. Don't skip this even if review is on.
- **Review — prefer the built-in pipeline.** Ralph now has a **built-in nondeterministic review pipeline**: a separate harness reviews each step's commit read-only, and a failed review automatically inserts a corrective step and re-parents dependents. You usually do **not** need to author explicit review steps anymore. Turn it on instead:
  - `ralph config review set --harness <h> --model <m> --enabled true` — set the global reviewer (use a *different* model from the implementer when possible, e.g. `codex` reviewing `claude`).
  - `ralph plan review on <slug>` — review every step in the plan; or
  - `ralph step edit <sel> --review on` — review only specific (risky / subtle-criteria) steps; `--review off` exempts one; `--review inherit` defers to plan/global. Precedence is step > plan > global > off.
  - Tune the recursion bound with `ralph plan create ... --max-review-corrections <n>` (default 3): if a corrective step keeps failing its own review past this many rounds, ralph raises a blocker for a human instead of looping forever.
- **Explicit review/fix steps are now the exception**, for cases the built-in pipeline doesn't cover (e.g. a whole-plan audit at the end, or a review that must run a command rather than read a diff). If you do author one, set `--change-policy optional` (reviewers produce no diff) and `--max-retries 1` (don't retry-loop on disagreement). A whole-plan audit step should `--depends-on` the steps it audits (so it runs after them); an independent check is `--root`. (`--after`/`--before` are dependency edges now, not "append at the end"; `ralph step move` only changes display/tie-break order, not edges.)

## Authoring (this is where the gotchas hide)

### Prefer `--import-json` for anything non-trivial

Build a JSON array and pipe it to `ralph step add --import-json -`. JSON sidesteps every shell-quoting failure mode at once (backticks, `$`, parens, quotes in code references all need escaping in inline `--description` strings, and the failure mode is **silent**) **and it carries the whole DAG in one document** — the correctness-first, fewest-tokens path. Give each step an `id` (a short readable label *you* choose) and list its parents' `id`s in `depends_on` (a parent may also be an existing plan step by short id or number). A step with no `depends_on` is a root. The batch is validated (unique ids, no dangling/cyclic edges) and inserted atomically — nothing is written if any edge is bad. **Don't** bulk-insert then wire edges in N follow-up commands; put the graph in the JSON.

```bash
# The whole DAG in one document. `parser` and `codegen` are independent
# roots; `integrate` is a fan-in that needs both.
ralph step add --import-json - <<'JSON'
[
  {
    "id": "parser",
    "title": "Add UserService struct",
    "description": "Add `UserService` in `src/services/user.rs` with methods `create`, `get_by_id`, `delete`. Follow the pattern in `src/services/auth.rs`. Acceptance: `cargo test services::user` passes."
  },
  {
    "id": "codegen",
    "title": "Add the user API handlers",
    "description": "..."
  },
  {
    "id": "integrate",
    "title": "Wire UserService into the API",
    "description": "...",
    "depends_on": ["parser", "codegen"]
  }
]
JSON
```

`id` is a **batch-local wiring label** for `depends_on` — it is *not* saved. ralph mints each step's persisted 8-char `short_id` (the handle `ralph step list` shows and `ralph step edit`/`step dependency` take afterwards); don't hand-write `short_id`. Omit `depends_on` (or give `[]`) for a root.

### When inline is acceptable, use a quoted heredoc + tempfile

For one-off cases where JSON is overkill, write the description to a tempfile via a **quoted** heredoc (`<<'EOF'`, single-quoted to disable expansion) and pass it via `$(cat $tempfile)`. Never inline `--description "...$VAR...\`backtick\`..."` — bash expands the backticks even with backslash escapes.

### The vanilla flow (only for short, simple descriptions)

```bash
ralph plan create <slug> --description "..." --test "<cmd>"
ralph step add "<Step 1 title>" <slug> --description "<short desc>"   # implied root (empty plan)
ralph step add "<Step 2 title>" <slug> --description "<short desc>" --after 1   # depends on step 1
ralph plan approve <slug>
```

## Writing good step descriptions

Each step gets a **fresh context** — there is no conversation in step N+1. Prompts must be self-contained.

- **Reference files and conventions explicitly**: which `AGENTS.md` / `CLAUDE.md` sections to read, which files to touch, which patterns to follow. Don't say "the previous step" — refer to specific files or the commit on the plan branch.
- **State concrete acceptance criteria**: "typecheck passes", "`grep -n FOO src/` returns 0 hits", "`cargo test services::user` passes", "route returns 404 not 500". Anything an agent can mechanically verify.
- **Avoid "based on the conversation above"** — there is no conversation. Cite files and signals the next step can find on its own.
- **Reference existing patterns**: "follow the pattern in `src/services/auth.rs`" beats abstract instructions.
- **Keep one concern per step**: don't combine "add the model" and "add the API endpoint".
- **Order dependencies correctly**: types before uses, modules before imports.

## Review verdicts (built-in pipeline and manual review steps)

The **built-in review pipeline** handles the verdict/correction loop for
you: the reviewer harness is told to emit a structured verdict, ralph
parses pass/fail, records the verdict as a git note, and on a failure
auto-inserts a corrective step and re-parents dependents — you don't write
the "append a fix step" logic yourself. Just pick a capable reviewer
harness/model via `ralph config review set` and enable review at the plan
or step level.

If you still author an **explicit manual review step** for a case the
built-in pipeline doesn't cover:

- Tell the reviewer to read `AGENTS.md` / `CLAUDE.md` and per-phase patterns explicitly — most harnesses don't load them by default.
- Have it emit a **structured verdict** so a following step (or a human) can parse it: a single line of either `REVIEW PASS — <one-line summary>` or `REVIEW FAIL — N issue(s)`, followed by a numbered list when failing. (This is the same verdict shape the built-in reviewer is instructed to produce, so the convention is consistent.)
- If that manual step must itself append fix steps via `ralph step add`, **describe the action in prose**, not as a copy-paste shell snippet — that re-introduces the embedded-quoting trap.

## Branching

Use `--branch feat/<slug>` so reviewers can run `git diff main..HEAD` from any step to see the cumulative work-to-date. (Ralph commits **per iteration** on the single plan branch — subject `ralph <short_id>.<n> - <title>` plus `Ralph-*` trailers — and history stays linear, one branch per plan; that's how rollback, per-step diff isolation, and the built-in reviewer's fixed-SHA review all work, so don't try to disable it. By default every iteration commit is kept; `ralph plan create ... --squash-on-complete` collapses a step's iteration commits into one when it completes.)

## Plan size

Pick granularity based on the work, not a target count. Rough bands:

- bugfix: 3–5 steps
- small feature: 5–15
- medium feature: 15–40
- large refactor or greenfield: 40–300

Don't compress a big task into a handful of mega-steps — you lose the per-step checkpointing, retry, and per-step diff isolation. Don't inflate a small task into trivial steps either. Whatever the size, every step must be atomic and independently verifiable.

## Anti-patterns

- ❌ Referencing "the previous step" by name in a prompt — refer to files or commits (a DAG has no positional "previous").
- ❌ Long inline `--description "..."` with mixed quoting — silent truncation. Use `--import-json` or a quoted heredoc → tempfile.
- ❌ Inventing dependency edges that aren't real ordering constraints — it serializes work the scheduler could have run independently and kills the DAG payoff.
- ❌ Expecting `--after <N>` to mean "insert at list position N" — it does not exist any more. `--after`/`--before` are **dependency edges**. Array order in `--import-json` is not a dependency either; only `depends_on` is.
- ❌ Bulk-inserting with `--import-json` and forgetting `depends_on` — you get an all-roots, edge-less plan that runs but has none of the gating/ordering you intended. Put the graph in the JSON.
- ❌ Deep independent branches that all re-converge at one late join — maximizes in-flight entanglement when something blocks. Prefer shallow + wide.
- ❌ Authoring explicit review steps when the built-in review pipeline already covers it — enable `ralph plan/step review` instead of hand-rolling reviewer + fix-step plumbing.
- ❌ Skipping `--change-policy optional` on a manual review/audit step — it'll be marked failed for producing no diff.
- ❌ Skipping the verify (deterministic test) step because "review will catch it" — reviewers read a diff, they don't run the code.
- ❌ Trying to make the harness commit instead of letting ralph commit — ralph commits each iteration by design; harness-side commits conflict and produce a clean diff at step end (which `change_policy=required` will correctly fail).

## Reference: useful CLI flags

- `ralph step add --import-json <FILE|->` — bulk insert steps from a JSON array (or one object). Carries the DAG: per-object `short_id` + `depends_on` (parents by short id, or an existing plan step by short id/number). Unique-short-id / acyclic / no-dangling validated; whole batch atomic. The recommended path for anything non-trivial.
- `ralph step add ... <placement>` — on a non-empty plan exactly one placement is required: `--after <S>` (depend on S), `--before <S>` (insert before S; it takes over S's incoming edges), `--depends-on <S>...` (depend on several — a join), or `--root` (explicit independent root). First step of an empty plan is the implied root. Self-edges and cycles are rejected.
- `ralph step dependency add|remove|list <num|short_id> [--depends-on <short_id|num>...]` — edit a step's dependency edges after creation.
- Every `<num>` step selector also accepts the step's stable 8-char `short_id` (shown by `ralph step list` / `ralph plan show`), which survives reordering and inserted corrective steps.
- `ralph step add ... --change-policy {required|optional|forbidden}` — `required` (default) fails on empty diff; `optional` allows it (use for review); `forbidden` fails on any diff (use for read-only audit).
- `ralph step add ... --max-retries <n>` — per-step retry override.
- `ralph step add ... --harness <name>` — per-step harness override.
- `ralph config review set [--harness <h>] [--model <m>] [--enabled <bool>]` — the global built-in-review harness/model/default (only the fields you pass are written).
- `ralph plan review <on|off> <slug>` — per-plan review toggle. `ralph step edit <sel> --review <on|off|inherit>` — per-step override. Precedence: step > plan > global > off (off by default).
- `ralph plan create ... --max-review-corrections <n>` — cap the review→correction recursion (default 3); over the cap, ralph raises a blocker for a human instead of looping.
- `ralph plan create ... --retry-strategy {keep|rollback}` / `ralph step add|edit ... --retry-strategy {keep|rollback}` — how a failed attempt's tree is handled before the retry. `keep` (the default) carries the dirty tree forward; `rollback` reverts to a clean tree and feeds the prior diff into the next prompt. Use `rollback` for steps where a half-done attempt would poison the retry (e.g. partial migrations). `ralph step edit --clear-retry-strategy` drops a step-level override back to plan/global inheritance.
- `ralph plan create ... --squash-on-complete` — by default every per-iteration step commit is kept (full audit trail). With this flag, a step's iteration commits are squashed into one commit when the step completes (the `Ralph-*` trailers are preserved). Opt in only when a clean one-commit-per-step history matters more than the per-iteration trail.
- `ralph skip [<slug>] [--step <n>] --changes {stash|commit|discard}` — skip a step; `--changes` (default `stash`) decides what happens to a killed harness's uncommitted work. `commit` writes a `[ralph wip]` commit with a `Ralph-Skipped-Step` trailer that `ralph step reset` can later revert.
- `ralph harness list` / `ralph harness show <name>` — verify configured harnesses, sandbox modes, and known foot-guns.
- `ralph plan harness set <harness> [<slug>]` — pick the plan-generation harness.
- `ralph step move <num> --to <n>` — reorder steps after creation.
