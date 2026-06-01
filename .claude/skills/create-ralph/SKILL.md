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
- You want per-step retry on failure (a failed attempt's dirty tree is preserved and its test/hook output is fed into the next attempt's prompt; on retry-budget exhaustion ralph raises a blocker so a human can choose retry with the parked changes restored vs. accept-failed instead of going straight terminal).

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

7. **Show the final plan** with `ralph plan show` for user review.

8. **Suggest hooks** (see *Hook Attachment*) for steps that warrant automated post-execution review.

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
  explicit** — on a non-empty plan `ralph step add` *requires* exactly one
  of the following (or the `--after` + `--before` splice combination
  described below):
  - `--after <S>` — the new step depends on `S` (a new branch off `S`);
  - `--before <S>` — insert before `S` (the new step takes over `S`'s
    incoming edges; `S` then depends only on it);
  - `--depends-on <S>` (repeat once per parent: `--depends-on a --depends-on b`) — depend on several prior steps (a fan-in/join). Note: space-separated multi-value (`--depends-on a b`) is *not* supported here because it would swallow the trailing `<plan>` positional;
  - `--root` — a deliberate independent root (no dependencies).

  `--after` and `--before` together is the **splice** operation: the new
  step takes over `--before` step's incoming edges and `--before` step
  then depends on the new step, while the new step depends on `--after`
  step — useful for inserting a step in the middle of a chain. The first
  step of an empty plan is the implied root. `--after`/`--before` are
  **dependency edges, not list position** — there is no positional insert
  (that ambiguity silently produced edge-less DAGs). In
  `--import-json`, the DAG is carried per-object via `id` +
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
  - `ralph step edit <sel> --review on` — review only specific (risky / subtle-criteria) steps; `--review off` exempts one; `--review inherit` defers to plan/global. Precedence is step > plan > global > off. (`--review` is `step edit`-only; `ralph step add` has no `--review` flag inline — set `review_enabled: true` per-object in `--import-json` to opt a new step in at creation time, or `ralph step edit <sel> --review on` immediately after.)
  - Tune the recursion bound with `ralph plan create ... --max-review-corrections <n>` (default 3): if a corrective step keeps failing its own review past this many rounds, ralph raises a blocker for a human instead of looping forever.
- **Explicit review/fix steps are now the exception**, for cases the built-in pipeline doesn't cover (e.g. a whole-plan audit at the end, or a review that must run a command rather than read a diff). If you do author one, set `--change-policy optional` (reviewers produce no diff) and `--max-retries 1` (don't retry-loop on disagreement). A manual reviewer that itself appends fix steps via `ralph step add` needs a sandbox that can write ralph's DB outside the workspace — use the `codex-orchestrator` harness for that step (default `codex`'s `workspace-write` sandbox blocks the DB write silently). A whole-plan audit step should `--depends-on` the steps it audits (so it runs after them); an independent check is `--root`. (`--after`/`--before` are dependency edges now, not "append at the end"; `ralph step move` only changes display/tie-break order, not edges.)

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

## Hook Attachment

Ralph supports lifecycle hooks that run shell commands at specific points
during step execution (pre-step, post-step, pre-test, post-test). The user has
a curated **hook library** of named hooks. You attach hooks by name — you do
NOT invent new shell commands. If a hook the user would benefit from doesn't
exist in the library, tell the user and ask them to create it with
`ralph hooks add`.

- `ralph plan set-hook <slug> --lifecycle <l> --hook <name>` — attach a
  plan-wide hook (fires for every step in the plan). Use this for things like
  "review every completed step".
- `ralph step set-hook <n> <slug> --lifecycle <l> --hook <name>` — attach a
  hook to a specific step. Use this when only certain steps need review,
  linting, or extra checks.
- `ralph plan hooks <slug>` — show all hooks attached to a plan.

Hooks are most useful for post-step review: e.g., if a step is particularly
risky or has subtle acceptance criteria, attach a `post-step` hook that runs
a review agent against the diff. Proactively suggest hooks when a step would
benefit from automated post-execution review.

## Writing good step descriptions

Each step gets a **fresh context** — there is no conversation in step N+1. Prompts must be self-contained.

- **Reference files and conventions explicitly**: which `AGENTS.md` / `CLAUDE.md` sections to read, which files to touch, which patterns to follow. Don't say "the previous step" — refer to specific files or the commit on the plan branch.
- **State concrete acceptance criteria**: "typecheck passes", "`grep -n FOO src/` returns 0 hits", "`cargo test services::user` passes", "route returns 404 not 500". Anything an agent can mechanically verify.
- **Avoid "based on the conversation above"** — there is no conversation. Cite files and signals the next step can find on its own.
- **Reference existing patterns**: "follow the pattern in `src/services/auth.rs`" beats abstract instructions.
- **Keep one concern per step**: don't combine "add the model" and "add the API endpoint".
- **Order dependencies correctly**: types before uses, modules before imports.

## Writing acceptance criteria (push intent into criteria, or expect the gap)

The single most common plan failure: **a ralph satisfies your acceptance criteria, not your intent.** Concrete example: if a "compatibility contract" lists `config.theme.transparent` as a kept symbol, a ralph will make the field assignable and call it done — it will not make the renderer actually honor the setting unless a criterion says so. The same trap applies anywhere a name appears in a spec without its effect: feature flags, CLI options, env vars, HTTP params, library surfaces, exported types, database columns. Spec gaps don't get filled in by good taste; they get filled by whichever interpretation is cheapest.

Push intent into criteria — concretely:

1. **Pair every named symbol with an observable behavior.** "`X` is a supported / kept surface" is half a spec — a ralph will literally make the field assignable / the function callable / the flag accepted and stop. Split it into (a) the symbol exists, and (b) "with `X = <value>` (or after calling `X(...)`), <observable thing> happens." Symbol-existence catches a missing rename; behavior catches a no-op stub.
2. **For replaced components, separate design direction from API contract.** "Replace library/module X with Y" is a *design* choice. "Existing callers of `X.foo(...)` must continue to work" is an *API* choice (a shim). They are different specs — name both, or explicitly mark one as out of scope. Otherwise the ralph reasonably concludes "drop X entirely" and breaks any caller still relying on X's surface.
3. **For every kept component, name its load/wiring contract.** "Keep dependency Z" leaves *how* Z is wired unspecified — a ralph optimizing for startup time, bundle size, or cold latency will reasonably pick the laziest option and silently break whatever depended on Z being already-loaded. State the wiring you want: eager init, lazy on `<trigger>`, registered as `<role>` in `<container>`, etc.
4. **Prefer real-world fixtures over synthetic ones.** A test against a synthetic input catches structural breakage; a test against a real captured input (a maintainer's actual config, a recorded request, a production data snapshot, a known-broken legacy file) catches what users actually do. One criterion of "passes under `tests/fixtures/<real-captured-input>`" beats a dozen "the module loads under a minimal hand-written input."
5. **Make every criterion mechanically checkable.** "It works" / "default behavior is correct" / "feels fast" are wishes. "`<cmd>` exits 0", "GET `/foo` returns 200 with body matching `<regex>`", "operation completes within 200ms p99", "`grep -n FOO src/` returns 0 hits", "the `users` table has exactly N rows after seeding" are tests. If a criterion can't be a shell command, a grep, an exit code, an HTTP status, a latency budget, or an observable file/state change, rephrase it until it can — or accept that nothing will verify it and the ralph will not invent the test for you.

Apply this at two scopes: the **plan-level test commands** (`--test <cmd>`) gate every step, so they're where load-bearing whole-system behavior lives; **per-step acceptance criteria** in step descriptions are where step-local symbol/behavior pairs live. A plan with only symbol-existence in step criteria and only a build/typecheck command in plan tests is the canonical failure mode — it will compile, the symbols will exist, and the behaviors will be silently absent.

## Review verdicts (built-in pipeline and manual review steps)

The **built-in review pipeline** handles the verdict/correction loop for
you: the reviewer harness is told to emit a structured verdict, ralph
parses pass/fail, records the verdict as a git note (it never amends the
commit), and on a failure
auto-inserts a corrective step and re-parents dependents — you don't write
the "append a fix step" logic yourself. Just pick a capable reviewer
harness/model via `ralph config review set` and enable review at the plan
or step level.

If you still author an **explicit manual review step** for a case the
built-in pipeline doesn't cover:

- Tell the reviewer to read `AGENTS.md` / `CLAUDE.md` and per-phase patterns explicitly — most harnesses don't load them by default.
- Have it emit a **structured verdict** so a following step (or a human) can parse it: a single line of either `REVIEW PASS — <one-line summary>` or `REVIEW FAIL — N issue(s)`, followed by a numbered list when failing. (This is the same verdict shape the built-in reviewer is instructed to produce, so the convention is consistent.)
- If that manual step must itself append fix steps via `ralph step add`, **describe the action in prose**, not as a copy-paste shell snippet — that re-introduces the embedded-quoting trap — and give it the `codex-orchestrator` harness so the out-of-workspace DB write isn't blocked.

## Branching

Use `--branch feat/<slug>` so reviewers can run `git diff main..HEAD` from any step to see the cumulative work-to-date. Ralph commits **once per step**, only after that step's deterministic tests pass — subject `ralph <short_id>.<n> - <title>` plus `Ralph-*` trailers, where `<n>` is the attempt number that finally passed. History stays linear, one branch per plan; the per-attempt audit trail lives in `execution_logs` (prompt / harness output / test output / diff per attempt, including failed ones). Failed attempts leave the dirty tree on disk so the next attempt can build on top — only the test/hook output from the prior attempt is injected into the next attempt's prompt; the diff is *not* repeated, so the next attempt must read the worktree itself (e.g. `git diff`, `git status`) to see what changed. When the retry budget is exhausted the dirty tree from the last failed attempt is parked (stashed) and the blocker offers a "Retry step with parked changes" option that restores it, or a "Mark step Failed" option. That commit shape is how rollback, per-step diff isolation, and the built-in reviewer's fixed-SHA review all work, so don't try to disable it.

## Plan size

Pick granularity based on the work, not a target count. Rough bands:

- bugfix: 3–5 steps
- small feature: 5–15
- medium feature: 15–40
- large refactor or greenfield: 40–300

Don't compress a big task into a handful of mega-steps — you lose the per-step checkpointing, retry, and per-step diff isolation. Don't inflate a small task into trivial steps either. Whatever the size, every step must be atomic and independently verifiable.

## Guidelines

- Each step should be atomic and independently verifiable.
- Steps should be ordered so that earlier steps don't depend on later ones.
- Include enough context in each step description that an agent can execute it without seeing other steps.
- Deterministic tests should validate the overall project health after each step.
- Prefer smaller, focused steps over large monolithic ones.

## Anti-patterns

- ❌ Referencing "the previous step" by name in a prompt — refer to files or commits (a DAG has no positional "previous").
- ❌ Long inline `--description "..."` with mixed quoting — silent truncation. Use `--import-json` or a quoted heredoc → tempfile.
- ❌ Inventing dependency edges that aren't real ordering constraints — it serializes work the scheduler could have run independently and kills the DAG payoff.
- ❌ Expecting `--after <N>` to mean "insert at list position N" — it does not exist any more. `--after`/`--before` are **dependency edges**. Array order in `--import-json` is not a dependency either; only `depends_on` is.
- ❌ Bulk-inserting with `--import-json` and forgetting `depends_on` — you get an all-roots, edge-less plan that runs but has none of the gating/ordering you intended. Put the graph in the JSON.
- ❌ Deep independent branches that all re-converge at one late join — maximizes in-flight entanglement when something blocks. Prefer shallow + wide.
- ❌ Authoring explicit review steps when the built-in review pipeline already covers it — enable `ralph plan/step review` instead of hand-rolling reviewer + fix-step plumbing.
- ❌ Skipping `--change-policy optional` on a manual review/audit step — it'll be marked failed for producing no diff.
- ❌ Using `codex` (default sandbox) for a manual review step that appends fix steps via `ralph step add` — writes to the ralph DB silently fail. Use `codex-orchestrator`.
- ❌ Skipping the verify (deterministic test) step because "review will catch it" — reviewers read a diff, they don't run the code.
- ❌ Listing a symbol as a "compatibility surface" without naming the behavior it implies — a ralph will make the field assignable / the function callable and stop there. Pair every named symbol with an observable behavior, or expect a no-op stub.
- ❌ Acceptance criteria that aren't mechanically checkable ("it works", "feels fast", "default behavior is correct", "looks right") — these are wishes, not specs. A criterion is a shell command, a `grep`, an exit code, an HTTP status, a latency budget, or an observable file/state change, or the ralph will not verify it.
- ❌ Trying to make the harness commit instead of letting ralph commit — ralph commits once per step on test-pass by design; harness-side commits leave the post-commit working tree clean (which `change_policy=required` will correctly fail) and break the reviewer's fixed-SHA contract.

## Reference: useful CLI flags

- `ralph step add --import-json <FILE|->` — bulk insert steps from a JSON array (or one object). Carries the DAG: per-object `id` + `depends_on` (parents by the batch-local `id`, or an existing plan step by short id/number; `short_id` may also be supplied to pin an exported handle). Unique-id / acyclic / no-dangling validated; whole batch atomic. The recommended path for anything non-trivial.
- `ralph step add ... <placement>` — on a non-empty plan exactly one placement is required (or the `--after` + `--before` splice combination): `--after <S>` (depend on S), `--before <S>` (insert before S; it takes over S's incoming edges), `--depends-on <S>` (depend on several — a join; **repeat the flag** once per parent: `--depends-on a --depends-on b`. Space-separated multi-value is not supported, it would swallow the trailing `<plan>` positional), or `--root` (explicit independent root). `--after` + `--before` together splices: the new step takes over `--before`'s incoming edges and `--before` then depends on the new step, while the new step depends on `--after` — useful for inserting a step in the middle of a chain. First step of an empty plan is the implied root. Self-edges and cycles are rejected.
- `ralph step dependency add|remove|list <num|short_id> [--depends-on <short_id|num>...]` — edit a step's dependency edges after creation.
- Every `<num>` step selector also accepts the step's stable 8-char `short_id` (shown by `ralph step list` / `ralph plan show`), which survives reordering and inserted corrective steps.
- `ralph step add ... --change-policy {required|optional}` — `required` (default) fails on empty diff; `optional` allows it (use for review).
- `ralph step add ... --max-retries <n>` — per-step retry override.
- `ralph step add ... --harness <name>` — per-step harness override.
- `ralph step add ... --model <name>` — per-step model override (forwarded to the harness's `model_args` template, e.g. `--model sonnet-4.6`); silently ignored if the resolved harness has no `model_args` configured.
- `ralph config review set [--harness <h>] [--model <m>] [--enabled <bool>]` — the global built-in-review harness/model/default (only the fields you pass are written).
- `ralph plan review <on|off> <slug>` — per-plan review toggle. `ralph step edit <sel> --review <on|off|inherit>` — per-step override. Precedence: step > plan > global > off (off by default).
- `ralph plan create ... --max-review-corrections <n>` — cap the review→correction recursion (default 3); over the cap, ralph raises a blocker for a human instead of looping.
- `ralph skip [<slug>] [--step <n>] --changes {stash|commit|discard}` — skip a step; `--changes` (default `stash`) decides what happens to a killed harness's uncommitted work. `commit` writes a `[ralph wip]` commit with a `Ralph-Skipped-Step` trailer that `ralph step reset` can later revert.
- `ralph harness list` / `ralph harness show <name>` — verify configured harnesses, sandbox modes, and known foot-guns.
- `ralph plan harness set <harness> [<slug>]` — pick the plan-generation harness.
- `ralph step move <num> --to <n>` — reorder steps after creation.
