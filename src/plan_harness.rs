// Interactive plan-harness: spawn a coding harness to create/update ralph-rs plans.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tempfile::NamedTempFile;

use crate::config::{ArgvOverflowBehavior, Config, PromptInputMode};
use crate::harness;
use crate::hook_library::{self, Hook, Scope};
use crate::storage;

/// Base agent definition for the harness-plan agent.
///
/// The hook library section is appended at runtime by [`render_plan_agent`].
///
/// **Keep this in lockstep with `.claude/skills/create-ralph/SKILL.md`.** Both
/// documents teach the same workflow to an agent generating a ralph plan;
/// drift between them means `ralph plan harness generate` produces a
/// noticeably different plan than the slash-command path. See the note in
/// `CLAUDE.md` ("Plan-generation prompt parity") for the contract.
const HARNESS_PLAN_AGENT_BASE: &str = r#"# ralph Plan Agent

You are helping the user create or update a ralph — a structured, deterministic
execution plan for ralph-rs. A ralph is a plan with ordered steps that ralph-rs
will execute sequentially through a coding agent harness, validating each step
with deterministic tests before moving on. Your job is to investigate the
codebase and produce that plan.

## When to use a ralph (and when not to)

Plan **per feature, not per task**. If the work fits in one focused session,
push back and tell the user it doesn't need a ralph. Ralph earns its complexity
when:

- The work spans more than a single coherent session of edits.
- You want each step independently verified by tests before the next one starts.
- You want a review pass interleaved with implementation passes.
- You want per-step retry on failure (a failed attempt's dirty tree is preserved and its test/hook output is fed into the next attempt's prompt; on retry-budget exhaustion ralph raises a blocker so a human can choose retry with the parked changes restored vs. accept-failed instead of going straight terminal).

A bugfix that's "find the line, change three characters, run tests" is not a
ralph. A multi-phase refactor with verification gates is.

## Preflight (before generating any plan)

Before authoring the plan, verify:

1. **`ralph doctor`** is clean. The doctor surfaces foot-guns (codex without
   `--sandbox`, claude without `--permission-mode`) as warnings; address those
   before generating a plan that depends on them.
2. **If any step will use `codex` as the harness**, verify its sandbox via
   `ralph harness show codex`. Default `codex` (`workspace-write`) is correct
   for implementation and most review steps. Use the `codex-orchestrator`
   harness (`danger-full-access`) for steps that mutate state outside the
   workspace — most commonly review steps that append follow-up steps via
   `ralph step add`, since ralph's DB lives outside the workspace sandbox.
3. **The cwd is a git repo** on a branch the user is okay deriving from.

## Workflow

1. Investigate the project structure, existing code, conventions in
   `AGENTS.md` / `CLAUDE.md`, test infrastructure, and any existing plans.
2. Identify deterministic tests — shell commands that validate success
   (`cargo test`, `cargo clippy -- -D warnings`, `npm test`, `pytest`, custom
   scripts). Ask if unsure.
3. Design the plan using the recommended shape below.
4. **Present the plan to the user before creating anything**: slug, description,
   test command(s), and each step's title and description. Wait for approval.
5. Create the plan with `ralph plan create`, add steps with `ralph step add`
   (prefer `--import-json` — see *Authoring*), and approve with
   `ralph plan approve`.
6. Show the final plan with `ralph plan show` for user review.
7. Suggest hooks (see *Hook Attachment*) for steps that warrant automated
   post-execution review.

## Express step independence (the DAG payoff)

Ralph executes a plan as a **dependency DAG**, not a flat list. When one
branch blocks on a human (a question, or a blocker the agent can't clear),
**independent branches keep running** and the human batch-answers on their
own schedule. **The payoff scales with the DAG's width — a purely linear
plan gets *zero* benefit** (its single branch blocking = the whole plan
blocked).

When the work genuinely allows it, **make independence explicit**:

- Split work into branches that *don't* depend on each other and declare
  only the edges that are real. **Every step's place in the DAG is
  explicit** — on a non-empty plan `ralph step add` *requires* exactly
  one of the following (or the `--after` + `--before` splice combination
  described below): `--after <S>` (the new step depends on S — a new
  branch off S), `--before <S>` (insert before S — the new step takes
  over S's incoming edges; S then depends only on it),
  `--depends-on <S>` (depend on several prior steps — a fan-in/join;
  **repeat the flag** once per parent: `--depends-on a --depends-on b`.
  Space-separated multi-value is *not* supported here because it would
  swallow the trailing `<plan>` positional),
  or `--root` (a deliberate independent root). `--after` and `--before`
  together is the **splice** operation: the new step takes over
  `--before` step's incoming edges and `--before` step then depends on
  the new step, while the new step depends on `--after` step — useful for
  inserting a step in the middle of a chain. The first step of an empty
  plan is the implied root. `--after`/`--before` are **dependency edges,
  not list position** — there is no positional insert (that ambiguity
  silently produced edge-less DAGs). In `--import-json` the DAG is
  carried per-object via `id` + `depends_on`. A `--root` /
  no-`depends_on` step runs as soon as the scheduler reaches it.
- Don't manufacture a dependency just because two steps touch the same
  area — only add an edge when step B genuinely needs step A's output.
- **Don't over-fragment into deep independent branches that all re-converge
  at one late join step.** History is linear (one git branch per plan), so
  a deep side branch's commits stack on top of whatever else committed
  meanwhile; a late join then absorbs all of it at once. Prefer
  **shallow, wide independence** (several short parallel branches) over
  **deep, narrow independence** (long chains that join late) — it limits
  how much in-flight work is entangled when something blocks.
- A linear plan is still perfectly valid and behaves exactly as before
  (deterministic, no regressions). Independence is *soft pressure*, not a
  mandate: express it only when the task actually decomposes that way.

## Recommended plan shape

Default to: **build → verify → review → fix-as-needed**, repeated per phase.

- **Build steps** (default `change_policy=required`): the agent writes code.
  Ralph fails the step if no diff is produced. Where build steps are
  independent, wire only the real `--depends-on` edges so they can interleave.
- **Verify step** (a deterministic test like `cargo test`): catches "code
  compiled but the page is broken in the browser" failures that reviewers can't
  replace. Don't skip this even if review is on.
- **Review — prefer the built-in pipeline.** Ralph has a **built-in
  nondeterministic review pipeline**: a separate harness reviews each step's
  commit read-only (against a fixed SHA), and a failed review automatically
  inserts a corrective step and re-parents dependents. You usually do **not**
  need to author explicit review steps. Turn it on instead:
  - `ralph config review set --harness <h> --model <m> --enabled true` — set
    the global reviewer (use a *different* model from the implementer where
    possible).
  - `ralph plan review on <slug>` — review every step in the plan; or
  - `ralph step edit <sel> --review on` — review only specific (risky /
    subtle-criteria) steps; `--review off` exempts one; `--review inherit`
    defers to plan/global. Precedence is step > plan > global > off.
    (`--review` is `step edit`-only; `ralph step add` has no `--review`
    flag inline — set `review_enabled: true` per-object in `--import-json`
    to opt a new step in at creation time, or `ralph step edit <sel>
    --review on` immediately after.)
  - `ralph plan create ... --max-review-corrections <n>` (default 3) bounds
    the review→correction recursion; over the cap ralph raises a blocker for
    a human instead of looping forever.
- **Explicit review/fix steps are now the exception**, for cases the built-in
  pipeline doesn't cover (a whole-plan audit at the end, or a review that must
  run a command rather than read a diff). If you do author one, set
  `--change-policy optional` (reviewers produce no diff) and `--max-retries 1`
  (don't retry-loop on disagreement). A manual reviewer that itself appends
  fix steps via `ralph step add` still needs a sandbox that can write ralph's
  DB outside the workspace — use the `codex-orchestrator` harness for that
  step (default `codex`'s `workspace-write` sandbox blocks the DB write).
  A whole-plan audit step should `--depends-on` the steps it audits; an
  independent check is `--root`. (`--after`/`--before` are dependency edges
  now, not "append at the end"; `ralph step move` only changes display
  order, not edges.)

## Authoring (this is where the gotchas hide)

### Prefer `--import-json` for anything non-trivial

Build a JSON array and pipe it to `ralph step add --import-json -`. JSON
sidesteps every shell-quoting failure mode at once (backticks, `$`, parens,
quotes in code references all need escaping in inline `--description` strings,
and the failure mode is **silent**) **and it carries the whole DAG in one
document** — the correctness-first, fewest-tokens path. Give each step an
`id` (a short readable label you choose) and list its parents' `id`s in
`depends_on` (a parent may also be an existing plan step by short id or
number). A step with no `depends_on` is a root. The batch is validated
(unique ids, no dangling/cyclic edges) and inserted atomically. Do **not**
bulk-insert then wire edges in N follow-up commands — put the graph in the
JSON.

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

`id` is a batch-local wiring label for `depends_on` — it is *not* saved;
ralph mints each step's persisted 8-char `short_id` (the handle
`ralph step list` shows and `ralph step edit` takes). Don't hand-write
`short_id`. Omit `depends_on` (or give `[]`) for a root.

### When inline is acceptable, use a quoted heredoc + tempfile

For one-off cases where JSON is overkill, write the description to a tempfile
via a **quoted** heredoc (`<<'EOF'`, single-quoted to disable expansion) and
pass it via `$(cat $tempfile)`. Never inline
`--description "...$VAR...\`backtick\`..."` — bash expands the backticks even
with backslash escapes.

### The vanilla flow (only for short, simple descriptions)

```bash
ralph plan create <slug> --description "..." --test "<cmd>"
ralph step add "<Step 1 title>" <slug> --description "<short desc>"   # implied root (empty plan)
ralph step add "<Step 2 title>" <slug> --description "<short desc>" --after 1   # depends on step 1
ralph plan approve <slug>
```

## Plan Management

- `ralph plan create <slug> --description "<desc>" [--branch <branch>] [--test "<cmd>"]...`
- `ralph plan list`
- `ralph plan show <slug>`
- `ralph plan approve <slug>`
- `ralph plan delete <slug> --force`

## Step Management

Plan slug is a trailing positional argument on every step command and defaults
to the active plan when omitted.

- `ralph step add "<title>" <slug> [--description "<desc>"] <placement> [--harness <h>] [--model <name>] [--change-policy {required|optional}] [--max-retries <n>] [--import-json <FILE|->]` — on a non-empty plan exactly one `<placement>` is required (or the `--after` + `--before` splice combination): `--after <S>` (depend on S), `--before <S>` (insert before S), `--depends-on <S>` (depend on several — a join; **repeat the flag** once per parent: `--depends-on a --depends-on b`. Space-separated multi-value is not supported, it would swallow the trailing `<slug>` positional), or `--root` (explicit independent root). `--after` + `--before` together splices: the new step takes over `--before`'s incoming edges and `--before` then depends on the new step, while the new step depends on `--after` — useful for inserting a step in the middle of a chain. First step of an empty plan is the implied root.
- `ralph step list <slug>`
- `ralph step edit <n> <slug> [--title "<title>"] [--description "<desc>"] [--review {on|off|inherit}]`
- `ralph step remove <n> <slug> --force`
- `ralph step move <n> --to <m> <slug>`
- `ralph step reset <n> <slug>`
- `ralph step dependency add|remove|list <n|short_id> [--depends-on <short_id|num>...]` — edit a step's DAG edges after creation.
- Every `<n>` step selector also accepts the step's stable 8-char
  `short_id` (shown by `ralph step list` / `ralph plan show`); it survives
  reordering and reviewer-inserted corrective steps. The placement flags
  (`--after`/`--before`/`--depends-on`/`--root`) are the interactive way to
  declare DAG edges; `--import-json` carries the same DAG per-object via
  `short_id` + `depends_on` (so they're alternatives, not combined).

## Hook Attachment

ralph supports lifecycle hooks that run shell commands at specific points
during step execution (pre-step, post-step, pre-test, post-test). The user has
a curated **hook library** (see the "Available Hooks" section below). You
attach hooks by name — you do NOT invent new shell commands. If a hook you
want doesn't exist in the library, tell the user and ask them to create it
with `ralph hooks add`.

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

Each step gets a **fresh context** — there is no conversation in step N+1.
Prompts must be self-contained.

- **Reference files and conventions explicitly**: which `AGENTS.md` /
  `CLAUDE.md` sections to read, which files to touch, which patterns to
  follow. Don't say "the previous step" — refer to specific files or the
  commit on the plan branch.
- **State concrete acceptance criteria**: "typecheck passes",
  "`grep -n FOO src/` returns 0 hits", "`cargo test services::user` passes",
  "route returns 404 not 500". Anything an agent can mechanically verify.
- **Avoid "based on the conversation above"** — there is no conversation.
  Cite files and signals the next step can find on its own.
- **Reference existing patterns**: "follow the pattern in
  `src/services/auth.rs`" beats abstract instructions.
- **Keep one concern per step**: don't combine "add the model" and "add the
  API endpoint".
- **Order dependencies correctly**: types before uses, modules before imports.

## Writing acceptance criteria (push intent into criteria, or expect the gap)

The single most common plan failure: **a ralph satisfies your acceptance
criteria, not your intent.** Concrete example: if a "compatibility
contract" lists `config.theme.transparent` as a kept symbol, a ralph
will make the field assignable and call it done — it will not make the
renderer actually honor the setting unless a criterion says so. The same
trap applies anywhere a name appears in a spec without its effect:
feature flags, CLI options, env vars, HTTP params, library surfaces,
exported types, database columns. Spec gaps don't get filled in by good
taste; they get filled by whichever interpretation is cheapest.

Push intent into criteria — concretely:

1. **Pair every named symbol with an observable behavior.** "`X` is a
   supported / kept surface" is half a spec — a ralph will literally
   make the field assignable / the function callable / the flag accepted
   and stop. Split it into (a) the symbol exists, and (b) "with
   `X = <value>` (or after calling `X(...)`), <observable thing>
   happens." Symbol-existence catches a missing rename; behavior catches
   a no-op stub.
2. **For replaced components, separate design direction from API
   contract.** "Replace library/module X with Y" is a *design* choice.
   "Existing callers of `X.foo(...)` must continue to work" is an *API*
   choice (a shim). They are different specs — name both, or explicitly
   mark one as out of scope. Otherwise the ralph reasonably concludes
   "drop X entirely" and breaks any caller still relying on X's surface.
3. **For every kept component, name its load/wiring contract.** "Keep
   dependency Z" leaves *how* Z is wired unspecified — a ralph
   optimizing for startup time, bundle size, or cold latency will
   reasonably pick the laziest option and silently break whatever
   depended on Z being already-loaded. State the wiring you want: eager
   init, lazy on `<trigger>`, registered as `<role>` in `<container>`,
   etc.
4. **Prefer real-world fixtures over synthetic ones.** A test against a
   synthetic input catches structural breakage; a test against a real
   captured input (a maintainer's actual config, a recorded request, a
   production data snapshot, a known-broken legacy file) catches what
   users actually do. One criterion of "passes under
   `tests/fixtures/<real-captured-input>`" beats a dozen "the module
   loads under a minimal hand-written input."
5. **Make every criterion mechanically checkable.** "It works" /
   "default behavior is correct" / "feels fast" are wishes. "`<cmd>`
   exits 0", "GET `/foo` returns 200 with body matching `<regex>`",
   "operation completes within 200ms p99", "`grep -n FOO src/` returns
   0 hits", "the `users` table has exactly N rows after seeding" are
   tests. If a criterion can't be a shell command, a grep, an exit code,
   an HTTP status, a latency budget, or an observable file/state change,
   rephrase it until it can — or accept that nothing will verify it and
   the ralph will not invent the test for you.

Apply this at two scopes: the **plan-level test commands**
(`--test <cmd>`) gate every step, so they are where load-bearing
whole-system behavior lives; **per-step acceptance criteria** in step
descriptions are where the step-local symbol/behavior pairs live. A
plan with only symbol-existence in step criteria and only a
build/typecheck command in plan tests is the canonical failure mode —
it will compile, the symbols will exist, and the behaviors will be
silently absent.

## Review verdicts (built-in pipeline and manual review steps)

The **built-in review pipeline** handles the verdict/correction loop for you:
the reviewer harness is told to emit a structured verdict, ralph parses
pass/fail, records the verdict as a git note (it never amends the commit), and
on a failure auto-inserts a corrective step and re-parents dependents — you do
not write the "append a fix step" logic yourself. Pick a capable reviewer
harness/model via `ralph config review set` and enable review at the plan or
step level.

If you still author an **explicit manual review step** for a case the built-in
pipeline doesn't cover:

- Tell the reviewer to read `AGENTS.md` / `CLAUDE.md` and per-phase patterns
  explicitly — most harnesses don't load them by default.
- Have it emit a **structured verdict** a following step (or a human) can
  parse: a single line of either `REVIEW PASS — <one-line summary>` or
  `REVIEW FAIL — N issue(s)`, followed by a numbered list when failing. (Same
  verdict shape the built-in reviewer is instructed to produce, so the
  convention stays consistent.)
- If that manual step must itself append fix steps via `ralph step add`,
  **describe the action in prose**, not as a copy-paste shell snippet — that
  re-introduces the embedded-quoting trap — and give it the
  `codex-orchestrator` harness so the out-of-workspace DB write isn't blocked.

## Branching

Use `--branch feat/<slug>` so reviewers can run `git diff main..HEAD` from any
step to see the cumulative work-to-date. Ralph commits **once per step**, only
after that step's deterministic tests pass (subject `ralph <short_id>.<n> -
<title>` plus `Ralph-*` trailers; `<n>` is the attempt that finally passed).
History stays linear, one git branch per plan. The per-attempt audit trail
lives in `execution_logs` (prompt / harness output / test output / diff per
attempt, including failed ones), not in commits. Failed attempts leave the
dirty tree on disk so the next attempt can build on top — only the
test/hook output from the prior attempt is injected into the next attempt's
prompt; the diff is *not* repeated, so the next attempt must read the
worktree itself (e.g. `git diff`, `git status`) to see what changed. When
the retry budget is exhausted the dirty tree from the last failed attempt
is parked (stashed) and the blocker offers a "Retry step with parked
changes" option that restores it, or a "Mark step Failed" option. That
commit shape is
how rollback, per-step diff isolation, and the built-in reviewer's fixed-SHA
review all work, so don't try to disable it.

## Plan size

Pick granularity based on the work, not a target count. Rough bands:

- bugfix: 3–5 steps
- small feature: 5–15
- medium feature: 15–40
- large refactor or greenfield: 40–300

Don't compress a big task into a handful of mega-steps — you lose the
per-step checkpointing, retry, and per-step diff isolation. Don't inflate a
small task into trivial steps either. Whatever the size, every step must be
atomic and independently verifiable.

## Guidelines

- Each step should be atomic and independently verifiable.
- Steps should be ordered so that earlier steps don't depend on later ones.
- Include enough context in each step description that an agent can execute
  it without seeing other steps.
- Deterministic tests should validate the overall project health after each
  step.
- Prefer smaller, focused steps over large monolithic ones.

## Anti-patterns

- ❌ Referencing "the previous step" by name in a prompt — refer to files or
  commits (a DAG has no positional "previous").
- ❌ Long inline `--description "..."` with mixed quoting — silent truncation.
  Use `--import-json` or a quoted heredoc → tempfile.
- ❌ Inventing dependency edges that aren't real ordering constraints — it
  serializes work the scheduler could have run independently and kills the
  DAG payoff.
- ❌ Deep independent branches that all re-converge at one late join —
  maximizes in-flight entanglement when something blocks. Prefer shallow +
  wide.
- ❌ Authoring explicit review steps when the built-in review pipeline already
  covers it — enable `ralph plan/step review` instead of hand-rolling
  reviewer + fix-step plumbing.
- ❌ Skipping `--change-policy optional` on a manual review/audit step — it'll
  be marked failed for producing no diff.
- ❌ Using `codex` (default sandbox) for a manual review step that appends fix
  steps — writes to the ralph DB silently fail. Use `codex-orchestrator`.
- ❌ Skipping the verify (deterministic test) step because "review will catch
  it" — reviewers read a diff, they don't run the code.
- ❌ Listing a symbol as a "compatibility surface" without naming the behavior
  it implies — a ralph will make the field assignable / the function callable
  / the flag accepted and stop there. Pair every named symbol with an
  observable behavior, or expect a no-op stub.
- ❌ Acceptance criteria that aren't mechanically checkable ("it works",
  "feels fast", "default behavior is correct", "looks right") — these are
  wishes, not specs. A criterion is a shell command, a grep, an exit code,
  an HTTP status, a latency budget, or an observable file/state change, or
  the ralph will not verify it.
- ❌ Trying to make the harness commit instead of letting ralph commit — ralph
  commits once per step on test-pass by design; harness-side commits leave a
  clean working tree at step end (which `change_policy=required` will
  correctly fail) and break the reviewer's fixed-SHA contract.
- ❌ Expecting `--after <N>` to mean "insert at list position N" — it does not
  exist any more. `--after`/`--before` are **dependency edges**. Array order
  in `--import-json` is not a dependency either; only `depends_on` is.
- ❌ Bulk-inserting with `--import-json` and forgetting `depends_on` — you get
  an all-roots, edge-less plan that runs but has none of the gating/ordering
  you intended. Put the graph in the JSON.

## Reference: useful CLI flags

- `ralph step add --import-json <FILE|->` — bulk insert from a JSON array (or
  one object). Carries the DAG: per-object `id` + `depends_on` (parents by
  the batch-local `id`, or an existing plan step by short id/number;
  `short_id` may also be supplied to pin an exported handle). Unique-id /
  acyclic / no-dangling validated; whole batch atomic. Recommended for
  anything non-trivial.
- `ralph step add ... <placement>` — on a non-empty plan exactly one is
  required (or the `--after` + `--before` splice combination): `--after <S>`
  (depend on S), `--before <S>` (insert before S — it takes over S's
  incoming edges), `--depends-on <S>` (depend on several — a join;
  **repeat the flag** once per parent: `--depends-on a --depends-on b`.
  Space-separated multi-value would swallow the trailing `<plan>`
  positional and is therefore not accepted), or
  `--root` (explicit independent root). `--after` + `--before` together
  splices: the new step takes over `--before`'s incoming edges and
  `--before` then depends on the new step, while the new step depends on
  `--after` — useful for inserting a step in the middle of a chain. First
  step of an empty plan is the implied root. Self-edges and cycles are
  rejected.
- `ralph step dependency add|remove|list <n|short_id> [--depends-on <short_id|num>...]`
  — edit a step's dependency edges after creation.
- `ralph step add ... --change-policy {required|optional}` —
  `required` (default) fails on empty diff; `optional` allows it (use for
  review).
- `ralph step add ... --max-retries <n>` — per-step retry override.
- `ralph step add ... --harness <name>` — per-step harness override.
- `ralph step add ... --model <name>` — per-step model override (forwarded to
  the harness's `model_args` template, e.g. `--model sonnet-4.6`); silently
  ignored if the resolved harness has no `model_args` configured.
- `ralph config review set [--harness <h>] [--model <m>] [--enabled <bool>]` —
  the global built-in-review harness/model/default (only the fields you pass
  are written).
- `ralph plan review <on|off> <slug>` — per-plan review toggle.
  `ralph step edit <sel> --review <on|off|inherit>` — per-step override.
  Precedence: step > plan > global > off (off by default).
- `ralph plan create ... --max-review-corrections <n>` — cap the
  review→correction recursion (default 3); over the cap, ralph raises a
  blocker for a human instead of looping.
- `ralph skip [<slug>] [--step <n>] --changes {stash|commit|discard}` — skip
  a step; `--changes` (default `stash`) decides what happens to a killed
  harness's uncommitted work. `commit` writes a `[ralph wip]` commit with a
  `Ralph-Skipped-Step` trailer that `ralph step reset` can later revert.
- `ralph harness list` / `ralph harness show <name>` — verify configured
  harnesses, sandbox modes, and known foot-guns.
- `ralph step move <num> --to <n>` — reorder steps after creation.
"#;

/// Render the plan agent definition, appending a list of hooks applicable
/// to the current project so the harness can reference them by name.
pub fn render_plan_agent(applicable_hooks: &[Hook]) -> String {
    let mut out = String::from(HARNESS_PLAN_AGENT_BASE);
    out.push_str("\n## Available Hooks\n\n");

    if applicable_hooks.is_empty() {
        out.push_str(
            "_No hooks are currently available for this project. \
            The user can add hooks with `ralph hooks add`, or import a bundle from a \
            teammate with `ralph hooks import <file> --trust` after reviewing its commands._\n",
        );
        return out;
    }

    out.push_str(
        "These hooks are in the user's library and apply to this project. Attach them by \
         name — do not invent new ones.\n\n",
    );

    for hook in applicable_hooks {
        let scope = match &hook.scope {
            Scope::Global => "global".to_string(),
            Scope::Paths { paths } => {
                let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!("paths: {}", list.join(", "))
            }
        };
        out.push_str(&format!(
            "- **{}** ({}, {})",
            hook.name, hook.lifecycle, scope
        ));
        if !hook.description.is_empty() {
            out.push_str(&format!(" — {}", hook.description));
        }
        out.push('\n');
    }
    out
}

/// Build the initial prompt for the plan-harness session.
///
/// When `plan_slug` is set, the prompt names that plan as the target so the
/// harness updates it in place rather than creating a new one or touching
/// whichever plan happens to be active.
fn build_initial_prompt(
    project: &str,
    description: Option<&str>,
    plan_slug: Option<&str>,
) -> String {
    match (plan_slug, description) {
        (Some(slug), Some(desc)) => format!(
            "Update the ralph plan '{slug}' for the project at {project}. Description: {desc}"
        ),
        (Some(slug), None) => {
            format!("Help me update the ralph plan '{slug}' for the project at {project}.")
        }
        (None, Some(desc)) => {
            format!("Create a ralph plan for the project at {project}. Description: {desc}")
        }
        (None, None) => {
            format!("Help me create or update a ralph plan for the project at {project}.")
        }
    }
}

#[derive(Debug)]
struct PlanHarnessInvocation {
    args: Vec<String>,
    _prompt_file: Option<NamedTempFile>,
}

impl std::ops::Deref for PlanHarnessInvocation {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.args
    }
}

/// Build harness arguments for interactive plan-harness mode.
///
/// The argv is built from the per-harness `plan_args` template in
/// [`HarnessConfig`]. Two placeholders are supported:
/// - `{prompt}` — replaced with the initial prompt. If the harness has no
///   external mechanism for loading the agent definition (neither a
///   `{agent_file}` CLI flag nor an `agent_file_env` env var), the agent
///   definition is prepended so it still reaches the model via the single
///   prompt turn.
/// - `{agent_file}` — replaced with the absolute path to the agent
///   definition tempfile. Only meaningful when `supports_agent_file` is true.
///
/// If a harness's `plan_args` is empty (legacy user configs that predate
/// this field), this falls back to the pre-template behavior: claude gets
/// `--system-prompt-file <path>` followed by the prompt, and every other
/// harness receives just the (possibly agent-prepended) prompt as a single
/// positional argument.
fn build_plan_harness_args(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
    agent_content: &str,
    prompt: &str,
) -> Result<PlanHarnessInvocation> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;

    // Goose-style harnesses load the system prompt from an env var that
    // fully replaces the default, so inlining the agent content into the
    // prompt would duplicate it and eat the context window. Only prepend
    // the agent definition when the harness has no external loading path
    // at all.
    let has_external_agent_loading =
        harness_config.supports_agent_file || harness_config.agent_file_env.is_some();
    let effective_prompt = if !has_external_agent_loading && agent_file_path.is_some() {
        format!("{agent_content}\n\n---\n\n{prompt}")
    } else {
        prompt.to_string()
    };

    // Legacy fallback for user configs that predate plan_args.
    if harness_config.plan_args.is_empty() {
        let mut args = Vec::new();
        if harness_config.supports_agent_file
            && let Some(path) = agent_file_path
        {
            args.push("--system-prompt-file".to_string());
            args.push(path.to_string_lossy().to_string());
        }
        args.push(effective_prompt.clone());
        return protect_plan_harness_argv(harness_name, harness_config, args, &effective_prompt);
    }

    // Template path: substitute {prompt} and {agent_file} in place using
    // substring replacement so tokens like "--prompt={prompt}" and
    // "--agent-file={agent_file}" work, matching build_harness_args semantics.
    //
    // Resolve `{agent_file}` BEFORE `{prompt}` so a prompt that happens
    // to contain the literal string `{agent_file}` (e.g. a plan
    // description discussing the placeholder system) cannot collide
    // with the no-agent removal pass — see harness::build_harness_args
    // for the matching fix.
    let mut args: Vec<String> = harness_config.plan_args.clone();

    if let Some(path) = agent_file_path {
        let agent_file_str = path.to_string_lossy();
        for arg in args.iter_mut() {
            *arg = arg.replace("{agent_file}", &agent_file_str);
        }
    } else {
        // Mirror build_harness_args's no-agent-file behavior: strip any
        // `{agent_file}` placeholder tokens and the preceding flag they go with.
        harness::remove_agent_file_args(&mut args);
    }

    // Now that `{agent_file}` is resolved, substitute `{prompt}` into
    // every arg position.
    for arg in args.iter_mut() {
        *arg = arg.replace("{prompt}", &effective_prompt);
    }

    protect_plan_harness_argv(harness_name, harness_config, args, &effective_prompt)
}

fn protect_plan_harness_argv(
    harness_name: &str,
    harness_config: &crate::config::HarnessConfig,
    mut args: Vec<String>,
    prompt: &str,
) -> Result<PlanHarnessInvocation> {
    let largest_arg = args.iter().map(|a| a.len()).max().unwrap_or(0);
    if largest_arg <= harness::ARGV_SPILL_THRESHOLD_BYTES {
        return Ok(PlanHarnessInvocation {
            args,
            _prompt_file: None,
        });
    }

    let prompt_arg_indexes: Vec<usize> = args
        .iter()
        .enumerate()
        .filter_map(|(idx, arg)| (arg == prompt).then_some(idx))
        .collect();
    if prompt_arg_indexes.is_empty() {
        bail!(
            "plan-harness '{harness_name}' produced an argv element of {largest_arg} bytes, \
             past the {} KB safety threshold for the 128 KB Linux argv ceiling; \
             refusing to spawn to avoid E2BIG",
            harness::ARGV_SPILL_THRESHOLD_BYTES / 1024,
        );
    }

    let can_spill_to_file = harness_config.prompt_input == PromptInputMode::TempFile
        || (harness_config.prompt_input == PromptInputMode::Argv
            && harness_config.argv_overflow == ArgvOverflowBehavior::SpillToTempFile);
    if !can_spill_to_file {
        bail!(
            "plan-harness '{harness_name}' prompt is {} bytes, past the {} KB argv safety \
             threshold; this interactive planner cannot spill the seed prompt to stdin, \
             and this harness is not configured for tempfile prompt delivery",
            prompt.len(),
            harness::ARGV_SPILL_THRESHOLD_BYTES / 1024,
        );
    }

    eprintln!(
        "ralph: warning: plan-harness prompt is {} bytes (>{} KB threshold); \
         spilling to temp file to avoid E2BIG on argv-mode harness '{}'.",
        prompt.len(),
        harness::ARGV_SPILL_THRESHOLD_BYTES / 1024,
        harness_name,
    );
    let mut tmp = NamedTempFile::new().context("failed to create plan-harness prompt temp file")?;
    tmp.write_all(prompt.as_bytes())
        .context("failed to write plan-harness prompt to temp file")?;
    tmp.flush()
        .context("failed to flush plan-harness prompt temp file")?;
    let path = tmp.path().to_string_lossy().to_string();
    for idx in prompt_arg_indexes {
        args[idx] = path.clone();
    }
    Ok(PlanHarnessInvocation {
        args,
        _prompt_file: Some(tmp),
    })
}

/// Build environment variables for the plan-harness session.
fn build_plan_harness_env(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
) -> Result<Vec<(String, String)>> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;
    let mut env_vars = Vec::new();

    // Goose uses an env var for the system prompt file
    if let Some(ref env_name) = harness_config.agent_file_env
        && !harness_config.supports_agent_file
        && let Some(path) = agent_file_path
    {
        env_vars.push((env_name.clone(), path.to_string_lossy().to_string()));
    }

    Ok(env_vars)
}

/// Refuse to start the planner when a `ralph run` is in progress on this
/// project.
///
/// The planner mutates plan/step rows the executor is about to read, so
/// running them concurrently can corrupt plan state (steps reordered or
/// deleted out from under the executor). The check is strict by default:
/// if a `run_locks` row exists for `project`, we bail — even if the
/// recorded pid is no longer alive. Liveness probing lives in
/// `ralph run --force` (reclaim path) and `ralph cancel` (clears dead
/// locks); duplicating that heuristic here would invite split-brain
/// behavior where the planner and executor disagree about whether a run
/// is live.
///
/// The bail message names the escape hatches so a user who hits a stale
/// row knows exactly how to recover.
pub fn preflight_no_live_run(conn: &Connection, project: &str) -> Result<()> {
    let live = storage::get_live_run(conn, project)?;
    if let Some(lr) = live {
        let plan_label = lr
            .plan_slug
            .as_deref()
            .map(|s| format!("plan {s}"))
            .unwrap_or_else(|| "<all plans>".to_string());
        bail!(
            "`ralph run` is active in this project (pid {pid}, {plan_label}, started {started_at}).\n\
             Refusing to start the planner while a run is in progress.\n\n\
             Cancel the run with `ralph cancel` first, or wait for it to finish.",
            pid = lr.pid,
            plan_label = plan_label,
            started_at = lr.started_at,
        );
    }
    Ok(())
}

/// Non-fatal post-generation DAG sanity check.
///
/// An authoring harness that expressed ordering by array/positional order
/// (the old `--after <N>` trap) instead of real `--depends-on`/`--after`
/// edges produces an **all-roots, edge-less** plan: it runs in sort order
/// so tests pass and it "looks linear", but it has none of the gating /
/// branch-isolation / review-reparenting the author intended. `ralph
/// import` validates its bundle; `plan harness generate` had no such guard.
/// This warns (never fails — generation already succeeded) and points at
/// how to inspect and fix it. Best-effort: any DB hiccup is swallowed.
pub fn warn_if_edgeless_dag(conn: &Connection, project: &str, plan_slug: Option<&str>) {
    // The just-generated plan: the named one, else the most recent.
    let plan = match plan_slug {
        Some(s) => storage::get_plan_by_slug(conn, s, project).ok().flatten(),
        None => storage::list_plans_sorted_by_recency(conn, project)
            .ok()
            .and_then(|v| v.into_iter().next()),
    };
    let Some(plan) = plan else { return };
    let Ok(steps) = storage::list_steps(conn, &plan.id) else {
        return;
    };
    if steps.len() < 2 {
        return; // a 0/1-step plan can't have a meaningful edge
    }
    let Ok(edges) = storage::list_step_dependency_edges(conn, &plan.id) else {
        return;
    };
    let total_edges: usize = edges.values().map(|v| v.len()).sum();
    if total_edges == 0 {
        eprintln!(
            "\nwarning: plan '{slug}' has {n} steps but ZERO dependency edges — \
             every step is an independent root.\n         \
             If you intended an ordering/DAG, this is an authoring mistake \
             (array/list order is NOT a dependency).\n         \
             Inspect:  ralph step list {slug}\n         \
             Fix:      ralph step dependency add <step> --depends-on <step>… \
             (or re-author with --after/--depends-on / `depends_on` in --import-json).",
            slug = plan.slug,
            n = steps.len(),
        );
    }
}

/// Run the interactive plan-harness: spawn a harness with the plan agent definition
/// and wait for it to exit.
///
/// Returns the harness exit code.
pub async fn run_plan_harness(
    config: &Config,
    harness_name: &str,
    project: &str,
    description: Option<&str>,
    plan_slug: Option<&str>,
) -> Result<i32> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;

    // Build the plan agent content, injecting the list of hooks applicable
    // to the current project so the harness can reference them by name.
    let project_path = std::path::Path::new(project);
    let hooks = hook_library::load_all()?;
    let applicable = hook_library::filter_by_project(hooks, project_path);
    let base_agent_content = render_plan_agent(&applicable);

    // Build the initial (task) prompt.
    let prompt = build_initial_prompt(project, description, plan_slug);

    // Plan generation must stay interactive. If this harness's `plan_args`
    // has no `{prompt}` slot, its TUI cannot be seeded with the task
    // (e.g. grok — only its headless `-p`/`--prompt-file` take a prompt,
    // and those exit). Fold the task into the `--agent` definition file so
    // just opening the harness still conveys WHAT to plan. This is the
    // mirror of `build_plan_harness_args`'s agent-into-prompt fold for the
    // no-agent-file case. (Empty `plan_args` is the legacy fallback handled
    // in `build_plan_harness_args`; don't fold there.)
    let plan_args_has_prompt = harness_config
        .plan_args
        .iter()
        .any(|a| a.contains("{prompt}"));
    let agent_content = if !harness_config.plan_args.is_empty() && !plan_args_has_prompt {
        format!("{base_agent_content}\n\n---\n\n# Your task\n\n{prompt}")
    } else {
        base_agent_content
    };

    // Write the agent definition to a temporary file.
    // This file lives for the duration of the harness process.
    let agent_temp_file = write_agent_temp_file(&agent_content)?;
    let agent_file_path = agent_temp_file.path();

    // Build per-harness args and env
    let invocation = build_plan_harness_args(
        harness_name,
        config,
        Some(agent_file_path),
        &agent_content,
        &prompt,
    )?;
    let env_vars = build_plan_harness_env(harness_name, config, Some(agent_file_path))?;

    // Spawn the harness interactively
    let cwd = std::path::Path::new(project);
    let mut child =
        harness::spawn_harness_interactive(harness_config, &invocation.args, &env_vars, cwd)
            .await
            .with_context(|| format!("Failed to spawn plan-harness '{harness_name}'"))?;

    // Wait for the harness to exit
    let status = child
        .wait()
        .await
        .context("Failed to wait for plan-harness process")?;

    // Temp files are cleaned up when agent_temp_file / invocation drop.
    Ok(plan_harness_exit_code(status))
}

fn plan_harness_exit_code(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }

    1
}

/// A temporary file that is cleaned up on drop.
pub struct TempAgentFile {
    path: PathBuf,
}

impl TempAgentFile {
    /// Returns the path to the temporary agent file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempAgentFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Write the given agent definition content to a temporary file. The filename
/// combines the process id with a uuid to prevent collisions between
/// concurrent callers in the same process (notably parallel tests).
fn write_agent_temp_file(content: &str) -> Result<TempAgentFile> {
    let file_name = format!(
        "ralph-rs-plan-agent-{}-{}.md",
        std::process::id(),
        uuid::Uuid::new_v4(),
    );
    let path = std::env::temp_dir().join(file_name);

    let mut file = std::fs::File::create(&path).with_context(|| {
        format!(
            "Failed to create temporary agent file at {}",
            path.display()
        )
    })?;
    file.write_all(content.as_bytes())
        .context("Failed to write agent definition to temp file")?;
    file.flush().context("Failed to flush agent temp file")?;

    Ok(TempAgentFile { path })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_build_initial_prompt_with_description() {
        let prompt = build_initial_prompt("/tmp/project", Some("Add authentication"), None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("Add authentication"));
        assert!(prompt.starts_with("Create a ralph plan"));
    }

    #[test]
    fn test_build_initial_prompt_without_description() {
        let prompt = build_initial_prompt("/tmp/project", None, None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.starts_with("Help me create or update"));
    }

    #[test]
    fn test_build_initial_prompt_with_plan_slug_and_description() {
        // Regression: `ralph plan harness generate <desc> my-plan` used to
        // discard the slug. The prompt must now name the target plan so the
        // harness updates it instead of touching the active plan.
        let prompt = build_initial_prompt("/tmp/project", Some("Wire up the API"), Some("my-plan"));
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("Wire up the API"));
        assert!(prompt.contains("'my-plan'"));
        assert!(prompt.starts_with("Update the ralph plan"));
    }

    #[test]
    fn test_build_initial_prompt_with_plan_slug_only() {
        let prompt = build_initial_prompt("/tmp/project", None, Some("my-plan"));
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("'my-plan'"));
        assert!(prompt.starts_with("Help me update the ralph plan"));
    }

    fn test_agent_content() -> String {
        render_plan_agent(&[])
    }

    #[test]
    fn test_build_plan_harness_args_claude() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        // Claude should get --system-prompt-file and the prompt as separate args
        assert!(args.contains(&"--system-prompt-file".to_string()));
        assert!(args.contains(&"Create a plan".to_string()));
        // Agent content should NOT be in the prompt
        assert!(!args.iter().any(|a| a.contains("ralph Plan Agent")));
        // {agent_file} placeholder should have been substituted with the real path
        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert!(args.contains(&path_str));
        assert!(!args.iter().any(|a| a.contains("{agent_file}")));
    }

    #[test]
    fn test_build_plan_harness_args_prompt_containing_agent_file_token() {
        // Regression test mirroring harness::build_harness_args: if the
        // prompt text contains the literal string `{agent_file}` (e.g. a
        // plan description discussing the placeholder system itself), the
        // no-agent removal pass must run BEFORE prompt substitution so
        // the preceding flag (here, claude's `--system-prompt-file` or a
        // harness's `-p` / `--prompt`) is not stripped along with the
        // prompt arg.
        let config = Config::default();
        let agent_content = test_agent_content();
        let prompt_with_placeholder =
            "Plan that discusses the {agent_file} placeholder collision bug.";

        // No agent file: hits the remove_agent_file_args branch for
        // any harness whose plan_args contain a `{agent_file}` token.
        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            None,
            &agent_content,
            prompt_with_placeholder,
        )
        .unwrap();
        // Claude's plan_args include `--permission-mode bypassPermissions`
        // which must survive unconditionally — and the prompt itself must
        // appear verbatim with its literal `{agent_file}` intact.
        assert!(
            claude_args.iter().any(|a| a == "--permission-mode"),
            "--permission-mode was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == "bypassPermissions"),
            "bypassPermissions was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == prompt_with_placeholder),
            "prompt was stripped or mangled; got args: {claude_args:?}"
        );
        // No residual `{agent_file}` token in args (outside the prompt).
        // Every arg equal to just "{agent_file}" on its own would indicate
        // a failed removal; the prompt-as-a-whole containing the substring
        // is fine and expected.
        assert!(
            !claude_args.iter().any(|a| a == "{agent_file}"),
            "raw {{agent_file}} token leaked through; got args: {claude_args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_args_copilot_uses_interactive_flag() {
        // The whole point of the plan_args template: copilot's run-mode -p
        // is one-shot and rejects a seeded positional prompt, so plan-harness
        // mode must invoke it with -i instead.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "copilot",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan this",
        )
        .unwrap();

        assert!(
            args.contains(&"-i".to_string()),
            "copilot plan_args must use -i, got: {args:?}"
        );
        assert!(
            !args.contains(&"-p".to_string()),
            "copilot plan_args must NOT use -p (one-shot, non-interactive): {args:?}"
        );
        assert!(args.contains(&"--allow-all".to_string()));
        assert!(args.contains(&"--allow-all-paths".to_string()));
        // The prompt (with prepended agent content) should still be present.
        assert!(args.iter().any(|a| a.contains("Plan this")));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
    }

    #[test]
    fn test_build_plan_harness_args_legacy_empty_template() {
        // A user config that predates plan_args ships an empty Vec. The
        // builder must fall back to the pre-template behavior (claude gets
        // --system-prompt-file + prompt; everything else gets the prepended
        // prompt as a bare positional).
        let mut config = Config::default();
        for harness in config.harnesses.values_mut() {
            harness.plan_args.clear();
        }
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();

        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        )
        .unwrap();
        assert_eq!(claude_args[0], "--system-prompt-file");
        assert_eq!(claude_args[1], agent_file.path().to_string_lossy());
        assert_eq!(claude_args[2], "Plan");
        assert_eq!(claude_args.len(), 3);

        let codex_args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        )
        .unwrap();
        assert_eq!(codex_args.len(), 1);
        assert!(codex_args[0].contains("ralph Plan Agent"));
        assert!(codex_args[0].contains("Plan"));
    }

    #[test]
    fn test_build_plan_harness_args_codex_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        // Codex doesn't support agent files, so agent content should be prepended
        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Create a plan")));
    }

    #[test]
    fn test_build_plan_harness_args_pi_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "pi",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        )
        .unwrap();

        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Help me plan")));
    }

    #[test]
    fn test_build_plan_harness_args_goose_does_not_inline_agent() {
        // Goose loads the agent definition via GOOSE_SYSTEM_PROMPT_FILE_PATH,
        // which fully replaces the default system prompt. If we ALSO inline
        // the definition into the -t prompt (as the old behavior did), goose
        // sees it twice: once as system, once as user. Pin the fix so any
        // future change to the prepend logic has to re-justify itself.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "goose",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        )
        .unwrap();

        assert!(
            args.iter().any(|a| a.contains("Help me plan")),
            "user prompt missing from argv: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.contains("ralph Plan Agent")),
            "agent definition was inlined into goose argv (double-load bug): {args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_env_goose() {
        // Goose's default config sets `agent_file_env` to
        // GOOSE_SYSTEM_PROMPT_FILE_PATH, so an agent file should be exported
        // as that env var to the subprocess.
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("goose", &config, Some(agent_file.path())).unwrap();

        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_SYSTEM_PROMPT_FILE_PATH");
        assert_eq!(env[0].1, agent_file.path().to_string_lossy());
    }

    #[test]
    fn test_build_plan_harness_env_goose_no_agent_file() {
        // With no agent file, nothing should be exported even if the env
        // var is configured.
        let config = Config::default();
        let env = build_plan_harness_env("goose", &config, None).unwrap();
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_env_claude_no_env() {
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("claude", &config, Some(agent_file.path())).unwrap();

        // Claude supports agent file natively, so env var should NOT be set
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_args_unknown_harness_returns_err() {
        // Regression: indexing into config.harnesses with an unknown key
        // used to panic. An unknown harness name must surface as a
        // descriptive Err that names the missing harness and lists the
        // available ones, not a process abort.
        let config = Config::default();
        let agent_content = test_agent_content();
        let result =
            build_plan_harness_args("does-not-exist", &config, None, &agent_content, "Plan");
        let err = result.expect_err("expected Err for unknown harness");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does-not-exist"),
            "error should name the missing harness: {msg}"
        );
    }

    #[test]
    fn test_build_plan_harness_env_unknown_harness_returns_err() {
        let config = Config::default();
        let result = build_plan_harness_env("does-not-exist", &config, None);
        let err = result.expect_err("expected Err for unknown harness");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does-not-exist"),
            "error should name the missing harness: {msg}"
        );
    }

    #[test]
    fn test_write_agent_temp_file() {
        let content = test_agent_content();
        let agent_file = write_agent_temp_file(&content).unwrap();
        let read = std::fs::read_to_string(agent_file.path()).unwrap();
        assert!(read.contains("ralph Plan Agent"));
        assert!(read.contains("ralph plan create"));
        assert!(read.contains("ralph step add"));
    }

    #[test]
    fn test_harness_plan_agent_content() {
        // Verify the rendered agent definition has key sections.
        let content = render_plan_agent(&[]);
        assert!(content.contains("Plan Management"));
        assert!(content.contains("Step Management"));
        assert!(content.contains("Hook Attachment"));
        assert!(content.contains("Workflow"));
        assert!(content.contains("Guidelines"));
        assert!(content.contains("ralph plan create"));
        assert!(content.contains("ralph step add"));
        assert!(content.contains("ralph plan approve"));
        assert!(content.contains("ralph step set-hook"));
        assert!(content.contains("Available Hooks"));
    }

    #[test]
    fn test_render_plan_agent_lists_hooks() {
        use crate::hook_library::{Hook, Lifecycle, Scope};
        let hooks = vec![
            Hook {
                name: "claude-review".to_string(),
                description: "Review with Claude".to_string(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Global,
                command: "claude -p 'review'".to_string(),
            },
            Hook {
                name: "rust-clippy".to_string(),
                description: String::new(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Paths {
                    paths: vec![std::path::PathBuf::from("/home/me/rust")],
                },
                command: "cargo clippy".to_string(),
            },
        ];
        let content = render_plan_agent(&hooks);
        assert!(content.contains("**claude-review**"));
        assert!(content.contains("Review with Claude"));
        assert!(content.contains("**rust-clippy**"));
        assert!(content.contains("/home/me/rust"));
    }

    #[test]
    fn test_render_plan_agent_no_hooks_message() {
        let content = render_plan_agent(&[]);
        assert!(content.contains("No hooks are currently available"));
        assert!(content.contains("ralph hooks add"));
    }

    /// Synthetic harness config builder used by the substring-replacement
    /// tests below. `HarnessConfig` doesn't derive `Default`, so we spell out
    /// all fields here to keep the tests independent of `Config::default()`.
    fn synthetic_harness(plan_args: Vec<String>, supports_agent_file: bool) -> Config {
        use crate::config::HarnessConfig;
        use std::collections::HashMap;
        let mut harnesses = HashMap::new();
        harnesses.insert(
            "synth".to_string(),
            HarnessConfig {
                command: "synth".to_string(),
                args: vec![],
                plan_args,
                supports_agent_file,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
                agent_file_args: vec![],
                model_args: vec![],
                default_model: None,
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );
        Config {
            default_harness: "synth".to_string(),
            max_retries_per_step: 0,
            timeout_secs: None,
            hook_timeout_secs: 120,
            auto_stash: true,
            prompt: None,
            min_free_disk_mb: 1024,
            display_timezone: "UTC".to_string(),
            harness_chunk_max_bytes: 4096,
            review: crate::config::ReviewConfig::default(),
            harnesses,
        }
    }

    #[test]
    fn test_plan_harness_args_substring_replacement() {
        // A plan_args template that embeds {prompt} inside a larger token
        // (e.g. `--prompt={prompt}`) must be substring-substituted, not
        // matched as a whole token — otherwise users copying run-mode
        // patterns into plan_args get the literal placeholder in argv.
        let config = synthetic_harness(vec!["--prompt={prompt}".to_string()], false);
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        assert_eq!(args.len(), 1, "expected a single arg, got {args:?}");
        // Because supports_agent_file=false, the agent content is prepended
        // to the effective prompt — so the arg begins with "--prompt=" and
        // contains both the agent content and the original prompt.
        assert!(args[0].starts_with("--prompt="));
        assert!(args[0].contains("Create a plan"));
        assert!(
            !args[0].contains("{prompt}"),
            "literal placeholder leaked into argv: {args:?}"
        );
    }

    #[test]
    fn test_plan_harness_args_agent_file_substring() {
        // Same story for {agent_file}: a token like `--agent-file={agent_file}`
        // must substring-substitute to the real path, not leak the literal.
        let config = synthetic_harness(
            vec![
                "--agent-file={agent_file}".to_string(),
                "{prompt}".to_string(),
            ],
            true,
        );
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert_eq!(args.len(), 2, "expected two args, got {args:?}");
        assert!(args[0].starts_with("--agent-file="));
        assert!(
            args[0].contains(&path_str),
            "expected path substring in first arg: {args:?}"
        );
        assert!(
            !args[0].contains("{agent_file}"),
            "literal {{agent_file}} leaked into argv: {args:?}"
        );
        assert_eq!(args[1], "Create a plan");
    }

    #[test]
    fn test_plan_harness_large_prompt_spills_to_tempfile_when_configured() {
        let mut config =
            synthetic_harness(vec!["--prompt".to_string(), "{prompt}".to_string()], false);
        let hc = config.harnesses.get_mut("synth").unwrap();
        hc.prompt_input = crate::config::PromptInputMode::Argv;
        hc.argv_overflow = crate::config::ArgvOverflowBehavior::SpillToTempFile;
        let prompt = "x".repeat(crate::harness::ARGV_SPILL_THRESHOLD_BYTES + 1024);
        let args =
            build_plan_harness_args("synth", &config, None, "", &prompt).expect("spill succeeds");

        assert_eq!(args[0], "--prompt");
        assert_ne!(args[1], prompt);
        assert!(
            args[1].len() < crate::harness::ARGV_SPILL_THRESHOLD_BYTES,
            "tempfile path should replace oversized prompt arg"
        );
        let contents = std::fs::read_to_string(&args[1]).expect("prompt tempfile readable");
        assert_eq!(contents, prompt);
    }

    #[test]
    fn test_plan_harness_large_prompt_errors_without_tempfile_delivery() {
        let mut config = synthetic_harness(vec!["{prompt}".to_string()], false);
        let hc = config.harnesses.get_mut("synth").unwrap();
        hc.prompt_input = crate::config::PromptInputMode::Argv;
        hc.argv_overflow = crate::config::ArgvOverflowBehavior::Error;
        let prompt = "x".repeat(crate::harness::ARGV_SPILL_THRESHOLD_BYTES + 1024);
        let err = build_plan_harness_args("synth", &config, None, "", &prompt).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("argv safety threshold"), "got: {msg}");
        assert!(msg.contains("tempfile"), "got: {msg}");
    }

    #[cfg(unix)]
    #[test]
    fn test_plan_harness_exit_code_preserves_signal_death() {
        use std::os::unix::process::ExitStatusExt;

        let status = std::process::ExitStatus::from_raw(9);
        assert_eq!(plan_harness_exit_code(status), 137);
    }

    // -- preflight_no_live_run tests --

    fn setup_conn() -> Connection {
        crate::db::open_memory().expect("open_memory")
    }

    #[test]
    fn test_planner_refuses_when_run_lock_present() {
        // A live `ralph run` row for this project must block the planner.
        // The bail message should explain the situation in terms the user
        // can act on.
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["/proj-live", 4242i64, "plan-id-x", "my-plan"],
        )
        .unwrap();

        let err = preflight_no_live_run(&conn, "/proj-live")
            .expect_err("expected Err when a run lock row exists");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("active") || msg.contains("Refusing"),
            "bail message should signal the run is active; got: {msg}"
        );
        // Observability: pid and plan slug should surface so the user can
        // identify the blocking run.
        assert!(msg.contains("4242"), "pid missing from message: {msg}");
        assert!(
            msg.contains("my-plan"),
            "plan slug missing from message: {msg}"
        );
        // The escape hatches must be named.
        assert!(
            msg.contains("ralph cancel"),
            "escape-hatch hint missing: {msg}"
        );
    }

    #[test]
    fn test_planner_proceeds_when_no_run_lock() {
        // No row → preflight returns Ok, so the caller proceeds to spawn
        // the planner harness. We don't drive the full planner path here;
        // the preflight function is the unit under test.
        let conn = setup_conn();
        let res = preflight_no_live_run(&conn, "/proj-empty");
        assert!(
            res.is_ok(),
            "expected Ok when no run_locks row present, got: {res:?}"
        );
    }

    #[test]
    fn test_planner_refuses_even_when_plan_slug_is_null() {
        // A run row without a plan_slug (e.g. `ralph run --all`) still
        // blocks the planner. The bail message should fall back to the
        // "<all plans>" label instead of omitting the plan field entirely.
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO run_locks (project, pid) VALUES (?1, ?2)",
            rusqlite::params!["/proj-all", 7777i64],
        )
        .unwrap();

        let err = preflight_no_live_run(&conn, "/proj-all")
            .expect_err("expected Err when a run lock row exists");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("<all plans>"),
            "missing all-plans label: {msg}"
        );
        assert!(msg.contains("7777"), "missing pid: {msg}");
    }
}
