# ralph-rs DAG Redesign — Design Document

**Status:** Draft for review
**Supersedes/extends:** linear-plan model, per-plan question system, manual
review-via-hooks pattern (`docs/review-hooks.md`)
**Companion:** `docs/dag-tui-mockups.html` (open in a browser) for the TUI
visualization options.

> **Shipped deviations:** this remains the authoritative *design draft*.
> Where the shipped code intentionally differs, `CLAUDE.md` ("DAG redesign
> — shipped shape") is authoritative. In particular the **step-add
> placement model** was tightened post-redesign: `ralph step add` no longer
> has a positional `--after <N>`; on a non-empty plan an explicit placement
> (`--after`/`--before`/`--depends-on`/`--root`) is **required**, and
> `--import-json` carries the DAG (`short_id` + `depends_on`). Treat any
> `ralph step add … --after <num>`-as-position usage in the examples below
> as superseded by that model.

---

## 1. Motivation

Today a plan is a **linear list of steps**. The human is the serialized
bottleneck: the moment the run needs a human (a clarifying question, a
blocker the agent can't clear), the *entire* plan stalls until the human
shows up, even though unrelated work could have proceeded.

This redesign makes a plan a **dependency DAG**. When one branch of the DAG
blocks on a human, other independent branches keep running. The human can
batch-answer interrupts on their own schedule while the machine stays busy.
We also make **nondeterministic review** a first-class, per-step pipeline
stage so plans can be implemented by one harness and reviewed step-by-step
by another.

**The payoff scales with DAG width.** A purely linear plan gets *zero*
benefit (its one branch blocking = whole plan blocked, same as today). This
implies soft pressure on plan authors and the plan-generation prompts
(`create-ralph` skill + `HARNESS_PLAN_AGENT_BASE`) to express independence
so the scheduler has slack to exploit. Behavior for a linear plan must
remain identical to today (no script regressions, deterministic order).

---

## 2. Confirmed design decisions

| # | Decision |
|---|----------|
| 1 | **One git branch per plan.** "Branches" are branches of the *dependency tree*, not git branches. Git history stays linear. |
| 2 | **DAG, not tree.** A step may depend on multiple steps; multiple roots allowed. Acyclicity is validated on every edge mutation. |
| 3 | **Direct dependents wait for the review verdict; unrelated branches run concurrently with a review.** |
| 4 | **Review off-switch at three scopes:** global config, per-plan, per-step (precedence step > plan > global, mirroring `RetryStrategy`). |
| 5 | **No prior-step diffs in prompts.** The DAG is a *scheduling + run-eligibility* construct, **not** a prompt-context-expansion construct. A step's prompt does not grow with the size or depth of the branch above it. |

Decision 5 is reinforced by the context-growth audit (§4): the current
implementation already does **not** inject prior-step diffs, and this
redesign must preserve that.

---

## 3. Conceptual model

### 3.1 Plan = DAG of steps

- Every step gains a **short 8-char id** (`short_id`), unique within the
  plan, stable for its lifetime. This replaces the positional step number
  as the user-facing handle, because a DAG has no stable linear ordinal.
  The internal UUID (`steps.id`) is unchanged.
- Steps gain **dependency edges** (`step_dependencies` table, §6).
- **Roots** = steps with no dependencies (≥1 required; a plan with no root
  is invalid).
- A step is **runnable** when *every* dependency step is `Complete` **and**
  each dependency's review has returned (passed, or disabled/skipped, or —
  if it failed — the corrective step it spawned is itself `Complete`; see
  §3.4 and §10).

### 3.2 The per-step pipeline

> **Update (post-redesign).** The "commit per iteration, *before* the
> deterministic test" rule in this section was **reversed** in the
> shipped code: a commit now happens **only on the first attempt whose
> tests pass**, and failed attempts preserve the dirty tree
> (`previous_test_output` — including pre-commit hook stderr, treated as
> a test failure — feeds the next prompt). Net effect: **at most one
> commit per step.** The `Ralph-Iteration: <n>` trailer is preserved as
> the attempt-number identifier. The §5 `RetryStrategy` reinterpretation
> ("`Keep` accumulates iteration commits; `Rollback` git-reverts the
> prior iteration") is therefore moot at runtime — both arms preserve
> the dirty tree, and `RetryStrategy` is vestigial pending removal. The
> `RUNNABLE → FAILED` transitions on `TestFailed` / `CommitFailed` shown
> below were also softened — see §3.4-bis ("Retry-exhaustion
> auto-blocker"). `CLAUDE.md` ("DAG redesign — shipped shape") is
> authoritative for the shipped flow.

Each step runs this pipeline. The iteration number `n` starts at 1 and
increments on every implementation re-attempt.

```
   ┌─ implement (harness) ──────────────────────────────┐
   │                                                     │
   │   raises interruption? ──► BLOCKED (no budget) ─────┼─► (resolved) ─┐
   │                                                     │               │
   ▼                                                     │               │
 commit  "ralph <short_id>.<n> - <title>"  + trailers    │               │
   │                                                     │               │
   ▼                                                     │               │
 deterministic tests ── fail & budget left ──────────────┘ (n := n+1) ◄──┘
   │            └─ fail & no budget ──► FAILED
   ▼ pass
 AwaitingReview ──► (review returns)
   │                  ├─ pass / disabled / skipped ──► COMPLETE
   │                  └─ fail ──► insert corrective step, re-parent
   │                              dependents, this step ──► COMPLETE
   ▼
 (dependents become runnable)
```

Key points:

- **Commit happens per iteration**, *before* the deterministic test, so
  that (a) the commit message can carry `<short_id>.<n>` and (b) the
  reviewer has a stable SHA to review. This is a change from today's
  "commit only on full success." See §5 for the git/history implications
  and the `RetryStrategy` reinterpretation.
- **Deterministic test failure** feeds the (already line-truncated) test
  output into the next iteration's retry context — exactly today's
  mechanism, just now after a per-iteration commit.
- **Review is read-only** with respect to the working tree (hard
  invariant — §9). Its only side effect is *requesting a corrective
  step* via the orchestrator.

### 3.3 Step state machine

`StepStatus` expands; we also add a separate `review_status` (analogous to
how `TestStatus` is separate from `TerminationReason` today).

`StepStatus`: `Pending` → `Runnable` → `InProgress` → `AwaitingReview` →
`Complete` | `Failed` | `Skipped` | `Aborted`, plus the orthogonal
`Blocked` overlay (an open interruption exists; the underlying status is
preserved and un-shadows on resolution — exactly how `PlanStatus::Question`
is *derived* today rather than stored).

`review_status` (per step): `Pending` | `InFlight` | `Passed` | `Failed` |
`Skipped` | `Disabled`.

A step reaches `Complete` only after: harness produced changes → committed
→ deterministic tests passed → review **returned** (any verdict). The
*protection* of dependents on a failed review is **structural** (the
re-parented edge to the corrective step), not status-based — see §10.

### 3.4 Interruptions: questions + blockers (unified)

> **Update (post-redesign).** The unified interruption model below still
> describes the shape of an interrupt. The shipped code adds a *second*
> way an interruption arises: the executor itself raises a
> `kind=Blocker` interruption on retry-budget exhaustion (test-fail or
> commit-hook-fail) instead of going terminal-`Failed` — see §3.4-bis.

Questions and blockers are the same entity — *a branch-pausing interrupt
that needs a human and may carry text forward into the next prompt*. One
table, one state machine, one TUI inbox, two CLI verbs.

```
Interruption {
  id, step_id, attempt,
  kind:        Question | Blocker,
  body:        text (the question, or the blocker explanation),
  options:     [ { text, priority:int } ]   // questions only; priority 1 = best
  resolution:  text | None,                  // chosen option text or freeform
  comment:     text | None,                  // extra human note, always injectable
  state:       Open | Resolved,
  asked_at, resolved_at,
}
```

- **Question**: agent proposes 0..N answers each with a priority integer
  (1 = agent's best). Human picks one (optionally adds a comment) **or**
  types a freeform answer. Freeform-only questions have no options.
- **Blocker**: agent explains what it can't do (needs sudo, needs access,
  needs information). Human *resolves* or *resolves with comment*.
- **Effect**: the step's branch is `Blocked`. **No retry budget is
  consumed.** The scheduler moves to another runnable step. The step's
  dependents wait (the step is not `Complete`).
- **Resolution** injects a bounded "Resolved interruptions" section into
  the step's next prompt (§8) — the chosen answer/resolution **and** any
  comment.

### 3.4-bis Retry-exhaustion auto-blocker (post-redesign)

The draft pipeline (§3.2) shows `tests fail & no budget ──► FAILED`. The
shipped executor instead converts the terminal failure into a *Blocker
interruption* so a human can choose between retrying from scratch and
accepting the failure. This makes retry budgets recoverable without
losing the per-step audit trail.

- **Trigger.** A step exhausts its retry budget on either `TestFailed`
  (deterministic tests still red on the last attempt) or `CommitFailed`
  (pre-commit hook still rejecting on the last attempt; the hook stderr
  is treated as a test failure end-to-end, so it gets retried like a
  test failure and the same exhaustion path fires). Other failure modes
  — `HarnessFailed`, `Timeout`, `NoChanges` — remain terminal `Failed`.
- **Effect.** The executor inserts a `kind=Blocker` interruption with
  two ranked options (`text` is the literal recognition key the
  resolution handler matches against):
  - priority 1 — `"Retry step with parked changes"`
    (`executor::RETRY_EXHAUSTED_OPTION_RETRY`)
  - priority 2 — `"Mark step Failed"`
    (`executor::RETRY_EXHAUSTED_OPTION_FAIL`)
  Body = `"Step failed after N attempts."` plus the final attempt's test
  output (and hook stderr when applicable), tail-truncated to a fixed
  byte cap so a runaway dump can't break the inbox UI. The step is
  parked at `status=Pending` with `attempts == max_attempts`; the
  derived `Blocked` overlay shadows it (never persisted, clears on
  resolution — exactly like §3.3). The dirty tree is rolled back; the
  scheduler advances to another runnable branch (consumes no further
  retry budget).
- **Resolution.** The TUI inbox and `ralph interruption resolve` route
  through `commands::interruption::apply_retry_exhausted_resolution`,
  which recognises a blocker with two options whose texts equal the two
  `pub const` strings above:
  - `RETRY_EXHAUSTED_OPTION_RETRY` → reset `attempts = 0`, status
    `Pending`; scheduler re-picks on its next tick.
  - `RETRY_EXHAUSTED_OPTION_FAIL` → status `Failed` (terminal).
  - Freeform answers matching neither are treated as **retry-with-hint**
    — `attempts = 0`, status `Pending`, and the human comment flows into
    the next prompt via the bounded "Resolved interruptions" section
    (§8). The inbox UI renders option lists for any blocker with
    non-empty `options`, not just questions, so the ranked-answer modal
    works uniformly.
- **Variant reuse, no new variants.**
  `TerminationReason::PausedForQuestion` and
  `StepOutcome::PausedForQuestion` are reused — the executor treats a
  retry-exhausted step exactly like a harness-raised pause for purposes
  of "did this attempt commit?" and "did this consume budget?". A
  downstream NDJSON consumer therefore sees a `step_finished` with
  `termination_reason="paused_for_question"` in both cases and must poll
  the `interruptions` table to distinguish them (see
  `docs/ndjson-events.md`).
- **Concurrency invariant.** The interruption insert + the
  `update_step_status(Pending)` happen inside a single
  `unchecked_transaction` so the scheduler can never observe `Pending
  without open interruption` mid-write — the §9 "single DAG writer"
  invariant holds across the auto-blocker path the same way it does for
  corrective-step insertion.

### 3.5 Scheduler

A single dynamic scheduler replaces the linear iterator:

1. Compute the runnable set (deps satisfied per §3.1, not `Blocked`, not
   terminal).
2. **Implementation is mutually exclusive** — at most one step in its
   implement/test phase at a time (a semaphore of 1; this *is* the
   "implementation steps don't run in parallel" rule).
3. **Reviews run concurrently** with the next *unrelated* implementation
   (read-only, against a fixed commit SHA — §9). A step's *direct
   dependents* are not in the runnable set until that step is `Complete`,
   so they never start on un-reviewed work.
4. **Deterministic tie-break.** Among runnable steps, pick by
   `(topological depth, sort_key, short_id)`. With no interruptions this
   reproduces the authored order, so a linear plan executes exactly as
   today — and runs are reproducible given the same human inputs.

---

## 4. Context-growth audit (and what it means here)

A full audit of prompt assembly (`prompt.rs`, `executor.rs`, `runner.rs`,
`git.rs`, hooks) found the current implementation **largely well-bounded**:

- **No cross-step accumulation.** Each step's prompt is rebuilt from
  current DB state; nothing from a completed step's diff/output/attempts
  enters a later step's prompt. The old O(n²) "prior step descriptions"
  dump was already replaced by a linear titles-only "Plan step map".
- Retry context is **overwrite-not-append** and line-truncated (200 diff
  lines / 100 test-output lines); resets per step.
- Hook stdout is discarded; never enters a prompt.
- The single **unbounded** vector: the **"Previously answered questions"**
  section (`storage::list_answered_questions_for_step` → no `LIMIT`,
  `format_answered_questions` → no per-entry truncation, no per-attempt
  cap on `ralph question ask`). Grows O(total Q&A across all attempts of
  a step).

**Implications for this redesign:**

- The new interruption-resolution injection (§8) **must** be bounded
  where the old one was not: keep last *N* resolved interruptions for the
  step, `truncate_text` each body/resolution/comment. This fixes the one
  pre-existing leak as part of the cutover.
- Decision 5 is already the status quo for diffs — the DAG must not
  regress it. A step depending on a deep branch must not inherit that
  branch's diffs. (If a step genuinely needs prior context, the author
  states it in the step description; the agent can `git log`/`git show`
  on demand — pull, not push.)
- The reviewer prompt takes a **single commit diff** (`git show <sha>`),
  not a cumulative diff — O(1) in plan size.

---

## 5. Git model

> **Update (post-redesign).** This section's "commit per iteration"
> rule and its corresponding `RetryStrategy::Keep` / `Rollback`
> reinterpretation were superseded by the **test-then-commit** flow
> (see §3.2 update callout). With **at most one commit per step**:
>
> - The subject + trailers (`Ralph-Plan` / `Ralph-Step` /
>   `Ralph-Iteration: <n>` / `Ralph-Review: pending`) are unchanged;
>   `<n>` now identifies *which attempt finally passed* rather than
>   counting commits.
> - `RetryStrategy` is **vestigial** — both arms preserve the dirty
>   tree between failed attempts (there is no per-iteration commit to
>   keep or revert). The enum, the per-plan/per-step columns, and the
>   `--retry-strategy` CLI flag are kept for migration compatibility,
>   slated for removal in a follow-up PR.
> - The §14.1 "iteration-commit history noise" open question is moot:
>   nothing to squash. The **`execution_logs` rows** are the per-attempt
>   audit trail (prompt / harness stdout+stderr / test output / diff
>   per attempt, including failed ones); the single committed SHA
>   represents only the passing attempt. `--squash-on-complete` / the
>   per-plan `squash_on_complete` column are vestigial alongside
>   `RetryStrategy`.
> - "Blocked branch parks no WIP specially" still holds for
>   harness-raised interruptions (the prior commit, if any, sits on the
>   linear history). The §3.4-bis auto-blocker is the *new* parking
>   case: the step's dirty tree is rolled back before the blocker is
>   raised, so the linear history never grows from a retry-exhausted
>   step until a human chooses to retry it.

- **Linear history, one branch per plan** (unchanged branch-per-plan
  policy).
- **Commit per iteration.** Message:
  `ralph <short_id>.<n> - <sanitized one-line title>` with trailers so
  tooling never parses the subject line:
  ```
  Ralph-Plan: <slug>
  Ralph-Step: <short_id>
  Ralph-Iteration: <n>
  Ralph-Review: pending        # later amended/annotated: passed|failed|skipped|disabled
  ```
  (Mirrors the existing `Ralph-Skipped-Step` trailer + `[ralph wip]`
  precedent.) `ralph log` / `step reset` map commits ↔ steps via trailers.
- **`RetryStrategy` reinterpreted under per-iteration commits:**
  - `Keep` (default): iteration *n+1* commits on top of *n* (history keeps
    every iteration; the agent sees prior work on disk; retry context
    omits the diff — as today).
  - `Rollback`: iteration *n*'s commit is reverted before *n+1*; the
    rolled-back diff is fed into the retry context (line-truncated — as
    today).
  - *Open question (§14):* whether completed steps' iteration commits are
    squashed at plan completion (clean history) or kept (full audit
    trail). Recommend: keep by default, optional `--squash-on-complete`.
- **Blocked branch parks no WIP specially** — its last iteration is
  already a real commit on the linear history. When resolved, the step
  re-runs with the resolution injected; the agent continues from the
  committed state. This reuses existing infra and avoids stash/pop
  conflict classes.
- **Accepted tradeoff:** because history is linear, a step from branch C
  may commit on top of a blocked step from branch B. That is the explicit
  price of *not* doing per-branch git branches (which would manufacture
  AI-vs-AI merge-conflict blockers and defeat the entire purpose). It is
  the same entanglement `RetryStrategy::Keep` already tolerates.

---

## 6. Schema changes

Next migration is **V25** (`MIGRATIONS.len()` is currently 24;
`CURRENT_VERSION` is derived). All changes are additive `ALTER`/`CREATE`
so old DBs migrate forward; old export JSON keeps round-tripping via
`#[serde(default)]`.

### V25 — short ids + step dependencies

```sql
ALTER TABLE steps ADD COLUMN short_id TEXT;          -- backfilled, then unique-per-plan
CREATE UNIQUE INDEX idx_steps_short_id ON steps(plan_id, short_id);

CREATE TABLE step_dependencies (
    step_id            TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    depends_on_step_id TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (step_id, depends_on_step_id),
    CHECK (step_id != depends_on_step_id)
);
CREATE INDEX idx_step_deps_step ON step_dependencies(step_id);
CREATE INDEX idx_step_deps_dep  ON step_dependencies(depends_on_step_id);
```

- This is a direct structural clone of `plan_dependencies` (V2) — reuse
  its cycle-detection pattern (`would_create_cycle` in `storage.rs`) for a
  `would_create_step_cycle`.
- **Backfill:** for every existing plan, assign each step a random 8-char
  `short_id` (collision-checked within the plan) and synthesize a linear
  chain: step *k* `depends_on` step *k−1* (by `sort_key` order). A
  migrated linear plan is therefore a degenerate DAG that executes
  identically.

### V26 — interruptions (supersedes `step_questions`)

```sql
CREATE TABLE interruptions (
    id          TEXT PRIMARY KEY,
    step_id     TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    attempt     INTEGER NOT NULL,
    kind        TEXT NOT NULL,                 -- 'question' | 'blocker'
    body        TEXT NOT NULL,
    options     TEXT NOT NULL DEFAULT '[]',    -- JSON [{text,priority}]
    resolution  TEXT,
    comment     TEXT,
    state       TEXT NOT NULL DEFAULT 'open',  -- 'open' | 'resolved'
    asked_at    TEXT NOT NULL,
    resolved_at TEXT
);
CREATE INDEX idx_interruptions_step ON interruptions(step_id);
CREATE INDEX idx_interruptions_open ON interruptions(state) WHERE state = 'open';
```

- **Data cutover:** copy `step_questions` rows → `interruptions`
  (`kind='question'`, `body=question`, `options` from `suggestions` with
  synthesized ascending priorities, `resolution=answer`,
  `state` from `answer IS NULL`). Then `DROP TABLE step_questions`.
- `PlanStatus::Question` becomes the broader derived `Interrupted` (an
  open interruption exists for any step); same "derived, never stored"
  mechanism as today.

### V27 — review configuration

```sql
ALTER TABLE plans ADD COLUMN review_enabled INTEGER;   -- NULL = inherit global
ALTER TABLE steps ADD COLUMN review_enabled INTEGER;   -- NULL = inherit plan
ALTER TABLE steps ADD COLUMN review_status  TEXT;      -- NULL = pending
ALTER TABLE steps ADD COLUMN corrects_step_id TEXT;    -- set on reviewer-inserted steps
```

Global review config (harness + model + default on/off) lives in
`config.json` (`~/.config/ralph-rs/config.json`), e.g.:

```json
"review": { "enabled": true, "harness": "codex", "model": "gpt-5-codex" }
```

Effective review = step.review_enabled ?? plan.review_enabled ??
config.review.enabled ?? false. Disabled at any level ⇒ `review_status =
Disabled`, step is `Complete` straight from passing tests.

---

## 7. CLI surface changes

```
# Short ids: every <num> selector also accepts a short id.
ralph step add <title> [<slug>] ... [--depends-on <short_id|num>]...
ralph step dependency add    <short_id> --depends-on <short_id>...
ralph step dependency remove <short_id> --depends-on <short_id>...
ralph step dependency list   <short_id>

# Interruptions (run by the harness inside a step):
ralph question ask <text> [-s "answer" --priority N]...   # priority optional, default appends
ralph block <text>                                        # raise a blocker
# Resolution (human, CLI side; TUI is the primary path):
ralph interruption list [<slug>]
ralph interruption show <id>
ralph interruption resolve <id> [--option <k>] [--answer <text>] [--comment <text>]

# Review:
ralph plan review <on|off> [<slug>]
ralph step edit <sel> --review <on|off|inherit>
ralph config review set --harness <h> --model <m> --enabled <bool>
```

- Harness protocol for raising an interruption (mirrors the skip-bridge):
  ralph sets `RALPH_STEP_ID` when spawning; the agent runs `ralph question
  ask` / `ralph block`, which writes an `interruptions` row keyed to that
  step+attempt, then the agent **exits cleanly**. The orchestrator, after
  the harness returns, checks for an open interruption on the step → marks
  the branch `Blocked`, consumes **no** retry budget, scheduler advances.
- `ralph question ask` outside a run, or with the plan/step review/question
  feature off, is rejected exactly as today.
- Legacy `ralph question list/answer` retained as thin aliases over
  `interruption` for one release, then removed.

---

## 8. Prompt model changes

The four-layer model (Global → Project → Plan → Step) is unchanged. Two
deltas:

1. **"Previously answered questions" → "Resolved interruptions"**, and it
   is **bounded** (fixing the §4 leak): keep the last *N* (e.g. 5)
   resolved interruptions for the step, `truncate_text` each
   body/resolution/comment. Render kind, body, chosen
   resolution + comment.
2. **Reviewer prompt** is a distinct, separately-assembled prompt (not
   `build_step_prompt`): plan/step context (titles, acceptance criteria),
   the **single** commit diff (`git show <sha>` — O(1)), and the
   instruction:

   > Review commit `<short_id>.<n>` against this step's acceptance
   > criteria. You are read-only — do **not** modify files. If and only
   > if you find a real defect in *this step's* implementation, request a
   > corrective step (it will be inserted immediately after this step and
   > everything that depended on this step will be re-pointed at the
   > correction).

No dependency diffs are ever injected (Decision 5).

---

## 9. Concurrency model

Hard invariants (violating any of these reintroduces the merge/locking
problems the design avoids):

1. **One implementation slot.** A semaphore of 1 guards the
   implement+test+commit phase. This *is* "implementation steps don't run
   in parallel."
2. **Reviews are strictly read-only** w.r.t. the working tree. A review
   runs `git show <sha>` against a fixed commit, never checks out, never
   edits. This is the *entire reason* a review can run concurrently with
   the next unrelated implementation.
3. **Single DAG writer.** Only the orchestrator mutates the DAG
   (insert/re-parent steps, status). A review subprocess *requests* a
   corrective step via a structured channel (NDJSON event + a DB bridge
   row), never writes the DAG itself. Two harness processes never both
   mutate the plan.
4. **Cross-process interruption bridge.** Mirror the V23 skip-bridge: an
   open `interruptions` row is the bridge; a CLI/TUI in a *different*
   process resolves it; the runner observes resolution at the next
   scheduler tick. The run-lock stays per-project; reviews do not take it.

---

## 10. Reviewer-inserted corrective steps & re-parenting

When the reviewer for step **A** rejects:

1. Insert corrective step **A′** with `corrects_step_id = A`,
   `A′ depends_on A`.
2. **Re-parent:** every step that depended on **A** now also depends on
   **A′**. (Without this, dependents would consume the un-corrected
   version — this is the heart of making review *protective* rather than
   cosmetic.)
3. **A** transitions to `Complete` with `review_status = Failed` (its
   commit stays in linear history; the fix lives in A′). Dependents are
   gated by the new structural edge to A′, not by A's status.
4. **Recursion cap.** A′ is itself reviewed. Bound the
   review→correction→review chain with a per-plan
   `max_review_corrections` depth (sibling concept to `max_retries`);
   exceeding it raises a **blocker** interruption ("review loop — needs
   human") rather than spawning indefinitely.

---

## 11. Determinism reframing

ralph's tagline is "Deterministic Execution Planner." This redesign keeps
**per-step determinism** (same inputs → same step behavior, deterministic
tie-broken scheduling order) but adds **dynamic scheduling** (order depends
on human-answer timing) and **first-class nondeterministic review**.

Action: reframe the promise in `README` / `CLAUDE.md` / `TUI-plan.md` as
*"deterministic dependencies & validation, dynamic scheduling, optional
nondeterministic review"* rather than letting the brand drift silently.
This also formally promotes the manual pattern in `docs/review-hooks.md`
to a built-in (the hook system stays for other lifecycle needs;
`review-hooks.md` gets a banner pointing at the built-in).

---

## 12. TUI changes

**Chosen primary view: Option A — dependency outline** (see
`docs/dag-tui-mockups.html`). Swimlanes (B) and connector-graph (C) are
recorded as rejected-for-now alternates, not built. The TUI is the
**largest single workstream**.

### 12.1 Outline view (replaces the flat step list in `plan_detail`)

Topologically ordered, indented by depth; a join step lists every
dependency by short id inline (`deps: …`). Keeps the existing
view/input/render split and per-view `HelpState`; `Blocked` renders as a
derived overlay like today's derived `Question` plan status; reviewer-
inserted steps show a `↳ corrects <short_id>` marker.

### 12.2 Focus / re-root navigation

- `z` (focus) on a step **re-roots the outline at that step**: show only
  that step and its transitive **dependents** (downstream cone);
  unrelated branches are hidden.
- **Direction (confirmed): downstream dependents cone.** Focus shows the
  step and what flows *out* of it; it never widens the prompt or the
  scheduler — purely what's drawn. The **upstream** context (what the
  step depended on) is carried by the breadcrumb path, not re-expanded
  in the body, so both directions are available without a second mode.
- `Z` / `Esc` (or clicking a breadcrumb crumb) **pops back toward the
  true root(s)**. Focus nests; popping unwinds one level, with a
  top-level "back to root" jump.
- The persistent breadcrumb chrome (`src/tui/chrome.rs`) shows the focus
  path, e.g. `add-oauth-login › focus: c9d4`.
- Focus is a **pure view transform** — no DB writes, no scheduler effect.
  Scheduling still spans the whole DAG; focus only filters what's drawn.

### 12.3 Interruptions inbox (`View::Inbox`) — confirmed

- Cross-branch list of every open question/blocker, decoupled from DAG
  navigation; reachable from anywhere via `i` with an open-count badge.
- **Run-through answering:** submitting an answer auto-advances to the
  next open interruption, so the human clears the whole queue in one
  pass without bouncing back to the list. `Esc` exits run-through.
- Resolved items stay visible (dimmed) for recent context.

### 12.4 Answer modal

Ranked proposed answers (priority order, agent's #1 pre-selected) +
optional comment + freeform escape hatch; blocker variant is
resolve / resolve-with-comment (no options). Extends `answer_modal.rs`.
Both the chosen answer and the comment flow into the bounded resolution
injection (§8).

There is deliberately **no** "let the agent decide" shortcut. The agent
raised the interruption precisely because it lacked confidence; resolving
a question requires an explicit human answer (a ranked option or
freeform). Abandoning a step entirely remains `ralph skip` — a separate,
deliberate operator action with its own change-handling — not a one-key
escape inside the answer modal.

### 12.5 Status colors

Each state maps to one color (glyph + title). Because **complete stays
green**, this is a *small, additive* change in `src/tui/theme.rs` — three
existing tokens are untouched, two new tokens are added, one value is
reused. The mapping is applied **TUI-wide** (DAG glyphs, plan-list status
dots, and the derived plan status) so the same concept never shows two
colors across screens.

| State (step / plan) | Color | Hex | theme.rs handling |
|---|---|---|---|
| Complete | green | `#34d058` | `STATUS_COMPLETE` (**unchanged**) |
| Implementing (in progress) | yellow | `#f7d135` | `STATUS_IN_PROGRESS` (**unchanged**) |
| Reviewing (awaiting / in review) | blue | `#3b82f6` | new `STATUS_REVIEWING` (reuses the value formerly on `STATUS_PENDING`) |
| Waiting-for-turn (pending) | bright white | `#f5f7fa` | new `STATUS_WAITING` |
| Failed / review-failed | red | `#ef4444` | `STATUS_FAILED` (**unchanged**) |
| Blocked (question *or* blocker) | orange | `#db6d28` | new `STATUS_BLOCKED` |
| Skipped | dim gray | — | `CHROME_DIM` (**unchanged**) |

Implementation notes:

- *Pending blue → white.* The old `STATUS_PENDING` blue (`#3b82f6`) is no
  longer a "pending" color; that exact value is reused as
  `STATUS_REVIEWING`. The "never run" plan dot and the step "waiting"
  glyph become `STATUS_WAITING`. White must be a deliberately bright
  white, distinct from default body text, or waiting rows won't stand
  out on the dark theme.
- *Reviewing shares its hex with `TOAST_INFO` (`#3b82f6`).* Acceptable —
  a transient info toast and a persistent step glyph are different
  surfaces — but keep them as separate named tokens so a future palette
  edit doesn't couple them.
- *Blocked = orange* (`#db6d28`, new `STATUS_BLOCKED`). The purple
  `STATUS_QUESTION` is retired: the step glyph **and** the derived
  plan-level "interrupted" status both go orange so questions and
  blockers read identically everywhere. (`#db6d28` kept as-is — checked
  for separation against implementing-yellow and failed-red.)
- *Cursor `#f7d135` == implementing yellow* by today's design
  (`in_progress_matches_cursor` test). Keep the cursor a row/border
  highlight + `→` glyph (not a text recolor) so a cursored
  non-implementing row stays distinguishable; loosen/drop that test
  accordingly.

New sub-views follow the existing pure-state-machine + render split so
transitions stay unit-testable without a terminal.

---

## 13. Migration, import & export

### 13.1 DB migration & backward compatibility

- **Existing plans:** V25 backfill turns each into a linear chain DAG +
  short ids → identical execution order, no behavior change.
- **Scripts:** any non-default flag still forces non-interactive behavior;
  deterministic tie-break preserves linear order; `<num>` selectors keep
  working alongside new `<short_id>` selectors.
- **Questions:** V26 copies `step_questions` → `interruptions`; the
  `ralph question list/answer` CLI aliases bridge one release.

### 13.2 Export schema (`src/export.rs`)

Today `ExportedPlan` is a **template**: it deliberately strips ids,
`project`, timestamps, and *all execution state* (attempts, status,
logs). Steps are an ordered `Vec<ExportedStep>` with **no ids**, and
*plan*-level dependencies are carried by **slug** (a portable handle),
each field gated by `#[serde(skip_serializing_if)]` with a matching
`#[serde(default)]` on import so old/new bundles round-trip. The DAG
redesign extends that same pattern — it does not change the philosophy:

- **`ExportedStep` gains `short_id: String`** — always emitted. It is the
  portable, plan-unique edge handle (the internal UUID is still never
  exported). Export uses the step's existing `short_id`; it is not
  re-minted on every export, so a bundle is stable across re-exports.
- **`ExportedStep` gains `depends_on: Vec<String>`** (list of *step*
  `short_id`s), `#[serde(default, skip_serializing_if = "Vec::is_empty")]`
  — so a degenerate linear plan (chain edges) still exports byte-identical
  to pre-DAG output for the common case, and only genuinely-branched
  plans carry edge data.
- **Review toggle is plan template data, so it is exported:**
  `ExportedPlanMeta.review_enabled: Option<bool>` and
  `ExportedStep.review_enabled: Option<bool>`, both
  `skip_serializing_if = "Option::is_none"` (mirrors the existing
  `retry_strategy` treatment). The **review harness/model** is *global
  config*, not plan data — it is **not** exported (a bundle stays
  portable across machines whose review harness differs).
- **Not exported (runtime state, by existing policy):** interruptions
  (open or resolved), `review_status`, `attempts`, iteration commits,
  `corrects_step_id` linkage. A reviewer-inserted corrective step *is*
  exported as an ordinary step (it became a real step with its own
  `short_id` and edges); its provenance pointer is dropped because it is
  history, not template.
- The step array order is preserved and is the deterministic
  scheduler tie-break seed (`sort_key`) on import.
- `ralph_rs_version` already stamps the bundle; bump it so an importer
  can detect a DAG-aware bundle vs. a legacy one.

### 13.3 Import behavior (`src/import.rs`)

- **Legacy bundle (no `short_id` / no per-step `depends_on`):** mint a
  fresh plan-unique `short_id` per step and synthesize a linear chain
  (step *k* depends on *k−1* by array order) — *exactly* the V25
  migration backfill, so import and migration produce the same DAG for
  the same linear input.
- **DAG-aware bundle:** validate on import, before any write:
  1. every `depends_on` entry resolves to a `short_id` present in the
     same bundle (no dangling edges);
  2. `short_id`s are unique within the bundle;
  3. the edge set is acyclic — reuse the `would_create_step_cycle`
     check (the §6 analogue of `storage::would_create_cycle`), applied to
     the whole imported edge set;
  4. ≥1 root (a step with no `depends_on`).
  Any failure aborts the import with a precise message (which short_id,
  which rule) — no partial plan is written.
- **`--slug` / `--branch` overrides** are unchanged. `short_id`s are
  preserved from the bundle on import (they're plan-scoped, so they don't
  collide across plans); only on the legacy-backfill path are they
  minted.
- **`--strict`** additionally rejects a bundle whose
  `review_enabled` is set but whose target machine has no review harness
  configured (consistent with `--strict` already rejecting unknown
  harness/agent names). Non-strict import keeps the toggle but
  `ralph doctor` warns until a review harness is configured.
- Round-trip guarantee: `export → import` of any plan reproduces the same
  DAG (edges, roots, review toggles, step order); only stripped runtime
  state and the cosmetic provenance pointer are not restored.

---

## 14. Open questions / risks

1. **Iteration-commit history noise** — keep every iteration commit
   (audit) vs. squash completed steps (clean history). Recommend keep +
   optional `--squash-on-complete`. Needs a decision before §5 lands.
2. **Diamond dependencies + linear WIP entanglement** — D depends on B
   and C; B blocks; C commits; D waits for both. C's commit sits on top
   of B's last (incomplete) iteration. Accepted per Decision 1, but the
   plan-generation prompts should discourage *deep* independent branches
   that share a late join, to limit entanglement surface.
3. **"Runnable" when a dependency is `Failed`** (retries exhausted, no
   corrective step) — dependents stay `Pending` forever unless the human
   intervenes. Need an explicit operator path (`ralph step reset` /
   skip-with-accept) and TUI affordance.
4. **Scheduler reproducibility** — deterministic given identical human
   inputs *and timing-independent* tie-break (we picked one); document
   that wall-clock interleave of concurrent reviews is *not* part of the
   reproducibility guarantee.
5. **Review of a corrective step that itself fails review** — bounded by
   `max_review_corrections` (§10) then escalates to a blocker.
6. **Skip/resume semantics under the DAG** — `ralph skip` on a step with
   dependents: skip just that step (dependents may become unreachable) vs.
   skip its subtree. Needs explicit semantics + confirm prompt.
7. **`change_policy` for reviewer-inserted steps** — corrective steps are
   `Required` (they must change code) but the reviewer step itself is a
   read-only review (no commit, `Optional`/forbidden). The existing
   `ChangePolicy::Optional` covers the latter.
8. The CI `ETXTBSY` test footgun (`CLAUDE.md`) is unaffected but new
   review/scheduler tests must keep using `sh_editor()`-style invocation.

---

## 15. Phased implementation plan

Each phase is independently shippable and leaves linear-plan behavior
unchanged until the phase that needs the change.

- **Phase 1 — DAG foundation (no behavior change for linear plans).**
  V25 (short ids + `step_dependencies` + cycle detection), the
  topological scheduler with deterministic tie-break, `step dependency`
  CLI, export/import fields, migration backfill. A linear plan executes
  exactly as before.
- **Phase 2 — Interruptions.** V26, unified `interruptions` model,
  `ralph block` + `question ask --priority`, cross-process bridge,
  branch-level `Blocked` + scheduler skip-to-other-branch, bounded
  resolution injection (fixes the §4 leak), `interruption` CLI.
- **Phase 3 — Built-in review pipeline.** V27, per-iteration commit +
  trailers, `RetryStrategy` reinterpretation, read-only reviewer prompt,
  concurrency (impl semaphore + concurrent read-only review),
  corrective-step insertion + re-parenting + recursion cap, review
  config/scopes.
- **Phase 4 — TUI.** DAG outline + swimlane toggle, interruptions inbox
  view, ranked-answer modal, review badges. Driven by the approved option
  from `docs/dag-tui-mockups.html`.

Phases 1–3 are testable headless (the project's strength); Phase 4 follows
the existing pure-state-machine + render split so transitions stay
unit-testable without a terminal.
