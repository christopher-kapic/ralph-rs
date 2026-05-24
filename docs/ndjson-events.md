# NDJSON event schema

When `--json` (or its alias `--jsonl`) is active on `ralph run` or
`ralph resume`, ralph emits one JSON object per line on stdout. Each
event is a tagged variant of the `RunEvent` enum (see
`src/output.rs`); the discriminator field is `event` and uses
`snake_case` for both the tag and all field names.

The TUI subscribes to this same stream when it spawns its own runner
subprocess (see `src/tui/events.rs`), so anything documented here is
both the public NDJSON contract and the internal IPC channel between
the TUI and the runner.

Consumers should ignore unknown event types — the schema is **additive**
and new variants may be introduced in any release.

The events fall into two groups:

- **Lifecycle** events mark durable state transitions (a step begins,
  finishes, the plan completes). They are stable, low-frequency, and
  written before the run-lock is released so they are guaranteed to
  appear in any successful run.
- **Streaming** events surface in-flight progress (harness output,
  test output, phase changes). They are intended for the TUI and
  similarly latency-sensitive consumers; durable per-attempt logs
  remain in the SQLite `execution_logs` table.

---

## Quick reference

| Event              | Group     | Emitted                                                    |
|--------------------|-----------|------------------------------------------------------------|
| `run_started`      | lifecycle | First event of a run; anchors elapsed-timer subscribers    |
| `step_started`     | lifecycle | Once per step at the start of its first/next attempt       |
| `step_finished`    | lifecycle | Once per step on success / failure / skip                  |
| `plan_complete`    | lifecycle | Once at the end of a run (compat shim — see `summary`)     |
| `summary`          | lifecycle | Once at the end of a run; final event in the stream        |
| `stale_steps_swept`| lifecycle | Once at run start when orphaned `in_progress` rows exist   |
| `plan_grew`        | lifecycle | After each runner-loop iteration when new steps appeared   |
| `prompt_prepared`  | lifecycle | Once per attempt, immediately before harness spawn         |
| `attempt_cancelled`| lifecycle | When the TUI skip dialog's Esc/cancel undoes an in-flight attempt |
| `paused_by_user`   | lifecycle | When the runner exits cleanly on an operator pause request |
| `review_started`   | lifecycle | When a read-only reviewer is spawned against a committed iteration |
| `review_finished`  | lifecycle | When a reviewer returns a pass/fail verdict                 |
| `corrective_step_requested`  | lifecycle | When a failed review requests a corrective step (the reviewer-side half of the structured channel) |
| `corrective_step_inserted`   | lifecycle | When the orchestrator has inserted `A′` and re-parented `A`'s dependents |
| `review_loop_escalated`      | lifecycle | When the review→correction chain hits the per-plan cap and a blocker is raised |
| `harness_chunk`    | streaming | Per newline of harness stdout/stderr during the harness phase |
| `test_chunk`       | streaming | Per newline of test stdout/stderr during the tests phase   |
| `phase_changed`    | streaming | On every `run_locks.phase` transition                      |

> **There is no `interruption_raised` / `interruption_resolved` event.**
> Questions and blockers (the unified *interruptions* model) do **not**
> emit a dedicated NDJSON variant. A harness raising `ralph question ask`
> / `ralph block` writes a row to the `interruptions` table and exits; the
> orchestrator observes it at the next scheduler tick and the step's
> derived `Blocked` overlay surfaces through the normal DB re-read the TUI
> already performs (the `run_locks` table is the cross-process bridge).
> Consumers that need interruption state poll the DB / `ralph interruption
> list`, not the event stream. The review→loop escalation path is the one
> case where an interruption (a `kind=blocker`) is *announced* on the
> stream — via `review_loop_escalated`, documented below.

---

## Lifecycle events

### `run_started`

The first event a `--json` runner emits, fired once after the per-project
run lock has been acquired and the plan has been marked
`in_progress`. Carries the wall-clock instant the runner began so
elapsed-timer subscribers (the TUI's in-process attach path, log
shippers) have a base instant before any `phase_changed` lands.

Payload:

| Field        | Type               | Notes                                |
|--------------|--------------------|--------------------------------------|
| `plan_slug`  | `string`           | Plan slug being run.                 |
| `started_at` | `string` (RFC3339) | UTC instant the run began.           |

```json
{
  "event": "run_started",
  "plan_slug": "tui-v1",
  "started_at": "2026-04-22T18:00:00Z"
}
```

Consumer expectations: precedes every other event in the stream.
Consumers that don't render an elapsed timer can ignore it.

### `step_started`

Emitted when the runner begins executing a step. Fires once per step
even if the step is retried — `step_finished.attempts` carries the
attempt count.

Payload:

| Field        | Type      | Notes                                              |
|--------------|-----------|----------------------------------------------------|
| `step_id`    | `string`  | UUID v4. Stable across retries and restarts.      |
| `step_title` | `string`  | Step title at start of execution.                  |
| `step_num`   | `integer` | 1-based position in the plan at emit time.         |
| `step_total` | `integer` | Total step count in the plan at emit time.         |

```json
{
  "event": "step_started",
  "step_id": "9b8c4f2c-…-…-…-…",
  "step_title": "Add foo",
  "step_num": 3,
  "step_total": 12
}
```

Consumer expectations: pair with `step_finished` by `step_id`. Don't
assume `step_num`/`step_total` are stable across the run — they shift
when the agent inserts new steps mid-run (see `plan_grew`).

### `step_finished`

Emitted when a step terminates (success, failure, or skip).

Payload:

| Field           | Type      | Notes                                                  |
|-----------------|-----------|--------------------------------------------------------|
| `step_id`       | `string`  | UUID matching the prior `step_started`.                |
| `step_title`    | `string`  | Step title at finish.                                  |
| `step_num`      | `integer` | 1-based position at finish.                            |
| `step_total`    | `integer` | Total step count at finish.                            |
| `outcome`       | `string`  | One of `success`, `failed`, `skipped`, `aborted`, `timeout`, `paused_for_question`. See note below on `paused_for_question`. |
| `attempts`      | `integer` | Number of harness invocations this step consumed.      |
| `duration_secs` | `number`  | Wall-clock seconds from `step_started` to finish.      |

```json
{
  "event": "step_finished",
  "step_id": "9b8c4f2c-…-…-…-…",
  "step_title": "Add foo",
  "step_num": 3,
  "step_total": 12,
  "outcome": "success",
  "attempts": 1,
  "duration_secs": 42.5
}
```

Consumer expectations: a `step_finished` is guaranteed to follow each
`step_started`, including when the runner is killed mid-flight (the
runner emits `outcome: aborted`). Stuck-state recovery is via
`stale_steps_swept` on the next run.

> **`outcome: paused_for_question` is now a two-way street.** It was
> originally the "harness raised an interruption mid-attempt"
> termination — `ralph question ask` / `ralph block` wrote an `open`
> row, the harness exited cleanly, the executor parked the branch
> `Blocked` with no retry-budget consumed. The shipped post-redesign
> executor *also* emits this outcome when a step **exhausts its retry
> budget** on `TestFailed` or `CommitFailed` (the retry-exhaustion
> auto-blocker — `docs/dag-redesign.md` §3.4-bis): the executor inserts
> a `kind=Blocker` interruption with two ranked options ("Retry the
> step from scratch" / "Mark step Failed") and parks the step at
> `status=Pending` with `attempts == max_attempts`, the dirty tree
> rolled back. `StepOutcome::PausedForQuestion` is reused — no new
> outcome variant — so a downstream consumer cannot distinguish the two
> cases from the event stream alone. To tell them apart, poll
> `interruptions` for the step and inspect the most recent open row:
> `kind=blocker` with two options whose `text` matches
> `executor::RETRY_EXHAUSTED_OPTION_RETRY` /
> `RETRY_EXHAUSTED_OPTION_FAIL` is the auto-blocker; anything else
> (typically `kind=question` or a freeform `kind=blocker` body) is a
> harness-raised pause. The same NDJSON-quiet rule that applies to
> harness-raised interruptions applies here: **no
> `interruption_raised` / `interruption_resolved` event is emitted** in
> either case. The only interruption announced on the stream remains
> the review-loop escalation (`review_loop_escalated`).

### `plan_complete`

Emitted at the end of the run with aggregate counts. Kept as a compat
shim alongside the newer `summary` event — meta-harness consumers
pinned to `plan_complete` should migrate to `summary` for the richer
payload, but both are emitted in tandem until the shim is removed.

Payload:

| Field             | Type      | Notes                                              |
|-------------------|-----------|----------------------------------------------------|
| `plan_slug`       | `string`  | Plan slug.                                         |
| `final_status`    | `string`  | `PlanStatus` discriminant (see `summary`).         |
| `steps_executed`  | `integer` | Steps that ran (success or failed).                |
| `steps_succeeded` | `integer` | Steps that completed.                              |
| `steps_failed`    | `integer` | Steps that exited unsuccessfully.                  |

```json
{
  "event": "plan_complete",
  "plan_slug": "tui-v1",
  "final_status": "complete",
  "steps_executed": 12,
  "steps_succeeded": 12,
  "steps_failed": 0
}
```

Consumer expectations: prefer `summary` for new integrations.

### `summary`

The new authoritative final event for `ralph run`. Carries timing and
cost information not present in `plan_complete`. `cost_usd` is omitted
when no usage data is available (e.g., a harness that doesn't report
costs).

Payload:

| Field            | Type           | Notes                                              |
|------------------|----------------|----------------------------------------------------|
| `plan_status`    | `string`       | `PlanStatus` discriminant.                         |
| `steps_complete` | `integer`      | Steps with `Complete` status at run end.           |
| `steps_total`    | `integer`      | Total steps in the plan at run end.                |
| `duration_secs`  | `number`       | Wall-clock seconds for the whole run.              |
| `cost_usd`       | `number?`      | Optional. Aggregate cost; omitted when unavailable.|
| `started_at`     | `string` (RFC3339) | UTC start instant.                             |
| `ended_at`       | `string` (RFC3339) | UTC end instant.                               |

```json
{
  "event": "summary",
  "plan_status": "complete",
  "steps_complete": 12,
  "steps_total": 12,
  "duration_secs": 1927.0,
  "cost_usd": 0.42,
  "started_at": "2026-04-22T18:00:00Z",
  "ended_at": "2026-04-22T18:32:07Z"
}
```

`plan_status` matches the `PlanStatus` enum:
`planning`, `ready`, `in_progress`, `complete`, `failed`, `aborted`,
`archived`, `interrupted`. (`interrupted` is the post-DAG-redesign
rename of the old `question` variant — it is a *derived* status,
reported whenever any step in the plan has an open interruption, never
stored. `question` is still accepted on the parse side for one release
as a legacy alias, but `summary`/`plan_complete` always *emit*
`interrupted`.)

Consumer expectations: this is the **last** event in a clean run.
Treat any subsequent line on stdout as out-of-band noise.

### `stale_steps_swept`

Emitted at run start when orphaned `in_progress` step rows from a
previous interrupted run are flipped to `aborted`. Helps consumers
explain "why did `step_finished` for these steps never fire on the
prior run?" without having to re-read `execution_logs`.

Payload:

| Field   | Type    | Notes                                          |
|---------|---------|------------------------------------------------|
| `steps` | `array` | Compact step refs: `{step_id, step_num, title}`. Empty array means there was nothing to sweep. |

```json
{
  "event": "stale_steps_swept",
  "steps": [
    { "step_id": "9b8c4…", "step_num": 4, "title": "Wire up X" }
  ]
}
```

### `plan_grew`

Emitted between iterations of the runner loop when the running agent
inserts new steps via `ralph step add`. Same payload shape as
`stale_steps_swept`.

```json
{
  "event": "plan_grew",
  "steps": [
    { "step_id": "9b8c4…", "step_num": 8, "title": "Follow-up" }
  ]
}
```

Consumer expectations: refresh any cached step-list view (the TUI
re-reads from the DB and re-renders). The `step_num` values reflect
positions at emit time and shift again on the next `plan_grew`.

### `prompt_prepared`

Emitted immediately before the harness is spawned for a given attempt.
`prompt_preview` is always the first 512 chars (independent of
`--verbose`) so consumers see a stable bounded payload; the full
prompt is persisted in `execution_logs.prompt_text`.

Payload:

| Field            | Type      | Notes                                              |
|------------------|-----------|----------------------------------------------------|
| `step_id`        | `string`  | UUID of the step being attempted.                  |
| `attempt`        | `integer` | 1-based attempt number for this step.              |
| `prompt_chars`   | `integer` | Total characters in the full prompt.               |
| `prompt_preview` | `string`  | First 512 chars (UTF-8 boundary respected).        |

```json
{
  "event": "prompt_prepared",
  "step_id": "9b8c4…",
  "attempt": 1,
  "prompt_chars": 18204,
  "prompt_preview": "You are running as part of a `ralph` plan…"
}
```

### `attempt_cancelled`

Emitted when the **TUI skip dialog's Esc/cancel path** undoes an
in-flight attempt (the user pressed `s` on a running step, the harness
was killed, and they then cancelled the change-handling dialog instead
of choosing stash/commit/discard).

Unlike `step_finished`, this is **not** terminal for the step. Before
emitting it the runner has:

- rolled the working tree back (preserving the user's pre-existing
  untracked files),
- written **no** `execution_logs` row for the cancelled attempt
  (the row created with the prompt before the harness spawned is
  deleted), and
- reset the cancel channel and the persisted attempt counter so it can
  re-enter the retry loop at the **same** `attempt` number.

Net effect: the cancelled attempt consumes **no** retry budget and
leaves **no** `UNIQUE(step_id, attempt)` row behind. Another
`prompt_prepared` / `phase_changed` for the same `step_id` at the same
`attempt` number follows.

Payload:

| Field      | Type               | Notes                                              |
|------------|--------------------|----------------------------------------------------|
| `step_id`  | `string`           | UUID of the step whose attempt was cancelled.      |
| `step_num` | `integer`          | 1-based position in the plan at emit time.         |
| `attempt`  | `integer`          | 1-based number of the attempt that was cancelled.  |
| `at`       | `string` (RFC3339) | UTC instant the cancellation was processed.        |

```json
{
  "event": "attempt_cancelled",
  "step_id": "9b8c4…",
  "step_num": 3,
  "attempt": 2,
  "at": "2026-05-16T09:30:00Z"
}
```

Consumer expectations: do **not** treat this as a step terminating.
Decrement any "attempts used" counter you derived from
`prompt_prepared`, and expect the same `step_id`/`attempt` to reappear.
It is only emitted on the TUI-spawned runner stream — CLI `ralph skip
--changes …` never produces it (those choices always finalize the
step).

### `paused_by_user`

Emitted when the runner exits cleanly because the operator requested a
pause (`plans.pause_requested`, set by the TUI `[P]` keybinding or
`ralph pause`). Distinct from `plan_complete` / `summary` so a consumer
can tell a deliberate pause from completion (the TUI surfaces a
"Paused. Use `ralph resume` to continue." toast on this event).

Payload:

| Field       | Type     | Notes                          |
|-------------|----------|--------------------------------|
| `plan_slug` | `string` | Plan slug that was paused.     |

```json
{ "event": "paused_by_user", "plan_slug": "dag-redesign" }
```

Consumer expectations: terminal for the run, like `summary` — the
runner exits after emitting it. The plan is resumable with
`ralph resume`.

### `review_started`

Emitted the moment a read-only reviewer subprocess is spawned against a
committed iteration (docs/dag-redesign.md §3.2 / §9-inv-2). The review
runs **concurrently** with the next unrelated implementation and is
read-only w.r.t. the working tree (it operates against the fixed
`commit_sha` in a throwaway isolated git worktree, never the live
tree).

Payload:

| Field        | Type      | Notes                                              |
|--------------|-----------|----------------------------------------------------|
| `step_id`    | `string`  | UUID of the step whose iteration is being reviewed.|
| `step_num`   | `integer` | 1-based position at emit time.                     |
| `commit_sha` | `string`  | Full SHA of the reviewed iteration commit.         |
| `iteration`  | `integer` | 1-based iteration number (`<short_id>.<n>`).       |

```json
{
  "event": "review_started",
  "step_id": "9b8c4…",
  "step_num": 7,
  "commit_sha": "1a2b3c4d…",
  "iteration": 2
}
```

Consumer expectations: pair with `review_finished` by `step_id` +
`commit_sha`. There is no `StepStatus::AwaitingReview` — a review-gated
step stays `InProgress`; gating is structural (dependents wait for the
step to become `Complete`).

### `review_finished`

Emitted when a reviewer returns a verdict. `passed: true` ⇒ `REVIEW
PASS` (the step is promoted toward `Complete`, `review_status =
Passed`); `passed: false` ⇒ `REVIEW FAIL` (a `corrective_step_requested`
follows). The `Ralph-Review` value is recorded as a git **note** on
`refs/notes/ralph-review` for the reviewed commit (`passed` / `failed`)
— **not** by amending the commit, so history and the working tree are
untouched under concurrency.

Payload:

| Field        | Type      | Notes                                              |
|--------------|-----------|----------------------------------------------------|
| `step_id`    | `string`  | UUID matching the prior `review_started`.          |
| `step_num`   | `integer` | 1-based position at emit time.                     |
| `commit_sha` | `string`  | Full SHA of the reviewed iteration commit.         |
| `iteration`  | `integer` | 1-based iteration number.                          |
| `passed`     | `boolean` | `true` = pass; `false` = fail (correction follows).|

```json
{
  "event": "review_finished",
  "step_id": "9b8c4…",
  "step_num": 7,
  "commit_sha": "1a2b3c4d…",
  "iteration": 2,
  "passed": false
}
```

### `corrective_step_requested`

The **reviewer-side half** of the single-DAG-writer structured channel
(docs/dag-redesign.md §9-inv-3). A failed review never mutates the DAG
itself; it *requests* a corrective step by emitting this event **and**
writing a `corrective_step_requests` bridge row. The orchestrator — the
sole DAG writer — consumes the bridge row at a later scheduler tick and
performs the §10 insert + re-parent (announced separately by
`corrective_step_inserted`). Always preceded by a
`review_finished` with `passed: false` for the same step/commit.

Payload:

| Field               | Type      | Notes                                              |
|---------------------|-----------|----------------------------------------------------|
| `reviewed_step_id`  | `string`  | UUID of the step `A` whose review failed.          |
| `reviewed_step_num` | `integer` | 1-based position at emit time.                     |
| `commit_sha`        | `string`  | SHA of the reviewed iteration commit.              |
| `iteration`         | `integer` | 1-based iteration number that failed review.       |
| `issues`            | `integer` | Issue count parsed from the reviewer's verdict.    |

```json
{
  "event": "corrective_step_requested",
  "reviewed_step_id": "9b8c4…",
  "reviewed_step_num": 7,
  "commit_sha": "1a2b3c4d…",
  "iteration": 2,
  "issues": 3
}
```

Consumer expectations: this is a *request*, not the mutation. Wait for
`corrective_step_inserted` (or `review_loop_escalated`) before assuming
the DAG changed.

### `corrective_step_inserted`

Emitted when the orchestrator (sole writer) has inserted corrective step
`A′` (`corrects_step_id = A`, `A′ depends_on A`) and re-parented every
former dependent of `A` onto `A′` (docs/dag-redesign.md §10). `A` itself
transitions to `Complete` with `review_status = Failed` — its commit
stays in linear history; the fix lives in `A′` and dependents are gated
by the new structural edge.

Payload:

| Field                 | Type     | Notes                                              |
|-----------------------|----------|----------------------------------------------------|
| `corrective_step_id`  | `string` | UUID of the newly inserted corrective step `A′`.   |
| `corrective_short_id` | `string` | 8-char `short_id` of `A′` (the user-facing handle).|
| `corrects_step_id`    | `string` | UUID of the reviewed step `A` that `A′` corrects.  |

```json
{
  "event": "corrective_step_inserted",
  "corrective_step_id": "c0ffee…",
  "corrective_short_id": "a1b2c3d4",
  "corrects_step_id": "9b8c4…"
}
```

Consumer expectations: refresh any cached step-list / DAG view — the
edge set changed (the new step plus re-parented edges).

### `review_loop_escalated`

Emitted when the review→correction→review chain hits the per-plan
`max_review_corrections` cap (docs/dag-redesign.md §10 item 4 / §14.5).
Instead of spawning another correction, the orchestrator raises a
`kind=blocker` interruption ("review loop — needs human") on the
offending step and stops the chain. This is the only place an
interruption is announced on the event stream (interruptions otherwise
have no NDJSON variant — see the note in the quick reference).

Payload:

| Field       | Type      | Notes                                                   |
|-------------|-----------|---------------------------------------------------------|
| `step_id`   | `string`  | UUID of the step whose correction chain was capped.     |
| `step_num`  | `integer` | 1-based position at emit time.                          |
| `chain_len` | `integer` | Number of corrections already applied to this chain.    |
| `cap`       | `integer` | The per-plan `max_review_corrections` value that was hit.|

```json
{
  "event": "review_loop_escalated",
  "step_id": "9b8c4…",
  "step_num": 7,
  "chain_len": 3,
  "cap": 3
}
```

Consumer expectations: the step is now `Complete`/`review_status =
Failed` with an **open blocker** keeping its dependents gated until a
human resolves it (`ralph interruption resolve`). No further corrective
steps will be spawned for this chain.

---

## Streaming events

These events fire mid-step at relatively high frequency. They are
intended for live UIs (the TUI's right-pane tails, future watch
dashboards) that don't want to poll `run_locks` and re-read
`execution_logs`. Consumers that don't render live progress can
ignore them entirely.

### `harness_chunk`

Line-buffered chunk of harness output, one emit per newline. Emitted
during the harness phase only; bracketed by a `phase_changed` for
`harness` start and the next `phase_changed` (or `step_finished`).

Payload:

| Field    | Type      | Notes                                                |
|----------|-----------|------------------------------------------------------|
| `stream` | `string`  | `stdout` or `stderr`.                                |
| `text`   | `string`  | One line, **with** trailing `\n`.                    |
| `seq`    | `integer` | Monotonic per run; consumers can reorder if needed.  |

`text` is truncated past `Config.harness_chunk_max_bytes`
(default 4096 bytes); the durable per-attempt full output is persisted
in `execution_logs.harness_stdout` / `harness_stderr`.

```json
{
  "event": "harness_chunk",
  "stream": "stdout",
  "text": "writing src/foo.rs…\n",
  "seq": 117
}
```

Consumer expectations: append to a per-step rolling buffer. The TUI
caps its tail at `TAIL_BUFFER_LINES` (see `src/tui/events.rs`); pick a
similar bound to avoid unbounded growth.

### `test_chunk`

Same shape as `harness_chunk`, but scoped to the deterministic-test
phase. `test_index` indexes into the plan's `deterministic_tests`
array, so consumers can correlate output with the specific test
command being run. Emitted only between a `phase_changed: tests`
event and the next `phase_changed` / `step_finished`.

Payload:

| Field        | Type      | Notes                                                |
|--------------|-----------|------------------------------------------------------|
| `test_index` | `integer` | 0-based index into `plan.deterministic_tests`.       |
| `stream`     | `string`  | `stdout` or `stderr`.                                |
| `text`       | `string`  | One line, **with** trailing `\n`.                    |
| `seq`        | `integer` | Monotonic per run.                                   |

```json
{
  "event": "test_chunk",
  "test_index": 1,
  "stream": "stderr",
  "text": "FAIL src/foo.rs - tests::bar\n",
  "seq": 312
}
```

### `phase_changed`

Emitted on every transition recorded into `run_locks.phase`. Lets the
TUI redraw the phase indicator without polling. Phases are stable
across runs of the same step, so consumers can build a mini state
machine off these events alone.

Payload:

| Field              | Type               | Notes                                       |
|--------------------|--------------------|---------------------------------------------|
| `phase`            | `string`           | `Phase` discriminant (see below).           |
| `phase_started_at` | `string` (RFC3339) | UTC instant of this transition; mirrors `run_locks.phase_started_at`. |

```json
{
  "event": "phase_changed",
  "phase": "tests",
  "phase_started_at": "2026-04-22T18:00:05Z"
}
```

`phase` matches the `Phase` enum: `idle`, `pre_step_hook`, `harness`,
`pre_test_hook`, `tests`, `post_test_hook`, `commit`, `rollback`,
`post_step_hook`.

Consumer expectations: the first `phase_changed` for a step is always
`pre_step_hook` (or `harness` when no pre-hooks attach); the last is
`commit` on success or `rollback` on failure. Subsequent steps reset
the phase machine to `idle` before the next `step_started`.

---

## Configuration

| Key                         | Default | Purpose                                                                                            |
|-----------------------------|---------|----------------------------------------------------------------------------------------------------|
| `harness_chunk_max_bytes`   | `4096`  | Caps the byte length of a single `harness_chunk` / `test_chunk` `text` payload before truncation.  |

---

## Compatibility

- New event variants are **additive**. Consumers MUST ignore unknown
  `event` discriminators.
- New optional fields on existing events MUST be ignored when absent.
- `plan_complete` is preserved for one release as a compat shim
  alongside `summary`. New integrations should consume `summary`.
- Streaming events (`harness_chunk`, `test_chunk`, `phase_changed`)
  are guaranteed to interleave with lifecycle events on a single
  stdout — there is no separate streaming channel.
