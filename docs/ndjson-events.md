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
| `harness_chunk`    | streaming | Per newline of harness stdout/stderr during the harness phase |
| `test_chunk`       | streaming | Per newline of test stdout/stderr during the tests phase   |
| `phase_changed`    | streaming | On every `run_locks.phase` transition                      |

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
| `outcome`       | `string`  | One of `success`, `failed`, `skipped`, `aborted`.      |
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
`archived`, `question`.

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
