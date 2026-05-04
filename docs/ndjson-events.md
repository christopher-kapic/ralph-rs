# NDJSON event schema

When `--json` (or its alias `--jsonl`) is active on `ralph run` or
`ralph resume`, ralph emits one JSON object per line on stdout. Each
event is a tagged variant of the `RunEvent` enum (see
`src/output.rs`); the discriminator field is `event` and uses
`snake_case` for both the tag and all field names.

Consumers should ignore unknown event types — the schema is additive
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

## Lifecycle events

### `step_started`

Emitted when the runner begins executing a step.

```json
{
  "event": "step_started",
  "step_id": "<uuid>",
  "step_title": "Add foo",
  "step_num": 3,
  "step_total": 12
}
```

### `step_finished`

Emitted when a step terminates (success, failure, or skip).

```json
{
  "event": "step_finished",
  "step_id": "<uuid>",
  "step_title": "Add foo",
  "step_num": 3,
  "step_total": 12,
  "outcome": "success",
  "attempts": 1,
  "duration_secs": 42.5
}
```

`outcome` is one of `success`, `failed`, `skipped`, or `aborted`.

### `plan_complete`

Emitted at the end of the run with aggregate counts. Kept as a compat
shim for one release alongside the newer `summary` event — meta-harness
consumers pinned to `plan_complete` should migrate to `summary` for the
richer payload, but both will be emitted in tandem until the shim is
removed.

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

### `summary`

The new authoritative final event for `ralph run`. Carries timing and
cost information not present in `plan_complete`. `cost_usd` is omitted
when no usage data is available (e.g., a harness that doesn't report
costs).

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

`plan_status` matches the `PlanStatus` enum: `planning`, `ready`,
`in_progress`, `complete`, `failed`, `aborted`, `archived`, `question`.

### `stale_steps_swept`

Emitted at run start when orphaned `in_progress` step rows from a
previous interrupted run are flipped to `aborted`.

```json
{
  "event": "stale_steps_swept",
  "steps": [
    { "step_id": "<uuid>", "step_num": 4, "title": "Wire up X" }
  ]
}
```

### `plan_grew`

Emitted mid-run when the running agent inserts new steps via
`ralph step add` between iterations of the runner loop.

```json
{
  "event": "plan_grew",
  "steps": [
    { "step_id": "<uuid>", "step_num": 8, "title": "Follow-up" }
  ]
}
```

### `prompt_prepared`

Emitted immediately before the harness is spawned for a given attempt.
`prompt_preview` is always the first 512 chars (independent of
`--verbose`) so consumers see a stable bounded payload; the full
prompt is persisted in `execution_logs.prompt_text`.

```json
{
  "event": "prompt_prepared",
  "step_id": "<uuid>",
  "attempt": 1,
  "prompt_chars": 18204,
  "prompt_preview": "You are running as part of a `ralph` plan…"
}
```

---

## Streaming events

These events fire mid-step at relatively high frequency. They are
intended for live UIs (the TUI's status pane, future watch dashboards)
that don't want to poll `run_locks` and re-read `execution_logs`.

### `harness_chunk`

Line-buffered chunk of harness output, one emit per newline. `seq` is
a monotonic counter scoped to the run so consumers can reorder if
needed. `text` is truncated past `Config.harness_chunk_max_bytes`
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

`stream` is `stdout` or `stderr`.

### `test_chunk`

Same shape as `harness_chunk`, but scoped to the deterministic-test
phase. `test_index` indexes into the plan's `deterministic_tests`
array, so consumers can correlate output with the specific test
command being run.

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
TUI redraw the phase indicator without polling.

```json
{
  "event": "phase_changed",
  "phase": "tests"
}
```

`phase` matches the `Phase` enum: `idle`, `pre_step_hook`, `harness`,
`pre_test_hook`, `tests`, `post_test_hook`, `commit`, `rollback`,
`post_step_hook`.

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
