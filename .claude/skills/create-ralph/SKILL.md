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
- You want the option to roll back a step on failure.

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

## Recommended plan shape

Default to: **build → verify → review → fix-as-needed**, repeated per phase.

- **Build steps** (default `change_policy=required`): the agent writes code. Ralph fails the step if no diff is produced.
- **Verify step** (a deterministic test like `cargo test`): catches "code compiled but the page is broken in the browser" failures that reviewers can't replace. Don't skip this even if you also have a review step.
- **Review steps** (use a different harness if available — e.g. `codex` when the implementer is `claude`): another model audits the diff. Set:
  - `--change-policy optional` — reviewers normally don't write code; without this the step fails for producing no diff.
  - `--max-retries 1` — review steps shouldn't retry-loop on disagreement.
  - `--harness codex-orchestrator` if the review step is supposed to **append fix steps via `ralph step add`** — that writes to ralph's DB outside the workspace, which the default sandbox blocks.
- **Fix steps**: appended by the reviewer to the end of the plan (no `--after`) when issues are found. Reordering is one `ralph step move` away if the user wants it.

## Authoring (this is where the gotchas hide)

### Prefer `--import-json` for anything non-trivial

Build a JSON array and pipe it to `ralph step add --import-json -`. JSON sidesteps every shell-quoting failure mode at once. Backticks, dollar signs, parentheses, and double quotes in code references all need escaping in inline `--description` strings, and the failure mode is **silent**: bash command-substitutes backticks at variable-expansion time, your references disappear, and you only notice when the harness produces wrong output.

```bash
# Build the steps as JSON. Each entry's `description` is a real string, no shell quoting.
ralph step add --import-json - <<'JSON'
[
  {
    "title": "Add UserService struct",
    "description": "Add `UserService` in `src/services/user.rs` with methods `create`, `get_by_id`, `delete`. Follow the pattern in `src/services/auth.rs`. Acceptance: `cargo test services::user` passes."
  },
  {
    "title": "Wire UserService into the API",
    "description": "..."
  }
]
JSON
```

### When inline is acceptable, use a quoted heredoc + tempfile

For one-off cases where JSON is overkill, write the description to a tempfile via a **quoted** heredoc (`<<'EOF'`, single-quoted to disable expansion) and pass it via `$(cat $tempfile)`. Never inline `--description "...$VAR...\`backtick\`..."` — bash expands the backticks even with backslash escapes.

### The vanilla flow (only for short, simple descriptions)

```bash
ralph plan create <slug> --description "..." --test "<cmd>"
ralph step add "<Step 1 title>" <slug> --description "<short desc>"
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

## Codex review prompts (when codex is the reviewer)

- Tell codex to read `AGENTS.md` / `CLAUDE.md` and per-phase patterns explicitly — it doesn't load them by default.
- Make codex emit a **structured verdict** so downstream steps can parse it: a single line of either `REVIEW PASS — <one-line summary>` or `REVIEW FAIL — N issue(s)`, followed by a numbered list when failing.
- If you want codex to append fix steps, **describe the action in prose**, not as a copy-paste shell snippet — that re-introduces the embedded-quoting trap. Tell it: "for each issue, run `ralph step add` with `--harness claude`, `--change-policy required`, and a description containing the restated issue, the affected files, the exact fix, the patterns to consult, and the acceptance signal."
- The codex review step itself needs `--harness codex-orchestrator` if it will call `ralph step add`.

## Branching

Use `--branch feat/<slug>` so reviewers can run `git diff main..HEAD` from any step to see the cumulative work-to-date. (Ralph auto-commits each successful step on the plan branch — that's how rollback and per-step diff isolation work, so don't try to disable it.)

## Plan size

Pick granularity based on the work, not a target count. Rough bands:

- bugfix: 3–5 steps
- small feature: 5–15
- medium feature: 15–40
- large refactor or greenfield: 40–300

Don't compress a big task into a handful of mega-steps — you lose the per-step checkpointing, retry, and rollback. Don't inflate a small task into trivial steps either. Whatever the size, every step must be atomic and independently verifiable.

## Anti-patterns

- ❌ Referencing "the previous step" by name in a prompt — refer to files or commits.
- ❌ Long inline `--description "..."` with mixed quoting — silent truncation. Use `--import-json` or a quoted heredoc → tempfile.
- ❌ Skipping `--change-policy optional` on review/audit steps — they'll be marked failed for producing no diff.
- ❌ Using `codex` (default sandbox) for a review step that appends fix steps — writes to the ralph DB silently fail. Use `codex-orchestrator`.
- ❌ Skipping the verify step because "the review step will catch it" — reviewers don't run the code.
- ❌ Trying to make the harness commit instead of letting ralph commit — ralph commits each successful step by design; harness-side commits will conflict and produce a clean diff at step end (which `change_policy=required` will correctly fail).

## Reference: useful CLI flags

- `ralph step add --import-json <FILE|->` — bulk insert steps from JSON array.
- `ralph step add ... --change-policy {required|optional|forbidden}` — `required` (default) fails on empty diff; `optional` allows it (use for review); `forbidden` fails on any diff (use for read-only audit).
- `ralph step add ... --max-retries <n>` — per-step retry override.
- `ralph step add ... --harness <name>` — per-step harness override.
- `ralph harness list` / `ralph harness show <name>` — verify configured harnesses, sandbox modes, and known foot-guns.
- `ralph plan harness set <harness> [<slug>]` — pick the plan-generation harness.
- `ralph step move <num> --to <n>` — reorder steps after creation.
