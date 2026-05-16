//! End-to-end test for the cross-process skip bridge (audit Fix 1).
//!
//! The BLOCKER this guards: in production the runner is a *separate process*
//! from both the TUI and `ralph skip`, so the process-global cancel registry
//! in `signal.rs` can never reach it. A `ralph skip` against a RUNNING step
//! must instead hand off through `plans.skip_requested_step_id` (V23), which
//! the runner polls mid-attempt and funnels into the same executor skip
//! handling.
//!
//! This test drives the real binary across a real process boundary:
//!
//!   1. spawn `ralph run --json <slug>` as a child (its own process);
//!   2. wait for its harness child to actually start (a marker file);
//!   3. from a *separate* `ralph skip --changes discard <slug>` process,
//!      request the skip (this writes the DB bridge row — the run lock must
//!      NOT block this);
//!   4. assert the harness is killed, the step ends `skipped` with exactly
//!      one `execution_logs` row (attempts == 1), and the run advances to and
//!      completes the next step (no whole-run abort).

#![cfg(unix)]

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn ralph_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ralph")
}

fn env_home(proj: &Path) -> std::path::PathBuf {
    proj.parent().unwrap().join("home")
}
fn env_xdg_config(proj: &Path) -> std::path::PathBuf {
    env_home(proj).join(".config")
}
fn env_xdg_data(proj: &Path) -> std::path::PathBuf {
    env_home(proj).join(".local").join("share")
}

fn run_check(cmd: &mut Command, label: &str) {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("{label}: spawn: {e}"));
    assert!(
        out.status.success(),
        "{label} failed: status={:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn ralph(proj: &Path) -> Command {
    let mut c = Command::new(ralph_bin());
    c.args(["-C"])
        .arg(proj)
        .env("HOME", env_home(proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(proj))
        .env("XDG_DATA_HOME", env_xdg_data(proj));
    c
}

/// Stand up a git-backed ralph project whose plan has two steps:
///  - step 1 uses a "blocking" harness that dirties the tree then sleeps long
///    (the one we will skip mid-flight);
///  - step 2 uses a "quick" harness that makes one change and exits 0 (so the
///    runner advancing past the skip has something to complete).
///
/// Returns `(tempdir, project_path, marker_path)` where `marker_path` is
/// touched by the blocking harness once it has started + dirtied the tree.
fn setup() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let home = dir.path().join("home");
    let proj = dir.path().join("proj");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&proj).unwrap();

    run_check(
        Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(&proj),
        "git init",
    );
    run_check(
        Command::new("git")
            .args(["config", "user.email", "ralph-test@example.com"])
            .current_dir(&proj),
        "git config email",
    );
    run_check(
        Command::new("git")
            .args(["config", "user.name", "ralph-test"])
            .current_dir(&proj),
        "git config name",
    );
    std::fs::write(proj.join("README"), "test").unwrap();
    run_check(
        Command::new("git").args(["add", "."]).current_dir(&proj),
        "git add",
    );
    run_check(
        Command::new("git")
            .args(["commit", "-q", "-m", "init"])
            .current_dir(&proj),
        "git commit",
    );

    // The blocking harness: dirty the tree (a tracked-file edit), drop a
    // marker so the test knows the harness is genuinely running, then sleep
    // far longer than the test. Invoked via /bin/sh to dodge the ETXTBSY
    // footgun documented in CLAUDE.md.
    let marker = dir.path().join("harness-started.marker");
    let block_sh = dir.path().join("block-harness.sh");
    std::fs::write(
        &block_sh,
        format!(
            "#!/bin/sh\n\
             cat >/dev/null 2>&1 || true\n\
             echo 'blocked harness edit' >> '{readme}'\n\
             : > '{marker}'\n\
             sleep 120\n",
            readme = proj.join("README").display(),
            marker = marker.display(),
        ),
    )
    .unwrap();

    // The quick harness: produce one change and exit 0. With no
    // deterministic tests configured this is treated as a passing step.
    let quick_sh = dir.path().join("quick-harness.sh");
    std::fs::write(
        &quick_sh,
        format!(
            "#!/bin/sh\n\
             cat >/dev/null 2>&1 || true\n\
             echo 'quick step output' >> '{f}'\n\
             exit 0\n",
            f = proj.join("step2.txt").display(),
        ),
    )
    .unwrap();

    run_check(
        ralph(&proj).args(["init", "--non-interactive", "--default-harness", "claude"]),
        "ralph init",
    );

    // Patch config.json: register the two script harnesses. `sh` is the
    // command; the script path is its first arg. Everything else defaults.
    let cfg_path = env_xdg_config(&proj).join("ralph-rs").join("config.json");
    let raw = std::fs::read_to_string(&cfg_path).expect("read config.json");
    let mut cfg: serde_json::Value = serde_json::from_str(&raw).expect("parse config.json");
    let harnesses = cfg
        .get_mut("harnesses")
        .and_then(|h| h.as_object_mut())
        .expect("config.harnesses object");
    for (name, script) in [
        ("blocker", block_sh.display().to_string()),
        ("quick", quick_sh.display().to_string()),
    ] {
        harnesses.insert(
            name.to_string(),
            serde_json::json!({
                "command": "/bin/sh",
                "args": [script],
            }),
        );
    }
    std::fs::write(&cfg_path, serde_json::to_string_pretty(&cfg).unwrap()).unwrap();

    run_check(
        ralph(&proj).args(["plan", "create", "skip-plan", "-d", "exercise the skip bridge"]),
        "plan create",
    );
    run_check(
        ralph(&proj).args([
            "step", "add", "Blocking step", "skip-plan", "-d", "sleeps", "--harness", "blocker",
        ]),
        "step add 1",
    );
    run_check(
        ralph(&proj).args([
            "step", "add", "Quick step", "skip-plan", "-d", "fast", "--harness", "quick",
        ]),
        "step add 2",
    );
    run_check(
        ralph(&proj).args(["plan", "approve", "skip-plan"]),
        "plan approve",
    );

    (dir, proj, marker)
}

#[test]
fn cross_process_skip_kills_harness_advances_run() {
    let (dir, proj, marker) = setup();

    // Spawn the runner in its OWN process. `--json` forces the
    // non-interactive path and streams NDJSON on stdout.
    let mut runner = ralph(&proj)
        .args(["run", "skip-plan", "--json", "--skip-preflight"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ralph run");

    // Wait until the blocking harness has actually started (marker present).
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        if Instant::now() > deadline {
            let _ = runner.kill();
            let _ = runner.wait();
            panic!("blocking harness never started within 30s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // From a SEPARATE process, request the skip. This must succeed even
    // though the runner holds the per-project run lock — the skip-request
    // write is deliberately not lock-gated.
    let skip_out = ralph(&proj)
        .args(["skip", "skip-plan", "--changes", "discard"])
        .output()
        .expect("spawn ralph skip");
    assert!(
        skip_out.status.success(),
        "`ralph skip` must succeed while a run holds the lock.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&skip_out.stdout),
        String::from_utf8_lossy(&skip_out.stderr),
    );

    // The runner must exit on its own (the skip killed the harness, the step
    // was skipped, the run advanced through step 2 and completed). If the
    // bridge were broken the runner would block on `sleep 120` forever.
    let runner_deadline = Instant::now() + Duration::from_secs(60);
    let stdout_buf: String;
    {
        let mut child_stdout = runner.stdout.take().expect("runner stdout piped");
        // Read in a background thread so a wedged runner can't deadlock the
        // test on a full pipe; bounded by the wait loop below.
        let handle = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = child_stdout.read_to_string(&mut s);
            s
        });
        loop {
            match runner.try_wait().expect("try_wait") {
                Some(status) => {
                    stdout_buf = handle.join().unwrap_or_default();
                    assert!(
                        status.success(),
                        "runner must exit 0 after a single-step skip + advance \
                         (skipped step must NOT abort the whole run). status={status:?}\n\
                         stdout:\n{stdout_buf}",
                    );
                    break;
                }
                None => {
                    if Instant::now() > runner_deadline {
                        let _ = runner.kill();
                        let _ = runner.wait();
                        panic!(
                            "runner did not exit within 60s of the cross-process skip — \
                             the skip bridge did not interrupt the in-flight harness"
                        );
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    // Parse the NDJSON stream. We expect:
    //  - step 1 finishes with outcome "skipped" and attempts == 1
    //    (exactly one execution_logs row);
    //  - step 2 finishes with outcome "success" (the run advanced);
    //  - plan_complete with final_status not "aborted".
    let mut step1_skipped = false;
    let mut step1_attempts = -1;
    let mut step2_success = false;
    let mut plan_complete_status: Option<String> = None;
    for line in stdout_buf.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(ev) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match ev.get("event").and_then(|e| e.as_str()) {
            Some("step_finished") => {
                let num = ev.get("step_num").and_then(|n| n.as_u64()).unwrap_or(0);
                let outcome = ev
                    .get("outcome")
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_string();
                if num == 1 {
                    step1_skipped = outcome == "skipped";
                    step1_attempts =
                        ev.get("attempts").and_then(|a| a.as_i64()).unwrap_or(-1) as i32;
                } else if num == 2 {
                    step2_success = outcome == "success";
                }
            }
            Some("plan_complete") => {
                plan_complete_status = ev
                    .get("final_status")
                    .and_then(|s| s.as_str())
                    .map(String::from);
            }
            _ => {}
        }
    }

    assert!(
        step1_skipped,
        "step 1 must finish `skipped` (not aborted/failed).\nNDJSON:\n{stdout_buf}"
    );
    assert_eq!(
        step1_attempts, 1,
        "the skipped step must have exactly one attempt → exactly one \
         execution_logs row.\nNDJSON:\n{stdout_buf}"
    );
    assert!(
        step2_success,
        "the run must ADVANCE past the skipped step and complete step 2 \
         (a skip drops one step; it must not tear the whole run down).\n\
         NDJSON:\n{stdout_buf}"
    );
    assert_ne!(
        plan_complete_status.as_deref(),
        Some("aborted"),
        "plan must not end `aborted` after a single-step skip.\nNDJSON:\n{stdout_buf}"
    );

    // Hard cross-check straight against the DB: exactly one execution_logs
    // row for step 1, and its step status is `skipped`.
    let db_path = env_xdg_data(&proj).join("ralph-rs").join("ralph.db");
    let conn = rusqlite::Connection::open(&db_path).expect("open ralph.db");
    let (step1_id, step1_status): (String, String) = conn
        .query_row(
            "SELECT s.id, s.status FROM steps s \
             JOIN plans p ON p.id = s.plan_id \
             WHERE p.slug = 'skip-plan' AND s.title = 'Blocking step'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("query step 1");
    assert_eq!(
        step1_status, "skipped",
        "step 1 DB status must be `skipped`"
    );
    let log_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM execution_logs WHERE step_id = ?1",
            [&step1_id],
            |r| r.get(0),
        )
        .expect("count execution_logs");
    assert_eq!(
        log_count, 1,
        "the skipped step must have exactly one execution_logs row (no \
         duplicate from a finalize_failure + finalize_skipped double-write)"
    );

    drop(dir);
}
