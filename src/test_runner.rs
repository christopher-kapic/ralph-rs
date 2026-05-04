// Test validation runner
//
// Executes deterministic test commands (shell commands) and collects structured results.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::process::Command;
use tokio::sync::watch;

use crate::io_util;
use crate::output::ChunkStream;

/// Per-stream cap for concurrent test-command pipe drainers. Tests are usually
/// chattier than harness invocations but shorter-lived, so 1 MiB per stream is
/// sufficient to keep structured failure output without letting a runaway
/// test balloon memory. Mirrors the deadlock fix in `executor.rs`:
/// draining *concurrently* with `child.wait()` keeps the pipe flowing past
/// the ~64 KiB kernel buffer.
const TEST_OUTPUT_TAIL_BYTES: usize = 1024 * 1024;

/// Result of a single test command execution.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TestResult {
    /// The shell command that was executed.
    pub command: String,
    /// Process exit code (`None` if terminated by signal).
    pub exit_code: Option<i32>,
    /// Last ~50 lines of combined stdout+stderr.
    pub output_tail: String,
    /// Whether the command succeeded (exit code 0).
    pub passed: bool,
}

/// Sink invoked once per emitted line of deterministic-test output. Receives
/// the index of the test in `plan.deterministic_tests`, the stream label, the
/// (possibly truncated) line text, and the monotonic `seq` allocated for this
/// emit.
pub type TestChunkSink = Arc<dyn Fn(usize, ChunkStream, String, u64) + Send + Sync + 'static>;

/// Per-run configuration for streaming `RunEvent::TestChunk` events from each
/// deterministic-test command's stdout/stderr.
///
/// `seq` is shared across **all** tests and both streams so the consumer sees
/// a strictly monotonic counter for the test phase. The runner threads the
/// same `Arc<AtomicU64>` it uses for `HarnessChunk` events through here, so a
/// single counter is monotonic across the whole run; per the acceptance
/// criteria, that means it is also monotonic across (and within) the test
/// phase. `max_bytes` mirrors `Config.harness_chunk_max_bytes`.
pub struct TestChunkConfig {
    pub seq: Arc<AtomicU64>,
    pub max_bytes: usize,
    pub sink: TestChunkSink,
}

/// Aggregated results from running a suite of test commands.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TestResults {
    /// Individual results for each executed test (may be fewer than the input
    /// list if short-circuiting occurred).
    pub results: Vec<TestResult>,
    /// `true` only when every test command passed.
    pub all_passed: bool,
    /// Index into `results` of the first failing test, if any.
    pub first_failure_index: Option<usize>,
    /// True if the run was cut short by an abort signal.
    pub aborted: bool,
}

/// Maximum number of output lines to retain per test command.
const TAIL_LINES: usize = 50;

/// Execute each test command sequentially via `sh -c`, short-circuiting on the
/// first failure or on an abort signal.
///
/// # Arguments
/// * `tests` – slice of shell command strings to run.
/// * `cwd` – working directory for each command.
/// * `abort_rx` – watch channel; when it flips to `true`, any running test is
///   killed and the run is reported as aborted.
/// * `chunk_cfg` – when `Some`, each test command's stdout/stderr is also
///   streamed line-by-line through the sink (one emit per newline plus a
///   final flush of any unterminated trailing line). When `None`, no chunk
///   events are emitted — only the captured output tail is returned. Per
///   TUI-plan §13.1.
///
/// # Returns
/// A [`TestResults`] summarising the run.
pub async fn run_tests(
    tests: &[String],
    cwd: &Path,
    abort_rx: watch::Receiver<bool>,
    chunk_cfg: Option<TestChunkConfig>,
) -> TestResults {
    let mut results: Vec<TestResult> = Vec::with_capacity(tests.len());
    let mut all_passed = true;
    let mut first_failure_index: Option<usize> = None;
    let mut aborted = false;

    for (i, cmd) in tests.iter().enumerate() {
        if *abort_rx.borrow() {
            aborted = true;
            all_passed = false;
            break;
        }

        let emitters = chunk_cfg
            .as_ref()
            .map(|cfg| build_test_emitters(cfg, i))
            .unwrap_or((None, None));
        let (result, was_aborted) = run_single_test(cmd, cwd, abort_rx.clone(), emitters).await;
        let passed = result.passed;
        results.push(result);

        if was_aborted {
            aborted = true;
            all_passed = false;
            first_failure_index = Some(i);
            break;
        }

        if !passed {
            all_passed = false;
            first_failure_index = Some(i);
            break;
        }
    }

    TestResults {
        results,
        all_passed,
        first_failure_index,
        aborted,
    }
}

/// Build the per-stream `ChunkEmitter` pair for a single test command.
///
/// Both emitters share `cfg.seq`, so seq is monotonic across stdout *and*
/// stderr of the same test, and across all tests in the phase. The sink is
/// curried with `test_index` so the inner [`io_util::ChunkSink`] only needs
/// the per-line `(stream, text, seq)` tuple.
fn build_test_emitters(
    cfg: &TestChunkConfig,
    test_index: usize,
) -> (Option<io_util::ChunkEmitter>, Option<io_util::ChunkEmitter>) {
    let sink_for_stdout = cfg.sink.clone();
    let sink_for_stderr = cfg.sink.clone();
    let stdout_inner: io_util::ChunkSink = Arc::new(move |stream, text, seq| {
        sink_for_stdout(test_index, stream, text, seq);
    });
    let stderr_inner: io_util::ChunkSink = Arc::new(move |stream, text, seq| {
        sink_for_stderr(test_index, stream, text, seq);
    });
    (
        Some(io_util::ChunkEmitter {
            stream: ChunkStream::Stdout,
            seq: cfg.seq.clone(),
            max_bytes: cfg.max_bytes,
            sink: stdout_inner,
        }),
        Some(io_util::ChunkEmitter {
            stream: ChunkStream::Stderr,
            seq: cfg.seq.clone(),
            max_bytes: cfg.max_bytes,
            sink: stderr_inner,
        }),
    )
}

/// Run a single shell command and capture its output, racing against an abort signal.
async fn run_single_test(
    cmd: &str,
    cwd: &Path,
    mut abort_rx: watch::Receiver<bool>,
    emitters: (Option<io_util::ChunkEmitter>, Option<io_util::ChunkEmitter>),
) -> (TestResult, bool) {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(cmd)
        .current_dir(cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // Put the child into its own process group so SIGKILL on abort fans out
    // to grandchildren (e.g. `cargo test` workers, `pnpm -> turbo -> next`).
    // Same rationale as `harness::spawn_harness`.
    #[cfg(unix)]
    {
        command.process_group(0);
    }

    let spawn_result = command.spawn();

    let mut child = match spawn_result {
        Ok(c) => c,
        Err(e) => {
            return (
                TestResult {
                    command: cmd.to_string(),
                    exit_code: None,
                    output_tail: format!("failed to execute command: {e}"),
                    passed: false,
                },
                false,
            );
        }
    };

    // Spawn concurrent drain tasks for stdout and stderr *before* waiting
    // on the child. A test that emits more than the kernel pipe buffer
    // (~64 KiB) would otherwise block on write(2) while we block on wait(),
    // deadlocking. See `io_util::drain_bounded` for the full rationale.
    //
    // When `emitters.0` / `emitters.1` are populated, each drainer also
    // emits one `RunEvent::TestChunk` per newline via the curried sink.
    let (stdout_emitter, stderr_emitter) = emitters;
    let stdout_task = io_util::drain_bounded_with_emitter(
        child.stdout.take(),
        TEST_OUTPUT_TAIL_BYTES,
        stdout_emitter,
    );
    let stderr_task = io_util::drain_bounded_with_emitter(
        child.stderr.take(),
        TEST_OUTPUT_TAIL_BYTES,
        stderr_emitter,
    );

    tokio::select! {
        status = child.wait() => {
            match status {
                Ok(exit_status) => {
                    let exit_code = exit_status.code();
                    let passed = exit_status.success();
                    // Child has exited; pipes will EOF and the drain tasks
                    // will finish on their own.
                    let stdout = io_util::join_drain_string(stdout_task).await;
                    let stderr = io_util::join_drain_string(stderr_task).await;
                    let combined = combine(&stdout, &stderr);
                    let output_tail = tail_lines(&combined, TAIL_LINES);
                    (
                        TestResult {
                            command: cmd.to_string(),
                            exit_code,
                            output_tail,
                            passed,
                        },
                        false,
                    )
                }
                Err(e) => {
                    // Reap the drain tasks even on wait() error so their
                    // handles don't linger.
                    let _ = io_util::join_drain(stdout_task).await;
                    let _ = io_util::join_drain(stderr_task).await;
                    (
                        TestResult {
                            command: cmd.to_string(),
                            exit_code: None,
                            output_tail: format!("failed to execute command: {e}"),
                            passed: false,
                        },
                        false,
                    )
                }
            }
        }
        _ = wait_for_abort(&mut abort_rx) => {
            // SIGKILL the entire process group so grandchildren (test-runner
            // workers, etc.) die with the shell. Without this, orphans would
            // keep holding the stdout/stderr pipes open past the abort, and
            // the drain tasks below would block on `read` until those
            // orphans exited on their own. See `executor::signal_process_group`
            // for the `-pid` convention.
            #[cfg(unix)]
            {
                if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                    crate::executor::signal_process_group(pid, libc::SIGKILL);
                }
            }
            #[cfg(not(unix))]
            {
                let _ = child.kill().await;
            }
            let _ = child.wait().await;
            // With the whole pgroup killed, the pipes EOF promptly, so we
            // can await the drain tasks normally. The output isn't used on
            // the abort path (it's hardcoded to "aborted by signal"), but
            // awaiting keeps the task handles tidy.
            let _ = io_util::join_drain(stdout_task).await;
            let _ = io_util::join_drain(stderr_task).await;
            (
                TestResult {
                    command: cmd.to_string(),
                    exit_code: None,
                    output_tail: "aborted by signal".to_string(),
                    passed: false,
                },
                true,
            )
        }
    }
}

/// Block until the abort watch channel signals `true`.
async fn wait_for_abort(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    loop {
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
            return;
        }
        if *rx.borrow() {
            return;
        }
    }
}

/// Combine stdout and stderr into one buffer, separated by a newline if needed.
fn combine(stdout: &str, stderr: &str) -> String {
    let mut buf = stdout.to_string();
    if !stderr.is_empty() {
        if !buf.is_empty() && !buf.ends_with('\n') {
            buf.push('\n');
        }
        buf.push_str(stderr);
    }
    buf
}

/// Return the last `n` lines of `text` as a single string, preserving the
/// input's trailing newline (present or absent) in both the fits-within-limit
/// and truncated paths.
fn tail_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= n {
        return text.to_string();
    }
    let mut result = lines[lines.len() - n..].join("\n");
    if text.ends_with('\n') {
        result.push('\n');
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn cwd() -> PathBuf {
        std::env::current_dir().expect("current dir")
    }

    fn never_abort() -> watch::Receiver<bool> {
        // Leak the sender: `wait_for_abort` already handles a closed channel
        // by pending forever, but a live sender keeps semantics identical to
        // the real abort channel the runner passes in.
        let (tx, rx) = watch::channel(false);
        Box::leak(Box::new(tx));
        rx
    }

    #[tokio::test]
    async fn test_all_pass() {
        let tests = vec!["true".to_string(), "echo hello".to_string()];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(res.all_passed);
        assert!(!res.aborted);
        assert_eq!(res.results.len(), 2);
        assert!(res.first_failure_index.is_none());
        for r in &res.results {
            assert!(r.passed);
            assert_eq!(r.exit_code, Some(0));
        }
    }

    #[tokio::test]
    async fn test_first_failure_short_circuits() {
        let tests = vec![
            "true".to_string(),
            "false".to_string(),
            "echo should_not_run".to_string(),
        ];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(!res.all_passed);
        assert_eq!(res.results.len(), 2);
        assert_eq!(res.first_failure_index, Some(1));
        assert!(res.results[0].passed);
        assert!(!res.results[1].passed);
    }

    #[tokio::test]
    async fn test_captures_output() {
        let tests = vec!["echo hello_world".to_string()];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(res.all_passed);
        assert!(res.results[0].output_tail.contains("hello_world"));
    }

    #[tokio::test]
    async fn test_captures_stderr() {
        let tests = vec!["echo err_output >&2".to_string()];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(res.all_passed);
        assert!(res.results[0].output_tail.contains("err_output"));
    }

    #[tokio::test]
    async fn test_exit_code_nonzero() {
        let tests = vec!["exit 42".to_string()];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(!res.all_passed);
        assert_eq!(res.results[0].exit_code, Some(42));
        assert!(!res.results[0].passed);
    }

    #[test]
    fn test_tail_lines_truncation() {
        let many_lines: String = (0..100)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let tail = tail_lines(&many_lines, 50);
        let lines: Vec<&str> = tail.lines().collect();
        assert_eq!(lines.len(), 50);
        assert!(lines[0].contains("line50"));
        assert!(lines[49].contains("line99"));
    }

    #[test]
    fn test_tail_lines_preserves_trailing_newline() {
        // Both paths — fits-within-limit and truncated — should match the
        // input's trailing-newline state.
        assert_eq!(tail_lines("a\nb\nc\n", 10), "a\nb\nc\n");
        assert_eq!(tail_lines("a\nb\nc", 10), "a\nb\nc");
        assert_eq!(tail_lines("a\nb\nc\n", 2), "b\nc\n");
        assert_eq!(tail_lines("a\nb\nc", 2), "b\nc");
        assert_eq!(tail_lines("", 5), "");
    }

    #[tokio::test]
    async fn test_empty_tests() {
        let tests: Vec<String> = vec![];
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(res.all_passed);
        assert!(res.results.is_empty());
        assert!(res.first_failure_index.is_none());
        assert!(!res.aborted);
    }

    #[tokio::test]
    async fn test_respects_cwd() {
        let tests = vec!["pwd".to_string()];
        let dir = std::env::temp_dir();
        let res = run_tests(&tests, &dir, never_abort(), None).await;
        assert!(res.all_passed);
        let canonical = dir.canonicalize().unwrap();
        let output_canonical = PathBuf::from(res.results[0].output_tail.trim())
            .canonicalize()
            .unwrap_or_default();
        assert_eq!(output_canonical, canonical);
    }

    #[tokio::test]
    async fn test_abort_signal_interrupts_running_test() {
        // A long-running test that would take 30 seconds to finish normally.
        let tests = vec!["sleep 30".to_string()];
        let (tx, rx) = watch::channel(false);

        // Fire the abort after a short delay, while the test is still running.
        let abort_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(true);
        });

        let start = std::time::Instant::now();
        let res = run_tests(&tests, &cwd(), rx, None).await;
        let elapsed = start.elapsed();

        abort_handle.await.ok();

        assert!(res.aborted, "run should be marked aborted");
        assert!(!res.all_passed);
        assert_eq!(res.results.len(), 1);
        assert!(!res.results[0].passed);
        // The sleep should have been killed well before its 30-second end.
        assert!(
            elapsed < Duration::from_secs(5),
            "abort took too long: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_abort_before_start() {
        // Abort already set before run_tests is called: no test should run.
        let tests = vec!["echo should_not_run".to_string()];
        let (tx, rx) = watch::channel(false);
        let _ = tx.send(true);

        let res = run_tests(&tests, &cwd(), rx, None).await;
        assert!(res.aborted);
        assert!(!res.all_passed);
        assert!(res.results.is_empty());
    }

    /// Regression: a test command that writes more than the kernel pipe
    /// buffer (~64 KiB) would deadlock before the concurrent-drain fix,
    /// because the child would block on write(2) while the parent blocks
    /// on wait(). 500 KB is well above the pipe buffer but well below the
    /// 1 MiB tail cap — the test should complete promptly with the final
    /// tail lines visible.
    #[tokio::test]
    async fn test_large_output_does_not_deadlock() {
        // `yes` emits "y\n" pairs; head -c cuts at 500000 bytes. The child
        // will try to write all 500 KB, which dwarfs the pipe buffer.
        let tests = vec!["yes | head -c 500000".to_string()];

        let start = std::time::Instant::now();
        let res = tokio::time::timeout(
            Duration::from_secs(30),
            run_tests(&tests, &cwd(), never_abort(), None),
        )
        .await
        .expect("test_runner should not deadlock on large output");
        let elapsed = start.elapsed();

        assert!(res.all_passed, "500 KB of stdout should pass: {res:?}");
        assert_eq!(res.results.len(), 1);
        assert!(res.results[0].passed);
        // Tail lines logic keeps the last N lines; they should contain
        // the expected 'y' chars.
        assert!(
            res.results[0].output_tail.contains('y'),
            "output tail should contain 'y' content"
        );
        // Sanity: 500 KB piped to head should finish in well under the
        // 30s test timeout.
        assert!(
            elapsed < Duration::from_secs(10),
            "500 KB output took too long: {elapsed:?}"
        );
    }

    // -------------------------------------------------------------------
    // TestChunk emission (TUI-plan §13.1)
    // -------------------------------------------------------------------

    /// Capturing sink: pushes `(test_index, stream, text, seq)` tuples into
    /// a shared `Mutex<Vec<_>>` so tests can inspect emission without going
    /// through `emit_ndjson` (which would write to stdout).
    fn collecting_chunk_cfg(
        max_bytes: usize,
    ) -> (
        TestChunkConfig,
        Arc<std::sync::Mutex<Vec<(usize, ChunkStream, String, u64)>>>,
    ) {
        let collected: Arc<std::sync::Mutex<Vec<(usize, ChunkStream, String, u64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_for_sink = collected.clone();
        let sink: TestChunkSink = Arc::new(move |idx, stream, text, seq| {
            collected_for_sink
                .lock()
                .unwrap()
                .push((idx, stream, text, seq));
        });
        let cfg = TestChunkConfig {
            seq: Arc::new(AtomicU64::new(0)),
            max_bytes,
            sink,
        };
        (cfg, collected)
    }

    /// Acceptance criterion: running a plan with two tests produces
    /// `TestChunk` events tagged with `test_index` 0 and 1 respectively,
    /// `seq` is monotonic across the whole phase, and `stream` carries the
    /// originating pipe.
    #[tokio::test]
    async fn test_chunk_events_tagged_with_correct_test_index() {
        let tests = vec![
            "echo first-line; echo first-err >&2".to_string(),
            "echo second-line; echo second-err >&2".to_string(),
        ];
        let (cfg, collected) = collecting_chunk_cfg(4096);
        let res = run_tests(&tests, &cwd(), never_abort(), Some(cfg)).await;
        assert!(res.all_passed, "tests should pass: {res:?}");

        let events = collected.lock().unwrap().clone();
        // Two tests, each emitting one stdout + one stderr line → 4 events.
        assert_eq!(events.len(), 4, "expected 4 events, got {events:?}");

        // Group by test_index. The first test must have test_index 0; the
        // second must have test_index 1. Both indices must be present.
        let test0: Vec<_> = events.iter().filter(|e| e.0 == 0).cloned().collect();
        let test1: Vec<_> = events.iter().filter(|e| e.0 == 1).cloned().collect();
        assert_eq!(test0.len(), 2, "expected 2 events for test 0: {test0:?}");
        assert_eq!(test1.len(), 2, "expected 2 events for test 1: {test1:?}");

        // The first test's events must reference its own command's lines,
        // not the second test's. (Tests run sequentially, so by the time
        // test 1 starts, test 0's emitter is already dropped.)
        let test0_texts: Vec<&str> = test0.iter().map(|e| e.2.as_str()).collect();
        let test1_texts: Vec<&str> = test1.iter().map(|e| e.2.as_str()).collect();
        assert!(
            test0_texts.contains(&"first-line"),
            "test 0 should have 'first-line': {test0_texts:?}"
        );
        assert!(
            test0_texts.contains(&"first-err"),
            "test 0 should have 'first-err': {test0_texts:?}"
        );
        assert!(
            test1_texts.contains(&"second-line"),
            "test 1 should have 'second-line': {test1_texts:?}"
        );
        assert!(
            test1_texts.contains(&"second-err"),
            "test 1 should have 'second-err': {test1_texts:?}"
        );

        // Stream labels: exactly one stdout + one stderr per test.
        for (label, group) in [("test 0", &test0), ("test 1", &test1)] {
            let stdout_count = group
                .iter()
                .filter(|e| matches!(e.1, ChunkStream::Stdout))
                .count();
            let stderr_count = group
                .iter()
                .filter(|e| matches!(e.1, ChunkStream::Stderr))
                .count();
            assert_eq!(stdout_count, 1, "{label} stdout count: {group:?}");
            assert_eq!(stderr_count, 1, "{label} stderr count: {group:?}");
        }

        // seq is monotonic and gap-free across the entire test phase. Tests
        // run sequentially, so test 0's seqs must all be lower than test 1's.
        let mut all_seqs: Vec<u64> = events.iter().map(|e| e.3).collect();
        all_seqs.sort();
        assert_eq!(all_seqs, vec![0, 1, 2, 3], "seq must be 0..3");
        let test0_max = test0.iter().map(|e| e.3).max().unwrap();
        let test1_min = test1.iter().map(|e| e.3).min().unwrap();
        assert!(
            test0_max < test1_min,
            "all of test 0's seqs must precede all of test 1's: \
             test0={test0:?} test1={test1:?}"
        );
    }

    /// `chunk_cfg = None` is the back-compat path: no events are emitted
    /// and the captured tail still works. Verifies the chunk-emit path is
    /// strictly additive.
    #[tokio::test]
    async fn test_chunk_no_emission_when_cfg_is_none() {
        let tests = vec!["echo no-stream".to_string()];
        // We can't easily prove "no events" via the public sink API since
        // there is no sink to observe; we instead verify that the captured
        // tail behavior is preserved exactly as it was before.
        let res = run_tests(&tests, &cwd(), never_abort(), None).await;
        assert!(res.all_passed);
        assert!(res.results[0].output_tail.contains("no-stream"));
    }
}
