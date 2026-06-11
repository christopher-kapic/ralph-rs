// Bounded-tail concurrent pipe drainer.
//
// Motivation: a child process writing more than the kernel's pipe buffer
// (~64 KiB on Linux) blocks on `write(2)` until the parent drains. If the
// parent is in `child.wait()`, that's a deadlock. The fix is to spawn a
// reader task *immediately* after taking each pipe so draining runs
// concurrently with the wait. When the child exits, its pipes EOF, the
// reader loop sees `read` return 0, and the task finishes.
//
// We keep the *tail* (last N bytes) rather than the whole stream because a
// runaway child could otherwise balloon memory without bound. Structured
// harness output (`session_id`, `cost_usd`) typically lives at the end, so
// tail-preservation is the right default.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

use crate::output::ChunkStream;

/// Marker appended to a captured buffer when truncation occurred. Exposed so
/// callers and tests can detect / match on it.
pub const TRUNCATION_MARKER_PREFIX: &str = "\n[output truncated to last ";

/// Build the full truncation marker for a given cap in bytes. Uses MiB when
/// the cap is an exact multiple, otherwise falls back to bytes.
fn truncation_marker(cap: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if cap >= MIB && cap.is_multiple_of(MIB) {
        format!("{TRUNCATION_MARKER_PREFIX}{} MiB]\n", cap / MIB)
    } else {
        format!("{TRUNCATION_MARKER_PREFIX}{cap} bytes]\n")
    }
}

/// Callback invoked once per line of output observed by a chunk-emitting
/// drainer. Takes the stream label, the (possibly truncated) line text,
/// and the monotonic `seq` allocated for this emit.
pub type ChunkSink = Arc<dyn Fn(ChunkStream, String, u64) + Send + Sync + 'static>;

/// Per-stream state for the chunk-emission side of a drainer.
///
/// `seq` is a counter shared across both stdout and stderr drainers (and
/// across all step invocations in a run), so a consumer can reorder by
/// `seq` if interleaving stderr/stdout matters. `max_bytes` mirrors
/// `Config.harness_chunk_max_bytes` and caps the byte length of each
/// emitted line's `text` payload.
pub struct ChunkEmitter {
    pub stream: ChunkStream,
    pub seq: Arc<AtomicU64>,
    pub max_bytes: usize,
    pub sink: ChunkSink,
}

/// Spawn a task that continuously drains `reader` into a `Vec<u8>` bounded at
/// `cap` bytes (preserving the *tail* — the last `cap` bytes). Returns a
/// `JoinHandle` whose value is the captured bytes, with a synthetic
/// truncation-marker line appended iff any bytes were dropped.
///
/// The task takes ownership of `reader`. If a read errors mid-stream, it
/// returns whatever was accumulated so far rather than erroring out — the
/// parent still needs diagnostic output. On EOF it returns normally.
///
/// Production callers use [`drain_bounded_with_emitter`] (with `None` to
/// disable emission, or a configured emitter to also stream chunk events);
/// this no-emitter convenience wrapper is preserved for any future caller
/// that doesn't need streaming.
pub fn drain_bounded<R>(reader: Option<R>, cap: usize) -> JoinHandle<Vec<u8>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    drain_bounded_with_emitter(reader, cap, None)
}

/// Like [`drain_bounded`] but also emits one chunk event per newline (and a
/// final event for any non-empty trailing buffer at EOF). Each emitted
/// `text` is truncated to at most `emitter.max_bytes` bytes — the truncation
/// is performed at a UTF-8 boundary so the resulting `String` is always
/// well-formed and never longer than `max_bytes`.
///
/// When `emitter` is `None`, this is identical to [`drain_bounded`].
pub fn drain_bounded_with_emitter<R>(
    reader: Option<R>,
    cap: usize,
    emitter: Option<ChunkEmitter>,
) -> JoinHandle<Vec<u8>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let Some(mut r) = reader else {
            return Vec::new();
        };

        // 8 KiB read chunks is a good balance: big enough that the syscall
        // overhead is amortised, small enough that we notice EOF promptly
        // when the child exits.
        let mut chunk = [0u8; 8 * 1024];
        let mut buf: Vec<u8> = Vec::new();
        let mut truncated = false;
        let mut line_buf: Vec<u8> = Vec::new();

        loop {
            match r.read(&mut chunk).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let new_bytes = &chunk[..n];

                    buf.extend_from_slice(new_bytes);
                    if buf.len() > cap {
                        // Keep the last `cap` bytes only.
                        let excess = buf.len() - cap;
                        buf.drain(..excess);
                        truncated = true;
                    }

                    // Emit one chunk per newline. A line larger than the
                    // emitter's cap is still emitted (truncated). A line
                    // is allowed to span multiple `read` calls — the
                    // unterminated tail accumulates in `line_buf`.
                    if let Some(ref em) = emitter {
                        for &b in new_bytes {
                            if b == b'\n' {
                                emit_line(em, &line_buf);
                                line_buf.clear();
                            } else if line_buf.len() < em.max_bytes {
                                line_buf.push(b);
                            }
                        }
                    }
                }
                Err(_) => {
                    // Mid-stream read failure — return whatever we have so
                    // callers still get partial diagnostics.
                    break;
                }
            }
        }

        // Flush any unterminated trailing line at EOF. Without this, a
        // harness whose final line lacks a `\n` would lose its last line
        // from the live stream (it'd still be in the captured tail, but
        // streaming consumers like the TUI wouldn't see it).
        if let Some(ref em) = emitter
            && !line_buf.is_empty()
        {
            emit_line(em, &line_buf);
        }

        if truncated {
            buf.extend_from_slice(truncation_marker(cap).as_bytes());
        }
        buf
    })
}

/// Build the per-line `text` payload and hand it to the emitter's sink,
/// allocating a fresh `seq` from the shared counter.
fn emit_line(em: &ChunkEmitter, line: &[u8]) {
    let text = truncate_to_utf8_boundary(line, em.max_bytes);
    let seq = em.seq.fetch_add(1, Ordering::Relaxed);
    (em.sink)(em.stream, text, seq);
}

/// Truncate a byte slice to at most `max_bytes` bytes, always cutting at a
/// valid UTF-8 boundary so the returned `String` is well-formed and its
/// `len()` is `<= max_bytes`.
///
/// Invalid UTF-8 anywhere in the input is replaced with U+FFFD via
/// `from_utf8_lossy` (matching the existing tail-capture behavior).
fn truncate_to_utf8_boundary(bytes: &[u8], max_bytes: usize) -> String {
    let cut = if bytes.len() <= max_bytes {
        bytes.len()
    } else {
        match std::str::from_utf8(&bytes[..max_bytes]) {
            Ok(_) => max_bytes,
            // `valid_up_to` is the largest prefix length that's valid
            // UTF-8; cutting there guarantees a clean character boundary.
            Err(e) => e.valid_up_to(),
        }
    };
    String::from_utf8_lossy(&bytes[..cut]).into_owned()
}

/// Await a drain task, returning its captured bytes. If the task panicked or
/// was cancelled, return an empty buffer rather than propagating — the child
/// is already dead by the time we call this, and we prefer to keep logging
/// rather than poison the failure path.
pub async fn join_drain(handle: JoinHandle<Vec<u8>>) -> Vec<u8> {
    handle.await.unwrap_or_default()
}

/// Convenience: await a drain task and decode as a lossy UTF-8 String.
pub async fn join_drain_string(handle: JoinHandle<Vec<u8>>) -> String {
    let bytes = join_drain(handle).await;
    String::from_utf8_lossy(&bytes).to_string()
}

// ---------------------------------------------------------------------------
// wait_capped — shared "drain bounded + wait with optional timeout + kill the
// whole process group on timeout + reap" policy.
// ---------------------------------------------------------------------------

/// Result of [`wait_capped`].
///
/// Carries everything both the hook path and the review path need:
/// success/exit-code, the bounded stdout/stderr tails, and whether the wait
/// hit the timeout (in which case the process group was SIGKILL'd and the
/// child reaped before returning, so there is no lingering zombie or orphan
/// grandchild).
#[derive(Debug)]
pub struct WaitCappedResult {
    /// `true` iff the child exited with a success status. Always `false` when
    /// `timed_out` is set (the child was killed) or when `wait()` errored.
    pub success: bool,
    /// The child's exit code, if it exited normally with one. `None` on a
    /// signal death, a `wait()` error, or a timeout kill.
    pub code: Option<i32>,
    /// Bounded tail (last `cap` bytes) of the child's stdout, lossily decoded.
    pub stdout: String,
    /// Bounded tail (last `cap` bytes) of the child's stderr, lossily decoded.
    pub stderr: String,
    /// `true` iff the optional timeout fired before the child exited. On a
    /// timeout the process group is SIGKILL'd (unix) / the leader killed
    /// (non-unix) and the child reaped before this returns.
    pub timed_out: bool,
}

/// Spawn-is-done helper: concurrently bounded-drain `child`'s stdout/stderr,
/// wait for it to exit (optionally bounded by `timeout`), and on a timeout
/// SIGKILL the *whole process group* (so grandchildren the child spawned die
/// too) and reap the child so it can't linger as a zombie.
///
/// This is the single home for the process-group-kill-on-timeout +
/// bounded-drain policy shared by the lifecycle-hook path
/// ([`crate::hooks`]) and the review path ([`crate::review`]). The reference
/// implementation of the same pattern lives in
/// `executor::wait_with_timeout_and_abort` / `test_runner::run_single_test`;
/// those are deliberately *not* routed through here (they additionally race
/// an abort `watch` channel and emit chunk events — out of scope for this
/// helper, which has exactly two callers).
///
/// Preconditions:
/// - `child` was spawned with `.stdout(piped())` / `.stderr(piped())` so the
///   handles can be taken and drained (a `None` handle drains to empty).
/// - On unix the child is its own process-group leader (`process_group(0)`),
///   so `kill(-pid, SIGKILL)` on a timeout reaches the whole descendant tree
///   (not just the direct child). This is **not** a documentation-only
///   contract: it is machine-checked by a `debug_assert!` below
///   (`getpgid(pid) == pid`), so any future caller that forgets
///   `.process_group(0)` fails loudly in the test suite / CI rather than
///   silently leaking grandchildren on a timeout in production. Both current
///   callers satisfy it (hooks set it explicitly; the review child is spawned
///   via `harness::spawn_harness_with_delivery`, which sets it).
///
/// Behavior:
/// - `timeout == None`: plain `child.wait()` (no timer), but stdout/stderr
///   are *still* drained concurrently and bounded to `cap` — the
///   unbounded-memory half of the bug is fixed regardless of the timeout.
/// - `timeout == Some(d)`: race `child.wait()` against `sleep(d)`. If the
///   timer wins, SIGKILL the process group (unix) / `child.kill()`
///   (non-unix), `child.wait()` to reap, and return with `timed_out = true`.
///
/// On a `child.wait()` error the captured tails are still returned (so the
/// caller keeps diagnostics) with `success = false`, `code = None`,
/// `timed_out = false`.
pub fn wait_capped(
    mut child: tokio::process::Child,
    timeout: Option<Duration>,
    cap: usize,
) -> impl std::future::Future<Output = WaitCappedResult> {
    #[cfg(unix)]
    struct ProcessGroupKillGuard {
        pid: Option<i32>,
        armed: bool,
    }

    #[cfg(unix)]
    impl ProcessGroupKillGuard {
        fn new(child: &tokio::process::Child) -> Self {
            Self {
                pid: child.id().and_then(|id| i32::try_from(id).ok()),
                armed: true,
            }
        }

        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    #[cfg(unix)]
    impl Drop for ProcessGroupKillGuard {
        fn drop(&mut self) {
            if self.armed
                && let Some(pid) = self.pid
            {
                crate::executor::signal_process_group(pid, libc::SIGKILL);
            }
        }
    }

    #[cfg(unix)]
    let mut process_group_guard = ProcessGroupKillGuard::new(&child);

    // Machine-check the process-group precondition. The child is freshly
    // spawned and alive here, so `getpgid` is meaningful. If it is its own
    // group leader, `pgid == pid` and the timeout's `kill(-pid)` tears down
    // the whole tree; if not, a timeout would leak grandchildren. This is a
    // programming error at the call site (a missing `.process_group(0)`), so
    // we trip it loudly in debug/test builds rather than shipping a silent
    // leak. Release builds keep the existing safe behavior (the timeout still
    // signals `-pid` and `child.kill()`s the leader; only a *misused*
    // non-leader child's grandchildren would survive — and this assert
    // guarantees that misuse never reaches a release build untested).
    #[cfg(all(unix, debug_assertions))]
    {
        if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
            // SAFETY: `getpgid` is a pure syscall wrapper; `pid` is a valid
            // i32 naming our just-spawned, still-live child.
            let pgid = unsafe { libc::getpgid(pid) };
            debug_assert_eq!(
                pgid, pid,
                "wait_capped precondition violated: child pid {pid} is not its \
                 own process-group leader (pgid {pgid}); a timeout kill could \
                 not reach its grandchildren. Spawn it with \
                 Command::process_group(0)."
            );
        }
    }

    async move {
        // Take the pipe handles and start draining *immediately* — before the
        // wait — so a child that writes more than the kernel pipe buffer
        // (~64 KiB) doesn't block on write(2) while we block on wait(),
        // deadlocking. Same rationale as `drain_bounded`'s module doc.
        let stdout_task = drain_bounded(child.stdout.take(), cap);
        let stderr_task = drain_bounded(child.stderr.take(), cap);

        // Reap the child after a timeout kill (or just collect on normal exit),
        // fanning the kill to the whole process group on unix.
        async fn kill_group_and_reap(child: &mut tokio::process::Child) {
            #[cfg(unix)]
            {
                if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                    crate::executor::signal_process_group(pid, libc::SIGKILL);
                }
            }
            let _ = child.kill().await;
            // Reap so the child doesn't linger as a zombie. After wait()
            // returns the pipes are definitively closed and the drain tasks
            // exit promptly.
            let _ = child.wait().await;
        }

        let result = match timeout {
            Some(dur) => {
                tokio::select! {
                    status = child.wait() => match status {
                        Ok(es) => {
                            let stdout = join_drain_string(stdout_task).await;
                            let stderr = join_drain_string(stderr_task).await;
                            WaitCappedResult {
                                success: es.success(),
                                code: es.code(),
                                stdout,
                                stderr,
                                timed_out: false,
                            }
                        }
                        Err(_) => {
                            // Still collect whatever the drainers captured.
                            let stdout = join_drain_string(stdout_task).await;
                            let stderr = join_drain_string(stderr_task).await;
                            WaitCappedResult {
                                success: false,
                                code: None,
                                stdout,
                                stderr,
                                timed_out: false,
                            }
                        }
                    },
                    _ = tokio::time::sleep(dur) => {
                        kill_group_and_reap(&mut child).await;
                        let stdout = join_drain_string(stdout_task).await;
                        let stderr = join_drain_string(stderr_task).await;
                        WaitCappedResult {
                            success: false,
                            code: None,
                            stdout,
                            stderr,
                            timed_out: true,
                        }
                    }
                }
            }
            None => match child.wait().await {
                Ok(es) => {
                    let stdout = join_drain_string(stdout_task).await;
                    let stderr = join_drain_string(stderr_task).await;
                    WaitCappedResult {
                        success: es.success(),
                        code: es.code(),
                        stdout,
                        stderr,
                        timed_out: false,
                    }
                }
                Err(_) => {
                    let stdout = join_drain_string(stdout_task).await;
                    let stderr = join_drain_string(stderr_task).await;
                    WaitCappedResult {
                        success: false,
                        code: None,
                        stdout,
                        stderr,
                        timed_out: false,
                    }
                }
            },
        };

        #[cfg(unix)]
        process_group_guard.disarm();

        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// When the input is smaller than `cap`, the buffer is returned as-is
    /// without a truncation marker.
    #[tokio::test]
    async fn test_drain_bounded_under_cap_no_marker() {
        let (mut w, r) = tokio::io::duplex(64);
        let handle = drain_bounded(Some(r), 1024);
        w.write_all(b"hello world").await.unwrap();
        drop(w);
        let out = join_drain_string(handle).await;
        assert_eq!(out, "hello world");
        assert!(!out.contains(TRUNCATION_MARKER_PREFIX));
    }

    /// When the input exceeds `cap`, only the tail is kept and a marker is
    /// appended.
    #[tokio::test]
    async fn test_drain_bounded_truncates_to_tail() {
        let cap = 100;
        let (mut w, r) = tokio::io::duplex(256);
        let handle = drain_bounded(Some(r), cap);

        // Write 500 bytes — enough to exceed the cap several times.
        let writer = tokio::spawn(async move {
            for i in 0..500 {
                let byte = [b'a' + (i % 26) as u8];
                w.write_all(&byte).await.unwrap();
            }
            drop(w);
        });
        writer.await.unwrap();

        let out = join_drain(handle).await;
        // Tail bytes + marker.
        let marker = truncation_marker(cap);
        assert!(out.len() <= cap + marker.len());
        assert!(out.ends_with(marker.as_bytes()));
        // And the tail portion is exactly `cap` bytes long.
        let tail_len = out.len() - marker.len();
        assert_eq!(tail_len, cap);
    }

    /// A `None` reader produces an empty buffer without spawning any work.
    #[tokio::test]
    async fn test_drain_bounded_none_reader() {
        let handle: JoinHandle<Vec<u8>> = drain_bounded(None::<tokio::io::DuplexStream>, 1024);
        let out = join_drain(handle).await;
        assert!(out.is_empty());
    }

    // -- ChunkEmitter / drain_bounded_with_emitter ---------------------------

    type CollectedChunks = Arc<std::sync::Mutex<Vec<(ChunkStream, String, u64)>>>;

    /// Build a ChunkEmitter whose sink pushes `(stream, text, seq)` tuples
    /// into a shared `Mutex<Vec<_>>` for the test to inspect. Returns the
    /// emitter and the collected-events handle.
    fn collecting_emitter(
        stream: ChunkStream,
        seq: Arc<AtomicU64>,
        max_bytes: usize,
    ) -> (ChunkEmitter, CollectedChunks) {
        let collected: CollectedChunks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_for_sink = collected.clone();
        let sink: ChunkSink = Arc::new(move |s, t, n| {
            collected_for_sink.lock().unwrap().push((s, t, n));
        });
        (
            ChunkEmitter {
                stream,
                seq,
                max_bytes,
                sink,
            },
            collected,
        )
    }

    /// N newline-terminated lines yield N events with seq 0..N-1, in order,
    /// with the trailing newline stripped from each line's `text`.
    #[tokio::test]
    async fn test_drain_emitter_emits_one_event_per_newline_in_order() {
        let (mut w, r) = tokio::io::duplex(256);
        let seq = Arc::new(AtomicU64::new(0));
        let (emitter, collected) = collecting_emitter(ChunkStream::Stdout, seq, 4096);

        let handle = drain_bounded_with_emitter(Some(r), 1024, Some(emitter));
        w.write_all(b"line one\nline two\nline three\n")
            .await
            .unwrap();
        drop(w);
        let _ = join_drain(handle).await;

        let events = collected.lock().unwrap().clone();
        assert_eq!(events.len(), 3, "expected 3 events, got {events:?}");
        assert_eq!(events[0], (ChunkStream::Stdout, "line one".to_string(), 0));
        assert_eq!(events[1], (ChunkStream::Stdout, "line two".to_string(), 1));
        assert_eq!(
            events[2],
            (ChunkStream::Stdout, "line three".to_string(), 2)
        );
    }

    /// A line longer than `max_bytes` has its `text` payload truncated to
    /// at most `max_bytes` bytes (and at a UTF-8 boundary).
    #[tokio::test]
    async fn test_drain_emitter_truncates_long_lines_at_max_bytes() {
        let (mut w, r) = tokio::io::duplex(64 * 1024);
        let seq = Arc::new(AtomicU64::new(0));
        let max_bytes = 10;
        let (emitter, collected) = collecting_emitter(ChunkStream::Stdout, seq, max_bytes);

        let handle = drain_bounded_with_emitter(Some(r), 64 * 1024, Some(emitter));
        // Write a 50-byte line; expect the emitted text to be at most 10 bytes.
        let long_line = "x".repeat(50);
        let payload = format!("{long_line}\nshort\n");
        w.write_all(payload.as_bytes()).await.unwrap();
        drop(w);
        let _ = join_drain(handle).await;

        let events = collected.lock().unwrap().clone();
        assert_eq!(events.len(), 2, "expected 2 events, got {events:?}");
        assert!(
            events[0].1.len() <= max_bytes,
            "emitted text exceeded max_bytes: got {} bytes ({:?})",
            events[0].1.len(),
            events[0].1,
        );
        assert_eq!(events[0].1, "x".repeat(max_bytes));
        assert_eq!(events[1].1, "short");
    }

    #[tokio::test]
    async fn test_drain_emitter_bounds_newline_free_line_buffer() {
        let (mut w, r) = tokio::io::duplex(64 * 1024);
        let seq = Arc::new(AtomicU64::new(0));
        let max_bytes = 16;
        let (emitter, collected) = collecting_emitter(ChunkStream::Stdout, seq, max_bytes);

        let handle = drain_bounded_with_emitter(Some(r), 64 * 1024, Some(emitter));
        w.write_all("x".repeat(8192).as_bytes()).await.unwrap();
        drop(w);
        let _ = join_drain(handle).await;

        let events = collected.lock().unwrap().clone();
        assert_eq!(events.len(), 1, "expected EOF flush, got {events:?}");
        assert_eq!(events[0].1.len(), max_bytes);
        assert_eq!(events[0].1, "x".repeat(max_bytes));
    }

    /// Truncation must cut at a UTF-8 character boundary so the emitted
    /// `text` is always a well-formed string of length `<= max_bytes`.
    #[test]
    fn test_truncate_to_utf8_boundary_respects_char_boundary() {
        // `é` is 2 bytes (0xC3 0xA9) in UTF-8. With max_bytes=3 on `"aé"`
        // (3 bytes total) we keep all 3. With max_bytes=2 we have to drop
        // the multi-byte char and keep just `"a"` (1 byte).
        assert_eq!(truncate_to_utf8_boundary("aé".as_bytes(), 3), "aé");
        assert_eq!(truncate_to_utf8_boundary("aé".as_bytes(), 2), "a");
        // Boundary exactly between chars is fine.
        assert_eq!(truncate_to_utf8_boundary("éé".as_bytes(), 2), "é");
        assert_eq!(truncate_to_utf8_boundary("éé".as_bytes(), 4), "éé");
        // Empty input is a stable empty string regardless of cap.
        assert_eq!(truncate_to_utf8_boundary(b"", 4), "");
    }

    /// `seq` is allocated from the shared counter, so two drainers (stdout
    /// and stderr) both emitting interleaved lines produce a strictly
    /// monotonic union.
    #[tokio::test]
    async fn test_drain_emitter_seq_is_monotonic_across_streams() {
        let (mut wo, ro) = tokio::io::duplex(256);
        let (mut we, re) = tokio::io::duplex(256);
        let seq = Arc::new(AtomicU64::new(0));

        // Both emitters share the same `seq` counter.
        let (em_out, collected_out) = collecting_emitter(ChunkStream::Stdout, seq.clone(), 4096);
        let (em_err, collected_err) = collecting_emitter(ChunkStream::Stderr, seq.clone(), 4096);

        let h_out = drain_bounded_with_emitter(Some(ro), 1024, Some(em_out));
        let h_err = drain_bounded_with_emitter(Some(re), 1024, Some(em_err));

        // Drive writes on the same task to keep a deterministic order:
        // out, err, out, err, out — five lines total → seq 0..=4.
        wo.write_all(b"o1\n").await.unwrap();
        we.write_all(b"e1\n").await.unwrap();
        wo.write_all(b"o2\n").await.unwrap();
        we.write_all(b"e2\n").await.unwrap();
        wo.write_all(b"o3\n").await.unwrap();
        drop(wo);
        drop(we);
        let _ = join_drain(h_out).await;
        let _ = join_drain(h_err).await;

        let mut all: Vec<_> = collected_out.lock().unwrap().clone();
        all.extend(collected_err.lock().unwrap().clone());
        all.sort_by_key(|e| e.2);

        // Five total events, seq is 0..5 with no gaps or duplicates.
        assert_eq!(all.len(), 5, "expected 5 events total, got {all:?}");
        let seqs: Vec<u64> = all.iter().map(|e| e.2).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4], "seqs should be 0..N-1 in union");
    }

    /// A trailing line without a final newline still produces an emit at EOF
    /// so streaming consumers don't lose the last line.
    #[tokio::test]
    async fn test_drain_emitter_flushes_unterminated_tail() {
        let (mut w, r) = tokio::io::duplex(256);
        let seq = Arc::new(AtomicU64::new(0));
        let (emitter, collected) = collecting_emitter(ChunkStream::Stdout, seq, 4096);

        let handle = drain_bounded_with_emitter(Some(r), 1024, Some(emitter));
        w.write_all(b"first\nsecond-no-newline").await.unwrap();
        drop(w);
        let _ = join_drain(handle).await;

        let events = collected.lock().unwrap().clone();
        assert_eq!(events.len(), 2, "expected 2 events, got {events:?}");
        assert_eq!(events[0].1, "first");
        assert_eq!(events[1].1, "second-no-newline");
    }

    /// Lines that span multiple `read` calls (because the writer flushes
    /// mid-line) must still be emitted as a single event when the newline
    /// finally arrives.
    #[tokio::test]
    async fn test_drain_emitter_handles_partial_reads() {
        let (mut w, r) = tokio::io::duplex(8);
        let seq = Arc::new(AtomicU64::new(0));
        let (emitter, collected) = collecting_emitter(ChunkStream::Stdout, seq, 4096);

        let handle = drain_bounded_with_emitter(Some(r), 1024, Some(emitter));
        // Force the reader to see the line in pieces by interleaving small
        // writes with explicit yields.
        w.write_all(b"hello ").await.unwrap();
        tokio::task::yield_now().await;
        w.write_all(b"world\n").await.unwrap();
        drop(w);
        let _ = join_drain(handle).await;

        let events = collected.lock().unwrap().clone();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            (ChunkStream::Stdout, "hello world".to_string(), 0)
        );
    }

    /// When `emitter` is `None`, the new function behaves exactly like the
    /// original `drain_bounded` — captures the full output and emits no
    /// events (there's no sink to call).
    #[tokio::test]
    async fn test_drain_bounded_with_emitter_none_matches_drain_bounded() {
        let (mut w, r) = tokio::io::duplex(256);
        let handle = drain_bounded_with_emitter::<tokio::io::DuplexStream>(Some(r), 1024, None);
        w.write_all(b"a\nb\nc\n").await.unwrap();
        drop(w);
        let out = join_drain_string(handle).await;
        assert_eq!(out, "a\nb\nc\n");
    }

    // -- wait_capped --------------------------------------------------------

    /// Spawn `sh -c <body>` in its own process group (mirroring the two
    /// production callers) with piped stdout/stderr.
    fn spawn_sh(body: &str) -> tokio::process::Child {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(body)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            cmd.process_group(0);
        }
        cmd.spawn().expect("spawn sh")
    }

    /// Normal exit: success/code/stdout are reported and `timed_out` is false.
    #[tokio::test]
    async fn test_wait_capped_normal_exit() {
        let child = spawn_sh("printf out; printf err 1>&2; exit 0");
        let r = wait_capped(child, Some(Duration::from_secs(10)), 64 * 1024).await;
        assert!(r.success);
        assert_eq!(r.code, Some(0));
        assert!(!r.timed_out);
        assert_eq!(r.stdout, "out");
        assert_eq!(r.stderr, "err");

        // Non-zero exit is reported faithfully.
        let child = spawn_sh("printf boom 1>&2; exit 3");
        let r = wait_capped(child, Some(Duration::from_secs(10)), 64 * 1024).await;
        assert!(!r.success);
        assert_eq!(r.code, Some(3));
        assert!(!r.timed_out);
        assert_eq!(r.stderr, "boom");
    }

    /// No-timeout path still drains (and bounds) output rather than blocking.
    #[tokio::test]
    async fn test_wait_capped_no_timeout_still_bounds() {
        // ~200 KiB of stdout with a 4 KiB cap → must be capped, and the call
        // must still complete (proving the concurrent drain prevented a
        // pipe-buffer deadlock even with no timer).
        let child = spawn_sh("yes ABCDEFGH | head -c 200000; exit 0");
        let cap = 4 * 1024;
        let r = wait_capped(child, None, cap).await;
        assert!(r.success);
        assert!(!r.timed_out);
        assert!(
            r.stdout.len() <= cap + truncation_marker(cap).len(),
            "stdout not bounded: {} bytes",
            r.stdout.len()
        );
        assert!(r.stdout.contains(TRUNCATION_MARKER_PREFIX));
    }

    /// Timeout path: a sleeping child is killed, `timed_out` is true, and the
    /// call returns promptly (well under the child's sleep).
    #[tokio::test]
    async fn test_wait_capped_timeout_kills_and_reaps() {
        let start = std::time::Instant::now();
        // A backgrounded grandchild that would outlive a leader-only kill.
        let child = spawn_sh("(sleep 30 &) ; echo started; sleep 30");
        let r = wait_capped(child, Some(Duration::from_secs(1)), 64 * 1024).await;
        let elapsed = start.elapsed();
        assert!(r.timed_out, "expected a timeout");
        assert!(!r.success);
        assert!(
            elapsed < Duration::from_secs(10),
            "timeout should fire promptly, elapsed = {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_wait_capped_future_abort_kills_process_group() {
        let tmp = tempfile::tempdir().unwrap();
        let ready = tmp.path().join("ready");
        let grandchild = tmp.path().join("grandchild");
        let body = format!(
            "sleep 1000 & echo $! > {grandchild}; touch {ready}; wait",
            grandchild = grandchild.display(),
            ready = ready.display(),
        );
        let child = spawn_sh(&body);
        let handle = tokio::spawn(wait_capped(child, Some(Duration::from_secs(60)), 1024));

        for _ in 0..100 {
            if ready.exists() && grandchild.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "child did not signal readiness");
        let grandchild_pid: i32 = std::fs::read_to_string(&grandchild)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        handle.abort();
        let _ = handle.await;

        for _ in 0..100 {
            // SAFETY: kill(pid, 0) probes process existence without sending a signal.
            let alive = unsafe { libc::kill(grandchild_pid, 0) } == 0;
            if !alive {
                return;
            }
            if let Ok(status) = std::fs::read_to_string(format!("/proc/{grandchild_pid}/status"))
                && status.lines().any(|line| line.starts_with("State:\tZ"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("grandchild process {grandchild_pid} survived wait_capped future abort");
    }

    /// The process-group precondition is machine-checked, not doc-only: a
    /// child spawned WITHOUT `.process_group(0)` (so it is not its own group
    /// leader — a timeout kill could not reach its grandchildren) must trip
    /// the `debug_assert!` in `wait_capped`. This is the test that guarantees
    /// any future caller forgetting the precondition fails loudly in CI
    /// instead of silently leaking grandchildren in production.
    #[cfg(all(unix, debug_assertions))]
    #[tokio::test]
    #[should_panic(expected = "wait_capped precondition violated")]
    async fn test_wait_capped_rejects_non_process_group_leader() {
        // Deliberately NO cmd.process_group(0): the child stays in the test
        // runner's process group, so getpgid(pid) != pid.
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg("sleep 5")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let child = cmd.spawn().expect("spawn sh");
        // Must panic on the precondition before doing any waiting.
        let _ = wait_capped(child, Some(Duration::from_secs(1)), 64 * 1024).await;
    }

    /// Truncation marker format uses MiB for exact MiB multiples and falls
    /// back to bytes otherwise.
    #[test]
    fn test_truncation_marker_format() {
        assert_eq!(
            truncation_marker(1024 * 1024),
            "\n[output truncated to last 1 MiB]\n"
        );
        assert_eq!(
            truncation_marker(4 * 1024 * 1024),
            "\n[output truncated to last 4 MiB]\n"
        );
        assert_eq!(
            truncation_marker(500),
            "\n[output truncated to last 500 bytes]\n"
        );
    }
}
