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

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

/// Marker appended to a captured buffer when truncation occurred. Exposed so
/// callers and tests can detect / match on it.
pub const TRUNCATION_MARKER_PREFIX: &str = "\n[output truncated to last ";

/// Build the full truncation marker for a given cap in bytes. Uses MiB when
/// the cap is an exact multiple, otherwise falls back to bytes.
fn truncation_marker(cap: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if cap >= MIB && cap % MIB == 0 {
        format!("{TRUNCATION_MARKER_PREFIX}{} MiB]\n", cap / MIB)
    } else {
        format!("{TRUNCATION_MARKER_PREFIX}{cap} bytes]\n")
    }
}

/// Spawn a task that continuously drains `reader` into a `Vec<u8>` bounded at
/// `cap` bytes (preserving the *tail* — the last `cap` bytes). Returns a
/// `JoinHandle` whose value is the captured bytes, with a synthetic
/// truncation-marker line appended iff any bytes were dropped.
///
/// The task takes ownership of `reader`. If a read errors mid-stream, it
/// returns whatever was accumulated so far rather than erroring out — the
/// parent still needs diagnostic output. On EOF it returns normally.
pub fn drain_bounded<R>(reader: Option<R>, cap: usize) -> JoinHandle<Vec<u8>>
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

        loop {
            match r.read(&mut chunk).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.len() > cap {
                        // Keep the last `cap` bytes only.
                        let excess = buf.len() - cap;
                        buf.drain(..excess);
                        truncated = true;
                    }
                }
                Err(_) => {
                    // Mid-stream read failure — return whatever we have so
                    // callers still get partial diagnostics.
                    break;
                }
            }
        }

        if truncated {
            buf.extend_from_slice(truncation_marker(cap).as_bytes());
        }
        buf
    })
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
        let handle: JoinHandle<Vec<u8>> =
            drain_bounded(None::<tokio::io::DuplexStream>, 1024);
        let out = join_drain(handle).await;
        assert!(out.is_empty());
    }

    /// Truncation marker format uses MiB for exact MiB multiples and falls
    /// back to bytes otherwise.
    #[test]
    fn test_truncation_marker_format() {
        assert_eq!(truncation_marker(1024 * 1024), "\n[output truncated to last 1 MiB]\n");
        assert_eq!(
            truncation_marker(4 * 1024 * 1024),
            "\n[output truncated to last 4 MiB]\n"
        );
        assert_eq!(truncation_marker(500), "\n[output truncated to last 500 bytes]\n");
    }
}
