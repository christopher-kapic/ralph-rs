//! Integration tests for `ralph run` routing (TUI-plan.md §2).
//!
//! Drives the actual `ralph` binary to verify the rule from
//! `is_default_run_invocation`:
//!
//! - bare `ralph run` from a TTY → plan-detail TUI (alt-screen entered).
//! - bare `ralph run` from a non-TTY → today's runner (no alt-screen).
//! - any non-default flag (e.g. `--json`) → today's runner (no alt-screen).
//!
//! Unit tests in `src/commands/run.rs` already cover the pure routing
//! decision; this file exercises end-to-end behavior so a future regression
//! that bypassed `run_tui_mode` (e.g. by calling `dispatch_run` unconditionally)
//! would surface here even if the routing predicate kept passing.

#![cfg(unix)]

use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Alternate-screen ENTER escape sequence (DECSET 1049). Crossterm emits this
/// when `EnterAlternateScreen` is executed, so its presence in the child's
/// output is a reliable proxy for "the TUI was launched."
const ALT_SCREEN_ENTER: &[u8] = b"\x1b[?1049h";

fn ralph_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ralph")
}

/// Stand up a tempdir as a fresh ralph project: git-init it, run `ralph init`,
/// create an approved plan with one (skipped) step so `ralph run` has
/// something to dispatch but doesn't actually invoke a harness. Returns the
/// tempdir handle (whose `Drop` cleans up) and the project path.
fn setup_project() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let home = dir.path().join("home");
    let proj = dir.path().join("proj");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&proj).unwrap();

    // git init + a single commit so the project has a HEAD (some ralph paths
    // probe git state).
    run_check(
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg("-b")
            .arg("main")
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
        Command::new("git")
            .args(["add", "."])
            .current_dir(&proj),
        "git add",
    );
    run_check(
        Command::new("git")
            .args(["commit", "-q", "-m", "init"])
            .current_dir(&proj),
        "git commit",
    );

    // Redirect HOME so `ralph init` writes config and DB under the tempdir
    // rather than polluting the developer's actual `~/.config/ralph-rs`.
    let env_home = home.clone();
    let env_xdg_config = home.join(".config");
    let env_xdg_data = home.join(".local").join("share");

    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["init", "--non-interactive", "--default-harness", "claude"])
            .env("HOME", &env_home)
            .env("XDG_CONFIG_HOME", &env_xdg_config)
            .env("XDG_DATA_HOME", &env_xdg_data),
        "ralph init",
    );

    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["plan", "create", "test-plan", "-d", "test"])
            .env("HOME", &env_home)
            .env("XDG_CONFIG_HOME", &env_xdg_config)
            .env("XDG_DATA_HOME", &env_xdg_data),
        "plan create",
    );
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["plan", "approve", "test-plan"])
            .env("HOME", &env_home)
            .env("XDG_CONFIG_HOME", &env_xdg_config)
            .env("XDG_DATA_HOME", &env_xdg_data),
        "plan approve",
    );

    (dir, proj)
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
    let out = cmd.output().unwrap_or_else(|e| panic!("{label}: spawn: {e}"));
    assert!(
        out.status.success(),
        "{label} failed: status={:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn json_flag_takes_noninteractive_path() {
    let (_dir, proj) = setup_project();

    let out = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["run", "test-plan", "--json", "--skip-preflight"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    let combined: Vec<u8> = out
        .stdout
        .iter()
        .chain(out.stderr.iter())
        .copied()
        .collect();
    assert!(
        !combined.windows(ALT_SCREEN_ENTER.len()).any(|w| w == ALT_SCREEN_ENTER),
        "--json must take the non-interactive path; alt-screen escape leaked.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn bare_run_with_piped_stdout_takes_noninteractive_path() {
    let (_dir, proj) = setup_project();

    // Stdout is piped → `IsTerminal::is_terminal` reports false → routing
    // falls through to today's dispatcher even though no flags are set.
    let out = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["run", "test-plan"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    let combined: Vec<u8> = out
        .stdout
        .iter()
        .chain(out.stderr.iter())
        .copied()
        .collect();
    assert!(
        !combined.windows(ALT_SCREEN_ENTER.len()).any(|w| w == ALT_SCREEN_ENTER),
        "non-TTY stdout must skip the TUI; alt-screen escape leaked.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn bare_run_with_pty_enters_tui() {
    let (_dir, proj) = setup_project();

    // Open a real pseudo-terminal so the child binary's stdout is a TTY.
    let (master, slave) = open_pty();
    let master_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(master) };
    let slave_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(slave) };

    // Each Stdio takes ownership, so dup the slave for stdin/stdout/stderr.
    let stdin_fd = slave_owned.try_clone().unwrap();
    let stdout_fd = slave_owned.try_clone().unwrap();
    let stderr_fd = slave_owned;

    let mut child = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["run", "test-plan"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        // Force a known TERM so crossterm doesn't bail early on an empty TERM
        // in the test environment.
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(stdin_fd))
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(stderr_fd))
        .spawn()
        .expect("spawn ralph");

    set_nonblocking(master_owned.as_raw_fd());
    let mut master_file = std::fs::File::from(master_owned);

    // Read with a 5s budget. We're looking for the alt-screen ENTER escape,
    // which crossterm emits as soon as the TUI dispatcher executes
    // `EnterAlternateScreen`. Anything quicker than EOF / kill is enough.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut buf = [0u8; 4096];
    let mut accum: Vec<u8> = Vec::new();
    let mut found = false;
    while Instant::now() < deadline && !found {
        match master_file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                accum.extend_from_slice(&buf[..n]);
                if accum
                    .windows(ALT_SCREEN_ENTER.len())
                    .any(|w| w == ALT_SCREEN_ENTER)
                {
                    found = true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => break,
        }
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        found,
        "expected alt-screen escape on PTY-attached `ralph run`; captured: {:?}",
        String::from_utf8_lossy(&accum),
    );
}

// ---------------------------------------------------------------------------
// Pty helpers (unix-only — gated at the file level via #![cfg(unix)]).
// ---------------------------------------------------------------------------

fn open_pty() -> (libc::c_int, libc::c_int) {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    let rc = unsafe {
        libc::openpty(
            &mut master as *mut _,
            &mut slave as *mut _,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(rc, 0, "openpty failed: {}", std::io::Error::last_os_error());
    (master, slave)
}

fn set_nonblocking(fd: libc::c_int) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        assert!(flags != -1, "fcntl GETFL failed");
        let rc = libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        assert_eq!(rc, 0, "fcntl SETFL O_NONBLOCK failed");
    }
}
