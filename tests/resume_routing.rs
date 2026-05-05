//! Integration tests for `ralph resume` routing (TUI-plan.md §2 / step 34).
//!
//! Mirrors `tests/run_routing.rs` for the resume command: drives the actual
//! `ralph` binary to verify the rule from `is_default_resume_invocation`.
//!
//! - bare `ralph resume <slug>` from a TTY → plan-detail TUI (alt-screen entered).
//! - bare `ralph resume` from a non-TTY → today's CLI runner (no alt-screen).
//! - `--json` / `--non-interactive` → today's CLI runner (no alt-screen).
//!
//! Unit tests in `src/commands/run.rs` already cover the pure routing decision;
//! this file exercises end-to-end behavior so a future regression that bypassed
//! `run_resume_tui_mode` (e.g. by calling `dispatch_resume` unconditionally)
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
/// create an approved plan so `ralph resume <slug>` can resolve it. Returns
/// the tempdir handle (whose `Drop` cleans up) and the project path.
fn setup_project() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let home = dir.path().join("home");
    let proj = dir.path().join("proj");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&proj).unwrap();

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
        Command::new("git").args(["add", "."]).current_dir(&proj),
        "git add",
    );
    run_check(
        Command::new("git")
            .args(["commit", "-q", "-m", "init"])
            .current_dir(&proj),
        "git commit",
    );

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

#[test]
fn json_flag_resume_takes_noninteractive_path() {
    let (_dir, proj) = setup_project();

    // `--json` is one of the explicit "force CLI mode" flags in
    // `is_default_resume_invocation`. Even on a TTY, scripts piping
    // NDJSON expect the non-interactive path.
    let out = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["--json", "resume", "test-plan"])
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
        !combined
            .windows(ALT_SCREEN_ENTER.len())
            .any(|w| w == ALT_SCREEN_ENTER),
        "--json must take the non-interactive path; alt-screen escape leaked.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn non_interactive_flag_resume_takes_noninteractive_path() {
    let (_dir, proj) = setup_project();

    // `--non-interactive` is the explicit opt-out from the TUI even when
    // stdout is a TTY. With piped stdout here we'd already fail the TTY
    // check, but the assertion holds either way — and locking it in here
    // documents the contract.
    let out = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["--non-interactive", "resume", "test-plan"])
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
        !combined
            .windows(ALT_SCREEN_ENTER.len())
            .any(|w| w == ALT_SCREEN_ENTER),
        "--non-interactive must take the CLI path; alt-screen escape leaked.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn bare_resume_with_piped_stdout_takes_noninteractive_path() {
    let (_dir, proj) = setup_project();

    // Stdout is piped → `IsTerminal::is_terminal` reports false → routing
    // falls through to today's dispatcher even though no flags are set.
    let out = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["resume", "test-plan"])
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
        !combined
            .windows(ALT_SCREEN_ENTER.len())
            .any(|w| w == ALT_SCREEN_ENTER),
        "non-TTY stdout must skip the TUI; alt-screen escape leaked.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn bare_resume_with_pty_enters_tui_with_slug() {
    let (_dir, proj) = setup_project();

    // Open a real pseudo-terminal so the child binary's stdout is a TTY.
    let (master, slave) = open_pty();
    let master_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(master) };
    let slave_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(slave) };

    let stdin_fd = slave_owned.try_clone().unwrap();
    let stdout_fd = slave_owned.try_clone().unwrap();
    let stderr_fd = slave_owned;

    // `ralph resume <slug>` from a TTY drops into plan-detail TUI per
    // step 34. Slug-form is exercised here because it bypasses the
    // branch-inference fast path in `resolve_resume_plan` and therefore
    // pins the routing decision purely on `is_default_resume_invocation`.
    let mut child = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["resume", "test-plan"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(stdin_fd))
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(stderr_fd))
        .spawn()
        .expect("spawn ralph");

    set_nonblocking(master_owned.as_raw_fd());
    let mut master_file = std::fs::File::from(master_owned);

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
        "expected alt-screen escape on PTY-attached `ralph resume <slug>`; captured: {:?}",
        String::from_utf8_lossy(&accum),
    );
}

#[test]
fn bare_resume_with_pty_enters_tui_branch_inferred() {
    let (_dir, proj) = setup_project();

    // Switch to the plan's branch so `resolve_resume_plan` infers the
    // slug from the current branch (per step 33). This is the canonical
    // form of `ralph resume`: zero args, no slug, and the plan is picked
    // up automatically from `git branch --show-current`. `plan create`
    // doesn't materialise the branch (that happens lazily on first
    // `ralph run`), so we create it here by hand.
    run_check(
        Command::new("git")
            .args(["checkout", "-q", "-b", "test-plan"])
            .current_dir(&proj),
        "git checkout -b test-plan",
    );

    let (master, slave) = open_pty();
    let master_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(master) };
    let slave_owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(slave) };

    let stdin_fd = slave_owned.try_clone().unwrap();
    let stdout_fd = slave_owned.try_clone().unwrap();
    let stderr_fd = slave_owned;

    let mut child = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["resume"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(stdin_fd))
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(stderr_fd))
        .spawn()
        .expect("spawn ralph");

    set_nonblocking(master_owned.as_raw_fd());
    let mut master_file = std::fs::File::from(master_owned);

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
        "expected alt-screen escape on PTY-attached bare `ralph resume`; captured: {:?}",
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
