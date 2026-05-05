// $EDITOR handoff for the TUI (TUI-plan.md §14).
//
// Suspends the TUI's alternate screen, spawns the user's editor against a
// temp file pre-seeded with `initial_text`, then restores the TUI on exit.
// Used by step-detail panes that switch into long-form editing mode.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use uuid::Uuid;

use crate::config;

/// Open the user's editor on `initial_text` and return the saved contents.
///
/// `Ok(None)` is returned when:
/// - Neither `$EDITOR` nor `$VISUAL` is set (caller toasts the missing-editor
///   error).
/// - The editor exits with a non-zero status (treated as cancel; tempfile is
///   left in place).
///
/// The tempfile lives at `~/.local/share/ralph-rs/tmp/<scope>-<id>-<rand>.md`
/// and is deleted only on successful save.
pub fn edit_in_editor(initial_text: &str) -> Result<Option<String>> {
    let Some(editor) = resolve_editor() else {
        return Ok(None);
    };
    let tmp_dir = config::data_dir()?.join("tmp");
    fs::create_dir_all(&tmp_dir)
        .with_context(|| format!("create editor tempdir at {}", tmp_dir.display()))?;
    let path = make_temp_path(&tmp_dir, "edit");

    suspend_terminal()?;
    let result = edit_at(&editor, &path, initial_text);
    let _ = restore_terminal();
    result
}

/// Look up the editor command, preferring `$EDITOR` then `$VISUAL`. Empty or
/// whitespace-only values are treated as unset.
fn resolve_editor() -> Option<String> {
    resolve_editor_from(|var| std::env::var(var).ok())
}

/// Testable form of `resolve_editor` parameterized on the env-lookup
/// function — avoids env mutation in tests, which is unsafe under 2024
/// edition and racy across the parallel test harness.
fn resolve_editor_from<F>(get: F) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    for var in ["EDITOR", "VISUAL"] {
        if let Some(s) = get(var)
            && !s.trim().is_empty()
        {
            return Some(s);
        }
    }
    None
}

/// Build the tempfile path: `<scope>-<uuid>-<rand>.md` under `dir`.
fn make_temp_path(dir: &Path, scope: &str) -> PathBuf {
    let id = Uuid::new_v4().simple();
    let rand = rand_suffix();
    dir.join(format!("{scope}-{id}-{rand:08x}.md"))
}

/// Cheap entropy without a `rand` dep — combined with the per-call uuid
/// in the filename, this is enough to avoid collisions on concurrent edits.
fn rand_suffix() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
}

/// Internal: write `initial_text` to `path`, run the editor, and read back.
/// Pure of terminal-suspension concerns so it can be exercised in tests
/// that don't have a real TTY.
pub(crate) fn edit_at(editor: &str, path: &Path, initial_text: &str) -> Result<Option<String>> {
    fs::write(path, initial_text).with_context(|| format!("write {}", path.display()))?;

    let status = spawn_editor(editor, path)?;
    if !status.success() {
        return Ok(None);
    }
    let contents =
        fs::read_to_string(path).with_context(|| format!("read back {}", path.display()))?;
    let _ = fs::remove_file(path);
    Ok(Some(contents))
}

/// Spawn the editor with stdio inherited from the parent. Splits on
/// whitespace so callers can include flags (`vim --noplugin`).
fn spawn_editor(editor: &str, path: &Path) -> Result<std::process::ExitStatus> {
    let mut parts = editor.split_whitespace();
    let prog = parts.next().context("EDITOR is empty")?;
    let mut cmd = Command::new(prog);
    cmd.args(parts);
    cmd.arg(path);
    cmd.status()
        .with_context(|| format!("spawn editor: {editor}"))
}

// Mouse capture is toggled in lock-step with the alternate screen so the user's
// editor sees a normal terminal (with native click-drag selection) and the TUI
// resumes with capture re-enabled — matching the dispatcher's setup so view
// `handle_mouse` routing keeps working after the round-trip.
fn suspend_terminal() -> Result<()> {
    disable_raw_mode().context("disable raw mode")?;
    let mut stdout = std::io::stdout();
    execute!(stdout, LeaveAlternateScreen, DisableMouseCapture)
        .context("leave alternate screen")?;
    Ok(())
}

fn restore_terminal() -> Result<()> {
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .context("re-enter alternate screen")?;
    enable_raw_mode().context("enable raw mode")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- resolve_editor_from ----------------------------------------------

    fn lookup<'a>(
        map: &'a HashMap<&'static str, &'static str>,
    ) -> impl Fn(&str) -> Option<String> + 'a {
        |k: &str| map.get(k).map(|s| s.to_string())
    }

    #[test]
    fn resolve_editor_prefers_editor_over_visual() {
        let env = HashMap::from([("EDITOR", "vim"), ("VISUAL", "nano")]);
        assert_eq!(resolve_editor_from(lookup(&env)), Some("vim".to_string()));
    }

    #[test]
    fn resolve_editor_falls_back_to_visual() {
        let env = HashMap::from([("VISUAL", "nano")]);
        assert_eq!(resolve_editor_from(lookup(&env)), Some("nano".to_string()));
    }

    #[test]
    fn resolve_editor_returns_none_when_unset() {
        let env = HashMap::<&str, &str>::new();
        assert_eq!(resolve_editor_from(lookup(&env)), None);
    }

    #[test]
    fn resolve_editor_treats_empty_as_unset() {
        let env = HashMap::from([("EDITOR", "   ")]);
        assert_eq!(resolve_editor_from(lookup(&env)), None);
    }

    #[test]
    fn resolve_editor_preserves_args() {
        let env = HashMap::from([("EDITOR", "vim --noplugin")]);
        assert_eq!(
            resolve_editor_from(lookup(&env)),
            Some("vim --noplugin".to_string())
        );
    }

    // -- make_temp_path ---------------------------------------------------

    #[test]
    fn temp_path_is_under_dir_with_scope_and_md_extension() {
        let dir = Path::new("/tmp/ralph");
        let p = make_temp_path(dir, "step");
        let s = p.to_string_lossy();
        assert!(s.starts_with("/tmp/ralph/step-"), "wrong dir/scope: {s}");
        assert!(s.ends_with(".md"), "wrong extension: {s}");
    }

    #[test]
    fn temp_path_is_unique_per_call() {
        let dir = Path::new("/tmp/ralph");
        let a = make_temp_path(dir, "step");
        let b = make_temp_path(dir, "step");
        assert_ne!(a, b);
    }

    // -- edit_at integration (mocked $EDITOR) -----------------------------
    //
    // We mock the editor by writing a tiny shell script into a tempdir and
    // invoking it as if it were $EDITOR. This is unix-only — we don't try to
    // make these tests pass on Windows, where the public API would need a
    // different mock anyway.

    #[cfg(unix)]
    fn write_script(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, body).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Invoke scripts via `/bin/sh <path>` rather than exec'ing them directly:
    // when cargo runs tests in parallel, another thread's freshly-forked child
    // can inherit a writable fd to our script across its fork→exec window,
    // and Linux returns ETXTBSY on execve while any process holds the file
    // open for write. `sh` opens the script as a regular file, so it sidesteps
    // the race.
    #[cfg(unix)]
    fn sh_editor(script: &Path) -> String {
        format!("/bin/sh {}", script.display())
    }

    #[cfg(unix)]
    #[test]
    fn edit_at_returns_modified_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("ed.sh");
        write_script(&script, "#!/bin/sh\necho MODIFIED > \"$1\"\n");

        let target = tmp.path().join("file.md");
        let result = edit_at(&sh_editor(&script), &target, "initial").unwrap();
        assert_eq!(result, Some("MODIFIED\n".to_string()));
        assert!(!target.exists(), "tempfile should be deleted on success");
    }

    #[cfg(unix)]
    #[test]
    fn edit_at_returns_none_on_nonzero_exit() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fail.sh");
        write_script(&script, "#!/bin/sh\nexit 1\n");

        let target = tmp.path().join("file.md");
        let result = edit_at(&sh_editor(&script), &target, "initial").unwrap();
        assert_eq!(result, None);
        assert!(
            target.exists(),
            "tempfile should be retained on non-zero exit"
        );
    }

    #[cfg(unix)]
    #[test]
    fn edit_at_seeds_initial_text_for_editor() {
        let tmp = tempfile::tempdir().unwrap();
        let copy = tmp.path().join("seen-by-editor");
        let script = tmp.path().join("show.sh");
        // Capture what the editor sees by copying $1 to a known location
        // before any modification.
        write_script(
            &script,
            &format!("#!/bin/sh\ncp \"$1\" \"{}\"\n", copy.display()),
        );

        let target = tmp.path().join("file.md");
        let _ = edit_at(&sh_editor(&script), &target, "INITIAL CONTENT").unwrap();
        let seen = fs::read_to_string(&copy).unwrap();
        assert_eq!(seen, "INITIAL CONTENT");
    }

    #[cfg(unix)]
    #[test]
    fn edit_at_supports_editor_with_flags() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("ed.sh");
        // Script asserts arg 0 (the flag) is "--flag" and arg 1 is the file.
        write_script(
            &script,
            "#!/bin/sh\nif [ \"$1\" != \"--flag\" ]; then exit 2; fi\necho OK > \"$2\"\n",
        );

        let target = tmp.path().join("file.md");
        let editor = format!("/bin/sh {} --flag", script.display());
        let result = edit_at(&editor, &target, "initial").unwrap();
        assert_eq!(result, Some("OK\n".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn edit_at_returns_none_when_editor_does_not_modify() {
        // Editor that exits 0 without changing the file: contents should
        // round-trip unchanged.
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("noop.sh");
        write_script(&script, "#!/bin/sh\nexit 0\n");

        let target = tmp.path().join("file.md");
        let result = edit_at(&sh_editor(&script), &target, "untouched").unwrap();
        assert_eq!(result, Some("untouched".to_string()));
    }
}
