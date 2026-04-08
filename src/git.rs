#![allow(dead_code)]
// Git integration – thin wrappers around `git` CLI via std::process::Command.
//
// Every public function accepts a `workdir` parameter so callers can target any
// working directory without mutating global state.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run a git command in `workdir` and return its stdout on success.
fn git(workdir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git {}", args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "git {} failed (exit {}): {}",
            args.join(" "),
            output.status,
            stderr.trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(stdout)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a new branch and switch to it.
pub fn create_and_checkout_branch(workdir: &Path, branch_name: &str) -> Result<()> {
    git(workdir, &["checkout", "-b", branch_name])
        .with_context(|| format!("could not create and checkout branch '{branch_name}'"))?;
    Ok(())
}

/// Return the name of the currently checked-out branch.
pub fn get_current_branch(workdir: &Path) -> Result<String> {
    let out = git(workdir, &["rev-parse", "--abbrev-ref", "HEAD"])
        .context("could not determine current branch")?;
    Ok(out.trim().to_string())
}

/// Return `true` when the working tree or index has uncommitted changes.
pub fn has_uncommitted_changes(workdir: &Path) -> Result<bool> {
    // `git status --porcelain` emits nothing when the tree is clean.
    let out = git(workdir, &["status", "--porcelain"])
        .context("could not check for uncommitted changes")?;
    Ok(!out.trim().is_empty())
}

/// Stage **all** changes (tracked + untracked) and commit with `message`.
///
/// This is a convenience wrapper equivalent to `git add -A && git commit -m <message>`.
pub fn commit_changes(workdir: &Path, message: &str) -> Result<()> {
    git(workdir, &["add", "-A"]).context("git add -A failed")?;
    git(workdir, &["commit", "-m", message]).context("git commit failed")?;
    Ok(())
}

/// Auto-commit any dirty state with a descriptive message.
///
/// If the working tree is clean this is a no-op.
pub fn auto_commit_dirty_state(workdir: &Path, message: &str) -> Result<()> {
    if has_uncommitted_changes(workdir)? {
        commit_changes(workdir, message)?;
    }
    Ok(())
}

/// Return a list of all changed files (staged, unstaged, and untracked).
pub fn get_all_changed_files(workdir: &Path) -> Result<Vec<String>> {
    let out = git(workdir, &["status", "--porcelain"]).context("could not list changed files")?;
    let files: Vec<String> = out
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            // `git status --porcelain` lines look like "XY filename" (3-char prefix).
            l.get(3..).unwrap_or(l).to_string()
        })
        .collect();
    Ok(files)
}

/// Return the unified diff of all current (unstaged + staged) changes.
pub fn get_diff(workdir: &Path) -> Result<String> {
    // Unstaged changes.
    let unstaged = git(workdir, &["diff"]).unwrap_or_default();
    // Staged changes.
    let staged = git(workdir, &["diff", "--cached"]).unwrap_or_default();

    let mut diff = String::new();
    if !unstaged.is_empty() {
        diff.push_str(&unstaged);
    }
    if !staged.is_empty() {
        if !diff.is_empty() {
            diff.push('\n');
        }
        diff.push_str(&staged);
    }
    Ok(diff)
}

/// Hard-reset the working directory to the last commit state.
///
/// Equivalent to `git checkout -- . && git clean -fd`.
pub fn rollback_changes(workdir: &Path) -> Result<()> {
    git(workdir, &["checkout", "--", "."]).context("git checkout -- . failed")?;
    git(workdir, &["clean", "-fd"]).context("git clean -fd failed")?;
    Ok(())
}

/// Return a list of untracked files (respecting .gitignore).
pub fn get_untracked_files(workdir: &Path) -> Result<Vec<String>> {
    let out = git(workdir, &["ls-files", "--others", "--exclude-standard"])
        .context("could not list untracked files")?;
    Ok(out
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect())
}

/// Stage all changes then unstage the specified files.
///
/// This lets us commit new work without accidentally staging pre-existing
/// untracked files that the user had in their working directory.
pub fn stage_except(workdir: &Path, exclude: &[String]) -> Result<()> {
    git(workdir, &["add", "-A"]).context("git add -A failed")?;
    for file in exclude {
        // Unstage the file. Ignore errors (file may not have been staged).
        let _ = git(workdir, &["reset", "HEAD", "--", file]);
    }
    Ok(())
}

/// Commit whatever is currently staged (does not run `git add`).
pub fn commit_staged(workdir: &Path, message: &str) -> Result<()> {
    git(workdir, &["commit", "-m", message]).context("git commit failed")?;
    Ok(())
}

/// Rollback changes while preserving specified untracked files.
///
/// Restores tracked files via `git checkout -- .`, then selectively removes
/// only untracked files that are NOT in the `preserve` list.
pub fn rollback_except(workdir: &Path, preserve: &[String]) -> Result<()> {
    // Restore tracked files.
    git(workdir, &["checkout", "--", "."]).context("git checkout -- . failed")?;

    // Get current untracked files and remove only those not in preserve list.
    let untracked = get_untracked_files(workdir)?;
    for file in &untracked {
        if !preserve.contains(file) {
            let path = workdir.join(file);
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(&path);
            } else {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

/// Return the full SHA of the current HEAD commit.
pub fn get_commit_hash(workdir: &Path) -> Result<String> {
    let out = git(workdir, &["rev-parse", "HEAD"]).context("could not get current commit hash")?;
    Ok(out.trim().to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Helper – create a temporary git repo with an initial commit.
    fn init_repo() -> (TempDir, std::path::PathBuf) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        git(&dir, &["init"]).unwrap();
        git(&dir, &["config", "user.email", "test@test.com"]).unwrap();
        git(&dir, &["config", "user.name", "Test"]).unwrap();

        // Need at least one commit so HEAD exists.
        fs::write(dir.join("README.md"), "# hello").unwrap();
        git(&dir, &["add", "-A"]).unwrap();
        git(&dir, &["commit", "-m", "init"]).unwrap();

        (tmp, dir)
    }

    #[test]
    fn test_get_current_branch() {
        let (_tmp, dir) = init_repo();
        // Default branch may be main or master; just check it's non-empty.
        let branch = get_current_branch(&dir).unwrap();
        assert!(!branch.is_empty());
    }

    #[test]
    fn test_create_and_checkout_branch() {
        let (_tmp, dir) = init_repo();
        create_and_checkout_branch(&dir, "feature/test").unwrap();
        assert_eq!(get_current_branch(&dir).unwrap(), "feature/test");
    }

    #[test]
    fn test_has_uncommitted_changes_clean() {
        let (_tmp, dir) = init_repo();
        assert!(!has_uncommitted_changes(&dir).unwrap());
    }

    #[test]
    fn test_has_uncommitted_changes_dirty() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("new.txt"), "data").unwrap();
        assert!(has_uncommitted_changes(&dir).unwrap());
    }

    #[test]
    fn test_commit_changes() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("file.txt"), "content").unwrap();
        commit_changes(&dir, "add file").unwrap();
        assert!(!has_uncommitted_changes(&dir).unwrap());
    }

    #[test]
    fn test_auto_commit_dirty_state_noop_when_clean() {
        let (_tmp, dir) = init_repo();
        let hash_before = get_commit_hash(&dir).unwrap();
        auto_commit_dirty_state(&dir, "should not commit").unwrap();
        let hash_after = get_commit_hash(&dir).unwrap();
        assert_eq!(hash_before, hash_after);
    }

    #[test]
    fn test_auto_commit_dirty_state_commits_when_dirty() {
        let (_tmp, dir) = init_repo();
        let hash_before = get_commit_hash(&dir).unwrap();
        fs::write(dir.join("dirty.txt"), "stuff").unwrap();
        auto_commit_dirty_state(&dir, "auto save").unwrap();
        let hash_after = get_commit_hash(&dir).unwrap();
        assert_ne!(hash_before, hash_after);
        assert!(!has_uncommitted_changes(&dir).unwrap());
    }

    #[test]
    fn test_get_all_changed_files() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("a.txt"), "a").unwrap();
        fs::write(dir.join("b.txt"), "b").unwrap();
        let files = get_all_changed_files(&dir).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"a.txt".to_string()));
        assert!(files.contains(&"b.txt".to_string()));
    }

    #[test]
    fn test_get_diff() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("README.md"), "# changed").unwrap();
        let diff = get_diff(&dir).unwrap();
        assert!(diff.contains("changed"));
    }

    #[test]
    fn test_rollback_changes() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("README.md"), "overwritten").unwrap();
        fs::write(dir.join("extra.txt"), "extra").unwrap();
        assert!(has_uncommitted_changes(&dir).unwrap());
        rollback_changes(&dir).unwrap();
        assert!(!has_uncommitted_changes(&dir).unwrap());
        // Original file restored.
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# hello"
        );
        // Untracked file removed.
        assert!(!dir.join("extra.txt").exists());
    }

    #[test]
    fn test_get_commit_hash() {
        let (_tmp, dir) = init_repo();
        let hash = get_commit_hash(&dir).unwrap();
        // SHA-1 hex is 40 chars.
        assert!(hash.len() >= 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_git_error_handling() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // Not a git repo – should fail.
        assert!(get_current_branch(&dir).is_err());
    }
}
