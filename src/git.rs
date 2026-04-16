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

/// Parse `git status --porcelain` output into a list of file paths.
///
/// Rename/copy entries (`R` or `C` status in either column) are split on
/// ` -> ` so both the old and new paths are returned as separate entries.
fn parse_porcelain_status(out: &str) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();
    for line in out.lines() {
        if line.is_empty() {
            continue;
        }
        // `git status --porcelain` lines look like "XY filename" (3-char prefix).
        let status = line.get(..2).unwrap_or("");
        let rest = line.get(3..).unwrap_or(line);
        let is_rename_or_copy = status.contains('R') || status.contains('C');
        if is_rename_or_copy {
            if let Some((old, new)) = rest.split_once(" -> ") {
                files.push(old.to_string());
                files.push(new.to_string());
                continue;
            }
        }
        files.push(rest.to_string());
    }
    files
}

/// Return a list of all changed files (staged, unstaged, and untracked).
pub fn get_all_changed_files(workdir: &Path) -> Result<Vec<String>> {
    let out = git(workdir, &["status", "--porcelain"]).context("could not list changed files")?;
    Ok(parse_porcelain_status(&out))
}

/// Return the unified diff of all current (unstaged + staged) changes.
pub fn get_diff(workdir: &Path) -> Result<String> {
    let unstaged = git(workdir, &["diff"]).context("could not get unstaged diff")?;
    let staged = git(workdir, &["diff", "--cached"]).context("could not get staged diff")?;

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
/// Equivalent to `git restore . && git clean -fd`. Requires git >= 2.23.
#[allow(dead_code)]
pub fn rollback_changes(workdir: &Path) -> Result<()> {
    git(workdir, &["restore", "."]).context("git restore . failed")?;
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
        git(workdir, &["reset", "HEAD", "--", file])
            .with_context(|| format!("git reset HEAD -- '{file}' failed"))?;
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
/// Restores tracked files via `git restore .`, then selectively removes
/// only untracked files that are NOT in the `preserve` list. Requires git >= 2.23.
pub fn rollback_except(workdir: &Path, preserve: &[String]) -> Result<()> {
    // Restore tracked files.
    git(workdir, &["restore", "."]).context("git restore . failed")?;

    let untracked = get_untracked_files(workdir)?;
    remove_untracked_except(workdir, preserve, &untracked)
}

/// Remove each path in `untracked` from `workdir` unless it appears in `preserve`.
///
/// Tolerates `NotFound` errors: a file may disappear between listing and
/// deletion (concurrent process, symlink chain, etc.). Other I/O errors are
/// propagated with context.
fn remove_untracked_except(
    workdir: &Path,
    preserve: &[String],
    untracked: &[String],
) -> Result<()> {
    for file in untracked {
        if preserve.contains(file) {
            continue;
        }
        let path = workdir.join(file);
        let result = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        if let Err(err) = result {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err)
                    .with_context(|| format!("failed to remove untracked path '{file}'"));
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

/// Create a new branch rooted at the given SHA and switch to it.
///
/// Equivalent to `git checkout -b <branch_name> <sha>`. Fails if the branch
/// already exists or the SHA is invalid; callers that need a "create-or-check
/// out" semantic should handle that at the call site.
pub fn create_branch_from_sha(workdir: &Path, branch_name: &str, sha: &str) -> Result<()> {
    git(workdir, &["checkout", "-b", branch_name, sha])
        .with_context(|| format!("could not create branch '{branch_name}' rooted at {sha}"))?;
    Ok(())
}

/// Merge the given SHA into the current branch using `git merge --no-ff`.
///
/// Fails if the merge cannot be completed (e.g. due to conflicts); the error
/// message contains the git stderr so callers can surface it to the user.
pub fn merge_sha(workdir: &Path, sha: &str) -> Result<()> {
    git(workdir, &["merge", "--no-ff", sha])
        .with_context(|| format!("could not merge {sha} into current branch"))?;
    Ok(())
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
    fn test_parse_porcelain_status_rename_and_copy() {
        // Simulated `git status --porcelain` output covering:
        //   - plain modifications
        //   - adds
        //   - untracked
        //   - a staged rename (R  old -> new)
        //   - a staged copy    (C  old -> new)
        //   - an unstaged rename where worktree column is R ( R old -> new)
        let lines = [
            " M modified.txt",
            "A  added.txt",
            "?? untracked.txt",
            "R  old_renamed.txt -> new_renamed.txt",
            "C  src_copied.txt -> dst_copied.txt",
            " R wt_old.txt -> wt_new.txt",
        ];
        let out = lines.join("\n") + "\n";
        let files = parse_porcelain_status(&out);
        assert_eq!(
            files,
            vec![
                "modified.txt".to_string(),
                "added.txt".to_string(),
                "untracked.txt".to_string(),
                "old_renamed.txt".to_string(),
                "new_renamed.txt".to_string(),
                "src_copied.txt".to_string(),
                "dst_copied.txt".to_string(),
                "wt_old.txt".to_string(),
                "wt_new.txt".to_string(),
            ]
        );
    }

    #[test]
    fn test_get_diff() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("README.md"), "# changed").unwrap();
        let diff = get_diff(&dir).unwrap();
        assert!(diff.contains("changed"));
    }

    #[test]
    fn test_get_diff_errors_when_not_a_repo() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // Not a git repo — git diff should fail and propagate.
        assert!(get_diff(&dir).is_err());
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

    #[test]
    fn test_create_branch_from_sha() {
        let (_tmp, dir) = init_repo();
        let initial_sha = get_commit_hash(&dir).unwrap();

        // Make a second commit on the default branch so we have history.
        fs::write(dir.join("second.txt"), "second").unwrap();
        commit_changes(&dir, "second").unwrap();
        let second_sha = get_commit_hash(&dir).unwrap();
        assert_ne!(initial_sha, second_sha);

        // Create a branch rooted at the initial SHA.
        create_branch_from_sha(&dir, "feature/from-initial", &initial_sha).unwrap();

        // We should now be on the new branch.
        assert_eq!(get_current_branch(&dir).unwrap(), "feature/from-initial");
        // And HEAD should match the initial SHA.
        assert_eq!(get_commit_hash(&dir).unwrap(), initial_sha);
        // The second commit's file should not exist in this branch.
        assert!(!dir.join("second.txt").exists());
    }

    #[test]
    fn test_merge_sha_clean() {
        let (_tmp, dir) = init_repo();
        let base_sha = get_commit_hash(&dir).unwrap();

        // Create branch A off base and add a file.
        create_branch_from_sha(&dir, "branch-a", &base_sha).unwrap();
        fs::write(dir.join("a.txt"), "a").unwrap();
        commit_changes(&dir, "a change").unwrap();
        let a_sha = get_commit_hash(&dir).unwrap();

        // Create branch B off base and add a different file.
        create_branch_from_sha(&dir, "branch-b", &base_sha).unwrap();
        fs::write(dir.join("b.txt"), "b").unwrap();
        commit_changes(&dir, "b change").unwrap();

        // Merge A into B — should succeed cleanly.
        merge_sha(&dir, &a_sha).unwrap();

        // Both files should now be present.
        assert!(dir.join("a.txt").exists());
        assert!(dir.join("b.txt").exists());
    }

    #[test]
    fn test_remove_untracked_except_tolerates_missing() {
        let (_tmp, dir) = init_repo();
        // "exists.txt" is on disk; "gone.txt" is only in the list (simulating
        // a file that disappeared between listing and deletion).
        fs::write(dir.join("exists.txt"), "data").unwrap();
        let untracked = vec!["exists.txt".to_string(), "gone.txt".to_string()];
        remove_untracked_except(&dir, &[], &untracked).unwrap();
        assert!(!dir.join("exists.txt").exists());
        assert!(!dir.join("gone.txt").exists());
    }

    #[test]
    fn test_remove_untracked_except_preserves_list() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("keep.txt"), "k").unwrap();
        fs::write(dir.join("drop.txt"), "d").unwrap();
        let untracked = vec!["keep.txt".to_string(), "drop.txt".to_string()];
        let preserve = vec!["keep.txt".to_string()];
        remove_untracked_except(&dir, &preserve, &untracked).unwrap();
        assert!(dir.join("keep.txt").exists());
        assert!(!dir.join("drop.txt").exists());
    }

    #[test]
    fn test_stage_except_unstages_excluded_files() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("keep.txt"), "k").unwrap();
        fs::write(dir.join("drop.txt"), "d").unwrap();

        stage_except(&dir, &["drop.txt".to_string()]).unwrap();

        // keep.txt should be staged; drop.txt should remain untracked.
        let status = git(&dir, &["status", "--porcelain"]).unwrap();
        assert!(status.contains("A  keep.txt"));
        assert!(status.contains("?? drop.txt"));
    }

    #[test]
    fn test_stage_except_tolerates_unstaged_file_in_exclude_list() {
        // `git reset HEAD -- <path>` is a no-op (exit 0) for paths that are
        // not currently staged, so excluding a file that was never staged
        // must not produce an error.
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("file.txt"), "data").unwrap();
        stage_except(&dir, &["never_staged.txt".to_string()]).unwrap();
    }

    #[test]
    fn test_stage_except_propagates_reset_errors() {
        // When the underlying `git reset` fails (e.g. not a git repo), the
        // error must surface rather than being swallowed.
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let result = stage_except(&dir, &["file.txt".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_sha_conflict() {
        let (_tmp, dir) = init_repo();
        let base_sha = get_commit_hash(&dir).unwrap();

        // Branch A modifies README.md one way.
        create_branch_from_sha(&dir, "branch-a", &base_sha).unwrap();
        fs::write(dir.join("README.md"), "# version A").unwrap();
        commit_changes(&dir, "a version").unwrap();
        let a_sha = get_commit_hash(&dir).unwrap();

        // Branch B modifies README.md a different way.
        create_branch_from_sha(&dir, "branch-b", &base_sha).unwrap();
        fs::write(dir.join("README.md"), "# version B").unwrap();
        commit_changes(&dir, "b version").unwrap();

        // Merging A into B should fail with conflicts.
        let result = merge_sha(&dir, &a_sha);
        assert!(result.is_err());
    }
}
