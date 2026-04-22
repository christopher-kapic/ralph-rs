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
#[allow(dead_code)]
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

/// Plain `git checkout <branch>` — does NOT create the branch.
///
/// Used by the run-teardown path to switch back to the source branch before
/// popping the ralph-owned stash.
pub fn checkout_branch(workdir: &Path, branch: &str) -> Result<()> {
    git(workdir, &["checkout", branch])
        .with_context(|| format!("could not checkout branch '{branch}'"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Stash helpers
// ---------------------------------------------------------------------------

/// Stable identifier for a stash created by ralph.
///
/// Wraps the stash's **commit SHA** (the `W` commit, not the `stash@{N}`
/// reference) because `stash@{N}` shifts whenever the user creates or drops
/// another stash during a run. The SHA is stable for the lifetime of the
/// stash. Compare for equality, don't parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StashRef(pub String);

impl StashRef {
    /// The underlying commit SHA.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Terminal outcome of popping a ralph-owned stash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StashPopOutcome {
    /// Stash applied and was dropped from the stack.
    Clean,
    /// `git stash pop` exited non-zero or left conflict markers. The stash
    /// was NOT dropped; the SHA is still valid and the user can pop it
    /// manually after resolving. The `String` carries the git stderr so
    /// callers can surface it.
    Conflicted(String),
    /// The stash SHA no longer exists in the stash list (e.g. the user
    /// dropped it manually between push and pop).
    NotFound,
}

/// `git stash push --include-untracked -m <message>`.
///
/// Returns:
/// - `Ok(Some(stash_ref))` when something was stashed. The SHA is the `W`
///   commit of the new stash entry, captured immediately by grepping `git
///   stash list` for `message`.
/// - `Ok(None)` when the tree was clean and git reported "No local changes
///   to save" — there's nothing to pop later.
/// - `Err(_)` when `git stash push` itself failed for any reason other than
///   a clean tree (e.g. not a git repo, permission error).
pub fn stash_push_with_untracked(workdir: &Path, message: &str) -> Result<Option<StashRef>> {
    // `git stash push` on a clean tree exits 0 with "No local changes to
    // save" on stdout — we have to distinguish that case from a real stash.
    // Rather than string-match stdout (brittle across locales), we ask git
    // for the pre-push stash list, push, and diff.
    let before = stash_list_shas(workdir)?;

    let output = Command::new("git")
        .args(["stash", "push", "--include-untracked", "-m", message])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git stash push -m '{message}'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "git stash push failed (exit {}): {}",
            output.status,
            stderr.trim()
        );
    }

    // Match by message against the post-push list. If nothing was pushed
    // (clean tree), the match will find no new stash and we return None.
    // If something was pushed, the new stash's SHA is one of (after -
    // before) and its subject matches `message`.
    let after = stash_list_shas_with_subjects(workdir)?;
    for (sha, subject) in &after {
        if !before.contains(sha) && subject_matches(subject, message) {
            return Ok(Some(StashRef(sha.clone())));
        }
    }
    // No new stash -> tree was clean.
    Ok(None)
}

/// Pop the stash identified by `stash_ref`.
///
/// Implementation note: `git stash pop <sha>` doesn't exist directly —
/// `pop` resolves its argument via `git stash apply` semantics, which do
/// accept a commit SHA but don't drop it. We therefore run `apply <sha>`
/// followed by `drop <stash@{N}>` where N is resolved from the current
/// stash list. On apply conflict we skip the drop so the user's stash
/// survives for manual recovery.
pub fn stash_pop(workdir: &Path, stash_ref: &StashRef) -> Result<StashPopOutcome> {
    // 1. Locate the stash@{N} entry whose commit SHA matches ours. If it's
    //    gone, the user already dropped it.
    let entries = stash_list_shas_with_refs(workdir)?;
    let stash_ref_name = match entries.iter().find(|(sha, _)| sha == stash_ref.as_str()) {
        Some((_, name)) => name.clone(),
        None => return Ok(StashPopOutcome::NotFound),
    };

    // 2. Apply the stash by its commit SHA. This lets us be robust to
    //    other stashes being pushed/popped between our push and pop — we
    //    always apply exactly the commit we created.
    let apply = Command::new("git")
        .args(["stash", "apply", stash_ref.as_str()])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git stash apply {}", stash_ref.as_str()))?;

    if !apply.status.success() {
        let stderr = String::from_utf8_lossy(&apply.stderr).to_string();
        return Ok(StashPopOutcome::Conflicted(stderr.trim().to_string()));
    }

    // `git stash apply` can exit 0 even when it wrote conflict markers —
    // check the worktree for unmerged entries and refuse to drop if we
    // find any.
    let status_out = git(workdir, &["status", "--porcelain"])
        .context("could not check git status after stash apply")?;
    if has_conflict_marker(&status_out) {
        return Ok(StashPopOutcome::Conflicted(
            "conflict markers present after stash apply; not dropping".to_string(),
        ));
    }

    // 3. Drop the named stash ref now that it's safely applied.
    let drop_out = Command::new("git")
        .args(["stash", "drop", &stash_ref_name])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git stash drop {stash_ref_name}"))?;

    if !drop_out.status.success() {
        let stderr = String::from_utf8_lossy(&drop_out.stderr);
        bail!(
            "stash apply succeeded but drop failed ({}): {} (manual: git stash list / git stash drop {})",
            drop_out.status,
            stderr.trim(),
            stash_ref_name,
        );
    }

    Ok(StashPopOutcome::Clean)
}

/// Find a stash (by its commit SHA) whose subject contains `message`.
///
/// Returns `None` if no stash matches. Used by recovery paths that want to
/// locate a ralph-owned stash without needing the SHA.
#[allow(dead_code)]
pub fn find_stash_by_message(workdir: &Path, message: &str) -> Result<Option<StashRef>> {
    let entries = stash_list_shas_with_subjects(workdir)?;
    for (sha, subject) in entries {
        if subject_matches(&subject, message) {
            return Ok(Some(StashRef(sha)));
        }
    }
    Ok(None)
}

/// Return the set of stash commit SHAs currently on the stack.
fn stash_list_shas(workdir: &Path) -> Result<Vec<String>> {
    let out = git(workdir, &["stash", "list", "--format=%H"])
        .context("could not list git stash entries")?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect())
}

/// Return (sha, subject) pairs for each stash entry, in stack order.
fn stash_list_shas_with_subjects(workdir: &Path) -> Result<Vec<(String, String)>> {
    // `%H` = full SHA, `%gs` = reflog subject ("On branch: message"). Tab
    // separator keeps it robust even if the message contains whitespace.
    let out = git(workdir, &["stash", "list", "--format=%H%x09%gs"])
        .context("could not list git stash entries")?;
    let mut entries = Vec::new();
    for line in out.lines() {
        if let Some((sha, subj)) = line.split_once('\t') {
            entries.push((sha.to_string(), subj.to_string()));
        }
    }
    Ok(entries)
}

/// Return (sha, stash@{N}) pairs for each stash entry. Used by `stash_pop`
/// to resolve the named ref that `git stash drop` requires.
fn stash_list_shas_with_refs(workdir: &Path) -> Result<Vec<(String, String)>> {
    let out = git(workdir, &["stash", "list", "--format=%H%x09%gd"])
        .context("could not list git stash entries")?;
    let mut entries = Vec::new();
    for line in out.lines() {
        if let Some((sha, name)) = line.split_once('\t') {
            entries.push((sha.to_string(), name.to_string()));
        }
    }
    Ok(entries)
}

/// A stash reflog subject looks like `On master: ralph: auto-stash for plan 'x' at ...`.
/// Our caller passes in the exact message substring; we match by `contains`
/// so the branch-prefix doesn't defeat the lookup.
fn subject_matches(subject: &str, message: &str) -> bool {
    subject.contains(message)
}

/// `git status --porcelain` marks unmerged paths with an XY prefix where one
/// of X/Y is 'U' (or both letters are the same non-space — e.g. `DD`, `AA`).
/// Those signal conflict markers. Returns true if any such line is present.
fn has_conflict_marker(porcelain_out: &str) -> bool {
    for line in porcelain_out.lines() {
        let prefix = line.get(..2).unwrap_or("");
        let mut chars = prefix.chars();
        let x = chars.next().unwrap_or(' ');
        let y = chars.next().unwrap_or(' ');
        if x == 'U' || y == 'U' {
            return true;
        }
        if x != ' ' && y != ' ' && x == y && matches!(x, 'A' | 'D') {
            return true;
        }
    }
    false
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

    // ----- stash helpers -----

    #[test]
    fn test_stash_push_clean_tree_returns_none() {
        let (_tmp, dir) = init_repo();
        let result = stash_push_with_untracked(&dir, "ralph: test stash on clean tree").unwrap();
        assert!(
            result.is_none(),
            "clean tree should produce no stash, got {result:?}"
        );
    }

    #[test]
    fn test_stash_push_dirty_tree_returns_sha_and_message() {
        let (_tmp, dir) = init_repo();
        // Tracked modification + an untracked file — the --include-untracked
        // flag must pick both up.
        fs::write(dir.join("README.md"), "# modified").unwrap();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();
        assert!(has_uncommitted_changes(&dir).unwrap());

        let msg = "ralph: auto-stash for plan 'demo' at 2026-04-22T00:00:00Z";
        let stash = stash_push_with_untracked(&dir, msg).unwrap().expect("sha");
        // SHA-like shape.
        assert_eq!(stash.as_str().len(), 40);
        assert!(stash.as_str().chars().all(|c| c.is_ascii_hexdigit()));

        // The stash was pushed and the tree is now clean.
        assert!(!has_uncommitted_changes(&dir).unwrap());

        // find_stash_by_message should locate our stash by substring match.
        let found = find_stash_by_message(&dir, msg).unwrap().expect("found");
        assert_eq!(found, stash);
    }

    #[test]
    fn test_stash_pop_clean() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();
        fs::write(dir.join("README.md"), "# modified").unwrap();

        let msg = "ralph: pop-round-trip test";
        let stash = stash_push_with_untracked(&dir, msg).unwrap().expect("sha");

        // Tree is clean post-stash, and original tracked file is reverted.
        assert!(!has_uncommitted_changes(&dir).unwrap());
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# hello"
        );
        assert!(!dir.join("scratch.txt").exists());

        // Pop restores both.
        let outcome = stash_pop(&dir, &stash).unwrap();
        assert_eq!(outcome, StashPopOutcome::Clean);
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# modified"
        );
        assert_eq!(fs::read_to_string(dir.join("scratch.txt")).unwrap(), "wip");

        // Stash is gone from the stack.
        let after = find_stash_by_message(&dir, msg).unwrap();
        assert!(after.is_none());
    }

    #[test]
    fn test_stash_pop_conflict_leaves_stash_intact() {
        let (_tmp, dir) = init_repo();

        // Write version A to README and stash it.
        fs::write(dir.join("README.md"), "# version A\n").unwrap();
        let msg = "ralph: conflict test stash";
        let stash = stash_push_with_untracked(&dir, msg).unwrap().expect("sha");

        // Now commit a DIFFERENT change to README so the stashed version
        // will conflict on pop.
        fs::write(dir.join("README.md"), "# version B\n").unwrap();
        commit_changes(&dir, "divergent").unwrap();

        // Pop must report a conflict.
        let outcome = stash_pop(&dir, &stash).unwrap();
        assert!(
            matches!(outcome, StashPopOutcome::Conflicted(_)),
            "expected Conflicted, got {outcome:?}"
        );

        // The stash must still be on the stack so the user can recover.
        let still_there = find_stash_by_message(&dir, msg).unwrap();
        assert_eq!(still_there, Some(stash));
    }

    #[test]
    fn test_find_stash_by_message_matches() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("a.txt"), "a").unwrap();
        let msg = "ralph: specific-marker-7f3";
        let stash = stash_push_with_untracked(&dir, msg).unwrap().expect("sha");

        let found = find_stash_by_message(&dir, msg).unwrap().expect("found");
        assert_eq!(found, stash);

        let missing = find_stash_by_message(&dir, "ralph: no-such-marker").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_stash_pop_not_found_when_dropped() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("a.txt"), "a").unwrap();
        let stash = stash_push_with_untracked(&dir, "ralph: gone")
            .unwrap()
            .expect("sha");

        // User drops it manually.
        let _ = Command::new("git")
            .args(["stash", "drop", "stash@{0}"])
            .current_dir(&dir)
            .output()
            .unwrap();

        let outcome = stash_pop(&dir, &stash).unwrap();
        assert_eq!(outcome, StashPopOutcome::NotFound);
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
