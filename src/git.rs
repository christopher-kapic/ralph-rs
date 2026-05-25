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

/// Run a git command in `workdir` and return its **raw** stdout bytes on
/// success.
///
/// Mirrors [`git`]'s error handling (non-zero exit → `bail!` with the
/// trimmed stderr) but does **not** lossily UTF-8-decode stdout. Used for
/// `git status --porcelain=v1 -z`, whose NUL-delimited records can carry
/// non-UTF8 path bytes that the lossy [`git`] helper would silently corrupt
/// (and, worse, whose replacement characters could change how a record is
/// split). Path bytes are converted to `String` only at the parse boundary
/// (`String::from_utf8_lossy`), matching how the rest of the codebase models
/// paths as `String` — truly non-UTF8 paths are still best-effort, but they
/// are no longer silently merged or mis-split.
fn git_bytes(workdir: &Path, args: &[&str]) -> Result<Vec<u8>> {
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

    Ok(output.stdout)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate that `branch_name` is a syntactically legal git branch name.
///
/// This is a **pure, deterministic** reimplementation of the rules in
/// git-check-ref-format(1), applied to the full ref `refs/heads/<branch>` —
/// exactly the decision `git check-ref-format refs/heads/<name>` (no
/// `--allow-onelevel`) would make. It deliberately does **not** spawn a
/// subprocess: a validation gate that runs before any DB write must not
/// depend on `git` being on `PATH`, on process-spawn succeeding, or on an
/// unbounded external command — it has to be fast, total, and byte-identical
/// on every machine. Git's own check still runs *authoritatively* at
/// branch-creation time (`create_and_checkout_branch` /
/// `create_branch_from_sha`), so git remains the final arbiter and any
/// conceivable divergence on an exotic name is still caught there as
/// defense-in-depth; this function's job is to reject the clearly-invalid
/// cases up front with an actionable, machine-independent message.
///
/// On any rejection this `bail!`s with a message naming the offending branch.
pub fn check_ref_format(branch_name: &str) -> Result<()> {
    if branch_name.trim().is_empty() {
        bail!(
            "invalid branch name: name is empty or whitespace-only \
             (got {branch_name:?})"
        );
    }

    // The git-check-ref-format(1) refname rules ACCEPT a leading dash, but a
    // leading-dash branch is still hazardous: it is later passed as a bare
    // argument to `git checkout -b <name>` where it would be misparsed as a
    // flag. Reject it explicitly so the failure is caught up front with a
    // clear message rather than as a confusing git CLI error later.
    if branch_name.starts_with('-') {
        bail!(
            "invalid branch name '{branch_name}': must not start with '-' \
             (it would be misinterpreted as a git command-line flag)"
        );
    }

    if let Err(rule) = validate_refname(&format!("refs/heads/{branch_name}")) {
        bail!("invalid branch name '{branch_name}': {rule}");
    }
    Ok(())
}

/// Pure validator for the git-check-ref-format(1) refname rules. `refname` is
/// the FULL ref (e.g. `refs/heads/<branch>`), so rule 2 ("must contain at
/// least one `/`") is always satisfied by the `refs/heads/` prefix — matching
/// `git check-ref-format refs/heads/<name>` without `--allow-onelevel`.
/// Returns the human-readable rule that was violated, or `Ok(())`.
///
/// Encodes every rule from git-check-ref-format(1) verbatim so it cannot
/// silently drift: whole-name shape (rules 6/7/9 and the `.lock`/`@{`/`..`
/// sequence rules), the forbidden character set (rules 4/5/10), and the
/// per-`/`-component rules (rule 1).
fn validate_refname(refname: &str) -> Result<(), &'static str> {
    // Whole-name shape rules.
    if refname == "@" {
        return Err("a ref cannot be the single character '@'");
    }
    if refname.starts_with('/') || refname.ends_with('/') {
        return Err("a ref cannot begin or end with '/'");
    }
    if refname.ends_with('.') {
        return Err("a ref cannot end with '.'");
    }
    if refname.contains("..") {
        return Err("a ref cannot contain two consecutive dots '..'");
    }
    if refname.contains("//") {
        return Err("a ref cannot contain consecutive slashes '//'");
    }
    if refname.contains("@{") {
        return Err("a ref cannot contain the sequence '@{'");
    }

    // Forbidden characters anywhere (rules 4/5/10): ASCII control (< 0x20)
    // and DEL (0x7f), space, ~ ^ : ? * [ and backslash.
    for ch in refname.chars() {
        if (ch as u32) < 0x20 || ch == '\u{7f}' {
            return Err("a ref cannot contain ASCII control characters");
        }
        match ch {
            ' ' => return Err("a ref cannot contain a space"),
            '~' | '^' | ':' => return Err("a ref cannot contain any of '~', '^', ':'"),
            '?' | '*' | '[' => return Err("a ref cannot contain any of '?', '*', '['"),
            '\\' => return Err("a ref cannot contain a backslash"),
            _ => {}
        }
    }

    // Per-slash-component rules (rule 1): no component may begin with '.' or
    // end with '.lock'. This must check EVERY component, not just the last
    // (e.g. `foo.lock/bar` is invalid), which the whole-name checks miss.
    for component in refname.split('/') {
        if component.starts_with('.') {
            return Err("no slash-separated ref component may begin with '.'");
        }
        if component.ends_with(".lock") {
            return Err("no slash-separated ref component may end with '.lock'");
        }
    }

    Ok(())
}

/// Create a new branch and switch to it.
pub fn create_and_checkout_branch(workdir: &Path, branch_name: &str) -> Result<()> {
    git(workdir, &["checkout", "-b", branch_name])
        .with_context(|| format!("could not create and checkout branch '{branch_name}'"))?;
    Ok(())
}

/// Check out an existing branch. Fails if the branch doesn't exist.
pub fn checkout_branch(workdir: &Path, branch_name: &str) -> Result<()> {
    git(workdir, &["checkout", branch_name])
        .with_context(|| format!("could not checkout branch '{branch_name}'"))?;
    Ok(())
}

/// Return `true` if a local branch with the given name exists.
pub fn branch_exists(workdir: &Path, branch_name: &str) -> Result<bool> {
    let output = Command::new("git")
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch_name}"),
        ])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git show-ref for '{branch_name}'"))?;
    Ok(output.status.success())
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

/// Parse `git status --porcelain=v1 -z` output into a list of file paths.
///
/// **Why `-z`:** the human-readable `--porcelain` (v1, no `-z`) C-quotes any
/// path containing "unusual" bytes — newlines, double-quotes, backslashes,
/// non-ASCII — wrapping it in double quotes with backslash escapes, and uses
/// a literal ` -> ` separator for renames/copies. A path that legitimately
/// contains those bytes would then be mis-quoted or mis-split by a naive
/// line/`" -> "` parser. With `-z`, git emits paths **raw** (never quoted)
/// and delimits every record with a NUL. Each record is `XY <path>` followed
/// by `\0`. For a rename/copy (`R` or `C` in either status column) the record
/// is followed by a **second** NUL-terminated record containing the
/// **original** path: `R  <new>\0<orig>\0`. We consume that following record
/// and push the old path *then* the new path (preserving this function's
/// existing returned-order contract: old before new).
///
/// `from_utf8_lossy` is applied only at this boundary; see [`git_bytes`].
///
/// **This is a total parser: any deviation from the porcelain-v1 `-z`
/// protocol is a hard error, never a silent best-effort guess.** The returned
/// list drives the skip/rollback preservation paths (`rollback_except`,
/// `restage_files`, `get_all_changed_files` callers): a *wrong* list there
/// means deleting a file the user wanted kept, or failing to restore one — so
/// a malformed/truncated stream MUST abort the operation rather than act on a
/// corrupt view. The only deviations git can legitimately produce are none;
/// every error arm below is an impossible-unless-git-or-the-pipe-broke case,
/// and in exactly those cases we refuse to proceed.
fn parse_porcelain_status_z(out: &[u8]) -> Result<Vec<String>> {
    let mut files: Vec<String> = Vec::new();
    // Split on NUL. A well-formed stream ends with a trailing NUL, so the
    // final split element is an empty slice; skip empties.
    let mut records = out.split(|&b| b == 0u8).filter(|r| !r.is_empty());

    while let Some(record) = records.next() {
        // A record is exactly `XY <path>`: a 2-char status column, a single
        // space at index 2, then the raw (never-quoted, under `-z`) path.
        // Anything shorter, or without the space delimiter, is not porcelain
        // v1 output — refuse rather than mis-slice.
        if record.len() < 4 || record[2] != b' ' {
            bail!(
                "malformed `git status --porcelain=v1 -z` record (expected \
                 `XY <path>`, got {} byte(s)): refusing to act on an \
                 unparseable status",
                record.len()
            );
        }
        let status = &record[..2];
        let path = &record[3..];
        let is_rename_or_copy = status.contains(&b'R') || status.contains(&b'C');
        if is_rename_or_copy {
            // A rename/copy is TWO records: `XY <new>\0<orig>\0`. The original
            // path MUST follow; its absence means a truncated stream. Acting
            // on just the new path here would silently drop the old path from
            // the preserve/rollback set — exactly the data-loss class this
            // total parser exists to prevent.
            let Some(orig) = records.next() else {
                bail!(
                    "truncated `git status --porcelain=v1 -z`: rename/copy \
                     record for {:?} has no following original-path record; \
                     refusing to act on an incomplete changed-file list",
                    String::from_utf8_lossy(path)
                );
            };
            // Order contract: old (original) then new, matching the prior
            // ` -> ` (old -> new) behavior every caller/test depends on.
            files.push(String::from_utf8_lossy(orig).into_owned());
            files.push(String::from_utf8_lossy(path).into_owned());
            continue;
        }
        files.push(String::from_utf8_lossy(path).into_owned());
    }
    Ok(files)
}

/// Return a list of all changed files (staged, unstaged, and untracked).
///
/// Propagates a hard error if git's porcelain output is malformed/truncated
/// (see [`parse_porcelain_status_z`]) — callers in the skip/rollback path
/// must abort rather than preserve/delete files off a corrupt view.
pub fn get_all_changed_files(workdir: &Path) -> Result<Vec<String>> {
    let out = git_bytes(workdir, &["status", "--porcelain=v1", "-z"])
        .context("could not list changed files")?;
    parse_porcelain_status_z(&out).context("could not parse `git status` output")
}

/// Return a list of paths that currently have **staged** changes (the index
/// differs from HEAD). Captured before [`stash_push_with_untracked`] so that
/// after `stash pop` (which always restores everything as unstaged) we can
/// re-stage exactly the files the user had staged before the run.
///
/// Uses `git diff --name-only --cached` rather than parsing porcelain so we
/// only get index-vs-HEAD differences and don't have to disambiguate the
/// per-file XY status codes.
pub fn list_staged_files(workdir: &Path) -> Result<Vec<String>> {
    let out = git(workdir, &["diff", "--name-only", "--cached"])
        .context("could not list staged files")?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Re-stage `paths` after a stash pop. Best-effort: the user's working tree
/// is already restored at this point, so a `git add` failure (typically a
/// path that no longer exists because the user reshuffled the worktree
/// mid-run) is logged and swallowed rather than propagated. The signature
/// reflects that — there is no error path the caller can act on.
///
/// `git add` accepts globs and pathspec magic; we pass `--` as a sentinel so
/// the caller's literal paths are interpreted as filenames even if they
/// happen to start with a dash.
pub fn restage_files(workdir: &Path, paths: &[String]) {
    if paths.is_empty() {
        return;
    }
    let mut args: Vec<&str> = vec!["add", "--"];
    args.extend(paths.iter().map(String::as_str));
    if let Err(e) = git(workdir, &args) {
        eprintln!("Warning: re-staging files after stash pop failed: {e}");
    }
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

/// Mixed-reset HEAD back to `sha`, **keeping the working tree intact**.
///
/// `git reset --mixed <sha>` moves the branch ref and unstages the index but
/// leaves every file on disk exactly as it was. Used by the executor's
/// `RetryStrategy::Keep` path when a prior attempt's agent committed on its
/// own: we un-commit that orphan commit so it can't become a second,
/// duplicate step commit, while still carrying the agent's work forward as
/// uncommitted changes (Keep's contract). A later successful attempt then
/// produces exactly one coherent `ralph:` step commit.
pub fn reset_mixed_to(workdir: &Path, sha: &str) -> Result<()> {
    git(workdir, &["reset", "--mixed", sha])
        .with_context(|| format!("git reset --mixed {sha} failed"))?;
    Ok(())
}

/// Rollback changes while preserving specified untracked files.
///
/// Unstages the index back to HEAD, restores tracked files via
/// `git restore .`, then selectively removes only untracked files that are
/// NOT in the `preserve` list. Requires git >= 2.23.
pub fn rollback_except(workdir: &Path, preserve: &[String]) -> Result<()> {
    // Unstage everything first (index → HEAD; the working tree is left
    // alone by `git reset`). Without this, a file the harness *created and
    // `git add`-ed* stays in the index: `git restore .` only syncs the
    // worktree from the index (so it keeps that file), and
    // `git ls-files --others` excludes staged paths (so the cleanup below
    // misses it) — the new file would survive a Discard/Cancel rollback.
    // After the reset that path is untracked (cleaned below, unless
    // preserved), and staged modifications to tracked files become unstaged
    // so the following `git restore .` reverts them to HEAD. Pre-existing
    // untracked files were never staged, so the reset doesn't change their
    // status and `preserve` still protects them.
    git(workdir, &["reset", "-q", "HEAD"]).context("git reset -q HEAD failed")?;

    // Restore tracked files to HEAD content.
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
        if let Err(err) = result
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).with_context(|| format!("failed to remove untracked path '{file}'"));
        }
    }
    Ok(())
}

/// Return the full SHA of the current HEAD commit.
pub fn get_commit_hash(workdir: &Path) -> Result<String> {
    let out = git(workdir, &["rev-parse", "HEAD"]).context("could not get current commit hash")?;
    Ok(out.trim().to_string())
}

/// Abbreviate a commit SHA to git's short form (`git rev-parse --short`).
///
/// Used by the read-only reviewer prompt (docs/dag-redesign.md §8) so the
/// `<short_id>.<n>` framing in the instruction reads against a human-sized
/// commit handle rather than a 40-char hash. Falls back to the first 12
/// chars of `sha` if `git` can't resolve it (e.g. a synthetic SHA in a unit
/// test) — purely cosmetic, never load-bearing.
pub fn short_sha(workdir: &Path, sha: &str) -> String {
    match git(workdir, &["rev-parse", "--short", sha]) {
        Ok(out) => out.trim().to_string(),
        Err(_) => sha.chars().take(12).collect(),
    }
}

/// True iff `sha` is an ancestor of (or equal to) the current `HEAD`
/// commit (`git merge-base --is-ancestor <sha> HEAD`).
///
/// Used by [`crate::review::ReviewTreeGuard`] to PROVE the §9-inv-2
/// read-only-review invariant in a way that is **sound under genuine
/// concurrency**. A review of step A runs while the next *unrelated*
/// implementation (step B) legitimately commits ON TOP of the branch,
/// advancing `HEAD` (the accepted §5 linear-history entanglement). That
/// keeps the reviewed commit an ancestor of `HEAD`, so this stays `true`
/// — no false positive. A *tampering* reviewer that checks out / resets /
/// `commit --amend`s / rebases the line containing the reviewed commit
/// removes it from `HEAD`'s ancestry, so this flips to `false` and the
/// review is rejected. (Pinning the reviewed commit's own object id is
/// useless here: git keeps an amended/orphaned commit reachable *by its
/// SHA* until GC, so `rev-parse <sha>` is a tautology — ancestry-from-HEAD
/// is the property that actually distinguishes tampering from a concurrent
/// forward commit.) `Ok(false)` if `sha` does not resolve at all (a
/// reviewer GC'd / rewrote it away).
pub fn is_ancestor_of_head(workdir: &Path, sha: &str) -> Result<bool> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", sha, "HEAD"])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("could not check ancestry of {sha} vs HEAD"))?;
    // `--is-ancestor` exits 0 = ancestor, 1 = not, other = error (e.g. a
    // bad/orphaned rev). Anything other than a clean 0 ⇒ not reachable.
    Ok(status.status.success())
}

/// Return the unified diff **introduced by a single commit** —
/// `git show <sha>` restricted to the patch with no pager/color.
///
/// This is the O(1) reviewer-diff primitive (docs/dag-redesign.md §4/§8,
/// Decision 5): the reviewer is shown *exactly one* commit's change, never a
/// cumulative range diff (`a..b`) and never a dependency's diff — so a
/// step's review cost does not grow with the depth or width of the branch
/// above it. The format is fixed to `--format=` (empty) + `--patch` so the
/// output is *only* the diff hunks (no commit-header noise that could be
/// mistaken for a second diff), keeping the §8/Decision-5 guarantee
/// machine-checkable.
pub fn show_commit_diff(workdir: &Path, sha: &str) -> Result<String> {
    git(
        workdir,
        &[
            "-c",
            "core.pager=cat",
            "show",
            "--no-color",
            "--format=",
            "--patch",
            sha,
        ],
    )
    .with_context(|| format!("could not `git show` commit {sha}"))
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

/// Drop the stash identified by `stash_ref` without applying it.
///
/// Used when a parked step worktree is being intentionally discarded
/// (skip/reset/terminal fail). Returns `Ok(true)` if a stash entry was found
/// and dropped, `Ok(false)` if it was already gone.
pub fn drop_stash(workdir: &Path, stash_ref: &StashRef) -> Result<bool> {
    let entries = stash_list_shas_with_refs(workdir)?;
    let stash_ref_name = match entries.iter().find(|(sha, _)| sha == stash_ref.as_str()) {
        Some((_, name)) => name.clone(),
        None => return Ok(false),
    };

    let drop_out = Command::new("git")
        .args(["stash", "drop", &stash_ref_name])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git stash drop {stash_ref_name}"))?;

    if !drop_out.status.success() {
        let stderr = String::from_utf8_lossy(&drop_out.stderr);
        bail!(
            "git stash drop {} failed ({}): {}",
            stash_ref_name,
            drop_out.status,
            stderr.trim(),
        );
    }

    Ok(true)
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
// Parking changes (skip handling)
// ---------------------------------------------------------------------------

/// The *kind* of [`ParkStrategy`] without its per-step payload (label /
/// subject). `Copy`, so it can ride through the cancel registry alongside
/// [`crate::signal::CancelReason`]: the skip command only knows the user's
/// `--changes` choice; the executor reconstitutes the full [`ParkStrategy`]
/// from the skipped step's identity at park time.
///
/// [`ParkStrategyKind::Cancel`] is **not** a park strategy at all — it's the
/// TUI skip dialog's Esc/cancel signal threaded through the same registry
/// slot (step 18). When the executor consumes it in `finalize_skipped`, it
/// rolls back the killed harness's work, emits an `attempt_cancelled` NDJSON
/// event, writes **no** `execution_logs` row, and re-enters the retry loop at
/// the *same* attempt number so the cancelled attempt consumes no retry
/// budget. It never reaches [`park_changes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkStrategyKind {
    Stash,
    Commit,
    Discard,
    /// TUI-only: the user pressed Esc on the skip dialog. See the type doc.
    Cancel,
}

impl ParkStrategyKind {
    /// Stable lowercase token used to serialize the kind into
    /// `plans.skip_changes` for the cross-process skip bridge. Round-trips
    /// with [`ParkStrategyKind::from_token`].
    pub fn as_token(&self) -> &'static str {
        match self {
            ParkStrategyKind::Stash => "stash",
            ParkStrategyKind::Commit => "commit",
            ParkStrategyKind::Discard => "discard",
            ParkStrategyKind::Cancel => "cancel",
        }
    }

    /// Parse a token written by [`ParkStrategyKind::as_token`]. An
    /// unrecognized value resolves to `Stash` so a corrupt/forward-compat
    /// `skip_changes` value can never make a skip silently destroy work.
    pub fn from_token(s: &str) -> ParkStrategyKind {
        match s {
            "commit" => ParkStrategyKind::Commit,
            "discard" => ParkStrategyKind::Discard,
            "cancel" => ParkStrategyKind::Cancel,
            // "stash" and anything unexpected → the non-destructive default.
            _ => ParkStrategyKind::Stash,
        }
    }
}

/// How [`park_changes`] should dispose of the working-tree changes left
/// behind when a step is skipped mid-run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // wired into the skip flow in a later step
pub enum ParkStrategy {
    /// `git stash push --include-untracked -m <label>` — recoverable later.
    Stash { label: String },
    /// `git add -A && git commit` with a `Ralph-Skipped-Step` trailer —
    /// preserves the WIP as a real commit.
    Commit { subject: String },
    /// Throw the changes away (delegates to [`rollback_except`]).
    Discard,
}

/// What [`park_changes`] actually did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // consumed by the skip flow in a later step
pub enum ParkOutcome {
    /// Changes were stashed; `stash_ref` recovers them.
    Stashed { stash_ref: StashRef },
    /// Changes were committed; `sha` is the new commit.
    Committed { sha: String },
    /// Changes were discarded.
    Discarded,
}

/// Dispose of the current working-tree changes per `strategy`.
///
/// A single entry point behind the three skip change-handling modes:
///
/// - [`ParkStrategy::Stash`] → `git stash push --include-untracked -m
///   <label>`. The returned [`StashRef`] is stable across later stash
///   pushes/drops. Errors if the tree is clean (nothing to stash).
/// - [`ParkStrategy::Commit`] → `git add -A && git commit` with a
///   `Ralph-Skipped-Step: <trailer_id>` trailer appended in git's standard
///   trailer format (a blank line, then `Token: value`). This stages
///   everything — including `pre_existing_untracked` — by design: a WIP
///   commit is meant to be a complete snapshot.
/// - [`ParkStrategy::Discard`] → [`rollback_except`], preserving
///   `pre_existing_untracked`.
///
/// `trailer_id` is only consulted for `Commit`; `pre_existing_untracked`
/// only for `Discard`.
#[allow(dead_code)] // called by the skip flow in a later step
pub fn park_changes(
    workdir: &Path,
    strategy: ParkStrategy,
    pre_existing_untracked: &[String],
    trailer_id: &str,
) -> Result<ParkOutcome> {
    match strategy {
        ParkStrategy::Stash { label } => match stash_push_with_untracked(workdir, &label)? {
            Some(stash_ref) => Ok(ParkOutcome::Stashed { stash_ref }),
            None => bail!("nothing to stash: working tree is clean"),
        },
        ParkStrategy::Commit { subject } => {
            // Blank line before the trailer block so git's trailer parser
            // (`%(trailers)`, `git interpret-trailers`) recognizes it.
            let message = format!("{subject}\n\nRalph-Skipped-Step: {trailer_id}\n");
            commit_changes(workdir, &message).context("could not commit skipped-step WIP")?;
            let sha = get_commit_hash(workdir)?;
            Ok(ParkOutcome::Committed { sha })
        }
        ParkStrategy::Discard => {
            rollback_except(workdir, pre_existing_untracked)
                .context("could not discard changes on skip")?;
            Ok(ParkOutcome::Discarded)
        }
    }
}

// ---------------------------------------------------------------------------
// Skipped-step WIP commit discovery & revert
// ---------------------------------------------------------------------------

/// The git trailer key written by [`park_changes`] for `ParkStrategy::Commit`.
pub const SKIPPED_STEP_TRAILER: &str = "Ralph-Skipped-Step";

/// A skip-WIP commit discovered on a branch: its SHA plus the step id pulled
/// out of the `Ralph-Skipped-Step` trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipWipCommit {
    pub sha: String,
    pub step_id: String,
}

/// Extract the `Ralph-Skipped-Step` trailer value from a single commit.
///
/// Uses `git interpret-trailers --parse` fed the commit's raw message so we
/// only ever match a *real* trailer line (git's own parser decides what
/// counts as the trailer block) — never the words happening to appear in a
/// commit body or a quoted diff. Returns `None` when the commit carries no
/// such trailer.
pub fn parse_skipped_step_trailer(workdir: &Path, sha: &str) -> Result<Option<String>> {
    let raw = git(workdir, &["log", "-1", "--format=%B", sha])
        .with_context(|| format!("could not read commit message for {sha}"))?;

    // `git interpret-trailers --parse` prints only the trailer block, one
    // `Key: value` per line. We feed the raw message on stdin.
    use std::io::Write;
    use std::process::Stdio;
    let mut child = Command::new("git")
        .args(["interpret-trailers", "--parse"])
        .current_dir(workdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn git interpret-trailers")?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(raw.as_bytes())
            .context("failed to write commit message to git interpret-trailers")?;
    }
    let output = child
        .wait_with_output()
        .context("git interpret-trailers --parse failed to run")?;
    if !output.status.success() {
        bail!(
            "git interpret-trailers --parse failed (exit {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let parsed = String::from_utf8_lossy(&output.stdout);
    for line in parsed.lines() {
        // Anchor on the trailer key at the start of a parsed trailer line so
        // a body sentence mentioning the token can't false-match.
        if let Some((key, value)) = line.split_once(':')
            && key.trim().eq_ignore_ascii_case(SKIPPED_STEP_TRAILER)
        {
            let v = value.trim();
            if !v.is_empty() {
                return Ok(Some(v.to_string()));
            }
        }
    }
    Ok(None)
}

/// Walk the commits reachable from `branch` and return every skip-WIP commit
/// (one carrying a `Ralph-Skipped-Step` trailer), **newest first** — i.e. in
/// reverse-chronological / `git log` order.
///
/// `branch` is resolved with `git rev-list`, so it works whether or not the
/// branch is currently checked out. Commits without the trailer are ignored.
pub fn list_skip_wip_commits(workdir: &Path, branch: &str) -> Result<Vec<SkipWipCommit>> {
    // `git rev-list` already yields newest-first.
    let shas = git(workdir, &["rev-list", branch])
        .with_context(|| format!("could not list commits on branch '{branch}'"))?;
    let mut out = Vec::new();
    for sha in shas.lines().map(str::trim).filter(|s| !s.is_empty()) {
        if let Some(step_id) = parse_skipped_step_trailer(workdir, sha)? {
            out.push(SkipWipCommit {
                sha: sha.to_string(),
                step_id,
            });
        }
    }
    Ok(out)
}

/// Skip-WIP commits on `branch` whose trailer step id equals `step_id`,
/// newest-first. Convenience filter over [`list_skip_wip_commits`].
pub fn skip_wip_commits_for_step(
    workdir: &Path,
    branch: &str,
    step_id: &str,
) -> Result<Vec<String>> {
    Ok(list_skip_wip_commits(workdir, branch)?
        .into_iter()
        .filter(|c| c.step_id == step_id)
        .map(|c| c.sha)
        .collect())
}

/// Outcome of attempting to `git revert --no-edit` a single skip-WIP commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevertOutcome {
    /// A new revert commit was created.
    Reverted { revert_sha: String },
    /// The revert was an effective no-op (the change is already gone — the
    /// WIP commit was manually reverted earlier). No revert commit created.
    AlreadyReverted,
}

/// `git revert --no-edit <sha>`.
///
/// Handles the "already reverted" edge case: when the commit's changes are
/// already absent, `git revert` either reports "nothing to commit" (empty
/// revert) or conflicts. In both cases we abort the in-progress revert with
/// `git revert --abort` (so the worktree/index is left clean) and return
/// [`RevertOutcome::AlreadyReverted`] instead of a hard error. A genuine
/// merge conflict from *unrelated* later work is still surfaced as an error
/// after aborting.
pub fn revert_commit(workdir: &Path, sha: &str) -> Result<RevertOutcome> {
    let output = Command::new("git")
        .args(["revert", "--no-edit", sha])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to execute git revert {sha}"))?;

    if output.status.success() {
        let revert_sha = get_commit_hash(workdir)?;
        return Ok(RevertOutcome::Reverted { revert_sha });
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{stderr}");

    // `git revert` leaves a revert-in-progress on failure; clean it up so the
    // tree isn't wedged regardless of which failure path we took.
    let revert_in_progress = workdir.join(".git").join("REVERT_HEAD").exists();
    if revert_in_progress {
        // Best-effort abort; ignore its own failure (nothing left to abort).
        let _ = Command::new("git")
            .args(["revert", "--abort"])
            .current_dir(workdir)
            .output();
    }

    // Effective no-op: the change is already gone. git phrases this as
    // "nothing to commit" / "previous cherry-pick/revert is now empty" / a
    // conflict where every hunk is already applied.
    let lc = combined.to_lowercase();
    if lc.contains("nothing to commit")
        || lc.contains("nothing added to commit")
        || lc.contains("is now empty")
        || lc.contains("no changes")
        || lc.contains("the previous cherry-pick is now empty")
    {
        return Ok(RevertOutcome::AlreadyReverted);
    }

    bail!(
        "git revert {sha} failed (exit {}): {}",
        output.status,
        combined.trim()
    );
}

// ---------------------------------------------------------------------------
// Per-iteration step commits + Ralph-* trailers (docs/dag-redesign.md §3.2/§5)
// ---------------------------------------------------------------------------

/// Trailer key carrying the plan slug on a per-iteration step commit.
pub const ITERATION_PLAN_TRAILER: &str = "Ralph-Plan";
/// Trailer key carrying the step `short_id` on a per-iteration step commit.
pub const ITERATION_STEP_TRAILER: &str = "Ralph-Step";
/// Trailer key carrying the 1-based iteration number on a per-iteration
/// step commit.
pub const ITERATION_NUM_TRAILER: &str = "Ralph-Iteration";
/// Trailer key carrying the review verdict on a per-iteration step commit
/// (`pending` initially; later annotated by the review pipeline).
pub const ITERATION_REVIEW_TRAILER: &str = "Ralph-Review";

/// Collapse a step title into a single sanitized commit-subject fragment.
///
/// The git subject line must be a single line: newlines/tabs/control chars
/// are replaced with spaces and runs of whitespace collapsed. The result is
/// length-capped so a pathological multi-paragraph title can't produce a
/// thousand-column subject. Tooling never parses the subject (it parses the
/// trailers), so this is purely cosmetic — but it must stay deterministic.
pub fn sanitize_commit_subject(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    let mut prev_space = false;
    for ch in title.chars() {
        if ch.is_control() || ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    let trimmed = out.trim();
    // Cap at 72 chars (a conventional git subject soft limit) on a char
    // boundary so multibyte titles don't panic.
    const MAX: usize = 72;
    if trimmed.chars().count() > MAX {
        trimmed.chars().take(MAX).collect::<String>()
    } else {
        trimmed.to_string()
    }
}

/// Build the full commit message for one per-iteration step commit.
///
/// Subject: `ralph <short_id>.<n> - <sanitized one-line title>`.
/// Trailers (a blank line then `Key: value`, git's standard trailer block —
/// the same shape [`park_changes`] uses for `Ralph-Skipped-Step`):
/// ```text
/// Ralph-Plan: <slug>
/// Ralph-Step: <short_id>
/// Ralph-Iteration: <n>
/// Ralph-Review: pending
/// ```
/// Tooling (`ralph log` / `step reset`) parses *only* the trailers via git's
/// own trailer parser, never the subject.
pub fn build_iteration_commit_message(
    short_id: &str,
    iteration: i32,
    title: &str,
    plan_slug: &str,
) -> String {
    let subject = format!(
        "ralph {}.{} - {}",
        short_id,
        iteration,
        sanitize_commit_subject(title)
    );
    format!(
        "{subject}\n\n\
         {ITERATION_PLAN_TRAILER}: {plan_slug}\n\
         {ITERATION_STEP_TRAILER}: {short_id}\n\
         {ITERATION_NUM_TRAILER}: {iteration}\n\
         {ITERATION_REVIEW_TRAILER}: pending\n"
    )
}

/// A per-iteration step commit discovered on a branch: its SHA plus the
/// `Ralph-Step` short id and `Ralph-Iteration` number pulled from trailers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IterationCommit {
    pub sha: String,
    pub plan_slug: Option<String>,
    pub step_short_id: String,
    pub iteration: i32,
}

/// Extract a single named trailer's value from a commit, using git's own
/// trailer parser (`git interpret-trailers --parse`) so a body sentence or a
/// quoted diff that merely *mentions* the key can never false-match. Returns
/// `None` when the commit carries no such trailer.
///
/// Generalizes [`parse_skipped_step_trailer`] (which is kept as-is for the
/// skip-WIP path) to any trailer key.
pub fn parse_trailer(workdir: &Path, sha: &str, key: &str) -> Result<Option<String>> {
    let raw = git(workdir, &["log", "-1", "--format=%B", sha])
        .with_context(|| format!("could not read commit message for {sha}"))?;

    use std::io::Write;
    use std::process::Stdio;
    let mut child = Command::new("git")
        .args(["interpret-trailers", "--parse"])
        .current_dir(workdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn git interpret-trailers")?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(raw.as_bytes())
            .context("failed to write commit message to git interpret-trailers")?;
    }
    let output = child
        .wait_with_output()
        .context("git interpret-trailers --parse failed to run")?;
    if !output.status.success() {
        bail!(
            "git interpret-trailers --parse failed (exit {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let parsed = String::from_utf8_lossy(&output.stdout);
    for line in parsed.lines() {
        if let Some((k, value)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case(key)
        {
            let v = value.trim();
            if !v.is_empty() {
                return Ok(Some(v.to_string()));
            }
        }
    }
    Ok(None)
}

/// Dedicated git-notes ref the review pipeline annotates verdicts under.
/// Notes attach to a *fixed* commit SHA without rewriting history (so it is
/// safe to annotate a non-HEAD historical iteration commit) and without ever
/// touching the working tree (so it does not violate the §9-inv-2 "reviews
/// are strictly read-only w.r.t. the working tree" hard invariant — a note
/// write changes `refs/notes/...`, not the index/worktree/branch).
pub const REVIEW_NOTES_REF: &str = "refs/notes/ralph-review";

/// Annotate the reviewed commit's `Ralph-Review` verdict (§5/§9).
///
/// The per-iteration commit bakes in `Ralph-Review: pending` at commit time
/// (it is immutable history). The verdict (`passed` | `failed` | `skipped` |
/// `disabled`) is recorded *after the fact* against the fixed SHA via a git
/// note on [`REVIEW_NOTES_REF`], NOT by amending/rewriting the commit:
///
/// - Amending a historical commit would rewrite linear history and shift
///   every later iteration commit's SHA — fatal under concurrent reviews
///   (§9-inv-2: a review runs against a *fixed* SHA while the next
///   implementation is already committing on top).
/// - A note write is read-only w.r.t. the working tree / index / branch ref,
///   so it preserves the read-only-review hard invariant.
///
/// The note body is a single `Ralph-Review: <verdict>` line — the same
/// trailer key ([`ITERATION_REVIEW_TRAILER`]) the commit carries — so
/// tooling reads the final verdict by preferring the note over the baked-in
/// `pending`. `--force` so a re-review (corrective-chain) overwrites a prior
/// note rather than erroring.
pub fn annotate_review_verdict(workdir: &Path, sha: &str, verdict: &str) -> Result<()> {
    let body = format!("{ITERATION_REVIEW_TRAILER}: {verdict}");
    git(
        workdir,
        &[
            "notes",
            "--ref",
            REVIEW_NOTES_REF,
            "add",
            "--force",
            "-m",
            &body,
            sha,
        ],
    )
    .with_context(|| format!("could not annotate review verdict on commit {sha}"))?;
    Ok(())
}

/// Read back the annotated `Ralph-Review` verdict for `sha` (the note on
/// [`REVIEW_NOTES_REF`]), or `None` when no verdict has been recorded yet
/// (the commit still carries only its baked-in `pending` trailer). Used by
/// tests and `ralph log` to surface the *final* verdict rather than the
/// commit-time placeholder.
pub fn read_review_verdict(workdir: &Path, sha: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(["notes", "--ref", REVIEW_NOTES_REF, "show", sha])
        .current_dir(workdir)
        .output()
        .with_context(|| format!("failed to read review note for {sha}"))?;
    if !output.status.success() {
        // `git notes show` exits non-zero when there is no note — that is the
        // "not yet reviewed / no verdict" case, not an error.
        return Ok(None);
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    for line in raw.lines() {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case(ITERATION_REVIEW_TRAILER)
        {
            let v = v.trim();
            if !v.is_empty() {
                return Ok(Some(v.to_string()));
            }
        }
    }
    Ok(None)
}

/// Walk every commit reachable from `branch` and return the per-iteration
/// step commits (those carrying a `Ralph-Step` trailer), **newest first**
/// (`git log` order). Commits without the trailer (or with an unparseable
/// `Ralph-Iteration`) are skipped.
///
/// `branch` is resolved with `git rev-list`, so it works whether or not it
/// is currently checked out — mirrors [`list_skip_wip_commits`].
pub fn list_iteration_commits(workdir: &Path, branch: &str) -> Result<Vec<IterationCommit>> {
    let shas = git(workdir, &["rev-list", branch])
        .with_context(|| format!("could not list commits on branch '{branch}'"))?;
    let mut out = Vec::new();
    for sha in shas.lines().map(str::trim).filter(|s| !s.is_empty()) {
        let Some(step_short_id) = parse_trailer(workdir, sha, ITERATION_STEP_TRAILER)? else {
            continue;
        };
        let iteration = match parse_trailer(workdir, sha, ITERATION_NUM_TRAILER)? {
            Some(s) => match s.parse::<i32>() {
                Ok(n) => n,
                // A Ralph-Step commit with a non-numeric Ralph-Iteration is
                // malformed — skip rather than guess.
                Err(_) => continue,
            },
            None => continue,
        };
        let plan_slug = parse_trailer(workdir, sha, ITERATION_PLAN_TRAILER)?;
        out.push(IterationCommit {
            sha: sha.to_string(),
            plan_slug,
            step_short_id,
            iteration,
        });
    }
    Ok(out)
}

/// Per-iteration commits on `branch` whose `Ralph-Step` trailer equals
/// `short_id`, newest-first. Convenience filter over
/// [`list_iteration_commits`].
pub fn iteration_commits_for_step(
    workdir: &Path,
    branch: &str,
    short_id: &str,
) -> Result<Vec<IterationCommit>> {
    Ok(list_iteration_commits(workdir, branch)?
        .into_iter()
        .filter(|c| c.step_short_id == short_id)
        .collect())
}

/// Return the distinct file paths touched by the listed commits (added,
/// modified, deleted, etc.). Uses `git diff-tree --name-only -r` per SHA.
/// Empty input or no output yields empty vec. Duplicates removed but order
/// is first-seen.
pub fn files_touched_by_commits(workdir: &Path, shas: &[String]) -> Result<Vec<String>> {
    let mut seen = Vec::new();
    for sha in shas {
        let out = git(
            workdir,
            &["diff-tree", "--no-commit-id", "--name-only", "-r", sha],
        )
        .with_context(|| format!("git diff-tree for commit {sha} failed"))?;
        for line in out.lines().map(str::trim).filter(|l| !l.is_empty()) {
            if !seen.contains(&line.to_string()) {
                seen.push(line.to_string());
            }
        }
    }
    Ok(seen)
}

/// Returns true iff there are Ralph-* iteration commits for `short_id` on
/// `branch` *and* the current uncommitted dirty files overlap at least one
/// path touched by the most recent (up to 3) of those commits.
///
/// This is the conservative "likely ralph-owned crash residue from the
/// InProgress step" test used by the medium crash-reconcile UX in
/// `stash_if_dirty`. Errors during detection are treated as "no" (do not
/// offer interactive recovery).
pub fn has_crash_residue_overlap_for_step(
    workdir: &Path,
    branch: &str,
    short_id: &str,
) -> Result<bool> {
    let commits = iteration_commits_for_step(workdir, branch, short_id)?;
    if commits.is_empty() {
        return Ok(false);
    }
    let recent_shas: Vec<String> = commits.into_iter().take(3).map(|c| c.sha).collect();
    let touched = files_touched_by_commits(workdir, &recent_shas)?;
    if touched.is_empty() {
        return Ok(false);
    }
    let dirty = get_all_changed_files(workdir)?;
    let overlap = dirty.iter().any(|d| touched.contains(d));
    Ok(overlap)
}

/// Squash every commit reachable from HEAD back to (but excluding) `base_sha`
/// into a single new commit with `message`, preserving the working tree.
///
/// Used by `--squash-on-complete` (docs/dag-redesign.md §14.1): when a step
/// reaches `Complete`, its N per-iteration commits collapse into one. We
/// `git reset --soft <base_sha>` (moves the branch ref back, keeps the index
/// and working tree exactly as the last iteration left them) then commit the
/// staged tree once. `base_sha` MUST be an ancestor of HEAD and is the SHA
/// that immediately preceded the step's first iteration commit.
pub fn squash_since(workdir: &Path, base_sha: &str, message: &str) -> Result<String> {
    git(workdir, &["reset", "--soft", base_sha])
        .with_context(|| format!("git reset --soft {base_sha} failed"))?;
    git(workdir, &["commit", "-m", message]).context("git commit (squash) failed")?;
    get_commit_hash(workdir)
}

/// Number of commits in the range `base_sha..HEAD` (commits reachable from
/// HEAD but not from `base_sha`). Used by `--squash-on-complete` to skip the
/// soft-reset+recommit churn when a step made only a single iteration commit
/// (nothing to collapse). `base_sha` must be an ancestor of HEAD.
pub fn count_commits_since(workdir: &Path, base_sha: &str) -> Result<usize> {
    let out = git(
        workdir,
        &["rev-list", "--count", &format!("{base_sha}..HEAD")],
    )
    .with_context(|| format!("could not count commits since {base_sha}"))?;
    Ok(out.trim().parse().unwrap_or(0))
}

/// Order an arbitrary set of `targets` SHAs by their position on `branch`,
/// **newest-first** (`git rev-list` order). SHAs not reachable from `branch`
/// are dropped. Used by `ralph step reset` so a mixed set of skip-WIP +
/// per-iteration commits is reverted in a clean newest-first sequence
/// regardless of how the two trailer scans interleaved them.
pub fn order_shas_newest_first(
    workdir: &Path,
    branch: &str,
    targets: &[String],
) -> Result<Vec<String>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let want: std::collections::HashSet<&str> = targets.iter().map(String::as_str).collect();
    let shas = git(workdir, &["rev-list", branch])
        .with_context(|| format!("could not list commits on branch '{branch}'"))?;
    Ok(shas
        .lines()
        .map(str::trim)
        .filter(|s| want.contains(s))
        .map(str::to_string)
        .collect())
}

// ---------------------------------------------------------------------------
// Throwaway review worktree (docs/dag-redesign.md §8 / §9 invariant 2)
// ---------------------------------------------------------------------------

/// Create a **detached** linked worktree of `workdir` pinned at `sha`, rooted
/// at `path` (which must not yet exist).
///
/// `git worktree add --detach <path> <sha>` checks `sha`'s tree out into a
/// brand-new, *physically separate* directory whose `HEAD` is detached at
/// `sha`. The reviewer harness is run with its cwd set to that directory, so
/// it is structurally incapable of touching the implementation's live working
/// tree: `echo evil >> src/foo.rs` inside the reviewer lands in the throwaway
/// directory, never in the shared `workdir` the next implementation commits
/// from. This is the §9-inv-2 "reviews are strictly read-only w.r.t. the
/// working tree" hard invariant enforced *structurally* rather than only
/// detected after the fact.
pub fn add_detached_worktree(workdir: &Path, path: &Path, sha: &str) -> Result<()> {
    git(
        workdir,
        &["worktree", "add", "--detach", &path.to_string_lossy(), sha],
    )
    .with_context(|| {
        format!(
            "could not create detached review worktree at {} pinned at {sha}",
            path.display()
        )
    })?;
    Ok(())
}

/// Forcibly remove the linked worktree at `path` and prune dangling
/// administrative entries.
///
/// `--force` because the reviewer may have left dirty/untracked junk in the
/// throwaway tree (that is exactly the tamper class we are containing — it is
/// *expected* there and must not block cleanup). Tolerant by design: if the
/// directory is already gone (panic/early-return raced cleanup, or a prior
/// call already removed it) `git worktree remove` errors, which we swallow,
/// then always `git worktree prune` so no orphan administrative entry is left
/// in `.git/worktrees/`. Cleanup must never itself fail a run.
pub fn remove_worktree(workdir: &Path, path: &Path) {
    // Best-effort: a failure here (already-removed dir, etc.) must not mask
    // the review outcome. We still prune unconditionally afterwards.
    let _ = Command::new("git")
        .args(["worktree", "remove", "--force", &path.to_string_lossy()])
        .current_dir(workdir)
        .output();
    let _ = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(workdir)
        .output();
    // Belt-and-suspenders: if `git worktree remove` could not delete the
    // directory (e.g. it was already detached from git's metadata), make sure
    // no throwaway tree is left on disk.
    if path.exists() {
        let _ = std::fs::remove_dir_all(path);
    }
}

/// Best-effort sweep of review worktrees stranded by a `SIGKILL`'d run.
///
/// `ReviewWorktree`'s RAII `Drop` cleans up on every *normal* exit path, but
/// a forceful second Ctrl+C (`std::process::exit(130)`) or an OS `SIGKILL`
/// skips `Drop`, leaving a `<tmp>/ralph-review-*` directory and a
/// `.git/worktrees/` admin entry behind. Reviews are short-lived (a single
/// read-only `git show` diff), so any `ralph-review-*` temp dir older than a
/// few hours is unambiguously orphaned. We prune git's admin entries, then
/// remove on-disk dirs older than the threshold. The mtime-age guard makes
/// this safe under a *concurrent* ralph run: an in-flight review's worktree
/// has a recent mtime and is left alone. Never fails the caller.
pub fn sweep_stale_review_worktrees(main_repo: &Path) {
    // 6h: orders of magnitude longer than any real review, far short of
    // anything that would race a live concurrent review.
    const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);
    if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("ralph-review-") {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|mtime| mtime.elapsed().ok())
                .map(|age| age >= STALE_AFTER)
                .unwrap_or(false);
            if stale {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    // Prune *after* the removals so that the `.git/worktrees/*` admin
    // entries for the directories we just deleted are reaped in the same
    // sweep (not stranded until some later run prunes again). A single
    // prune at the end also clears any entries whose directory was already
    // gone for unrelated reasons, so this still covers the original intent.
    let _ = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(main_repo)
        .output();
}

/// List the filesystem paths of every registered worktree (including the
/// main one), for tests asserting no orphan review worktree leaked.
#[cfg(test)]
pub fn list_worktree_paths(workdir: &Path) -> Result<Vec<String>> {
    let out =
        git(workdir, &["worktree", "list", "--porcelain"]).context("could not list worktrees")?;
    Ok(out
        .lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .map(|s| s.trim().to_string())
        .collect())
}

/// RAII guard for a throwaway review worktree (docs/dag-redesign.md §8/§9-inv-2).
///
/// Construction creates the detached worktree at the reviewed SHA; `Drop`
/// removes it. Because `Drop` runs on **every** exit path of the function that
/// holds the guard — normal return, `?`-propagated error, the spawn/await
/// failing, a panic unwinding through the spawned review task, or the task
/// being aborted/timed out — the throwaway tree is *always* torn down. There
/// is no code path that creates one and leaks it. The path lives under the
/// OS temp dir with a unique component so concurrent reviews never collide.
pub struct ReviewWorktree {
    /// The main repository the linked worktree is attached to (where
    /// `git worktree remove/prune` must run).
    main_repo: std::path::PathBuf,
    /// The throwaway worktree directory; the reviewer harness's cwd.
    path: std::path::PathBuf,
}

impl ReviewWorktree {
    /// Create a uniquely-named detached worktree of `main_repo` at `sha`.
    ///
    /// The directory is `<tmp>/ralph-review-<pid>-<sha12>-<nanos>` so two
    /// concurrent reviews (allowed by §3.5 item 3) never collide and a stale
    /// dir from a crashed prior run can never be reused.
    pub fn create(main_repo: &Path, sha: &str) -> Result<Self> {
        let unique = format!(
            "ralph-review-{}-{}-{}",
            std::process::id(),
            sha.chars().take(12).collect::<String>(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        let path = std::env::temp_dir().join(unique);
        add_detached_worktree(main_repo, &path, sha)?;
        Ok(Self {
            main_repo: main_repo.to_path_buf(),
            path,
        })
    }

    /// The throwaway worktree directory — the cwd to spawn the reviewer in.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ReviewWorktree {
    fn drop(&mut self) {
        remove_worktree(&self.main_repo, &self.path);
    }
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
    fn test_checkout_branch_existing() {
        let (_tmp, dir) = init_repo();
        let initial = get_current_branch(&dir).unwrap();
        create_and_checkout_branch(&dir, "feature/exists").unwrap();
        // Switch back via the plain checkout helper.
        checkout_branch(&dir, &initial).unwrap();
        assert_eq!(get_current_branch(&dir).unwrap(), initial);
    }

    #[test]
    fn test_checkout_branch_missing_errors() {
        let (_tmp, dir) = init_repo();
        let result = checkout_branch(&dir, "feature/never-created");
        assert!(result.is_err());
    }

    #[test]
    fn test_branch_exists_reports_true_for_existing() {
        let (_tmp, dir) = init_repo();
        create_and_checkout_branch(&dir, "feature/here").unwrap();
        assert!(branch_exists(&dir, "feature/here").unwrap());
    }

    #[test]
    fn test_branch_exists_reports_false_for_missing() {
        let (_tmp, dir) = init_repo();
        assert!(!branch_exists(&dir, "feature/never").unwrap());
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

    /// Build a NUL-delimited `git status --porcelain=v1 -z` byte stream from
    /// `(status, path[, orig])` tuples. For an `R`/`C` record git emits the
    /// `XY <new>` record first, then a SEPARATE NUL-terminated record with
    /// the ORIGINAL path — this helper reproduces that exact framing.
    type PorcelainRec<'a> = (&'a str, &'a [u8], Option<&'a [u8]>);

    fn porcelain_z(records: &[PorcelainRec]) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        for (status, path, orig) in records {
            out.extend_from_slice(status.as_bytes());
            out.push(b' ');
            out.extend_from_slice(path);
            out.push(0u8);
            if let Some(orig) = orig {
                out.extend_from_slice(orig);
                out.push(0u8);
            }
        }
        out
    }

    #[test]
    fn test_parse_porcelain_status_rename_and_copy() {
        // Simulated `git status --porcelain=v1 -z` output covering:
        //   - plain modifications
        //   - adds
        //   - untracked
        //   - a staged rename (R  with a following orig-path record)
        //   - a staged copy   (C  with a following orig-path record)
        //   - an unstaged rename where the worktree column is R ( R …)
        // Returned-order contract: for R/C we push ORIG then NEW.
        let out = porcelain_z(&[
            (" M", b"modified.txt", None),
            ("A ", b"added.txt", None),
            ("??", b"untracked.txt", None),
            ("R ", b"new_renamed.txt", Some(b"old_renamed.txt")),
            ("C ", b"dst_copied.txt", Some(b"src_copied.txt")),
            (" R", b"wt_new.txt", Some(b"wt_old.txt")),
        ]);
        let files = parse_porcelain_status_z(&out).expect("well-formed -z stream parses");
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

    /// Paths containing a space, a newline, a double-quote and a non-ASCII
    /// (non-UTF8) byte. Under the old line/`" -> "` parser these would have
    /// been C-quoted by git and mis-split / mis-decoded; under `-z` they are
    /// emitted RAW and must round-trip (the non-UTF8 byte goes through the
    /// documented best-effort `from_utf8_lossy` boundary).
    #[test]
    fn test_parse_porcelain_status_z_unusual_paths() {
        // 0xFF is never valid UTF-8 → exercises the lossy boundary.
        let spacey = b"a file.txt".as_slice();
        let newliney = b"line1\nline2.txt".as_slice();
        let quoted = b"weird\"name.txt".as_slice();
        let non_utf8: &[u8] = &[b'b', b'a', b'd', 0xFF, b'.', b't', b'x', b't'];

        let out = porcelain_z(&[
            (" M", spacey, None),
            ("A ", newliney, None),
            ("??", quoted, None),
            // A rename whose NEW and ORIG paths both carry unusual bytes.
            ("R ", b"to .txt", Some(non_utf8)),
        ]);
        let files = parse_porcelain_status_z(&out).expect("raw -z paths parse");

        assert_eq!(files[0], "a file.txt");
        assert_eq!(files[1], "line1\nline2.txt");
        assert_eq!(files[2], "weird\"name.txt");
        // orig (non-UTF8) pushed before new; lossy-decoded but not split.
        assert_eq!(files[3], String::from_utf8_lossy(non_utf8));
        assert!(files[3].contains('\u{FFFD}'), "non-UTF8 byte lossily kept");
        assert_eq!(files[4], "to .txt");
        assert_eq!(files.len(), 5);
    }

    /// Total-parser contract: an empty stream (clean tree) is `Ok([])`, not
    /// an error.
    #[test]
    fn test_parse_porcelain_status_z_empty_is_ok_empty() {
        assert_eq!(parse_porcelain_status_z(b"").unwrap(), Vec::<String>::new());
        // A lone trailing NUL (the well-formed "no entries" shape) is also
        // an empty list, not a malformed record.
        assert_eq!(
            parse_porcelain_status_z(b"\0").unwrap(),
            Vec::<String>::new()
        );
    }

    /// Total-parser contract: a rename/copy record whose required following
    /// original-path record is missing (truncated pipe) is a HARD ERROR —
    /// acting on just the new path would silently drop the old path from the
    /// rollback/preserve set (the data-loss class this parser prevents).
    #[test]
    fn test_parse_porcelain_status_z_truncated_rename_is_error() {
        // `R  new.txt\0` with NO following `\0`-terminated orig record.
        let mut out: Vec<u8> = Vec::new();
        out.extend_from_slice(b"R ");
        out.push(b' ');
        out.extend_from_slice(b"new.txt");
        out.push(0u8);
        let err = parse_porcelain_status_z(&out)
            .expect_err("a rename with no orig record must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("truncated") && msg.contains("rename/copy"),
            "error must name the truncation: {msg}"
        );
    }

    /// Total-parser contract: a record that is not `XY <path>` (too short, or
    /// missing the space delimiter at index 2) is a HARD ERROR, never
    /// mis-sliced into a bogus path.
    #[test]
    fn test_parse_porcelain_status_z_malformed_record_is_error() {
        // Missing the space at index 2 ("XYpath" instead of "XY path").
        let err = parse_porcelain_status_z(b"MMnospace.txt\0")
            .expect_err("record without the `XY ` delimiter must be rejected");
        assert!(err.to_string().contains("malformed"), "{err}");
        // Too short to even contain `XY <1-char path>`.
        let err2 = parse_porcelain_status_z(b"M\0").expect_err("a 1-byte record must be rejected");
        assert!(err2.to_string().contains("malformed"), "{err2}");
    }

    #[test]
    fn test_check_ref_format_accepts_valid_names() {
        // A representative spread of legal branch names, including ones that
        // exercise "allowed" edges of the rules: single-level, slashed,
        // dots-but-not-double, embedded (non-leading) dash, digits, a
        // component that merely *contains* "lock", and `@` not as `@{` or a
        // lone `@`.
        for ok in [
            "main",
            "feat/foo",
            "release-1.2.3",
            "feat/JIRA-123_some-thing",
            "user/feature.work",
            "v2",
            "a/b/c/d",
            "lockfile-update",
            "has@sign",
        ] {
            check_ref_format(ok).unwrap_or_else(|e| panic!("{ok:?} must be valid: {e}"));
        }
    }

    /// Every git-check-ref-format(1) rule the native validator encodes must
    /// reject, plus our two explicit pre-checks (empty, leading dash). Each
    /// case asserts the user-facing `invalid branch name` framing so callers
    /// always get an actionable message.
    #[test]
    fn test_check_ref_format_rejects_every_rule() {
        let cases: &[&str] = &[
            "",                 // empty (pre-check)
            "   ",              // whitespace-only (pre-check)
            "-leading-dash",    // leading '-' (pre-check; CLI-flag hazard)
            "feat/bad..branch", // rule: consecutive dots
            "..",               // consecutive dots / ends with '.'
            "bad branch",       // rule: space
            "feat/..hidden",    // component begins with '.'
            ".hidden",          // component begins with '.'
            "ends.",            // ends with '.'
            "foo.lock",         // component ends with '.lock'
            "foo.lock/bar",     // NON-last component ends with '.lock'
            "foo//bar",         // consecutive slashes
            "trailing/",        // ends with '/'
            "has~tilde",        // rule: '~'
            "has^caret",        // rule: '^'
            "has:colon",        // rule: ':'
            "has?q",            // rule: '?'
            "has*star",         // rule: '*'
            "has[bracket",      // rule: '['
            "back\\slash",      // rule: backslash
            "ctrl\u{7f}del",    // rule: DEL control char
            "ctrl\u{1}soh",     // rule: <0x20 control char
            "ref@{0}",          // rule: '@{' sequence
        ];
        for bad in cases {
            let e = check_ref_format(bad).expect_err(&format!("{bad:?} must be rejected"));
            assert!(
                e.to_string().contains("invalid branch name"),
                "rejection for {bad:?} must use the actionable framing: {e}"
            );
        }
    }

    /// Slash-shape branches map onto the `refs/heads/<name>` form correctly:
    /// a leading slash collapses to `refs/heads//x` (consecutive slashes) and
    /// is rejected deterministically.
    #[test]
    fn test_check_ref_format_slash_edges() {
        check_ref_format("/leading").expect_err("a '/'-leading branch must reject");
        check_ref_format("a//b").expect_err("consecutive slashes must reject");
        check_ref_format("trailing/").expect_err("a trailing-'/' branch must reject");
    }

    /// `validate_refname` is the pure rule core; pin the documented decision
    /// for the bare-ref forms (no `refs/heads/` prefix) so the encoding can't
    /// silently drift from git-check-ref-format(1).
    #[test]
    fn test_validate_refname_core_rules() {
        assert!(validate_refname("refs/heads/main").is_ok());
        assert!(validate_refname("@").is_err()); // rule 9
        assert!(validate_refname("/x").is_err()); // rule 6 (leading slash)
        assert!(validate_refname("x/").is_err()); // rule 6 (trailing slash)
        assert!(validate_refname("a..b").is_err()); // rule 3
        assert!(validate_refname("a//b").is_err()); // rule 6
        assert!(validate_refname("a.").is_err()); // rule 7
        assert!(validate_refname("a@{b").is_err()); // rule 8
        assert!(validate_refname("a\\b").is_err()); // rule 10
        assert!(validate_refname(".hidden/x").is_err()); // rule 1 (begins '.')
        assert!(validate_refname("x/foo.lock").is_err()); // rule 1 ('.lock')
        assert!(validate_refname("a b").is_err()); // rule 4 (space)
        assert!(validate_refname("a\u{0}b").is_err()); // rule 4 (control)
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

    /// `rollback_except` must drop a file the harness created **and**
    /// `git add`-ed (staged-new): `git restore .` alone keeps it (worktree
    /// ← index) and `git ls-files --others` excludes staged paths, so before
    /// the index-unstage step such a file survived a Discard/Cancel. It must
    /// also revert a staged modification to a tracked file, while preserving
    /// a genuinely pre-existing untracked file named in `preserve`.
    #[test]
    fn test_rollback_except_drops_staged_new_files_keeps_preserved() {
        let (_tmp, dir) = init_repo();

        // Pre-existing untracked file (existed before the "harness" ran) —
        // must be preserved.
        fs::write(dir.join("user-scratch.txt"), "keep me").unwrap();
        let preserve = vec!["user-scratch.txt".to_string()];

        // Harness work: a new file it created and staged, plus a staged
        // modification to a tracked file.
        fs::write(dir.join("harness-new.rs"), "fn generated() {}").unwrap();
        fs::write(dir.join("README.md"), "clobbered by harness").unwrap();
        git(&dir, &["add", "harness-new.rs", "README.md"]).unwrap();
        assert!(has_uncommitted_changes(&dir).unwrap());

        rollback_except(&dir, &preserve).unwrap();

        // Staged-new harness file is gone.
        assert!(
            !dir.join("harness-new.rs").exists(),
            "a harness-staged new file must not survive rollback"
        );
        // Tracked file reverted to HEAD content.
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# hello"
        );
        // Pre-existing untracked file preserved.
        assert!(dir.join("user-scratch.txt").exists());
        assert_eq!(
            fs::read_to_string(dir.join("user-scratch.txt")).unwrap(),
            "keep me"
        );
        // Nothing tracked left dirty (the preserved untracked file is the
        // only remaining change).
        let staged = list_staged_files(&dir).unwrap();
        assert!(staged.is_empty(), "index must be clean after rollback");
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
    fn test_drop_stash_discards_entry_without_applying() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("note.txt"), "park me
").unwrap();
        let stash = stash_push_with_untracked(&dir, "ralph: discard me")
            .unwrap()
            .expect("sha");

        let dropped = drop_stash(&dir, &stash).unwrap();

        assert!(dropped);
        assert!(!dir.join("note.txt").exists(), "discard must not apply the stash");
        assert_eq!(stash_pop(&dir, &stash).unwrap(), StashPopOutcome::NotFound);
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

    // ----- park_changes -----

    #[test]
    fn test_park_changes_stash_label_recoverable() {
        let (_tmp, dir) = init_repo();
        // Tracked modification + untracked file — --include-untracked picks
        // up both.
        fs::write(dir.join("README.md"), "# modified").unwrap();
        fs::write(dir.join("scratch.txt"), "wip").unwrap();

        let label = "ralph: skipped step 3 wip";
        let outcome = park_changes(
            &dir,
            ParkStrategy::Stash {
                label: label.to_string(),
            },
            &[],
            "trailer-id-irrelevant-for-stash",
        )
        .unwrap();

        let stash_ref = match outcome {
            ParkOutcome::Stashed { stash_ref } => stash_ref,
            other => panic!("expected Stashed, got {other:?}"),
        };
        assert_eq!(stash_ref.as_str().len(), 40);

        // Stashing left the tree clean.
        assert!(!has_uncommitted_changes(&dir).unwrap());

        // The label is recoverable via `git stash list`.
        let list = git(&dir, &["stash", "list"]).unwrap();
        assert!(list.contains(label), "stash list missing label: {list}");

        // And the returned ref resolves back to that stash.
        let found = find_stash_by_message(&dir, label).unwrap().expect("found");
        assert_eq!(found, stash_ref);
    }

    #[test]
    fn test_park_changes_stash_clean_tree_errors() {
        let (_tmp, dir) = init_repo();
        let result = park_changes(
            &dir,
            ParkStrategy::Stash {
                label: "ralph: nothing here".to_string(),
            },
            &[],
            "ignored",
        );
        assert!(result.is_err(), "clean tree should error, got {result:?}");
    }

    #[test]
    fn test_park_changes_commit_trailer_greppable() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("README.md"), "# modified").unwrap();
        fs::write(dir.join("new.txt"), "added on skip").unwrap();

        let trailer_id = "9f3c2a10-dead-beef-0000-000000000001";
        let outcome = park_changes(
            &dir,
            ParkStrategy::Commit {
                subject: "WIP: skipped step 7".to_string(),
            },
            &[],
            trailer_id,
        )
        .unwrap();

        let sha = match outcome {
            ParkOutcome::Committed { sha } => sha,
            other => panic!("expected Committed, got {other:?}"),
        };
        assert_eq!(sha, get_commit_hash(&dir).unwrap());

        // Everything got committed — tree is clean.
        assert!(!has_uncommitted_changes(&dir).unwrap());

        // The trailer is grep-pable in the raw commit body.
        let body = git(&dir, &["log", "-1", "--format=%B"]).unwrap();
        assert!(
            body.contains(&format!("Ralph-Skipped-Step: {trailer_id}")),
            "commit body missing trailer: {body}"
        );

        // git's own trailer parser also recognizes it as a real trailer.
        let parsed = git(
            &dir,
            &[
                "log",
                "-1",
                "--format=%(trailers:key=Ralph-Skipped-Step,valueonly)",
            ],
        )
        .unwrap();
        assert!(
            parsed.contains(trailer_id),
            "git did not parse trailer: {parsed:?}"
        );

        // The new file is tracked now.
        let tracked = git(&dir, &["ls-files"]).unwrap();
        assert!(tracked.contains("new.txt"), "new.txt not committed");
    }

    #[test]
    fn test_park_changes_discard_preserves_pre_existing_untracked() {
        let (_tmp, dir) = init_repo();

        // A pre-existing untracked file the user had before the run.
        fs::write(dir.join("user-scratch.txt"), "user data").unwrap();
        // The harness's work: a tracked modification + a new untracked file.
        fs::write(dir.join("README.md"), "# clobbered by harness").unwrap();
        fs::write(dir.join("agent-output.txt"), "agent junk").unwrap();

        let preserve = vec!["user-scratch.txt".to_string()];
        let outcome = park_changes(&dir, ParkStrategy::Discard, &preserve, "ignored").unwrap();
        assert_eq!(outcome, ParkOutcome::Discarded);

        // Tracked modification rolled back.
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "# hello"
        );
        // The harness's untracked file is gone.
        assert!(!dir.join("agent-output.txt").exists());
        // The user's pre-existing untracked file is preserved untouched.
        assert!(dir.join("user-scratch.txt").exists());
        assert_eq!(
            fs::read_to_string(dir.join("user-scratch.txt")).unwrap(),
            "user data"
        );
    }

    // ----- skip-WIP discovery & revert (STEP 19) -----

    /// Stage everything and write a WIP commit carrying the trailer, the same
    /// way `park_changes(Commit)` does. Returns the new commit SHA.
    fn commit_wip(dir: &Path, subject: &str, step_id: &str) -> String {
        let message = format!("{subject}\n\nRalph-Skipped-Step: {step_id}\n");
        commit_changes(dir, &message).unwrap();
        get_commit_hash(dir).unwrap()
    }

    #[test]
    fn test_parse_skipped_step_trailer_detects_only_real_trailer() {
        let (_tmp, dir) = init_repo();

        // A commit whose *body* merely mentions the token must NOT match.
        fs::write(dir.join("a.txt"), "1").unwrap();
        commit_changes(
            &dir,
            "normal commit\n\nWe discussed Ralph-Skipped-Step: not-a-trailer here in prose.\n",
        )
        .unwrap();
        let body_sha = get_commit_hash(&dir).unwrap();
        assert_eq!(
            parse_skipped_step_trailer(&dir, &body_sha).unwrap(),
            None,
            "prose mention must not be parsed as a trailer"
        );

        // A real trailer commit matches.
        fs::write(dir.join("b.txt"), "2").unwrap();
        let sha = commit_wip(&dir, "[ralph wip] skipped step 2: foo", "step-uuid-2");
        assert_eq!(
            parse_skipped_step_trailer(&dir, &sha).unwrap(),
            Some("step-uuid-2".to_string())
        );
    }

    #[test]
    fn test_list_skip_wip_commits_newest_first() {
        let (_tmp, dir) = init_repo();
        let branch = get_current_branch(&dir).unwrap();

        fs::write(dir.join("x.txt"), "1").unwrap();
        let first = commit_wip(&dir, "[ralph wip] skipped step 1: a", "step-A");
        // An ordinary commit in between — must be ignored.
        fs::write(dir.join("y.txt"), "2").unwrap();
        commit_changes(&dir, "ordinary work").unwrap();
        fs::write(dir.join("z.txt"), "3").unwrap();
        let second = commit_wip(&dir, "[ralph wip] skipped step 2: b", "step-B");

        let wips = list_skip_wip_commits(&dir, &branch).unwrap();
        assert_eq!(wips.len(), 2, "ordinary commit should be excluded");
        // Newest first.
        assert_eq!(wips[0].sha, second);
        assert_eq!(wips[0].step_id, "step-B");
        assert_eq!(wips[1].sha, first);
        assert_eq!(wips[1].step_id, "step-A");

        // Filtered convenience accessor.
        let only_a = skip_wip_commits_for_step(&dir, &branch, "step-A").unwrap();
        assert_eq!(only_a, vec![first]);
    }

    #[test]
    fn test_revert_commit_success() {
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("wip.txt"), "wip content").unwrap();
        let sha = commit_wip(&dir, "[ralph wip] skipped step 1: t", "step-1");
        assert!(dir.join("wip.txt").exists());

        match revert_commit(&dir, &sha).unwrap() {
            RevertOutcome::Reverted { revert_sha } => {
                assert_eq!(revert_sha, get_commit_hash(&dir).unwrap());
            }
            other => panic!("expected Reverted, got {other:?}"),
        }
        // The WIP file is gone, history preserved (3 commits: init, wip, revert).
        assert!(!dir.join("wip.txt").exists());
        let log = git(&dir, &["rev-list", "--count", "HEAD"]).unwrap();
        assert_eq!(log.trim(), "3");
    }

    #[test]
    fn test_revert_commit_not_on_head() {
        // Edge case: a later step committed on top of the WIP. Revert must
        // still work and must NOT touch the later commit's file.
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("wip.txt"), "wip").unwrap();
        let wip = commit_wip(&dir, "[ralph wip] skipped step 1: t", "step-1");
        fs::write(dir.join("later.txt"), "later step output").unwrap();
        commit_changes(&dir, "step 2 done").unwrap();

        assert!(matches!(
            revert_commit(&dir, &wip).unwrap(),
            RevertOutcome::Reverted { .. }
        ));
        assert!(!dir.join("wip.txt").exists(), "WIP change reverted");
        assert!(
            dir.join("later.txt").exists(),
            "later step's work preserved"
        );
    }

    #[test]
    fn test_revert_commit_already_reverted_is_noop() {
        // Edge case: the WIP was already manually reverted. A second revert is
        // an effective no-op — detect and report cleanly, no hard error, tree
        // left clean.
        let (_tmp, dir) = init_repo();
        fs::write(dir.join("wip.txt"), "wip").unwrap();
        let wip = commit_wip(&dir, "[ralph wip] skipped step 1: t", "step-1");

        assert!(matches!(
            revert_commit(&dir, &wip).unwrap(),
            RevertOutcome::Reverted { .. }
        ));
        // Second revert: already gone.
        let outcome = revert_commit(&dir, &wip).unwrap();
        assert_eq!(outcome, RevertOutcome::AlreadyReverted);
        // Tree is clean and no revert-in-progress is wedged.
        assert!(!has_uncommitted_changes(&dir).unwrap());
        assert!(!dir.join(".git").join("REVERT_HEAD").exists());
    }

    #[test]
    fn test_revert_multiple_wip_commits_newest_first() {
        // Edge case: the same step was skipped+committed more than once.
        // Reverting newest-first applies each revert cleanly.
        let (_tmp, dir) = init_repo();
        let branch = get_current_branch(&dir).unwrap();

        fs::write(dir.join("f.txt"), "v1\n").unwrap();
        let first = commit_wip(&dir, "[ralph wip] skipped step 1: a", "step-1");
        fs::write(dir.join("f.txt"), "v1\nv2\n").unwrap();
        let second = commit_wip(&dir, "[ralph wip] skipped step 1: a again", "step-1");

        let shas = skip_wip_commits_for_step(&dir, &branch, "step-1").unwrap();
        assert_eq!(shas, vec![second.clone(), first.clone()], "newest first");

        for sha in &shas {
            assert!(matches!(
                revert_commit(&dir, sha).unwrap(),
                RevertOutcome::Reverted { .. }
            ));
        }
        // Both layers undone; f.txt no longer exists (back to init state).
        assert!(!dir.join("f.txt").exists());
        assert!(!has_uncommitted_changes(&dir).unwrap());
    }

    // ----- Per-iteration commit message + Ralph-* trailers (STEP 32/34) -----

    #[test]
    fn test_sanitize_commit_subject_collapses_and_caps() {
        assert_eq!(
            sanitize_commit_subject("Add  OAuth\tlogin"),
            "Add OAuth login"
        );
        assert_eq!(
            sanitize_commit_subject("line one\nline two\n\nline three"),
            "line one line two line three"
        );
        assert_eq!(sanitize_commit_subject("  trim me  "), "trim me");
        let long = "x".repeat(200);
        assert_eq!(sanitize_commit_subject(&long).chars().count(), 72);
        // Multibyte titles cap on a char boundary (no panic).
        let multi = "é".repeat(100);
        assert_eq!(sanitize_commit_subject(&multi).chars().count(), 72);
    }

    #[test]
    fn test_build_iteration_commit_message_subject_and_trailers() {
        let msg = build_iteration_commit_message("a1b2c3d4", 3, "Add  the\nthing", "my-plan");
        let mut lines = msg.lines();
        assert_eq!(
            lines.next().unwrap(),
            "ralph a1b2c3d4.3 - Add the thing",
            "subject is `ralph <short_id>.<n> - <sanitized title>`"
        );
        assert!(msg.contains("\nRalph-Plan: my-plan\n"));
        assert!(msg.contains("\nRalph-Step: a1b2c3d4\n"));
        assert!(msg.contains("\nRalph-Iteration: 3\n"));
        assert!(msg.contains("\nRalph-Review: pending\n"));
    }

    #[test]
    fn test_iteration_commit_trailers_parsed_by_git_not_subject() {
        let (_tmp, dir) = init_repo();
        let branch = get_current_branch(&dir).unwrap();

        // Commit two iterations for one step + one for another.
        fs::write(dir.join("a.txt"), "1").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("STEPAAAA", 1, "A", "p"),
        )
        .unwrap();
        let a1 = get_commit_hash(&dir).unwrap();
        fs::write(dir.join("a.txt"), "2").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("STEPAAAA", 2, "A", "p"),
        )
        .unwrap();
        let a2 = get_commit_hash(&dir).unwrap();
        fs::write(dir.join("b.txt"), "1").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("STEPBBBB", 1, "B", "p"),
        )
        .unwrap();
        let b1 = get_commit_hash(&dir).unwrap();

        // Trailer parsing is by git's own parser (not subject scraping):
        // a prose mention must not false-match.
        fs::write(dir.join("c.txt"), "1").unwrap();
        commit_changes(&dir, "talks about Ralph-Step: NOTATRAILER inline\n").unwrap();
        let prose = get_commit_hash(&dir).unwrap();
        assert_eq!(
            parse_trailer(&dir, &prose, ITERATION_STEP_TRAILER).unwrap(),
            None
        );

        // All iteration commits discovered, newest-first, grouped by step.
        let all = list_iteration_commits(&dir, &branch).unwrap();
        let shas: Vec<&str> = all.iter().map(|c| c.sha.as_str()).collect();
        assert_eq!(shas, vec![b1.as_str(), a2.as_str(), a1.as_str()]);

        let a = iteration_commits_for_step(&dir, &branch, "STEPAAAA").unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].iteration, 2);
        assert_eq!(a[1].iteration, 1);
        assert_eq!(a[0].plan_slug.as_deref(), Some("p"));

        // step reset isolation: ordering a step's SHAs newest-first and
        // reverting them touches ONLY that step's commits.
        let targets: Vec<String> = a.iter().map(|c| c.sha.clone()).collect();
        let ordered = order_shas_newest_first(&dir, &branch, &targets).unwrap();
        assert_eq!(ordered, vec![a2.clone(), a1.clone()]);
        for sha in &ordered {
            assert!(matches!(
                revert_commit(&dir, sha).unwrap(),
                RevertOutcome::Reverted { .. }
            ));
        }
        // Step A's file is gone (both iterations reverted); step B's stays.
        assert!(!dir.join("a.txt").exists(), "A reverted");
        assert_eq!(
            fs::read_to_string(dir.join("b.txt")).unwrap(),
            "1",
            "B intact"
        );
    }

    #[test]
    fn test_squash_since_collapses_to_one_commit_preserving_tree() {
        let (_tmp, dir) = init_repo();
        let base = get_commit_hash(&dir).unwrap();

        fs::write(dir.join("acc.txt"), "1\n").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("SQ123456", 1, "Acc", "p"),
        )
        .unwrap();
        fs::write(dir.join("acc.txt"), "1\n2\n").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("SQ123456", 2, "Acc", "p"),
        )
        .unwrap();
        fs::write(dir.join("acc.txt"), "1\n2\n3\n").unwrap();
        commit_changes(
            &dir,
            &build_iteration_commit_message("SQ123456", 3, "Acc", "p"),
        )
        .unwrap();

        let count_before = git(&dir, &["rev-list", "--count", "HEAD"])
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();

        let squash_msg = build_iteration_commit_message("SQ123456", 3, "Acc", "p");
        let sha = squash_since(&dir, &base, &squash_msg).unwrap();

        let count_after = git(&dir, &["rev-list", "--count", "HEAD"])
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap();
        assert_eq!(
            count_after,
            count_before - 2,
            "3 iteration commits collapse into 1"
        );
        // The squashed commit keeps the final tree state and the trailers.
        assert_eq!(
            fs::read_to_string(dir.join("acc.txt")).unwrap(),
            "1\n2\n3\n"
        );
        assert_eq!(
            parse_trailer(&dir, &sha, ITERATION_STEP_TRAILER)
                .unwrap()
                .as_deref(),
            Some("SQ123456")
        );
        assert_eq!(
            parse_trailer(&dir, &sha, ITERATION_NUM_TRAILER)
                .unwrap()
                .as_deref(),
            Some("3"),
            "Ralph-Iteration collapsed to the final n"
        );
    }
}
