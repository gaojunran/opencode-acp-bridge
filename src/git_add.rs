//! Turn-scoped staging (`--zed-git-add`, Release 0.8.0) — the bridge-native
//! port of the `opencode-git-add` plugin's v2 lane semantics.
//!
//! Mechanism: the opencode server's NATIVE snapshot diff — each
//! `session.step.ended` carries `files`: a `git diff --name-only
//! --no-renames` of the step's start/end trees, as worktree-TOP-relative
//! paths (session-dir scope, untracked ≤2 MiB included, ignored files and
//! paths outside the session dir excluded). The bridge collects those
//! paths per ROOT session (a subagent's steps count as its ROOT session's
//! agent changes), then at the next USER-initiated prompt on the ROOT
//! session runs `git add -- <paths>` for exactly those paths and clears
//! the set. Zed's unstaged-changes view then always shows only the CURRENT
//! turn's changes, and unrelated working-tree edits (the user's own) are
//! never touched. Turn with no diff → nothing staged — deliberately NOT
//! `git add .`.
//!
//! Accepted premises (user-decided, documented in docs/opencode-api.md):
//! - the snapshot has NO tool attribution — anything an agent turn changes
//!   on disk within the session dir lands in `files` (bash included;
//!   concurrent manual/other-session edits within the turn window too —
//!   assumed: one session edits the same directory at a time);
//! - interrupted steps carry no `files` (no finish/failure) — not chased;
//! - ignored files / out-of-session-dir paths / non-git projects are not
//!   covered by the server, hence not staged.
//!
//! Deviations/policies:
//! - the pending set is keyed by ROOT SESSION and stores RAW
//!   worktree-relative strings (no early resolution — the bridge and the
//!   server may run on different machines; the snapshot paths are
//!   SERVER-side);
//! - paths resolve at STAGING time via `git -C <cwd> rev-parse
//!   --show-toplevel` (the LOCAL worktree top), then the existing topology
//!   guards apply (containment inside cwd, local existence, cwd inside a
//!   git work tree) — any failed guard retains the whole set, never stages
//!   partially;
//! - `git add` failures retry `STAGING_ATTEMPTS` times (`STAGING_RETRY_DELAY`
//!   apart) and retain the set on final failure (staged at the next prompt).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Retry budget for a failing `git add` (git failures — most commonly a
/// concurrent process holding `.git/index.lock`, which exits 128 — must
/// never kill the user's message; matches the plugin's budget).
pub const STAGING_ATTEMPTS: usize = 3;
/// Delay between `git add` retry attempts (matches the plugin).
pub const STAGING_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// Pending paths of one ROOT session: what the agent touched since the last
/// staging — RAW worktree-relative path strings from the server's snapshot
/// diffs. Cleared only by a successful staging (any failure retains — no
/// data loss, staged at the next prompt).
#[derive(Default)]
pub struct GitAddState {
    pending: HashSet<String>,
}

impl GitAddState {
    pub fn add(&mut self, rel: impl Into<String>) {
        self.pending.insert(rel.into());
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Snapshot of the pending paths (deduped raw strings).
    pub fn pending(&self) -> Vec<String> {
        self.pending.iter().cloned().collect()
    }

    pub fn clear(&mut self) {
        self.pending.clear();
    }
}

/// Containment check — `abs` is inside `dir` (component-boundary aware,
/// so `/tmp/foo2` is NOT inside `/tmp/foo`).
pub fn is_inside(dir: &Path, abs: &Path) -> bool {
    abs == dir || abs.starts_with(dir)
}

/// Resolve ONE raw path (a snapshot-diff entry, worktree-TOP-relative)
/// against the LOCAL worktree top and reject anything that escapes it —
/// a path outside the tree is NEVER staged. Absolute forms resolve
/// verbatim; both forms normalize `.`/`..` (defense — the server's diff
/// never emits parent escapes).
pub fn resolve_worktree(top: &Path, p: &str) -> Option<PathBuf> {
    if p.is_empty() {
        return None;
    }
    let raw = if Path::new(p).is_absolute() {
        PathBuf::from(p)
    } else {
        top.join(p)
    };
    let abs = normalize(&raw);
    is_inside(top, &abs).then_some(abs)
}

/// Lexically normalize `.` and `..` components without touching the
/// filesystem (canonicalize would fail for deleted/staged paths and follow
/// symlinks the server may not see).
fn normalize(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Outcome of one [`stage_paths`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingOutcome {
    /// All paths staged; the caller clears the pending set.
    Staged,
    /// Topology pre-check failed (a path missing locally, or the cwd not
    /// inside a git work tree) — nothing was attempted; retain the set.
    PrecheckFailed,
    /// `git add` failed after all retries; retain the set.
    Failed,
}

/// The LOCAL worktree top for `cwd`, or `None` when `cwd` is not inside a
/// git work tree (`git -C <cwd> rev-parse --show-toplevel` — the top itself
/// is the guard; one invocation instead of two).
fn worktree_top(cwd: &Path) -> Option<PathBuf> {
    std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| {
            let top = String::from_utf8_lossy(&out.stdout).trim().to_string();
            (!top.is_empty()).then(|| PathBuf::from(top))
        })
}

/// Stage the ROOT session's pending snapshot-diff paths: resolve each RAW
/// worktree-TOP-relative path against the LOCAL worktree top (the bridge
/// and the server may run on different machines — the recorded paths are
/// server-side, the resolution is local), then run
/// `git -C <cwd> add -- <resolved>` (the `--` stops option parsing — paths
/// stay literal, no shell, no glob expansion).
///
/// Pre-checks (topology guards): the worktree top must resolve, every
/// resolved path must stay inside `cwd` (the session scope) and EXIST on
/// the local filesystem. Any failure → `PrecheckFailed` with NOTHING
/// attempted (never a partial stage). `git add` failures retry
/// [`STAGING_ATTEMPTS`] times, then `Failed`. Staging never throws — a git
/// failure must never kill the user's message.
pub async fn stage_paths(cwd: &Path, files: &HashSet<String>) -> StagingOutcome {
    let Some(top) = worktree_top(cwd) else {
        tracing::warn!(cwd = %cwd.display(), "zed-git-add: pre-check failed — cwd is not inside a git work tree");
        return StagingOutcome::PrecheckFailed;
    };
    let mut resolved: Vec<PathBuf> = Vec::with_capacity(files.len());
    let mut skipped: Vec<String> = Vec::new();
    for f in files {
        match resolve_worktree(&top, f) {
            Some(abs) if is_inside(cwd, &abs) && abs.exists() => resolved.push(abs),
            Some(abs) if is_inside(cwd, &abs) => {
                skipped.push(format!("{} (missing locally)", abs.display()))
            }
            Some(abs) => skipped.push(format!("{} (outside the session directory)", abs.display())),
            None => skipped.push(format!("{f:?} (unresolvable / outside the worktree top)")),
        }
    }
    if !skipped.is_empty() {
        tracing::warn!(
            cwd = %cwd.display(),
            skipped = ?skipped,
            "zed-git-add: pre-check failed — path(s) fail the topology guards; NOTHING staged, retaining"
        );
        return StagingOutcome::PrecheckFailed;
    }
    for attempt in 1..=STAGING_ATTEMPTS {
        let run = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .arg("add")
            .arg("--")
            .args(&resolved)
            .output();
        match run {
            Ok(out) if out.status.success() => return StagingOutcome::Staged,
            Ok(out) => {
                tracing::warn!(
                    cwd = %cwd.display(),
                    attempt,
                    stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                    "zed-git-add: git add failed, retrying"
                );
            }
            Err(e) => {
                tracing::warn!(cwd = %cwd.display(), attempt, error = %e, "zed-git-add: git add could not be spawned, retrying");
            }
        }
        if attempt < STAGING_ATTEMPTS {
            tokio::time::sleep(STAGING_RETRY_DELAY).await;
        }
    }
    StagingOutcome::Failed
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scaffold a temp git repo + a temp non-repo dir (the CALLER must call
    /// `drop` on the guard to remove them).
    fn scaffold(repo: bool) -> (tempdir_guard::TempDir, PathBuf) {
        let dir = tempdir_guard::TempDir::new("zed-git-add-test");
        if repo {
            let out = std::process::Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(&dir.path)
                .output()
                .expect("git init runs in tests");
            assert!(out.status.success(), "git init failed");
        }
        let path = dir.path.clone();
        (dir, path)
    }

    #[test]
    fn resolve_worktree_resolves_relative_and_rejects_escapes() {
        let top = std::path::Path::new("/proj");
        // Worktree-top-relative entries resolve against the top.
        assert_eq!(
            resolve_worktree(top, "notes.txt"),
            Some(PathBuf::from("/proj/notes.txt"))
        );
        assert_eq!(
            resolve_worktree(top, "sub/a.txt"),
            Some(PathBuf::from("/proj/sub/a.txt"))
        );
        // `..` escapes are rejected in BOTH forms (defense — the server's
        // snapshot diff never emits them).
        assert_eq!(resolve_worktree(top, "../evil.txt"), None);
        assert_eq!(resolve_worktree(top, "/etc/passwd"), None);
        assert_eq!(resolve_worktree(top, "/proj/../proj2/evil.txt"), None);
        // Harmless `..` that stays inside normalizes fine.
        assert_eq!(
            resolve_worktree(top, "proj/../notes.txt"),
            Some(PathBuf::from("/proj/notes.txt"))
        );
        // Empty entry — nothing.
        assert_eq!(resolve_worktree(top, ""), None);
    }

    #[test]
    fn pending_raw_strings_dedup_and_clear() {
        let mut state = GitAddState::default();
        state.add("notes.txt");
        state.add("notes.txt");
        state.add("sub/a.txt");
        let mut pending = state.pending();
        pending.sort();
        assert_eq!(pending, vec!["notes.txt", "sub/a.txt"], "raw strings, deduped");
        state.clear();
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn staging_resolves_raw_relative_paths_against_worktree_top() {
        // The RAW pending paths are worktree-TOP-relative: staging from a
        // session whose cwd is a SUBDIR of the worktree must still resolve
        // them against the top (git diff --name-only always emits
        // top-relative paths) — the session dir's files come back as
        // "sub/…" entries and land inside the session cwd.
        let (_guard, repo) = scaffold(true);
        let sub = repo.join("sub");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("a.txt"), "hello").unwrap();
        let mut state = GitAddState::default();
        state.add("sub/a.txt");
        let outcome = stage_paths(&sub, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::Staged);
        assert_eq!(staged_files(&repo), vec!["sub/a.txt"]);
        state.clear();
    }

    #[tokio::test]
    async fn staging_executes_git_add_and_clears_pending() {
        let (_guard, repo) = scaffold(true);
        std::fs::write(repo.join("notes.txt"), "hello").unwrap();
        std::fs::write(repo.join("unrelated.txt"), "user manual edit").unwrap();
        let mut state = GitAddState::default();
        state.add("notes.txt");
        let outcome = stage_paths(&repo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::Staged);
        // EXACTLY the tracked file is staged — the manual edit is untouched.
        assert_eq!(staged_files(&repo), vec!["notes.txt"]);
        state.clear();
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn precheck_failure_retains_pending() {
        // Not a git work tree → PrecheckFailed, nothing attempted.
        let (_guard_dir, nonrepo) = scaffold(false);
        let mut state = GitAddState::default();
        state.add("no-such.txt");
        let outcome = stage_paths(&nonrepo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::PrecheckFailed);
        assert!(!state.is_empty(), "retained on pre-check failure");

        // A git repo where the recorded path does not exist locally →
        // PrecheckFailed too (never a partial stage).
        let (_guard_repo, repo) = scaffold(true);
        let mut state2 = GitAddState::default();
        state2.add("never-created.txt");
        let outcome = stage_paths(&repo, &state2.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::PrecheckFailed);
        assert!(!state2.is_empty());

        // A path that resolves INSIDE the worktree top but OUTSIDE the
        // session cwd (top-relative sibling) → session-scope guard rejects.
        let (_guard_repo2, repo2) = scaffold(true);
        std::fs::write(repo2.join("sibling.txt"), "x").unwrap();
        let sub = repo2.join("sub");
        std::fs::create_dir(&sub).unwrap();
        let mut state3 = GitAddState::default();
        state3.add("sibling.txt");
        let outcome = stage_paths(&sub, &state3.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::PrecheckFailed);
        assert!(!state3.is_empty());
    }

    #[tokio::test]
    async fn git_add_failure_retries_and_retains_then_recovers() {
        let (_guard, repo) = scaffold(true);
        std::fs::write(repo.join("a.txt"), "hello").unwrap();
        let mut state = GitAddState::default();
        state.add("a.txt");
        // A concurrent process holding .git/index.lock (exit 128).
        std::fs::write(repo.join(".git/index.lock"), "held by another process").unwrap();
        let started = std::time::Instant::now();
        let outcome = stage_paths(&repo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::Failed);
        assert!(
            started.elapsed() >= STAGING_RETRY_DELAY * (STAGING_ATTEMPTS as u32 - 1),
            "retried with the configured delay"
        );
        assert!(!state.is_empty(), "retained on git failure");
        // Once the lock is gone, the next attempt stages normally.
        std::fs::remove_file(repo.join(".git/index.lock")).unwrap();
        let outcome = stage_paths(&repo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::Staged);
    }

    fn staged_files(repo: &Path) -> Vec<String> {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["-c", "diff.renames=false", "diff", "--cached", "--name-only"])
            .output()
            .expect("git diff runs in tests");
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_string)
            .collect()
    }
}

/// Minimal temp-dir guard (the tests need real repos; `tempfile` is not a
/// dependency). Removes the directory on drop.
#[cfg(test)]
pub(crate) mod tempdir_guard {
    use std::path::PathBuf;

    pub struct TempDir {
        pub path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> Self {
            let base = std::env::temp_dir();
            let unique = format!(
                "{prefix}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let path = base.join(unique);
            std::fs::create_dir_all(&path).expect("create temp dir");
            TempDir { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
