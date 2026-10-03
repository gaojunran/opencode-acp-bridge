//! Turn-scoped staging (`--zed-git-add`, Release 0.7.0) — the bridge-native
//! port of the `opencode-git-add` plugin's v2 lane semantics.
//!
//! Mechanism: track the paths touched by completed `write` / `edit` /
//! `apply_patch` tool calls (from ALL sessions — a subagent's writes count
//! as its ROOT session's agent changes), then at the next user prompt on
//! the ROOT session run `git add -- <paths>` limited to EXACTLY those
//! paths, and clear the set. Zed's unstaged-changes view then always shows
//! only the CURRENT turn's changes, and unrelated working-tree edits (the
//! user's own) are never touched. Turn with no tracked writes → nothing
//! staged — deliberately NOT `git add .`.
//!
//! Deliberate deviations from the plugin (documented in docs/opencode-api.md):
//! - the pending set is keyed by ROOT SESSION (the plugin keys by project
//!   directory); children's edits resolve to their parent via the
//!   Release 0.6.0 child maps, and a child's own prompt never stages;
//! - pre-staging topology guards: every path must EXIST locally and the
//!   session cwd must be inside a git work tree (the bridge and the server
//!   may run on different machines — tool-event paths are SERVER-side); any
//!   failed pre-check retains the set, never stages partially;
//! - `git add` failures retry `STAGING_ATTEMPTS` times (`STAGING_RETRY_DELAY`
//!   apart) and retain the set on final failure (staged at the next prompt).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub use crate::dto::ToolMetadata;

/// Retry budget for a failing `git add` (git failures — most commonly a
/// concurrent process holding `.git/index.lock`, which exits 128 — must
/// never kill the user's message; matches the plugin's budget).
pub const STAGING_ATTEMPTS: usize = 3;
/// Delay between `git add` retry attempts (matches the plugin).
pub const STAGING_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// Tools whose completed writes are tracked. `write`/`edit` carry the path
/// directly in `input.path`; `apply_patch` carries no path field, so its
/// paths come from the result metadata `files[]` list or the patch-text
/// file headers (see [`patch_text_paths`]).
pub const TRACKED_TOOLS: [&str; 3] = ["write", "edit", "apply_patch"];

/// Is this tool's completed write tracked?
pub fn is_tracked_tool(tool: &str) -> bool {
    TRACKED_TOOLS.contains(&tool)
}

/// One tracked path with the source it was extracted from (journaling).
pub struct Touched {
    pub abs: PathBuf,
    pub source: &'static str,
}

/// Pending paths of one ROOT session: what the agent touched since the last
/// staging. Cleared only by a successful staging (any failure retains — no
/// data loss, staged at the next prompt).
#[derive(Default)]
pub struct GitAddState {
    pending: HashSet<PathBuf>,
}

impl GitAddState {
    pub fn add(&mut self, abs: PathBuf) {
        self.pending.insert(abs);
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Snapshot of the pending paths (deduped).
    pub fn pending(&self) -> Vec<PathBuf> {
        self.pending.iter().cloned().collect()
    }

    pub fn clear(&mut self) {
        self.pending.clear();
    }
}

/// The plugin's apply_patch patch-text file-section headers. Every file
/// section opens with one of these lines; `*** Begin Patch` / `*** End
/// Patch` delimit the whole block and match neither.
///
/// Faithful port of the plugin's regexes
/// (`/^\*\*\* (Update File|Add File|Delete File|Move to): (.*)$/` and
/// `/^\*\*\* Rename File: (.*) to (.*)$/` — dot excludes `\r`, so CRLF
/// lines and empty paths are skipped exactly like upstream):
/// - rename forms count BOTH the old and the new path as touched;
/// - `*** Update File: <a>` + `*** Move to: <b>` counts both sides too;
/// - garbage patchText (bogus ops, empty paths, bare content) → nothing.
pub fn patch_text_paths(patch_text: &str) -> Vec<String> {
    let mut paths = HashSet::new();
    for line in patch_text.split('\n') {
        if let Some(rest) = line.strip_prefix("*** Rename File: ") {
            // The plugin's greedy `(.*) to (.*)` backtracks to the LAST
            // ` to ` — mirror that with rsplit_once.
            if let Some((a, b)) = rest.rsplit_once(" to ") {
                if !a.is_empty() && !a.contains('\r') {
                    paths.insert(a.to_string());
                }
                if !b.is_empty() && !b.contains('\r') {
                    paths.insert(b.to_string());
                }
            }
            continue;
        }
        for op in ["Update File: ", "Add File: ", "Delete File: ", "Move to: "] {
            if let Some(rest) = line.strip_prefix("*** ").and_then(|l| l.strip_prefix(op)) {
                if !rest.is_empty() && !rest.contains('\r') {
                    paths.insert(rest.to_string());
                }
            }
        }
    }
    paths.into_iter().collect()
}

/// Containment check — `abs` is inside `dir` (component-boundary aware,
/// so `/tmp/foo2` is NOT inside `/tmp/foo`).
pub fn is_inside(dir: &Path, abs: &Path) -> bool {
    abs == dir || abs.starts_with(dir)
}

/// Resolve a tool path (absolute or cwd-relative) and reject anything that
/// escapes the session's cwd — a path outside the tree is NEVER staged.
pub fn resolve_tracked(dir: &Path, p: &str) -> Option<PathBuf> {
    let raw = if Path::new(p).is_absolute() {
        PathBuf::from(p)
    } else {
        dir.join(p)
    };
    // Normalize `.`/`..` segments (the plugin's `path.resolve` does this for
    // relative paths; we do it for BOTH forms — an absolute path with `..`
    // must not bypass the containment check).
    let abs = normalize(&raw);
    is_inside(dir, &abs).then_some(abs)
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

/// Extract the touched paths of ONE completed tool call, resolved against
/// the session's cwd and containment-checked. `input` is the cached tool
/// input (from the mapping's input cache); `metadata` the tool result
/// metadata — for `apply_patch` the result `files[].filePath` list is
/// AUTHORITATIVE when present (even `files: []` falls back to the
/// patch-text headers, matching the plugin).
///
/// Only completed writes reach this function — `ToolFailed` events are
/// never wired to it (the plugin's `status == "completed"` check).
pub fn extract_touched(
    tool: &str,
    cwd: &Path,
    input: Option<&serde_json::Value>,
    metadata: Option<&ToolMetadata>,
) -> Vec<Touched> {
    let mut out = Vec::new();
    match tool {
        "write" | "edit" => {
            if let Some(p) = input.and_then(|i| i.get("path")).and_then(|v| v.as_str()) {
                if !p.is_empty() {
                    if let Some(abs) = resolve_tracked(cwd, p) {
                        out.push(Touched { abs, source: "input.path" });
                    }
                }
            }
        }
        "apply_patch" => {
            let files = metadata.and_then(|m| m.files.as_ref());
            if let Some(files) = files {
                if !files.is_empty() {
                    for f in files {
                        if f.file_path.is_empty() {
                            continue;
                        }
                        if let Some(abs) = resolve_tracked(cwd, &f.file_path) {
                            out.push(Touched { abs, source: "metadata.files" });
                        }
                    }
                    return out; // metadata wins — no patch-text fallback
                }
            }
            if let Some(text) = input.and_then(|i| i.get("patchText")).and_then(|v| v.as_str()) {
                for p in patch_text_paths(text) {
                    if let Some(abs) = resolve_tracked(cwd, &p) {
                        out.push(Touched { abs, source: "patchText" });
                    }
                }
            }
        }
        _ => {}
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

/// Is `cwd` inside a git work tree? (`git rev-parse --is-inside-work-tree` —
/// false for bare repositories and non-repos, per the design.)
fn is_git_work_tree(cwd: &Path) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .map(|out| {
            out.status.success()
                && String::from_utf8_lossy(&out.stdout).trim() == "true"
        })
        .unwrap_or(false)
}

/// Stage exactly the given absolute paths: `git -C <cwd> add -- <paths>`
/// (the `--` stops option parsing — paths stay literal, no shell, no glob
/// expansion). Pathspec-limited staging also stages deletions of
/// now-missing files upstream; the exists() pre-check here deliberately
/// does NOT (see the module doc — topology guards win).
///
/// Pre-checks (topology guards — the bridge and the server may run on
/// different machines; tool-event paths are SERVER-side): every path must
/// EXIST on the local filesystem and the cwd must be inside a git work
/// tree. Any failure → `PrecheckFailed` with NOTHING attempted (never a
/// partial stage). `git add` failures retry [`STAGING_ATTEMPTS`] times,
/// then `Failed`. Staging never throws — a git failure must never kill the
/// user's message.
pub async fn stage_paths(cwd: &Path, paths: &HashSet<PathBuf>) -> StagingOutcome {
    if !is_git_work_tree(cwd) {
        tracing::warn!(cwd = %cwd.display(), "zed-git-add: pre-check failed — cwd is not inside a git work tree");
        return StagingOutcome::PrecheckFailed;
    }
    let missing: Vec<_> = paths.iter().filter(|p| !p.exists()).collect();
    if !missing.is_empty() {
        tracing::warn!(
            cwd = %cwd.display(),
            missing = ?missing.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
            "zed-git-add: pre-check failed — path(s) missing on the local filesystem"
        );
        return StagingOutcome::PrecheckFailed;
    }
    for attempt in 1..=STAGING_ATTEMPTS {
        let run = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .arg("add")
            .arg("--")
            .args(paths)
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
    fn patch_text_headers_all_forms() {
        let text = [
            "*** Begin Patch",
            "*** Update File: upd.txt",
            "+b",
            "*** Add File: new.txt",
            "+n",
            "*** Delete File: del.txt",
            "*** Rename File: ren.txt to ren2.txt",
            "*** Update File: mov.txt",
            "*** Move to: mov2.txt",
            "*** End Patch",
        ]
        .join("\n");
        let mut paths = patch_text_paths(&text);
        paths.sort();
        assert_eq!(
            paths,
            vec!["del.txt", "mov.txt", "mov2.txt", "new.txt", "ren.txt", "ren2.txt", "upd.txt"]
        );
    }

    #[test]
    fn garbage_patch_text_tracks_nothing() {
        let text = [
            "*** Begin Patch",
            "*** Bogus Op: sneaky.txt",
            "*** Update File: ",
            "just some content",
            "*** End Patch",
        ]
        .join("\n");
        assert!(patch_text_paths(&text).is_empty());
    }

    #[test]
    fn crlf_patch_lines_are_skipped_like_upstream() {
        // The plugin's `(.*)$` excludes \r — a CRLF patchText yields nothing.
        assert!(patch_text_paths("*** Update File: notes.txt\r\n").is_empty());
    }

    #[test]
    fn rename_backtracks_to_last_separator() {
        // Greedy `(.*) to (.*)` splits at the LAST " to ".
        let paths = patch_text_paths("*** Rename File: my to file.txt to final.txt");
        assert!(paths.iter().any(|p| p == "my to file.txt"));
        assert!(paths.iter().any(|p| p == "final.txt"));
    }

    #[test]
    fn extract_write_and_edit_input_paths() {
        let cwd = std::path::Path::new("/proj");
        for tool in ["write", "edit"] {
            let out = extract_touched(
                tool,
                cwd,
                Some(&serde_json::json!({"path": "notes.txt", "content": "x"})),
                None,
            );
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].abs, PathBuf::from("/proj/notes.txt"));
            assert_eq!(out[0].source, "input.path");
        }
    }

    #[test]
    fn apply_patch_metadata_files_win_over_patch_text() {
        let cwd = std::path::Path::new("/proj");
        let meta = ToolMetadata {
            diff: None,
            filediff: None,
            files: Some(vec![crate::dto::FileEntry {
                file_path: "/proj/from-meta.txt".into(),
                relative_path: Some("from-meta.txt".into()),
                r#type: None,
                patch: String::new(),
                additions: None,
                deletions: None,
                move_path: None,
            }]),
            title: None,
            truncated: None,
            diagnostics: None,
        };
        let out = extract_touched(
            "apply_patch",
            cwd,
            Some(&serde_json::json!({"patchText": "*** Update File: from-headers.txt"})),
            Some(&meta),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].abs, PathBuf::from("/proj/from-meta.txt"));
        assert_eq!(out[0].source, "metadata.files");
    }

    #[test]
    fn apply_patch_empty_metadata_falls_back_to_patch_text() {
        let cwd = std::path::Path::new("/proj");
        let meta = ToolMetadata {
            files: Some(vec![]),
            ..ToolMetadata::default()
        };
        let out = extract_touched(
            "apply_patch",
            cwd,
            Some(&serde_json::json!({"patchText": "*** Update File: notes.txt\n*** Add File: new.txt"})),
            Some(&meta),
        );
        let mut abs: Vec<_> = out.iter().map(|t| t.abs.clone()).collect();
        abs.sort();
        assert_eq!(abs, vec![PathBuf::from("/proj/new.txt"), PathBuf::from("/proj/notes.txt")]);
        assert!(out.iter().all(|t| t.source == "patchText"));
    }

    #[test]
    fn containment_rejects_outside_paths() {
        let cwd = std::path::Path::new("/proj");
        // Relative escape.
        assert!(extract_touched("write", cwd, Some(&serde_json::json!({"path": "../evil.txt"})), None).is_empty());
        // Absolute outside.
        assert!(extract_touched("edit", cwd, Some(&serde_json::json!({"path": "/etc/passwd"})), None).is_empty());
        // Empty path.
        assert!(extract_touched("write", cwd, Some(&serde_json::json!({"path": ""})), None).is_empty());
        // apply_patch metadata path that escapes.
        let meta = ToolMetadata {
            files: Some(vec![crate::dto::FileEntry {
                file_path: "/etc/passwd".into(),
                ..Default::default()
            }]),
            ..ToolMetadata::default()
        };
        assert!(extract_touched("apply_patch", cwd, None, Some(&meta)).is_empty());
        // Sibling prefix is NOT inside (component boundary).
        assert!(extract_touched("write", cwd, Some(&serde_json::json!({"path": "/proj2/x.txt"})), None).is_empty());
        // Untracked tool.
        assert!(extract_touched("bash", cwd, Some(&serde_json::json!({"path": "x"})), None).is_empty());
    }

    #[tokio::test]
    async fn staging_executes_git_add_and_clears_pending() {
        let (_guard, repo) = scaffold(true);
        std::fs::write(repo.join("notes.txt"), "hello").unwrap();
        std::fs::write(repo.join("unrelated.txt"), "user manual edit").unwrap();
        let mut state = GitAddState::default();
        state.add(repo.join("notes.txt"));
        let outcome = stage_paths(&repo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::Staged);
        // EXACTLY the tracked file is staged — the manual edit is untouched.
        let staged = staged_files(&repo);
        assert_eq!(staged, vec!["notes.txt"]);
        state.clear();
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn precheck_failure_retains_pending() {
        let (_guard_dir, nonrepo) = scaffold(false);
        let missing = nonrepo.join("no-such.txt");
        let mut state = GitAddState::default();
        state.add(missing.clone());
        // Not a git work tree → PrecheckFailed, nothing attempted.
        let outcome = stage_paths(&nonrepo, &state.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::PrecheckFailed);
        assert!(!state.is_empty(), "retained on pre-check failure");

        // A git repo without the tracked file existing → PrecheckFailed too.
        let (_guard_repo, repo) = scaffold(true);
        let mut state2 = GitAddState::default();
        state2.add(repo.join("never-created.txt"));
        let outcome = stage_paths(&repo, &state2.pending().into_iter().collect()).await;
        assert_eq!(outcome, StagingOutcome::PrecheckFailed);
        assert!(!state2.is_empty());
    }

    #[tokio::test]
    async fn git_add_failure_retries_and_retains_then_recovers() {
        let (_guard, repo) = scaffold(true);
        std::fs::write(repo.join("a.txt"), "hello").unwrap();
        let mut state = GitAddState::default();
        state.add(repo.join("a.txt"));
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
