//! #52636 diff-fix: `dto::ToolMetadata` → ACP `ToolCallContent::Diff` blocks.
//!
//! The 2.0.21 wire carries file changes in `session.tool.success` /
//! persisted completed-tool metadata. Priority chain (dialect-neutral —
//! core and the aft hoist share every shape):
//!
//! 1. `metadata.filediff` — `{file: <absolute path>, patch: <unified diff>, ...}`
//! 2. `metadata.files[]` — one per file: `{filePath, relativePath, type,
//!    patch, additions, deletions}`; `filePath` is authoritative (never
//!    derived from the patch text) and each entry's `patch` is its own
//!    `Index:` section. Present ⇒ authoritative: a failing entry is
//!    skipped with a warning, there is NO fallback to the combined string.
//! 3. `metadata.diff` — the combined patch as a single string (fallback;
//!    may contain several `Index:` sections for multi-file tools).
//!
//! `patch` is an SVN-style unified diff with an `Index: <path>` header. Real
//! fixture quirk: **new files end with a stray `-\n` line** (a phantom empty
//! removed line) that must not leak into `oldText`.
//!
//! Contract (ACP v1): `Diff.old_text` is `None` for new files; `new_text`
//! carries the post-change content; a deletion yields `new_text = ""`.
//!
//! Robustness rule (the actual #52636 bug class): a malformed/unparseable
//! patch must NEVER break the tool-call completion — we `tracing::warn` and
//! drop that file instead.

use std::path::PathBuf;

use agent_client_protocol::schema::v1::{Diff, ToolCallContent};
use tracing::warn;

use crate::dto::ToolMetadata;

impl From<ParsedDiff> for Diff {
    fn from(p: ParsedDiff) -> Self {
        let mut d = Diff::new(p.path, p.new_text);
        if let Some(old) = p.old_text {
            d = d.old_text(old);
        }
        d
    }
}

/// Extract ACP diff content blocks from a successful tool's metadata.
///
/// Priority chain (see module docs): `filediff` → `files[]` (authoritative
/// when present) → combined `diff` string. Unparseable patches are skipped
/// with a warning — never an error, never a whole-metadata fallback.
pub fn diff_blocks(meta: &ToolMetadata) -> Vec<ToolCallContent> {
    let mut out = Vec::new();

    match &meta.filediff {
        // Primary: structured single-file diff. `file` is authoritative even
        // if the embedded `Index:` header disagrees.
        Some(fd) => match parse_patch(&fd.patch, Some(&fd.file)) {
            Ok(diffs) => out.extend(diffs),
            Err(e) => warn!(file = %fd.file, error = %e, "tool diff parse failed (filediff), skipping"),
        },
        None => {
            // Level ②: structured `files[]` — authoritative when present;
            // per-entry failure skips ONLY that entry (see module docs).
            if let Some(files) = &meta.files
                && !files.is_empty()
            {
                return diff_blocks_from_files(files);
            }
            // Level ③: fallback — parse the combined `diff` string section
            // by section.
            let Some(diff) = &meta.diff else {
                return Vec::new();
            };
            match parse_patch(diff, None) {
                Ok(diffs) => out.extend(diffs),
                Err(e) => warn!(error = %e, "tool diff parse failed (fallback diff), skipping"),
            }
        }
    }

    out.into_iter().map(Diff::from).map(ToolCallContent::Diff).collect()
}

/// Level ②: map `metadata.files[]` to diff blocks.
///
/// `filePath` is the authoritative path — never derived from the patch
/// text. Each entry's `patch` is its own `Index:` section (the header line
/// is stripped, the body feeds the shared hunk parser). Entry semantics:
/// `type = "add"` (wire-verified) drops the old side entirely;
/// `type = "delete"` (modeled from the core source, not wire-verified)
/// empties the new side; any other value uses the generic reconstruction
/// from the patch body. `move_path` is deliberately ignored (not observed
/// on the wire). Entry-level isolation: a failing entry is skipped with a
/// warning; the remaining entries still map.
fn diff_blocks_from_files(files: &[crate::dto::FileEntry]) -> Vec<ToolCallContent> {
    let mut out = Vec::new();
    for entry in files {
        let lines: Vec<&str> = entry.patch.lines().collect();
        // The entry patch is a single `Index:` section: drop the header
        // line, feed the rest to the shared hunk parser. Lacking a header
        // (never seen on the wire), the whole patch is the body.
        let body: &[&str] = match lines.first() {
            Some(l) if l.starts_with("Index: ") => &lines[1..],
            _ => &lines,
        };
        match parse_section(&entry.file_path, body) {
            Ok(Some(mut d)) => {
                match entry.r#type.as_deref() {
                    Some("add") => d.old_text = None,
                    Some("delete") => d.new_text = String::new(),
                    _ => {}
                }
                out.push(ToolCallContent::Diff(d.into()));
            }
            Ok(None) => warn!(file = %entry.file_path, "files[] entry has no hunks — skipping"),
            Err(e) => warn!(
                file = %entry.file_path,
                error = %e,
                "files[] entry parse failed — skipping"
            ),
        }
    }
    out
}

/// A parsed per-file diff, ready to become an ACP `Diff` block.
struct ParsedDiff {
    path: PathBuf,
    /// `None` = new file (ACP contract).
    old_text: Option<String>,
    new_text: String,
}

/// Parse a unified diff (SVN `Index:` style) into per-file diffs.
///
/// Handles one or more `Index: <path>` sections. If the text contains no
/// `Index:` header, the whole text is treated as a single section whose path
/// is taken from `fallback_path` first, then from the `+++ ` header.
fn parse_patch(patch: &str, fallback_path: Option<&str>) -> Result<Vec<ParsedDiff>, String> {
    let lines: Vec<&str> = patch.lines().collect();

    // Split at `Index: <path>` lines → (path, body) sections.
    let mut sections: Vec<(Option<&str>, &[&str])> = Vec::new();
    let index_lines: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("Index: "))
        .map(|(i, _)| i)
        .collect();

    if index_lines.is_empty() {
        sections.push((fallback_path, lines.as_slice()));
    } else {
        for (k, &start) in index_lines.iter().enumerate() {
            let end = index_lines.get(k + 1).copied().unwrap_or(lines.len());
            let path = lines[start].strip_prefix("Index: ").map(str::trim);
            // Skip the header itself; separator lines ("===", "---", "+++")
            // are skipped inside hunk parsing.
            let body = &lines[start + 1..end];
            sections.push((path, body));
        }
    }

    let mut out = Vec::new();
    for (header_path, body) in sections {
        let file = header_path
            .map(str::to_string)
            .or_else(|| derive_path_from_headers(body))
            .filter(|p| !p.is_empty());
        let Some(file) = file else {
            warn!("diff section without a file path — skipping section");
            continue;
        };
        match parse_section(&file, body) {
            Ok(Some(d)) => out.push(d),
            Ok(None) => warn!(file = %file, "diff section has no hunks — skipping"),
            Err(e) => warn!(file = %file, error = %e, "diff hunk parse failed — skipping section"),
        }
    }
    Ok(out)
}

/// Derive a file path from `+++ ` / `--- ` headers (git-style patches and
/// bare unified diffs). `+++ ` wins; strips git `a/` `b/` prefixes.
fn derive_path_from_headers(body: &[&str]) -> Option<String> {
    let mut candidate: Option<String> = None;
    for line in body {
        if let Some(p) = line.strip_prefix("+++ ") {
            return Some(strip_git_prefix(p.trim()));
        }
        if candidate.is_none()
            && let Some(p) = line.strip_prefix("--- ") {
                candidate = Some(strip_git_prefix(p.trim()));
            }
    }
    candidate
}

fn strip_git_prefix(p: &str) -> String {
    p.strip_prefix("a/")
        .or_else(|| p.strip_prefix("b/"))
        .unwrap_or(p)
        .to_string()
}

/// Which file-side a content line belongs to — the `\ No newline at end of
/// file` marker annotates the content line directly above it.
#[derive(Clone, Copy)]
enum Side {
    /// A context line (` ` prefix) — belongs to both sides.
    Both,
    Old,
    New,
}

/// Parse one file's body (after the `Index:` line) into hunks and rebuild
/// old/new text. Returns `Ok(None)` when there is nothing to diff.
fn parse_section(file: &str, body: &[&str]) -> Result<Option<ParsedDiff>, String> {
    let mut old_lines: Vec<String> = Vec::new();
    let mut new_lines: Vec<String> = Vec::new();
    // `\ No newline at end of file` marker state: true ⇒ the side's last
    // line carries NO terminating newline. A marker sets its side(s); any
    // later content line on that side clears it again — so only a marker
    // on the side's final line survives.
    //
    // Release 0.8.3: without marker-aware trailing newlines (and
    // `join_lines` defaulting to `\n`), EOF-appended blocks whose tail
    // repeats the old file's closing lines rendered shifted in Zed —
    // imara-diff tokenizes the final "x" and the mid-file "x\n"
    // differently, so the common-suffix match latched onto the wrong
    // duplicate.
    let mut old_no_nl = false;
    let mut new_no_nl = false;

    let mut in_hunk = false;
    let mut prev: Option<Side> = None;
    for line in body {
        if line.starts_with("@@") {
            // Loose hunk-header validation: `@@ -a[,b] +c[,d] @@`.
            if !line.contains("@@") || !line[2..].trim_start().starts_with('-') {
                return Err(format!("malformed hunk header: {line:?}"));
            }
            in_hunk = true;
            prev = None;
            continue;
        }
        if !in_hunk {
            continue; // header lines ("===", "---", "+++", ...)
        }
        match line.chars().next() {
            Some(' ') => {
                let content = line[1..].to_string();
                old_no_nl = false;
                new_no_nl = false;
                old_lines.push(content.clone());
                new_lines.push(content);
                prev = Some(Side::Both);
            }
            Some('-') => {
                old_no_nl = false;
                old_lines.push(line[1..].to_string());
                prev = Some(Side::Old);
            }
            Some('+') => {
                new_no_nl = false;
                new_lines.push(line[1..].to_string());
                prev = Some(Side::New);
            }
            // `\` = "\ No newline at end of file": flags the side(s) of the
            // line directly above (context = both; `-` = old; `+` = new).
            Some('\\') => match prev {
                Some(Side::Both) => {
                    old_no_nl = true;
                    new_no_nl = true;
                }
                Some(Side::Old) => old_no_nl = true,
                Some(Side::New) => new_no_nl = true,
                None => {}
            },
            // Other junk ignored.
            _ => {}
        }
    }

    if old_lines.is_empty() && new_lines.is_empty() {
        return Ok(None);
    }

    let old_text = join_lines(&old_lines, old_no_nl);
    let new_text = join_lines(&new_lines, new_no_nl);

    // New-file detection: the old side is empty (git `-0,0`) or only carries
    // the fixture's phantom empty `-\n` trailer. ACP: old_text = None.
    let old_all_empty = old_lines.iter().all(|l| l.is_empty());
    if !new_lines.is_empty() && (old_lines.is_empty() || old_all_empty) {
        return Ok(Some(ParsedDiff {
            path: PathBuf::from(file),
            old_text: None,
            new_text,
        }));
    }

    // Deletion: nothing left on the new side.
    if new_lines.is_empty() {
        return Ok(Some(ParsedDiff {
            path: PathBuf::from(file),
            old_text: Some(old_text),
            new_text: String::new(),
        }));
    }

    // Plain modification.
    Ok(Some(ParsedDiff {
        path: PathBuf::from(file),
        old_text: Some(old_text),
        new_text,
    }))
}

/// Rebuild one side's text. Every line is newline-terminated by default —
/// a unified diff only omits the file's final newline when that side's
/// last line carries the `\ No newline at end of file` marker (`no_nl`).
fn join_lines(lines: &[String], no_nl: bool) -> String {
    if lines.is_empty() {
        return String::new();
    }
    let mut s = lines.join("\n");
    if !no_nl {
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{FileDiff, ToolMetadata};

    /// The real fixture patch (sse-tool-turn.sse / messages-tool-turn.json):
    /// a new file with the trailing `-\n` quirk.
    const FIXTURE_PATCH: &str = "Index: /tmp/opencode/hello-acp-test.txt\n\
===================================================================\n\
--- /tmp/opencode/hello-acp-test.txt\n\
+++ /tmp/opencode/hello-acp-test.txt\n\
@@ -1,1 +1,1 @@\n\
+bridge test line\n\
-\n";

    #[test]
    fn fixture_new_file_via_filediff() {
        let meta = ToolMetadata {
            diff: Some(FIXTURE_PATCH.to_string()),
            filediff: Some(FileDiff {
                file: "/tmp/opencode/hello-acp-test.txt".into(),
                patch: FIXTURE_PATCH.to_string(),
                additions: Some(1),
                deletions: Some(0),
            }),
            files: None,
            title: Some("hello-acp-test.txt".into()),
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1, "one diff block expected");
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("expected Diff block, got {blocks:?}");
        };
        assert_eq!(d.path.to_string_lossy(), "/tmp/opencode/hello-acp-test.txt");
        assert_eq!(d.old_text, None, "new file: old_text must be None (ACP contract)");
        assert_eq!(d.new_text, "bridge test line\n");
    }

    #[test]
    fn fallback_diff_field_without_filediff() {
        let meta = ToolMetadata {
            diff: Some(FIXTURE_PATCH.to_string()),
            filediff: None,
            files: None,
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else { panic!("Diff expected") };
        assert_eq!(d.new_text, "bridge test line\n");
    }

    #[test]
    fn modification_rebuilds_old_and_new() {
        // NOTE: keep every context line space-prefixed — Rust string
        // continuations (backslash-newline) strip leading whitespace.
        let patch = concat!(
            "Index: /repo/src/main.rs\n",
            "===\n",
            "--- /repo/src/main.rs\n",
            "+++ /repo/src/main.rs\n",
            "@@ -1,4 +1,4 @@\n",
            " use std::io;\n",
            "-fn main() {\n",
            "+fn main() -> io::Result<()> {\n",
            "    println!(\"hi\");\n",
            " }\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else { panic!("Diff expected") };
        assert_eq!(d.path.to_string_lossy(), "/repo/src/main.rs");
        assert_eq!(
            d.old_text.as_deref(),
            Some("use std::io;\nfn main() {\n   println!(\"hi\");\n}\n")
        );
        assert_eq!(
            d.new_text,
            "use std::io;\nfn main() -> io::Result<()> {\n   println!(\"hi\");\n}\n"
        );
    }

    #[test]
    fn deletion_yields_empty_new_text() {
        let patch = "Index: /repo/old.rs\n\
===\n\
--- /repo/old.rs\n\
+++ /dev/null\n\
@@ -1,3 +0,0 @@\n\
-line one\n\
-line two\n\
-line three\n";
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else { panic!("Diff expected") };
        assert_eq!(
            d.old_text.as_deref(),
            Some("line one\nline two\nline three\n")
        );
        assert_eq!(d.new_text, "");
    }

    // ============ Trailing-newline semantics (`\ No newline at end of file`) ============

    /// No marker ⇒ both sides end with a bare `\n` appended by join_lines.
    #[test]
    fn eof_append_without_marker_keeps_both_trailing_newlines() {
        let patch = concat!(
            "Index: /repo/data.json\n",
            "===\n",
            "--- /repo/data.json\n",
            "+++ /repo/data.json\n",
            "@@ -1,3 +1,4 @@\n",
            " {\n",
            "   \"a\": 1\n",
            "-}\n",
            "+}\n",
            "+  \"venus\": {}\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(d.old_text.as_deref(), Some("{\n  \"a\": 1\n}\n"));
        assert_eq!(d.new_text, "{\n  \"a\": 1\n}\n  \"venus\": {}\n");
    }

    /// An insertion mid-file with trailing context lines: neither side
    /// touches EOF, both keep the final newline.
    #[test]
    fn mid_file_insert_with_trailing_context_keeps_newlines() {
        let patch = concat!(
            "Index: /repo/main.rs\n",
            "===\n",
            "@@ -1,3 +1,4 @@\n",
            " fn f() {\n",
            "+    let x = 1;\n",
            "     println!(\"{x}\");\n",
            " }\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(
            d.old_text.as_deref(),
            Some("fn f() {\n    println!(\"{x}\");\n}\n")
        );
        assert_eq!(
            d.new_text,
            "fn f() {\n    let x = 1;\n    println!(\"{x}\");\n}\n"
        );
    }

    /// Marker after a `-` line: only the OLD side loses its trailing
    /// newline; the new side is untouched (no marker on its last line).
    #[test]
    fn old_side_marker_drops_only_old_trailing_newline() {
        let patch = concat!(
            "Index: /repo/x.txt\n",
            "===\n",
            "@@ -1 +1,2 @@\n",
            "-last\n",
            "\\ No newline at end of file\n",
            "+last\n",
            "+appended\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(d.old_text.as_deref(), Some("last"));
        assert_eq!(d.new_text, "last\nappended\n");
    }

    /// Marker after a context line: both sides lose the trailing newline.
    #[test]
    fn context_marker_drops_both_trailing_newlines() {
        let patch = concat!(
            "Index: /repo/x.txt\n",
            "===\n",
            "@@ -1,2 +1,2 @@\n",
            " one\n",
            " two\n",
            "\\ No newline at end of file\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(d.old_text.as_deref(), Some("one\ntwo"));
        assert_eq!(d.new_text, "one\ntwo");
    }

    /// A marker mid-hunk is stale once another line follows on that side:
    /// only a marker on the side's FINAL line counts.
    #[test]
    fn mid_hunk_marker_is_stale_after_later_lines() {
        let patch = concat!(
            "Index: /repo/x.txt\n",
            "===\n",
            "@@ -1,4 +1,4 @@\n",
            "-a\n",
            "\\ No newline at end of file\n",
            "+b\n",
            "-c\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(d.old_text.as_deref(), Some("a\nc\n"));
        assert_eq!(d.new_text, "b\n");
    }

    /// The real jsdiff 8.0.4 wire shape (opencode's generator, captured):
    /// the old file's final line is marked, the appended block's final
    /// line is marked — neither side ends with a newline.
    #[test]
    fn jsdiff_eof_append_without_final_newlines() {
        let patch = concat!(
            "Index: /repo/data.json\n",
            "===================================================================\n",
            "--- /repo/data.json\n",
            "+++ /repo/data.json\n",
            "@@ -1,3 +1,4 @@\n",
            " {\n",
            "   \"a\": 1\n",
            "-}\n",
            "\\ No newline at end of file\n",
            "+}\n",
            "+  \"venus\": {}\n",
            "\\ No newline at end of file\n",
        );
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("Diff expected")
        };
        assert_eq!(d.old_text.as_deref(), Some("{\n  \"a\": 1\n}"));
        assert_eq!(d.new_text, "{\n  \"a\": 1\n}\n  \"venus\": {}");
    }

    /// Empty side: no lines, no newline — regardless of the marker flag.
    #[test]
    fn join_lines_empty_side_is_empty_string() {
        assert_eq!(join_lines(&[], false), "");
        assert_eq!(join_lines(&[], true), "");
    }

    #[test]
    fn multi_file_sections_produce_multiple_blocks() {
        let patch = format!(
            "{FIXTURE_PATCH}Index: /repo/util.rs\n\
===\n\
--- /repo/util.rs\n\
+++ /repo/util.rs\n\
@@ -1,1 +1,2 @@\n\
-old util\n\
+new util\n\
+extra\n"
        );
        let meta = ToolMetadata {
            diff: Some(patch),
            filediff: None,
            files: None,
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 2, "one block per Index: section");
        let ToolCallContent::Diff(d0) = &blocks[0] else { panic!() };
        assert_eq!(d0.path.to_string_lossy(), "/tmp/opencode/hello-acp-test.txt");
        let ToolCallContent::Diff(d1) = &blocks[1] else { panic!() };
        assert_eq!(d1.path.to_string_lossy(), "/repo/util.rs");
        assert_eq!(d1.old_text.as_deref(), Some("old util\n"));
        assert_eq!(d1.new_text, "new util\nextra\n");
    }

    #[test]
    fn git_style_path_derivation_without_index_header() {
        // No Index: header — path must come from the +++ line (b/ prefix stripped).
        let patch = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n";
        let meta = meta_with(patch);
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else { panic!("Diff expected") };
        assert_eq!(d.path.to_string_lossy(), "src/lib.rs");
    }

    #[test]
    fn bad_patch_never_breaks_the_call() {
        let meta = meta_with("this is not a diff at all\nno hunks here\n");
        let blocks = diff_blocks(&meta);
        assert!(blocks.is_empty(), "bad patch must yield no blocks, not an error");
    }

    #[test]
    fn garbage_hunk_header_is_skipped() {
        let meta = meta_with("Index: /x\n===\n@@ this is not a hunk @@\nsome body\n");
        let blocks = diff_blocks(&meta);
        assert!(blocks.is_empty());
    }

    #[test]
    fn empty_metadata_yields_nothing() {
        let meta = ToolMetadata {
            diff: None,
            filediff: None,
            files: None,
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        assert!(diff_blocks(&meta).is_empty());
    }

    fn meta_with(diff: &str) -> ToolMetadata {
        ToolMetadata {
            diff: Some(diff.to_string()),
            filediff: None,
            files: None,
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        }
    }

    // ======================= Level ②: metadata.files[] =======================

    use crate::dto::FileEntry;

    /// The real apply_patch entry from the aft capture
    /// (tests/fixtures/aft-tool-turn.sse): `type: "add"`, single-file.
    fn fixture_entry() -> FileEntry {
        FileEntry {
            file_path: "/tmp/opencode/aft-probe/added.txt".into(),
            relative_path: Some(".aft-probe/added.txt".into()),
            r#type: Some("add".into()),
            patch: "Index: /tmp/opencode/aft-probe/added.txt\n\
				   ===================================================================\n\
				   --- /tmp/opencode/aft-probe/added.txt\n\
				   +++ /tmp/opencode/aft-probe/added.txt\n\
				   @@ -0,0 +1 @@\n\
				   +patched ok\n"
                .lines()
                .map(|l| l.trim_start())
                .collect::<Vec<_>>()
                .join("\n"),
            additions: Some(1),
            deletions: Some(0),
            move_path: None,
        }
    }

    /// Fixture-driven: the aft apply_patch success maps via files[] with
    /// `filePath` as the authoritative path — the absolute path, matching
    /// the source of truth, not the Index: header text.
    #[test]
    fn files_entry_maps_with_authoritative_filepath() {
        let meta = ToolMetadata {
            diff: Some("Index: /should/never/be/used.txt\n".to_string()),
            filediff: None,
            files: Some(vec![fixture_entry()]),
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1);
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("expected a Diff block");
        };
        assert_eq!(d.path, PathBuf::from("/tmp/opencode/aft-probe/added.txt"));
        assert!(d.old_text.is_none(), "type=add drops the old side");
        assert_eq!(d.new_text, "patched ok\n");
    }

    /// Synthetic: a files[] entry whose path disagrees with the combined
    /// diff string — Level ② wins, the string is never consulted.
    #[test]
    fn files_win_over_the_combined_diff_string() {
        let meta = ToolMetadata {
            diff: Some(
                "Index: /fake/B.txt\n\
				 ===================================================================\n\
				 --- /fake/B.txt\n\
				 +++ /fake/B.txt\n\
				 @@ -1 +1 @@\n\
				 -from-string\n\
				 +from-string\n"
                    .to_string(),
            ),
            filediff: None,
            files: Some(vec![fixture_entry()]),
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1, "files[] only — the diff string is not parsed");
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("expected a Diff block");
        };
        assert_eq!(d.path, PathBuf::from("/tmp/opencode/aft-probe/added.txt"));
        assert_eq!(d.new_text, "patched ok\n");
    }

    /// files[] present but every entry malformed ⇒ NO fallback to the
    /// combined string (authoritative), and the tool never breaks.
    #[test]
    fn files_present_never_touches_the_fallback() {
        let meta = ToolMetadata {
            diff: Some(
                "Index: /ok/from-string.txt\n\
				 @@ -1 +1 @@\n\
				 +would-have-parsed\n"
                    .to_string(),
            ),
            filediff: None,
            files: Some(vec![FileEntry {
                // Garbage patch: a hunk header that is not a hunk header.
                patch: "Index: /bad.txt\nnot a diff at all\n@@nope\n".into(),
                ..fixture_entry()
            }]),
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert!(blocks.is_empty(), "malformed entry skipped, no fallback to the string");
    }

    /// Entry-level isolation: one malformed entry is skipped, the sibling
    /// still maps.
    #[test]
    fn malformed_entry_skips_only_itself() {
        let good = fixture_entry();
        let bad = FileEntry {
            file_path: "/tmp/bad.txt".into(),
            patch: "Index: /tmp/bad.txt\nno hunks here\n".into(),
            ..fixture_entry()
        };
        let meta = ToolMetadata {
            diff: None,
            filediff: None,
            files: Some(vec![bad, good]),
            title: None,
            truncated: None,
            diagnostics: None,
            ..Default::default()
        };
        let blocks = diff_blocks(&meta);
        assert_eq!(blocks.len(), 1, "only the malformed entry is skipped");
        let ToolCallContent::Diff(d) = &blocks[0] else {
            panic!("expected a Diff block");
        };
        assert_eq!(d.path, PathBuf::from("/tmp/opencode/aft-probe/added.txt"));
    }
}
