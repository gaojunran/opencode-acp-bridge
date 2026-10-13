# Fixture provenance

Every capture in this directory was recorded against an **aft-enabled**
opencode server (the cortexkit/aft plugin was active). There is **no
core-only live capture** yet.

- `sse-tool-turn.sse` — the canonical tool turn; carries `rpc.aft.*` events
  (9 frames), i.e. the aft plugin was live during capture.
- `aft-tool-turn.sse` / `aft-image-read.sse` — dedicated aft captures
  (read/edit/apply_patch turn; image read).
- `messages-tool-turn.json` — persisted-message records of an aft-enabled
  turn (the tool metadata shows the aft fingerprint: `filediff`/`diff`/
  `filePath`, `preview`, `isImage`…).
- `core-v2-tool-turn.sse` — **synthesized** from core 2.0.22 source shapes
  (`write.ts`, `edit.ts`, `patch.ts`, `read.ts`, `tool/results.ts`,
  `packages/schema/src/file-diff.ts`): core `edit`/`patch` publish
  `metadata.files[] = FileDiff.Info{file, patch, additions, deletions,
  status}` where `file` is a resource (project-relative, external absolute);
  core `write` publishes NO result metadata; core `read` emits
  `{type:"file", uri:"data:…", mime}` content for images AND PDFs. No
  core-only live capture exists yet — treat the shapes as source-derived.