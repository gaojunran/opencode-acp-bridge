# opencode-acp-bridge

A Rust [ACP](https://agentclientprotocol.com) agent that connects Zed (or any
ACP v1 client) to one **shared** opencode server over its HTTP API — instead
of letting each editor window spawn its own private `opencode serve`.

- **One shared server** — every window attaches to the same `opencode serve`:
  the same sessions, the same state, and turns started anywhere (TUI included)
  stream live into every open thread
- **Small and fast** — a single tokio + reqwest binary (~10–20 MB RSS per
  window, millisecond startup) instead of a ~255 MB private server with ~8 s
  cold start per window
  ([opencode PR #52075](https://github.com/anomalyco/opencode/pull/52075))
- **Built for Zed's thread UI** — native diff cards with "Go to File",
  subagent cards with embedded transcripts, permission previews, form cards,
  tool-kind icons, and a context-usage ring
- **Wire-faithful** — the opencode 2.0.21/2.0.22 HTTP/SSE contract is
  captured in [`docs/opencode-api.md`](docs/opencode-api.md) and drives the
  mapping layer

## Install

With [mise](https://mise.jdx.dev):

```sh
mise use -g github:gaojunran/opencode-acp-bridge
```

Prebuilt binaries for Linux (gnu/musl, x64/arm64), macOS (x64/arm64), and
Windows are attached to each
[release](https://github.com/gaojunran/opencode-acp-bridge/releases) (plus
`SHA256SUMS`); mise puts the binary on its PATH shim
(`~/.local/share/mise/shims/opencode-acp-bridge` by default).

From source (stable Rust):

```sh
cargo build --release
```

## Configure Zed

Run your shared server once — `opencode serve` writes its address and password
to `~/.config/opencode/service.json`, and the bridge reads that by default, so
no secrets go into the Zed config:

```jsonc
{
  "agent_servers": {
    "OpenCode": {
      "command": {
        // adjust to your install location (mise shim shown)
        "path": "~/.local/share/mise/shims/opencode-acp-bridge"
      }
    }
  }
}
```

Zed starts one bridge process per project window; all of them connect to the
same server.

To pin a specific server instead, pass the URL and password explicitly:

```jsonc
{
  "agent_servers": {
    "OpenCode": {
      "command": {
        "path": ".../opencode-acp-bridge",
        "args": ["--attach", "http://127.0.0.1:44041"],
        "env": { "OPENCODE_PASSWORD": "<server password>" }
      }
    }
  }
}
```

## Connection

| Mode | Server URL | Password |
| --- | --- | --- |
| default (no flags) | `~/.config/opencode/service.json` — `{port, password, hostname}`, with `0.0.0.0` mapped to `127.0.0.1`; if the file is absent, the `OPENCODE_URL` env var | from the file, or the `OPENCODE_PASSWORD` / `OPENCODE_SERVER_PASSWORD` env |
| `--attach <url>` | the given URL | `OPENCODE_PASSWORD` / `OPENCODE_SERVER_PASSWORD` env (required) |
| `OPENCODE_URL` env | the given URL (fallback when no service file exists) | same env vars |

A present-but-broken service file is surfaced as-is (never masked by the env
fallback); a stale registration — the server restarted with a new port —
prints a hint to restart the server or use `--attach`. The server is probed at
startup (`GET /api/config`) and failures are classified — unreachable,
credentials rejected, HTTP status — with the connection source named in the
message.

Logs go to stderr (Zed collects them in its per-agent debug log; level from
`RUST_LOG`, default `info`). Exit codes: `0` normal, `1` connection failure,
`2` usage error.

## Options

All of these are off by default:

| Flag | Effect |
| --- | --- |
| `--no-aft` | Disable the aft tool-call hoist adaptations (file/image content passthrough in tool results). Diff extraction stays enabled — `filediff`/`diff` are dialect-neutral and work either way. |
| `--zed-git-add` | Turn-scoped staging: at each user prompt, `git add --` the paths the previous turn changed (see *Turn-scoped staging* under Features). |
| `--show-synthetic` | Render server-injected `synthetic` inbox messages (background bash completions, subagent-completion notifications) as user chunks, live and on replay. Never affects staging. |
| `--shell-terminal` | Stream bash/shell output into an embedded display-only terminal card (≈1 s polling until the call ends). The card takes vertical space, so it is opt-in. |
| `--help` / `--version` | Usage / version. |

## Features

### Sessions & streaming

- **Full session surface** — create, reopen with full history replay, list,
  delete, close (bridge-local cancellation + unregister), and fork (native
  opencode fork with full-transcript copy). Zed's *Import External Agent
  Threads* lists the server's sessions via `session/list` and imports them as
  archived threads that replay on open.
- **Streaming turns** — text, reasoning (one chunk per part, official-parity
  message ids), and tool calls; cancel maps to `interrupt` with a bounded
  drain of in-flight tools.
- **Live sync of other frontends** — a persistent background listener streams
  turns started elsewhere (the TUI, or any client of the same server) into
  the connected editor: user messages, text/reasoning, tool calls, usage, and
  remote agent/model switches — no reload needed.
- **Editor attachments** — pasted images travel to the server as opencode
  attachments, and each `@`-file reference carries the lines it points at
  (Zed's `#L10:20` range → a `[path:line]` prefix), so the model sees the
  selection rather than just the file; on reload, user-message file chips
  replay as ACP resource links. Unmapped block types degrade to a warning,
  never an error.

### Editing & review

- **Diff blocks on every edit path** — `edit`, `write`, `apply_patch`, and
  plugin-hoisted tools alike, derived from server-side result metadata
  (`filediff` / `files[]`) with faithful trailing-newline reconstruction.
- **"Go to File" on diff cards** — tool calls carry the affected file as an
  ACP location (at running time and on completion), lighting up Zed's
  jump-to-file header.
- **Permission previews** — permission requests carry an inline diff preview
  (when the server provides `metadata.files`) plus the affected paths, so the
  approval card shows what will change.
- **Turn-scoped staging** (`--zed-git-add`) — each finished step's native
  snapshot diff (`step.ended.files`) is collected per root session (a
  subagent's steps accumulate under its parent), and the next user-initiated
  prompt — local or remote — runs `git add --` on exactly those paths before
  the turn starts. Covers `write`/`edit`/`apply_patch` and `bash` (any on-disk
  change in the session directory); only user prompts stage. Paths are
  validated against the local worktree (partial batches are retained, not
  staged; `git add` failures retry 3× and retain for the next prompt), so
  Zed's unstaged-changes view shows only the current turn's changes and your
  own working-tree edits are never touched. Accepted premise: one session at
  a time in a directory — concurrent manual edits within the turn window are
  staged too.

### Conversation UI

- **Tool kinds** — official-parity ACP kinds (`bash`/`shell` → execute,
  `edit`/`write`/`apply_patch` → edit, `read` → read, `grep`/`glob` →
  search, `webfetch` → fetch, `task`/`subagent` → think), so Zed picks the
  right icon and layout per tool.
- **Native subagent cards** — the parent's task call declares
  `_meta.subagent_session_info` so Zed renders its own subagent card: child
  events stream into the embedded transcript, tool boundaries aggregate onto
  the task card, continuations re-slice, background children outliving the
  parent turn stay visible, and the card is titled with the dispatch
  description.
- **Permissions routed to the editor** — asks surface as ACP
  `requestPermission`, including asks originating from subagent child
  sessions.
- **Modes & pickers** — opencode agents (build/plan/…) as ACP modes, session
  model and agent as ACP config options (Zed's pickers), all switchable live,
  with remote switches and catalog reloads pushed back.
- **Slash commands** — available commands are pushed to the editor as they
  become available; submitting one is routed client-side: `/compact` triggers
  compaction, catalog commands go through the server's command endpoint,
  anything else falls back to a plain prompt.
- **Forms** — opencode's `question` tool surfaces as an ACP elicitation form
  card (single/multi-select options and custom answers); answers route back
  to the server, Decline/Cancel cancel the form, and forms the client cannot
  render are cancelled cleanly instead of hanging the turn.
- **Context usage & failures** — the activity-bar ring shows the last step's
  token usage against the model's context window (`used / limit.context`)
  plus session cost; tool failures render the server's error message in the
  card.

## Compared to the official `opencode acp`

Anchored to opencode 2.0.21/2.0.22 — official behavior source-checked, bridge
behavior verified against a real server:

| Area | Official `opencode acp` | This bridge |
| --- | --- | --- |
| Process model ([#40696](https://github.com/anomalyco/opencode/issues/40696), PR [#52075](https://github.com/anomalyco/opencode/pull/52075)) | private `opencode serve` per window — ~255 MB + ~8 s each, sessions invisible across windows | one shared server — ~10–20 MB bridge process, ms-scale startup, sessions shared |
| Diff blocks for file edits ([#52636](https://github.com/anomalyco/opencode/issues/52636)) | only from the `edit` tool's inputs; `write` / `apply_patch` / plugin tools produce none | result-metadata driven, covers every edit path incl. plugin tools |
| Subagent permission asks ([#48232](https://github.com/anomalyco/opencode/issues/48232)) | replies hang — they never reach the child session that asked | routed to the asking session (the client request targets the CHILD session in native subagent mode), round-trip verified |
| Subagent (child) sessions | namespaced tool projections into the parent stream (`${child.id}:` cards, official #48232 fallback) | Zed's native subagent cards — `_meta.subagent_session_info` pairing, child-session streaming, embedded transcript slices |
| Session & mode surface | `session/list` / `resume` / `delete` / `fork`, `set_mode`, command pushes (present in source) | full parity, plus `current_mode_update` on remote switches and `session/close` (bridge-local; official has no close surface) |

Not mappable on either side: todo/plan outlines
([#40745](https://github.com/anomalyco/opencode/issues/40745)) — the
`todowrite` tool was removed from the 2.x core, so there is no wire data.

## Development

The opencode 2.0.21 wire contract (REST + SSE envelopes, event taxonomy,
permission loop) is captured in `docs/opencode-api.md` — live-verified, and
the authority when touching `src/dto.rs` or the mapping layer. `cargo test`
runs the unit suite; end-to-end tests against a live server are gated behind
`BRIDGE_IT=1`.

## License

[MIT](LICENSE)
