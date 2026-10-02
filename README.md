# opencode-acp-bridge

A Rust [ACP](https://agentclientprotocol.com) agent that bridges Zed (or any
ACP v1 client) to a **shared** opencode server over its HTTP API — instead of
letting `opencode acp` spawn a full private `opencode serve` for every editor
window.

## Why

`opencode acp` (2.0.x) starts its own `opencode serve --stdio --port 0` child
per editor window: ~255 MB RSS and ~8 s cold start per window, and the
sessions of those private servers are invisible to each other (the pain behind
opencode PR #52075). This bridge talks to one shared server over the wire:

- **~10–20 MB** per window instead of ~255 MB (tokio + reqwest, single binary)
- **millisecond** startup instead of ~8 s
- every window sees the **same server and the same sessions**
- fixes opencode #52636 (file edits produce no diff blocks for
  `write`/`apply_patch`) by deriving diffs from the tool-result
  `metadata.filediff` — server-side truth, unlike the official input-based
  reconstruction

## Build

```sh
cargo build --release
# binary: target/release/opencode-acp-bridge
```

## Configure Zed

Add to `~/.config/zed/settings.json` (the agent panel → agent servers):

```jsonc
{
  "agent_servers": {
    "OpenCode": {
      "command": {
        // adjust to your build location
        "path": "~/Playground/opencode-acp-bridge/target/release/opencode-acp-bridge",
        "args": ["--attach"]
      }
    }
  }
}
```

`--attach` (bare) reads `~/.config/opencode/service.json` — the port/password
registration a running `opencode serve` writes — so no secrets go into the
Zed config. Zed starts one bridge process per project window; all of them
connect to the same server.

To pin a specific server instead, use the explicit form and pass the password
via env:

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

## Connection modes

| Invocation | Server URL | Password |
| --- | --- | --- |
| `--attach <url>` | the given URL | `OPENCODE_PASSWORD` / `OPENCODE_SERVER_PASSWORD` env |
| `--attach` | `~/.config/opencode/service.json` (`{port, password, hostname}`; `0.0.0.0` → `127.0.0.1`) | from the file |
| *(none)* | `OPENCODE_URL` env | same env vars as `--attach <url>` |
| `--no-aft` | *(any connection mode)* | disables the aft tool-call hoist adaptations (File/image content passthrough in tool results); diff extraction stays enabled |

The server is probed at startup (`GET /api/config`); failures are classified
(unreachable / credentials rejected / HTTP status) with the connection source
named in the message. Logs go to stderr — Zed collects them in its per-agent
debug log. Exit codes: `0` normal, `1` probe/resolution failure, `2` usage
error.

## AFT compatibility (tool-call hoist)

With the [`@cortexkit/aft-opencode`](https://www.npmjs.com/package/@cortexkit/aft-opencode)
plugin active, the built-in tools are replaced by same-name registrations, but
the SSE stream keeps the core shape (tool names, event taxonomy, envelopes),
so the bridge works unmodified — the diff plane is identical to core
(`edit`/`write` → `filediff`; `apply_patch` → `files[]`). The only
aft-specific delta: reading an image yields a `file` part (data-URI), which
is mapped to an ACP image content block instead of being dropped. `--no-aft`
disables just that image/file passthrough; diff extraction stays on
(dialect-neutral). Verified against a live aft v0.58.0 environment (captures
in `tests/fixtures/aft-*.sse`). Unlike the official adapter, which builds
diffs from `input.oldString/newString` (empty under aft's hoist), this bridge
reads result metadata — the same code path covers both dialects.

## Compared to the official `opencode acp`

Anchored to opencode 2.0.21/2.0.22 — official behavior source-checked,
bridge behavior live-verified (see Status):

| Area | Official `opencode acp` | This bridge |
| --- | --- | --- |
| Diff blocks for file edits ([#52636](https://github.com/anomalyco/opencode/issues/52636)) | Diffs only from the `edit` tool's `input.oldString/newString`; `write` / `apply_patch` / plugin tools produce no diff — the editor silently shows no file changes (unfixed in 2.0.22) | Result-metadata driven chain (`filediff` → `files[]` → `diff` string) — covers every edit path incl. plugin-hoisted tools |
| Subagent permission asks ([#48232](https://github.com/anomalyco/opencode/issues/48232)) | Replies hang: the reply must reach the child session that asked (still open in 2.0.22) | Replies routed to the asking session — child asks round-trip (live E2E) |
| Process model ([#40696](https://github.com/anomalyco/opencode/issues/40696), PR [#52075](https://github.com/anomalyco/opencode/pull/52075)) | Spawns a private `opencode serve` per editor window: ~255 MB + ~8 s cold start each, sessions invisible across windows | Attaches to one shared server (`--attach` / service.json): ~2 MB bridge process, ms-scale startup, sessions shared |
| AFT tool hoist | Diff extraction reads tool inputs → empty under the hoist; image reads dropped | Reads result metadata (dialect-neutral) and maps image file parts to ACP image blocks; `--no-aft` opts out |
| Protocol dialect | v1 + v2 draft negotiation; elicitation forms (2.0.22) | ACP v1 — what Zed negotiates in practice; no elicitation yet |

Not fixable on either side: todo/plan outlines ([#40745](https://github.com/anomalyco/opencode/issues/40745)) — the
`todowrite` tool was removed from the 2.x core, so there is no wire data to
map.

## Status

Working (live-verified against opencode 2.0.21 + a live aft environment):

- session/new + load with full history replay (before the response, per ACP
  contract)
- prompt → streaming text/reasoning/tool events, turn end via
  `session.execution.*`
- tool-call state machine incl. diff content blocks (#52636 fix)
- cancel → `interrupt` (with bounded drain of in-flight tools)
- permission loop: `permission.asked` → ACP `requestPermission` → reply
  routing (incl. child sessions)
- child-session projection (#48232): child tool events forwarded under
  `${child.id}:`-prefixed toolCallIds
- structured turn failures surfaced as ACP errors with the provider message
- aft dialect: image passthrough + `--no-aft` opt-out (see above)

Not yet (see `docs/opencode-api.md`): form elicitation, retry/compaction
`session_info` markers and catalog-reload pushes are structure-ready but have
no wire event on 2.0.21 to trigger them live.

## Wire contract

`docs/opencode-api.md` is the live-verified contract against opencode 2.0.21
(REST + SSE envelopes, event taxonomy, permission loop, the official ACP
adapter's mapping extracted from the shipped binary, and the v2.0.22 source
deltas). Trust that file over assumptions when touching `src/dto.rs` or the
mapping layer.
