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
plugin active, seven built-in tools (read/edit/write/apply_patch/bash/grep/glob)
are replaced by same-name registrations. The SSE event stream keeps the core
shape — tool names, event taxonomy, and envelopes are unchanged — so most of
the bridge works unmodified. The diff plane is identical to core in both
dialects (`edit`/`write` → `filediff`; `apply_patch` → `diff` + `files[]`, no
`filediff` — core 2.0.21 live-verified too), so the only aft-specific wire
delta is the image file part below. What was verified against a live aft
v0.58.0 environment (captures in `tests/fixtures/aft-*.sse`):

| aft behavior | Wire effect | Bridge handling |
| --- | --- | --- |
| Same-name tool replacement | Stream undeformed: same tool names, same event types | No change needed — all existing mappings apply |
| `edit` / `write` results | `metadata.filediff` identical in shape to core | Diff blocks via the primary `filediff` path (unchanged) |
| `apply_patch` results | No `filediff`; `metadata.diff` (`Index:`-format string) + `metadata.files[]` — **same shape as core, not aft-specific** | Diff blocks via the existing diff-string fallback chain |
| `read` on an image | Content part `{type:"file", uri:"data:<mime>;base64,…", mime:"image/png"}` | Mapped to an ACP image content block (base64 payload + `mime_type`, original `uri` preserved) — previously dropped as unknown |
| Tool input args | Model's raw parameters (aft canonicalizes on a copy) | Permission `toolCall` construction works unchanged |
| Non-image or non-data-URI file parts | Not observed on the wire | Skipped — never guessed (`#[serde(other)]` sink) |
| `--no-aft` flag | — | Disables the file/image passthrough only; diff extraction stays on (dialect-neutral) |

Note the contrast with the official adapter: it builds diff blocks from the
edit tool's `input.oldString/newString`, which is empty under aft's hoist —
this bridge reads result metadata instead, so the same code path covers both
dialects. Image delivery itself does not depend on the relay: the image block
is emitted with the tool result; whether the *turn* then completes depends on
the model provider accepting image input.

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
