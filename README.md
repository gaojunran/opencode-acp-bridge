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

## Status

Working (live-verified against opencode 2.0.21):

- session/new + load with full history replay (before the response, per ACP
  contract)
- prompt → streaming text/reasoning/tool events, turn end via
  `session.execution.*`
- tool-call state machine incl. diff content blocks (#52636 fix)
- cancel → `interrupt`
- permission loop: `permission.asked` → ACP `requestPermission` (Wave 3,
  landing now) — wire contract live-verified end to end

Not yet (Wave 4 backlog — see `docs/opencode-api.md`): child-session
forwarding, form elicitation, compaction/retry markers, config-option pushes,
cancel drain details.

## Wire contract

`docs/opencode-api.md` is the live-verified contract against opencode 2.0.21
(REST + SSE envelopes, event taxonomy, permission loop, the official ACP
adapter's mapping extracted from the shipped binary, and the v2.0.22 source
deltas). Trust that file over assumptions when touching `src/dto.rs` or the
mapping layer.
