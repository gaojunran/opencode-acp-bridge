# opencode-acp-bridge

A Rust [ACP](https://agentclientprotocol.com) agent that connects Zed (or any
ACP v1 client) to one **shared** opencode server over its HTTP API — instead
of letting each editor window spawn its own private `opencode serve`.

## Why

`opencode acp` (2.0.x) starts a private `opencode serve` for every editor
window: ~255 MB RSS and ~8 s cold start each, and the sessions of those
private servers are invisible to each other (the pain behind opencode
PR #52075). This bridge attaches to a single shared server instead:

- **~10–20 MB** per window instead of ~255 MB (tokio + reqwest, single binary)
- **millisecond** startup instead of ~8 s
- every window sees the same server and the same sessions
- correct diff blocks for every file-edit path (fixes opencode #52636)

## Install

With [mise](https://mise.jdx.dev):

```sh
mise use -g github:gaojunran/opencode-acp-bridge
```

Prebuilt binaries for Linux (gnu/musl, x64/arm64), macOS (x64/arm64) and
Windows are attached to each
[release](https://github.com/gaojunran/opencode-acp-bridge/releases); mise
puts the binary on its PATH shim
(`~/.local/share/mise/shims/opencode-acp-bridge` by default).

From source (stable Rust):

```sh
cargo build --release
```

## Configure Zed

Run your shared server once — `opencode serve` writes its address and
password to `~/.config/opencode/service.json`, and the bridge picks that up
by default, so no secrets go into the Zed config:

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

| Invocation | Server URL | Password |
| --- | --- | --- |
| *(none — default)* | `~/.config/opencode/service.json` (`{port, password, hostname}`, `0.0.0.0` → `127.0.0.1`); if the file is absent, the `OPENCODE_URL` env var | from the file, or `OPENCODE_PASSWORD` / `OPENCODE_SERVER_PASSWORD` env |
| `--attach <url>` | the given URL | `OPENCODE_PASSWORD` / `OPENCODE_SERVER_PASSWORD` env |
| `--attach` | `~/.config/opencode/service.json` — like the default, without the env fallback | from the file |
| `--no-aft` | *(composes with any connection mode)* | disables the aft tool-call hoist adaptations: File/image content passthrough in tool results; diff extraction stays enabled |

The server is probed at startup (`GET /api/config`) and failures are
classified — unreachable, credentials rejected, HTTP status — with the
connection source named in the message, so a stale service registration reads
differently from a dead server. Logs go to stderr (Zed collects them in its
per-agent debug log). Exit codes: `0` normal, `1` connection failure, `2`
usage error.

## Features

- **Sessions, shared everywhere** — create, resume with full history replay,
  list, delete; every window sees the same sessions on the same server
- **Streaming turns** — text, reasoning, and tool-call events; cancel maps to
  interrupt with a bounded drain of in-flight tools
- **Diff blocks on every edit path** — `edit`, `write`, `apply_patch`, and
  plugin-hoisted tools alike, derived from server-side result metadata
  (`filediff` / `files[]`) instead of reconstructed tool inputs
- **Permissions routed to the editor** — asks surface as ACP
  `requestPermission`, including asks originating from subagent child
  sessions
- **Modes** — opencode agents (build/plan/…) exposed as ACP modes, switchable
  live, with remote switches reflected
- **Model & agent pickers** — session model and agent as ACP config options
  (what Zed renders as its pickers), switchable live via
  `session/set_config_option`, with remote switches and catalog reloads
  reflected as `config_option_update` pushes
- **Slash commands** pushed to the editor as they become available
- **aft plugin compatible** — image reads map to ACP image content blocks;
  `--no-aft` opts out of the hoist adaptations

## Compared to the official `opencode acp`

Anchored to opencode 2.0.21/2.0.22 — official behavior source-checked, bridge
behavior verified against a real server:

| Area | Official `opencode acp` | This bridge |
| --- | --- | --- |
| Process model ([#40696](https://github.com/anomalyco/opencode/issues/40696), PR [#52075](https://github.com/anomalyco/opencode/pull/52075)) | private `opencode serve` per window — ~255 MB + ~8 s each, sessions invisible across windows | one shared server — ~10–20 MB bridge process, ms-scale startup, sessions shared |
| Diff blocks for file edits ([#52636](https://github.com/anomalyco/opencode/issues/52636)) | only from the `edit` tool's inputs; `write` / `apply_patch` / plugin tools produce none | result-metadata driven, covers every edit path incl. plugin tools |
| Subagent permission asks ([#48232](https://github.com/anomalyco/opencode/issues/48232)) | replies hang — they never reach the child session that asked | routed to the asking session, round-trip verified |
| Session & mode surface | `session/list` / `resume` / `delete`, `set_mode`, command pushes (present in source) | full parity, plus `current_mode_update` on remote switches |

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
