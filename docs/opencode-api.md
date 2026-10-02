# opencode 2.0.21 HTTP API — verified wire contract

**Target dialect: opencode v2.0.21** (production server `127.0.0.1:44041`, binary
`~/.opencode/bin/opencode`). Every non-obvious claim below was verified empirically on
2026-10-02 against a scratch 2.0.21 server, including one real model turn with a file
write. Raw evidence: `tests/fixtures/` + `/tmp/opencode/` on this machine.

The dev clone (`~/Work/OSS/opencode`, 2026-09-20) is **6 weeks newer** and speaks a
DIFFERENT dialect (see "Version drift" at the bottom). Use it for behavior reference
only — the wire contract is what this document says.

## Transport & auth

- JSON API is mounted under **`/api/*`**. Root paths (`/config`, `/session`, …) serve
  the web UI → HTML. Do not call root paths.
- Auth: **HTTP Basic**, username `opencode`, password = server's `OPENCODE_PASSWORD`
  env (2.0.21 also honors `OPENCODE_SERVER_PASSWORD`; if neither is set the server
  generates a random password and prints `server password <pw>` to its console).
- Response envelope: `{"location": {"directory": …}?, "data": <payload>}`.
  Paginated lists add a top-level `"cursor"` alongside `data`.
- SSE: `GET /api/event` — `text/event-stream`; frames are `data: {json}` lines,
  keep-alives are `: heartbeat` comment lines. There is **no** Last-Event-ID replay.

## Bridge endpoint surface

| Purpose | Method & path | Body → data payload |
|---|---|---|
| Create session | `POST /api/session` | `{id?, title?, agent?, model?, location: {directory}, metadata?}` → session info |
| List sessions | `GET /api/session?directory=…&cursor=…` | → `{data: [session…], cursor?}` |
| Get session | `GET /api/session/{id}` | → session info |
| Update session | `PATCH /api/session/{id}` | `{title?…}` |
| Delete session | `DELETE /api/session/{id}` | |
| Prompt | `POST /api/session/{id}/prompt` | `{text, files?, agents?, skills?, metadata?, delivery?}` → inbox user msg (returns **immediately**) |
| Run command | `POST /api/session/{id}/command` | `{command, arguments?…}` (verify vs openapi) |
| Compact/summarize | `POST /api/session/{id}/compact` | |
| Cancel turn | `POST /api/session/{id}/interrupt` | (dev calls this `abort`) |
| Fork | `POST /api/session/{id}/fork` | |
| Switch model | `POST /api/session/{id}/model` | `{model: {id, providerID, variant?}}` → 204 |
| Messages | `GET /api/session/{id}/message` | → `{data: [MessageRecord…], cursor}` (newest first) |
| Message detail | `GET /api/session/{id}/message/{msgID}` | → message record |
| Delete message | `DELETE /api/session/{id}/message/{msgID}` | |
| Permissions list | `GET /api/session/{id}/permission` | → `{data: []}` |
| Permission get | `GET /api/session/{id}/permission/{requestID}` | |
| Permission reply | `POST /api/session/{id}/permission/{requestID}/reply` | `{decision: "once"\|"always"\|"reject", message?}` |
| Event stream | `GET /api/event` | SSE (see taxonomy) |
| Agents | `GET /api/agent` | → `{location, data: [...]}` (fixture `agents.json`) |
| Commands | `GET /api/command` | → `{location, data: [...]}` |
| Skills | `GET /api/skill` | → `{location, data: [...]}` |
| Models | `GET /api/model` | → `{data: [{id, modelID, providerID, name}…]}` |
| Providers | `GET /api/provider` | → `{data: [{id, name, activation, package, settings}…]}` |
| Config | `GET /api/config` | → config documents array |
| MCP add | `PUT /api/experimental/mcp/{server}` | (dev calls this `POST /mcp`) |
| VCS diff | `GET /api/session/{id}/diff` | |

Unverified-but-in-openapi fields are marked; when in doubt, read
`tests/fixtures/openapi-2021.json` (the server's own spec dump) — it is authoritative
for request/response schemas; fixtures are authoritative for real-world shapes.

### Verified response quirks (live probe, 2026-10-02)

- `PATCH /api/session/{id}`, `POST …/model`, `DELETE …/{id}` and `DELETE …/message/{msgID}`
  return **204 No Content** — no envelope, no body.
- `POST …/interrupt` returns a **bare** `{"interrupted": …}` — NOT envelope-wrapped.
- `GET /api/config` returns a **bare array** — NOT envelope-wrapped.
- `POST …/compact` and `POST …/fork` **require a JSON object body** (empty body →
  `400 InvalidRequestError "Expected object"`); use `{}` for compact, `{"before": …}`
  for fork.

## SSE event taxonomy (verified on the wire)

Envelope per frame:

```json
{"id": "evt_…", "created": 1790913415940, "type": "session.text.delta",
 "location": {"directory": "…"}, "data": {…}, "durable": {"aggregateID": "ses_…", "seq": 18, "version": 1}}
```

`id`/`created`/`location`/`durable`/`metadata` may be absent; `type` + `data` always
present. Plugin events (`rpc.aft.*`) also flow on this stream — skip unknown types.

### Turn lifecycle (verified: successful + failed tool turn)

Order observed for one prompt that reasoned, called `write`, then replied:

```
session.inbox.enqueued      {inboxID, sessionID, item: {type:"user", payload:{text}, delivery:"steer"}}
session.execution.started   {sessionID}
session.inbox.delivered     {sessionID, inboxID}
session.usage.updated       {sessionID, cost, tokens:{input,output,reasoning,cache:{read,write}}}   (×N)
session.renamed             {sessionID, title}
session.step.started        {sessionID, agent, model: ModelRef, assistantMessageID, snapshot, started}
session.reasoning.started   {sessionID, assistantMessageID, ordinal, state:{reasoningField}}
session.reasoning.delta     {sessionID, assistantMessageID, ordinal, delta}                        (×N)
session.reasoning.ended     {sessionID, assistantMessageID, ordinal, text}   (text = full reasoning)
session.tool.input.started  {sessionID, assistantMessageID, id: "call_…", name: "write"}
session.tool.input.ended    {sessionID, assistantMessageID, id, text}   (text = raw input JSON string)
session.tool.called         {sessionID, assistantMessageID, id, input: <parsed>, executed: false}
session.step.streamed       {sessionID, assistantMessageID}
session.tool.progress       {sessionID, assistantMessageID, id, metadata}  (may lack `location`)
session.tool.success        {sessionID, assistantMessageID, id, content: [ToolContent…], metadata: ToolMetadata, executed}
session.step.ended          {sessionID, assistantMessageID, finish, rawFinish, cost, tokens, snapshot, files}
  … second step: step.started → text.started → text.delta → text.ended → step.streamed → step.ended
session.execution.succeeded {sessionID}        ← turn-completion gate (maps to ACP end_turn)
```

Failure path: `session.execution.failed {sessionID, error: {type, message}}` replaces
`execution.succeeded`. One `step` == one assistant message (`assistantMessageID`).
`ordinal` = part index inside that assistant message. Tool id is `call_*` (ACP
toolCallID).

### Other verified events

- `session.created {sessionID, slug, version, projectID, location, subpath}`
- `session.instructions.updated {sessionID, delta}`
- `model.updated` / `provider.updated` `{}` (directory changed → re-fetch model list)
- `project.updated {id, canonical, vcs, time, sandboxes}`
- `server.connected {}` (first frame after connect)

### Unverified event names (discover live, then update this doc)

- **Permission request event** — fires when a tool needs approval. Discover: on the
  scratch server, run a prompt that triggers a permission-requiring tool (e.g. bash
  with a project config permission rule), watch `/api/event`. Expected shape per
  openapi `Permission.Request`; reply via the session-scoped reply route above.
- `session.tool.error` / `session.tool.failed` — tool failure event name (persisted
  `ToolState.Error` exists, live event unobserved). Discover with a failing tool call.
- `session.deleted` / `session.status` — unobserved.

## Persisted message records (`GET …/message`)

Heterogeneous array, discriminated by `type`, **newest first**:

- `{"type":"user", "id":"msg_…", "text":…, "time":{…}}`
- `{"type":"assistant", "id":"msg_…", "agent":"orchestrator", "model":ModelRef,
   "content":[Part…], "time":{created,streamed,completed}, "snapshot":{start,end,files},
   "finish":"tool-calls"|"stop"|…, "rawFinish", "cost", "tokens":Usage}`
- `{"type": "<execution outcome>", "id", "outcome", "time"}` — outcome records
- `{"type": "<model switch>", "id", "model", "previous"?, "time"}`

### Parts (assistant `content[]`)

- `{"type":"text", "text":…, "time":{…}}`
- `{"type":"reasoning", "text":…, "state":{"reasoningField":…}, "time":{…}}`
- `{"type":"tool", "id":"call_…", "name":…, "executed":bool, "state":ToolState, "time":{created,ran?,completed?}}`

### ToolState (tag = `status`)

- `streaming`: `{status, input: <raw JSON string>}`
- `running`: `{status, input: <object>, metadata}`
- `completed`: `{status, input, content:[ToolContent…], metadata?}`
- `error`: `{status, input, error:{…StructuredError}, content?, metadata?}`

`ToolContent`: `{"type":"text","text":…}` (+other kinds, model as open map).

### ToolMetadata — **the #52636 diff-fix data source**

```json
{
  "diff": "Index: /abs/path\n===…\n--- /abs/path\n+++ /abs/path\n@@ -1,1 +1,1 @@\n+bridge test line\n-\n",
  "filediff": {"file": "/abs/path", "patch": "<same format>", "additions": 1, "deletions": 0},
  "diagnostics": {},
  "title": "hello-acp-test.txt",
  "truncated": false
}
```

- `filediff.file` is an **absolute** path. `patch` is an SVN-style `Index:` header +
  unified diff (note: trailing `-\n` quirk observed for new files).
- `title` is a display title for the tool call (use for ACP tool_call title).
- Multi-file tools (apply_patch) — **unverified** whether multiple `filediff`-like
  entries or one combined `diff`; verify live and extend `dto::ToolMetadata` if needed.
- This is 2.0.21 shape; the dev clone's `metadata.files[]` array shape does NOT apply.

## Turn flow for the bridge (prompt)

1. `POST …/prompt` returns immediately with the inbox user message — do NOT block on it.
2. Stream `/api/event`; forward text/reasoning deltas and tool lifecycle as ACP
   session updates.
3. `session.execution.succeeded` → ACP `stop` (reason end_turn).
   `session.execution.failed` → ACP `stop` (reason error, include error message).
4. `POST …/interrupt` maps to ACP `session/cancel` → stop reason cancelled.

## Version drift: dev clone vs 2.0.21

| dev clone (post-2.0.21) | 2.0.21 (target) |
|---|---|
| root paths `/session/…` | `/api/session/…` |
| `GET /global/event` | `GET /api/event` |
| `POST …/abort` | `POST …/interrupt` |
| `GET /config/providers` | `GET /api/provider` |
| `POST /mcp` | `PUT /api/experimental/mcp/{server}` |
| `POST …/message` (sync prompt) | `POST …/prompt` (async inbox) |
| events `message.part.updated/delta` | `session.text.*` / `session.tool.*` / `session.reasoning.*` |
| `metadata.files[]` array | `metadata.filediff` object (+`diff` string) |
| `GET /doc` OpenAPI | `GET /openapi.json` |

## Scratch server recipe (for tests)

Currently running (started 2026-10-02, keep or restart freely — do NOT touch the
production servers 44041 / `serve --service`):

```bash
cd /tmp/opencode/acp-fixture-project
(setsid env OPENCODE_PASSWORD=test123 nohup ~/.opencode/bin/opencode serve \
   --hostname 127.0.0.1 --port 47779 > /tmp/opencode/serve-2021.log 2>&1 < /dev/null &)
# auth: opencode:test123 — curl -u opencode:test123 http://127.0.0.1:47779/api/…
```

Models with working credentials on the scratch: `astra/GLM-5.3-astra` (verified
working). `cctq/*` gave `provider.no-route`. CodeBuddy lacks its token in scratch env.

Integration tests must be opt-in via env (`BRIDGE_IT=1`) so plain `cargo test` stays
hermetic (fixtures only).

## Governance for lanes

- `src/dto.rs` is the shared contract. Lanes may **add** fields (with serde defaults)
  but must not rename/remove without updating this doc + dto together.
- Fixtures are read-only evidence; if a live probe contradicts a fixture, trust the
  live probe and update both.
