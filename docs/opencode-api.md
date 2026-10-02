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

### Permission loop — fully verified on the wire (2026-10-02, dual-subscriber)

Trigger: project config `[permission] bash = "ask"` (v1-compat, maps to action `shell`).
Prompt "use the bash tool to run: echo hi". Observed on live 2.0.21:

1. **`permission.asked`** — broadcast to **every** `/api/event` subscriber
   (dual-curl proof: both got the identical frame, 22/22). Also appears in
   `GET /api/session/{id}/permission` (`data[0]`) while pending. Ask persists
   ≥10 min without expiring. Frame:

   ```json
   {"id":"evt_…","created":…,"type":"permission.asked",
    "location":{"directory":"…"},
    "data":{"id":"per_…","sessionID":"ses_…","action":"shell",
            "resources":["echo hi"],"save":["echo *"],"metadata":{},
            "source":{"type":"tool","messageID":"msg_…","id":"call_…"}}}
    ```

   - `action` is **`shell`** for bash (not "bash"); `resources` = the command
     args; `save` = suggested rule pattern for an "always" reply.
   - `source.tool.id` is the tool-call ID → correlates with the pending
     `session.tool.called` part; `data.id` is the requestID for the reply route.
2. **`POST /api/session/{id}/permission/{requestID}/reply {"decision":"once"}`**
   → `204`. Turn resumes within ~2s.
3. **`permission.replied {sessionID, requestID, reply}`** echoes on the stream.
4. Resumed-turn sequence (single-session connected trace): `permission.replied` →
   `session.tool.progress` → `session.tool.success` (content `[{type:"text",text:"hi\n"}]` —
   the command really executed) → `session.step.ended` → `session.usage.updated` →
   `session.step.started` (model wraps up) → reasoning/text deltas →
   `session.step.ended` → `session.execution.succeeded`.

Failure mode when nobody replies (perm-loop test): the tool call stays at
`input.ended`, no further frames for the whole ask window; the model eventually
gets a refusal error and the turn ends `execution.succeeded` with the refusal
reasoned into the assistant message. No error event for the refused tool.

Bridge mapping → ACP `session/request_permission` (3 options: once / always /
reject); "always" sends `{"decision":"always"}` — server derives the rule from
`save`.

### Additional verified events (live turns)

- `session.tool.progress {sessionID, assistantMessageID, id, metadata}` — fires
  when a tool resumes after permission and during execution.
- `session.step.started` / `session.step.ended {assistantMessageID, finish?}` —
  step boundaries; `session.step.streamed` at stream completion.
- `session.reasoning.started/ended`, `session.text.started/ended` — block
  boundaries around the delta streams.
- `session.renamed {sessionID, title}` — auto title generation.
- `session.model.selected {sessionID, model}` — after set-model.
- `session.usage.updated {sessionID, tokens}` — per step end.
- `session.inbox.enqueued/delivered {inboxID, …}` — prompt admission.

### Still unobserved

- `session.tool.failed` — tool failure live event (persisted `ToolState.Error`
  exists; official ACP code consumes `session.tool.failed {…, error:{type,message}}`).
- `session.execution.interrupted`, `session.retry.scheduled`, `form.created`,
  `session.forked/moved/deleted` — present in the official ACP consumer loop
  (see below) but not yet on our captures.

### Official 2.0.21 ACP adapter — event→ACP mapping (extracted from the binary)

The shipped 2.0.21 binary bundles the complete official ACP adapter (minified;
extracted via strings 2026-10-02). Its event pump consumes the same
`GET /api/event` stream — authoritative reference for the bridge:

- `session.text.delta` → `agent_message_chunk` (messageId = `assistantMessageID`)
- `session.reasoning.delta` → `agent_thought_chunk`, messageId =
  `` `${assistantMessageID}:reasoning:${ordinal}` ``
- `session.tool.input.started` → `tool_call` (pending) · `tool.called` →
  `tool_call_update` (input) · `tool.progress` → in_progress · `tool.success` →
  completed (content+metadata) · `tool.failed` → failed (`error.message`)
- `permission.asked` → ACP `requestPermission` (tool preview from cached input);
  reply → `POST …/permission/{requestID}/reply {"decision":…}`
- `form.created` → **auto-cancelled** (`session.form.cancel`) — 2.0.21 official
  bridge cannot answer forms; elicitation support (#38121) is the gap
- `session.retry.scheduled` → `session_info_update` with retry `_meta`
- `session.execution.succeeded|interrupted|failed` → stopReason: end_turn /
  cancelled / error (`provider.auth` → authRequired error)
- usage: `tokens.{input,output,reasoning,cache.read,cache.write}` →
  `{inputTokens, outputTokens, totalTokens, thoughtTokens, cachedRead/WriteTokens}`
- `newSession`: `session.create({location:{directory}})` — **no model passed**
  (model comes from catalog/config) · `listSessions`: `session.list({directory,
  order:desc, limit:100, cursor})` · config-option set failures reload the
  catalog and retry once (model/effort/mode switch race mitigation)
- turn admission gate: the pump ignores session events until
  `session.inbox.delivered` whose `inboxID` matches the prompt's inbox entry
  (guards against picking up earlier turns' events)
- child sessions: `session.created` with `parentID` tracked; forwarded only when
  the client advertises `opencode/child-session-updates` in `_meta`

### v2.0.22 deltas vs the 2.0.21 binary above (surveyed 2026-10-02)

The notes above describe the 2.0.21 binary we run; tag v2.0.22
(`packages/cli/src/acp/`, 15 files) changed behavior:

- **Forms are no longer unconditionally cancelled.** When the client declares
  `clientCapabilities.elicitation.form` and the form is representable
  (`metadata.kind` question/websearch.provider; no credential-looking field
  keys; no external/when/hidden-required-without-default fields; options
  representable as oneOf), the adapter asks via `client/elicitation.create
  {mode:"form", requestedSchema}` and answers through `session.form.reply`;
  non-accept/invalid answers cancel. Non-declaring clients and
  unrepresentable forms still auto-cancel. → #38121 is addressed at v2.0.22
  for elicitation-capable clients.
- **Child sessions are always surfaced.** Capability declared →
  `opencode/session/child_update` notifications (update or status
  created/running/completed/failed/interrupted); not declared → child events
  projected into the parent stream with `_meta["opencode/child-session"]` and
  `toolCallId = "${child.id}:${toolCallId}"` (title `"${child.title}: …"`).
  Child permission asks carry the CHILD's sessionID — the reply must be
  addressed there. → #48232 root cause is client-side correlation (filtering
  asks to the main session, or matching non-prefixed toolCallIds), not server.
- **Prompt carries a client-generated `id` + `delivery:"steer"`**; turn
  admission gates on `session.inbox.delivered` whose `inboxID` matches that
  id. Permission asks, forms, and child tracking are handled even
  pre-admission (before the gate). Compaction is a separate submit path
  (`session.compact` with a client minted message id).
- **Permission previews are computed live on ask** (reads the file for
  write/edit, `Patch.derive` for patch hunks; title from input; locations
  fall back to `resources` minus `*`), not from cached input.
- **Usage accumulates from `session.step.ended`** and is reported once in the
  PromptResponse, plus a post-turn `usage_update {used, size, cost}` — size
  from the model catalog `limit.context`, cost from `session.get`.
  `session.usage.updated` and `session.renamed` are IGNORED by the official
  adapter (our incremental mappings are a superset — keep).
- **Compaction/retry markers**: `session.compaction.*` → session_info_update
  `_meta["opencode/compaction"] {status,messageId,reason,error?}`;
  `session.retry.scheduled` → `_meta["opencode/retry"]
  {attempt,nextRetryAt,error}` (cleared on next step.started; pending retry
  folds into PromptResponse `_meta`).
- **Cancel drain**: cancel → interrupt → forward wind-down ≤5 s → fail every
  still-open tool with error "Cancelled" (abandonTools). `session/close`
  interrupts even an idle session. Permission/form replies are
  uninterruptible server-side.
- **Config-option pushes**: `config_option_update` / `available_commands_update`
  on catalog reload (`model.updated` / `agent.updated` / `command.updated`,
  scoped by directory) and on selection changes from other clients
  (`session.model.selected` / `session.agent.selected`). Set failures reload
  the catalog once and retry; selection is updated before `switchModel` so the
  echo diffs to no change.
- **#52636 still present at v2.0.22**: result `content.diff` is built only
  from `input.oldString/newString` (edit tool); write/apply_patch produce no
  diff block; `metadata.filediff` passes through untouched inside
  `rawOutput.metadata`. Our metadata.filediff-based diff remains the fix and
  loses no information for official clients.
- Replay: `message.list` asc `limit:200` cursor-paginated; per-message
  translation failures are logged and skipped; any attach error detaches
  cleanly. Output ordering: post-response updates are buffered until the
  session/load|new|resume response has been written.
- initialize declares: `protocolVersion: 1`, loadSession, mcp http not sse,
  prompt embeddedContext+image, sessionCapabilities close/delete/fork/list/
  resume/additionalDirectories, `_meta` child-session-updates; authMethods
  `opencode-login` (terminal-auth `_meta` only for declaring clients). No
  fs.writeTextFile capability.

**Bridge backlog (Wave 4+, priority-ordered)**: 1. prompt `id` + inbox
admission gate; 2. step.failed/execution.failed terminal + auth-error
mapping (Wave 3); 3. child sessions + child permission asks; 4. usage
size/cost from catalog + PromptResponse usage; 5. form elicitation
(capability-gated); 6. compaction/retry markers; 7. config-option pushes;
8. cancel drain + abandonTools; 9. error-taxonomy parity; 10. output-ordering
buffer after session/load response; 11. prompt enrichment (audience
annotations, images, files, slash-commands).

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
