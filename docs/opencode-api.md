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
| Fork | `POST /api/session/{id}/fork` | `{"before": "msg_…"` \| `null`} → the NEW session's info (agent/model/location inherited from the parent) |
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
- `POST …/fork` on a session with **no messages** → `400 InvalidRequestError`
  `{"message": "Cannot fork empty session: <id>", "kind": "empty_session"}`
  (live-verified 2026-10-03).

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
- apply_patch (core 2.0.21, live-verified): **no `filediff`**; emits
  `metadata.diff` (combined `Index:`-format string) + `metadata.files[]`
  (`{filePath, relativePath, type, patch, additions, deletions}` per file —
  the dev-clone array shape DOES appear here, alongside the combined string).
  `edit`/`write` emit `filediff` instead (edit live-verified, write verified
  since Wave 0). The bridge's diff priority chain for apply_patch:
  **`files[]` (structured — `filePath` authoritative) → combined `diff`
  string fallback**; `files[]` present ⇒ authoritative, the string is never
  consulted (per-entry failures skip only that entry).

## Turn flow for the bridge (prompt)

1. `POST …/prompt` returns immediately with the inbox user message — do NOT block on it.
2. Stream `/api/event`; forward text/reasoning deltas and tool lifecycle as ACP
   session updates.
3. `session.execution.succeeded` → ACP `stop` (reason end_turn).
   `session.execution.failed` → ACP `stop` (reason error, include error message).
4. `POST …/interrupt` maps to ACP `session/cancel` → stop reason cancelled.

### ACP prompt block → prompt body mapping (Release 0.3.1, official-adapter semantics)

The prompt body is `{text, files, agents, skills, metadata}`; `files[]` entries
are `{type: "file", url, filename, mime}`. Mapping per ACP `ContentBlock` (a
block that cannot be mapped is DROPPED with a `warn` — it never kills the
turn; only a prompt that ends up with BOTH empty text and no files is
rejected):

| ACP block | wire surface |
|---|---|
| `Text` | appended to `text` (multiple blocks concatenated) |
| `Image` with base64 `data` | files entry `url: "data:{mime_type};base64,{data}"`, `filename: basename(uri) or "image"`, `mime: mime_type` |
| `Image` uri-only (`data:`/`http(s)://`) | files entry `url: uri`, filename = basename(uri) or "image" |
| `Image` with any other uri scheme | dropped |
| `ResourceLink` (Zed @-mentions — the 0.3.1 bug) | files entry `url: link.uri` **verbatim** (the opencode server resolves `file://` locally), `filename: link.name or basename(uri) or "file"`, `mime: link.mime_type or "text/plain"` |
| `Resource` text + `file://` uri (rare from Zed) | appended to `text` as `[<pathname>[:<line>]] <text>` (line from a `#L<digits>` uri fragment) |
| `Resource` text + `data:` uri | files entry (`url: uri`, mime `text/plain` default) |
| `Resource` blob + `file://`/`data:` uri | files entry (mime `application/octet-stream` default) |
| `Resource` other uri schemes | dropped |
| `Audio` and any future variant | dropped (no official mapping either) |

`filename` derives from the uri basename (query/fragment stripped, `file://`
scheme removed); `data:` uris have no basename → the fallback name.

## aft dialect (tool-call hoist, v0.58.0, wire-verified)

The cortexkit/aft plugin replaces the registered tool implementations
(read/edit/write/apply_patch/bash/grep/glob) with its own — the SSE event
stream is unchanged, but successful tool calls may carry aft-shaped payloads.
Verified against a live aft-enabled server (fixtures:
`tests/fixtures/aft-tool-turn.sse` — read/edit/apply_patch turn;
`tests/fixtures/aft-image-read.sse` — image read):

| shape | aft | core (2.0.21) | bridge handling |
|---|---|---|---|
| `tool.success.metadata.filediff` `{file, patch, additions, deletions}` | present for edit | present for edit/write (live-verified) | primary diff source (unchanged) |
| `tool.success.metadata.diff` (Index:-style string) | present for apply_patch (no `filediff`) | **same — apply_patch has no `filediff` on core either** (live-verified; not an aft divergence) | level ③ fallback only — apply_patch maps from `metadata.files[]` (level ②, structured) when present |
| `tool.success.content[]` file part `{"type":"file","uri":"data:…;base64,…","mime":"image/png"}` | image reads | absent | mapped to ACP `ImageContent` (`data` = payload after the data-URI prefix, `mime_type` = `mime`, `uri` preserved). Only `image/*` mimes are mapped — the only verified scenario; non-image / non-data-URI file parts are skipped, never guessed |
| `tool.called.input` | model's raw args (canonicalization happens on a copy) | same | unchanged |

The `mime` (not `mimeType`) field and the aft metadata fingerprint (`preview`,
`filepath`, `isImage`, `isPdf`, `truncated`) are the hoist's wire markers.

`--no-aft` disables the File/image content passthrough (back to plain
text+diff behavior); the diff extraction chain is NOT gated — `filediff`/`diff`
are dialect-neutral and keep working under both.

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

## Modes (agent ↔ ACP mode mapping, wire-verified 2.0.21)

1. **Agent catalog**: `GET /api/agent?location[directory]=<cwd>` — the
   `location[directory]` deepObject query is MANDATORY; omitted, the endpoint
   returns an empty list (Lane A `agents()` once hit this).
   `{location, data: Agent.Info[]}`, fields `id, name, mode, hidden,
   description, color, model, request, system, steps, permissions`;
   `mode ∈ {subagent, primary, all}`.
2. **Mode filtering**: an agent is a mode iff `mode ∈ {primary, all}` AND
   `hidden == false`. On the scratch fixture (18 agents) that leaves
   `orchestrator` + `build` (explorer/fixer are subagents; compaction/title/
   dreamer-* are hidden internals).
3. **Switch**: `POST /api/session/{id}/agent` body `{"agent": "<id>"}` → 204
   no body (the ACP `session/set_mode` wire).
4. **Switch event**: SSE `session.agent.selected` `{sessionID, agent}` — sent
   for BOTH own and remote switches. The bridge tracks the mode and emits
   `current_mode_update` only on an actual tracked-value change: a remote
   switch updates, the own-switch echo diffs to zero and stays suppressed.
5. **Default agent**: DERIVED, never hardcoded — the first visible
   `primary` agent in wire order (stock 2.0.21 = `orchestrator`, cross-checked
   against fixture step.started: parent sessions all run orchestrator), falling
   back to the first mode-eligible agent (`mode ∈ {primary, all}` &&
   `!hidden`) when the catalog has no primary. A catalog that yields no modes
   at all omits the `modes` payload from newSession/load/resume (an empty or
   failed agents fetch degrades the same way) so Zed renders no picker instead
   of an unmatched "Unknown" current mode.
6. **Message records carry the agent**: assistant messages
   `{"type":"assistant", ..., "agent":"orchestrator", "model":{...}}` —
   the load/resume `currentModeId` source (the LAST assistant message's
   agent; no assistant message → the derived default).
7. **Schema**: `SessionModeState.current_mode_id` is REQUIRED (not Option);
   `SessionMode{id, name, description?}`.

Self-heal: a `step.started.agent` differing from the tracked value (e.g. the
server config changed the default agent) updates the tracked mode and emits
`current_mode_update` once.

## Config options (model + agent pickers, Release 0.3.0, wire-verified)

Zed renders the model picker ONLY via ACP `configOptions`, and config options
are UI-mutually-exclusive with the modes dropdown, so the bridge exposes BOTH
pickers as session config options (`session/set_config_option`):

1. **Capability gate**: `config_options` is sent ONLY when the client
   declared `clientCapabilities.session.configOptions` (non-null) in
   initialize. Zed declares it. The `modes` payload keeps flowing in ALL
   cases (other clients use it; Zed ignores it when config options exist).
2. **Agent option**: id `"agent"`, name `"Agent"`, category `mode`,
   ungrouped select over the SAME visible-agent filter as the modes payload
   (`mode ∈ {primary, all}` && `!hidden`); current value = the tracked agent
   id. Omitted when the agents catalog is empty/failed **or** the current
   agent drifted out of the visible list (never an unmatched current value).
3. **Model option**: id `"model"`, name `"Model"`, category `model`, select
   GROUPED by providerID (group id + name = providerID, first-seen order);
   option value scheme `<providerID>/<id>` (CLI notation, split on the FIRST
   `/` when resolving), display name = `name` falling back to `modelID` then
   `id` (`GET /api/model` → `{data: [{id, modelID, providerID, name}…]}`).
   Omitted when the model catalog is empty/failed.
4. **UNKNOWN current model → `__default__`**: newSession cannot know the
   model (session create sends none, the server assigns the config default;
   the create response and fresh `GET /api/session/{id}` carry no model;
   `/api/config` documents carry no default-model field; primary agents have
   `model: null`). The current value is then the synthetic `"__default__"`,
   and a synthetic `SessionConfigSelectOption{value:"__default__",
   name:"Default"}` is PREPENDED to the first group — an unmatched current
   value would render "Unknown" in Zed. There is no server API to unset a
   model, so once the model is concrete the Default option is no longer
   listed.
5. **Current values for load/resume**: `GET /api/session/{id}` — its `agent`
   and `model` fields are authoritative (includes post-switch state).
   Fallbacks when absent/failed: agent = last assistant message's agent, then
   the derived default; model = last assistant message's `model`, then
   `__default__` (with Default listed).
6. **`session/set_config_option` dispatch**:
   - `config_id "model"` + `"<provider>/<model>"` → catalog lookup (the value
     is a verbatim echo of what was pushed) → `POST /api/session/{id}/model`
     body `{model: {id, providerID}}` → 204 → tracked model updated →
     response + push carry the full state. `"__default__"` → no-op success
     (echoes the current state; NO set-model call). Value not in the catalog
     (e.g. a stale Zed-persisted default like `codebuddy/gpt-6-sol`) or a
     failed switch → invalid-params error followed by a current-state push —
     the client self-corrects.
   - `config_id "agent"` → the exact `session/set_mode` wire (set_agent +
     tracked mode + remote-echo suppression), kept alongside `set_mode` for
     protocol compat.
   - unknown config id → invalid-params error.
7. **Push triggers** — `config_option_update` with the FULL state (both
   options, current values) fires on: catalog reload (`model.updated` /
   `provider.updated`, both `{}`, catalogs re-fetched), a remote
   `session.model.selected {sessionID, model}` / `session.agent.selected`
   switch, and `step.started` self-heals (the event carries `model` and
   `agent` every step; a tracked-value mismatch updates + pushes). Echo
   suppression: the bridge tracks the value BEFORE responding to
   set_config_option/set_mode, so the server's own-switch echo diffs to zero.
8. **Degrade**: agents fetch failed → agent option omitted; models fetch
   failed → model option omitted; both failed or capability absent → no
   `config_options` field at all. A config option is NEVER emitted with an
   empty/blank current value that is not a listed option value.

## Session lifecycle close + fork (Release 0.4.0, wire-verified 2.0.21)

1. **`session/close` is bridge-local — opencode has NO close concept.** The
   2.0.21 OpenAPI (`/openapi.json`, probed 2026-10-03) contains no close-like
   endpoint; sessions live in the server store until deleted. The ACP
   contract (cancel ongoing work as if `session/cancel` was called, then free
   resources) therefore maps to: flag the in-flight turn loop + best-effort
   `POST /api/session/{id}/interrupt`, then drop the bridge's tracked entry.
   The opencode session itself is left intact (a later `session/load`/
   `session/resume` still works). Unknown sessions → no-op `{}` success.
   Advertised via `sessionCapabilities.close` (stable schema 1.5.0).
2. **`session/fork` is native opencode**: `POST /api/session/{id}/fork` with
   body `{"before": "msg_…"}` or `{"before": null}` → `{data: Session.Info}`
   of the NEW session. The ACP request has no boundary field, so the bridge
   always sends `before: null` — the fork copies the full transcript up to
   now.
3. **Fork inheritance (live-verified on a real fork)**: the new session's
   `Session.Info` carries `agent` and `model` inherited from the parent
   (these are the authoritative current values for the ACP modes payload and
   config options — NOT the synthetic `__default__` that newSession must
   use), `location.directory` inherited from the parent, `title` suffixed
   `" (fork #N)"`, and a `fork: {sessionID, boundary: {type: "before",
   messageID}}` lineage field.
4. **Bridge-side only fields**: `ForkSessionRequest.cwd` is used for the
   agent-catalog fetch / mode derivation and the tracked entry — opencode
   has no fork-into-directory support, the server session keeps the parent's
   location. `additional_directories` and `mcp_servers` are not modeled on
   the wire and are ignored.
5. **Error path**: an empty parent (no messages) → the 400
   `empty_session` above, surfaced by the bridge as an internal error (the
   request itself was valid — the parent just has no history to fork).
   Advertised via `sessionCapabilities.fork` (unstable, enabled by the
   `unstable_session_fork` umbrella feature the bridge pins).

## Background listener: live sync of remote turns (Release 0.5.0)

The per-turn SSE subscription (`event_stream(sessionID)`, opened inside the
turn loop before each prompt POST) only covers turns the bridge itself
started. Turns started in OTHER frontends on the same opencode server (the
TUI, the web UI, a second ACP client) were invisible to the connected ACP
client until a `session/load` replay. Release 0.5.0 adds a persistent
background listener: **one long-lived server-wide SSE subscription per ACP
connection** (`event_stream_global` — the wire is `GET /api/event`, the same
server broadcast the per-turn streams consume; the 2026-10-02 dual-subscriber
probe confirmed the server fans every event out to all subscribers).

### Lifecycle

- Spawned when the connection's message loop starts (the `connect_with`
  main closure), torn down with the connection — task-tracking drop, or a
  notification send failure (client gone) ends it quietly. It never panics
  and never blocks the serve loop.
- First connect is eager and retried with the SSE module's backoff (1 s → 2 s
  → 4 s → 5 s cap); after the first connect the stream reconnects internally.

### Routing rules (per event)

1. Catalog reloads (`model.updated` / `provider.updated`, no session):
   push the full config-options state to every registered session WITHOUT a
   local turn in flight (the per-turn loop pushes for its own in-turn
   session).
2. Session NOT in the bridge's registry (`self.sessions`, populated by
   `session/new`, `session/load`, `session/resume`, `session/fork`) →
   ignored. CHILD (subagent) sessions are not registered but route via
   their PARENT's entry — see the [native subagent section](#native-subagent-sessions-release-060) below.
3. Session HAS an in-flight LOCAL turn (`in_turn` flag, set by the turn loop
   around its full lifecycle including the cancel drain) → the event is
   DROPPED, no state, no push: the turn loop's own subscription delivers it.
4. Otherwise → projected (below).

### Projection surface (byte-compatible with the turn loop's push)

- `session.inbox.enqueued` with a `user` item → one `user_message_chunk`
  (text + the inbox id as `messageId`) — the remote frontend's prompt text,
  which the ACP client cannot otherwise see.
- Text/reasoning deltas → `agent_message_chunk` / `agent_thought_chunk`.
- Tool events → declaration-before-update via the same `MappingState`
  machinery (introduce-on-first-sight: a tool first seen after a frozen
  window gets a synthesized declaration from its own fields).
- `usage.updated` → `usage_update`; retry/compaction/rename → the same
  info-update mapping as the turn loop.
- `session.agent.selected` / `session.model.selected` / `step.started`
  agent/model → tracked mode/model (echo suppression via tracked-first
  comparison) + `current_mode_update` and the full `config_option_update`
  push (capability-gated).
- The projector state is SESSION-scoped: created on demand when a remote
  turn's activity starts, cleared at the terminal event
  (`execution.succeeded` / `execution.failed` / `execution.interrupted`) —
  each remote turn starts with fresh declarations.
- Terminal events themselves push nothing (the client has no open prompt).
- `permission.asked` during a remote turn is NOT forwarded: the frontend
  that started the turn owns the ask and answers it there (the server
  accepts the first reply); the bridge would only double-prompt or hang the
  turn.

### Suppression of the local user message

The turn loop records the enqueued user message's inbox id (from the prompt
POST response) on the session entry; the listener matches
`session.inbox.enqueued` against it (consuming the id on match) and drops it.
The local user message can therefore never project as a remote user chunk —
even when the listener processes it after the local turn already ended. The
ACP client drafted its own prompt; a second user entry would be a duplicate.

### Known degraded edge (documented, not fixed)

The wire serializes executions per session (a prompt during an active turn
is queued), so remote and local turns cannot interleave mid-execution on the
same session. The in-turn gate still drops events processed while a local
turn is in flight, which covers the queued-remote-turn window and the local
turn's own tail. Residual loss — no worse than today's full loss:

- Events emitted by a remote turn that races a local turn on the SAME
  session are dropped (the local turn's own stream delivers its events).
- Tools first seen after the frozen window get synthesized declarations
  (introduce-on-first-sight), but any input JSON emitted during the freeze
  is missed.

## Governance for lanes

- `src/dto.rs` is the shared contract. Lanes may **add** fields (with serde defaults)
  but must not rename/remove without updating this doc + dto together.
- Fixtures are read-only evidence; if a live probe contradicts a fixture, trust the
  live probe and update both.

## Native subagent sessions (Release 0.6.0)

Zed's native subagent mechanism replaces the namespaced child-tool projection
(the `#48232` fallback) with Zed's own subagent cards: the bridge attaches
`_meta.subagent_session_info` to the PARENT's task tool call, Zed renders the
card in subagent mode and routes child-session `session/update` notifications
into the embedded transcript. Contract verified against Zed main 2026-08-16
and against the live 2.0.21 wire.

### Empirical capture (live 2.0.21 server, 2026-10-03)

Full capture: `tests/fixtures/subagent-native.sse.jsonl` (fresh spawn +
continuation). Wire facts the pairing relies on:

- **The spawner tool is named `subagent`** on the bridge's production server
  (`task` in stock 2.0.21 docs, `subagent` aliased) — matched by BOTH names
  (`updates::is_spawner_name`).
- **Event order on a fresh spawn**: parent `tool.input.started` → `input.ended`
  → `called` (input `{agent, description, prompt}` — NO session id) →
  `session.created {parentID, title, agent}` (~17 ms later) → `tool.progress`
  with `metadata: {"sessionID": "<child>", "status": "running"}` → child
  inbox/step/tool events (child's own session id).
- **The direct child linkage rides `tool.progress.metadata.sessionID`** (and is
  echoed on the spawner's `tool.success.metadata` as `status: "completed"`) —
  an authoritative call→child map that needs no ordering assumptions.
- **Continuation calls carry `sessionID` inside the tool input** (`called` /
  `input.ended`) and fire NO `session.created` — paired directly by the input
  field (verified: second prompt on the same child, input
  `{"agent": "explorer", "prompt": "...", "sessionID": "ses_..."}`).
- **The spawner card's title is the dispatch `description`** (Zed's own
  `spawn_agent` cards label with it; Release 0.7.1). The declaration is
  DEFERRED from `input.started` to `input.ended` — the description only
  exists in the input — so the card's first sight carries it (no "subagent"
  flash, no retitle update). Fallback chain: the trimmed `description`
  (truncated to 80 chars + "…", char-boundary safe on non-ASCII) → the tool
  name. A pairing that lands before the input stream ends (synthetic
  orderings; the real wire delivers the input ~17 ms before `session.created`)
  declares the card at announce time with the fallback name instead.
- **The persisted task tool part carries NO child id in `state.metadata`** —
  the child id appears inside the result content
  (`<subagent sessionID="..." state="completed">`) and in the success
  metadata. Replay-time pairing therefore uses `GET /api/session?parentID=`
  (verified: returns the children array) + order-matching, not the part shape.
- **The child's task prompt arrives as `session.inbox.enqueued`** with
  `item.type == "user"` and `payload.text` (prefixed
  `You are a subagent spawned by another session.\n`) — projected as the
  child's `user_message_chunk` (messageId = the inbox id).

### Pairing (turn loop AND background listener, shared `SubagentTracker`)

The tracker lives on the parent's `SessionEntry` (shared, mutex-serialized):

1. Spawner-call `input.started` (`subagent`/`task`) → joins an unpaired FIFO.
2. `session.created {parentID == parent}` → pairs with the OLDEST unpaired
   call; `_meta.subagent_session_info {session_id, message_start_index:
   <accumulated entry count>, message_end_index: null}` is announced on the
   parent's task call BEFORE any child traffic.
3. Direct linkages pair immediately: input `sessionID` (continuation) and
   `tool.progress.metadata.sessionID` (also the fallback when the FIFO is
   empty). Re-announce suppressed when the values are unchanged.
4. A spawner call that terminates unpaired is retired (a later child creation
   never pairs with a dead call).
5. Children with no observed spawner call buffer their events (cap 256,
   drop-oldest with a warn); the announce → flush happens in order.

### Live child streaming

Child events map through the SAME `MappingState` machinery but are addressed
to the CHILD's session id with PLAIN tool ids (no `${child}:` namespace):
inbox user item → `user_message_chunk`; text/reasoning deltas → chunks (per
assistant message id); tool events → declaration-before-update; usage →
`usage_update`. `permission.asked` from a child is forwarded as an ACP
request addressed to the CHILD session (Zed loaded it via the card), plain
ids/titles, reply routed to the ask's own session id.

Transcript slice indices (Zed's `message_start_index` / `message_end_index`):
entries = 1 per user message, 1 per distinct assistant message id, 1 per
distinct tool call id, counted persistently across turns. The announce opens
the slice at the current count (0 fresh / accumulated for a continuation);
the spawner's terminal `tool_call_update` closes it (`message_end_index` =
the count then, inclusive) and carries the final output as content. Tool
boundaries (`input.started` name, `called` name+first-string-arg) aggregate
as title-prefixed content lines on the parent card, kept to the trailing 6,
throttled at call boundaries only.

### Background listener routing (the v0.5.0 gap fix)

`session.created {parentID, title}` is learned globally; a child's events
resolve to its parent and gate on the PARENT's in-turn flag: a local parent
turn → drop (the turn loop owns the child); otherwise → project to the
child's own id (live child streaming after Zed loaded the child; background
subagents outliving the parent turn stay visible — the exact v0.5.0 gap) +
drive the parent's task card while the spawner call is still declared. Remote
(TUI) turns announce/pair/close exactly like local ones.

### Replay (`session/load` of a parent)

The parent's replay keeps parent content only (task call + its result — the
child thread is NOT inlined). The replayed spawner declaration + terminal
update carry `_meta.subagent_session_info` so Zed's view-creation scan
discovers and loads the children: children discovered via
`GET /api/session?parentID=<parent>`, ordered by creation time and zipped
with the parent's spawner calls in chronological order — ONLY when the counts
match (ambiguous pairings — e.g. prior-turn continuations with n children ≠ m
calls — attach nothing rather than a wrong meta). `message_end_index` = the
child's total entry count from its own history (Zed caps the embedded
display to the trailing 8). Loading a CHILD id uses the existing generic
load path (child history replays as child-id notifications — correct by
construction).

## Turn-scoped staging (--zed-git-add, Release 0.7.0)

The bridge-native port of the `opencode-git-add` plugin's v2 staging lane —
gated behind the `--zed-git-add` CLI flag (default OFF; with the flag off
NO tracking happens and NO git subprocess is ever spawned — every staging
entry point returns before touching state).

### Semantics (plugin parity + bridge deltas)

The plugin's settled semantics, ported 1:1 where the wire allows:

- Track ONLY completed tool calls (`session.tool.success`; `ToolFailed`
  events are never wired — the plugin's `status == "completed"` gate).
- `write` / `edit`: `input.path` (from the mapping's per-call input cache —
  success events carry no input on the wire; a call whose input was missed
  (mid-attach) is skipped, like a draft without an input).
- `apply_patch`: `result.metadata.files[].filePath` is AUTHORITATIVE when
  present (even `files: []` falls back); otherwise parse `input.patchText`
  headers exactly like the plugin's regexes:
  `*** Update File: <p>` / `*** Add File: <p>` / `*** Delete File: <p>` /
  `*** Move to: <p>` and `*** Rename File: <a> to <b>` with BOTH sides
  (greedy ` to ` split = last separator). Garbage patchText (bogus ops,
  empty paths, CRLF lines) tracks nothing.
- Resolution + containment: cwd-relative paths resolve against the session
  cwd; absolute paths pass through; `..` segments are normalized in BOTH
  forms (the plugin only normalizes relative paths — an absolute `..` path
  leaks its startsWith check; we close that) and anything escaping the
  session cwd is never tracked.
- Staging at the next user prompt, `git -C <cwd> add -- <deduped absolute
  paths>` — EXACTLY the tracked paths (never `git add .` / `-A`: only
  what the agent touched is staged, so the user's own edits and untracked
  scaffolding are never swept in). No shell, no globs, `--` stops option
  parsing.
- Failure policy: any pre-check failure or `git add` failure (after 3
  retries, 500 ms apart — the plugin's budget) logs a warning and RETAINS
  the pending set; the next prompt retries. A git failure never kills the
  user's message.
- Log at info on success: session, path count, first path.

Bridge deltas (by design, settled upstream):

- Pending is keyed by ROOT SESSION, not project directory. A subagent
  child's edits (events carrying the child's session id, resolved via the
  Release 0.6.0 child tracker / `child_parents` map) accumulate under the
  PARENT's pending set, and the PARENT's next prompt stages them together
  with its own. A child's own prompt NEVER stages (`parent_id` is captured
  at session/load from `GET /api/session/{id}`'s `parentID`).
- Topology guards for the split-machine case (bridge and server may run on
  different machines — tool-event paths are SERVER-side): every path must
  EXIST on the bridge's local filesystem, and the session cwd must be
  inside a git work tree (`git rev-parse --is-inside-work-tree`). Any
  failure skips staging entirely (never a partial batch) and retains.
  Note: a DELETED file (apply_patch `Delete File`) therefore never stages
  (exists() fails → retained, like any pre-check failure) — the plugin,
  which has no such guard, stages deletions via git.
- Two triggers, one shared `stage_pending(root)`:
  - LOCAL prompt: in the `session/prompt` handler, BEFORE the prompt POST.
  - REMOTE prompt: the background listener's `session.inbox.enqueued`
    handler for user items that are NOT the local turn's own suppressed
    inbox id (local prompts were already staged by the handler).
- Dedup: the plugin's message-id dedup is unnecessary here — a prompt
  reaches exactly one trigger (the handler XOR the listener, since the
  listener consumes the local inbox id).

### Wires & state

- `session.tool.success` `metadata.files[].filePath` — already decoded
  (`dto::FileEntry`); no wire extension needed for staging.
- `GET /api/session/{id}` `parentID` — added to `dto::SessionInfo` (serde
  default; governance lane-add) for the loaded-child gate.
- `git_add.rs` owns the pure logic (header parsing, containment, staging
  with retries) — unit-tested against real temp git repos; `SessionEntry`
  carries `git_add: Mutex<GitAddState>` (pending set) plus `parent_id`.

### Verification (as shipped)

- 13 `git_add.rs` unit tests (real temp git repos): header forms incl.
  rename both sides, garbage patchText, metadata-over-headers priority,
  empty-metadata fallback, CRLF skip, containment incl. `..` and
  sibling-prefix escapes, exact-path staging (manual edits untouched),
  pre-check retains (non-repo + missing path), retry-then-recover under
  `.git/index.lock`.
- 6 agent-level tests (MockBackend + real git repos): previous-turn staging
  at the next prompt, metadata path staging, child-edit-under-parent, flag
  OFF never stages (identical traffic, nothing staged), failure retains
  then recovers, remote prompt stages the previous remote turn.
- Live server check: see the scratch-server recipe — a session whose cwd is
  a temp git project, prompted to edit a file, stages the file at the
  SECOND prompt (visible in `git diff --cached` from the temp project, and
  in the bridge's `zed-git-add: staged` info log with RUST_LOG=info).
