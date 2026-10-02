//! opencode v2.0.21 wire DTOs — the sole contract between the HTTP client
//! (`opencode/`) and the ACP mapping (`acp/`).
//!
//! Source of truth: `docs/opencode-api.md` + `tests/fixtures/` (captured from a
//! live 2.0.21 server, including one real model turn with a file write).
//! Field names mirror the wire exactly (camelCase with `ID` suffixes) — structs
//! carry `#[allow(non_snake_case)]` so the mapping is 1:1 and reviewable
//! against fixtures. Unknown fields are ignored everywhere (forward compat).
//!
//! Governance: lanes may ADD optional fields, never rename/remove without
//! updating the contract doc together with this file.

#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================
// Shared envelopes
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Location {
    pub directory: String,
}

/// Standard 2.0.21 response envelope: `{location?, data}` (+ `cursor` on lists).
#[derive(Debug, Clone, Deserialize)]
pub struct Envelope<T> {
    pub data: T,
    #[serde(default)]
    pub location: Option<Location>,
    #[serde(default)]
    pub cursor: Option<Cursor>,
}

/// Pagination cursor (base64 tokens), verified on `GET …/message`.
#[derive(Debug, Clone, Deserialize)]
pub struct Cursor {
    #[serde(default)]
    pub previous: Option<String>,
    #[serde(default)]
    pub next: Option<String>,
}

/// GET /api/session/{id}/message response: `{data: [...], cursor?}`.
pub type MessagesEnvelope = Envelope<Vec<MessageRecord>>;

// ============================================================
// Sessions & directory
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRef {
    pub id: String,
    pub providerID: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

/// POST /api/session body (fields optional unless noted).
#[derive(Debug, Clone, Serialize)]
pub struct SessionCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    pub location: Location,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Session info (create/get/list item). Loose on purpose — the persisted shape
/// gained fields across point releases; only `id` is universal.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionInfo {
    pub id: String,
    #[serde(default)]
    pub projectID: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub version: Option<Value>,
    #[serde(default)]
    pub subpath: Option<Value>,
    #[serde(default)]
    pub location: Option<Location>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<ModelRef>,
    #[serde(default)]
    pub summary: Option<Value>,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub tokens: Option<Usage>,
    #[serde(default)]
    pub time: Option<Value>,
}

/// GET /api/model item (fixture: models.json).
#[derive(Debug, Clone, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub modelID: Option<String>,
    pub providerID: String,
    #[serde(default)]
    pub name: Option<String>,
}

/// GET /api/provider item. `settings` deliberately NOT modeled (contains
/// credentials the bridge never needs).
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub activation: Option<String>,
    #[serde(default)]
    pub package: Option<String>,
}

// Agent/command/skill shapes: model from fixtures when needed (agents.json).
// Untyped for now — the bridge's ACP surface (modes/commands/skills) can start
// from these raw values.
pub type AgentInfo = Value;
pub type CommandInfo = Value;
pub type SkillInfo = Value;

// ============================================================
// Prompt & inbox
// ============================================================

/// POST /api/session/{id}/prompt body.
#[derive(Debug, Clone, Serialize)]
pub struct PromptRequest {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Prompt response payload — the enqueued user message. The turn itself plays
/// out on the event stream; this returns immediately.
#[derive(Debug, Clone, Deserialize)]
pub struct InboxUserMessage {
    pub id: String,
    #[serde(default)]
    pub sessionID: Option<String>,
    #[serde(default)]
    pub time: Option<Value>,
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub payload: Option<InboxPayload>,
    #[serde(default)]
    pub delivery: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InboxPayload {
    #[serde(default)]
    pub text: Option<String>,
}

// ============================================================
// Permission
// ============================================================

/// POST /api/session/{id}/permission/{requestID}/reply body (openapi enum).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionReply {
    Once,
    Always,
    Reject,
}

#[derive(Debug, Clone, Serialize)]
pub struct PermissionReplyRequest {
    pub decision: PermissionReply,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ============================================================
// Persisted messages (GET /api/session/{id}/message)
// ============================================================

/// Heterogeneous message record, discriminated by `type`. Loose struct: record
/// kinds beyond user/assistant (execution outcomes, model switches) parse fine
/// and are skipped by kind match in the mapping layer.
#[derive(Debug, Clone, Deserialize)]
pub struct MessageRecord {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    // user
    #[serde(default)]
    pub text: Option<String>,
    // assistant
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<ModelRef>,
    #[serde(default)]
    pub content: Option<Vec<Part>>,
    #[serde(default)]
    pub finish: Option<String>,
    #[serde(default)]
    pub rawFinish: Option<String>,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub tokens: Option<Usage>,
    #[serde(default)]
    pub time: Option<Value>,
}

/// Assistant part, discriminated by `type`. `Unknown` swallows part kinds the
/// bridge does not map (keeps deserialization forward-compatible).
/// NOTE: tagged-enum variant matching is case-sensitive — `rename_all` maps
/// `Text`→`"text"` etc.; without it a mismatch falls SILENTLY into `Unknown`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Part {
    Text {
        text: String,
        #[serde(default)]
        time: Option<Value>,
    },
    Reasoning {
        text: String,
        #[serde(default)]
        state: Option<Value>,
        #[serde(default)]
        time: Option<Value>,
    },
    Tool {
        id: String,
        name: String,
        #[serde(default)]
        executed: Option<bool>,
        state: ToolState,
        #[serde(default)]
        time: Option<Value>,
    },
    #[serde(other)]
    Unknown,
}

/// Tool state machine, discriminated by `status` (openapi: Session.Message.ToolState.*).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolState {
    /// Input still streaming as raw JSON text.
    Streaming {
        input: String,
    },
    /// Executing.
    Running {
        input: Value,
        #[serde(default)]
        metadata: Option<Value>,
    },
    /// Finished successfully.
    Completed {
        input: Value,
        #[serde(default)]
        content: Option<Vec<ToolContent>>,
        #[serde(default)]
        metadata: Option<ToolMetadata>,
    },
    /// Failed.
    Error {
        input: Value,
        error: StructuredError,
        #[serde(default)]
        content: Option<Vec<ToolContent>>,
        #[serde(default)]
        metadata: Option<ToolMetadata>,
    },
}

/// Tool result content block. `"type":"text"` is the only kind observed;
/// others fall into `Unknown`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolContent {
    Text { text: String },
    /// A file reference in tool output — produced by the aft tool-call hoist
    /// (aft v0.58.0) for image reads: `{type: "file", uri: "data:image/png;base64,…",
    /// mime: "image/png"}`. The uri is a data-URI; the mime drives the ACP
    /// image mapping (only `image/*` is mapped — the only verified scenario).
    File {
        uri: String,
        #[serde(rename = "mime", default)]
        mime: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

impl ToolContent {
    pub fn text(&self) -> Option<&str> {
        match self {
            ToolContent::Text { text } => Some(text),
            ToolContent::File { .. } | ToolContent::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StructuredError {
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

// ============================================================
// Tool metadata — the #52636 diff-fix data source (2.0.21 shape)
// ============================================================

/// `session.tool.success` / persisted completed-tool `state.metadata`.
///
/// VERIFIED (single-file `write`): `filediff` object + `diff` string + `title`.
/// UNVERIFIED: multi-file tools (apply_patch) — may carry a different layout;
/// if a live probe shows one, extend with a `filediffs: Vec<FileDiff>` field.
#[derive(Debug, Clone, Deserialize)]
pub struct ToolMetadata {
    /// Unified patch (SVN-style `Index:` header) as a single string.
    #[serde(default)]
    pub diff: Option<String>,
    /// Structured single-file diff.
    #[serde(default)]
    pub filediff: Option<FileDiff>,
    /// Display title for the tool call (e.g. "hello-acp-test.txt").
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub truncated: Option<bool>,
    #[serde(default)]
    pub diagnostics: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileDiff {
    /// Absolute path of the changed file.
    pub file: String,
    /// Unified patch (SVN-style `Index:` header). Note: new files carry a
    /// trailing `-\n` line quirk.
    pub patch: String,
    #[serde(default)]
    pub additions: Option<u64>,
    #[serde(default)]
    pub deletions: Option<u64>,
}

// ============================================================
// Usage
// ============================================================

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input: Option<u64>,
    #[serde(default)]
    pub output: Option<u64>,
    #[serde(default)]
    pub reasoning: Option<u64>,
    #[serde(default)]
    pub cache: Option<CacheUsage>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CacheUsage {
    #[serde(default)]
    pub read: Option<u64>,
    #[serde(default)]
    pub write: Option<u64>,
}

// ============================================================
// SSE events
// ============================================================

/// Raw SSE frame. `data` is decoded lazily by `decode_event` so unknown event
/// types (plugin `rpc.*`, future types) skip without error.
#[derive(Debug, Clone, Deserialize)]
pub struct EventEnvelope {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub created: Option<u64>,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub location: Option<Location>,
    pub data: Value,
    #[serde(default)]
    pub durable: Option<Value>,
}

/// Typed payloads for the event kinds the bridge consumes (all verified on the
/// wire except where noted). Payload field sets are the observed supersets.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionRef {
    pub sessionID: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageRef {
    pub sessionID: String,
    pub assistantMessageID: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrdinalRef {
    pub sessionID: String,
    pub assistantMessageID: String,
    #[serde(default)]
    pub ordinal: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolRef {
    pub sessionID: String,
    pub assistantMessageID: String,
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionFailed {
    #[serde(flatten)]
    pub session: SessionRef,
    pub error: StructuredError,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StepStarted {
    #[serde(flatten)]
    pub session: SessionRef,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<ModelRef>,
    pub assistantMessageID: String,
    #[serde(default)]
    pub snapshot: Option<String>,
    #[serde(default)]
    pub started: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StepEnded {
    #[serde(flatten)]
    pub session: SessionRef,
    pub assistantMessageID: String,
    #[serde(default)]
    pub finish: Option<String>,
    #[serde(default)]
    pub rawFinish: Option<String>,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub tokens: Option<Usage>,
    #[serde(default)]
    pub snapshot: Option<Value>,
    #[serde(default)]
    pub files: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TextDelta {
    #[serde(flatten)]
    pub base: OrdinalRef,
    pub delta: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TextEnded {
    #[serde(flatten)]
    pub base: OrdinalRef,
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReasoningStarted {
    #[serde(flatten)]
    pub base: OrdinalRef,
    #[serde(default)]
    pub state: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolInputStarted {
    #[serde(flatten)]
    pub base: ToolRef,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolInputEnded {
    #[serde(flatten)]
    pub base: ToolRef,
    /// Raw input JSON as a string (parse with serde_json when needed).
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolCalled {
    #[serde(flatten)]
    pub base: ToolRef,
    pub input: Value,
    #[serde(default)]
    pub executed: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ToolSuccess {
    #[serde(flatten)]
    pub base: ToolRef,
    #[serde(default)]
    pub content: Option<Vec<ToolContent>>,
    #[serde(default)]
    pub metadata: Option<ToolMetadata>,
    #[serde(default)]
    pub executed: Option<bool>,
}

/// Payload of a `session.tool.failed` event: a tool call ended in error.
/// Official-adapter shape (`{assistantMessageID, error, …}`); extra fields
/// are tolerated so an unobserved real shape degrades to a silent skip.
#[derive(Debug, Clone, Deserialize)]
pub struct ToolRefError {
    #[serde(flatten)]
    pub base: ToolRef,
    pub error: StructuredError,
}

/// Payload of a `session.step.failed` event (2.0.21 event processors carry
/// `{assistantMessageID, error, finish?}`). The official ACP adapter does not
/// consume this — step failures surface via `session.execution.failed` — but
/// the kind must decode so the bridge can log the step-level error taxonomy.
#[derive(Debug, Clone, Deserialize)]
pub struct StepFailed {
    #[serde(flatten)]
    pub session: SessionRef,
    pub assistantMessageID: String,
    pub error: StructuredError,
    #[serde(default)]
    pub finish: Option<String>,
}

/// Payload of a `session.retry.scheduled` event. Field set taken from the
/// official adapter's session_info_update mapping; wire-unknown — all fields
/// tolerant so a different real shape degrades to a silent skip.
#[derive(Debug, Clone, Deserialize)]
pub struct RetryScheduled {
    pub sessionID: String,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub nextRetryAt: Option<Value>,
    #[serde(default)]
    pub error: Option<StructuredError>,
}

/// Payload of a `session.compaction.started` event (wire-captured in
/// tests/fixtures/compaction.sse.jsonl: `{sessionID, reason, recent,
/// inputID}`).
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionStarted {
    pub sessionID: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub recent: Option<String>,
    #[serde(default)]
    pub inputID: Option<String>,
}

/// Payload of a `session.compaction.ended` event. Wire-unknown (only
/// started/failed captured); all fields tolerant.
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionEnded {
    pub sessionID: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub inputID: Option<String>,
}

/// Payload of a `session.compaction.failed` event (wire-captured in
/// tests/fixtures/compaction.sse.jsonl: `{sessionID, reason, inputID,
/// error}`).
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionFailed {
    pub sessionID: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub inputID: Option<String>,
    #[serde(default)]
    pub error: Option<StructuredError>,
}

/// Payload of `model.updated` / `provider.updated` events — verified empty
/// (`{}`) on the wire: they are directory-change signals; the catalog itself
/// is fetched on demand.
#[derive(Debug, Clone, Deserialize)]
pub struct ModelOrProviderUpdated {}

#[derive(Debug, Clone, Deserialize)]
pub struct UsageUpdated {
    #[serde(flatten)]
    pub session: SessionRef,
    #[serde(default)]
    pub cost: Option<f64>,
    #[serde(default)]
    pub tokens: Option<Usage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionRenamed {
    #[serde(flatten)]
    pub session: SessionRef,
    pub title: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionCreated {
    #[serde(default)]
    pub sessionID: Option<String>,
    #[serde(default)]
    pub slug: Option<Value>,
    #[serde(default)]
    pub version: Option<Value>,
    #[serde(default)]
    pub projectID: Option<Value>,
    #[serde(default)]
    pub location: Option<Value>,
    #[serde(default)]
    pub subpath: Option<Value>,
    /// Parent session id — present when this is a subagent (child) session
    /// (wire-verified: spawned sessions carry `parentID` + `title` + `agent`).
    #[serde(default)]
    pub parentID: Option<String>,
    /// Human-readable title (child sessions carry the task prompt title,
    /// used for the `${child.title}: …` ACP projection prefix).
    #[serde(default)]
    pub title: Option<String>,
}

/// The `source` of a permission prompt: the tool call that triggered it.
#[derive(Debug, Clone, Deserialize)]
pub struct PermissionSource {
    /// Present in the wire frames (`"type": "tool"`); modeled loosely in
    /// case other source kinds appear later.
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub messageID: Option<String>,
    pub id: String,
}

/// Payload of a `permission.asked` SSE event (wire shape captured in
/// tests/fixtures/perm-asked.sse.jsonl): `data.id` is the requestID that the
/// permission reply must target, `resources` are the human-readable
/// descriptions of what the tool wanted to run.
#[derive(Debug, Clone, Deserialize)]
pub struct PermissionAsked {
    /// The permission request ID (`per_...`).
    pub id: String,
    pub sessionID: String,
    /// The permission action kind (`"shell"` for bash tool calls).
    pub action: String,
    /// Descriptions of the requested operation (e.g. the shell command).
    pub resources: Vec<String>,
    #[serde(default)]
    pub save: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub source: Option<PermissionSource>,
}

/// Payload of a `permission.replied` SSE event — the server echoing the
/// reply back. The bridge ignores these (it already knows the outcome it
/// forwarded); the type exists so the event decodes and is skipped
/// explicitly instead of falling through silently.
#[derive(Debug, Clone, Deserialize)]
pub struct PermissionReplied {
    pub sessionID: String,
    pub requestID: String,
    #[serde(default)]
    pub reply: Option<String>,
}

/// Typed decoding of the event kinds the bridge maps to ACP updates.
///
/// The `permission.*` pair is verified on the wire (both fixtures in
/// tests/fixtures/): `permission.asked` drives the ACP permission flow in
/// `acp::agent`, `permission.replied` is the server echo of the bridge's own
/// reply and is deliberately ignored by the mapping layer.
#[derive(Debug, Clone)]
pub enum SessionEvent {
    // turn lifecycle
    ExecutionStarted(SessionRef),
    ExecutionSucceeded(SessionRef),
    ExecutionFailed(ExecutionFailed),
    /// Cancellation path (official `session.execution.interrupted`); mapped
    /// to stopReason cancelled.
    ExecutionInterrupted(SessionRef),
    // permissions
    PermissionAsked(PermissionAsked),
    /// Server echo of a reply the bridge itself sent — ignored, decoded only.
    PermissionReplied(PermissionReplied),
    // steps
    StepStarted(StepStarted),
    StepStreamed(MessageRef),
    StepEnded(StepEnded),
    // text
    TextStarted(OrdinalRef),
    TextDelta(TextDelta),
    TextEnded(TextEnded),
    // reasoning
    ReasoningStarted(ReasoningStarted),
    ReasoningDelta(TextDelta),
    ReasoningEnded(TextEnded),
    // tools
    ToolInputStarted(ToolInputStarted),
    ToolInputEnded(ToolInputEnded),
    ToolCalled(ToolCalled),
    ToolProgress(ToolRef),
    ToolSuccess(ToolSuccess),
    /// Failure kind for a tool call (`session.tool.failed`, consumed by the
    /// official 2.0.21 adapter; not yet on the bridge's own captures —
    /// decoded tolerantly so a wrong field shape degrades to a silent skip).
    ToolFailed(ToolRefError),
    // steps
    /// `session.step.failed` — per-step error path (wire-verified to exist in
    /// the 2.0.21 event processors; NOT consumed by the official ACP adapter,
    /// which surfaces step errors via `session.execution.failed`).
    StepFailed(StepFailed),
    /// `session.retry.scheduled` — server scheduled an automatic retry.
    RetryScheduled(RetryScheduled),
    /// `session.compaction.started` — compaction of the session began.
    CompactionStarted(CompactionStarted),
    /// `session.compaction.ended` — compaction finished (payload shape
    /// unobserved on the wire; tolerant decode).
    CompactionEnded(CompactionEnded),
    /// `session.compaction.failed` — compaction could not run.
    CompactionFailed(CompactionFailed),
    // catalog
    /// `model.updated {}` — the model directory changed; clients refresh
    /// the model catalog.
    ModelUpdated(ModelOrProviderUpdated),
    /// `provider.updated {}` — the provider directory changed; clients
    /// refresh the model catalog.
    ProviderUpdated(ModelOrProviderUpdated),
    // meta
    UsageUpdated(UsageUpdated),
    Renamed(SessionRenamed),
    SessionCreated(SessionCreated),
}

fn parse<T: serde::de::DeserializeOwned>(x: &Value) -> Option<T> {
    serde_json::from_value(x.clone()).ok()
}

/// Decode an SSE frame's `data` into a typed event. Returns `None` for kinds
/// the bridge does not consume (including plugin `rpc.*` traffic and any
/// future/unverified types) — callers must skip those silently.
pub fn decode_event(kind: &str, data: &Value) -> Option<SessionEvent> {
    match kind {
        "session.execution.started" => parse(data).map(SessionEvent::ExecutionStarted),
        "session.execution.succeeded" => parse(data).map(SessionEvent::ExecutionSucceeded),
        "session.execution.failed" => parse(data).map(SessionEvent::ExecutionFailed),
        "session.execution.interrupted" => parse(data).map(SessionEvent::ExecutionInterrupted),
        "permission.asked" => parse(data).map(SessionEvent::PermissionAsked),
        "permission.replied" => parse(data).map(SessionEvent::PermissionReplied),
        "session.step.started" => parse(data).map(SessionEvent::StepStarted),
        "session.step.streamed" => parse(data).map(SessionEvent::StepStreamed),
        "session.step.ended" => parse(data).map(SessionEvent::StepEnded),
        "session.text.started" => parse(data).map(SessionEvent::TextStarted),
        "session.text.delta" => parse(data).map(SessionEvent::TextDelta),
        "session.text.ended" => parse(data).map(SessionEvent::TextEnded),
        "session.reasoning.started" => parse(data).map(SessionEvent::ReasoningStarted),
        "session.reasoning.delta" => parse(data).map(SessionEvent::ReasoningDelta),
        "session.reasoning.ended" => parse(data).map(SessionEvent::ReasoningEnded),
        "session.tool.input.started" => parse(data).map(SessionEvent::ToolInputStarted),
        "session.tool.input.ended" => parse(data).map(SessionEvent::ToolInputEnded),
        "session.tool.called" => parse(data).map(SessionEvent::ToolCalled),
        "session.tool.progress" => parse(data).map(SessionEvent::ToolProgress),
        "session.tool.success" => parse(data).map(SessionEvent::ToolSuccess),
        "session.tool.failed" => parse(data).map(SessionEvent::ToolFailed),
        "session.step.failed" => parse(data).map(SessionEvent::StepFailed),
        "session.retry.scheduled" => parse(data).map(SessionEvent::RetryScheduled),
        "session.compaction.started" => parse(data).map(SessionEvent::CompactionStarted),
        "session.compaction.ended" => parse(data).map(SessionEvent::CompactionEnded),
        "session.compaction.failed" => parse(data).map(SessionEvent::CompactionFailed),
        "model.updated" => parse(data).map(SessionEvent::ModelUpdated),
        "provider.updated" => parse(data).map(SessionEvent::ProviderUpdated),
        "session.usage.updated" => parse(data).map(SessionEvent::UsageUpdated),
        "session.renamed" => parse(data).map(SessionEvent::Renamed),
        "session.created" => parse(data).map(SessionEvent::SessionCreated),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full successful tool turn capture must decode end-to-end: every
    /// frame parses as an envelope, every ACP-relevant kind decodes.
    /// Skipped-by-design kinds: plugin `rpc.*`, `server.connected`,
    /// `session.inbox.*` (the bridge gets the inbox message from the prompt
    /// response, not from events).
    #[test]
    fn decode_tool_turn_capture() {
        let raw = include_str!("../tests/fixtures/sse-tool-turn.sse");
        let mut envelopes = 0;
        let mut decoded = 0;
        for line in raw.lines() {
            let line = line.trim();
            if !line.starts_with("data: ") || line == "data: " {
                continue;
            }
            let env: EventEnvelope = serde_json::from_str(&line[6..]).expect("envelope parses");
            envelopes += 1;
            let skipped = env.kind.starts_with("rpc.")
                || matches!(
                    env.kind.as_str(),
                    "server.connected" | "session.inbox.enqueued" | "session.inbox.delivered"
                );
            if skipped {
                continue;
            }
            assert!(
                decode_event(&env.kind, &env.data).is_some(),
                "frame kind `{}` must decode to a typed event",
                env.kind
            );
            decoded += 1;
        }
        assert!(decoded > 20, "expected a real turn, got {decoded} relevant frames");
        assert!(envelopes > decoded, "fixture should contain skipped kinds too");
    }

    /// Wave 3: the captured permission-ask turn must decode end-to-end, and
    /// the ask frame must expose the exact fields the ACP mapping needs
    /// (requestID `data.id`, tool-call source, shell action + command).
    #[test]
    fn decode_permission_asked_capture() {
        let raw = include_str!("../tests/fixtures/perm-asked.sse.jsonl");
        let mut envelopes = 0;
        let mut decoded = 0;
        let mut asked: Option<PermissionAsked> = None;
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line == ": heartbeat" {
                continue;
            }
            let env: EventEnvelope = serde_json::from_str(
                line.strip_prefix("data: ").expect("data: prefix"),
            )
            .expect("envelope parses");
            envelopes += 1;
            let skipped = env.kind.starts_with("rpc.")
                || matches!(
                    env.kind.as_str(),
                    "server.connected"
                        | "session.inbox.enqueued"
                        | "session.inbox.delivered"
                        | "project.updated"
                        | "session.instructions.updated"
                        | "session.model.selected"
                );
            if skipped {
                continue;
            }
            let event = decode_event(&env.kind, &env.data)
                .unwrap_or_else(|| panic!("frame kind `{}` must decode to a typed event", env.kind));
            if let SessionEvent::PermissionAsked(pa) = event {
                asked = Some(pa);
            }
            decoded += 1;
        }
        let asked = asked.expect("fixture contains one permission.asked frame");
        assert!(asked.id.starts_with("per_"));
        assert!(asked.sessionID.starts_with("ses_"));
        assert_eq!(asked.action, "shell");
        assert_eq!(asked.resources, vec!["echo hi"]);
        assert_eq!(asked.save.as_deref(), Some(&["echo *".to_string()][..]));
        assert_eq!(asked.metadata.as_ref(), Some(&serde_json::Map::new()));
        let source = asked.source.expect("source present");
        assert_eq!(source.kind.as_deref(), Some("tool"));
        assert!(source.messageID.as_deref().unwrap_or("").starts_with("msg_"));
        assert!(source.id.starts_with("call_"));
        assert!(decoded > 5, "expected a real turn, got {decoded} relevant frames");
        assert!(envelopes > decoded, "fixture should contain skipped kinds too");
    }

    /// Wave 3: the post-reply resume capture decodes fully, including the
    /// `permission.replied` echo (which the bridge ignores) and the turn's
    /// terminal `session.execution.succeeded`.
    #[test]
    fn decode_permission_replied_resume_capture() {
        let raw = include_str!("../tests/fixtures/perm-replied-resume.sse.jsonl");
        let mut decoded = 0;
        let mut saw_replied = false;
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line == ": heartbeat" {
                continue;
            }
            let env: EventEnvelope = serde_json::from_str(
                line.strip_prefix("data: ").expect("data: prefix"),
            )
            .expect("envelope parses");
            match env.kind.as_str() {
                "server.connected" | "project.updated" => continue,
                "permission.replied" => {
                    if let SessionEvent::PermissionReplied(replied) =
                        decode_event(&env.kind, &env.data).expect("replied decodes")
                    {
                        assert!(replied.sessionID.starts_with("ses_"));
                        assert!(replied.requestID.starts_with("per_"));
                        assert_eq!(replied.reply.as_deref(), Some("once"));
                        saw_replied = true;
                    }
                }
                _ => {
                    assert!(
                        decode_event(&env.kind, &env.data).is_some(),
                        "frame kind `{}` must decode to a typed event",
                        env.kind
                    );
                }
            }
            decoded += 1;
        }
        assert!(saw_replied, "fixture must contain the permission.replied echo");
        assert!(decoded > 10, "expected the resumed turn, got {decoded} relevant frames");
        assert!(
            raw.contains("session.execution.succeeded"),
            "fixture resumes to a successful turn end"
        );
    }

    /// Wave 4: the live-captured subagent (child-session) turn must decode
    /// end-to-end. Wire facts pinned by this fixture:
    ///   • child events flow in the shared stream under the CHILD's own
    ///     sessionID (no marker on the tool events themselves),
    ///   • `session.created` announces each child with `parentID` + `title`,
    ///   • `model.updated` / `provider.updated` carry an empty payload `{}`.
    #[test]
    fn decode_subagent_child_capture() {
        let raw = include_str!("../tests/fixtures/subagent-child.sse.jsonl");
        let mut decoded = 0;
        let mut created_with_parent = 0;
        let mut child_tool_events = 0;
        let mut saw_model_updated = false;
        let mut saw_provider_updated = false;
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line == ": heartbeat" {
                continue;
            }
            let env: EventEnvelope =
                serde_json::from_str(line.strip_prefix("data: ").expect("data: prefix"))
                    .expect("envelope parses");
            let skipped = env.kind.starts_with("rpc.")
                || matches!(
                    env.kind.as_str(),
                    "server.connected"
                        | "session.inbox.enqueued"
                        | "session.inbox.delivered"
                        | "project.updated"
                        | "session.instructions.updated"
                        | "session.permissions"
                        | "session.model.selected"
                );
            if skipped {
                continue;
            }
            let event = decode_event(&env.kind, &env.data)
                .unwrap_or_else(|| panic!("frame kind `{}` must decode", env.kind));
            match event {
                SessionEvent::SessionCreated(created) => {
                    if created.parentID.is_some() {
                        created_with_parent += 1;
                        assert!(
                            created.title.as_deref().is_some_and(|t| !t.is_empty()),
                            "child session.created carries a title"
                        );
                    }
                }
                SessionEvent::ModelUpdated(_) => saw_model_updated = true,
                SessionEvent::ProviderUpdated(_) => saw_provider_updated = true,
                _ => {}
            }
            // Count tool-kind frames under a child sessionID (not the main
            // fixture session). The fixture has one parent + two children.
            if matches!(
                env.kind.as_str(),
                "session.tool.input.started"
                    | "session.tool.input.ended"
                    | "session.tool.called"
                    | "session.tool.success"
                    | "session.tool.progress"
            ) && env.data.get("sessionID").and_then(|v| v.as_str()) != Some("ses_f0434a963ffendAHE24ngf0ScP")
            {
                child_tool_events += 1;
            }
            decoded += 1;
        }
        assert!(decoded > 100, "expected the full subagent turns, got {decoded} frames");
        assert!(created_with_parent >= 2, "children announced with parentID: {created_with_parent}");
        assert!(child_tool_events >= 4, "child tool events ride their own sessionID");
        assert!(saw_model_updated && saw_provider_updated, "catalog reload events present");
    }

    /// Wave 4: the live-captured compaction turn (POST /api/session/…/compact)
    /// — `session.compaction.started` / `session.compaction.failed` shapes.
    #[test]
    fn decode_compaction_capture() {
        let raw = include_str!("../tests/fixtures/compaction.sse.jsonl");
        let mut saw_started = false;
        let mut saw_failed = false;
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line == ": heartbeat" {
                continue;
            }
            let env: EventEnvelope =
                serde_json::from_str(line.strip_prefix("data: ").expect("data: prefix"))
                    .expect("envelope parses");
            match env.kind.as_str() {
                "server.connected" | "session.inbox.enqueued" | "session.inbox.delivered" => {}
                "session.compaction.started" => {
                    let SessionEvent::CompactionStarted(started) =
                        decode_event(&env.kind, &env.data).expect("started decodes")
                    else {
                        panic!("wrong variant");
                    };
                    assert_eq!(started.reason.as_deref(), Some("manual"));
                    assert!(started.inputID.as_deref().is_some_and(|i| i.starts_with("msg_")));
                    saw_started = true;
                }
                "session.compaction.failed" => {
                    let SessionEvent::CompactionFailed(failed) =
                        decode_event(&env.kind, &env.data).expect("failed decodes")
                    else {
                        panic!("wrong variant");
                    };
                    assert_eq!(
                        failed.error.as_ref().and_then(|e| e.kind.as_deref()),
                        Some("compaction.unavailable")
                    );
                    saw_failed = true;
                }
                _ => {
                    assert!(
                        decode_event(&env.kind, &env.data).is_some(),
                        "frame kind `{}` must decode",
                        env.kind
                    );
                }
            }
        }
        assert!(saw_started && saw_failed, "compaction started+failed frames present");
        assert!(raw.contains("session.execution.succeeded"), "compact run ends the turn");
    }

    /// The persisted tool part must expose the diff-fix data source.
    #[test]
    fn persisted_tool_part_carries_filediff() {
        let raw = include_str!("../tests/fixtures/messages-tool-turn.json");
        let env: MessagesEnvelope = serde_json::from_str(raw).expect("messages parse");
        let mut found = false;
        for record in &env.data {
            for part in record.content.iter().flatten() {
                if let Part::Tool { state: ToolState::Completed { metadata, .. }, .. } = part {
                    let meta = metadata.as_ref().expect("completed tool has metadata");
                    let fd = meta.filediff.as_ref().expect("filediff present");
                    assert!(fd.patch.starts_with("Index: "));
                    assert!(fd.file.starts_with('/'), "file is absolute");
                    assert!(meta.title.is_some());
                    found = true;
                }
            }
        }
        assert!(found, "fixture must contain a completed tool part");
    }

    // ======================= Wave 5: aft hoist dialect =======================

    /// Parse every frame of an aft capture, returning the decoded
    /// `ToolSuccess` events (in fixture order).
    fn decode_aft_capture(path: &str) -> Vec<(String, ToolSuccess)> {
        let raw = aft_fixture(path);
        let mut successes = Vec::new();
        let mut decoded_kinds = 0usize;
        for line in raw.lines() {
            let line = line.trim();
            if !line.starts_with("data: ") || line == "data: " {
                continue;
            }
            let env: EventEnvelope = serde_json::from_str(&line[6..]).expect("envelope parses");
            let skipped = env.kind.starts_with("rpc.")
                || matches!(
                    env.kind.as_str(),
                    "server.connected"
                        | "session.inbox.enqueued"
                        | "session.inbox.delivered"
                        | "project.updated"
                        | "session.instructions.updated"
                        | "session.permissions"
                        | "session.model.selected"
                );
            if skipped {
                continue;
            }
            match decode_event(&env.kind, &env.data) {
                Some(SessionEvent::ToolSuccess(t)) => successes.push((env.kind.clone(), t)),
                Some(_) => decoded_kinds += 1,
                // After the dto.rs additions for Wave 5 there must be NO
                // frame left that silently falls into `Unknown`.
                None => panic!("frame kind `{}` must decode to a typed event", env.kind),
            }
        }
        assert!(decoded_kinds > 10, "expected a real turn, got {decoded_kinds} non-tool frames");
        successes
    }

    fn aft_fixture(name: &str) -> &'static str {
        match name {
            "aft-tool-turn" => include_str!("../tests/fixtures/aft-tool-turn.sse"),
            "aft-image-read" => include_str!("../tests/fixtures/aft-image-read.sse"),
            other => panic!("unknown aft fixture {other}"),
        }
    }

    /// aft read/edit/apply_patch turn: every success decodes with typed
    /// content (no `Unknown` escapes); the edit carries BOTH `filediff` and
    /// `diff`, while apply_patch carries only the `diff` fallback string.
    #[test]
    fn decode_aft_tool_turn_capture() {
        let successes = decode_aft_capture("aft-tool-turn");
        assert_eq!(successes.len(), 3, "read + edit + apply_patch");
        // Every content part is typed; none escaped to Unknown.
        for (_, t) in &successes {
            if let Some(content) = &t.content {
                assert!(
                    content.iter().all(|c| !matches!(c, ToolContent::Unknown)),
                    "no Unknown escapes in {}",
                    t.base.id
                );
            }
        }
        let edit = &successes[1].1;
        let edit_meta = edit.metadata.as_ref().expect("edit metadata");
        assert!(edit_meta.filediff.is_some(), "edit carries the structured filediff");
        assert!(edit_meta.diff.is_some(), "edit carries the diff string too");
        let patch_meta = successes[2].1.metadata.as_ref().expect("apply_patch metadata");
        assert!(patch_meta.filediff.is_none(), "apply_patch has no filediff");
        assert!(patch_meta.diff.is_some(), "apply_patch carries only the diff string");
    }

    /// aft image read: the `{"type":"file","uri":"data:…;base64,…",
    /// "mime":"image/png"}` part decodes to the File variant (the aft
    /// hoist's only wire deviation from core — fields on the wire shape).
    #[test]
    fn decode_aft_image_read_capture() {
        let successes = decode_aft_capture("aft-image-read");
        assert_eq!(successes.len(), 1);
        let content = successes[0].1.content.as_ref().expect("read has content");
        assert_eq!(content.len(), 2, "text + file");
        assert!(matches!(&content[0], ToolContent::Text { .. }));
        let ToolContent::File { uri, mime } = &content[1] else {
            panic!("file part must decode to the File variant, got {:?}", content[1]);
        };
        assert!(uri.starts_with("data:image/png;base64,"), "data-URI on the wire");
        assert_eq!(mime.as_deref(), Some("image/png"));
        assert!(matches!(&content[1], ToolContent::File { .. }));
    }
}
