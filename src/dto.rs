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
#[derive(Debug, Clone, Copy, Serialize)]
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
    #[serde(other)]
    Unknown,
}

impl ToolContent {
    pub fn text(&self) -> Option<&str> {
        match self {
            ToolContent::Text { text } => Some(text),
            ToolContent::Unknown => None,
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
}

/// Typed decoding of the event kinds the bridge maps to ACP updates.
///
/// `permission` variants are deliberately absent: the permission-request
/// event type name is UNVERIFIED (no permission prompt fired during capture).
/// Discover it live (see docs/opencode-api.md), then add a variant here.
#[derive(Debug, Clone)]
pub enum SessionEvent {
    // turn lifecycle
    ExecutionStarted(SessionRef),
    ExecutionSucceeded(SessionRef),
    ExecutionFailed(ExecutionFailed),
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

    /// The persisted tool part must expose the diff-fix data source.
    #[test]
    fn persisted_tool_part_carries_filediff() {
        let raw = include_str!("../tests/fixtures/messages-tool-turn.json");
        let env: MessagesEnvelope = serde_json::from_str(raw).expect("messages parse");
        let mut found = false;
        for record in &env.data {
            for part in record.content.iter().flatten() {
                if let Part::Tool { state, .. } = part {
                    if let ToolState::Completed { metadata, .. } = state {
                        let meta = metadata.as_ref().expect("completed tool has metadata");
                        let fd = meta.filediff.as_ref().expect("filediff present");
                        assert!(fd.patch.starts_with("Index: "));
                        assert!(fd.file.starts_with('/'), "file is absolute");
                        assert!(meta.title.is_some());
                        found = true;
                    }
                }
            }
        }
        assert!(found, "fixture must contain a completed tool part");
    }
}
