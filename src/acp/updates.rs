//! Event → ACP session update mapping (Lane B core).
//!
//! Pure functions, no IO — unit-testable without any HTTP/SSE transport.
//! The session-ID envelope (`SessionNotification.session_id`) is applied by
//! the agent layer (`agent.rs`); this layer emits bare `SessionUpdate`s.
//!
//! Word of caution on the wire: opencode's `session.*` events do NOT carry
//! ACP-style part IDs, only `{assistantMessageID, ordinal}`. ACP chunks need a
//! `messageId` — we reuse the assistant message ID for both text and reasoning
//! chunks of the same assistant message (each chunk type gets its own
//! `message_id` from the same ID; the client groups by that ID).

use std::collections::{HashMap, HashSet};

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, Cost, ImageContent, SessionInfoUpdate, SessionUpdate, TextContent,
    ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
};

use crate::dto::{self, ToolContent, ToolMetadata};

use super::diff;

/// Namespace of a subagent (child) session whose tool events are projected
/// into the parent turn (official `#48232` behavior: when the client does not
/// declare `opencode/child-session-updates`, child tool events surface in the
/// parent stream as nested tool calls).
///
/// Wire facts (captured in tests/fixtures/subagent-child.sse.jsonl): child
/// events flow through the shared `/api/event` stream carrying the CHILD's
/// own sessionID; `session.created` announces the child with `parentID` +
/// `title` + `agent`. The ACP mapping prefixes every projected tool call id
/// with `${child.id}:` and every title with `${child.title}: …`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolNs {
    /// Child session id (`ses_...`).
    pub child_id: String,
    /// Child session title (from `session.created`).
    pub child_title: String,
}

impl ToolNs {
    /// The ACP toolCallId for a raw opencode `call_...` id of this child.
    pub fn tool_call_id(&self, raw: &str) -> String {
        format!("{}:{}", self.child_id, raw)
    }

    /// The ACP display title for a raw title of this child.
    pub fn title(&self, raw: &str) -> String {
        format!("{}: {}", self.child_title, raw)
    }
}

/// The identity of an opencode tool event's call — raw `call_...` id, plus
/// the child namespace when the event belongs to a projected subagent.
fn tool_call_id(ns: Option<&ToolNs>, raw: &str) -> String {
    match ns {
        Some(ns) => ns.tool_call_id(raw),
        None => raw.to_string(),
    }
}

fn tool_title(ns: Option<&ToolNs>, raw: &str) -> String {
    match ns {
        Some(ns) => ns.title(raw),
        None => raw.to_string(),
    }
}


/// Per-session bookkeeping for the event → update mapping.
///
/// The only cross-event state the wire forces on us: opencode tool call ids
/// (`call_*`) carry no display title after `input.started`, so the name seen
/// there is indexed for later update stages. Everything else (message ids,
/// ordinals) is carried inside each event payload.
#[derive(Debug, Default)]
pub struct MappingState {
    /// opencode tool call id → display title (from `session.tool.input.started`).
    tool_titles: HashMap<String, String>,
    /// opencode tool call id → parsed tool input, for the ACP permission
    /// prompt (Wave 3: `state.input` of the pending tool call). Populated
    /// best-effort from `session.tool.input.ended` (raw JSON text) and
    /// authoritatively from `session.tool.called` (parsed input object).
    /// Keyed by the FINAL (namespaced) toolCallId so the permission prompt
    /// and the cancel drain address the same calls the client sees.
    pub(crate) tool_inputs: HashMap<String, serde_json::Value>,
    /// Tool calls still open (final toolCallIds): advertised to the client
    /// but not yet completed/failed. Read by the cancel drain to abandon
    /// stragglers with `Failed` + "Cancelled".
    open_tools: HashSet<String>,
    /// Pending automatic retry (from `session.retry.scheduled`) — cleared on
    /// the next `step.started`, folded into the PromptResponse `_meta` when
    /// the turn ends before the retry fires.
    retry: Option<serde_json::Value>,
    /// `--no-aft`: drop the aft hoist adaptations (File/image content
    /// passthrough). Diff extraction is NOT gated — `filediff`/`diff` are
    /// dialect-neutral and keep working under both.
    no_aft: bool,
}

impl MappingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: `--no-aft` — disable the aft hoist adaptations (File/image
    /// content passthrough). Diff extraction stays enabled (dialect-neutral).
    pub fn with_no_aft(mut self, no_aft: bool) -> Self {
        self.no_aft = no_aft;
        self
    }

    /// The parsed input of a tool call, if seen this turn (`tool.input.ended`
    /// / `tool.called`). Used to build the permission prompt's `state.input`.
    pub fn tool_input(&self, tool_call_id: &str) -> Option<&serde_json::Value> {
        self.tool_inputs.get(tool_call_id)
    }

    /// Mark a tool call open. `id` must be the FINAL (namespaced) toolCallId.
    pub(crate) fn open_tool(&mut self, id: impl Into<String>) {
        self.open_tools.insert(id.into());
    }

    /// Mark a tool call closed (completed or failed).
    pub(crate) fn close_tool(&mut self, id: &str) {
        self.open_tools.remove(id);
    }

    /// The still-open tool calls at end-of-turn, abandoned as failed with
    /// title "Cancelled" (official cancel-drain behavior).
    pub fn abandon_open_tools(&self) -> Vec<SessionUpdate> {
        self.open_tools
            .iter()
            .map(|id| {
                tool_update(
                    id,
                    ToolCallUpdateFields::new()
                        .status(ToolCallStatus::Failed)
                        .title("Cancelled"),
                )
            })
            .collect()
    }

    /// Pending retry meta object (the value for `_meta["opencode/retry"]`).
    pub fn retry_meta(&self) -> Option<&serde_json::Value> {
        self.retry.as_ref()
    }

    /// Take (and clear) the pending retry — used by the step.started clear.
    fn take_retry(&mut self) -> Option<serde_json::Value> {
        self.retry.take()
    }
}

/// Map one decoded opencode event to zero or more ACP session updates, in
/// wire order. Events with no ACP counterpart (step lifecycle, inbox, …) map
/// to the empty vec.
pub fn to_updates(event: &dto::SessionEvent, state: &mut MappingState) -> Vec<SessionUpdate> {
    match event {
        // ---------- text ----------
        dto::SessionEvent::TextDelta(d) => vec![SessionUpdate::AgentMessageChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(d.delta.clone())))
                .message_id(d.base.assistantMessageID.as_str()),
        )],

        // ---------- reasoning ----------
        dto::SessionEvent::ReasoningDelta(d) => vec![SessionUpdate::AgentThoughtChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(d.delta.clone())))
                .message_id(d.base.assistantMessageID.as_str()),
        )],

        // ---------- tools (parent namespace) ----------
        dto::SessionEvent::ToolInputStarted(_)
        | dto::SessionEvent::ToolInputEnded(_)
        | dto::SessionEvent::ToolCalled(_)
        | dto::SessionEvent::ToolSuccess(_)
        | dto::SessionEvent::ToolFailed(_) => to_tool_updates(event, None, state),
        dto::SessionEvent::ToolProgress(_) => {
            // No-op: the call is already `in_progress` since `tool.called`.
            // Kept as its own arm so the mapping is explicit and greppable.
            vec![]
        }

        // ---------- scheduling / maintenance ----------
        dto::SessionEvent::RetryScheduled(r) => {
            let mut obj = serde_json::Map::new();
            if let Some(attempt) = r.attempt {
                obj.insert("attempt".into(), serde_json::Value::from(attempt));
            }
            if let Some(next) = &r.nextRetryAt {
                obj.insert("nextRetryAt".into(), next.clone());
            }
            if let Some(err) = &r.error {
                obj.insert("error".into(), error_value(err));
            }
            let obj = serde_json::Value::Object(obj);
            state.retry = Some(obj.clone());
            vec![info_update_with_meta("opencode/retry", &obj)]
        }
        dto::SessionEvent::StepStarted(_) => {
            // Official: a pending retry is cleared when the next step starts.
            state.take_retry().map_or_else(Vec::new, |_| {
                vec![info_update_with_meta("opencode/retry", &serde_json::Value::Null)]
            })
        }
        dto::SessionEvent::CompactionStarted(c) => {
            let mut obj = serde_json::Map::new();
            obj.insert("status".into(), "started".into());
            if let Some(input) = &c.inputID {
                obj.insert("messageId".into(), input.clone().into());
            }
            if let Some(reason) = &c.reason {
                obj.insert("reason".into(), reason.clone().into());
            }
            vec![info_update_with_meta(
                "opencode/compaction",
                &serde_json::Value::Object(obj),
            )]
        }
        dto::SessionEvent::CompactionEnded(c) => {
            let mut obj = serde_json::Map::new();
            obj.insert("status".into(), "ended".into());
            if let Some(input) = &c.inputID {
                obj.insert("messageId".into(), input.clone().into());
            }
            if let Some(reason) = &c.reason {
                obj.insert("reason".into(), reason.clone().into());
            }
            vec![info_update_with_meta(
                "opencode/compaction",
                &serde_json::Value::Object(obj),
            )]
        }
        dto::SessionEvent::CompactionFailed(c) => {
            let mut obj = serde_json::Map::new();
            obj.insert("status".into(), "failed".into());
            if let Some(input) = &c.inputID {
                obj.insert("messageId".into(), input.clone().into());
            }
            if let Some(reason) = &c.reason {
                obj.insert("reason".into(), reason.clone().into());
            }
            if let Some(err) = &c.error {
                obj.insert("error".into(), error_value(err));
            }
            vec![info_update_with_meta(
                "opencode/compaction",
                &serde_json::Value::Object(obj),
            )]
        }

        // ---------- steps ----------
        dto::SessionEvent::StepFailed(_) => {
            // No ACP update: v1 has no per-step failure surface and the
            // official adapter does not consume this event — the error
            // taxonomy arrives via `session.execution.failed`. The agent
            // layer logs the step-level outcome (`step_failed_outcome`).
            vec![]
        }

        // ---------- catalog ----------
        dto::SessionEvent::ModelUpdated(_) | dto::SessionEvent::ProviderUpdated(_) => {
            // The events are directory-change signals; the catalog push
            // itself is built by the agent layer (it needs a backend fetch).
            vec![]
        }

        // ---------- meta ----------
        dto::SessionEvent::UsageUpdated(u) => {
            let tokens = u.tokens.as_ref();
            let used = tokens.map(|t| {
                t.input.unwrap_or(0) + t.output.unwrap_or(0) + t.reasoning.unwrap_or(0)
            });
            let mut usage = UsageUpdate::new(used.unwrap_or(0), 0);
            if let Some(cost) = u.cost {
                usage = usage.cost(Cost::new(cost, "USD"));
            }
            vec![SessionUpdate::UsageUpdate(usage)]
        }
        dto::SessionEvent::Renamed(r) => vec![SessionUpdate::SessionInfoUpdate(
            SessionInfoUpdate::new().title(r.title.clone()),
        )],

        // Everything else has no ACP update counterpart.
        _ => vec![],
    }
}

/// Tool-event arms shared by the parent namespace (`to_updates`) and the
/// child projection (`to_child_updates`). `ns` is `Some` only for child
/// events: every id is prefixed `${child.id}:`, every title `${child.title}:`.
fn to_tool_updates(
    event: &dto::SessionEvent,
    ns: Option<&ToolNs>,
    state: &mut MappingState,
) -> Vec<SessionUpdate> {
    match event {
        dto::SessionEvent::ToolInputStarted(t) => {
            // ACP convention (mirrors the TS bridge): while input streams, the
            // call is advertised as `pending` with the tool name as title.
            let id = tool_call_id(ns, &t.base.id);
            state.tool_titles.insert(id.clone(), t.name.clone());
            state.open_tool(id.clone());
            vec![tool_update(
                &id,
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Pending)
                    .title(tool_title(ns, &t.name)),
            )]
        }
        dto::SessionEvent::ToolInputEnded(t) => {
            // `text` is the raw JSON input string — pass it through verbatim
            // (the pending call's streaming input, as a JSON string value).
            // Also cache the parsed input for the permission prompt.
            let id = tool_call_id(ns, &t.base.id);
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&t.text) {
                state.tool_inputs.insert(id.clone(), parsed);
            }
            vec![tool_update(
                &id,
                ToolCallUpdateFields::new().raw_input(serde_json::Value::String(t.text.clone())),
            )]
        }
        dto::SessionEvent::ToolCalled(t) => {
            // Parsed input now available → mark in progress.
            let id = tool_call_id(ns, &t.base.id);
            state.tool_inputs.insert(id.clone(), t.input.clone());
            vec![tool_update(
                &id,
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::InProgress)
                    .raw_input(t.input.clone()),
            )]
        }
        dto::SessionEvent::ToolSuccess(t) => {
            let id = tool_call_id(ns, &t.base.id);
            state.close_tool(&id);
            let mut fields = ToolCallUpdateFields::new().status(ToolCallStatus::Completed);
            if let Some(meta) = &t.metadata {
                if let Some(title) = &meta.title {
                    fields = fields.title(tool_title(ns, title));
                }
            }
            if let Some(content) = &t.content {
                fields = fields.content(Some(tool_result_blocks(content, &t.metadata, state.no_aft)));
            } else if let Some(meta) = &t.metadata {
                // Content-less success (e.g. progress-only tools) may still
                // carry diffs — never drop the file changes.
                let blocks = tool_result_blocks(&[], &Some(meta.clone()), state.no_aft);
                if !blocks.is_empty() {
                    fields = fields.content(Some(blocks));
                }
            }
            vec![tool_update(&id, fields)]
        }
        dto::SessionEvent::ToolFailed(t) => {
            let id = tool_call_id(ns, &t.base.id);
            state.close_tool(&id);
            // v1 has no error field on tool updates: the failure surfaces as
            // `Failed` with the error message in the raw output.
            let message = t
                .error
                .message
                .clone()
                .unwrap_or_else(|| "Tool execution failed".to_string());
            vec![tool_update(
                &id,
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Failed)
                    .raw_output(serde_json::Value::String(message)),
            )]
        }
        _ => vec![],
    }
}

/// Map a projected CHILD-session event to ACP updates. Only tool events have
/// a projection (nested tool calls); child lifecycle/text/reasoning events
/// are not surfaced (the parent's own stream carries the orchestration).
pub fn to_child_updates(
    event: &dto::SessionEvent,
    ns: &ToolNs,
    state: &mut MappingState,
) -> Vec<SessionUpdate> {
    to_tool_updates(event, Some(ns), state)
}

/// `_meta` object for a `session_info_update` (retry / compaction pushes).
fn info_update_with_meta(key: &str, value: &serde_json::Value) -> SessionUpdate {
    let mut meta = serde_json::Map::new();
    meta.insert(key.to_string(), value.clone());
    let mut update = SessionInfoUpdate::new();
    update.meta = Some(meta);
    SessionUpdate::SessionInfoUpdate(update)
}

/// StructuredError → JSON object (for `_meta` error fields).
fn error_value(err: &dto::StructuredError) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if let Some(kind) = &err.kind {
        obj.insert("type".into(), kind.clone().into());
    }
    if let Some(message) = &err.message {
        obj.insert("message".into(), message.clone().into());
    }
    serde_json::Value::Object(obj)
}

/// Outcome of a finished execution — the ACP stop signal.
///
/// ACP v1 has NO `stop` session-update variant: the end of a turn is delivered
/// as the `session/prompt` RESPONSE (`PromptResponse{stop_reason}`). This
/// helper therefore decides *what* to deliver for a turn-completion event.
#[derive(Debug)]
pub enum TurnEnd {
    /// `session.execution.succeeded` → respond `StopReason::EndTurn`.
    EndTurn,
    /// `session.execution.interrupted` → respond `StopReason::Cancelled`.
    Cancelled,
    /// `length` failure (output token limit) → respond `StopReason::MaxTokens`.
    MaxTokens,
    /// `content-filter` failure → respond `StopReason::Refusal`.
    Refusal,
    /// `provider.auth` failure → respond with the ACP authentication-required
    /// error (JSON-RPC -32000), carrying the opencode message.
    AuthRequired { message: Option<String> },
    /// Any other `session.execution.failed` → respond with a JSON-RPC error
    /// carrying the opencode error message (there is no `Error` stop reason
    /// in v1).
    Error { message: Option<String> },
}

/// Official 2.0.21 adapter mapping of an execution-failure error to the ACP
/// turn outcome, extracted from the shipped binary + the dev-clone acp
/// package (`packages/opencode/src/acp/event.ts` response conversion):
/// `provider.auth` → authentication-required error · `content-filter` →
/// refusal · `aborted` → cancelled · `length` → max_tokens · anything else →
/// error stop with the safe message.
pub(crate) fn failure_outcome(error: &dto::StructuredError) -> TurnEnd {
    match error.kind.as_deref() {
        Some("provider.auth") => TurnEnd::AuthRequired { message: error.message.clone() },
        Some("content-filter") => TurnEnd::Refusal,
        Some("aborted") => TurnEnd::Cancelled,
        Some("length") => TurnEnd::MaxTokens,
        _ => TurnEnd::Error { message: error.message.clone() },
    }
}

/// Step-level error taxonomy: `session.step.failed` maps through the same
/// table as execution failures. The bridge does NOT stop the turn on it (the
/// official adapter surfaces step errors via `session.execution.failed`); the
/// agent layer uses this to log/attribute the step-level failure kind.
pub fn step_failed_outcome(event: &dto::SessionEvent) -> Option<TurnEnd> {
    match event {
        dto::SessionEvent::StepFailed(s) => Some(failure_outcome(&s.error)),
        _ => None,
    }
}

/// Map a turn-completion event to the agent-layer stop outcome.
pub fn stop_update(event: &dto::SessionEvent) -> Option<TurnEnd> {
    match event {
        dto::SessionEvent::ExecutionSucceeded(_) => Some(TurnEnd::EndTurn),
        dto::SessionEvent::ExecutionInterrupted(_) => Some(TurnEnd::Cancelled),
        dto::SessionEvent::ExecutionFailed(f) => Some(failure_outcome(&f.error)),
        _ => None,
    }
}

/// The sessionID an event belongs to (for filtering the shared SSE stream),
/// or `None` for events without one (`SessionCreated`).
pub fn event_session_id(event: &dto::SessionEvent) -> Option<&str> {
    match event {
        dto::SessionEvent::ExecutionStarted(r) => Some(&r.sessionID),
        dto::SessionEvent::ExecutionSucceeded(r) => Some(&r.sessionID),
        dto::SessionEvent::ExecutionInterrupted(r) => Some(&r.sessionID),
        dto::SessionEvent::ExecutionFailed(f) => Some(&f.session.sessionID),
        dto::SessionEvent::PermissionAsked(p) => Some(&p.sessionID),
        dto::SessionEvent::PermissionReplied(p) => Some(&p.sessionID),
        dto::SessionEvent::StepStarted(s) => Some(&s.session.sessionID),
        dto::SessionEvent::StepStreamed(m) => Some(&m.sessionID),
        dto::SessionEvent::StepEnded(s) => Some(&s.session.sessionID),
        dto::SessionEvent::TextStarted(o) => Some(&o.sessionID),
        dto::SessionEvent::TextDelta(d) => Some(&d.base.sessionID),
        dto::SessionEvent::TextEnded(d) => Some(&d.base.sessionID),
        dto::SessionEvent::ReasoningStarted(r) => Some(&r.base.sessionID),
        dto::SessionEvent::ReasoningDelta(d) => Some(&d.base.sessionID),
        dto::SessionEvent::ReasoningEnded(d) => Some(&d.base.sessionID),
        dto::SessionEvent::ToolInputStarted(t) => Some(&t.base.sessionID),
        dto::SessionEvent::ToolInputEnded(t) => Some(&t.base.sessionID),
        dto::SessionEvent::ToolCalled(t) => Some(&t.base.sessionID),
        dto::SessionEvent::ToolProgress(t) => Some(&t.sessionID),
        dto::SessionEvent::ToolSuccess(t) => Some(&t.base.sessionID),
        dto::SessionEvent::ToolFailed(t) => Some(&t.base.sessionID),
        dto::SessionEvent::StepFailed(s) => Some(&s.session.sessionID),
        dto::SessionEvent::RetryScheduled(r) => Some(&r.sessionID),
        dto::SessionEvent::CompactionStarted(c) => Some(&c.sessionID),
        dto::SessionEvent::CompactionEnded(c) => Some(&c.sessionID),
        dto::SessionEvent::CompactionFailed(c) => Some(&c.sessionID),
        dto::SessionEvent::ModelUpdated(_) | dto::SessionEvent::ProviderUpdated(_) => None,
        dto::SessionEvent::UsageUpdated(u) => Some(&u.session.sessionID),
        dto::SessionEvent::Renamed(r) => Some(&r.session.sessionID),
        dto::SessionEvent::SessionCreated(_) => None,
        dto::SessionEvent::AgentSelected(sel) => Some(&sel.sessionID),
    }
}

fn tool_update(
    tool_call_id: &str,
    fields: ToolCallUpdateFields,
) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id.to_string(), fields))
}

/// Shared helper: tool result content → ACP `ToolCallContent` (text blocks
/// plus #52636 diff blocks). Used by the live path and the replay path.
/// The aft hoist File part → ACP content block.
///
/// Only `image/*` mimes are mapped — the only verified aft scenario (image
/// reads). Non-image file parts are skipped. The uri must be a data-URI
/// (`data:<mime>;base64,<payload>`); anything else is unverified wire and is
/// skipped rather than guessed. `--no-aft` disables the whole mapping.
fn image_content_block(part: &dto::ToolContent, no_aft: bool) -> Option<ToolCallContent> {
    if no_aft {
        return None;
    }
    let dto::ToolContent::File { uri, mime } = part else {
        return None;
    };
    let mime_type = mime.as_deref()?;
    if !mime_type.starts_with("image/") {
        return None;
    }
    let (prefix, payload) = uri.split_once(";base64,")?;
    if !prefix.starts_with("data:") {
        return None;
    }
    Some(ToolCallContent::from(ContentBlock::Image(
        ImageContent::new(payload.to_string(), mime_type.to_string()).uri(uri.clone()),
    )))
}

/// Map tool output content + metadata to ACP result blocks (shared by the
/// live tool-success path and the session/load replay path).
///
/// - `Text` parts → text blocks (verbatim).
/// - `File` parts (aft hoist) → image blocks, see [`image_content_block`].
/// - metadata → diff blocks via the dialect-neutral `filediff`/`diff` chain.
pub fn tool_result_blocks(
    content: &[ToolContent],
    metadata: &Option<ToolMetadata>,
    no_aft: bool,
) -> Vec<ToolCallContent> {
    let mut blocks: Vec<ToolCallContent> = Vec::new();
    for part in content {
        match part {
            ToolContent::Text { text } => blocks
                .push(ToolCallContent::from(ContentBlock::Text(TextContent::new(text.clone())))),
            ToolContent::File { .. } => {
                if let Some(block) = image_content_block(part, no_aft) {
                    blocks.push(block);
                }
            }
            ToolContent::Unknown => {}
        }
    }
    if let Some(meta) = metadata {
        blocks.extend(diff::diff_blocks(meta));
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Text of a text content block, or panic.
    fn chunk_text(c: &ContentChunk) -> &str {
        match &c.content {
            ContentBlock::Text(t) => t.text.as_str(),
            other => panic!("expected text block, got {other:?}"),
        }
    }

    fn decode_fixture() -> Vec<dto::SessionEvent> {
        let raw = include_str!("../../tests/fixtures/sse-tool-turn.sse");
        let mut events = Vec::new();
        for line in raw.lines() {
            let line = line.trim();
            if !line.starts_with("data: ") || line == "data: " {
                continue;
            }
            let env: dto::EventEnvelope =
                serde_json::from_str(&line[6..]).expect("envelope parses");
            if let Some(ev) = dto::decode_event(&env.kind, &env.data) {
                events.push(ev);
            }
        }
        events
    }

    #[test]
    fn full_tool_turn_maps_to_acp_updates() {
        let events = decode_fixture();
        let mut state = MappingState::new();
        let updates: Vec<SessionUpdate> = events
            .iter()
            .flat_map(|e| to_updates(e, &mut state))
            .collect();

        // 1. First update is the initial usage bump.
        match &updates[0] {
            SessionUpdate::UsageUpdate(_) => {}
            other => panic!("first update should be usage, got {other:?}"),
        }

        // 2. Renamed → SessionInfoUpdate.
        let expected_title = dto_session_title();
        assert!(updates
            .iter()
            .any(|u| matches!(u, SessionUpdate::SessionInfoUpdate(s) if s.title.value().map(|t| t.as_str()) == Some(expected_title.as_str()))));

        // 3. Reasoning chunks stream before the tool call.
        let thought: Vec<&str> = updates
            .iter()
            .filter_map(|u| match u {
                SessionUpdate::AgentThoughtChunk(c) => Some(chunk_text(c)),
                _ => None,
            })
            .collect();
        assert!(!thought.is_empty());
        assert_eq!(thought[0], "The");

        // 4. Tool call state machine: pending → (raw-input) → in_progress → completed.
        let tool: Vec<&ToolCallUpdate> = updates
            .iter()
            .filter_map(|u| match u {
                SessionUpdate::ToolCallUpdate(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(tool.len(), 4, "started/ended/called/success = 4 updates");
        let statuses: Vec<Option<ToolCallStatus>> =
            tool.iter().map(|t| t.fields.status.clone()).collect();
        assert_eq!(
            statuses,
            vec![
                Some(ToolCallStatus::Pending),
                None, // input.ended: only raw_input is set
                Some(ToolCallStatus::InProgress),
                Some(ToolCallStatus::Completed),
            ],
            "pending → in_progress → completed state machine"
        );
        // pending carries the tool name as title.
        assert_eq!(tool[0].fields.title.as_deref(), Some("write"));
        // input.ended carries the raw JSON string verbatim.
        assert_eq!(
            tool[1].fields.raw_input,
            Some(serde_json::Value::String(
                r#"{"content": "bridge test line", "path": "hello-acp-test.txt"}"#.to_string()
            ))
        );
        // called carries the parsed input object.
        assert_eq!(
            tool[2].fields.raw_input,
            Some(serde_json::json!({"content": "bridge test line", "path": "hello-acp-test.txt"}))
        );

        // 5. Completed carries text + the #52636 diff block.
        let completed = &tool[3];
        let blocks = completed.fields.content.as_ref().expect("completed has content");
        assert_eq!(blocks.len(), 2, "one text block + one diff block");
        let ToolCallContent::Content(text) = &blocks[0] else {
            panic!("first block should be text");
        };
        let ContentBlock::Text(text_block) = &text.content else {
            panic!("text content block expected");
        };
        assert!(text_block.text.starts_with("Created new file."));
        let ToolCallContent::Diff(d) = &blocks[1] else {
            panic!("second block should be a diff");
        };
        assert_eq!(d.path.to_string_lossy(), "/tmp/opencode/hello-acp-test.txt");
        assert_eq!(d.old_text, None);
        assert_eq!(d.new_text, "bridge test line");

        // 6. Final text delta streams after the tool.
        let texts: Vec<&str> = updates
            .iter()
            .filter_map(|u| match u {
                SessionUpdate::AgentMessageChunk(c) => Some(chunk_text(c)),
                _ => None,
            })
            .collect();
        assert_eq!(texts, vec!["done"]);

        // 7. Usage updates exist (usage.updated fires three times in the
        //    fixture: session start, after the tool input, after the run).
        let usage_count = updates
            .iter()
            .filter(|u| matches!(u, SessionUpdate::UsageUpdate(_)))
            .count();
        assert_eq!(usage_count, 3);
    }

    #[test]
    fn execution_succeeded_maps_to_end_turn() {
        let events = decode_fixture();
        let end = events
            .iter()
            .find_map(stop_update)
            .expect("fixture has an execution.succeeded");
        match end {
            TurnEnd::EndTurn => {}
            other => panic!("expected EndTurn, got {other:?}"),
        }
    }

    #[test]
    fn execution_failed_maps_to_error_outcome() {
        let ev = dto::SessionEvent::ExecutionFailed(dto::ExecutionFailed {
            session: dto::SessionRef { sessionID: "ses_x".into() },
            error: dto::StructuredError {
                kind: Some("internal".into()),
                message: Some("boom".into()),
            },
        });
        match stop_update(&ev) {
            Some(TurnEnd::Error { message }) => assert_eq!(message.as_deref(), Some("boom")),
            other => panic!("expected Error outcome, got {other:?}"),
        }
    }

    /// Wave 3: the official failure-kind → stop outcome table.
    #[test]
    fn failure_kind_mapping_table() {
        type Case = (&'static str, fn(&TurnEnd) -> bool, Option<&'static str>);

        fn failed(kind: &str) -> dto::SessionEvent {
            dto::SessionEvent::ExecutionFailed(dto::ExecutionFailed {
                session: dto::SessionRef { sessionID: "ses_x".into() },
                error: dto::StructuredError {
                    kind: Some(kind.into()),
                    message: Some(format!("{kind} boom")),
                },
            })
        }
        // (kind, expected TurnEnd variant, message check)
        let cases: Vec<Case> = vec![
            ("provider.auth", |t| matches!(t, TurnEnd::AuthRequired { .. }), None),
            ("content-filter", |t| matches!(t, TurnEnd::Refusal), None),
            ("aborted", |t| matches!(t, TurnEnd::Cancelled), None),
            ("length", |t| matches!(t, TurnEnd::MaxTokens), None),
            ("internal", |t| matches!(t, TurnEnd::Error { .. }), Some("internal boom")),
            ("provider.rate_limited", |t| matches!(t, TurnEnd::Error { .. }), None),
            ("", |t| matches!(t, TurnEnd::Error { .. }), None),
        ];
        for (kind, check, message) in cases {
            match stop_update(&failed(kind)) {
                Some(outcome) => {
                    assert!(check(&outcome), "kind `{kind}`: unexpected outcome {outcome:?}");
                    if let Some(expected) = message {
                        let TurnEnd::Error { message } = &outcome else {
                            panic!("kind `{kind}`: expected Error with message");
                        };
                        assert_eq!(message.as_deref(), Some(expected));
                    }
                }
                None => panic!("kind `{kind}`: stop_update returned None"),
            }
        }
    }

    /// Wave 3: `session.execution.interrupted` → stopReason cancelled.
    #[test]
    fn execution_interrupted_maps_to_cancelled() {
        let ev = dto::SessionEvent::ExecutionInterrupted(dto::SessionRef {
            sessionID: "ses_x".into(),
        });
        match stop_update(&ev) {
            Some(TurnEnd::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
        // not a stop signal for other events
        assert!(stop_update(&dto::SessionEvent::ExecutionStarted(dto::SessionRef {
            sessionID: "ses_x".into()
        }))
        .is_none());
    }

    /// Wave 3: permission events belong to a session (so the agent filter can
    /// route them) but map to no ACP session updates — the ask drives the
    /// permission REQUEST in the agent layer, the replied echo is ignored.
    #[test]
    fn permission_events_route_to_session_but_no_updates() {
        let mut state = MappingState::new();
        let asked = dto::SessionEvent::PermissionAsked(dto::PermissionAsked {
            id: "per_x".into(),
            sessionID: "ses_x".into(),
            action: "shell".into(),
            resources: vec!["echo hi".into()],
            save: None,
            metadata: None,
            source: None,
        });
        let replied = dto::SessionEvent::PermissionReplied(dto::PermissionReplied {
            sessionID: "ses_x".into(),
            requestID: "per_x".into(),
            reply: Some("once".into()),
        });
        for ev in [&asked, &replied] {
            assert_eq!(
                event_session_id(ev),
                Some("ses_x"),
                "permission events filter by session"
            );
            assert!(to_updates(ev, &mut state).is_empty(), "no ACP update counterpart");
        }
    }

    /// Wave 3: the tool-input cache feeds the permission prompt's `state.input`.
    #[test]
    fn tool_input_cache_covers_ended_and_called() {
        let mut state = MappingState::new();
        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: dto::ToolRef {
                sessionID: "ses_x".into(),
                assistantMessageID: "msg_x".into(),
                id: "call_x".into(),
            },
            text: r#"{"command": "echo hi"}"#.into(),
        });
        let _ = to_updates(&ended, &mut state);
        assert_eq!(
            state.tool_input("call_x"),
            Some(&serde_json::json!({ "command": "echo hi" })),
            "input.ended parses the raw JSON into the cache"
        );
        // tool.called overwrites with the authoritative parsed input.
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: dto::ToolRef {
                sessionID: "ses_x".into(),
                assistantMessageID: "msg_x".into(),
                id: "call_x".into(),
            },
            input: serde_json::json!({ "command": "echo hi", "confirmed": true }),
            executed: None,
        });
        let _ = to_updates(&called, &mut state);
        assert_eq!(
            state.tool_input("call_x"),
            Some(&serde_json::json!({ "command": "echo hi", "confirmed": true })),
            "tool.called is authoritative"
        );
        assert_eq!(state.tool_input("call_missing"), None);
        // Unparseable ended-input leaves the cache untouched.
        let junk = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: dto::ToolRef {
                sessionID: "ses_x".into(),
                assistantMessageID: "msg_x".into(),
                id: "call_junk".into(),
            },
            text: "{ not json".into(),
        });
        let _ = to_updates(&junk, &mut state);
        assert_eq!(state.tool_input("call_junk"), None);
    }

    #[test]
    fn unmapped_events_produce_no_updates() {
        let mut state = MappingState::new();
        for ev in [
            dto::SessionEvent::ExecutionStarted(dto::SessionRef { sessionID: "ses_x".into() }),
            dto::SessionEvent::StepStarted(dto::StepStarted {
                session: dto::SessionRef { sessionID: "ses_x".into() },
                agent: None,
                model: None,
                assistantMessageID: "msg_x".into(),
                snapshot: None,
                started: None,
            }),
            dto::SessionEvent::ToolProgress(dto::ToolRef {
                sessionID: "ses_x".into(),
                assistantMessageID: "msg_x".into(),
                id: "call_x".into(),
            }),
        ] {
            assert!(to_updates(&ev, &mut state).is_empty(), "{ev:?} must map to nothing");
        }
    }

    fn dto_session_title() -> String {
        // The fixture's session.renamed title.
        "Current directory listing with ls -la".to_string()
    }

    // ======================= Wave 4 =======================

    use agent_client_protocol::schema::v1 as acp;

    fn tool_ref(session: &str, msg: &str, id: &str) -> dto::ToolRef {
        dto::ToolRef {
            sessionID: session.into(),
            assistantMessageID: msg.into(),
            id: id.into(),
        }
    }

    #[test]
    fn tool_failed_closes_the_call_and_maps_failed_status() {
        let mut state = MappingState::new();
        // Open the call first (input.started), then fail it.
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_x", "msg_x", "call_x"),
            name: "bash".into(),
        });
        let _ = to_updates(&started, &mut state);
        let failed = dto::SessionEvent::ToolFailed(dto::ToolRefError {
            base: tool_ref("ses_x", "msg_x", "call_x"),
            error: dto::StructuredError {
                kind: Some("tool.execution".into()),
                message: Some("boom".into()),
            },
        });
        let updates = to_updates(&failed, &mut state);
        assert_eq!(updates.len(), 1);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("expected a tool_call update")
        };
        assert_eq!(u.tool_call_id.0.as_ref(), "call_x");
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Failed));
        // v1 has no error field on tool updates — the message rides raw_output.
        assert_eq!(u.fields.raw_output, Some(serde_json::Value::String("boom".into())));
        // The call is closed: the cancel drain must not re-abandon it.
        assert!(state.abandon_open_tools().is_empty());
    }

    #[test]
    fn abandon_open_tools_fails_only_still_open_calls() {
        let mut state = MappingState::new();
        for id in ["call_a", "call_b", "call_c"] {
            let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
                base: tool_ref("ses_x", "msg_x", id),
                name: "bash".into(),
            });
            let _ = to_updates(&started, &mut state);
        }
        // Close call_a normally, fail call_b, leave call_c open.
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_x", "msg_x", "call_a"),
            content: None,
            metadata: None,
            executed: None,
        });
        let _ = to_updates(&success, &mut state);
        let failed = dto::SessionEvent::ToolFailed(dto::ToolRefError {
            base: tool_ref("ses_x", "msg_x", "call_b"),
            error: dto::StructuredError {
                kind: Some("tool.execution".into()),
                message: None,
            },
        });
        let _ = to_updates(&failed, &mut state);

        let abandoned = state.abandon_open_tools();
        assert_eq!(abandoned.len(), 1, "only the still-open call is abandoned");
        let acp::SessionUpdate::ToolCallUpdate(u) = &abandoned[0] else {
            panic!("expected a tool_call update")
        };
        assert_eq!(u.tool_call_id.0.as_ref(), "call_c");
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Failed));
        assert_eq!(u.fields.title, Some("Cancelled".into()));
    }

    // ======================= Wave 5: aft hoist dialect =======================

    /// Decoded ToolSuccess events of an aft capture, in fixture order.
    fn aft_successes(path: &str) -> Vec<dto::ToolSuccess> {
        let raw = aft_fixture(path);
        let mut out = Vec::new();
        for line in raw.lines() {
            let line = line.trim();
            if !line.starts_with("data: ") || line == "data: " {
                continue;
            }
            let env: dto::EventEnvelope =
                serde_json::from_str(&line[6..]).expect("envelope parses");
            if let Some(dto::SessionEvent::ToolSuccess(t)) = dto::decode_event(&env.kind, &env.data)
            {
                out.push(t);
            }
        }
        out
    }

    fn aft_fixture(name: &str) -> &'static str {
        match name {
            "aft-tool-turn" => include_str!("../../tests/fixtures/aft-tool-turn.sse"),
            "aft-image-read" => include_str!("../../tests/fixtures/aft-image-read.sse"),
            other => panic!("unknown aft fixture {other}"),
        }
    }

    /// The image read's content maps to [Text, Image] blocks — the Data-URI
    /// split into the base64 payload + mime, original uri preserved. With
    /// `--no-aft` the image block is dropped (plain text behavior); the diff
    /// chain is untouched (not exercised here).
    #[test]
    fn aft_image_content_maps_to_image_block_and_no_aft_drops_it() {
        let successes = aft_successes("aft-image-read");
        assert_eq!(successes.len(), 1);

        let mut state = MappingState::new();
        let updates = to_updates(&dto::SessionEvent::ToolSuccess(successes[0].clone()), &mut state);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("expected a tool_call update")
        };
        let blocks = u.fields.content.as_ref().expect("content present");
        assert_eq!(blocks.len(), 2, "text + image");

        let acp::ToolCallContent::Content(c) = &blocks[1] else {
            panic!("image must map to a Content block, got {:?}", blocks[1]);
        };
        let acp::ContentBlock::Image(img) = &c.content else {
            panic!("expected an image block, got {:?}", c.content);
        };
        assert_eq!(img.mime_type, "image/png");
        assert!(img.data.starts_with("iVBORw0KGgo"), "base64 payload after the data-URI prefix");
        assert!(
            !img.data.contains("data:"),
            "the data-URI prefix must be stripped from the payload"
        );
        assert_eq!(
            img.uri.as_deref(),
            Some(
                "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
            )
        );

        // --no-aft: image passthrough off, text stays.
        let mut no_aft = MappingState::new().with_no_aft(true);
        let updates = to_updates(&dto::SessionEvent::ToolSuccess(successes[0].clone()), &mut no_aft);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("expected a tool_call update")
        };
        let blocks = u.fields.content.as_ref().expect("content present");
        assert_eq!(blocks.len(), 1, "image dropped under --no-aft");
        assert!(matches!(&blocks[0], acp::ToolCallContent::Content(c) if matches!(&c.content, acp::ContentBlock::Text(_))));
    }

    /// aft read/edit/apply_patch turn through the live mapping: the edit's
    /// filediff (primary) and the apply_patch's `diff` string (fallback) both
    /// produce Diff blocks — the aft dialect does not change the diff chain.
    #[test]
    fn aft_diff_primary_and_fallback_blocks() {
        let successes = aft_successes("aft-tool-turn");
        assert_eq!(successes.len(), 3);

        // Success #0 (read) carries no file changes; #1 (edit) uses the
        // primary filediff; #2 (apply_patch) falls back to the `diff` string.
        let mut state = MappingState::new();
        let n_diffs: Vec<usize> = successes
            .iter()
            .map(|t| {
                let updates =
                    to_updates(&dto::SessionEvent::ToolSuccess(t.clone()), &mut state);
                let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
                    panic!("expected a tool_call update")
                };
                u.fields
                    .content
                    .as_ref()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter(|b| matches!(b, acp::ToolCallContent::Diff(_)))
                            .count()
                    })
                    .unwrap_or(0)
            })
            .collect();
        assert_eq!(n_diffs[0], 0, "the read carries no file changes");
        assert_eq!(n_diffs[1], 1, "the edit's filediff maps to one Diff block");
        assert_eq!(n_diffs[2], 1, "the apply_patch's diff-string fallback maps too");
    }

    #[test]
    fn retry_scheduled_pushes_meta_and_step_started_clears_it() {
        let mut state = MappingState::new();
        let retry = dto::SessionEvent::RetryScheduled(dto::RetryScheduled {
            sessionID: "ses_x".into(),
            attempt: Some(2),
            nextRetryAt: Some(serde_json::json!({"ms": 1790930000000u64})),
            error: Some(dto::StructuredError {
                kind: Some("provider.internal".into()),
                message: Some("retrying".into()),
            }),
        });
        let updates = to_updates(&retry, &mut state);
        assert_eq!(updates.len(), 1, "retry.scheduled → one session_info_update");
        let acp::SessionUpdate::SessionInfoUpdate(info) = &updates[0] else {
            panic!("expected session_info_update")
        };
        let meta = info.meta.as_ref().expect("_meta present");
        let obj = meta.get("opencode/retry").and_then(|v| v.as_object()).expect("retry object");
        assert_eq!(obj.get("attempt"), Some(&serde_json::json!(2)));
        assert_eq!(obj.get("nextRetryAt"), Some(&serde_json::json!({"ms": 1790930000000u64})));
        assert_eq!(
            obj.get("error").and_then(|e| e.get("type")),
            Some(&serde_json::json!("provider.internal"))
        );
        assert_eq!(state.retry_meta(), Some(&serde_json::json!(obj)));

        // step.started clears the pending retry (official: meta → null).
        let started = dto::SessionEvent::StepStarted(dto::StepStarted {
            session: dto::SessionRef { sessionID: "ses_x".into() },
            agent: None,
            model: None,
            assistantMessageID: "msg_x".into(),
            snapshot: None,
            started: None,
        });
        let updates = to_updates(&started, &mut state);
        assert_eq!(updates.len(), 1, "clear pushes one update");
        let acp::SessionUpdate::SessionInfoUpdate(info) = &updates[0] else {
            panic!("expected session_info_update")
        };
        assert_eq!(
            info.meta.as_ref().and_then(|m| m.get("opencode/retry")),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(state.retry_meta(), None, "pending retry cleared");
        // A second step.started pushes nothing (nothing pending).
        assert!(to_updates(&started, &mut state).is_empty());
    }

    #[test]
    fn compaction_events_push_compaction_meta() {
        let mut state = MappingState::new();
        let started = dto::SessionEvent::CompactionStarted(dto::CompactionStarted {
            sessionID: "ses_x".into(),
            reason: Some("manual".into()),
            recent: Some("".into()),
            inputID: Some("msg_inbox_1".into()),
        });
        let updates = to_updates(&started, &mut state);
        let acp::SessionUpdate::SessionInfoUpdate(info) = &updates[0] else {
            panic!("expected session_info_update")
        };
        let obj = info
            .meta
            .as_ref()
            .and_then(|m| m.get("opencode/compaction"))
            .and_then(|v| v.as_object())
            .expect("compaction object");
        assert_eq!(obj.get("status"), Some(&serde_json::json!("started")));
        assert_eq!(obj.get("messageId"), Some(&serde_json::json!("msg_inbox_1")));

        let failed = dto::SessionEvent::CompactionFailed(dto::CompactionFailed {
            sessionID: "ses_x".into(),
            reason: Some("manual".into()),
            inputID: Some("msg_inbox_1".into()),
            error: Some(dto::StructuredError {
                kind: Some("compaction.unavailable".into()),
                message: Some("Nothing to compact yet".into()),
            }),
        });
        let updates = to_updates(&failed, &mut state);
        let acp::SessionUpdate::SessionInfoUpdate(info) = &updates[0] else {
            panic!("expected session_info_update")
        };
        let obj = info
            .meta
            .as_ref()
            .and_then(|m| m.get("opencode/compaction"))
            .and_then(|v| v.as_object())
            .expect("compaction object");
        assert_eq!(obj.get("status"), Some(&serde_json::json!("failed")));
        assert_eq!(
            obj.get("error").and_then(|e| e.get("message")),
            Some(&serde_json::json!("Nothing to compact yet"))
        );
        // The tolerant ended event also maps to a meta push.
        let ended = dto::SessionEvent::CompactionEnded(dto::CompactionEnded {
            sessionID: "ses_x".into(),
            reason: None,
            inputID: None,
        });
        let updates = to_updates(&ended, &mut state);
        let acp::SessionUpdate::SessionInfoUpdate(info) = &updates[0] else {
            panic!("expected session_info_update")
        };
        assert_eq!(
            info.meta
                .as_ref()
                .and_then(|m| m.get("opencode/compaction"))
                .and_then(|v| v.get("status")),
            Some(&serde_json::json!("ended"))
        );
    }

    #[test]
    fn child_projection_prefixes_ids_and_titles() {
        let mut state = MappingState::new();
        let ns = ToolNs {
            child_id: "ses_child_1".into(),
            child_title: "Explore the repo".into(),
        };
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_child_1", "msg_c1", "call_c1"),
            name: "grep".into(),
        });
        let updates = to_child_updates(&started, &ns, &mut state);
        assert_eq!(updates.len(), 1);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("expected a tool_call update")
        };
        assert_eq!(u.tool_call_id.0.as_ref(), "ses_child_1:call_c1");
        assert_eq!(u.fields.title, Some("Explore the repo: grep".into()));
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Pending));

        // Success closes the NAMESPACED id and prefixes the meta title.
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_child_1", "msg_c1", "call_c1"),
            content: None,
            metadata: Some(dto::ToolMetadata {
                title: Some("grep *.rs".into()),
                diff: None,
                filediff: None,
                files: None,
                truncated: None,
                diagnostics: None,
            }),
            executed: None,
        });
        let updates = to_child_updates(&success, &ns, &mut state);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("expected a tool_call update")
        };
        assert_eq!(u.tool_call_id.0.as_ref(), "ses_child_1:call_c1");
        assert_eq!(u.fields.title, Some("Explore the repo: grep *.rs".into()));
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Completed));
        assert!(state.abandon_open_tools().is_empty(), "namespaced call closed");

        // Non-tool child events do not project.
        let exec = dto::SessionEvent::ExecutionStarted(dto::SessionRef {
            sessionID: "ses_child_1".into(),
        });
        assert!(to_child_updates(&exec, &ns, &mut state).is_empty());
        let text = dto::SessionEvent::TextDelta(dto::TextDelta {
            base: dto::OrdinalRef {
                sessionID: "ses_child_1".into(),
                assistantMessageID: "msg_c1".into(),
                ordinal: Some(0),
            },
            delta: "child thinking".into(),
        });
        assert!(to_child_updates(&text, &ns, &mut state).is_empty());
        // …and the permission input cache is keyed by the namespaced id.
        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: tool_ref("ses_child_1", "msg_c1", "call_c2"),
            text: r#"{"command": "ls"}"#.into(),
        });
        let _ = to_child_updates(&ended, &ns, &mut state);
        assert_eq!(
            state.tool_input("ses_child_1:call_c2"),
            Some(&serde_json::json!({"command": "ls"}))
        );
        assert_eq!(state.tool_input("call_c2"), None);
    }

    #[test]
    fn step_failed_maps_through_the_failure_taxonomy() {
        let event = dto::SessionEvent::StepFailed(dto::StepFailed {
            session: dto::SessionRef { sessionID: "ses_x".into() },
            assistantMessageID: "msg_x".into(),
            error: dto::StructuredError {
                kind: Some("provider.auth".into()),
                message: Some("auth expired".into()),
            },
            finish: None,
        });
        // No ACP update for step failures (v1 has no per-step surface)…
        let mut state = MappingState::new();
        assert!(to_updates(&event, &mut state).is_empty());
        assert_eq!(event_session_id(&event), Some("ses_x"));
        // …but the taxonomy is exposed for the agent layer to log.
        let outcome = step_failed_outcome(&event).expect("step outcome");
        assert!(matches!(outcome, TurnEnd::AuthRequired { .. }), "{outcome:?}");
        assert!(step_failed_outcome(&dto::SessionEvent::ExecutionSucceeded(
            dto::SessionRef { sessionID: "ses_x".into() }
        ))
        .is_none());
    }
}
