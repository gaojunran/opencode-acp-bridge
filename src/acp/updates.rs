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

use std::collections::HashMap;

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, Cost, SessionInfoUpdate, SessionUpdate, TextContent,
    ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
};

use crate::dto::{self, ToolContent, ToolMetadata};

use super::diff;


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
}

impl MappingState {
    pub fn new() -> Self {
        Self::default()
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

        // ---------- tools ----------
        dto::SessionEvent::ToolInputStarted(t) => {
            // ACP convention (mirrors the TS bridge): while input streams, the
            // call is advertised as `pending` with the tool name as title.
            state.tool_titles.insert(t.base.id.clone(), t.name.clone());
            vec![tool_update(&t.base.id, ToolCallUpdateFields::new()
                .status(ToolCallStatus::Pending)
                .title(t.name.clone()))]
        }
        dto::SessionEvent::ToolInputEnded(t) => {
            // `text` is the raw JSON input string — pass it through verbatim
            // (the pending call's streaming input, as a JSON string value).
            vec![tool_update(&t.base.id, ToolCallUpdateFields::new()
                .raw_input(serde_json::Value::String(t.text.clone())))]
        }
        dto::SessionEvent::ToolCalled(t) => {
            // Parsed input now available → mark in progress.
            vec![tool_update(&t.base.id, ToolCallUpdateFields::new()
                .status(ToolCallStatus::InProgress)
                .raw_input(t.input.clone()))]
        }
        dto::SessionEvent::ToolProgress(_) => {
            // No-op: the call is already `in_progress` since `tool.called`.
            // Kept as its own arm so the mapping is explicit and greppable.
            vec![]
        }
        dto::SessionEvent::ToolSuccess(t) => {
            let mut fields = ToolCallUpdateFields::new().status(ToolCallStatus::Completed);
            if let Some(meta) = &t.metadata {
                if let Some(title) = &meta.title {
                    fields = fields.title(title.clone());
                }
            }
            if let Some(content) = &t.content {
                let mut blocks: Vec<ToolCallContent> = Vec::new();
                for part in content {
                    match part {
                        ToolContent::Text { text } => blocks.push(ToolCallContent::from(
                            ContentBlock::Text(TextContent::new(text.clone())),
                        )),
                        ToolContent::Unknown => {}
                    }
                }
                if let Some(meta) = &t.metadata {
                    blocks.extend(diff::diff_blocks(meta));
                }
                fields = fields.content(Some(blocks));
            } else if let Some(meta) = &t.metadata {
                // Content-less success (e.g. progress-only tools) may still
                // carry diffs — never drop the file changes.
                let blocks = diff::diff_blocks(meta);
                if !blocks.is_empty() {
                    fields = fields.content(Some(blocks));
                }
            }
            vec![tool_update(&t.base.id, fields)]
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

/// Outcome of a finished execution — the ACP stop signal.
///
/// ACP v1 has NO `stop` session-update variant: the end of a turn is delivered
/// as the `session/prompt` RESPONSE (`PromptResponse{stop_reason}`). This
/// helper therefore decides *what* to deliver for a turn-completion event.
#[derive(Debug)]
pub enum TurnEnd {
    /// `session.execution.succeeded` → respond `StopReason::EndTurn`.
    EndTurn,
    /// `session.execution.failed` → respond with a JSON-RPC error carrying the
    /// opencode error message (there is no `Error` stop reason in v1).
    Error { message: Option<String> },
}

/// Map a turn-completion event to the agent-layer stop outcome.
pub fn stop_update(event: &dto::SessionEvent) -> Option<TurnEnd> {
    match event {
        dto::SessionEvent::ExecutionSucceeded(_) => Some(TurnEnd::EndTurn),
        dto::SessionEvent::ExecutionFailed(f) => Some(TurnEnd::Error {
            message: f.error.message.clone(),
        }),
        _ => None,
    }
}

/// The sessionID an event belongs to (for filtering the shared SSE stream),
/// or `None` for events without one (`SessionCreated`).
pub fn event_session_id(event: &dto::SessionEvent) -> Option<&str> {
    match event {
        dto::SessionEvent::ExecutionStarted(r) => Some(&r.sessionID),
        dto::SessionEvent::ExecutionSucceeded(r) => Some(&r.sessionID),
        dto::SessionEvent::ExecutionFailed(f) => Some(&f.session.sessionID),
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
        dto::SessionEvent::UsageUpdated(u) => Some(&u.session.sessionID),
        dto::SessionEvent::Renamed(r) => Some(&r.session.sessionID),
        dto::SessionEvent::SessionCreated(_) => None,
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
pub fn tool_result_blocks(
    content: &[ToolContent],
    metadata: &Option<ToolMetadata>,
) -> Vec<ToolCallContent> {
    let mut blocks: Vec<ToolCallContent> = Vec::new();
    for part in content {
        match part {
            ToolContent::Text { text } => blocks
                .push(ToolCallContent::from(ContentBlock::Text(TextContent::new(text.clone())))),
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
}
