//! `session/load` history replay — the ACP hard contract: replays all
//! persisted session updates BEFORE the `session/load` response, so clients
//! reconstruct the transcript (Zed restores the same `messageId`s).
//!
//! Records arrive newest-first (2.0.21 `GET …/message`); this module reverses
//! to chronological order. `kind=idle|model-switched` records carry no user
//! content and are skipped.

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, SessionUpdate, TextContent, ToolCall, ToolCallContent,
    ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
};

use crate::dto::{MessageRecord, Part, ToolState};

use super::updates::tool_result_blocks;

/// Build the full ACP transcript for a session's persisted messages.
pub fn replay_updates(records: &[MessageRecord]) -> Vec<SessionUpdate> {
    let mut out = Vec::new();
    for record in records.iter().rev() {
        match record.kind.as_str() {
            "user" => {
                let Some(text) = &record.text else { continue };
                out.push(SessionUpdate::UserMessageChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                        .message_id(record.id.as_str()),
                ));
            }
            "assistant" => {
                for part in record.content.iter().flatten() {
                    out.extend(assistant_part(part, &record.id));
                }
            }
            // Execution bookkeeping records — nothing to show the client.
            "idle" | "model-switched" => continue,
            other => {
                tracing::debug!(kind = other, "skipping unhandled message record kind");
            }
        }
    }
    out
}

fn assistant_part(part: &Part, message_id: &str) -> Vec<SessionUpdate> {
    match part {
        Part::Text { text, .. } => vec![SessionUpdate::AgentMessageChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                .message_id(message_id),
        )],
        Part::Reasoning { text, .. } => vec![SessionUpdate::AgentThoughtChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                .message_id(message_id),
        )],
        Part::Tool { id, name, state, .. } => tool_part(id, name, state),
        Part::Unknown => vec![],
    }
}

/// A persisted tool part becomes an initial `pending` `ToolCall` (ACP requires
/// the call to exist before it can be updated) followed by the terminal update
/// matching the persisted state.
fn tool_part(id: &str, name: &str, state: &ToolState) -> Vec<SessionUpdate> {
    let mut out = Vec::new();

    // The opening `ToolCall` — mirrors `session.tool.input.started` in the
    // live path (title = tool name).
    let title_for_call = match state {
        ToolState::Completed { metadata, .. } => {
            metadata.as_ref().and_then(|m| m.title.clone()).unwrap_or_else(|| name.to_string())
        }
        _ => name.to_string(),
    };
    let mut call = ToolCall::new(id.to_string(), title_for_call);
    let raw_input = match state {
        ToolState::Streaming { input } => Some(serde_json::Value::String(input.clone())),
        other => other.input().cloned(),
    };
    if let Some(input) = raw_input {
        call = call.raw_input(input);
    }
    out.push(SessionUpdate::ToolCall(call));

    match state {
        // Input streamed but the turn ended before the call actually ran —
        // the pending `ToolCall` above is the whole story.
        ToolState::Streaming { .. } => {}

        ToolState::Running { .. } => out.push(tool_update(
            id,
            ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
        )),

        ToolState::Completed { content, metadata, .. } => {
            let mut blocks: Vec<ToolCallContent> = Vec::new();
            if let Some(content) = content {
                blocks = tool_result_blocks(content, metadata);
            } else if let Some(meta) = metadata {
                blocks = tool_result_blocks(&[], &Some(meta.clone()));
            }
            let mut fields = ToolCallUpdateFields::new().status(ToolCallStatus::Completed);
            if !blocks.is_empty() {
                fields = fields.content(Some(blocks));
            }
            out.push(tool_update(id, fields));
        }

        ToolState::Error { error, content, metadata, .. } => {
            // ACP's `failed` state carries display content: the tool's own
            // output (if any) plus the error text.
            let mut blocks: Vec<ToolCallContent> = Vec::new();
            if let Some(content) = content {
                blocks = tool_result_blocks(content, metadata);
            }
            if let Some(msg) = &error.message {
                blocks.push(ToolCallContent::from(ContentBlock::Text(TextContent::new(
                    msg.clone(),
                ))));
            }
            let fields = ToolCallUpdateFields::new()
                .status(ToolCallStatus::Failed)
                .content(Some(blocks));
            out.push(tool_update(id, fields));
        }
    }

    out
}

fn tool_update(tool_call_id: &str, fields: ToolCallUpdateFields) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id.to_string(), fields))
}

/// Small helper trait to read the input value of a tool state (all variants).
trait HasInput {
    fn input(&self) -> Option<&serde_json::Value>;
}

impl HasInput for ToolState {
    fn input(&self) -> Option<&serde_json::Value> {
        match self {
            ToolState::Streaming { .. } => None,
            ToolState::Running { input, .. }
            | ToolState::Completed { input, .. }
            | ToolState::Error { input, .. } => Some(input),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{MessagesEnvelope, StructuredError};

    fn fixture_records() -> Vec<MessageRecord> {
        let raw = include_str!("../../tests/fixtures/messages-tool-turn.json");
        let env: MessagesEnvelope = serde_json::from_str(raw).expect("messages envelope");
        env.data
    }

    fn text_of(u: &SessionUpdate) -> Option<&str> {
        let chunk = match u {
            SessionUpdate::UserMessageChunk(c) | SessionUpdate::AgentMessageChunk(c) => c,
            _ => return None,
        };
        match &chunk.content {
            ContentBlock::Text(t) => Some(t.text.as_str()),
            _ => None,
        }
    }

    #[test]
    fn replay_orders_history_and_maps_transcript() {
        let updates = replay_updates(&fixture_records());

        // 1. Chronological order: the OLDEST user message comes first.
        let first = &updates[0];
        let SessionUpdate::UserMessageChunk(_c) = first else {
            panic!("replay must start with the oldest user message, got {first:?}");
        };
        assert!(text_of(first).unwrap().contains("ls -la"));

        // 2. Three user messages survive (the two older ones + the write-tool
        //    one); idle/model-switched records are skipped entirely.
        let user_count = updates
            .iter()
            .filter(|u| matches!(u, SessionUpdate::UserMessageChunk(_)))
            .count();
        assert_eq!(user_count, 3, "idle/model-switched must be skipped");

        // 3. The write-tool turn: reasoning → pending tool call → completed
        //    update with text + diff.
        let tool_idx = updates
            .iter()
            .position(|u| matches!(u, SessionUpdate::ToolCall(t) if t.tool_call_id.0.as_ref() == "call_9663d4974690464f98d40e7c"))
            .expect("tool call present");
        let SessionUpdate::ToolCall(call) = &updates[tool_idx] else { unreachable!() };
        assert_eq!(call.title, "hello-acp-test.txt", "completed metadata title wins");
        // The assistant message that owns the tool call streams out first.
        let thought_before = &updates[tool_idx - 1];
        assert!(matches!(thought_before, SessionUpdate::AgentThoughtChunk(_)));

        let terminal = &updates[tool_idx + 1];
        let SessionUpdate::ToolCallUpdate(update) = terminal else {
            panic!("expected ToolCallUpdate after ToolCall, got {terminal:?}");
        };
        assert_eq!(update.fields.status, Some(ToolCallStatus::Completed));
        let blocks = update.fields.content.as_ref().expect("completed tool has content");
        assert_eq!(blocks.len(), 2);
        let ToolCallContent::Diff(d) = &blocks[1] else { panic!("diff block expected") };
        assert_eq!(d.path.to_string_lossy(), "/tmp/opencode/hello-acp-test.txt");
        assert_eq!(d.old_text, None);
        assert_eq!(d.new_text, "bridge test line");

        // 4. Final assistant text.
        let last = updates.last().unwrap();
        let SessionUpdate::AgentMessageChunk(c) = last else { panic!("last update is text") };
        assert_eq!(text_of(last).unwrap(), "done");
        // The two assistant messages keep their persisted ids.
        assert_eq!(c.message_id.as_ref().map(|m| &*m.0), Some("msg_0fac22cb7001wso3ZZF1AWkVXt"));
    }

    #[test]
    fn streaming_and_running_states_map_without_terminal_update() {
        // Streaming: raw input stays a raw JSON string (matches ToolInputEnded
        // semantics in the live path).
        let streaming = replay_updates(&[record_with_tool(ToolState::Streaming {
            input: r#"{"x":1}"#.into(),
        })]);
        assert_eq!(streaming.len(), 1);
        let SessionUpdate::ToolCall(call) = &streaming[0] else { panic!() };
        assert_eq!(call.raw_input, Some(serde_json::Value::String(r#"{"x":1}"#.into())));

        // Running: pending call + in-progress update.
        let running = replay_updates(&[record_with_tool(ToolState::Running {
            input: serde_json::json!({"x": 1}),
            metadata: None,
        })]);
        assert_eq!(running.len(), 2);
        let SessionUpdate::ToolCallUpdate(u) = &running[1] else { panic!() };
        assert_eq!(u.fields.status, Some(ToolCallStatus::InProgress));
    }

    #[test]
    fn error_state_maps_to_failed_with_error_text() {
        let records = [record_with_tool(ToolState::Error {
            input: serde_json::json!({"x": 1}),
            error: StructuredError {
                kind: Some("tool.execution".into()),
                message: Some("failed to write file".into()),
            },
            content: None,
            metadata: None,
        })];
        let updates = replay_updates(&records);
        assert_eq!(updates.len(), 2);
        let SessionUpdate::ToolCallUpdate(u) = &updates[1] else { panic!() };
        assert_eq!(u.fields.status, Some(ToolCallStatus::Failed));
        let blocks = u.fields.content.as_ref().expect("failed update carries error text");
        let Some(ToolCallContent::Content(c)) = blocks.first() else {
            panic!("error text block expected");
        };
        let ContentBlock::Text(t) = &c.content else { panic!("text block expected") };
        assert_eq!(t.text, "failed to write file");
    }

    #[test]
    fn reasoning_and_text_parts_share_the_assistant_message_id() {
        let updates = replay_updates(&fixture_records());
        // The write-tool assistant message carries a reasoning part (and no
        // text part in this capture) — the thought chunk must keep the
        // message id so clients can anchor it under the right assistant turn.
        let msg = "msg_0fac20b1a001EggNXWtAEmLWrL";
        let chunks_with_id = updates
            .iter()
            .filter(|u| match u {
                SessionUpdate::AgentMessageChunk(c) | SessionUpdate::AgentThoughtChunk(c) => {
                    c.message_id.as_ref().map(|m| &*m.0) == Some(msg)
                }
                _ => false,
            })
            .count();
        assert!(chunks_with_id >= 1, "reasoning part keeps the assistant message id");
    }

    fn record_with_tool(state: ToolState) -> MessageRecord {
        MessageRecord {
            kind: "assistant".into(),
            id: "msg_synthetic".into(),
            text: None,
            agent: Some("build".into()),
            model: None,
            content: Some(vec![Part::Tool {
                id: "call_synthetic".into(),
                name: "bash".into(),
                executed: Some(true),
                state,
                time: None,
            }]),
            finish: Some("end_turn".into()),
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        }
    }
}