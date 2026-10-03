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

/// Release 0.6.0: a child (subagent) session matched to ONE replayed
/// spawner tool call of the parent — the replay attaches
/// `_meta.subagent_session_info` to the replayed declaration + terminal
/// update so Zed's view-creation scan discovers and loads the child.
/// `entries` is the child's total transcript entry count (its own
/// history: user messages + assistant messages + tool parts) — the
/// closed-slice end index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayChildMeta {
    /// The spawner tool call id on the parent (the replayed `Part::Tool` id).
    pub call_id: String,
    /// The child session id.
    pub session_id: String,
    /// Total transcript entries of the child (`message_end_index`).
    pub entries: usize,
}

/// Build the full ACP transcript for a session's persisted messages.
/// `no_aft` — see [`MappingState::with_no_aft`]: replays drop File/image
/// passthrough (diffs stay — dialect-neutral). `children` — the child
/// metas for the parent's spawner calls (see [`ReplayChildMeta`]); the
/// parent's own replay keeps PARENT content only — the child thread is
/// NOT inlined (Zed loads the children itself, keyed by the meta).
pub fn replay_updates(
    records: &[MessageRecord],
    no_aft: bool,
    children: &[ReplayChildMeta],
) -> Vec<SessionUpdate> {
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
                    out.extend(assistant_part(part, &record.id, no_aft, children));
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

fn assistant_part(
    part: &Part,
    message_id: &str,
    no_aft: bool,
    children: &[ReplayChildMeta],
) -> Vec<SessionUpdate> {
    match part {
        Part::Text { text, .. } => vec![SessionUpdate::AgentMessageChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                .message_id(message_id),
        )],
        Part::Reasoning { text, .. } => vec![SessionUpdate::AgentThoughtChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                .message_id(message_id),
        )],
        Part::Tool { id, name, state, .. } => {
            let child = children.iter().find(|c| c.call_id == *id);
            tool_part(id, name, state, no_aft, child)
        }
        Part::Unknown => vec![],
    }
}

/// A persisted tool part becomes an initial `pending` `ToolCall` (ACP requires
/// the call to exist before it can be updated) followed by the terminal update
/// matching the persisted state. `child` — the subagent meta for a spawner
/// call (Release 0.6.0): both the declaration and the terminal update carry
/// `_meta.subagent_session_info` so Zed's view-creation scan discovers the
/// child and shows the completed slice.
fn tool_part(
    id: &str,
    name: &str,
    state: &ToolState,
    no_aft: bool,
    child: Option<&ReplayChildMeta>,
) -> Vec<SessionUpdate> {
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
    if let Some(child) = child {
        call = call.meta(subagent_meta_of(child));
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
                blocks = tool_result_blocks(content, metadata, no_aft);
            } else if let Some(meta) = metadata {
                blocks = tool_result_blocks(&[], &Some(meta.clone()), no_aft);
            }
            let mut fields = ToolCallUpdateFields::new().status(ToolCallStatus::Completed);
            if !blocks.is_empty() {
                fields = fields.content(Some(blocks));
            }
            out.push(tool_update_meta(id, fields, child));
        }

        ToolState::Error { error, content, metadata, .. } => {
            // ACP's `failed` state carries display content: the tool's own
            // output (if any) plus the error text.
            let mut blocks: Vec<ToolCallContent> = Vec::new();
            if let Some(content) = content {
                blocks = tool_result_blocks(content, metadata, no_aft);
            }
            if let Some(msg) = &error.message {
                blocks.push(ToolCallContent::from(ContentBlock::Text(TextContent::new(
                    msg.clone(),
                ))));
            }
            let fields = ToolCallUpdateFields::new()
                .status(ToolCallStatus::Failed)
                .content(Some(blocks));
            out.push(tool_update_meta(id, fields, child));
        }
    }

    out
}

/// The `_meta.subagent_session_info` map for a replayed child: the open
/// slice is closed — start 0, end = the child's total entry count (Zed caps
/// the embedded transcript display to the trailing 8 anyway).
fn subagent_meta_of(child: &ReplayChildMeta) -> serde_json::Map<String, serde_json::Value> {
    let mut info = serde_json::Map::new();
    info.insert("session_id".into(), serde_json::Value::from(child.session_id.as_str()));
    info.insert("message_start_index".into(), serde_json::Value::from(0));
    info.insert(
        "message_end_index".into(),
        serde_json::Value::from(child.entries),
    );
    let mut meta = serde_json::Map::new();
    meta.insert("subagent_session_info".into(), serde_json::Value::Object(info));
    meta
}

fn tool_update(tool_call_id: &str, fields: ToolCallUpdateFields) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id.to_string(), fields))
}

/// `tool_update` with the replayed subagent meta attached (if any).
fn tool_update_meta(
    tool_call_id: &str,
    fields: ToolCallUpdateFields,
    child: Option<&ReplayChildMeta>,
) -> SessionUpdate {
    match child {
        Some(child) => SessionUpdate::ToolCallUpdate(
            ToolCallUpdate::new(tool_call_id.to_string(), fields)
                .meta(subagent_meta_of(child)),
        ),
        None => tool_update(tool_call_id, fields),
    }
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
        let updates = replay_updates(&fixture_records(), false, &[]);

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
        })], false, &[]);
        assert_eq!(streaming.len(), 1);
        let SessionUpdate::ToolCall(call) = &streaming[0] else { panic!() };
        assert_eq!(call.raw_input, Some(serde_json::Value::String(r#"{"x":1}"#.into())));

        // Running: pending call + in-progress update.
        let running = replay_updates(&[record_with_tool(ToolState::Running {
            input: serde_json::json!({"x": 1}),
            metadata: None,
        })], false, &[]);
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
        let updates = replay_updates(&records, false, &[]);
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
        let updates = replay_updates(&fixture_records(), false, &[]);
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

    fn spawner_record(state: ToolState) -> MessageRecord {
        let mut r = record_with_tool(state);
        if let Some(parts) = &mut r.content {
            if let Some(Part::Tool { name, id, .. }) = parts.first_mut() {
                *name = "subagent".into();
                *id = "call_task_1".into();
            }
        }
        r
    }

    // ==================== Release 0.6.0: replay child metas ====================

    /// A matched child attaches `_meta.subagent_session_info` to BOTH the
    /// replayed spawner declaration and its terminal update — the closed
    /// slice {session_id, 0, entries}.
    #[test]
    fn spawner_replay_carries_subagent_meta() {
        let records = [spawner_record(ToolState::Completed {
            input: serde_json::json!({ "agent": "explorer" }),
            content: Some(vec![crate::dto::ToolContent::Text {
                text: "found".into(),
            }]),
            metadata: None,
        })];
        let children = [ReplayChildMeta {
            call_id: "call_task_1".into(),
            session_id: "ses_child_1".into(),
            entries: 5,
        }];
        let updates = replay_updates(&records, false, &children);
        assert_eq!(updates.len(), 2, "declaration + terminal");
        // Declaration carries the meta (the view-creation scan reads it HERE
        // to discover + load the child).
        let SessionUpdate::ToolCall(call) = &updates[0] else {
            panic!("declaration expected")
        };
        let info = call.meta.as_ref().unwrap()["subagent_session_info"]
            .as_object()
            .unwrap();
        assert_eq!(info["session_id"], serde_json::json!("ses_child_1"));
        assert_eq!(info["message_start_index"], serde_json::json!(0));
        assert_eq!(info["message_end_index"], serde_json::json!(5));
        // Terminal update carries the same closed slice + the final output.
        let SessionUpdate::ToolCallUpdate(update) = &updates[1] else {
            panic!("terminal update expected")
        };
        assert_eq!(update.fields.status, Some(ToolCallStatus::Completed));
        let info = update.meta.as_ref().unwrap()["subagent_session_info"]
            .as_object()
            .unwrap();
        assert_eq!(info["session_id"], serde_json::json!("ses_child_1"));
        assert_eq!(info["message_end_index"], serde_json::json!(5));
    }

    /// Unmatched calls carry no meta; an EMPTY child list replays the parent
    /// content only (the pre-0.6.0 shape).
    #[test]
    fn unmatching_child_pairs_leave_the_replay_plain() {
        let records = [spawner_record(ToolState::Completed {
            input: serde_json::json!({ "agent": "explorer" }),
            content: None,
            metadata: None,
        })];
        // Different call id → no meta.
        let wrong = [ReplayChildMeta {
            call_id: "call_other".into(),
            session_id: "ses_child_1".into(),
            entries: 5,
        }];
        let updates = replay_updates(&records, false, &wrong);
        let SessionUpdate::ToolCall(call) = &updates[0] else {
            panic!("declaration expected")
        };
        assert!(call.meta.is_none(), "unmatched call stays plain");
        // Empty children → plain too.
        let updates = replay_updates(&records, false, &[]);
        let SessionUpdate::ToolCall(call) = &updates[0] else {
            panic!("declaration expected")
        };
        assert!(call.meta.is_none());
    }
}
