//! `session/load` history replay — the ACP hard contract: replays all
//! persisted session updates BEFORE the `session/load` response, so clients
//! reconstruct the transcript (Zed restores the same `messageId`s).
//!
//! Records arrive newest-first (2.0.21 `GET …/message`); this module reverses
//! to chronological order. `kind=idle|model-switched` records carry no user
//! content and are skipped.

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, ResourceLink, SessionUpdate, TextContent, ToolCall,
    ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
};

use crate::dto::{AttachmentFile, MessageRecord, Part, ToolState};

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
    show_synthetic: bool,
) -> Vec<SessionUpdate> {
    let mut out = Vec::new();
    for record in records.iter().rev() {
        match record.kind.as_str() {
            "user" => {
                out.extend(user_message_chunks(record));
            }
            // Release 0.8.1 (`--show-synthetic`): system-injected messages
            // replay as user chunks ONLY with the flag on — off (default)
            // they are skipped, matching the live listener. The wire text
            // is the notification body (e.g. the `<shell …>` completion
            // block); the `description` lane (background command) is not
            // decoded — the body renders as-is, same text source as live.
            "synthetic" => {
                if show_synthetic {
                    out.extend(user_message_chunks(record));
                }
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

/// One user record (`kind: "user"` or `"synthetic"`) → its user chunks:
/// a Text chunk for `text` plus one ResourceLink chunk per usable
/// attachment, all on the record's message id (Zed merges adjacent chunks
/// by messageId into one user message). Text first, files after —
/// deterministic order. Empty records (no text, no files) yield nothing.
fn user_message_chunks(record: &MessageRecord) -> Vec<SessionUpdate> {
    let mut out = Vec::new();
    if let Some(text) = &record.text {
        out.push(SessionUpdate::UserMessageChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new(text.clone())))
                .message_id(record.id.as_str()),
        ));
    }
    // Release 0.7.2: restore the @-attachment chips — one ResourceLink
    // chunk per file, SAME message id.
    for file in record.files.iter().flatten() {
        if let Some(update) = user_file_link(file, &record.id) {
            out.push(update);
        }
    }
    out
}

/// One user-message attachment → an ACP `ResourceLink` user chunk (link
/// semantics — the bridge never decodes the base64 `data`). `None` when the
/// file carries no usable uri (missing source / empty uri): a link without
/// a target is worse than no link.
pub(crate) fn user_file_link(
    file: &AttachmentFile,
    message_id: &str,
) -> Option<SessionUpdate> {
    let uri = file.source.as_ref().and_then(|s| s.uri.as_deref())?;
    if uri.is_empty() {
        return None;
    }
    let mut link = ResourceLink::new(file.name.clone(), uri.to_string());
    if let Some(mime) = &file.mime {
        link = link.mime_type(Some(mime.clone()));
    }
    Some(SessionUpdate::UserMessageChunk(
        ContentChunk::new(ContentBlock::ResourceLink(link)).message_id(message_id),
    ))
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
    use crate::dto::{AttachmentSource, MessagesEnvelope, StructuredError};

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
        let updates = replay_updates(&fixture_records(), false, &[], false);

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
        })], false, &[], false);
        assert_eq!(streaming.len(), 1);
        let SessionUpdate::ToolCall(call) = &streaming[0] else { panic!() };
        assert_eq!(call.raw_input, Some(serde_json::Value::String(r#"{"x":1}"#.into())));

        // Running: pending call + in-progress update.
        let running = replay_updates(&[record_with_tool(ToolState::Running {
            input: serde_json::json!({"x": 1}),
            metadata: None,
        })], false, &[], false);
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
        let updates = replay_updates(&records, false, &[], false);
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
        let updates = replay_updates(&fixture_records(), false, &[], false);
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
            files: None,
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
        let updates = replay_updates(&records, false, &children, false);
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

    fn link_file(name: &str, uri: Option<&str>, mime: Option<&str>) -> AttachmentFile {
        AttachmentFile {
            name: name.into(),
            mime: mime.map(str::to_string),
            source: uri.map(|u| AttachmentSource {
                kind: Some("uri".into()),
                uri: Some(u.into()),
            }),
        }
    }

    /// Release 0.7.2: a user record with `files` replays as the text chunk
    /// followed by ONE ResourceLink chunk per file — same message id (Zed
    /// merges adjacent chunks by messageId into one user message). Files
    /// without a usable uri (missing source / empty uri) project nothing.
    /// Release 0.8.1 (`--show-synthetic`): `kind: "synthetic"` records
    /// (system-injected messages — background bash completions, subagent-
    /// completion notifications) replay as user chunks ONLY with the flag.
    /// Default off: skipped, matching the live listener. User records are
    /// unaffected either way.
    #[test]
    fn synthetic_records_skip_by_default_and_replay_with_flag() {
        let synthetic = MessageRecord {
            kind: "synthetic".into(),
            id: "msg_syn".into(),
            files: None,
            text: Some("<shell id=\"sh_1\" state=\"completed\" command=\"sleep 2\">\ndone\n</shell>".into()),
            agent: None,
            model: None,
            content: None,
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        };
        let user = MessageRecord {
            kind: "user".into(),
            id: "msg_user".into(),
            files: None,
            text: Some("a real prompt".into()),
            agent: None,
            model: None,
            content: None,
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        };
        let records = [synthetic, user];

        // Flag off (default): only the user record renders.
        let off = replay_updates(&records, false, &[], false);
        assert_eq!(off.len(), 1, "synthetic skipped with the flag off");
        let SessionUpdate::UserMessageChunk(chunk) = &off[0] else {
            panic!("the single update is the user text chunk");
        };
        assert!(matches!(&chunk.content, ContentBlock::Text(t) if t.text == "a real prompt"));

        // Flag on: BOTH render as user chunks (records replay newest-first:
        // the synthetic record is older, so it comes second).
        let on = replay_updates(&records, false, &[], true);
        assert_eq!(on.len(), 2, "synthetic renders with the flag on");
        let SessionUpdate::UserMessageChunk(user_chunk) = &on[0] else {
            panic!("first update is the user text chunk");
        };
        assert!(matches!(&user_chunk.content, ContentBlock::Text(t) if t.text == "a real prompt"));
        let SessionUpdate::UserMessageChunk(syn_chunk) = &on[1] else {
            panic!("second update is the synthetic text chunk");
        };
        assert!(matches!(
            &syn_chunk.content,
            ContentBlock::Text(t) if t.text.contains("<shell id=\"sh_1\"")
        ));
        assert_eq!(
            syn_chunk.message_id.as_ref().map(|m| m.0.as_ref()),
            Some("msg_syn"),
            "synthetic record replays on its own message id"
        );
    }

    #[test]
    fn user_record_with_files_replays_text_then_resource_links() {
        let record = MessageRecord {
            kind: "user".into(),
            id: "msg_u_files".into(),
            files: Some(vec![
                link_file("notes.txt", Some("file:///tmp/notes.txt"), Some("text/plain")),
                link_file("empty.ts", None, None),
                link_file("README.md", Some(""), Some("text/markdown")),
            ]),
            text: Some("read these".into()),
            agent: None,
            model: None,
            content: None,
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        };
        let updates = replay_updates(&[record], false, &[], false);

        // Text first, then exactly ONE link chunk (the two degenerate files
        // project nothing).
        assert_eq!(updates.len(), 2, "text + one usable link");
        let SessionUpdate::UserMessageChunk(text_chunk) = &updates[0] else {
            panic!("first update is the text chunk");
        };
        assert!(matches!(
            &text_chunk.content,
            ContentBlock::Text(t) if t.text == "read these"
        ));
        let SessionUpdate::UserMessageChunk(link_chunk) = &updates[1] else {
            panic!("second update is the link chunk");
        };
        let ContentBlock::ResourceLink(link) = &link_chunk.content else {
            panic!("content is a resource link");
        };
        assert_eq!(link.name, "notes.txt");
        assert_eq!(link.uri, "file:///tmp/notes.txt");
        assert_eq!(link.mime_type.as_deref(), Some("text/plain"));
        for chunk in [text_chunk, link_chunk] {
            assert_eq!(
                chunk.message_id.as_ref().map(|m| m.0.as_ref()),
                Some("msg_u_files"),
                "text and links share the user message id"
            );
        }
    }

    /// The live 2.0.21 wire shape (captured 2026-10-03): the record carries
    /// the base64 `data` — the bridge does NOT decode it (serde skips the
    /// unknown field) and replays only the link fields.
    #[test]
    fn user_record_decodes_files_and_skips_base64_data() {
        let raw = r#"{"id":"msg_att","time":{"created":0,"updated":0},
            "text":"capture attachment shape",
            "files":[{"data":"aGVsbG8gYXR0YWNobWVudA==","mime":"text/plain",
                      "source":{"type":"uri","uri":"file:///tmp/acr-attach/notes.txt"},
                      "name":"notes.txt"}],
            "type":"user"}"#;
        let rec: MessageRecord = serde_json::from_str(raw).expect("decode");
        let files = rec.files.as_ref().expect("files decoded");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "notes.txt");
        assert_eq!(files[0].mime.as_deref(), Some("text/plain"));
        assert_eq!(
            files[0].source.as_ref().and_then(|s| s.uri.as_deref()),
            Some("file:///tmp/acr-attach/notes.txt")
        );

        // The decoded record replays as text + link, like the live user saw
        // before reload.
        let updates = replay_updates(&[rec], false, &[], false);
        assert_eq!(updates.len(), 2, "text chunk + resource link chunk");
        let SessionUpdate::UserMessageChunk(c) = &updates[1] else { panic!() };
        assert!(matches!(&c.content, ContentBlock::ResourceLink(_)));
        assert_eq!(c.message_id.as_ref().map(|m| m.0.as_ref()), Some("msg_att"));
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
        let updates = replay_updates(&records, false, &wrong, false);
        let SessionUpdate::ToolCall(call) = &updates[0] else {
            panic!("declaration expected")
        };
        assert!(call.meta.is_none(), "unmatched call stays plain");
        // Empty children → plain too.
        let updates = replay_updates(&records, false, &[], false);
        let SessionUpdate::ToolCall(call) = &updates[0] else {
            panic!("declaration expected")
        };
        assert!(call.meta.is_none());
    }
}
