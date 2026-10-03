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

use std::collections::{HashMap, HashSet, VecDeque};

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, Cost, ImageContent, SessionInfoUpdate, SessionUpdate, TextContent,
    ToolCall, ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
    UsageUpdate,
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
    /// ToolCallIds already DECLARED to the client as an initial `ToolCall`
    /// (Release 0.3.2). ACP requires the declaration before any
    /// `ToolCallUpdate` — Zed otherwise renders a "Tool call not found"
    /// placeholder card. Filled on the FIRST emission for each id; keyed by
    /// the FINAL (namespaced) id like `open_tools`, so child projections
    /// declare their own calls.
    introduced_tools: HashSet<String>,
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

    /// The display title (tool name) indexed for a tool call id, if seen
    /// (from `session.tool.input.started`).
    pub(crate) fn tool_title(&self, tool_call_id: &str) -> Option<&str> {
        self.tool_titles.get(tool_call_id).map(String::as_str)
    }

    /// Whether a tool call id was already DECLARED to the client (Release
    /// 0.3.2 introduce-on-first-sight bookkeeping). Used by the background
    /// listener to avoid card updates for calls the client never saw.
    pub(crate) fn is_introduced(&self, tool_call_id: &str) -> bool {
        self.introduced_tools.contains(tool_call_id)
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

    /// Release 0.3.2: declare a tool call to the client on its FIRST
    /// emission — ACP requires the initial `ToolCall` before any
    /// `ToolCallUpdate` (the live path historically pushed updates only,
    /// which Zed renders as a "Tool call not found" placeholder). Returns
    /// the initial `ToolCall` update, or `None` when the id was already
    /// introduced.
    pub(crate) fn introduce_tool(
        &mut self,
        id: &str,
        title: String,
        kind: ToolKind,
        status: ToolCallStatus,
        raw_input: Option<serde_json::Value>,
    ) -> Option<SessionUpdate> {
        if !self.introduced_tools.insert(id.to_string()) {
            return None;
        }
        let mut call = ToolCall::new(id.to_string(), title).kind(kind).status(status);
        if let Some(input) = raw_input {
            call = call.raw_input(input);
        }
        Some(SessionUpdate::ToolCall(call))
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
        | dto::SessionEvent::ToolFailed(_) => to_tool_updates(event, state, None),
        dto::SessionEvent::ToolProgress(_) => {
            // No-op: the call is already `in_progress` since `tool.called`.
            // Kept as its own arm so the mapping is explicit and greppable.
            vec![]
        }
        // inbox enqueued: the PER-TURN path never surfaces the user's own
        // message (the ACP client authored the local prompt and shows its
        // own draft). The remote-turn user chunk is produced by the
        // background listener (agent.rs), which handles the same event
        // before the generic mapping.
        dto::SessionEvent::InboxEnqueued(_) => vec![],

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

// ============================================================
// Release 0.6.0: native subagent sessions
// ============================================================
//
// Zed's native subagent mechanism (verified against Zed main 2026-08-16 and
// against the live 2.0.21 wire, capture in
// tests/fixtures/subagent-native.sse.jsonl): the bridge attaches
// `_meta.subagent_session_info` {session_id, message_start_index,
// message_end_index} to the PARENT's task tool call — Zed then renders the
// card in a subagent mode and routes `session/update` notifications
// addressed to the CHILD's own session id into the embedded transcript.
//
// Wire facts the pairing relies on (live capture):
// - The spawner tool is named `subagent` (aliased) or `task` (stock); its
//   input events precede the child's `session.created {parentID, title}`.
// - `session.tool.progress` on the PARENT's spawner call carries
//   `metadata: {"sessionID": "<child>", "status": "running"}` — the direct
//   child linkage, also echoed on the spawner's `tool.success` metadata.
// - Continuation calls carry `sessionID` inside the tool input (`called` /
//   `input.ended`) and fire NO `session.created` (the child already exists).

/// The `_meta` key Zed consumes on a tool call (declaration or update).
pub const SUBAGENT_META_KEY: &str = "subagent_session_info";

/// Tool names that spawn child sessions on the wire: `task` (stock
/// opencode 2.0.21) and `subagent` (the aliased name on the bridge's
/// production server — wire-verified in both captures).
pub fn is_spawner_name(name: &str) -> bool {
    matches!(name, "task" | "subagent")
}

/// Release 0.7.1: the display title for a subagent-SPAWNER tool call.
///
/// Zed's own `spawn_agent` card labels itself with the dispatch
/// `description`; the bridge aligns the spawner card title to the same
/// convention: the input's `description` (trimmed), truncated to 80
/// characters + "…" on the wire (the renderer truncates via CSS too, but
/// the wire must not carry unbounded strings), falling back to the tool
/// name.
pub(crate) fn spawner_display_title(name: &str, input: Option<&serde_json::Value>) -> String {
    let description = input
        .and_then(|i| i.get("description"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match description {
        Some(d) => truncate_title(d),
        None => name.to_string(),
    }
}

/// Cap the wire title at 80 chars + "…" — char-boundary safe (descriptions
/// may be non-ASCII; slicing at byte 80 could split a multi-byte char).
fn truncate_title(s: &str) -> String {
    let count = s.chars().count();
    if count <= 80 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(80).collect();
        out.push('…');
        out
    }
}

/// The `_meta.subagent_session_info` object: `message_end_index` None → the
/// open slice (announce), Some → the completed slice (task call success).
pub fn subagent_meta(
    session_id: &str,
    start: usize,
    end: Option<usize>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut info = serde_json::Map::new();
    info.insert("session_id".into(), serde_json::Value::from(session_id));
    info.insert("message_start_index".into(), serde_json::Value::from(start));
    info.insert(
        "message_end_index".into(),
        end.map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    let mut meta = serde_json::Map::new();
    meta.insert(SUBAGENT_META_KEY.into(), serde_json::Value::Object(info));
    meta
}

/// Cap of buffered child events awaiting pair completion (drop-oldest).
const PENDING_CAP: usize = 256;
/// Cap of card content lines aggregated on the parent's task card.
pub const MAX_CARD_LINES: usize = 6;

/// Persistent (across turns) state of one child (subagent) session, keyed
/// by child session id on the PARENT's [`SessionEntry`]-owned tracker.
#[derive(Debug, Default)]
pub struct ChildTrack {
    /// Child session id (`ses_...`).
    pub child_id: String,
    /// Child session title (from `session.created`).
    pub title: String,
    /// The spawner tool call id on the parent (plain id); `None` while the
    /// pairing is unresolved.
    pub parent_call: Option<String>,
    /// `message_start_index` of the CURRENT announced meta (the slice start
    /// of the live turn). Guards re-announcing identical state.
    announced_start: Option<usize>,
    /// Total transcript entries emitted for this child (across turns) —
    /// the index space Zed slices with `message_start/end_index`.
    pub entries: usize,
    /// User/assistant message ids already counted as transcript entries.
    counted_messages: HashSet<String>,
    /// Tool call ids already counted as transcript entries.
    counted_tools: HashSet<String>,
    /// Aggregated card content lines (title-prefixed, capped at
    /// [`MAX_CARD_LINES`]).
    card_lines: Vec<String>,
    /// Child events buffered while the pairing is unresolved (the announce
    /// must precede any child traffic; flush after pairing, in order).
    pending: VecDeque<dto::SessionEvent>,
}

impl ChildTrack {
    fn new(child_id: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            child_id: child_id.into(),
            title: title.into(),
            ..Self::default()
        }
    }

    /// Count a user/assistant message as a transcript entry; `1` when new.
    pub fn record_message(&mut self, id: &str) -> usize {
        if self.counted_messages.insert(id.to_string()) {
            self.entries += 1;
            1
        } else {
            0
        }
    }

    /// Count a tool call id as a transcript entry; `1` when new.
    pub fn record_tool(&mut self, id: &str) -> usize {
        if self.counted_tools.insert(id.to_string()) {
            self.entries += 1;
            1
        } else {
            0
        }
    }

    /// Append a card content line (`"{title}: {label}"`), keeping the last
    /// [`MAX_CARD_LINES`].
    pub fn push_card_line(&mut self, label: String) {
        self.card_lines.push(format!("{}: {}", self.title, label));
        if self.card_lines.len() > MAX_CARD_LINES {
            self.card_lines.remove(0);
        }
    }

    /// The aggregated card content (text lines).
    pub fn card_content(&self) -> Vec<ToolCallContent> {
        self.card_lines
            .iter()
            .map(|line| ToolCallContent::from(ContentBlock::Text(TextContent::new(line.clone()))))
            .collect()
    }

    /// The open-slice meta to announce on the parent's task call: the slice
    /// starts at the CURRENT entry count and stays open (end: null).
    pub fn meta_open(&self) -> serde_json::Map<String, serde_json::Value> {
        subagent_meta(&self.child_id, self.entries, None)
    }

    /// The closed-slice meta for the task call's terminal update: the
    /// announced start, end = the current entry count (inclusive).
    pub fn meta_closed(&self) -> serde_json::Map<String, serde_json::Value> {
        let start = self.announced_start.unwrap_or(self.entries);
        subagent_meta(&self.child_id, start, Some(self.entries))
    }

    /// Whether a meta with this start was already announced (spam guard).
    pub fn should_announce(&self, start: usize) -> bool {
        self.announced_start != Some(start)
    }

    /// Record the announced slice start.
    pub fn mark_announced(&mut self, start: usize) {
        self.announced_start = Some(start);
    }

    /// Buffer a child event while the pairing is unresolved. Returns `false`
    /// when the oldest buffered event was dropped (cap reached).
    pub fn buffer(&mut self, event: dto::SessionEvent) -> bool {
        if self.pending.len() >= PENDING_CAP {
            tracing::warn!(
                child = %self.child_id,
                "subagent pending buffer full — dropping oldest buffered child event"
            );
            self.pending.pop_front();
            self.pending.push_back(event);
            false
        } else {
            self.pending.push_back(event);
            true
        }
    }

    /// Take the buffered events (in order) for post-pairing flush.
    pub fn drain(&mut self) -> Vec<dto::SessionEvent> {
        self.pending.drain(..).collect()
    }
}

/// Per-parent-session tracker pairing the parent's spawner tool calls with
/// their child sessions + the persistent per-child state. Shared between
/// the turn loop and the background listener (serialized by the owning
/// `SessionEntry`'s mutex).
#[derive(Debug, Default)]
pub struct SubagentTracker {
    /// Unpaired spawner call ids, FIFO (`note_spawner_call`).
    unpaired_calls: VecDeque<String>,
    /// Child session id → persistent track.
    children: HashMap<String, ChildTrack>,
    /// Spawner call id → child session id (paired).
    call_to_child: HashMap<String, String>,
}

impl SubagentTracker {
    /// Register a spawner tool call (its `input.started` carried a spawner
    /// name) as a pairing candidate. Idempotent.
    pub fn note_spawner_call(&mut self, call_id: &str) {
        if self.call_to_child.contains_key(call_id)
            || self.unpaired_calls.iter().any(|c| c == call_id)
        {
            return;
        }
        self.unpaired_calls.push_back(call_id.to_string());
    }

    /// The child session id a spawner call is paired with, if any.
    pub fn call_child(&self, call_id: &str) -> Option<&str> {
        self.call_to_child.get(call_id).map(String::as_str)
    }

    /// Whether `session_id` is a tracked child.
    pub fn is_child(&self, session_id: &str) -> bool {
        self.children.contains_key(session_id)
    }

    pub fn child(&self, child_id: &str) -> Option<&ChildTrack> {
        self.children.get(child_id)
    }

    pub fn child_mut(&mut self, child_id: &str) -> Option<&mut ChildTrack> {
        self.children.get_mut(child_id)
    }

    /// Drop a still-unpaired spawner call (its terminal arrived without a
    /// child — the child creation failed; never pair a dead call later).
    fn retire_unpaired(&mut self, call_id: &str) {
        self.unpaired_calls.retain(|c| c != call_id);
    }

    /// Pair `call_id` with `child_id` DIRECTLY (input `sessionID` or
    /// progress-metadata linkage). Returns `(child_id, call_id)` when a NEW
    /// announce must go out (never for already-announced pairs).
    #[allow(clippy::type_complexity)]
    fn pair_direct(
        &mut self,
        call_id: &str,
        child_id: &str,
        title: &str,
    ) -> Option<(String, String)> {
        self.unpaired_calls.retain(|c| c != call_id);
        self.call_to_child
            .insert(call_id.to_string(), child_id.to_string());
        let track = self
            .children
            .entry(child_id.to_string())
            .or_insert_with(|| ChildTrack::new(child_id, title));
        if track.parent_call.is_some() {
            // Already announced paired (a continuation call). Re-announce
            // only when the slice start changed (new turn → new slice).
            let start = track.entries;
            if track.should_announce(start) {
                track.mark_announced(start);
                return Some((child_id.to_string(), call_id.to_string()));
            }
            return None;
        }
        track.parent_call = Some(call_id.to_string());
        if track.title.is_empty() {
            track.title = title.to_string();
        }
        let start = track.entries;
        track.mark_announced(start);
        Some((child_id.to_string(), call_id.to_string()))
    }

    /// Pair a NEW child (`session.created`) with the OLDEST unpaired
    /// spawner call. Returns `(child_id, call_id)` when an announce must go
    /// out; `(child_id, "")` when the child is tracked but unpaired; `None`
    /// when the child was already paired.
    fn pair_created(&mut self, child_id: &str, title: &str) -> Option<(String, String)> {
        if self.children.get(child_id).map(|t| t.parent_call.is_some()) == Some(true) {
            return None;
        }
        let track = self
            .children
            .entry(child_id.to_string())
            .or_insert_with(|| ChildTrack::new(child_id, title));
        if track.title.is_empty() {
            track.title = title.to_string();
        }
        match self.unpaired_calls.pop_front() {
            Some(call_id) => {
                self.call_to_child
                    .insert(call_id.clone(), child_id.to_string());
                track.parent_call = Some(call_id.clone());
                let start = track.entries;
                track.mark_announced(start);
                Some((child_id.to_string(), call_id))
            }
            None => {
                // No observed spawner call yet — the child is tracked
                // unpaired (its events buffer until a pairing lands).
                tracing::warn!(
                    child = %child_id,
                    "child session created without a matching spawner call observed — buffering its events"
                );
                Some((child_id.to_string(), String::new()))
            }
        }
    }
}

/// What the pairing bookkeeping of ONE parent event produced.
#[derive(Debug, Default)]
pub struct TrackerOutcome {
    /// A new open-slice meta to announce on the parent session:
    /// (child_id, spawner call id, meta). Followed by the pending flush.
    pub announce: Option<(String, String, serde_json::Map<String, serde_json::Value>)>,
    /// The completion meta for the spawner call's terminal update:
    /// (call id, meta) — attach to the mapped update.
    pub complete: Option<(String, serde_json::Map<String, serde_json::Value>)>,
}

fn input_session_id(input: &serde_json::Value) -> Option<&str> {
    input
        .as_object()
        .and_then(|o| o.get("sessionID"))
        .and_then(|v| v.as_str())
}

/// Feed one event of the PARENT session (or a `session.created` on the
/// shared stream) into the tracker: spawner-call registration, pairing
/// (input `sessionID`, progress-metadata, `session.created` queue), and the
/// spawner-termination completion meta. Pure tracker bookkeeping — the
/// caller sends the notifications.
pub fn track_event(
    tracker: &mut SubagentTracker,
    event: &dto::SessionEvent,
    parent_id: &str,
) -> TrackerOutcome {
    match event {
        dto::SessionEvent::SessionCreated(created) => {
            let Some(child_id) = created.sessionID.as_deref() else {
                return TrackerOutcome::default();
            };
            if created.parentID.as_deref() != Some(parent_id) {
                return TrackerOutcome::default();
            }
            let title = created.title.clone().unwrap_or_default();
            match tracker.pair_created(child_id, &title) {
                Some((child_id, call_id)) if !call_id.is_empty() => {
                    let meta = tracker
                        .child(&child_id)
                        .expect("paired child tracked")
                        .meta_open();
                    TrackerOutcome {
                        announce: Some((child_id, call_id, meta)),
                        ..TrackerOutcome::default()
                    }
                }
                // Unpaired (no spawner call observed) or already paired.
                _ => TrackerOutcome::default(),
            }
        }
        dto::SessionEvent::ToolInputStarted(t) => {
            if is_spawner_name(&t.name) {
                tracker.note_spawner_call(&t.base.id);
            }
            TrackerOutcome::default()
        }
        dto::SessionEvent::ToolInputEnded(t) => {
            if tracker.call_child(&t.base.id).is_some() {
                return TrackerOutcome::default();
            }
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&t.text) {
                if let Some(child) = input_session_id(&parsed) {
                    return pair_announce(tracker, &t.base.id, child);
                }
            }
            TrackerOutcome::default()
        }
        dto::SessionEvent::ToolCalled(t) => {
            if tracker.call_child(&t.base.id).is_some() {
                return TrackerOutcome::default();
            }
            if let Some(child) = input_session_id(&t.input) {
                return pair_announce(tracker, &t.base.id, child);
            }
            TrackerOutcome::default()
        }
        // The parent's spawner call announces its child on the wire:
        // `metadata.sessionID` — the direct linkage. (Child tools ride
        // their own session id, so this arm only ever sees parent calls.)
        dto::SessionEvent::ToolProgress(p) => {
            if p.base.sessionID != parent_id {
                return TrackerOutcome::default();
            }
            let Some(child) = p
                .metadata
                .as_ref()
                .and_then(|m| m.sessionID.as_deref())
                .filter(|c| !c.is_empty())
            else {
                return TrackerOutcome::default();
            };
            if tracker.call_child(&p.base.id) == Some(child) {
                return TrackerOutcome::default();
            }
            pair_announce(tracker, &p.base.id, child)
        }
        dto::SessionEvent::ToolSuccess(t) => {
            terminal_call(tracker, &t.base.id)
        }
        dto::SessionEvent::ToolFailed(t) => {
            terminal_call(tracker, &t.base.id)
        }
        _ => TrackerOutcome::default(),
    }
}

/// Terminal bookkeeping for a paired/unpaired spawner call.
fn terminal_call(
    tracker: &mut SubagentTracker,
    id: &str,
) -> TrackerOutcome {
    match tracker.call_child(id) {
        Some(child_id) => {
            let meta = tracker
                .child(child_id)
                .expect("paired child tracked")
                .meta_closed();
            TrackerOutcome {
                complete: Some((id.to_string(), meta)),
                ..TrackerOutcome::default()
            }
        }
        None => {
            tracker.retire_unpaired(id);
            TrackerOutcome::default()
        }
    }
}

/// The shared tail of the direct-pairing arms: pair + build the announce.
fn pair_announce(
    tracker: &mut SubagentTracker,
    call_id: &str,
    child: &str,
) -> TrackerOutcome {
    let title = tracker.child(child).map(|t| t.title.clone()).unwrap_or_default();
    match tracker.pair_direct(call_id, child, &title) {
        Some((child_id, call_id)) => {
            let meta = tracker.child(&child_id).expect("paired child tracked").meta_open();
            TrackerOutcome {
                announce: Some((child_id, call_id, meta)),
                ..TrackerOutcome::default()
            }
        }
        None => TrackerOutcome::default(),
    }
}

/// Map one CHILD-session event to ACP updates addressed at the CHILD's own
/// session id (native subagent mode). Returns (updates, card) — `card` is a
/// refreshed content payload for the PARENT's task card when a child tool
/// boundary advanced (the caller sends it as a parent-session
/// `tool_call_update`).
///
/// Transcript entries are counted inside `track` (persistent across turns):
/// the child's user message (inbox), one per distinct assistant message id,
/// one per distinct tool call id — the index space `message_start/end_index`
/// slices on the parent's task card.
pub fn to_child_updates(
    event: &dto::SessionEvent,
    state: &mut MappingState,
    track: &mut ChildTrack,
) -> (Vec<SessionUpdate>, Option<(String, Vec<ToolCallContent>)>) {
    match event {
        // The child's task prompt: the user inbox item → user chunk.
        dto::SessionEvent::InboxEnqueued(inbox)
            if inbox.item.as_ref().and_then(|i| i.kind.as_deref()) == Some("user") =>
        {
            let Some(text) = inbox
                .item
                .as_ref()
                .and_then(|i| i.payload.as_ref())
                .and_then(|p| p.text.clone())
            else {
                return (Vec::new(), None);
            };
            track.record_message(&inbox.inboxID);
            let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)))
                .message_id(inbox.inboxID.as_str());
            (vec![SessionUpdate::UserMessageChunk(chunk)], None)
        }
        dto::SessionEvent::TextDelta(d) => {
            track.record_message(&d.base.assistantMessageID);
            (
                vec![SessionUpdate::AgentMessageChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(d.delta.clone())))
                        .message_id(d.base.assistantMessageID.as_str()),
                )],
                None,
            )
        }
        dto::SessionEvent::ReasoningDelta(d) => {
            track.record_message(&d.base.assistantMessageID);
            (
                vec![SessionUpdate::AgentThoughtChunk(
                    ContentChunk::new(ContentBlock::Text(TextContent::new(d.delta.clone())))
                        .message_id(d.base.assistantMessageID.as_str()),
                )],
                None,
            )
        }
        dto::SessionEvent::ToolInputStarted(_)
        | dto::SessionEvent::ToolInputEnded(_)
        | dto::SessionEvent::ToolCalled(_)
        | dto::SessionEvent::ToolSuccess(_)
        | dto::SessionEvent::ToolFailed(_) => {
            let updates = to_tool_updates(event, state, None);
            let card = child_tool_boundary(event, state, track);
            // One transcript entry per distinct tool call id.
            if let Some(id) = tool_event_id(event) {
                track.record_tool(id);
            }
            (updates, card)
        }
        other => (to_updates(other, state), None),
    }
}

/// The raw opencode call id of a tool event.
fn tool_event_id(event: &dto::SessionEvent) -> Option<&str> {
    match event {
        dto::SessionEvent::ToolInputStarted(t) => Some(&t.base.id),
        dto::SessionEvent::ToolInputEnded(t) => Some(&t.base.id),
        dto::SessionEvent::ToolCalled(t) => Some(&t.base.id),
        dto::SessionEvent::ToolSuccess(t) => Some(&t.base.id),
        dto::SessionEvent::ToolFailed(t) => Some(&t.base.id),
        _ => None,
    }
}

/// One-line label of a child tool boundary for the parent's task card: the
/// tool name (at `input.started`), then `name <first string arg>` (at
/// `called`). Keeps the card live without per-delta churn.
fn child_tool_boundary(
    event: &dto::SessionEvent,
    state: &MappingState,
    track: &mut ChildTrack,
) -> Option<(String, Vec<ToolCallContent>)> {
    match event {
        dto::SessionEvent::ToolInputStarted(t) => {
            track.push_card_line(t.name.clone());
        }
        dto::SessionEvent::ToolCalled(t) => {
            let name = state.tool_title(&t.base.id).unwrap_or("tool").to_string();
            let arg = t
                .input
                .as_object()
                .and_then(|o| o.values().find_map(|v| v.as_str().map(str::to_string)))
                .filter(|a| !a.is_empty());
            let label = match arg {
                Some(arg) if arg.len() <= 60 => format!("{name} {arg}"),
                Some(mut arg) => {
                    arg.truncate(60);
                    format!("{name} {arg}…")
                }
                None => name,
            };
            track.push_card_line(label);
        }
        _ => return None,
    }
    let parent_call = track.parent_call.clone()?;
    Some((parent_call, track.card_content()))
}

/// Count the transcript entries a child's persisted message records
/// represent (user messages + assistant messages + tool parts) — the
/// replay-side mirror of the live [`ChildTrack`] counting, so the
/// `message_end_index` attached at replay matches the live slice math.
pub fn count_entry_records(records: &[crate::dto::MessageRecord]) -> usize {
    records
        .iter()
        .map(|r| match r.kind.as_str() {
            "user" => 1,
            "assistant" => {
                1 + r
                    .content
                    .as_ref()
                    .map(|parts| {
                        parts
                            .iter()
                            .filter(|p| matches!(p, crate::dto::Part::Tool { .. }))
                            .count()
                    })
                    .unwrap_or(0)
            }
            _ => 0,
        })
        .sum()
}

/// `to_updates`, but the tool-event arms carry an optional `_meta` — the
/// subagent-spawner completion meta (Release 0.6.0) attached to the
/// terminal update of the paired task call. Non-tool events delegate to
/// [`to_updates`] (only tool terminals can carry it).
pub fn to_updates_annotated(
    event: &dto::SessionEvent,
    state: &mut MappingState,
    completion_meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Vec<SessionUpdate> {
    match event {
        dto::SessionEvent::ToolInputStarted(_)
        | dto::SessionEvent::ToolInputEnded(_)
        | dto::SessionEvent::ToolCalled(_)
        | dto::SessionEvent::ToolSuccess(_)
        | dto::SessionEvent::ToolFailed(_) => to_tool_updates(event, state, completion_meta),
        other => to_updates(other, state),
    }
}

/// Tool-event arms shared by the parent path (`to_updates`) and the
/// child-session path (`to_child_updates`). Native subagent mode uses PLAIN
/// call ids everywhere (no child namespace — the child's events are
/// addressed to the child session itself).
///
/// `completion_meta`: `_meta` for the terminal update of a subagent-spawner
/// call (`subagent_session_info` with `message_end_index`) — the caller
/// (turn loop / background listener) computes it from the pairing tracker.
fn to_tool_updates(
    event: &dto::SessionEvent,
    state: &mut MappingState,
    completion_meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Vec<SessionUpdate> {
    match event {
        dto::SessionEvent::ToolInputStarted(t) => {
            // ACP convention (mirrors the TS bridge): while input streams, the
            // call is advertised as `pending` with the tool name as title.
            let id = t.base.id.clone();
            state.tool_titles.insert(id.clone(), t.name.clone());
            state.open_tool(id.clone());
            // Release 0.7.1: a subagent-SPAWNER call's declaration is DEFERRED
            // to `tool.input.ended` — its card title is the dispatch
            // `description` (Zed's spawn_agent convention), which only exists
            // in the input, and the input has not arrived yet. Declaring here
            // would paint the card with the bare tool name ("subagent") until
            // a later retitle; declaring once at input.ended (same emission as
            // the raw input) makes the FIRST sight of the call carry the
            // description title — no flash, no retitle update. Non-spawner
            // tools declare exactly as before.
            if is_spawner_name(&t.name) {
                return Vec::new();
            }
            // Release 0.3.2: the initial `ToolCall` declaration rides the
            // first emission (the client must see the call before any update).
            let mut out = Vec::with_capacity(2);
            if let Some(decl) = state.introduce_tool(
                &id,
                t.name.clone(),
                tool_kind(&t.name),
                ToolCallStatus::Pending,
                None,
            ) {
                out.push(decl);
            }
            out.push(tool_update(
                &id,
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Pending)
                    .title(t.name.clone()),
            ));
            out
        }
        dto::SessionEvent::ToolInputEnded(t) => {
            // `text` is the raw JSON input string — pass it through verbatim
            // (the pending call's streaming input, as a JSON string value).
            // Also cache the parsed input for the permission prompt.
            let id = t.base.id.clone();
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&t.text) {
                state.tool_inputs.insert(id.clone(), parsed);
            }
            // Release 0.7.1: the spawner's deferred declaration lands here —
            // the parsed input is now on hand, so the card's FIRST
            // declaration carries the dispatch `description` as title
            // (fallback: the tool name), with Pending + the raw JSON input.
            // Degenerate introduced-here case (input.started skipped, or a
            // non-spawner tool): no name on the event — the id itself is the
            // only truthful title; the raw JSON input rides the declaration.
            let mut out = Vec::with_capacity(2);
            let raw_input = Some(serde_json::Value::String(t.text.clone()));
            match state.tool_titles.get(&id) {
                Some(name) if is_spawner_name(name) => {
                    let title = spawner_display_title(name, state.tool_input(&id));
                    if let Some(decl) = state.introduce_tool(
                        &id,
                        title,
                        tool_kind(name),
                        ToolCallStatus::Pending,
                        raw_input,
                    ) {
                        out.push(decl);
                    }
                }
                _ => {
                    if let Some(decl) = state.introduce_tool(
                        &id,
                        id.clone(),
                        ToolKind::Other,
                        ToolCallStatus::Pending,
                        raw_input,
                    ) {
                        out.push(decl);
                    }
                }
            }
            out.push(tool_update(
                &id,
                ToolCallUpdateFields::new().raw_input(serde_json::Value::String(t.text.clone())),
            ));
            out
        }
        dto::SessionEvent::ToolCalled(t) => {
            // Parsed input now available → mark in progress.
            let id = t.base.id.clone();
            state.tool_inputs.insert(id.clone(), t.input.clone());
            let mut out = Vec::with_capacity(2);
            // Release 0.7.1: introduce-with-input for a spawner whose input
            // events were skipped uses the same description title as the
            // deferred declaration (the input is on hand here) — fallback:
            // the tool name. Non-spawner degenerate introductions keep the
            // id-as-title behavior (never a truthful name on this event).
            match state.tool_titles.get(&id) {
                Some(name) if is_spawner_name(name) => {
                    let title = spawner_display_title(name, Some(&t.input));
                    if let Some(decl) = state.introduce_tool(
                        &id,
                        title,
                        tool_kind(name),
                        ToolCallStatus::InProgress,
                        Some(t.input.clone()),
                    ) {
                        out.push(decl);
                    }
                }
                _ => {
                    if let Some(decl) = state.introduce_tool(
                        &id,
                        id.clone(),
                        ToolKind::Other,
                        ToolCallStatus::InProgress,
                        Some(t.input.clone()),
                    ) {
                        out.push(decl);
                    }
                }
            }
            out.push(tool_update(
                &id,
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::InProgress)
                    .raw_input(t.input.clone()),
            ));
            out
        }
        dto::SessionEvent::ToolSuccess(t) => {
            let id = t.base.id.clone();
            state.close_tool(&id);
            // Introduce-with-terminal when the whole lifecycle was missed
            // (no input events at all): the event's own title field, then
            // the indexed tool name, then the id.
            let title = t
                .metadata
                .as_ref()
                .and_then(|m| m.title.clone())
                .or_else(|| state.tool_titles.get(&id).cloned())
                .unwrap_or_else(|| id.clone());
            let mut out = Vec::with_capacity(2);
            if let Some(decl) =
                state.introduce_tool(&id, title, ToolKind::Other, ToolCallStatus::Completed, None)
            {
                out.push(decl);
            }
            let mut fields = ToolCallUpdateFields::new().status(ToolCallStatus::Completed);
            if let Some(meta) = &t.metadata {
                if let Some(title) = &meta.title {
                    // Release 0.7.1: a spawner's terminal title stays
                    // consistent with its declared card title (the dispatch
                    // description, same fallback chain) when the server
                    // carries one; non-spawner terminal titles (e.g. the
                    // edited file's name) are untouched.
                    match state.tool_titles.get(&id) {
                        Some(name) if is_spawner_name(name) => {
                            let spawner_title = spawner_display_title(name, state.tool_input(&id));
                            fields = fields.title(spawner_title);
                        }
                        _ => fields = fields.title(title.clone()),
                    }
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
            out.push(tool_update_meta(&id, fields, completion_meta));
            out
        }
        dto::SessionEvent::ToolFailed(t) => {
            let id = t.base.id.clone();
            state.close_tool(&id);
            let mut out = Vec::with_capacity(2);
            let title = state
                .tool_titles
                .get(&id)
                .cloned()
                .unwrap_or_else(|| id.clone());
            if let Some(decl) = state.introduce_tool(
                &id,
                title,
                ToolKind::Other,
                ToolCallStatus::Failed,
                None,
            ) {
                out.push(decl);
            }
            // v1 has no error field on tool updates: the failure surfaces as
            // `Failed` with the error message in the raw output.
            let message = t
                .error
                .message
                .clone()
                .unwrap_or_else(|| "Tool execution failed".to_string());
            let update = ToolCallUpdate::new(
                id.clone(),
                ToolCallUpdateFields::new()
                    .status(ToolCallStatus::Failed)
                    .raw_output(serde_json::Value::String(message)),
            );
            let update = match completion_meta {
                Some(meta) => update.meta(meta.clone()),
                None => update,
            };
            out.push(SessionUpdate::ToolCallUpdate(update));
            out
        }
        _ => vec![],
    }
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
        dto::SessionEvent::ToolProgress(t) => Some(&t.base.sessionID),
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
        dto::SessionEvent::ModelSelected(sel) => Some(&sel.sessionID),
        // Release 0.5.0: inbox events ride their session id (the payload's
        // `sessionID` is optional on the wire; absent → unroutable).
        dto::SessionEvent::InboxEnqueued(e) => e.sessionID.as_deref(),
    }
}

fn tool_update(
    tool_call_id: &str,
    fields: ToolCallUpdateFields,
) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id.to_string(), fields))
}

/// `tool_update` with an optional `_meta` (the subagent completion meta).
fn tool_update_meta(
    tool_call_id: &str,
    fields: ToolCallUpdateFields,
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> SessionUpdate {
    match meta {
        Some(meta) => SessionUpdate::ToolCallUpdate(
            ToolCallUpdate::new(tool_call_id.to_string(), fields).meta(meta.clone()),
        ),
        None => tool_update(tool_call_id, fields),
    }
}

/// Release 0.3.2: conservative ACP `ToolKind` from an opencode tool name —
/// the obvious families only (file-modifying tools → `Edit`, read-family →
/// `Read`); anything else stays `Other` (unknown tools are not guessed).
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "edit" | "write" | "apply_patch" => ToolKind::Edit,
        "read" | "grep" | "glob" => ToolKind::Read,
        _ => ToolKind::Other,
    }
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
            dto::SessionEvent::ToolProgress(dto::ToolProgress {
                base: dto::ToolRef {
                    sessionID: "ses_x".into(),
                    assistantMessageID: "msg_x".into(),
                    id: "call_x".into(),
                },
                metadata: None,
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

    // ================ Release 0.3.2: declare-before-update ================

    /// (a) The captured LIVE turn (real server stream, fixture) — every tool
    /// call id is DECLARED via `ToolCall` (non-unknown id + non-empty title)
    /// before its first `ToolCallUpdate`. This is the fix for Zed's
    /// "Tool call not found" placeholder cards.
    #[test]
    fn live_turn_introduces_every_call_before_its_first_update() {
        let events = decode_fixture();
        let mut state = MappingState::new();
        let mut introduced: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut declarations = 0usize;
        for ev in &events {
            for u in to_updates(ev, &mut state) {
                match u {
                    SessionUpdate::ToolCall(c) => {
                        declarations += 1;
                        assert!(!c.title.is_empty(), "declared call must carry a title");
                        let id = c.tool_call_id.0.as_ref().to_string();
                        assert!(id.starts_with("call_"), "id must be a real call id, got {id}");
                        assert!(
                            introduced.insert(id),
                            "an id must be declared at most once"
                        );
                    }
                    SessionUpdate::ToolCallUpdate(u) => {
                        let id = u.tool_call_id.0.as_ref().to_string();
                        assert!(
                            introduced.contains(&id),
                            "update for {id} arrived before any tool_call declaration"
                        );
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(declarations, 1, "the live fixture has one tool call");
        assert!(introduced.contains("call_9663d4974690464f98d40e7c"));
        // The declaration carries the tool name as title (input.started arm)
        // and an edit-family kind for `write`.
        let mut state = MappingState::new();
        let all: Vec<SessionUpdate> =
            events.iter().flat_map(|ev| to_updates(ev, &mut state)).collect();
        let SessionUpdate::ToolCall(c) = all.iter().find(|u| matches!(u, SessionUpdate::ToolCall(_))).expect("fixture has a declaration") else {
            panic!("the fixture's first tool emission must be the declaration")
        };
        assert_eq!(c.title, "write");
        assert_eq!(c.kind, acp::ToolKind::Edit);
    }

    /// (b) Out-of-order lifecycle starting at `called` (input.started
    /// skipped): the `called` arm introduces with `InProgress` + the parsed
    /// input, then updates. A later event must NOT re-introduce.
    #[test]
    fn called_first_still_introduces_with_in_progress() {
        let mut state = MappingState::new();
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: tool_ref("ses_x", "msg_x", "call_x"),
            input: serde_json::json!({ "command": "ls" }),
            executed: None,
        });
        let updates = to_updates(&called, &mut state);
        assert_eq!(updates.len(), 2, "declaration + update");
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("first emission must be the initial tool_call")
        };
        assert_eq!(c.tool_call_id.0.as_ref(), "call_x");
        assert_eq!(c.status, acp::ToolCallStatus::InProgress);
        assert_eq!(c.raw_input, Some(serde_json::json!({ "command": "ls" })));
        let SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
            panic!("expected the tool_call update")
        };
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::InProgress));

        // No re-declaration on the next event for the same id.
        let updates = to_updates(&called, &mut state);
        assert_eq!(updates.len(), 1);
        assert!(matches!(&updates[0], SessionUpdate::ToolCallUpdate(_)));
    }

    /// (c) No input events at all: success- and failure-first sequences
    /// introduce with the TERMINAL status directly.
    #[test]
    fn terminal_first_introduces_with_terminal_status() {
        let mut state = MappingState::new();
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_x", "msg_x", "call_ok"),
            content: None,
            metadata: None,
            executed: None,
        });
        let updates = to_updates(&success, &mut state);
        assert_eq!(updates.len(), 2);
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("success-first must declare the call")
        };
        assert_eq!(c.tool_call_id.0.as_ref(), "call_ok");
        assert_eq!(c.status, acp::ToolCallStatus::Completed, "terminal status, not pending");
        assert_eq!(c.title, "call_ok", "no title anywhere → the id fallback");

        let mut state = MappingState::new();
        let failed = dto::SessionEvent::ToolFailed(dto::ToolRefError {
            base: tool_ref("ses_x", "msg_x", "call_err"),
            error: dto::StructuredError {
                kind: Some("tool.execution".into()),
                message: Some("boom".into()),
            },
        });
        let updates = to_updates(&failed, &mut state);
        assert_eq!(updates.len(), 2);
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("failure-first must declare the call")
        };
        assert_eq!(c.status, acp::ToolCallStatus::Failed);
        let SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
            panic!("expected the tool_call update")
        };
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::Failed));
    }

    /// The kind mapping is deliberately conservative: write/edit/apply_patch
    /// → Edit, read/grep/glob → Read, everything else → Other.
    #[test]
    fn tool_kind_mapping_is_conservative() {
        let mut state = MappingState::new();
        for (name, kind) in [
            ("write", acp::ToolKind::Edit),
            ("edit", acp::ToolKind::Edit),
            ("apply_patch", acp::ToolKind::Edit),
            ("read", acp::ToolKind::Read),
            ("grep", acp::ToolKind::Read),
            ("glob", acp::ToolKind::Read),
            ("bash", acp::ToolKind::Other),
            ("weird_custom_tool", acp::ToolKind::Other),
        ] {
            let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
                base: tool_ref("ses_x", "msg_x", &format!("call_{name}")),
                name: name.into(),
            });
            let updates = to_updates(&started, &mut state);
            let SessionUpdate::ToolCall(c) = &updates[0] else {
                panic!("expected the initial tool_call")
            };
            assert_eq!(c.kind, kind, "kind for {name}");
        }
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
        // Release 0.3.2: success-first introductions declare the call with
        // its terminal status, then update.
        assert_eq!(updates.len(), 2);
        let acp::SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("expected the initial tool_call declaration")
        };
        assert_eq!(c.tool_call_id.0.as_ref(), "call_45f029f2a7754b23a4c05df2");
        assert_eq!(c.status, acp::ToolCallStatus::Completed);
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
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
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
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
                // Release 0.3.2: [ToolCall, ToolCallUpdate] for these
                // success-first sequences.
                assert!(matches!(&updates[0], acp::SessionUpdate::ToolCall(_)));
                let acp::SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
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

// ================== Release 0.6.0: native subagent sessions ==================

    fn child_track(id: &str, title: &str) -> ChildTrack {
        let mut t = ChildTrack::new(id, title);
        t.parent_call = Some(format!("call_task_{id}"));
        t
    }

    fn subagent_info(
        meta: &serde_json::Map<String, serde_json::Value>,
    ) -> serde_json::Map<String, serde_json::Value> {
        meta.get(SUBAGENT_META_KEY)
            .and_then(|v| v.as_object())
            .cloned()
            .expect("subagent_session_info present")
    }

    /// Child tool events map with PLAIN ids (no `${child}:` namespace) and
    /// count one transcript entry per call; the parent card gets a
    /// title-prefixed content line per tool boundary.
    #[test]
    fn child_events_map_to_plain_ids_and_count_entries() {
        let mut state = MappingState::new();
        let mut track = child_track("ses_child_1", "Explore the repo");

        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_child_1", "msg_c1", "call_c1"),
            name: "grep".into(),
        });
        let (updates, card) = to_child_updates(&started, &mut state, &mut track);
        assert_eq!(updates.len(), 2, "declaration + update");
        let acp::SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("expected the initial tool_call declaration")
        };
        // PLAIN ids — the child session is addressed by the notification.
        assert_eq!(c.tool_call_id.0.as_ref(), "call_c1");
        assert_eq!(c.title, "grep");
        assert_eq!(c.kind, acp::ToolKind::Read);
        let (call_id, content) = card.expect("input.started is a card boundary");
        assert_eq!(call_id, "call_task_ses_child_1");
        let acp::ToolCallContent::Content(block) = &content[0] else {
            panic!("card line is a text block")
        };
        let acp::ContentBlock::Text(t) = &block.content else { panic!("text") };
        assert_eq!(t.text, "Explore the repo: grep");
        assert_eq!(track.entries, 1, "one tool call entry");

        // called → richer label (name + first string arg).
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: tool_ref("ses_child_1", "msg_c1", "call_c1"),
            input: serde_json::json!({ "pattern": "**/*.rs" }),
            executed: None,
        });
        let (updates, card) = to_child_updates(&called, &mut state, &mut track);
        assert_eq!(updates.len(), 1, "already declared — update only");
        let acp::SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("called maps to an update")
        };
        assert_eq!(u.fields.status, Some(acp::ToolCallStatus::InProgress));
        let (_, content) = card.expect("called is a card boundary");
        let acp::ToolCallContent::Content(block) = content.last().expect("newest line") else {
            panic!()
        };
        let acp::ContentBlock::Text(t) = &block.content else { panic!() };
        assert_eq!(t.text, "Explore the repo: grep **/*.rs");
        assert_eq!(track.entries, 1, "same call id counts once");

        // A second call: new entry + the line list keeps both (cap 6).
        let started2 = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_child_1", "msg_c2", "call_c2"),
            name: "read".into(),
        });
        let _ = to_child_updates(&started2, &mut state, &mut track);
        assert_eq!(track.entries, 2);
        assert_eq!(track.card_content().len(), 3, "started + called + started2 lines");

        // The card line list wraps at MAX_CARD_LINES.
        for i in 0..8 {
            let ev = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
                base: tool_ref("ses_child_1", "msg_c2", &format!("call_more_{i}")),
                name: "bash".into(),
            });
            let _ = to_child_updates(&ev, &mut state, &mut track);
        }
        assert_eq!(track.card_content().len(), MAX_CARD_LINES);
    }

    /// The child's user prompt (inbox, user item) → user chunk addressed to
    /// the child session; text/reasoning chunks count per assistant message.
    #[test]
    fn child_prompt_and_text_chunks_address_the_child_and_count_entries() {
        let mut state = MappingState::new();
        let mut track = child_track("ses_child_1", "t");

        let inbox = dto::SessionEvent::InboxEnqueued(dto::InboxEnqueued {
            inboxID: "msg_inbox_child".into(),
            sessionID: Some("ses_child_1".into()),
            item: Some(dto::InboxEventItem {
                kind: Some("user".into()),
                payload: Some(dto::InboxPayload {
                    text: Some(
                        "You are a subagent spawned by another session.\nDo the thing".into(),
                    ),
                    files: None,
                }),
            }),
        });
        let (updates, card) = to_child_updates(&inbox, &mut state, &mut track);
        assert!(card.is_none(), "inbox is not a card boundary");
        assert_eq!(updates.len(), 1);
        let acp::SessionUpdate::UserMessageChunk(c) = &updates[0] else {
            panic!("expected user chunk")
        };
        assert_eq!(c.message_id.as_ref().map(|m| m.0.as_ref()), Some("msg_inbox_child"));
        assert_eq!(track.entries, 1, "the user prompt is one entry");

        // Many deltas of the SAME assistant message count once.
        for _ in 0..3 {
            let ev = dto::SessionEvent::ReasoningDelta(dto::TextDelta {
                base: dto::OrdinalRef {
                    sessionID: "ses_child_1".into(),
                    assistantMessageID: "msg_child_a".into(),
                    ordinal: Some(0),
                },
                delta: "think".into(),
            });
            let (updates, card) = to_child_updates(&ev, &mut state, &mut track);
            assert!(card.is_none());
            assert!(matches!(&updates[0], acp::SessionUpdate::AgentThoughtChunk(_)));
        }
        assert_eq!(track.entries, 2, "one entry per distinct assistant message");
        let text = dto::SessionEvent::TextDelta(dto::TextDelta {
            base: dto::OrdinalRef {
                sessionID: "ses_child_1".into(),
                assistantMessageID: "msg_child_b".into(),
                ordinal: Some(0),
            },
            delta: "answer".into(),
        });
        let (updates, _) = to_child_updates(&text, &mut state, &mut track);
        assert!(matches!(&updates[0], acp::SessionUpdate::AgentMessageChunk(_)));
        assert_eq!(track.entries, 3);
    }

    /// The replayed-message entry math mirrors the live counting.
    #[test]
    fn count_entry_records_matches_live_slice_math() {
        let record = |kind: &str, tools: usize| crate::dto::MessageRecord {
            kind: kind.into(),
            id: "msg_x".into(),
            files: None,
            text: None,
            agent: None,
            model: None,
            content: Some(
                (0..tools)
                    .map(|i| crate::dto::Part::Tool {
                        id: format!("call_{i}"),
                        name: "read".into(),
                        executed: Some(true),
                        state: crate::dto::ToolState::Completed {
                            input: serde_json::Value::Null,
                            content: None,
                            metadata: None,
                        },
                        time: None,
                    })
                    .collect(),
            ),
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        };
        // 1 user + 2 assistants (one with 2 tool parts) = 1 + 2 + 2 = 5.
        let records = vec![
            record("user", 0),
            record("assistant", 2),
            record("assistant", 0),
            record("idle", 0),
        ];
        assert_eq!(count_entry_records(&records), 5);
    }

    /// Pairing via the `session.created` FIFO queue: the spawner call is
    /// registered from its input events; the child's `session.created` pairs
    /// with the OLDEST unpaired call and produces the open announce meta.
    #[test]
    fn tracker_pairs_created_child_with_oldest_spawner_call() {
        let mut tracker = SubagentTracker::default();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_parent", "msg_p", "call_task_1"),
            name: "subagent".into(),
        });
        let out = track_event(&mut tracker, &started, "ses_parent");
        assert!(out.announce.is_none() && out.complete.is_none());
        // A second spawner call inside the SAME turn (parallel subagents).
        let started2 = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_parent", "msg_p", "call_task_2"),
            name: "task".into(),
        });
        let _ = track_event(&mut tracker, &started2, "ses_parent");

        let created = dto::SessionEvent::SessionCreated(dto::SessionCreated {
            sessionID: Some("ses_child_a".into()),
            parentID: Some("ses_parent".into()),
            title: Some("Child A".into()),
            ..Default::default()
        });
        let out = track_event(&mut tracker, &created, "ses_parent");
        let (child_id, call_id, meta) = out.announce.expect("first child announces");
        assert_eq!(child_id, "ses_child_a");
        assert_eq!(call_id, "call_task_1", "oldest unpaired call wins");
        let info = subagent_info(&meta);
        assert_eq!(info.get("session_id").and_then(|v| v.as_str()), Some("ses_child_a"));
        assert_eq!(info.get("message_start_index"), Some(&serde_json::json!(0)));
        assert_eq!(info.get("message_end_index"), Some(&serde_json::Value::Null));

        let created2 = dto::SessionEvent::SessionCreated(dto::SessionCreated {
            sessionID: Some("ses_child_b".into()),
            parentID: Some("ses_parent".into()),
            title: Some("Child B".into()),
            ..Default::default()
        });
        let out = track_event(&mut tracker, &created2, "ses_parent");
        let (_, call_id, _) = out.announce.expect("second child announces");
        assert_eq!(call_id, "call_task_2");
        assert_eq!(tracker.call_child("call_task_1"), Some("ses_child_a"));
        assert_eq!(tracker.call_child("call_task_2"), Some("ses_child_b"));

        // Duplicate session.created for an already-paired child: no re-announce.
        let out = track_event(&mut tracker, &created2, "ses_parent");
        assert!(out.announce.is_none() && out.complete.is_none());
    }

    /// Continuation: the spawner call's input carries `sessionID` — paired
    /// DIRECTLY with no `session.created`; the announce re-slices from the
    /// accumulated entry count.
    #[test]
    fn tracker_pairs_continuation_via_input_session_id() {
        let mut tracker = SubagentTracker::default();
        // Turn 1: created-pairing.
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_parent", "msg_p", "call_task_1"),
            name: "subagent".into(),
        });
        let _ = track_event(&mut tracker, &started, "ses_parent");
        let created = dto::SessionEvent::SessionCreated(dto::SessionCreated {
            sessionID: Some("ses_child".into()),
            parentID: Some("ses_parent".into()),
            title: Some("Child".into()),
            ..Default::default()
        });
        let _ = track_event(&mut tracker, &created, "ses_parent");
        // Simulate a full turn-1 child stream: prompt + one tool call.
        tracker.child_mut("ses_child").unwrap().record_message("msg_in1");
        tracker.child_mut("ses_child").unwrap().record_tool("call_c1");
        assert_eq!(tracker.child("ses_child").unwrap().entries, 2);

        // Turn 2 continuation: called carries sessionID; NO session.created.
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: tool_ref("ses_parent", "msg_p2", "call_task_2"),
            input: serde_json::json!({
                "agent": "explorer",
                "prompt": "continue",
                "sessionID": "ses_child"
            }),
            executed: None,
        });
        let out = track_event(&mut tracker, &called, "ses_parent");
        let (child_id, call_id, meta) = out.announce.expect("continuation announces");
        assert_eq!(child_id, "ses_child");
        assert_eq!(call_id, "call_task_2");
        let info = subagent_info(&meta);
        assert_eq!(
            info.get("message_start_index"),
            Some(&serde_json::json!(2)),
            "the re-slice starts at the accumulated entry count"
        );
        // The continuation call completes: end = entries INCLUDING the new
        // turn's traffic.
        tracker.child_mut("ses_child").unwrap().record_tool("call_c2");
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_parent", "msg_p2", "call_task_2"),
            content: Some(vec![dto::ToolContent::Text { text: "done".into() }]),
            metadata: None,
            executed: None,
        });
        let out = track_event(&mut tracker, &success, "ses_parent");
        let (_, meta) = out.complete.expect("completion meta");
        let info = subagent_info(&meta);
        assert_eq!(info.get("message_start_index"), Some(&serde_json::json!(2)));
        assert_eq!(info.get("message_end_index"), Some(&serde_json::json!(3)));
    }

    /// The progress-metadata linkage (`metadata.sessionID` on the parent's
    /// spawner call) pairs DIRECTLY — the fallback when the queue is empty
    /// or the pairing signal arrives late.
    #[test]
    fn tracker_pairs_via_progress_metadata() {
        let mut tracker = SubagentTracker::default();
        // No input.started observed (e.g. mid-turn attach): the progress
        // linkage still pairs.
        let progress = dto::SessionEvent::ToolProgress(dto::ToolProgress {
            base: tool_ref("ses_parent", "msg_p", "call_task_1"),
            metadata: Some(dto::ToolProgressMeta {
                sessionID: Some("ses_child".into()),
                status: Some("running".into()),
            }),
        });
        let out = track_event(&mut tracker, &progress, "ses_parent");
        let (child_id, call_id, meta) = out.announce.expect("progress announces");
        assert_eq!((child_id.as_str(), call_id.as_str()), ("ses_child", "call_task_1"));
        assert!(meta.contains_key(SUBAGENT_META_KEY));
        // Idempotent: the same linkage again does not re-announce.
        let out = track_event(&mut tracker, &progress, "ses_parent");
        assert!(out.announce.is_none());
        // A child's OWN tool progress (metadata {}) or other sessions'
        // events never pair.
        let child_progress = dto::SessionEvent::ToolProgress(dto::ToolProgress {
            base: tool_ref("ses_child", "msg_c", "call_c1"),
            metadata: Some(dto::ToolProgressMeta::default()),
        });
        assert!(track_event(&mut tracker, &child_progress, "ses_parent").announce.is_none());
        let foreign = dto::SessionEvent::ToolProgress(dto::ToolProgress {
            base: tool_ref("ses_other", "msg_o", "call_o"),
            metadata: Some(dto::ToolProgressMeta {
                sessionID: Some("ses_child".into()),
                status: Some("running".into()),
            }),
        });
        assert!(track_event(&mut tracker, &foreign, "ses_parent").announce.is_none());
    }

    /// A spawner call that dies without a child is retired — a later child
    /// creation must not pair with the dead call.
    #[test]
    fn tracker_retires_unpaired_calls_on_terminal() {
        let mut tracker = SubagentTracker::default();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_parent", "msg_p", "call_dead"),
            name: "subagent".into(),
        });
        let _ = track_event(&mut tracker, &started, "ses_parent");
        let failed = dto::SessionEvent::ToolFailed(dto::ToolRefError {
            base: tool_ref("ses_parent", "msg_p", "call_dead"),
            error: dto::StructuredError {
                kind: Some("tool.execution".into()),
                message: Some("child spawn failed".into()),
            },
        });
        let out = track_event(&mut tracker, &failed, "ses_parent");
        assert!(out.complete.is_none(), "unpaired calls get no completion meta");
        let created = dto::SessionEvent::SessionCreated(dto::SessionCreated {
            sessionID: Some("ses_child".into()),
            parentID: Some("ses_parent".into()),
            title: Some("Child".into()),
            ..Default::default()
        });
        let out = track_event(&mut tracker, &created, "ses_parent");
        assert!(
            out.announce.is_none(),
            "the dead call must not be paired (no card to announce)"
        );
        assert!(tracker.call_child("call_dead").is_none());
    }

    /// The full 0.6.0 subagent fixture (live 2.0.21 capture): pairing via
    /// session.created, meta announce, child-id content, completion meta.
    fn decode_native_fixture() -> Vec<dto::SessionEvent> {
        let raw = include_str!("../../tests/fixtures/subagent-native.sse.jsonl");
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

    /// Drive the captured live turn through the native machinery: the
    /// pairing, the announce, child-id updates, card lines, and the terminal
    /// completion meta all fall out of the real event order.
    #[test]
    fn native_fixture_drives_pairing_announce_and_completion() {
        let events = decode_native_fixture();
        let mut tracker = SubagentTracker::default();
        let parent = "ses_efe9caf88ffeMl8ZrxaWo8sgoR";
        let child = "ses_efe9c6aceffeAbuU5CkcIR1kH0";
        let mut child_state = MappingState::new();
        let mut announces = Vec::new();
        let mut completions = Vec::new();
        let mut cards = 0usize;
        let mut child_updates = 0usize;
        for ev in &events {
            let out = track_event(&mut tracker, ev, parent);
            if let Some((_, call_id, meta)) = out.announce {
                announces.push((call_id, meta));
            }
            if let Some((call_id, meta)) = out.complete {
                completions.push((call_id, meta));
            }
            let sid = event_session_id(ev);
            if sid == Some(child) {
                if let Some(track) = tracker.child_mut(child) {
                    // Unpaired children buffer; paired ones map.
                    if track.parent_call.is_some() {
                        let (updates, card) = to_child_updates(ev, &mut child_state, track);
                        child_updates += updates.len();
                        if card.is_some() {
                            cards += 1;
                        }
                    } else {
                        track.buffer(ev.clone());
                    }
                }
            }
        }
        // Two spawner turns (fresh + continuation) → two announces, two
        // completion metas, all on the same child.
        assert_eq!(announces.len(), 2, "fresh + continuation announce");
        assert_eq!(completions.len(), 2);
        assert!(announces[0].0.starts_with("call_"), "announce rides the spawner call");
        assert!(completions[0].0.starts_with("call_"));
        let first = subagent_info(&announces[0].1);
        assert_eq!(first.get("session_id").and_then(|v| v.as_str()), Some(child));
        assert_eq!(first.get("message_start_index"), Some(&serde_json::json!(0)));
        let second = subagent_info(&announces[1].1);
        assert_eq!(second.get("session_id").and_then(|v| v.as_str()), Some(child));
        let second_start = second.get("message_start_index").and_then(|v| v.as_u64()).unwrap();
        assert!(second_start > 0, "continuation re-slices from the accumulated count");
        let end = subagent_info(&completions[1].1)["message_end_index"]
            .as_u64()
            .unwrap();
        assert!(end >= second_start, "the closed slice covers the second turn");
        assert!(child_updates > 0, "child events project");
        assert!(cards > 0, "card lines appear on child tool boundaries");
        let track = tracker.child(child).expect("child tracked");
        assert_eq!(track.entries as u64, end, "end index = total child entries");
    }

    /// The full 0.6.0 fixture through the PARENT mapping: the spawner cards
    /// declare with the dispatch `description` as title (not the tool name).
    #[test]
    fn native_fixture_spawner_cards_declare_description_title() {
        let mut state = MappingState::new();
        let declares: Vec<(String, String)> = decode_native_fixture()
            .iter()
            .flat_map(|ev| to_updates(ev, &mut state))
            .filter_map(|u| match u {
                SessionUpdate::ToolCall(c) => {
                    Some((c.tool_call_id.0.as_ref().to_string(), c.title.clone()))
                }
                _ => None,
            })
            .collect();
        // The two spawner calls (fresh + continuation) declare with their
        // descriptions; the child tool calls with their tool names.
        let spawner_titles: Vec<&str> = declares
            .iter()
            .filter(|(id, _)| id.starts_with("call_"))
            .map(|(_, t)| t.as_str())
            .filter(|t| *t != "grep" && *t != "read" && *t != "glob" && *t != "bash" && *t != "ctx_reduce")
            .collect();
        assert_eq!(
            spawner_titles,
            vec!["Read and report README.md", "Check for txt files"],
            "spawner declarations carry the dispatch descriptions"
        );
    }

    /// Release 0.7.1: the spawner declaration is deferred from
    /// `input.started` to `input.ended` (the input must arrive before the
    /// description title exists), with Pending + the raw JSON input on the
    /// declaration — and no tool-name flash beforehand.
    #[test]
    fn spawner_declares_description_at_input_ended() {
        let mut state = MappingState::new();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            name: "subagent".into(),
        });
        // input.started emits NOTHING for a spawner (non-spawner tools
        // declare here — covered by the other mapping tests).
        assert_eq!(to_updates(&started, &mut state).len(), 0);

        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            text: "{\"agent\":\"explorer\",\"description\":\"Read and report README.md\",\"prompt\":\"go\"}"
                .to_string(),
        });
        let updates = to_updates(&ended, &mut state);
        assert_eq!(updates.len(), 2, "declaration + raw_input update");
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("first emission must be the declaration")
        };
        assert_eq!(c.tool_call_id.0.as_ref(), "call_spawn");
        assert_eq!(c.status, acp::ToolCallStatus::Pending);
        assert_eq!(c.title, "Read and report README.md");
        // The raw JSON input rides the declaration (the input.ended arm
        // passes the raw text through verbatim).
        assert!(c.raw_input.as_ref().is_some());
        let SessionUpdate::ToolCallUpdate(u) = &updates[1] else {
            panic!("second emission must be the raw_input update")
        };
        assert!(u.fields.raw_input.is_some(), "raw input update follows");

        // called: in-progress update, no re-declaration.
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            input: serde_json::json!({"agent": "explorer", "description": "Read and report README.md"}),
            executed: None,
        });
        let updates = to_updates(&called, &mut state);
        assert_eq!(updates.len(), 1);
        assert!(matches!(&updates[0], SessionUpdate::ToolCallUpdate(u)
            if u.fields.status == Some(acp::ToolCallStatus::InProgress)));
    }

    /// The `task` alias gets the same treatment; long descriptions are
    /// truncated to 80 chars + "…" at char boundaries (never a mid-char
    /// split on non-ASCII input).
    #[test]
    fn spawner_task_alias_truncates_long_description_char_boundary_safe() {
        let mut state = MappingState::new();
        let long_desc = "x".repeat(100);
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_task_1"),
            name: "task".into(),
        });
        let _ = to_updates(&started, &mut state);
        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: tool_ref("ses_p", "msg_p", "call_task_1"),
            text: format!("{{\"description\":\"{long_desc}\"}}"),
        });
        let updates = to_updates(&ended, &mut state);
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("declaration expected")
        };
        assert_eq!(c.title.chars().count(), 81, "80 chars + the ellipsis");
        assert!(c.title.ends_with('…'));

        // Multi-byte (Chinese) description: byte slicing would split a char;
        // the char-boundary truncation must not.
        let mut state = MappingState::new();
        let chinese = "描述".repeat(60); // 120 chars, 240 bytes
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_task_2"),
            name: "task".into(),
        });
        let _ = to_updates(&started, &mut state);
        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: tool_ref("ses_p", "msg_p", "call_task_2"),
            text: format!("{{\"description\":\"{chinese}\"}}"),
        });
        let updates = to_updates(&ended, &mut state);
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("declaration expected")
        };
        assert_eq!(c.title.chars().count(), 81, "80 chars + the ellipsis");
        assert!(c.title.is_char_boundary(c.title.len()), "no mid-char split");
    }

    /// Fallback chain: a missing, empty, or whitespace-only `description`
    /// falls back to the tool name ("subagent"/"task" — the pre-0.7.1
    /// title).
    #[test]
    fn spawner_blank_description_falls_back_to_tool_name() {
        for (text, name, expected) in [
            ("{\"agent\":\"explorer\",\"prompt\":\"go\"}", "subagent", "subagent"),
            (
                "{\"agent\":\"explorer\",\"description\":\"\",\"prompt\":\"go\"}",
                "subagent",
                "subagent",
            ),
            (
                "{\"agent\":\"explorer\",\"description\":\"   \",\"prompt\":\"go\"}",
                "task",
                "task",
            ),
        ] {
            let mut state = MappingState::new();
            let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
                base: tool_ref("ses_p", "msg_p", "call_z"),
                name: name.into(),
            });
            let _ = to_updates(&started, &mut state);
            let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
                base: tool_ref("ses_p", "msg_p", "call_z"),
                text: text.to_string(),
            });
            let updates = to_updates(&ended, &mut state);
            let SessionUpdate::ToolCall(c) = &updates[0] else {
                panic!("declaration expected for {text}")
            };
            assert_eq!(c.title, expected, "fallback for {text}");
        }
    }

    /// `input.ended` skipped entirely: `called` introduces with the same
    /// description title (the input is on hand there).
    #[test]
    fn spawner_called_introduces_description_when_ended_missed() {
        let mut state = MappingState::new();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            name: "subagent".into(),
        });
        let _ = to_updates(&started, &mut state);
        let called = dto::SessionEvent::ToolCalled(dto::ToolCalled {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            input: serde_json::json!({"agent": "explorer", "description": "List the repo"}),
            executed: None,
        });
        let updates = to_updates(&called, &mut state);
        let SessionUpdate::ToolCall(c) = &updates[0] else {
            panic!("called-first must declare the spawner card")
        };
        assert_eq!(c.title, "List the repo");
        assert_eq!(c.status, acp::ToolCallStatus::InProgress);
    }

    /// A spawner's TERMINAL update stays consistent with the declared card
    /// title: when the server metadata carries a `title`, a spawner uses
    /// the description-derived title instead; non-spawner tools keep the
    /// server title (e.g. the edited file's name).
    #[test]
    fn spawner_terminal_title_syncs_to_description_when_metadata_title_present() {
        // Spawner: metadata.title present → replaced by the description.
        let mut state = MappingState::new();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            name: "subagent".into(),
        });
        let _ = to_updates(&started, &mut state);
        let ended = dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            text: "{\"description\":\"Read the repo\"}".to_string(),
        });
        let _ = to_updates(&ended, &mut state);
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_p", "msg_p", "call_spawn"),
            content: None,
            metadata: Some(dto::ToolMetadata {
                title: Some("server-side-title".into()),
                ..dto::ToolMetadata::default()
            }),
            executed: None,
        });
        let updates = to_updates(&success, &mut state);
        let SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("completion update expected")
        };
        assert_eq!(u.fields.title.as_deref(), Some("Read the repo"));

        // Non-spawner: metadata.title passes through untouched.
        let mut state = MappingState::new();
        let started = dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: tool_ref("ses_p", "msg_p", "call_edit"),
            name: "edit".into(),
        });
        let _ = to_updates(&started, &mut state);
        let success = dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
            base: tool_ref("ses_p", "msg_p", "call_edit"),
            content: None,
            metadata: Some(dto::ToolMetadata {
                title: Some("notes.txt".into()),
                ..dto::ToolMetadata::default()
            }),
            executed: None,
        });
        let updates = to_updates(&success, &mut state);
        let SessionUpdate::ToolCallUpdate(u) = &updates[0] else {
            panic!("completion update expected")
        };
        assert_eq!(u.fields.title.as_deref(), Some("notes.txt"));
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
