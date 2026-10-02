//! ACP agent assembly (Wave 1 skeleton).
//!
//! Protocol surface only — the stdio main loop and the `session/list` +
//! `session/resume` unstable surface arrive in Wave 2. The ACP ↔ opencode
//! wiring runs through the [`OpenCodeBackend`] trait (implemented by the
//! HTTP/SSE lane), which keeps this module fully mock-testable.
//!
//! Concurrency note: the prompt turn is **spawned** (`ConnectionTo::spawn`)
//! rather than awaited inline, so the dispatch loop stays free to process
//! `session/cancel` while a turn streams. Cancel sets a per-session flag the
//! turn loop polls between events; the loop then calls
//! [`OpenCodeBackend::interrupt`] and answers `StopReason::Cancelled`
//! (ACP v1 requirement for `session/cancel`).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1 as acp;
use agent_client_protocol::{Agent, Client, ConnectTo, ConnectionTo, Error as AcpError,
    Responder, on_receive_notification, on_receive_request};
use futures_util::{Stream, StreamExt};

use crate::dto;

use super::{replay, updates};

/// Convenience aliases for backend implementors.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type EventStream = Pin<Box<dyn Stream<Item = dto::SessionEvent> + Send>>;

/// The opencode-facing capabilities Lane B needs (implemented by the HTTP/SSE
/// lane in Wave 2). The event stream is the session-tagged SSE stream:
/// implementors MUST deliver every `session.*` event emitted during a turn —
/// subscribe before calling [`prompt`](OpenCodeBackend::prompt) when possible,
/// and treat this as a continuous stream (events before the prompt call for
/// the same turn still appear once the prompt begins).
pub trait OpenCodeBackend: Send + Sync + 'static {
    /// Create a new opencode session; returns its `ses_*` id.
    fn create_session(&self, cwd: &str) -> BoxFuture<'_, Result<String, anyhow::Error>>;

    /// Enqueue a prompt (turn start). Returns once enqueued — the turn
    /// itself plays out on the event stream.
    fn prompt(&self, session_id: &str, text: &str) -> BoxFuture<'_, Result<(), anyhow::Error>>;

    /// Interrupt a running turn (`session/cancel`).
    fn interrupt(&self, session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>>;

    /// Persisted messages of a session, newest-first (as the wire hands them).
    fn messages(
        &self,
        session_id: &str,
    ) -> BoxFuture<'_, Result<Vec<dto::MessageRecord>, anyhow::Error>>;

    /// The session-tagged event stream (see trait docs for the contract).
    fn event_stream(&self, session_id: &str) -> BoxFuture<'_, Result<EventStream, anyhow::Error>>;
}

/// The ACP agent service: state + handler wiring.
pub struct AgentService {
    backend: Arc<dyn OpenCodeBackend>,
    sessions: Mutex<HashMap<acp::SessionId, Arc<SessionEntry>>>,
}

struct SessionEntry {
    /// Set by `session/cancel`; polled by the turn loop between events.
    cancel: AtomicBool,
}

impl AgentService {
    pub fn new(backend: Arc<dyn OpenCodeBackend>) -> Self {
        Self { backend, sessions: Mutex::new(HashMap::new()) }
    }

    /// Run the agent over the given transport until the connection closes.
    pub async fn serve(
        self: Arc<Self>,
        transport: impl ConnectTo<Agent> + 'static,
    ) -> Result<(), AcpError> {
        Agent.builder()
            .name("opencode")
            // ---------- initialize ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::InitializeRequest, responder, _cx| {
                        svc.initialize(req, responder)
                    }
                },
                on_receive_request!(),
            )
            // ---------- newSession ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::NewSessionRequest, responder, _cx| {
                        svc.new_session(req, responder).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- loadSession ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::LoadSessionRequest, responder, cx| {
                        svc.load_session(req, responder, cx).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- prompt ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::PromptRequest, responder, cx| {
                        // Spawn the turn: the dispatch loop must stay free to
                        // process `session/cancel` while the turn streams.
                        let svc = Arc::clone(&svc);
                        let cx_task = cx.clone();
                        cx.spawn(async move { svc.run_prompt(req, responder, cx_task).await })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            // ---------- cancel (notification) ----------
            .on_receive_notification(
                {
                    let svc = Arc::clone(&self);
                    async move |notif: acp::CancelNotification, cx| {
                        svc.cancel(notif.session_id, cx).await
                    }
                },
                on_receive_notification!(),
            )
            .connect_to(transport)
            .await
    }

    // ============================================================
    // handlers
    // ============================================================

    fn initialize(
        &self,
        req: acp::InitializeRequest,
        responder: Responder<acp::InitializeResponse>,
    ) -> Result<(), AcpError> {
        let caps = acp::AgentCapabilities::new().load_session(true);
        let info = acp::Implementation::new(
            "opencode-acp-bridge",
            concat!("opencode ", env!("CARGO_PKG_VERSION")),
        );
        responder.respond(
            acp::InitializeResponse::new(req.protocol_version)
                .agent_capabilities(caps)
                .agent_info(info),
        )
    }

    async fn new_session(
        &self,
        req: acp::NewSessionRequest,
        responder: Responder<acp::NewSessionResponse>,
    ) -> Result<(), AcpError> {
        let cwd = req.cwd;
        if !cwd.is_absolute() {
            return responder.respond_with_error(
                AcpError::invalid_params().data(serde_json::json!({
                    "message": "newSession.cwd must be an absolute path"
                })),
            );
        }
        let cwd = cwd.to_string_lossy().to_string();
        let session_id = match self.backend.create_session(&cwd).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, "backend create_session failed");
                return responder.respond_with_internal_error(format!(
                    "opencode session creation failed: {e}"
                ));
            }
        };
        self.sessions.lock().expect("sessions lock").insert(
            acp::SessionId::from(session_id.clone()),
            Arc::new(SessionEntry { cancel: AtomicBool::new(false) }),
        );
        tracing::info!(session_id, "ACP newSession -> opencode session");
        responder.respond(acp::NewSessionResponse::new(session_id))
    }

    async fn load_session(
        &self,
        req: acp::LoadSessionRequest,
        responder: Responder<acp::LoadSessionResponse>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        // ACP hard contract: replay the full transcript BEFORE responding.
        let records = match self.backend.messages(&req.session_id.0).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, session = %req.session_id, "load: messages fetch failed");
                return responder.respond_with_internal_error(format!(
                    "failed to load session messages: {e}"
                ));
            }
        };
        for update in replay::replay_updates(&records) {
            cx.send_notification(acp::SessionNotification::new(
                req.session_id.clone(),
                update,
            ))?;
        }
        // Refresh state so a subsequent prompt on this session works.
        self.sessions.lock().expect("sessions lock").insert(
            req.session_id.clone(),
            Arc::new(SessionEntry { cancel: AtomicBool::new(false) }),
        );
        responder.respond(acp::LoadSessionResponse::new())
    }

    /// The ongoing turn: forwards mapped updates, answers the prompt at
    /// execution end. Runs inside a spawned task (see module docs).
    async fn run_prompt(
        self: Arc<Self>,
        req: acp::PromptRequest,
        responder: Responder<acp::PromptResponse>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        let Some(entry) =
            self.sessions.lock().expect("sessions lock").get(&req.session_id).cloned()
        else {
            return responder.respond_with_error(
                AcpError::invalid_params().data(serde_json::json!({
                    "message": "unknown session (call session/new or session/load first)"
                })),
            );
        };

        // Only plain text prompts are supported in Wave 1.
        let mut text = String::new();
        for block in &req.prompt {
            match block {
                acp::ContentBlock::Text(t) => text.push_str(&t.text),
                other => {
                    return responder.respond_with_error(
                        AcpError::invalid_params().data(serde_json::json!({
                            "message": format!("unsupported prompt content block: {other:?}")
                        })),
                    );
                }
            }
        }
        if text.trim().is_empty() {
            return responder.respond_with_error(AcpError::invalid_params());
        }

        // A cancel that raced in before the turn started wins immediately.
        if entry.cancel.load(Ordering::Acquire) {
            return responder.respond(acp::PromptResponse::new(acp::StopReason::Cancelled));
        }

        let backend = Arc::clone(&self.backend);
        if let Err(e) = backend.prompt(&req.session_id.0, &text).await {
            tracing::error!(error = %e, "backend prompt failed");
            return responder.respond_with_internal_error(format!("prompt failed: {e}"));
        }
        let mut stream = match backend.event_stream(&req.session_id.0).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "backend event_stream failed");
                return responder.respond_with_internal_error(format!(
                    "failed to subscribe to events: {e}"
                ));
            }
        };

        let mut state = updates::MappingState::new();
        while let Some(event) = stream.next().await {
            // The SSE stream is session-tagged; drop other sessions' traffic.
            if let Some(sid) = updates::event_session_id(&event) {
                if sid != req.session_id.0.as_ref() {
                    continue;
                }
            }

            // session/cancel won? End the turn (the interrupt call itself was
            // already made by the cancel handler).
            if entry.cancel.load(Ordering::Acquire) {
                tracing::info!(session = %req.session_id, "turn cancelled via session/cancel");
                return responder.respond(acp::PromptResponse::new(acp::StopReason::Cancelled));
            }

            match updates::stop_update(&event) {
                Some(updates::TurnEnd::EndTurn) => {
                    tracing::info!(session = %req.session_id, "turn ended (end_turn)");
                    return responder.respond(acp::PromptResponse::new(
                        acp::StopReason::EndTurn,
                    ));
                }
                Some(updates::TurnEnd::Error { message }) => {
                    // ACP v1 has no error stop reason — fail the request.
                    // Note: `ExecutionFailed` itself maps to no update, so
                    // everything the client needs to see has already been sent.
                    let msg = message.unwrap_or_else(|| "execution failed".to_string());
                    tracing::info!(session = %req.session_id, error = %msg, "turn failed");
                    return responder.respond_with_internal_error(msg);
                }
                None => {
                    for update in updates::to_updates(&event, &mut state) {
                        cx.send_notification(acp::SessionNotification::new(
                            req.session_id.clone(),
                            update,
                        ))?;
                    }
                }
            }
        }

        // Stream ended without a terminal event: connection lost or the
        // backend did not emit execution.succeeded/failed.
        tracing::warn!(session = %req.session_id, "event stream ended without a terminal event");
        responder.respond_with_internal_error(
            "event stream ended before the execution completed".to_string(),
        )
    }

    async fn cancel(
        &self,
        session_id: acp::SessionId,
        _cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        let entry = self.sessions.lock().expect("sessions lock").get(&session_id).cloned();
        let Some(entry) = entry else {
            return Ok(());
        };
        entry.cancel.store(true, Ordering::Release);
        tracing::info!(session = %session_id, "session/cancel received");
        // Interrupt opencode right away: the turn loop may be blocked on the
        // event stream and never notice the flag on its own (opencode emits a
        // terminal event after the interrupt, which the loop then turns into
        // the Cancelled response). Best-effort; failures are logged.
        if let Err(e) = self.backend.interrupt(&session_id.0).await {
            tracing::warn!(error = %e, session = %session_id, "interrupt call failed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::schema::v1::{
        CancelNotification, ContentBlock, InitializeRequest, LoadSessionRequest, NewSessionRequest,
        PromptRequest, SessionNotification, TextContent,
    };
    use tokio::sync::{mpsc, oneshot};

    // ---------------- mock backend ----------------

    #[derive(Default)]
    struct MockBackend {
        /// Event queue handed out once, on the first `event_stream` call.
        events: Mutex<Option<mpsc::UnboundedReceiver<dto::SessionEvent>>>,
        events_tx: Mutex<Option<mpsc::UnboundedSender<dto::SessionEvent>>>,
        messages_out: Mutex<Option<Vec<dto::MessageRecord>>>,
        interrupted: AtomicBool,
        interrupt_seen: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl MockBackend {
        fn new() -> Arc<Self> {
            let (tx, rx) = mpsc::unbounded_channel();
            Arc::new(Self {
                events: Mutex::new(Some(rx)),
                events_tx: Mutex::new(Some(tx)),
                messages_out: Mutex::new(None),
                interrupted: AtomicBool::new(false),
                interrupt_seen: Mutex::new(None),
            })
        }

        fn push(&self, event: dto::SessionEvent) {
            let tx = self.events_tx.lock().expect("tx lock");
            let Some(tx) = tx.as_ref() else { panic!("backend already consumed") };
            let _ = tx.send(event);
        }

        fn set_messages(&self, records: Vec<dto::MessageRecord>) {
            *self.messages_out.lock().expect("messages lock") = Some(records);
        }

        fn install_interrupt_seen(&self) -> oneshot::Receiver<()> {
            let (tx, rx) = oneshot::channel();
            *self.interrupt_seen.lock().expect("interrupt lock") = Some(tx);
            rx
        }
    }

    impl OpenCodeBackend for MockBackend {
        fn create_session(&self, _cwd: &str) -> BoxFuture<'_, Result<String, anyhow::Error>> {
            Box::pin(async { Ok("ses_mock_1".to_string()) })
        }

        fn prompt(&self, _session_id: &str, _text: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            Box::pin(async { Ok(()) })
        }

        fn interrupt(&self, _session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            let seen = self.interrupt_seen.lock().expect("interrupt lock").take();
            self.interrupted.store(true, Ordering::SeqCst);
            Box::pin(async move {
                if let Some(tx) = seen {
                    let _ = tx.send(());
                }
                Ok(())
            })
        }

        fn messages(
            &self,
            _session_id: &str,
        ) -> BoxFuture<'_, Result<Vec<dto::MessageRecord>, anyhow::Error>> {
            let out =
                self.messages_out.lock().expect("messages lock").take().unwrap_or_default();
            Box::pin(async move { Ok(out) })
        }

        fn event_stream(
            &self,
            _session_id: &str,
        ) -> BoxFuture<'_, Result<EventStream, anyhow::Error>> {
            let rx =
                self.events.lock().expect("events lock").take().expect("stream already taken");
            Box::pin(async move {
                let stream: EventStream = Box::pin(futures_util::stream::unfold(
                    rx,
                    |mut rx| async move { rx.recv().await.map(|event| (event, rx)) },
                ));
                Ok(stream)
            })
        }
    }

    // ---------------- client harness ----------------

    #[tokio::test]
    async fn protocol_turn_round_trip() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });
        let collected = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_notification(
                {
                    let collected = Arc::clone(&collected);
                    async move |notif: SessionNotification, _cx| {
                        collected.lock().expect("collected lock").push(notif);
                        Ok(())
                    }
                },
                on_receive_notification!(),
            )
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    // 1. initialize
                    let init = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    assert_eq!(init.agent_capabilities.load_session, true);
                    assert_eq!(init.agent_info.as_ref().map(|i| i.name.as_str()), Some("opencode-acp-bridge"));

                    // 2. newSession
                    let ns = cx
                        .send_request(NewSessionRequest::new("/tmp/opencode/acp-fixture-project"))
                        .block_task()
                        .await?;
                    let sid = ns.session_id.clone();
                    assert_eq!(sid.0.as_ref(), "ses_mock_1");

                    // 3. prompt — events are pushed through the mock's channel
                    //    BEFORE the request: the agent's turn task consumes them
                    //    from the channel buffer while the client awaits.
                    const SID: &str = "ses_mock_1";
                    backend.push(dto::SessionEvent::TextStarted(dto::OrdinalRef {
                        sessionID: SID.into(),
                        assistantMessageID: "msg_mock_1".into(),
                        ordinal: Some(0),
                    }));
                    backend.push(dto::SessionEvent::TextDelta(dto::TextDelta {
                        base: dto::OrdinalRef {
                            sessionID: SID.into(),
                            assistantMessageID: "msg_mock_1".into(),
                            ordinal: Some(0),
                        },
                        delta: "hello from the bridge".into(),
                    }));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(
                        dto::SessionRef { sessionID: SID.into() },
                    ));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            sid.clone(),
                            vec![ContentBlock::Text(TextContent::new("write a file"))],
                        ))
                        .block_task()
                        .await?;

                    assert_eq!(prompt_req.stop_reason, acp::StopReason::EndTurn);
                    Ok(())
                }
            })
            .await;

        agent_task.abort(); // drop the agent connection
        outcome.expect("client run ok");

        let notifications = collected.lock().expect("collected lock");
        // TextDelta → AgentMessageChunk, forwarded under the session id.
        let chunks: Vec<&str> = notifications
            .iter()
            .filter_map(|n| match &n.update {
                acp::SessionUpdate::AgentMessageChunk(c) => {
                    let ContentBlock::Text(t) = &c.content else { return None };
                    Some(t.text.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(chunks, vec!["hello from the bridge"]);
        assert!(notifications.iter().all(|n| &*n.session_id.0 == "ses_mock_1"));
    }

    #[tokio::test]
    async fn load_session_replays_history_before_responding() {
        let backend = MockBackend::new();
        // Reuse the persisted fixture: user msg + assistant tool turn.
        let raw = include_str!("../../tests/fixtures/messages-tool-turn.json");
        let env: dto::MessagesEnvelope = serde_json::from_str(raw).expect("fixture parses");
        let records: Vec<dto::MessageRecord> = env
            .data
            .into_iter()
            .filter(|r| r.kind == "user" || r.kind == "assistant")
            .collect();
        backend.set_messages(records);

        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });
        let collected = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_notification(
                {
                    let collected = Arc::clone(&collected);
                    async move |notif: SessionNotification, _cx| {
                        collected.lock().expect("collected lock").push(notif);
                        Ok(())
                    }
                },
                on_receive_notification!(),
            )
            .connect_with(client_side, async move |cx| {
                // Load must replay the transcript BEFORE the response arrives.
                let _ = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let resp = cx
                    .send_request(LoadSessionRequest::new(
                        "ses_mock_1",
                        "/tmp/opencode/acp-fixture-project",
                    ))
                    .block_task()
                    .await?;
                assert!(resp.modes.is_none());
                Ok(())
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");

        let notifications = collected.lock().expect("collected lock");
        assert!(
            notifications.len() >= 5,
            "user msgs + tool call + updates + final text, got {}",
            notifications.len()
        );
        // Order: oldest user message first, then the tool call, completed
        // update, and the final assistant text with its persisted id.
        assert!(matches!(
            &notifications[0].update,
            acp::SessionUpdate::UserMessageChunk(_)
        ));
        assert!(notifications.iter().any(|n| matches!(
            &n.update,
            acp::SessionUpdate::ToolCall(t) if t.tool_call_id.0.as_ref() == "call_9663d4974690464f98d40e7c"
        )));
        assert!(notifications.iter().any(|n| matches!(
            &n.update,
            acp::SessionUpdate::ToolCallUpdate(u) if u.fields.status == Some(acp::ToolCallStatus::Completed)
        )));
        let last = notifications.last().expect("updates present");
        let acp::SessionUpdate::AgentMessageChunk(c) = &last.update else {
            panic!("last replayed update is the final assistant text");
        };
        assert_eq!(
            c.message_id.as_ref().map(|m| &*m.0),
            Some("msg_0fac22cb7001wso3ZZF1AWkVXt")
        );
    }

    #[tokio::test]
    async fn cancel_maps_to_interrupt_and_cancelled_response() {
        let backend = MockBackend::new();
        let interrupt_seen = backend.install_interrupt_seen();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .connect_with(client_side, async move |cx| {
                let _ = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let ns = cx
                    .send_request(NewSessionRequest::new("/tmp"))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();

                // Turn starts; we never push a terminal event.
                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("do something"))],
                ));

                // Client cancels; the agent must interrupt opencode immediately.
                cx.send_notification(CancelNotification::new(sid.clone()))?;

                // Barrier: arrived only after the cancel handler ran
                // (flag set + interrupt called).
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    interrupt_seen,
                )
                .await
                .expect("cancel handler must trigger backend.interrupt");
                assert!(
                    backend.interrupted.load(Ordering::SeqCst),
                    "backend interrupt must have been called"
                );

                // One more event lets the turn loop observe the cancel flag
                // and answer the pending prompt.
                backend.push(dto::SessionEvent::TextDelta(dto::TextDelta {
                    base: dto::OrdinalRef {
                        sessionID: "ses_mock_1".into(),
                        assistantMessageID: "msg_mock_1".into(),
                        ordinal: Some(0),
                    },
                    delta: "never rendered".into(),
                }));

                let resp = prompt.block_task().await?;
                assert_eq!(resp.stop_reason, acp::StopReason::Cancelled);
                Ok(())
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn new_session_rejects_relative_cwd() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .connect_with(client_side, async move |cx| {
                let _ = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let resp = cx
                    .send_request(NewSessionRequest::new("relative/dir"))
                    .block_task()
                    .await
                    .expect_err("relative cwd must be rejected");
                assert_eq!(resp.code, agent_client_protocol::ErrorCode::InvalidParams);
                Ok(())
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
    }
}