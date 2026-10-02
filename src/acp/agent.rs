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

    /// Answer an opencode permission prompt (Wave 3). Called once the ACP
    /// client has decided on a `session/request_permission`; the decision is
    /// forwarded verbatim to the server's reply endpoint.
    fn permission_reply(
        &self,
        session_id: &str,
        request_id: &str,
        decision: dto::PermissionReply,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>>;
}

/// The ACP agent service: state + handler wiring.
pub struct AgentService {
    backend: Arc<dyn OpenCodeBackend>,
    sessions: Mutex<HashMap<acp::SessionId, Arc<SessionEntry>>>,
}

struct SessionEntry {
    /// Set by `session/cancel`; polled by the turn loop between events.
    cancel: AtomicBool,
    /// The working directory of the ACP session (`newSession.cwd` /
    /// `loadSession.cwd`), used as the tool-call location on permission
    /// prompts (mirrors the official adapter's `cwd` for shell tools).
    cwd: String,
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
            Arc::new(SessionEntry {
                cancel: AtomicBool::new(false),
                cwd: cwd.clone(),
            }),
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
            Arc::new(SessionEntry {
                cancel: AtomicBool::new(false),
                cwd: req.cwd.to_string_lossy().to_string(),
            }),
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

            tracing::debug!(session = %req.session_id, event = ?event, "sse event");

            // session/cancel won? End the turn (the interrupt call itself was
            // already made by the cancel handler).
            if entry.cancel.load(Ordering::Acquire) {
                tracing::info!(session = %req.session_id, "turn cancelled via session/cancel");
                return responder.respond(acp::PromptResponse::new(acp::StopReason::Cancelled));
            }

            // ---------- permission bridging (Wave 3) ----------
            // `permission.asked` is a turn-level signal, not an update: ask
            // the ACP client and forward the decision before the turn can
            // resume. The server holds the execution until we reply.
            if let dto::SessionEvent::PermissionAsked(asked) = &event {
                self.forward_permission(asked, &mut state, &entry.cwd, &cx).await;
                continue;
            }

            match updates::stop_update(&event) {
                Some(updates::TurnEnd::EndTurn) => {
                    tracing::info!(session = %req.session_id, "turn ended (end_turn)");
                    return responder.respond(acp::PromptResponse::new(
                        acp::StopReason::EndTurn,
                    ));
                }
                Some(updates::TurnEnd::Cancelled) => {
                    // `session.execution.interrupted` (official cancellation
                    // path) or an `aborted`-kind failure.
                    tracing::info!(session = %req.session_id, "turn cancelled (interrupted)");
                    return responder.respond(acp::PromptResponse::new(
                        acp::StopReason::Cancelled,
                    ));
                }
                Some(updates::TurnEnd::MaxTokens) => {
                    // `length` failure — official mapping to max_tokens.
                    tracing::info!(session = %req.session_id, "turn ended (max_tokens)");
                    return responder.respond(acp::PromptResponse::new(
                        acp::StopReason::MaxTokens,
                    ));
                }
                Some(updates::TurnEnd::Refusal) => {
                    // `content-filter` failure — official mapping to refusal.
                    tracing::info!(session = %req.session_id, "turn ended (refusal)");
                    return responder.respond(acp::PromptResponse::new(
                        acp::StopReason::Refusal,
                    ));
                }
                Some(updates::TurnEnd::AuthRequired { message }) => {
                    // `provider.auth` failure — the ACP JSON-RPC contract has
                    // an exact code for this (authRequired, -32000); attach
                    // the opencode message as data.
                    let mut err = AcpError::auth_required();
                    if let Some(msg) = message {
                        err = err.data(serde_json::json!({ "message": msg }));
                    }
                    tracing::info!(session = %req.session_id, "turn failed (auth required)");
                    return responder.respond_with_error(err);
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

    /// Wave 3 permission bridging: turn a `permission.asked` event into an
    /// ACP `session/request_permission`, then route the client's decision
    /// back to opencode via [`OpenCodeBackend::permission_reply`].
    ///
    /// Wire contract (verified): `data.id` is the requestID the reply must
    /// target, `source.id` is the tool-call id, `action` is the permission
    /// kind (`"shell"`), `resources` are human-readable descriptions.
    ///
    /// Outcome routing per the official adapter (`packages/cli/src/acp/
    /// permission.ts`): `once` → allow_once "Allow once", `always` →
    /// allow_always "Always allow", `reject` → reject_once "Reject"; any
    /// other outcome (dismissed, cancelled, transport error) rejects — the
    /// "race cancel → reject" rule, and the server reply is uninterruptible.
    async fn forward_permission(
        self: &Arc<Self>,
        asked: &dto::PermissionAsked,
        state: &mut updates::MappingState,
        cwd: &str,
        cx: &ConnectionTo<Client>,
    ) {
        // Tool-call identity: `source.id` (the call_* id of the triggering
        // tool call); fall back to the permission request id.
        let tool_call_id = asked
            .source
            .as_ref()
            .map(|s| s.id.clone())
            .unwrap_or_else(|| asked.id.clone());
        // Title: "<action>: <first resource>" — e.g. "shell: echo hi".
        let title = match asked.resources.first() {
            Some(resource) => format!("{}: {}", asked.action, resource),
            None => asked.action.clone(),
        };
        // state.input = metadata merged over the cached tool input (tool
        // input cached from tool.input.ended / tool.called by the mapping).
        let mut input = serde_json::Map::new();
        if let Some(meta) = &asked.metadata {
            input.extend(meta.clone());
        }
        if let Some(serde_json::Value::Object(cached_obj)) = state.tool_input(&tool_call_id) {
            input.extend(cached_obj.clone());
        }
        let update = acp::ToolCallUpdate::new(
            tool_call_id,
            acp::ToolCallUpdateFields::new()
                .title(title)
                .status(acp::ToolCallStatus::Pending)
                .raw_input(serde_json::Value::Object(input))
                .locations(vec![acp::ToolCallLocation::new(cwd)]),
        );
        let request = acp::RequestPermissionRequest::new(
            acp::SessionId::from(asked.sessionID.clone()),
            update,
            Vec::from([
                acp::PermissionOption::new(
                    "once",
                    "Allow once",
                    acp::PermissionOptionKind::AllowOnce,
                ),
                acp::PermissionOption::new(
                    "always",
                    "Always allow",
                    acp::PermissionOptionKind::AllowAlways,
                ),
                acp::PermissionOption::new("reject", "Reject", acp::PermissionOptionKind::RejectOnce),
            ]),
        );

        // Block on the client's answer. A client that cancels the prompt turn
        // MUST answer the pending request with `Cancelled`; a dead connection
        // surfaces as a transport error. Both reject — official "race cancel
        // → reject" semantics. The opencode reply is then sent unconditionally.
        let decision = match cx.send_request(request).block_task().await {
            Ok(resp) => match &resp.outcome {
                acp::RequestPermissionOutcome::Selected(sel) => {
                    match sel.option_id.0.as_ref() {
                        "once" => dto::PermissionReply::Once,
                        "always" => dto::PermissionReply::Always,
                        other => {
                            tracing::warn!(
                                option = other,
                                session = %asked.sessionID,
                                "unknown permission option id — rejecting"
                            );
                            dto::PermissionReply::Reject
                        }
                    }
                }
                acp::RequestPermissionOutcome::Cancelled => dto::PermissionReply::Reject,
                // The protocol's outcome enum is non-exhaustive: unknown
                // outcomes reject too (safe default).
                _ => dto::PermissionReply::Reject,
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    session = %asked.sessionID,
                    "permission request to ACP client failed — rejecting"
                );
                dto::PermissionReply::Reject
            }
        };
        tracing::info!(
            session = %asked.sessionID,
            request = %asked.id,
            ?decision,
            "permission decision forwarded to opencode"
        );
        if let Err(e) = self
            .backend
            .permission_reply(&asked.sessionID, &asked.id, decision)
            .await
        {
            tracing::warn!(
                error = %e,
                session = %asked.sessionID,
                request = %asked.id,
                "permission reply to opencode failed"
            );
        }
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
        /// Recorded (session_id, request_id, decision) of permission replies.
        permission_replies: Mutex<Vec<(String, String, dto::PermissionReply)>>,
        permission_seen: Mutex<Option<oneshot::Sender<()>>>,
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
                permission_replies: Mutex::new(Vec::new()),
                permission_seen: Mutex::new(None),
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

        fn install_permission_seen(&self) -> oneshot::Receiver<()> {
            let (tx, rx) = oneshot::channel();
            *self.permission_seen.lock().expect("permission lock") = Some(tx);
            rx
        }

        fn recorded_replies(&self) -> Vec<(String, String, dto::PermissionReply)> {
            self.permission_replies.lock().expect("permission lock").clone()
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

        fn permission_reply(
            &self,
            session_id: &str,
            request_id: &str,
            decision: dto::PermissionReply,
        ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            self.permission_replies
                .lock()
                .expect("permission lock")
                .push((session_id.to_string(), request_id.to_string(), decision));
            let seen = self.permission_seen.lock().expect("permission lock").take();
            Box::pin(async move {
                if let Some(tx) = seen {
                    let _ = tx.send(());
                }
                Ok(())
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

    // ======================= Wave 3: permission bridging =======================

    /// One fixture-faithful `permission.asked` frame.
    fn asked_frame(id: &str, call_id: &str, resource: &str) -> dto::SessionEvent {
        dto::SessionEvent::PermissionAsked(dto::PermissionAsked {
            id: id.into(),
            sessionID: "ses_mock_1".into(),
            action: "shell".into(),
            resources: vec![resource.into()],
            save: None,
            metadata: None,
            source: Some(dto::PermissionSource {
                kind: Some("tool".into()),
                messageID: Some("msg_mock_1".into()),
                id: call_id.into(),
            }),
        })
    }

    /// What the client should answer to each incoming `requestPermission`.
    #[derive(Clone)]
    enum ClientReply {
        /// Respond with this outcome.
        Outcome(acp::RequestPermissionOutcome),
        /// Handler failure — simulates a dismissed/failed client request.
        HandlerError,
    }


    #[tokio::test]
    async fn permission_asked_forwards_decisions() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let responses = Arc::new(Mutex::new(std::collections::VecDeque::from([
            ClientReply::Outcome(acp::RequestPermissionOutcome::Selected(
                acp::SelectedPermissionOutcome::new("once"),
            )),
            ClientReply::Outcome(acp::RequestPermissionOutcome::Selected(
                acp::SelectedPermissionOutcome::new("always"),
            )),
        ])));
        let seen_requests = Arc::new(Mutex::new(Vec::new()));
        let reply_seen = backend.install_permission_seen();

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_request(
                {
                    let seen_requests = Arc::clone(&seen_requests);
                    let responses = Arc::clone(&responses);
                    async move |req: acp::RequestPermissionRequest,
                                responder: Responder<acp::RequestPermissionResponse>,
                                _cx: ConnectionTo<Agent>| {
                        seen_requests.lock().expect("requests lock").push(req);
                        match responses.lock().expect("responses lock").pop_front() {
                            Some(ClientReply::Outcome(outcome)) => {
                                responder.respond(acp::RequestPermissionResponse::new(outcome))
                            }
                            Some(ClientReply::HandlerError) => Err(AcpError::internal_error()),
                            None => panic!("permission request with no queued client reply"),
                        }
                    }
                },
                on_receive_request!(),
            )
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let ns = cx
                        .send_request(NewSessionRequest::new("/tmp/opencode/acp-fixture-project"))
                        .block_task()
                        .await?;
                    let sid = ns.session_id.clone();

                    // Tool input streams, then two asks; the first reuses the
                    // cached input, the second carries only metadata.
                    backend.push(dto::SessionEvent::ToolInputEnded(dto::ToolInputEnded {
                        base: dto::ToolRef {
                            sessionID: "ses_mock_1".into(),
                            assistantMessageID: "msg_mock_1".into(),
                            id: "call_42b2e3814e6a4745a9d2aa1e".into(),
                        },
                        text: r#"{"command": "echo hi"}"#.into(),
                    }));
                    backend.push(asked_frame(
                        "per_0fb5ca336001m94Lbqk9xdVFcC",
                        "call_42b2e3814e6a4745a9d2aa1e",
                        "echo hi",
                    ));
                    backend.push(dto::SessionEvent::PermissionAsked(dto::PermissionAsked {
                        id: "per_2".into(),
                        sessionID: "ses_mock_1".into(),
                        action: "shell".into(),
                        resources: vec!["echo bye".into()],
                        save: None,
                        metadata: Some(serde_json::json!({"confirm": true}).as_object().unwrap().clone()),
                        source: Some(dto::PermissionSource {
                            kind: Some("tool".into()),
                            messageID: Some("msg_mock_1".into()),
                            id: "call_2".into(),
                        }),
                    }));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_mock_1".into(),
                    }));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            sid.clone(),
                            vec![ContentBlock::Text(TextContent::new("run echo"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(prompt_req.stop_reason, acp::StopReason::EndTurn);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");

        // Reply routing table: once → Once · always → Always, targeting the
        // exact requestIDs of the asks.
        assert_eq!(
            backend.recorded_replies(),
            vec![
                ("ses_mock_1".to_string(), "per_0fb5ca336001m94Lbqk9xdVFcC".to_string(), dto::PermissionReply::Once),
                ("ses_mock_1".to_string(), "per_2".to_string(), dto::PermissionReply::Always),
            ]
        );

        // The reply forwarding completed before the turn could resume.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), reply_seen)
            .await
            .expect("permission replies must reach the backend");

        // Option set construction (official pattern): 3 options, stable ids.
        let requests = seen_requests.lock().expect("requests lock");
        assert_eq!(requests.len(), 2, "one requestPermission per ask");
        let options = &requests[0].options;
        assert_eq!(options.len(), 3);
        assert_eq!(options[0].option_id.0.as_ref(), "once");
        assert_eq!(options[0].name, "Allow once");
        assert_eq!(options[0].kind, acp::PermissionOptionKind::AllowOnce);
        assert_eq!(options[1].option_id.0.as_ref(), "always");
        assert_eq!(options[1].name, "Always allow");
        assert_eq!(options[1].kind, acp::PermissionOptionKind::AllowAlways);
        assert_eq!(options[2].option_id.0.as_ref(), "reject");
        assert_eq!(options[2].name, "Reject");
        assert_eq!(options[2].kind, acp::PermissionOptionKind::RejectOnce);

        // Tool-call construction: id from source.id, title "shell: <cmd>",
        // state.input = metadata + cached tool input, cwd in locations.
        let tc = &requests[0].tool_call;
        assert_eq!(tc.tool_call_id.0.as_ref(), "call_42b2e3814e6a4745a9d2aa1e");
        assert_eq!(tc.fields.title.as_deref(), Some("shell: echo hi"));
        assert_eq!(tc.fields.status, Some(acp::ToolCallStatus::Pending));
        assert_eq!(tc.fields.raw_input, Some(serde_json::json!({ "command": "echo hi" })));
        let locations = tc.fields.locations.as_ref().expect("locations set");
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].path.to_string_lossy(), "/tmp/opencode/acp-fixture-project");
        // Second ask: metadata-only input (no cached input for call_2).
        let tc2 = &requests[1].tool_call;
        assert_eq!(tc2.tool_call_id.0.as_ref(), "call_2");
        assert_eq!(tc2.fields.title.as_deref(), Some("shell: echo bye"));
        assert_eq!(
            tc2.fields.raw_input,
            Some(serde_json::json!({ "confirm": true }))
        );
    }

    #[tokio::test]
    async fn permission_cancelled_or_dismissed_rejects() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let responses = Arc::new(Mutex::new(std::collections::VecDeque::from([
            // Client cancelled the prompt turn → the request answers Cancelled.
            ClientReply::Outcome(acp::RequestPermissionOutcome::Cancelled),
            // Client-side failure (dismissed transport/UI error) → Err.
            ClientReply::HandlerError,
        ])));
        let seen_requests = Arc::new(Mutex::new(Vec::new()));

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_request(
                {
                    let seen_requests = Arc::clone(&seen_requests);
                    let responses = Arc::clone(&responses);
                    async move |req: acp::RequestPermissionRequest,
                                responder: Responder<acp::RequestPermissionResponse>,
                                _cx: ConnectionTo<Agent>| {
                        seen_requests.lock().expect("requests lock").push(req);
                        match responses.lock().expect("responses lock").pop_front() {
                            Some(ClientReply::Outcome(outcome)) => {
                                responder.respond(acp::RequestPermissionResponse::new(outcome))
                            }
                            Some(ClientReply::HandlerError) => Err(AcpError::internal_error()),
                            None => panic!("permission request with no queued client reply"),
                        }
                    }
                },
                on_receive_request!(),
            )
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let ns = cx
                        .send_request(NewSessionRequest::new("/tmp"))
                        .block_task()
                        .await?;
                    let sid = ns.session_id.clone();

                    backend.push(asked_frame("per_c1", "call_c1", "rm -rf /"));
                    backend.push(asked_frame("per_c2", "call_c2", "sudo rm -rf /"));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_mock_1".into(),
                    }));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            sid.clone(),
                            vec![ContentBlock::Text(TextContent::new("run commands"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(prompt_req.stop_reason, acp::StopReason::EndTurn);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");

        // Official semantics: cancelled AND dismissed both reject; the server
        // reply is uninterruptible.
        assert_eq!(
            backend.recorded_replies(),
            vec![
                ("ses_mock_1".to_string(), "per_c1".to_string(), dto::PermissionReply::Reject),
                ("ses_mock_1".to_string(), "per_c2".to_string(), dto::PermissionReply::Reject),
            ]
        );
    }

    #[tokio::test]
    async fn permission_replied_echo_is_ignored() {
        // The server echoes our own replies as `permission.replied`; the turn
        // must not treat it as anything (no requestPermission to the client).
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });
        let seen_requests = Arc::new(Mutex::new(Vec::new()));
        let responses = Arc::new(Mutex::new(std::collections::VecDeque::new()));

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_request(
                {
                    let seen_requests = Arc::clone(&seen_requests);
                    let responses = Arc::clone(&responses);
                    async move |req: acp::RequestPermissionRequest,
                                responder: Responder<acp::RequestPermissionResponse>,
                                _cx: ConnectionTo<Agent>| {
                        seen_requests.lock().expect("requests lock").push(req);
                        match responses.lock().expect("responses lock").pop_front() {
                            Some(ClientReply::Outcome(outcome)) => {
                                responder.respond(acp::RequestPermissionResponse::new(outcome))
                            }
                            Some(ClientReply::HandlerError) => Err(AcpError::internal_error()),
                            None => panic!("permission request with no queued client reply"),
                        }
                    }
                },
                on_receive_request!(),
            )
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let _ = cx
                        .send_request(NewSessionRequest::new("/tmp"))
                        .block_task()
                        .await?;

                    backend.push(dto::SessionEvent::PermissionReplied(dto::PermissionReplied {
                        sessionID: "ses_mock_1".into(),
                        requestID: "per_ignored".into(),
                        reply: Some("once".into()),
                    }));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_mock_1".into(),
                    }));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            "ses_mock_1",
                            vec![ContentBlock::Text(TextContent::new("hi"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(prompt_req.stop_reason, acp::StopReason::EndTurn);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
        assert!(seen_requests.lock().expect("requests lock").is_empty(), "no client request for the echo");
        assert!(backend.recorded_replies().is_empty(), "no reply forwarded for the echo");
    }

    // ======================= Wave 3: stopReason refinement =======================

    #[tokio::test]
    async fn execution_interrupted_stops_with_cancelled() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let _ = cx
                        .send_request(NewSessionRequest::new("/tmp"))
                        .block_task()
                        .await?;
                    backend.push(dto::SessionEvent::ExecutionInterrupted(
                        dto::SessionRef { sessionID: "ses_mock_1".into() },
                    ));
                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            "ses_mock_1",
                            vec![ContentBlock::Text(TextContent::new("run"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(prompt_req.stop_reason, acp::StopReason::Cancelled);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn provider_auth_failure_surfaces_auth_required_error() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let _ = cx
                        .send_request(NewSessionRequest::new("/tmp"))
                        .block_task()
                        .await?;
                    backend.push(dto::SessionEvent::ExecutionFailed(dto::ExecutionFailed {
                        session: dto::SessionRef { sessionID: "ses_mock_1".into() },
                        error: dto::StructuredError {
                            kind: Some("provider.auth".into()),
                            message: Some("provider astra is not authenticated".into()),
                        },
                    }));
                    let resp = cx
                        .send_request(PromptRequest::new(
                            "ses_mock_1",
                            vec![ContentBlock::Text(TextContent::new("run"))],
                        ))
                        .block_task()
                        .await
                        .expect_err("provider.auth must fail the request");
                    assert_eq!(
                        resp.code,
                        agent_client_protocol::ErrorCode::AuthRequired,
                        "official authRequired code -32000"
                    );
                    assert_eq!(
                        resp.data.as_ref().and_then(|d| d.get("message")),
                        Some(&serde_json::json!("provider astra is not authenticated"))
                    );
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn content_filter_failure_stops_with_refusal() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .connect_with(client_side, {
                let backend = Arc::clone(&backend);
                async move |cx| {
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let _ = cx
                        .send_request(NewSessionRequest::new("/tmp"))
                        .block_task()
                        .await?;
                    backend.push(dto::SessionEvent::ExecutionFailed(dto::ExecutionFailed {
                        session: dto::SessionRef { sessionID: "ses_mock_1".into() },
                        error: dto::StructuredError {
                            kind: Some("content-filter".into()),
                            message: Some("blocked by content policy".into()),
                        },
                    }));
                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            "ses_mock_1",
                            vec![ContentBlock::Text(TextContent::new("run"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(prompt_req.stop_reason, acp::StopReason::Refusal);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");
    }
}
