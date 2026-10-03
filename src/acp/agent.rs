//! ACP agent assembly.
//!
//! Protocol surface: initialize / newSession / loadSession / resume / list /
//! delete / prompt / cancel (Wave 6a closes the session-management ring:
//! `session/list`, `session/resume`, `session/delete` + the
//! `available_commands_update` initial push). The ACP ↔ opencode wiring runs
//! through the [`OpenCodeBackend`] trait (implemented by the HTTP/SSE lane),
//! which keeps this module fully mock-testable.
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

/// Wave 6a: a session list — (sessions, next-page token).
/// (Kept in the `list_sessions` fallible signature to stay readable.)
pub type SessionList = (Vec<dto::SessionInfo>, Option<String>);

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

    /// Wave 4 catalog fetch: the model list backing `config_option_update`
    /// pushes on catalog reload. Default: unavailable — the live bridge
    /// (`HttpBackend`) does not override yet, so the push is skipped until
    /// the fetch lands in a follow-up wave (the wave boundary keeps
    /// `src/bridge/**` untouched); mocks override to exercise the push path.
    fn list_models(&self) -> BoxFuture<'_, Option<Vec<dto::ModelInfo>>> {
        Box::pin(async { None })
    }

    /// Wave 4 slash-command catalog for `available_commands_update` pushes.
    /// Same default-unavailable contract as [`Self::list_models`].
    fn list_commands(&self) -> BoxFuture<'_, Option<Vec<serde_json::Value>>> {
        Box::pin(async { None })
    }

    /// Wave 6: list opencode sessions for a directory (`session/list`
    /// backend). `cursor` is the ACP opaque page token, forwarded verbatim
    /// (as the `next` token). Returns (sessions, next-page token — the wire
    /// envelope's `next` cursor, if any). Default: unavailable — same wave
    /// boundary as [`Self::list_models`]; the live override lands in a
    /// follow-up wave.
    fn list_sessions(
        &self,
        _directory: Option<&str>,
        _cursor: Option<&str>,
    ) -> BoxFuture<'_, Result<SessionList, anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("list_sessions not available on this backend")) })
    }

    /// Wave 6: delete a session (`session/delete` backend). Default:
    /// unavailable — same wave boundary as [`Self::list_models`].
    fn delete_session(&self, _session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("delete_session not available on this backend")) })
    }

    /// Wave 6b: the agent catalog for a directory (`GET /api/agent` with
    /// the deepObject `location[directory]` filter — the ACP mode list
    /// source). Default: unavailable (wave boundary); the live override
    /// landed on `HttpBackend` this wave.
    fn agents(&self, _directory: &str) -> BoxFuture<'_, Result<Vec<dto::AgentInfo>, anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("agents not available on this backend")) })
    }

    /// Wave 6b: switch the agent running a session (`POST
    /// /api/session/{id}/agent`, the `session/set_mode` wire). Default:
    /// unavailable (wave boundary); live override on `HttpBackend`.
    fn set_agent(&self, _session_id: &str, _agent: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("set_agent not available on this backend")) })
    }

    /// Release 0.3.0: the session record (`GET /api/session/{id}`) — the
    /// authoritative `agent` / `model` fields for load/resume config-option
    /// current values (includes post-switch state). Default: unavailable
    /// (mock override + live `HttpBackend`); a failure degrades to the
    /// last-assistant-message / catalog fallbacks, never fails the lifecycle.
    fn get_session(&self, _session_id: &str) -> BoxFuture<'_, Result<dto::SessionInfo, anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("get_session not available on this backend")) })
    }

    /// Release 0.3.0: switch the model running a session (`POST
    /// /api/session/{id}/model` → 204 — the model config-option wire for
    /// `session/set_config_option`). Default: unavailable; live override on
    /// `HttpBackend`.
    fn set_model(&self, _session_id: &str, _model: &dto::ModelRef) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        Box::pin(async { Err(anyhow::anyhow!("set_model not available on this backend")) })
    }
}

/// The ACP agent service: state + handler wiring.
pub struct AgentService {
    backend: Arc<dyn OpenCodeBackend>,
    sessions: Mutex<HashMap<acp::SessionId, Arc<SessionEntry>>>,
    /// How long the turn loop keeps draining in-flight events after a
    /// `session/cancel` before abandoning still-open tool calls (official
    /// wind-down, ~5s).
    drain_window: std::time::Duration,
    /// `--no-aft`: disable the aft hoist adaptations (File/image content
    /// passthrough). Diff extraction stays (dialect-neutral).
    no_aft: bool,
    /// Release 0.3.0: whether the client declared the session config-options
    /// capability (`clientCapabilities.session.configOptions` non-null) in
    /// its initialize request. Gates the `config_options` payload in
    /// lifecycle responses AND every `config_option_update` push. The
    /// service is per-connection (one stdio ACP session), so a single flag
    /// is the per-connection store.
    config_options_supported: AtomicBool,
}

struct SessionEntry {
    /// Set by `session/cancel`; polled by the turn loop between events.
    cancel: AtomicBool,
    /// The working directory of the ACP session (`newSession.cwd` /
    /// `loadSession.cwd`), used as the tool-call location on permission
    /// prompts (mirrors the official adapter's `cwd` for shell tools).
    cwd: String,
    /// Tracked ACP mode (the opencode agent id). Established by the
    /// lifecycle responses (newSession → the derived default — the first
    /// visible primary agent; load/resume → the last assistant message's
    /// agent, or the derived default); updated by `session/set_mode`, the
    /// `session.agent.selected` SSE event and (self-heal) mismatched
    /// `step.started` agents. `current_mode_update` fires only when this
    /// value actually changes — which is what suppresses the own-switch
    /// SSE echo.
    mode: Mutex<Option<String>>,
    /// Release 0.3.0: tracked session model (the `ModelRef` the server runs
    /// the session with). Established by the lifecycle responses (newSession
    /// → `None` — the default model is not discoverable pre-first-turn;
    /// load/resume → `get_session.model`, else the last assistant message's
    /// model); updated by `session/set_config_option` (model id), the
    /// `session.model.selected` SSE event and (self-heal) mismatched
    /// `step.started` models. `config_option_update` pushes fire only on an
    /// ACTUAL change — which suppresses the own-switch SSE echo.
    model: Mutex<Option<dto::ModelRef>>,
}

impl AgentService {
    pub fn new(backend: Arc<dyn OpenCodeBackend>) -> Self {
        Self {
            backend,
            sessions: Mutex::new(HashMap::new()),
            drain_window: std::time::Duration::from_secs(5),
            no_aft: false,
            config_options_supported: AtomicBool::new(false),
        }
    }

    /// Derive a service with `--no-aft`: the aft tool-call hoist adaptations
    /// (File/image content passthrough) are disabled. Diff extraction stays —
    /// `filediff`/`diff` are dialect-neutral.
    pub fn with_no_aft(mut self, no_aft: bool) -> Self {
        self.no_aft = no_aft;
        self
    }

    /// Override the cancel-drain window (default 5s, official behavior).
    #[allow(dead_code)]
    pub fn with_drain_window(mut self, drain_window: std::time::Duration) -> Self {
        self.drain_window = drain_window;
        self
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
                    async move |req: acp::NewSessionRequest, responder, cx| {
                        svc.new_session(req, responder, cx).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- listSession ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::ListSessionsRequest, responder, _cx| {
                        svc.list_sessions(req, responder).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- resumeSession ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::ResumeSessionRequest, responder, cx| {
                        svc.resume_session(req, responder, cx).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- deleteSession ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::DeleteSessionRequest, responder, _cx| {
                        svc.delete_session(req, responder).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- setMode ----------
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::SetSessionModeRequest, responder, cx| {
                        svc.set_mode(req, responder, cx).await
                    }
                },
                on_receive_request!(),
            )
            // ---------- setConfigOption (Release 0.3.0) ----------
            // `session/set_config_option` — the model/agent pickers (Zed
            // renders these via configOptions, mutually exclusive with the
            // modes dropdown). Shares `set_mode`'s wire for the agent id;
            // the model id routes to `POST /api/session/{id}/model`.
            .on_receive_request(
                {
                    let svc = Arc::clone(&self);
                    async move |req: acp::SetSessionConfigOptionRequest, responder, cx| {
                        svc.set_config_option(req, responder, cx).await
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
        // Wave 6a: advertise the session-management ring. Each sub-capability
        // is a marker struct — `{}` on the wire (schema 1.5.0 stable; only
        // `fork` is unstable-feature-gated, we do not advertise it).
        let caps = acp::AgentCapabilities::new().load_session(true).session_capabilities(
            acp::SessionCapabilities::new()
                .list(acp::SessionListCapabilities::new())
                .delete(acp::SessionDeleteCapabilities::new())
                .resume(acp::SessionResumeCapabilities::new()),
        );
        // Release 0.3.0: the config-options capability gate. Zed declares
        // `clientCapabilities.session.configOptions` → it renders the model
        // picker ONLY via configOptions (mutually exclusive with the modes
        // dropdown); other clients keep receiving `modes`. Storing the flag
        // per connection is exact because the service serves exactly one ACP
        // connection.
        let config_options_supported = req
            .client_capabilities
            .session
            .as_ref()
            .and_then(|s| s.config_options.as_ref())
            .is_some();
        self.config_options_supported
            .store(config_options_supported, Ordering::SeqCst);
        tracing::debug!(config_options_supported, "initialize: client capabilities");
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
        self: &Arc<Self>,
        req: acp::NewSessionRequest,
        responder: Responder<acp::NewSessionResponse>,
        cx: ConnectionTo<Client>,
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
        // Wave 6b: the mode list from the agent catalog (degraded to empty
        // on failure — session creation must not fail on metadata), and the
        // derived default agent as the initial mode.
        let agents = match self.backend.agents(&cwd).await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "agents fetch failed — empty mode list");
                Vec::new()
            }
        };
        let modes = to_session_modes(&agents);
        let current = default_mode_id(&agents);
        // Release 0.3.0: the model catalog for the model config option,
        // fetched ONLY when the client declared the capability (no point in
        // the round-trip otherwise). `None` (fetch failure or no catalog
        // access) omits the model option from the payload.
        let models = if self.config_options_supported.load(Ordering::SeqCst) {
            self.backend.list_models().await
        } else {
            None
        };
        let session_id = acp::SessionId::from(session_id);
        self.sessions.lock().expect("sessions lock").insert(
            session_id.clone(),
            Arc::new(SessionEntry {
                cancel: AtomicBool::new(false),
                cwd: cwd.clone(),
                mode: Mutex::new(current.clone()),
                // newSession cannot know the model: session creation sends no
                // model (the server assigns the config default), the create
                // response and GET /api/session/{id} carry none pre-first-
                // turn, and config documents have no default-model field.
                // UNKNOWN is surfaced as the synthetic "__default__" option.
                model: Mutex::new(None),
            }),
        );
        tracing::info!(%session_id, modes = %modes.len(), current_mode = ?current, "ACP newSession -> opencode session");
        let mut response = match &current {
            // The derived default is in the filtered list by construction.
            Some(id) => acp::NewSessionResponse::new(session_id.clone())
                .modes(acp::SessionModeState::new(id.clone(), modes)),
            // No pickable modes: omit the payload — Zed renders no picker
            // instead of an unmatched "Unknown" current mode.
            None => acp::NewSessionResponse::new(session_id.clone()),
        };
        // Release 0.3.0: config options = agent picker + model picker. Only
        // when the client declared the capability; per-option degrade: one
        // catalog fetch failed → that option is omitted. The model's current
        // value is UNKNOWN here → "__default__" plus the synthetic Default
        // option (an unmatched current value would render "Unknown" in Zed;
        // once a concrete model is known there is no server API to unset it,
        // so Default is listed only while unknown).
        if self.config_options_supported.load(Ordering::SeqCst) {
            let options = build_config_options(
                &agents,
                models.as_deref().unwrap_or(&[]),
                current.as_deref(),
                None,
            );
            if !options.is_empty() {
                response = response.config_options(options);
            }
        }
        responder.respond(response)?;
        // Wave 6a: initial `available_commands_update` push, AFTER the
        // response, spawned so the fetch can never gate session creation.
        self.spawn_commands_push(&session_id, &cx);
        Ok(())
    }

    /// Wave 6a `session/list`: list the sessions of a directory, mapping
    /// each opencode session into ACP [`acp::SessionInfo`]. The ACP cursor is
    /// opaque — forwarded verbatim, and the wire envelope's `next` token
    /// comes back as `nextCursor`.
    async fn list_sessions(
        &self,
        req: acp::ListSessionsRequest,
        responder: Responder<acp::ListSessionsResponse>,
    ) -> Result<(), AcpError> {
        let cwd = req.cwd.as_ref().map(|p| p.to_string_lossy().to_string());
        let (sessions, next) = match self
            .backend
            .list_sessions(cwd.as_deref(), req.cursor.as_deref())
            .await
        {
            Ok(ok) => ok,
            Err(e) => {
                tracing::error!(error = %e, "backend list_sessions failed");
                return responder.respond_with_internal_error(format!(
                    "failed to list sessions: {e}"
                ));
            }
        };
        let mapped: Vec<acp::SessionInfo> = sessions
            .iter()
            .map(|s| to_acp_session_info(s, cwd.as_deref()))
            .collect();
        tracing::info!(sessions = mapped.len(), ?next, "ACP session/list");
        responder.respond(acp::ListSessionsResponse::new(mapped).next_cursor(next))
    }

    /// Wave 6b: the raw agent catalog for a session's directory (the ACP
    /// mode list + derived default source). Degraded to an empty list on
    /// fetch failure (a lifecycle response must not fail on metadata).
    async fn fetch_agents(&self, cwd: &str) -> Vec<dto::AgentInfo> {
        match self.backend.agents(cwd).await {
            Ok(agents) => agents,
            Err(e) => {
                tracing::warn!(error = %e, "agents fetch failed — empty mode list");
                Vec::new()
            }
        }
    }

    /// Wave 6a `session/resume`: like `session/load` (register the session
    /// so a later prompt works) but WITHOUT replaying history — the response
    /// carries only modes/configOptions (both populated modes here; no
    /// configOptions). Messages are fetched for the currentModeId metadata
    /// only; nothing is replayed.
    async fn resume_session(
        self: &Arc<Self>,
        req: acp::ResumeSessionRequest,
        responder: Responder<acp::ResumeSessionResponse>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        // Metadata pass: last assistant message's agent → current mode.
        // A fetch failure degrades to the derived default (resume must not
        // fail).
        let records = match self.backend.messages(&req.session_id.0).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, session = %req.session_id, "resume: messages fetch failed (mode falls back to the default)");
                Vec::new()
            }
        };
        let agents = self.fetch_agents(&req.cwd.to_string_lossy()).await;
        // Release 0.3.0: the session record carries the authoritative
        // current agent + model (includes post-switch state); a fetch
        // failure or absent fields fall back to the message/catalog data
        // (never fails the lifecycle).
        let session_info = match self.backend.get_session(&req.session_id.0).await {
            Ok(info) => Some(info),
            Err(e) => {
                tracing::warn!(error = %e, session = %req.session_id, "resume: get_session failed (agent/model fall back to messages/catalog)");
                None
            }
        };
        let models = if self.config_options_supported.load(Ordering::SeqCst) {
            self.backend.list_models().await
        } else {
            None
        };
        // The honest last-assistant agent wins even when it drifted out of
        // the catalog (step.started self-heals); otherwise the derived
        // default (first visible primary in wire order). The session record's
        // agent is authoritative over both.
        let current = session_info
            .as_ref()
            .and_then(|i| i.agent.clone())
            .or_else(|| last_assistant_agent(&records))
            .or_else(|| default_mode_id(&agents));
        let current_model = session_info
            .as_ref()
            .and_then(|i| i.model.clone())
            .or_else(|| last_assistant_model(&records));
        let modes = to_session_modes(&agents);
        // Register unconditionally — the client may resume a session this
        // bridge never loaded, and the next prompt must still route.
        self.sessions.lock().expect("sessions lock").insert(
            req.session_id.clone(),
            Arc::new(SessionEntry {
                cancel: AtomicBool::new(false),
                cwd: req.cwd.to_string_lossy().to_string(),
                mode: Mutex::new(current.clone()),
                model: Mutex::new(current_model.clone()),
            }),
        );
        // No pickable modes → omit the payload (Zed renders no picker
        // instead of an unmatched "Unknown" current mode).
        let mut response = if modes.is_empty() {
            acp::ResumeSessionResponse::new()
        } else {
            acp::ResumeSessionResponse::new().modes(acp::SessionModeState::new(
                current.clone().expect("modes non-empty ⇒ a derived default exists"),
                modes,
            ))
        };
        // Release 0.3.0: config options (agent + model pickers), capability
        // gated and per-option degraded exactly like newSession.
        if self.config_options_supported.load(Ordering::SeqCst) {
            let options = build_config_options(
                &agents,
                models.as_deref().unwrap_or(&[]),
                current.as_deref(),
                current_model.as_ref(),
            );
            if !options.is_empty() {
                response = response.config_options(options);
            }
        }
        tracing::info!(session = %req.session_id, "ACP session/resume (no replay)");
        responder.respond(response)?;
        // Wave 6a: initial `available_commands_update` push, after the
        // response (spawned — never gate the resume).
        self.spawn_commands_push(&req.session_id, &cx);
        Ok(())
    }

    /// Wave 6b `session/set_mode`: switch the opencode agent running the
    /// session (`POST /api/session/{id}/agent` body `{"agent": modeId}` →
    /// 204 → empty ACP response). Tracks the mode FIRST, then emits exactly
    /// one `current_mode_update` — the server's own-switch SSE echo
    /// (`session.agent.selected` with the same agent) then diffs to zero
    /// against the tracked value and stays suppressed.
    async fn set_mode(
        self: &Arc<Self>,
        req: acp::SetSessionModeRequest,
        responder: Responder<acp::SetSessionModeResponse>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        let Some(entry) = self
            .sessions
            .lock()
            .expect("sessions lock")
            .get(&req.session_id)
            .cloned()
        else {
            return responder.respond_with_error(
                AcpError::invalid_params().data(serde_json::json!({
                    "message": "set_mode: unknown session"
                })),
            );
        };
        if let Err(e) = self.backend.set_agent(&req.session_id.0, &req.mode_id.0).await {
            tracing::error!(error = %e, session = %req.session_id, mode = %req.mode_id, "set_agent failed");
            return responder.respond_with_internal_error(format!(
                "opencode agent switch failed: {e}"
            ));
        }
        let mode = req.mode_id.0.to_string();
        *entry.mode.lock().expect("mode lock") = Some(mode.clone());
        tracing::info!(session = %req.session_id, mode, "session/set_mode -> opencode agent");
        responder.respond(acp::SetSessionModeResponse::new())?;
        cx.send_notification(acp::SessionNotification::new(
            req.session_id.clone(),
            acp::SessionUpdate::CurrentModeUpdate(acp::CurrentModeUpdate::new(mode)),
        ))?;
        // Release 0.3.0: keep the config-options surface honest too — Zed
        // renders the agent picker from the config option, so a mode switch
        // from another client must refresh it. Tracked-first means the
        // server's own-switch SSE echo diffs to zero.
        self.push_config_options(&req.session_id, &entry, &cx).await;
        Ok(())
    }

    /// Release 0.3.0 `session/set_config_option`: the model/agent pickers.
    ///
    /// `config_id "model"` — value `"<provider>/<model>"` (split on the
    /// FIRST `/`): looked up in the model catalog, switched via the existing
    /// `POST /api/session/{id}/model` wire, then the tracked model is updated
    /// and the full config-options state goes into the response AND a
    /// `config_option_update` push. `"__default__"` is a no-op success (there
    /// is no server API to unset a model) that echoes the current state. A
    /// value not in the catalog (e.g. a stale Zed-persisted default like
    /// `codebuddy/gpt-6-sol`) or a failed switch is an invalid-value error
    /// followed by a current-state push — the client self-corrects.
    ///
    /// `config_id "agent"` — value = agent id: the EXACT `set_mode` path
    /// (`set_agent` + tracked mode + remote-echo suppression), with the
    /// response/push carrying the full config-options state.
    ///
    /// Unknown config ids are invalid-params errors.
    async fn set_config_option(
        self: &Arc<Self>,
        req: acp::SetSessionConfigOptionRequest,
        responder: Responder<acp::SetSessionConfigOptionResponse>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        let Some(entry) = self
            .sessions
            .lock()
            .expect("sessions lock")
            .get(&req.session_id)
            .cloned()
        else {
            return responder.respond_with_error(
                AcpError::invalid_params().data(serde_json::json!({
                    "message": "set_config_option: unknown session"
                })),
            );
        };
        let value = req.value.as_value_id().map(|v| v.0.as_ref()).unwrap_or("");
        match req.config_id.0.as_ref() {
            "model" => match value {
                // No server API to unset a model: accept the synthetic
                // default as a no-op and echo the current state (the
                // "__default__" option stays listed while the model is
                // unknown).
                "__default__" => {
                    tracing::info!(session = %req.session_id, "set_config_option(model=__default__) no-op");
                    let state = self.current_config_options(&entry).await;
                    responder.respond(acp::SetSessionConfigOptionResponse::new(state.clone()))?;
                    self.push_config_options(&req.session_id, &entry, &cx).await;
                }
                v => {
                    // Split on the FIRST `/`: provider ids may contain
                    // slashes, model ids may not (the value scheme is the
                    // CLI's `<provider>/<model>`).
                    let Some((provider, model_id)) = v.split_once('/') else {
                        return self
                            .reject_config_value(
                                &req,
                                responder,
                                &entry,
                                &cx,
                                format!("invalid model value {v:?}: expected \"<provider>/<model>\""),
                            )
                            .await;
                    };
                    // Catalog lookup: the value is a verbatim echo of what
                    // we pushed, so it must be present.
                    let models = self.backend.list_models().await.unwrap_or_default();
                    let found = models
                        .iter()
                        .find(|m| m.providerID == provider && m.id == model_id)
                        .cloned();
                    let Some(model) = found else {
                        return self
                            .reject_config_value(
                                &req,
                                responder,
                                &entry,
                                &cx,
                                format!("unknown model {v:?}: not in the model catalog"),
                            )
                            .await;
                    };
                    let model_ref = dto::ModelRef {
                        id: model.id.clone(),
                        providerID: model.providerID.clone(),
                        variant: None,
                    };
                    if let Err(e) = self.backend.set_model(&req.session_id.0, &model_ref).await {
                        tracing::error!(error = %e, session = %req.session_id, model = %value, "set_model failed");
                        return self
                            .reject_config_value(
                                &req,
                                responder,
                                &entry,
                                &cx,
                                format!("model switch failed: {e}"),
                            )
                            .await;
                    }
                    // Track BEFORE responding: the server's own-switch echo
                    // (`session.model.selected`) then diffs to zero against
                    // the tracked model and stays suppressed.
                    *entry.model.lock().expect("model lock") = Some(model_ref);
                    tracing::info!(session = %req.session_id, model = %value, "set_config_option(model) -> opencode");
                    let fresh = self.current_config_options(&entry).await;
                    responder.respond(acp::SetSessionConfigOptionResponse::new(fresh))?;
                    self.push_config_options(&req.session_id, &entry, &cx).await;
                }
            },
            "agent" => {
                if value.is_empty() {
                    return self
                        .reject_config_value(&req, responder, &entry, &cx, "invalid agent value: empty".into())
                        .await;
                }
                if let Err(e) = self.backend.set_agent(&req.session_id.0, value).await {
                    tracing::error!(error = %e, session = %req.session_id, mode = %value, "set_agent failed");
                    return responder.respond_with_internal_error(format!(
                        "opencode agent switch failed: {e}"
                    ));
                }
                let mode = value.to_string();
                *entry.mode.lock().expect("mode lock") = Some(mode.clone());
                tracing::info!(session = %req.session_id, mode, "set_config_option(agent) -> opencode agent");
                let fresh = self.current_config_options(&entry).await;
                responder.respond(acp::SetSessionConfigOptionResponse::new(fresh))?;
                // Same semantics as set_mode: current_mode_update for the
                // modes surface + the config-options push for the pickers.
                cx.send_notification(acp::SessionNotification::new(
                    req.session_id.clone(),
                    acp::SessionUpdate::CurrentModeUpdate(acp::CurrentModeUpdate::new(mode)),
                ))?;
                self.push_config_options(&req.session_id, &entry, &cx).await;
            }
            other => {
                return responder.respond_with_error(
                    AcpError::invalid_params().data(serde_json::json!({
                        "message": format!("set_config_option: unknown config option {other:?}")
                    })),
                );
            }
        }
        Ok(())
    }

    /// Error response for an invalid config-option value, followed by a
    /// current-state push — the client self-corrects (e.g. a stale
    /// Zed-persisted default no longer in the catalog renders its value
    /// again instead of leaving the picker stuck).
    async fn reject_config_value(
        self: &Arc<Self>,
        req: &acp::SetSessionConfigOptionRequest,
        responder: Responder<acp::SetSessionConfigOptionResponse>,
        entry: &SessionEntry,
        cx: &ConnectionTo<Client>,
        message: String,
    ) -> Result<(), AcpError> {
        responder.respond_with_error(AcpError::invalid_params().data(serde_json::json!({ "message": message })))?;
        let state = self.current_config_options(entry).await;
        if !state.is_empty() {
            let update =
                acp::SessionUpdate::ConfigOptionUpdate(acp::ConfigOptionUpdate::new(state));
            let _ = cx.send_notification(acp::SessionNotification::new(req.session_id.clone(), update));
        }
        Ok(())
    }

    /// Wave 6a `session/delete`: drop the registry entry and delete the
    /// opencode session. Empty response `{}` on success.
    async fn delete_session(
        &self,
        req: acp::DeleteSessionRequest,
        responder: Responder<acp::DeleteSessionResponse>,
    ) -> Result<(), AcpError> {
        self.sessions.lock().expect("sessions lock").remove(&req.session_id);
        match self.backend.delete_session(&req.session_id.0).await {
            Ok(()) => {
                tracing::info!(session = %req.session_id, "ACP session/delete -> opencode");
                responder.respond(acp::DeleteSessionResponse::new())
            }
            Err(e) => {
                tracing::error!(error = %e, session = %req.session_id, "backend delete_session failed");
                responder.respond_with_internal_error(format!(
                    "opencode session deletion failed: {e}"
                ))
            }
        }
    }

    async fn load_session(
        self: &Arc<Self>,
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
        for update in replay::replay_updates(&records, self.no_aft) {
            cx.send_notification(acp::SessionNotification::new(
                req.session_id.clone(),
                update,
            ))?;
        }
        // Wave 6b: current mode = the last assistant message's agent
        // (wire fact: assistant messages carry `agent`); no assistant
        // message yet → the derived default (first visible primary in wire
        // order). Mode list + default both come from the one catalog fetch.
        let agents = self.fetch_agents(&req.cwd.to_string_lossy()).await;
        // Release 0.3.0: the session record's agent/model are authoritative
        // (post-switch state); failures/absent fields fall back to the
        // message/catalog data (never fails the lifecycle).
        let session_info = match self.backend.get_session(&req.session_id.0).await {
            Ok(info) => Some(info),
            Err(e) => {
                tracing::warn!(error = %e, session = %req.session_id, "load: get_session failed (agent/model fall back to messages/catalog)");
                None
            }
        };
        let models = if self.config_options_supported.load(Ordering::SeqCst) {
            self.backend.list_models().await
        } else {
            None
        };
        // The honest last-assistant agent wins even when it drifted out of
        // the catalog (step.started self-heals); otherwise the derived
        // default.
        let current = session_info
            .as_ref()
            .and_then(|i| i.agent.clone())
            .or_else(|| last_assistant_agent(&records))
            .or_else(|| default_mode_id(&agents));
        let current_model = session_info
            .as_ref()
            .and_then(|i| i.model.clone())
            .or_else(|| last_assistant_model(&records));
        let modes = to_session_modes(&agents);
        // Refresh state so a subsequent prompt on this session works.
        self.sessions.lock().expect("sessions lock").insert(
            req.session_id.clone(),
            Arc::new(SessionEntry {
                cancel: AtomicBool::new(false),
                cwd: req.cwd.to_string_lossy().to_string(),
                mode: Mutex::new(current.clone()),
                model: Mutex::new(current_model.clone()),
            }),
        );
        // No pickable modes → omit the payload (Zed renders no picker
        // instead of an unmatched "Unknown" current mode).
        let mut response = if modes.is_empty() {
            acp::LoadSessionResponse::new()
        } else {
            acp::LoadSessionResponse::new().modes(acp::SessionModeState::new(
                current.clone().expect("modes non-empty ⇒ a derived default exists"),
                modes,
            ))
        };
        // Release 0.3.0: config options (agent + model pickers), capability
        // gated and per-option degraded exactly like newSession.
        if self.config_options_supported.load(Ordering::SeqCst) {
            let options = build_config_options(
                &agents,
                models.as_deref().unwrap_or(&[]),
                current.as_deref(),
                current_model.as_ref(),
            );
            if !options.is_empty() {
                response = response.config_options(options);
            }
        }
        responder.respond(response)?;
        // Wave 6a: initial `available_commands_update` push, after the
        // response (spawned — never gate the load).
        self.spawn_commands_push(&req.session_id, &cx);
        Ok(())
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

        let mut state = updates::MappingState::new().with_no_aft(self.no_aft);
        // Child (subagent) sessions of this session, registered from
        // `session.created {parentID}` — their tool events project into this
        // turn as nested ACP tool calls (`${child.id}:` / `${child.title}:
        //` prefixes, official #48232 behavior).
        let mut children: Vec<updates::ToolNs> = Vec::new();
        // Cancel drain: after `session/cancel` the loop keeps consuming the
        // stream for up to `drain_window` (official ~5s wind-down), then
        // abandons still-open tool calls as failed.
        let mut draining = false;
        let mut drain_deadline = std::time::Instant::now();
        loop {
            // Next event. Drain mode bounds the wait so a silent server
            // cannot hang the cancel forever.
            let event = if draining {
                let remaining =
                    drain_deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, stream.next()).await {
                    Ok(Some(event)) => event,
                    _ => break,
                }
            } else {
                match stream.next().await {
                    Some(event) => event,
                    None => break,
                }
            };

            // ---------- session filter ----------
            // First: register children of THIS session (the wire emits
            // `session.created` before any child event). Nothing else maps
            // from `session.created`, so registration is the whole handling.
            if let dto::SessionEvent::SessionCreated(created) = &event {
                let Some((child_id, child_title)) = created
                    .parentID
                    .as_deref()
                    .filter(|parent_id| *parent_id == req.session_id.0.as_ref())
                    .and(created.sessionID.as_deref())
                    .map(|child_id| {
                        (child_id.to_string(), created.title.clone().unwrap_or_default())
                    })
                else {
                    continue;
                };
                tracing::info!(
                    session = %req.session_id,
                    child = %child_id,
                    title = %child_title,
                    "child session registered for projection"
                );
                children.push(updates::ToolNs { child_id, child_title });
                continue;
            }
            // Then drop other sessions' traffic — except registered
            // children, whose events ride the same stream under their own
            // (child) sessionID (wire-verified in subagent-child fixture).
            let is_child_event = updates::event_session_id(&event)
                .map(|sid| {
                    sid != req.session_id.0.as_ref()
                        && children.iter().any(|c| c.child_id == sid)
                })
                .unwrap_or(false);
            if matches!(
                updates::event_session_id(&event),
                Some(sid) if sid != req.session_id.0.as_ref() && !is_child_event
            ) {
                continue;
            }

            tracing::debug!(session = %req.session_id, event = ?event, "sse event");

            // ---------- cancel → drain ----------
            // `session/cancel` won (the interrupt call itself was already
            // made by the cancel handler): keep consuming in-flight events
            // within the drain window instead of cutting the stream.
            if !draining && entry.cancel.load(Ordering::Acquire) {
                draining = true;
                drain_deadline = std::time::Instant::now() + self.drain_window;
                tracing::info!(
                    session = %req.session_id,
                    window_ms = self.drain_window.as_millis(),
                    "turn cancelled via session/cancel — draining in-flight events"
                );
            }

            // ---------- mode/model tracking (Wave 6b + 0.3.0) ----------
            // `session.agent.selected` / `session.model.selected` (own-switch
            // echo + remote switches) and `step.started.agent` /
            // `step.started.model` (self-heal: the server ran a different
            // agent/model than tracked — e.g. a config default change) drive
            // the tracked values; `current_mode_update` fires only on an
            // ACTUAL agent change and the `config_option_update` push fires
            // only on an actual change of either. Own-switch echo
            // suppression falls out of this: set_mode/set_config_option
            // updated the tracked values first, so the echo diffs to zero.
            // Only the parent session's events are tracked (children's ride
            // their own sessionID and are dropped by the session filter
            // above; during the cancel drain pushes are muted).
            if !draining {
                let event_agent: Option<&str> = match &event {
                    dto::SessionEvent::AgentSelected(sel)
                        if sel.sessionID == req.session_id.0.as_ref() =>
                    {
                        Some(&sel.agent)
                    }
                    dto::SessionEvent::StepStarted(s)
                        if s.session.sessionID == req.session_id.0.as_ref() =>
                    {
                        s.agent.as_deref()
                    }
                    _ => None,
                };
                let event_model: Option<&dto::ModelRef> = match &event {
                    dto::SessionEvent::ModelSelected(sel)
                        if sel.sessionID == req.session_id.0.as_ref() =>
                    {
                        Some(&sel.model)
                    }
                    dto::SessionEvent::StepStarted(s)
                        if s.session.sessionID == req.session_id.0.as_ref() =>
                    {
                        s.model.as_ref()
                    }
                    _ => None,
                };
                let mut any_changed = false;
                if let Some(agent) = event_agent {
                    let changed = {
                        let mut mode = entry.mode.lock().expect("mode lock");
                        if mode.as_deref() != Some(agent) {
                            *mode = Some(agent.to_string());
                            true
                        } else {
                            false
                        }
                    };
                    if changed {
                        any_changed = true;
                        tracing::info!(session = %req.session_id, agent, "current mode -> {agent}");
                        cx.send_notification(acp::SessionNotification::new(
                            req.session_id.clone(),
                            acp::SessionUpdate::CurrentModeUpdate(
                                acp::CurrentModeUpdate::new(agent.to_string()),
                            ),
                        ))?;
                    } else {
                        tracing::debug!(
                            session = %req.session_id,
                            agent,
                            "agent echo suppressed (tracked mode unchanged)"
                        );
                    }
                }
                if let Some(model) = event_model {
                    let changed = {
                        let mut tracked = entry.model.lock().expect("model lock");
                        if tracked
                            .as_ref()
                            .map(|m| (m.providerID.as_str(), m.id.as_str()))
                            != Some((model.providerID.as_str(), model.id.as_str()))
                        {
                            *tracked = Some(model.clone());
                            true
                        } else {
                            false
                        }
                    };
                    if changed {
                        any_changed = true;
                        tracing::info!(
                            session = %req.session_id,
                            model = %format!("{}/{}", model.providerID, model.id),
                            "current model -> {}/{}",
                            model.providerID,
                            model.id
                        );
                    } else {
                        tracing::debug!(
                            session = %req.session_id,
                            model = %format!("{}/{}", model.providerID, model.id),
                            "model echo suppressed (tracked model unchanged)"
                        );
                    }
                }
                if any_changed {
                    // Full config-options push (both options, current
                    // values) so the model/agent pickers stay honest.
                    self.push_config_options(&req.session_id, &entry, &cx).await;
                }
                // `AgentSelected` / `ModelSelected` have no ACP update
                // mapping — consume them here; `StepStarted` continues to
                // the mapping below (retry-clear bookkeeping).
                if matches!(
                    event,
                    dto::SessionEvent::AgentSelected(_) | dto::SessionEvent::ModelSelected(_)
                ) {
                    continue;
                }
            }

            // ---------- permission bridging (Wave 3) ----------
            // `permission.asked` is a turn-level signal, not an update: ask
            // the ACP client and forward the decision before the turn can
            // resume. The server holds the execution until we reply. A child
            // ask carries the CHILD's sessionID (wire-verified) — the client
            // request is addressed to the client's own (parent) session, the
            // reply goes to the ask's sessionID (the #48232 routing rule).
            // During the cancel drain asks are auto-rejected: the user
            // already cancelled, and the client would auto-cancel the prompt
            // anyway — but the server reply must still be sent
            // (uninterruptible reply rule).
            if let dto::SessionEvent::PermissionAsked(asked) = &event {
                if draining {
                    if let Err(e) = self
                        .backend
                        .permission_reply(&asked.sessionID, &asked.id, dto::PermissionReply::Reject)
                        .await
                    {
                        tracing::warn!(
                            error = %e,
                            session = %asked.sessionID,
                            request = %asked.id,
                            "drain-time permission reject failed"
                        );
                    }
                } else {
                    let ns = children.iter().find(|c| c.child_id == asked.sessionID);
                    self.forward_permission(asked, ns, &req.session_id, &mut state, &entry.cwd, &cx)
                        .await;
                }
                continue;
            }

            // ---------- child projection ----------
            // Child lifecycle/text/reasoning events are not surfaced; tool
            // events project as nested tool calls under the child namespace.
            if is_child_event {
                if let Some(ns) = children.iter().find(|c| {
                    updates::event_session_id(&event) == Some(c.child_id.as_str())
                }) {
                    for update in updates::to_child_updates(&event, ns, &mut state) {
                        cx.send_notification(acp::SessionNotification::new(
                            req.session_id.clone(),
                            update,
                        ))?;
                    }
                }
                continue;
            }

            // ---------- turn end ----------
            // While draining, the first parent terminal event ends the drain
            // — the cancel already won, the response is Cancelled either way.
            if draining && updates::stop_update(&event).is_some() {
                self.abandon_open_tools(&req, &state, &cx)?;
                return responder.respond(self.stop_response(acp::StopReason::Cancelled, &state));
            }
            match updates::stop_update(&event) {
                Some(updates::TurnEnd::EndTurn) => {
                    tracing::info!(session = %req.session_id, "turn ended (end_turn)");
                    return responder
                        .respond(self.stop_response(acp::StopReason::EndTurn, &state));
                }
                Some(updates::TurnEnd::Cancelled) => {
                    // `session.execution.interrupted` (official cancellation
                    // path) or an `aborted`-kind failure.
                    tracing::info!(session = %req.session_id, "turn cancelled (interrupted)");
                    self.abandon_open_tools(&req, &state, &cx)?;
                    return responder
                        .respond(self.stop_response(acp::StopReason::Cancelled, &state));
                }
                Some(updates::TurnEnd::MaxTokens) => {
                    // `length` failure — official mapping to max_tokens.
                    tracing::info!(session = %req.session_id, "turn ended (max_tokens)");
                    return responder
                        .respond(self.stop_response(acp::StopReason::MaxTokens, &state));
                }
                Some(updates::TurnEnd::Refusal) => {
                    // `content-filter` failure — official mapping to refusal.
                    tracing::info!(session = %req.session_id, "turn ended (refusal)");
                    return responder
                        .respond(self.stop_response(acp::StopReason::Refusal, &state));
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
                    // Wave 4: catalog reload pushes. `model.updated` /
                    // `provider.updated` carry no session (`{}`) and reach
                    // every active turn loop; the push targets the client's
                    // active session.
                    if matches!(
                        &event,
                        dto::SessionEvent::ModelUpdated(_) | dto::SessionEvent::ProviderUpdated(_)
                    ) {
                        self.push_config_options(&req.session_id, &entry, &cx).await;
                    }
                    // Wave 4: step-level error taxonomy (log-only — v1 has
                    // no per-step failure update; the turn outcome arrives
                    // via `session.execution.failed`).
                    if let Some(outcome) = updates::step_failed_outcome(&event) {
                        tracing::info!(
                            session = %req.session_id,
                            step_outcome = ?outcome,
                            "step failed"
                        );
                    }
                    for update in updates::to_updates(&event, &mut state) {
                        cx.send_notification(acp::SessionNotification::new(
                            req.session_id.clone(),
                            update,
                        ))?;
                    }
                }
            }
        }

        // Stream ended without a terminal event: connection lost, the
        // backend did not emit execution.succeeded/failed — or the cancel
        // drain window elapsed. Both end cancelled when draining.
        if draining {
            self.abandon_open_tools(&req, &state, &cx)?;
            return responder.respond(self.stop_response(acp::StopReason::Cancelled, &state));
        }
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
    ///
    /// Wave 4: `ns` is `Some` for asks raised inside a child session — the
    /// client request is addressed to the parent session (the client only
    /// knows one session), the tool call id/title get the child namespace
    /// prefix, and the backend reply goes to the ask's OWN (child) sessionID
    /// — the #48232 routing rule (wire-verified: child asks carry the child
    /// sessionID).
    async fn forward_permission(
        self: &Arc<Self>,
        asked: &dto::PermissionAsked,
        ns: Option<&updates::ToolNs>,
        parent_session_id: &acp::SessionId,
        state: &mut updates::MappingState,
        cwd: &str,
        cx: &ConnectionTo<Client>,
    ) {
        // Tool-call identity: `source.id` (the call_* id of the triggering
        // tool call); fall back to the permission request id. Child asks
        // come prefixed with the child namespace.
        let tool_call_id = asked
            .source
            .as_ref()
            .map(|s| s.id.clone())
            .unwrap_or_else(|| asked.id.clone());
        let tool_call_id = ns
            .map(|n| n.tool_call_id(&tool_call_id))
            .unwrap_or(tool_call_id);
        // Title: "<action>: <first resource>" — e.g. "shell: echo hi".
        // Child asks get the `${child.title}: …` prefix.
        let title = match asked.resources.first() {
            Some(resource) => format!("{}: {}", asked.action, resource),
            None => asked.action.clone(),
        };
        let title = ns.map(|n| n.title(&title)).unwrap_or(title);
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
            // The client's session — the PARENT session for child asks (the
            // client never sees child session ids on the wire).
            parent_session_id.clone(),
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

    /// Abandon still-open tool calls as failed ("Cancelled") — official
    /// cancel-drain behavior. No-op when nothing is open. Must run before
    /// the prompt response so the client sees the terminal updates first.
    fn abandon_open_tools(
        self: &Arc<Self>,
        req: &acp::PromptRequest,
        state: &updates::MappingState,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), AcpError> {
        for update in state.abandon_open_tools() {
            cx.send_notification(acp::SessionNotification::new(req.session_id.clone(), update))?;
        }
        Ok(())
    }

    /// The prompt response for a turn end — folds a pending retry into the
    /// `_meta` (`{"opencode/retry": …}`), official behavior: the retry is
    /// announced so the client can reflect "will retry" even if the turn
    /// ended before the retry fired.
    fn stop_response(
        &self,
        reason: acp::StopReason,
        state: &updates::MappingState,
    ) -> acp::PromptResponse {
        let response = acp::PromptResponse::new(reason);
        match state.retry_meta() {
            Some(meta) => {
                let mut response = response;
                response.meta = Some(serde_json::Map::from_iter([(
                    "opencode/retry".to_string(),
                    meta.clone(),
                )]));
                response
            }
            None => response,
        }
    }

    /// Wave 6a: initial `available_commands_update` push (full-replacement
    /// semantics) after a session lifecycle response — newSession, loadSession
    /// and resume each push once, targeted at the new/loaded session. The
    /// wire catalog is global (`GET /api/command` carries no directory), so
    /// the fetch is per-push and the cwd scoping is the session the push
    /// targets. Sent on a spawned task so the lifecycle response is never
    /// gated on the catalog fetch.
    fn spawn_commands_push(self: &Arc<Self>, session_id: &acp::SessionId, cx: &ConnectionTo<Client>) {
        let session_id = session_id.clone();
        let svc = Arc::clone(self);
        let task_cx = cx.clone();
        let log_id = session_id.clone();
        if let Err(e) = cx.clone().spawn(async move {
            svc.push_available_commands(&session_id, &task_cx).await;
            Ok(())
        }) {
            tracing::warn!(error = %e, session = %log_id, "commands push spawn failed");
        }
    }

    /// The push itself: fetch the command catalog and send one
    /// `available_commands_update` (full replacement). opencode commands have
    /// no input schema, so ACP `input` is omitted. Degrades gracefully: no
    /// catalog or an empty one → warn/skip, never an error (session
    /// establishment is unaffected).
    async fn push_available_commands(self: &Arc<Self>, session_id: &acp::SessionId, cx: &ConnectionTo<Client>) {
        // Fetch failures surface as `None` (the trait's Option return cannot
        // express the error distantly); warn per the drop-degradation
        // contract so the skip is visible in logs.
        let Some(commands) = self.backend.list_commands().await else {
            tracing::warn!(session = %session_id, "commands push skipped: no command catalog");
            return;
        };
        if commands.is_empty() {
            return;
        }
        // Command shape on the wire: `{name, description?}` (Command.Info,
        // live-probed; `changelog`-style entries carry no description).
        // ACP requires a description — default to "". No input schema.
        let available: Vec<acp::AvailableCommand> = commands
            .iter()
            .filter_map(|c| {
                let name = c.get("name")?.as_str()?;
                let description = c
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string();
                Some(acp::AvailableCommand::new(name.to_string(), description))
            })
            .collect();
        if available.is_empty() {
            return;
        }
        let update =
            acp::SessionUpdate::AvailableCommandsUpdate(acp::AvailableCommandsUpdate::new(available));
        tracing::info!(session = %session_id, "available_commands_update push");
        let _ = cx.send_notification(acp::SessionNotification::new(session_id.clone(), update));
    }

    /// Release 0.3.0: fetch the catalogs (agents for the session's cwd +
    /// the model catalog) and build the full config-options state — BOTH
    /// options with their current values. Capability-gated; per-option
    /// degrade: a catalog that failed to fetch or came back empty omits its
    /// option (never a blank/unmatched current value).
    async fn current_config_options(self: &Arc<Self>, entry: &SessionEntry) -> Vec<acp::SessionConfigOption> {
        if !self.config_options_supported.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let agents = match self.backend.agents(&entry.cwd).await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "config options: agents fetch failed — agent option omitted");
                Vec::new()
            }
        };
        let models = self.backend.list_models().await;
        let mode = entry.mode.lock().expect("mode lock").clone();
        let model = entry.model.lock().expect("model lock").clone();
        build_config_options(
            &agents,
            models.as_deref().unwrap_or(&[]),
            mode.as_deref(),
            model.as_ref(),
        )
    }

    /// Release 0.3.0: push `config_option_update` (the FULL state — agent +
    /// model options with current values) after any tracked-value change:
    /// catalog reloads (`model.updated` / `provider.updated` — both carry
    /// `{}`; the catalogs are re-fetched), `session.model.selected` remote
    /// switches, `step.started` self-heals and set_mode / set_config_option.
    /// Skipped when the capability is absent, both catalogs are unavailable
    /// or the current agent drifted out of the visible list (the option
    /// would render "Unknown").
    async fn push_config_options(
        self: &Arc<Self>,
        session_id: &acp::SessionId,
        entry: &SessionEntry,
        cx: &ConnectionTo<Client>,
    ) {
        let options = self.current_config_options(entry).await;
        if options.is_empty() {
            return;
        }
        let update = acp::SessionUpdate::ConfigOptionUpdate(acp::ConfigOptionUpdate::new(options));
        tracing::info!(session = %session_id, "config_option_update push");
        let _ = cx.send_notification(acp::SessionNotification::new(session_id.clone(), update));
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

// ============================================================
// Wave 6a mapping helpers (session/list)
// ============================================================

/// Map one opencode wire session into ACP `SessionInfo`.
///
/// - `session_id` / `cwd`: the wire `id` + `location.directory` (the ACP
///   type requires a cwd; sessions returned for a filtered list fall back to
///   the request filter, then to empty — the wire always carries a location
///   for real sessions).
/// - `title`: passthrough.
/// - `updated_at`: the wire `time.updated` (epoch **milliseconds**) converted
///   to ISO 8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`); absent → `None`.
/// - `additional_directories`: not modeled on the wire — stays empty
///   (omitted from the response via the crate's skip-serialization).
fn to_acp_session_info(session: &dto::SessionInfo, fallback_cwd: Option<&str>) -> acp::SessionInfo {
    let cwd = session
        .location
        .as_ref()
        .map(|l| l.directory.as_str())
        .or(fallback_cwd)
        .unwrap_or_default();
    let updated_at = session
        .time
        .as_ref()
        .and_then(|t| t.get("updated"))
        .and_then(|u| {
            u.as_i64()
                .or_else(|| u.as_u64().map(|v| v as i64))
                .or_else(|| u.as_f64().map(|v| v as i64))
        })
        .and_then(epoch_ms_to_iso);
    acp::SessionInfo::new(session.id.clone(), cwd)
        .title(session.title.clone())
        .updated_at(updated_at)
}

/// Epoch milliseconds → RFC 3339 UTC `YYYY-MM-DDTHH:MM:SSZ` (no sub-second
/// precision), via Howard Hinnant's civil-from-days algorithm. No `chrono`/
/// `time` dependency on purpose (both are absent from Cargo.toml and the
/// wave may not touch it); the 40-line algorithm is fully unit-tested.
pub fn epoch_ms_to_iso(epoch_ms: i64) -> Option<String> {
    let secs = epoch_ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (h, min, s) = (secs_of_day / 3600, (secs_of_day % 3600) / 60, secs_of_day % 60);

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    Some(format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}Z"))
}

/// Wave 6b: filter the wire agent catalog into the ACP mode list. Wire
/// rules (2.0.21): modes are `mode ∈ {primary, all}` (subagents are
/// `subagent`), and `hidden == false` (compaction/title/dreamer-* are
/// hidden internals). Sorted by wire order (the server returns them in
/// config order: orchestrator, build, …).
fn to_session_modes(agents: &[dto::AgentInfo]) -> Vec<acp::SessionMode> {
    agents
        .iter()
        .filter(|a| !a.hidden && matches!(a.mode.as_deref(), Some("primary") | Some("all")))
        .map(|a| {
            let mut mode = acp::SessionMode::new(a.id.clone(), a.name.clone());
            if let Some(d) = &a.description {
                mode = mode.description(d.clone());
            }
            mode
        })
        .collect()
}

/// The default agent id, derived from the wire catalog (opencode semantics:
/// the first visible `primary` agent in wire order; the stock 2.0.21 catalog
/// lists `orchestrator` first). Falls back to the first agent that would
/// surface as a mode ([`to_session_modes`]'s filter: `mode ∈ {primary, all}`
/// && `!hidden`), and to `None` when the catalog yields no modes at all.
///
/// `None` ⇒ callers must omit the ACP `modes` payload entirely, so clients
/// (Zed) render no mode picker instead of an unmatched "Unknown" current
/// mode when the server's catalog differs from the stock one.
fn default_mode_id(agents: &[dto::AgentInfo]) -> Option<String> {
    agents
        .iter()
        .find(|a| !a.hidden && a.mode.as_deref() == Some("primary"))
        .or_else(|| {
            agents
                .iter()
                .find(|a| !a.hidden && matches!(a.mode.as_deref(), Some("primary") | Some("all")))
        })
        .map(|a| a.id.clone())
}

/// Wave 6b: the last assistant message's `agent` field (the currentModeId
/// source for load/resume), or `None` when the session has no assistant
/// message yet (callers fall back to the derived default agent).
fn last_assistant_agent(records: &[dto::MessageRecord]) -> Option<String> {
    records
        .iter()
        .rev()
        .find(|r| r.kind == "assistant")
        .and_then(|r| r.agent.clone())
}

/// Release 0.3.0: the last assistant message's `model` field (the model
/// config-option current value source for load/resume when the session
/// record carried none), or `None` when the session has no assistant message
/// yet.
fn last_assistant_model(records: &[dto::MessageRecord]) -> Option<dto::ModelRef> {
    records
        .iter()
        .rev()
        .find(|r| r.kind == "assistant")
        .and_then(|r| r.model.clone())
}

/// Release 0.3.0: build the ACP `config_options` payload — the agent picker
/// (category Mode, ungrouped, the same visible-agent filter as
/// [`to_session_modes`], current = the tracked agent id) and the model
/// picker (category Model, grouped by provider, value scheme
/// `<provider>/<model>`, current = the tracked model).
///
/// Degrade contract (mirrors the modes payload): an empty agents catalog —
/// fetch failure or no pickable agents — omits the agent option; an empty
/// models catalog omits the model option; neither present → empty vec
/// (callers omit the field). A config option is NEVER emitted with a
/// blank/unmatched current value: the agent option additionally requires the
/// current agent to be among the listed values.
///
/// The model's UNKNOWN state (`current_model == None`, e.g. newSession — the
/// default model is not discoverable pre-first-turn) surfaces as the
/// synthetic value `"__default__"` with a `"Default"` option PREPENDED to the
/// first group. There is no server API to unset a model, so once the model is
/// concrete the Default option is not listed.
fn build_config_options(
    agents: &[dto::AgentInfo],
    models: &[dto::ModelInfo],
    current_agent: Option<&str>,
    current_model: Option<&dto::ModelRef>,
) -> Vec<acp::SessionConfigOption> {
    let mut options = Vec::with_capacity(2);

    // ---- agent option (category Mode) ----
    let visible: Vec<&dto::AgentInfo> = agents
        .iter()
        .filter(|a| !a.hidden && matches!(a.mode.as_deref(), Some("primary") | Some("all")))
        .collect();
    if let Some(current) = current_agent {
        if visible.iter().any(|a| a.id == current) {
            let select_options: Vec<acp::SessionConfigSelectOption> = visible
                .iter()
                .map(|a| acp::SessionConfigSelectOption::new(a.id.clone(), a.name.clone()))
                .collect();
            options.push(
                acp::SessionConfigOption::select(
                    "agent",
                    "Agent",
                    current.to_string(),
                    acp::SessionConfigSelectOptions::Ungrouped(select_options),
                )
                .category(acp::SessionConfigOptionCategory::Mode),
            );
        }
    }

    // ---- model option (category Model) ----
    if !models.is_empty() {
        // Grouped by providerID, first-seen order (the catalog is ordered).
        let mut groups: Vec<acp::SessionConfigSelectGroup> = Vec::new();
        for m in models {
            let entry = groups
                .iter_mut()
                .find(|g| g.group.0.as_ref() == m.providerID);
            let name = m
                .name
                .clone()
                .unwrap_or_else(|| m.modelID.clone().unwrap_or_else(|| m.id.clone()));
            let option = acp::SessionConfigSelectOption::new(
                format!("{}/{}", m.providerID, m.id),
                name,
            );
            match entry {
                Some(group) => group.options.push(option),
                None => groups.push(acp::SessionConfigSelectGroup::new(
                    m.providerID.clone(),
                    m.providerID.clone(),
                    vec![option],
                )),
            }
        }
        let current = current_model
            .map(|m| format!("{}/{}", m.providerID, m.id))
            .unwrap_or_else(|| "__default__".to_string());
        if current_model.is_none() {
            // UNKNOWN current model: list the synthetic Default option FIRST
            // so the current value is a listed option (an unmatched value
            // renders "Unknown" in Zed).
            if let Some(first) = groups.first_mut() {
                first.options.insert(
                    0,
                    acp::SessionConfigSelectOption::new("__default__", "Default"),
                );
            }
        }
        options.push(
            acp::SessionConfigOption::select(
                "model",
                "Model",
                current,
                acp::SessionConfigSelectOptions::Grouped(groups),
            )
            .category(acp::SessionConfigOptionCategory::Model),
        );
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::schema::v1::{
        CancelNotification, ClientCapabilities, ClientSessionCapabilities, ContentBlock,
        InitializeRequest, LoadSessionRequest, NewSessionRequest, PromptRequest,
        SessionConfigOptionsCapabilities, SessionNotification, TextContent,
    };
    use tokio::sync::{broadcast, oneshot};

    // ---------------- mock backend ----------------

    struct MockBackend {
        /// Wave 6b: event bus for the turn loop. A broadcast sender carries
    /// mid-turn pushes to the LIVE stream; pushes made while no stream is
    /// subscribed (between turns) land in `pending` for the next prompt's
    /// stream. Multi-turn tests get exact per-turn delivery: no loss, no
    /// stale replay.
    events_tx: Mutex<broadcast::Sender<dto::SessionEvent>>,
    pending: Mutex<Vec<dto::SessionEvent>>,
        messages_out: Mutex<Option<Vec<dto::MessageRecord>>>,
        /// Wave 6a: set by `messages()` — resume must never call it.
        messages_called: AtomicBool,
        interrupted: AtomicBool,
        interrupt_seen: Mutex<Option<oneshot::Sender<()>>>,
        /// Recorded (session_id, request_id, decision) of permission replies.
        permission_replies: Mutex<Vec<(String, String, dto::PermissionReply)>>,
        permission_seen: Mutex<Option<oneshot::Sender<()>>>,
        /// Wave 4: canned model catalog for `config_option_update` pushes.
        models: Mutex<Option<Vec<dto::ModelInfo>>>,
        /// Wave 6a: canned session list for `session/list`.
        sessions_out: Mutex<Option<Vec<dto::SessionInfo>>>,
        /// Wave 6a: every (directory, cursor) pair handed to `list_sessions`.
        list_calls: Mutex<Vec<(Option<String>, Option<String>)>>,
        /// Wave 6a: session ids passed to `delete_session`.
        deleted: Mutex<Vec<String>>,
        /// Wave 6a: canned command catalog for `available_commands_update`.
        commands: Mutex<Option<Vec<serde_json::Value>>>,
        /// Wave 6a: optional gate — `list_commands` blocks until released
        /// (proves the lifecycle response is not gated on the catalog fetch).
        commands_gate: Mutex<Option<oneshot::Receiver<()>>>,
        /// Wave 6b: canned agent catalog (the ACP mode list source).
        agents_out: Mutex<Option<Vec<dto::AgentInfo>>>,
        /// Wave 6b: force `agents()` to fail (proves the degrade path).
        agents_fail: AtomicBool,
        /// Wave 6b: every (session_id, agent) passed to `set_agent`.
        set_agent_calls: Mutex<Vec<(String, String)>>,
        /// Release 0.3.0: canned session record for `get_session` (the
        /// authoritative agent/model sources for load/resume).
        session_out: Mutex<Option<dto::SessionInfo>>,
        /// Release 0.3.0: session ids passed to `get_session`.
        get_session_calls: Mutex<Vec<String>>,
        /// Release 0.3.0: every (session_id, model) passed to `set_model`.
        set_model_calls: Mutex<Vec<(String, dto::ModelRef)>>,
        /// Release 0.3.0: force `set_model` to fail (proves the error+push
        /// degrade path).
        set_model_fail: AtomicBool,
    }

    impl MockBackend {
        fn new() -> Arc<Self> {
            let (tx, _rx) = broadcast::channel(1024);
            Arc::new(Self {
                events_tx: Mutex::new(tx),
                pending: Mutex::new(Vec::new()),
                messages_out: Mutex::new(None),
                messages_called: AtomicBool::new(false),
                interrupted: AtomicBool::new(false),
                interrupt_seen: Mutex::new(None),
                permission_replies: Mutex::new(Vec::new()),
                permission_seen: Mutex::new(None),
                models: Mutex::new(None),
                sessions_out: Mutex::new(None),
                list_calls: Mutex::new(Vec::new()),
                deleted: Mutex::new(Vec::new()),
                commands: Mutex::new(None),
                commands_gate: Mutex::new(None),
                agents_out: Mutex::new(None),
                agents_fail: AtomicBool::new(false),
                set_agent_calls: Mutex::new(Vec::new()),
                session_out: Mutex::new(None),
                get_session_calls: Mutex::new(Vec::new()),
                set_model_calls: Mutex::new(Vec::new()),
                set_model_fail: AtomicBool::new(false),
            })
        }

        fn push(&self, event: dto::SessionEvent) {
            // Both copies, deliberately: `pending` is drained by the next
            // stream (events pushed between turns, or while the previous
            // stream's drop is still settling), the broadcast reaches a
            // mid-turn push to the running stream. A push consumed by a
            // live stream leaves a pending copy that only matters if
            // another turn starts before it is drained — no test does that.
            self.pending.lock().expect("pending lock").push(event.clone());
            let _ = self.events_tx.lock().expect("tx lock").send(event);
        }

        fn set_messages(&self, records: Vec<dto::MessageRecord>) {
            *self.messages_out.lock().expect("messages lock") = Some(records);
        }

        fn set_sessions(&self, sessions: Vec<dto::SessionInfo>) {
            *self.sessions_out.lock().expect("sessions lock") = Some(sessions);
        }

        fn recorded_list_calls(&self) -> Vec<(Option<String>, Option<String>)> {
            self.list_calls.lock().expect("list lock").clone()
        }

        fn recorded_deleted(&self) -> Vec<String> {
            self.deleted.lock().expect("deleted lock").clone()
        }

        fn set_commands(&self, commands: Vec<serde_json::Value>) {
            *self.commands.lock().expect("commands lock") = Some(commands);
        }

        fn set_agents(&self, agents: Vec<dto::AgentInfo>) {
            *self.agents_out.lock().expect("agents lock") = Some(agents);
        }

        /// Make the next `agents()` calls fail (the lifecycle responses must
        /// degrade to an omitted modes payload, not fail).
        fn fail_agents(&self) {
            self.agents_fail.store(true, Ordering::SeqCst);
        }

        fn recorded_set_agent_calls(&self) -> Vec<(String, String)> {
            self.set_agent_calls.lock().expect("set_agent lock").clone()
        }

        /// Block the next `list_commands` call until the returned sender
        /// fires — used to prove the lifecycle response is sent before the
        /// catalog fetch runs.
        fn gate_commands(&self) -> oneshot::Sender<()> {
            let (tx, rx) = oneshot::channel();
            *self.commands_gate.lock().expect("gate lock") = Some(rx);
            tx
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

        fn set_models(&self, models: Vec<dto::ModelInfo>) {
            *self.models.lock().expect("models lock") = Some(models);
        }

        fn set_session(&self, info: dto::SessionInfo) {
            *self.session_out.lock().expect("session lock") = Some(info);
        }

        fn recorded_get_session_calls(&self) -> Vec<String> {
            self.get_session_calls.lock().expect("get_session lock").clone()
        }

        fn recorded_set_model_calls(&self) -> Vec<(String, dto::ModelRef)> {
            self.set_model_calls.lock().expect("set_model lock").clone()
        }

        /// Make every `set_model` call fail (the set_config_option error
        /// path must still push the current state).
        fn fail_set_model(&self) {
            self.set_model_fail.store(true, Ordering::SeqCst);
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
            self.messages_called.store(true, Ordering::SeqCst);
            let out =
                self.messages_out.lock().expect("messages lock").take().unwrap_or_default();
            Box::pin(async move { Ok(out) })
        }

        fn event_stream(
            &self,
            _session_id: &str,
        ) -> BoxFuture<'_, Result<EventStream, anyhow::Error>> {
            // One stream per prompt. Events pushed between turns were parked
            // in `pending`; the fresh subscription only sees pushes that
            // happen after it (tokio broadcast semantics).
            let ledger = std::mem::take(&mut *self.pending.lock().expect("pending lock"));
            let tx = self.events_tx.lock().expect("tx lock").clone();
            Box::pin(async move {
                let rx = tx.subscribe();
                let live = futures_util::stream::unfold(rx, |mut rx| async move {
                    loop {
                        match rx.recv().await {
                            Ok(event) => return Some((event, rx)),
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                });
                let stream: EventStream =
                    Box::pin(futures_util::stream::iter(ledger).chain(live));
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

        fn list_models(&self) -> BoxFuture<'_, Option<Vec<dto::ModelInfo>>> {
            let models = self.models.lock().expect("models lock").clone();
            Box::pin(async move { models })
        }

        fn list_commands(&self) -> BoxFuture<'_, Option<Vec<serde_json::Value>>> {
            let commands = self.commands.lock().expect("commands lock").clone();
            let gate = self.commands_gate.lock().expect("gate lock").take();
            Box::pin(async move {
                if let Some(rx) = gate {
                    // Test gate: block until the client releases. A bounded
                    // wait keeps a broken implementation from hanging CI.
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        rx,
                    )
                    .await;
                }
                commands
            })
        }

        fn list_sessions(
            &self,
            directory: Option<&str>,
            cursor: Option<&str>,
        ) -> BoxFuture<'_, Result<(Vec<dto::SessionInfo>, Option<String>), anyhow::Error>> {
            self.list_calls.lock().expect("list lock").push((
                directory.map(str::to_string),
                cursor.map(str::to_string),
            ));
            let out = self.sessions_out.lock().expect("sessions lock").clone();
            // Echo the request cursor back as the next token: proves the
            // opaque token round-trips through the handler untouched.
            let next = cursor.map(str::to_string).or_else(|| Some("next-page-token".into()));
            Box::pin(async move { Ok((out.unwrap_or_default(), next)) })
        }

        fn delete_session(&self, session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            self.deleted.lock().expect("deleted lock").push(session_id.to_string());
            Box::pin(async { Ok(()) })
        }

        fn agents(&self, _directory: &str) -> BoxFuture<'_, Result<Vec<dto::AgentInfo>, anyhow::Error>> {
            let fail = self.agents_fail.load(Ordering::SeqCst);
            let out = self.agents_out.lock().expect("agents lock").clone();
            Box::pin(async move {
                if fail {
                    Err(anyhow::anyhow!("mock agents fetch failure"))
                } else {
                    Ok(out.unwrap_or_default())
                }
            })
        }

        fn set_agent(&self, session_id: &str, agent: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            self.set_agent_calls
                .lock()
                .expect("set_agent lock")
                .push((session_id.to_string(), agent.to_string()));
            Box::pin(async { Ok(()) })
        }

        fn get_session(
            &self,
            session_id: &str,
        ) -> BoxFuture<'_, Result<dto::SessionInfo, anyhow::Error>> {
            self.get_session_calls
                .lock()
                .expect("get_session lock")
                .push(session_id.to_string());
            let out = self.session_out.lock().expect("session lock").clone();
            Box::pin(async move {
                out.ok_or_else(|| anyhow::anyhow!("mock get_session not configured"))
            })
        }

        fn set_model(
            &self,
            session_id: &str,
            model: &dto::ModelRef,
        ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
            self.set_model_calls
                .lock()
                .expect("set_model lock")
                .push((session_id.to_string(), model.clone()));
            let fail = self.set_model_fail.load(Ordering::SeqCst);
            Box::pin(async move {
                if fail {
                    Err(anyhow::anyhow!("mock set_model failure"))
                } else {
                    Ok(())
                }
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
                    assert!(init.agent_capabilities.load_session);
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
                // Wave 6b: the mock has no agent catalog → the modes payload
                // is omitted (no picker) instead of a "Unknown" current mode.
                assert!(resp.modes.is_none(), "empty catalog omits the modes payload");
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
                // and enter the drain; the interrupted event then ends the
                // drain (the cancel already won).
                backend.push(dto::SessionEvent::TextDelta(dto::TextDelta {
                    base: dto::OrdinalRef {
                        sessionID: "ses_mock_1".into(),
                        assistantMessageID: "msg_mock_1".into(),
                        ordinal: Some(0),
                    },
                    delta: "never rendered".into(),
                }));
                backend.push(dto::SessionEvent::ExecutionInterrupted(
                    dto::SessionRef { sessionID: "ses_mock_1".into() },
                ));

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

    // ======================= Wave 4 =======================

    fn child_created(parent: &str) -> dto::SessionEvent {
        dto::SessionEvent::SessionCreated(dto::SessionCreated {
            sessionID: Some("ses_child_1".into()),
            slug: None,
            version: None,
            projectID: None,
            location: None,
            subpath: None,
            parentID: Some(parent.into()),
            title: Some("List the repo".into()),
        })
    }

    fn child_tool_started(id: &str) -> dto::SessionEvent {
        dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
            base: dto::ToolRef {
                sessionID: "ses_child_1".into(),
                assistantMessageID: "msg_c1".into(),
                id: id.into(),
            },
            name: "grep".into(),
        })
    }

    #[tokio::test]
    async fn child_session_tool_events_project_as_namespaced_calls() {
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
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
                    let sid = ns.session_id.clone();

                    // Child announced (wire: session.created with parentID),
                    // then its tool events ride the child's own sessionID,
                    // then the child turn ENDS — which must NOT stop the
                    // parent turn.
                    backend.push(child_created("ses_mock_1"));
                    backend.push(child_tool_started("call_c1"));
                    backend.push(dto::SessionEvent::ToolCalled(dto::ToolCalled {
                        base: dto::ToolRef {
                            sessionID: "ses_child_1".into(),
                            assistantMessageID: "msg_c1".into(),
                            id: "call_c1".into(),
                        },
                        input: serde_json::json!({ "query": "*.rs" }),
                        executed: Some(false),
                    }));
                    backend.push(dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
                        base: dto::ToolRef {
                            sessionID: "ses_child_1".into(),
                            assistantMessageID: "msg_c1".into(),
                            id: "call_c1".into(),
                        },
                        content: None,
                        metadata: None,
                        executed: None,
                    }));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_child_1".into(),
                    }));
                    // The parent turn ends normally only afterwards.
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_mock_1".into(),
                    }));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            sid.clone(),
                            vec![ContentBlock::Text(TextContent::new("use the subagent"))],
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

        // The child's tool events projected as nested calls: `${child.id}:`
        // toolCallId + `${child.title}:` title prefix.
        let notifications = collected.lock().expect("collected lock");
        let tool_updates: Vec<(String, Option<String>, Option<acp::ToolCallStatus>)> =
            notifications
                .iter()
                .filter_map(|n| match &n.update {
                    acp::SessionUpdate::ToolCallUpdate(u) => Some((
                        u.tool_call_id.0.to_string(),
                        u.fields.title.clone(),
                        u.fields.status,
                    )),
                    _ => None,
                })
                .collect();
        assert_eq!(tool_updates.len(), 3, "pending + called + completed");
        assert!(tool_updates.iter().all(|(id, _, _)| id == "ses_child_1:call_c1"));
        assert_eq!(tool_updates[0].1.as_deref(), Some("List the repo: grep"));
        assert_eq!(tool_updates[0].2, Some(acp::ToolCallStatus::Pending));
        assert_eq!(tool_updates[1].2, Some(acp::ToolCallStatus::InProgress));
        assert_eq!(tool_updates[2].2, Some(acp::ToolCallStatus::Completed));
        // All updates are addressed to the parent session (the client never
        // hears child session ids).
        assert!(notifications.iter().all(|n| &*n.session_id.0 == "ses_mock_1"));
    }

    #[tokio::test]
    async fn child_permission_ask_targets_parent_and_replies_to_child() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });
        let seen_requests = Arc::new(Mutex::new(Vec::new()));
        let reply_seen = backend.install_permission_seen();

        let outcome: Result<(), AcpError> = Client.builder()
            .name("acp-test-client")
            .on_receive_request(
                {
                    let seen_requests = Arc::clone(&seen_requests);
                    async move |req: acp::RequestPermissionRequest,
                                responder: Responder<acp::RequestPermissionResponse>,
                                _cx: ConnectionTo<Agent>| {
                        seen_requests.lock().expect("requests lock").push(req);
                        responder.respond(acp::RequestPermissionResponse::new(
                            acp::RequestPermissionOutcome::Selected(
                                acp::SelectedPermissionOutcome::new("once"),
                            ),
                        ))
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
                    let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
                    let sid = ns.session_id.clone();

                    // A permission ask from INSIDE the child session (its
                    // sessionID on the wire — the #48232 routing trap).
                    backend.push(child_created("ses_mock_1"));
                    backend.push(dto::SessionEvent::PermissionAsked(dto::PermissionAsked {
                        id: "per_c1".into(),
                        sessionID: "ses_child_1".into(),
                        action: "shell".into(),
                        resources: vec!["echo hi".into()],
                        save: None,
                        metadata: None,
                        source: Some(dto::PermissionSource {
                            kind: Some("tool".into()),
                            messageID: Some("msg_c1".into()),
                            id: "call_c1".into(),
                        }),
                    }));
                    backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                        sessionID: "ses_mock_1".into(),
                    }));

                    let prompt_req = cx
                        .send_request(PromptRequest::new(
                            sid.clone(),
                            vec![ContentBlock::Text(TextContent::new("run"))],
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

        // The client asked once — addressed to the PARENT session with the
        // namespaced toolCallId and the projected title.
        {
            let requests = seen_requests.lock().expect("requests lock");
            assert_eq!(requests.len(), 1);
            assert_eq!(&*requests[0].session_id.0, "ses_mock_1");
            assert_eq!(
                requests[0].tool_call.tool_call_id.0.as_ref(),
                "ses_child_1:call_c1"
            );
            assert_eq!(
                requests[0].tool_call.fields.title.as_deref(),
                Some("List the repo: shell: echo hi")
            );
            // …and the decision went to the ask's sessionID — the CHILD
            // (#48232: replies must be addressed to the child session).
            assert_eq!(
                backend.recorded_replies(),
                vec![(
                    "ses_child_1".to_string(),
                    "per_c1".to_string(),
                    dto::PermissionReply::Once
                )]
            );
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), reply_seen)
            .await
            .expect("child permission reply must reach the backend");
    }

    #[tokio::test]
    async fn cancel_drain_abandons_still_open_tools() {
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
                    let _ = cx
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
                    let sid = ns.session_id.clone();

                    // Two tools start (pushed while the turn is in flight —
                    // the mock stream only delivers to an active turn)…
                    let prompt = cx.send_request(PromptRequest::new(
                        sid.clone(),
                        vec![ContentBlock::Text(TextContent::new("run two"))],
                    ));
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    for id in ["call_a", "call_b"] {
                        backend.push(dto::SessionEvent::ToolInputStarted(dto::ToolInputStarted {
                            base: dto::ToolRef {
                                sessionID: "ses_mock_1".into(),
                                assistantMessageID: "msg_mock_1".into(),
                                id: id.into(),
                            },
                            name: "bash".into(),
                        }));
                    }

                    // …then the client cancels. In-flight events keep flowing
                    // during the wind-down: call_a completes, call_b stays
                    // open; the interrupted event ends the drain.
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    cx.send_notification(CancelNotification::new(sid.clone()))?;
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    backend.push(dto::SessionEvent::ToolSuccess(dto::ToolSuccess {
                        base: dto::ToolRef {
                            sessionID: "ses_mock_1".into(),
                            assistantMessageID: "msg_mock_1".into(),
                            id: "call_a".into(),
                        },
                        content: None,
                        metadata: None,
                        executed: None,
                    }));
                    backend.push(dto::SessionEvent::ExecutionInterrupted(
                        dto::SessionRef { sessionID: "ses_mock_1".into() },
                    ));

                    let resp = prompt.block_task().await?;
                    assert_eq!(resp.stop_reason, acp::StopReason::Cancelled);
                    Ok(())
                }
            })
            .await;

        agent_task.abort();
        outcome.expect("client run ok");

        // Exactly the still-open call is abandoned, Failed + "Cancelled";
        // the call that completed during the drain is untouched.
        let notifications = collected.lock().expect("collected lock");
        let failed: Vec<(String, Option<String>)> = notifications
            .iter()
            .filter_map(|n| match &n.update {
                acp::SessionUpdate::ToolCallUpdate(u)
                    if u.fields.status == Some(acp::ToolCallStatus::Failed) =>
                {
                    Some((u.tool_call_id.0.to_string(), u.fields.title.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(failed.len(), 1, "only the still-open call is abandoned");
        assert_eq!(failed[0].0, "call_b");
        assert_eq!(failed[0].1.as_deref(), Some("Cancelled"));
        let completed = notifications.iter().any(|n| match &n.update {
            acp::SessionUpdate::ToolCallUpdate(u) => {
                u.tool_call_id.0.as_ref() == "call_a"
                    && u.fields.status == Some(acp::ToolCallStatus::Completed)
            }
            _ => false,
        });
        assert!(completed, "call_a completed during the drain");
    }

    #[tokio::test]
    async fn catalog_reload_pushes_full_config_option_state() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |backend, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();

            // Catalog reload signal (wire shape: `{}`) arrives mid-turn; the
            // catalogs are re-fetched and the FULL state (agent + model
            // options with real current values) is pushed.
            backend.push(dto::SessionEvent::ModelUpdated(dto::ModelOrProviderUpdated {}));
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: "ses_mock_1".into(),
            }));

            let prompt_req = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("hi"))],
                ))
                .block_task()
                .await?;
            assert_eq!(prompt_req.stop_reason, acp::StopReason::EndTurn);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");

        // Exactly ONE ConfigOptionUpdate: the catalog-reload push (the
        // lifecycle response carries the state, it is not a push).
        let notifications = collected.lock().expect("collected lock");
        let configs = config_option_pushes(&notifications);
        assert_eq!(configs.len(), 1, "one catalog reload push");
        let options = &configs[0].config_options;
        // Both options, real current values (no blank "" anywhere).
        let agent = option_by_id(options, "agent");
        assert_eq!(agent.category, Some(acp::SessionConfigOptionCategory::Mode));
        assert_eq!(as_select(agent).current_value.0.as_ref(), "orchestrator");
        let model = option_by_id(options, "model");
        assert_eq!(model.category, Some(acp::SessionConfigOptionCategory::Model));
        // The tracked model is still UNKNOWN (no step yet) → the synthetic
        // Default option is listed and the current value is "__default__".
        assert_eq!(as_select(model).current_value.0.as_ref(), "__default__");
        let groups = grouped(as_select(model));
        assert_eq!(groups[0].options[0].value.0.as_ref(), "__default__");
        assert_eq!(groups[0].options[0].name.as_str(), "Default");
    }

    // ======================= Wave 6a: session management =======================

    /// One-session wire shape for list tests (matches the session-create
    /// fixture: `time` is `{created, updated}` epoch **milliseconds**).
    fn wire_session(id: &str, title: Option<&str>, location: Option<&str>, updated_ms: Option<u64>) -> dto::SessionInfo {
        dto::SessionInfo {
            id: id.into(),
            projectID: None,
            title: title.map(str::to_string),
            version: None,
            subpath: None,
            location: location.map(|d| dto::Location { directory: d.into() }),
            agent: None,
            model: None,
            summary: None,
            cost: None,
            tokens: None,
            time: updated_ms.map(|ms| {
                serde_json::json!({
                    "created": ms,
                    "updated": ms,
                })
            }),
        }
    }

    /// Shared duplex harness: agent on one end, a notification-collecting
    /// client on the other; `f` runs the client script. Aborts the agent
    /// task when the script completes and returns (outcome, notifications).
    async fn run_client<F, Fut>(
        svc: Arc<AgentService>,
        backend: Arc<MockBackend>,
        f: F,
    ) -> (
        Result<(), AcpError>,
        Arc<Mutex<Vec<SessionNotification>>>,
    )
    where
        F: FnOnce(
                Arc<MockBackend>,
                Arc<Mutex<Vec<SessionNotification>>>,
                ConnectionTo<Agent>,
            ) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = Result<(), AcpError>> + Send,
    {
        let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
        let agent_task = tokio::spawn({
            let svc = Arc::clone(&svc);
            async move { let _ = svc.serve(agent_side).await; }
        });
        let collected = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));
        let outcome: Result<(), AcpError> = Client
            .builder()
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
            .connect_with(
                client_side,
                {
                    let backend = Arc::clone(&backend);
                    let collected = Arc::clone(&collected);
                    async move |cx| f(backend, collected, cx).await
                },
            )
            .await;
        agent_task.abort();
        (outcome, collected)
    }

    /// Wait until `pred` holds over the collected notifications (bounded).
    async fn wait_for<F: Fn(&[SessionNotification]) -> bool>(collected: &Arc<Mutex<Vec<SessionNotification>>>, pred: F) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if pred(&collected.lock().expect("collected lock")) {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("notification did not arrive within 5s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    fn commands_pushes(collected: &[SessionNotification]) -> Vec<&acp::AvailableCommandsUpdate> {
        collected
            .iter()
            .filter_map(|n| match &n.update {
                acp::SessionUpdate::AvailableCommandsUpdate(u) => Some(u),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn initialize_advertises_list_delete_resume_capabilities() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_backend, _collected, cx| async move {
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let caps = init.agent_capabilities;
            assert!(caps.load_session);
            // Wave 6a: list/delete/resume are marker structs — `{}` on the wire.
            assert!(caps.session_capabilities.list.is_some(), "session/list advertised");
            assert!(caps.session_capabilities.delete.is_some(), "session/delete advertised");
            assert!(caps.session_capabilities.resume.is_some(), "session/resume advertised");
            // Not advertised: close, fork (unstable), additionalDirectories.
            assert!(caps.session_capabilities.close.is_none());
            assert!(caps.session_capabilities.additional_directories.is_none());
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn list_sessions_passes_cursor_and_maps_session_info() {
        let backend = MockBackend::new();
        backend.set_sessions(vec![
            wire_session("ses_list_1", Some("checkout review"), Some("/tmp/elsewhere"), Some(1_791_023_018_226)),
            wire_session("ses_list_2", None, None, None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_backend, _collected, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let resp = cx
                .send_request(
                acp::ListSessionsRequest::new()
                    .cwd(std::path::PathBuf::from("/tmp/opencode/acp-fixture-project"))
                    .cursor("tok-7"),
            )
                .block_task()
                .await?;
            // The opaque token round-trips: request cursor → backend → response.
            assert_eq!(resp.next_cursor.as_deref(), Some("tok-7"));
            assert_eq!(resp.sessions.len(), 2);
            let s1 = &resp.sessions[0];
            assert_eq!(s1.session_id.0.as_ref(), "ses_list_1");
            assert_eq!(s1.cwd.to_string_lossy().as_ref(), "/tmp/elsewhere");
            assert_eq!(s1.title.as_deref(), Some("checkout review"));
            // time.updated (epoch ms, live wire shape) → ISO 8601 UTC.
            assert_eq!(s1.updated_at.as_deref(), Some("2026-10-03T10:23:38Z"));
            // No wire location → the list filter's cwd; no time → updated_at None.
            let s2 = &resp.sessions[1];
            assert_eq!(s2.cwd.to_string_lossy().as_ref(), "/tmp/opencode/acp-fixture-project");
            assert!(s2.title.is_none());
            assert!(s2.updated_at.is_none());
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        // The backend saw the directory filter AND the cursor.
        assert_eq!(
            backend.recorded_list_calls(),
            vec![(Some("/tmp/opencode/acp-fixture-project".into()), Some("tok-7".into()))]
        );
    }

    #[tokio::test]
    async fn resume_registers_without_replaying_and_prompt_works() {
        let backend = MockBackend::new();
        // A replayer would fetch history — make the mock notice.
        backend.set_messages(vec![]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |backend, _collected, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            // Resume a session this bridge never loaded.
            let resp = cx
                .send_request(acp::ResumeSessionRequest::new(
                    "ses_never_loaded",
                    "/tmp/opencode/acp-fixture-project",
                ))
                .block_task()
                .await?;
            // Wave 6b: no assistant message + empty catalog → the modes
            // payload is omitted entirely (Zed renders no picker).
            assert!(resp.modes.is_none(), "empty catalog omits the modes payload");
            assert!(resp.config_options.is_none());
            // No command catalog → no push; the resumed session is registered,
            // so a prompt must route (and end) normally.
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: "ses_never_loaded".into(),
            }));
            let prompt = cx
                .send_request(PromptRequest::new(
                    "ses_never_loaded",
                    vec![ContentBlock::Text(TextContent::new("hi"))],
                ))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        // Wave 6b: resume fetches messages for the currentModeId metadata
        // (nothing else — the replay path stays closed).
        assert!(
            backend.messages_called.load(Ordering::SeqCst),
            "resume fetches messages for the mode metadata"
        );
        let notifications = collected.lock().expect("collected lock");
        assert!(
            notifications.is_empty(),
            "zero updates around resume (no replay, no push): got {}",
            notifications.len()
        );
    }

    #[tokio::test]
    async fn delete_calls_backend_and_unregisters() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_backend, _collected, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx
                .send_request(NewSessionRequest::new("/tmp"))
                .block_task()
                .await?;
            let sid = ns.session_id.clone();
            // Empty response `{}` on success.
            let _ = cx
                .send_request(acp::DeleteSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            // The registry entry is gone: a prompt must fail as unknown.
            let err = cx
                .send_request(PromptRequest::new(
                    sid,
                    vec![ContentBlock::Text(TextContent::new("hi"))],
                ))
                .block_task()
                .await
                .expect_err("deleted session must be unknown");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert_eq!(backend.recorded_deleted(), vec!["ses_mock_1".to_string()]);
    }

    #[tokio::test]
    async fn commands_push_after_new_session_not_gated_on_fetch() {
        let backend = MockBackend::new();
        backend.set_commands(vec![
            serde_json::json!({ "name": "review", "description": "review changes" }),
            // Wire shape: a command may carry no description (
            // `changelog` on the live probe) — ACP requires one.
            serde_json::json!({ "name": "changelog" }),
        ]);
        // Gate the catalog fetch: the newSession response must arrive while
        // the push is still blocked — proving the response is not gated on it.
        let gate = backend.gate_commands();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_backend, collected, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns_fut = cx.send_request(NewSessionRequest::new("/tmp"));
            let ns = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                ns_fut.block_task(),
            )
            .await
            .expect("newSession response within 10s (must not wait for the command fetch)")
            .expect("newSession ok");
            let sid = ns.session_id.clone();
            // Release the fetch; the push then completes.
            let _ = gate.send(());
            wait_for(&collected, |n| !commands_pushes(n).is_empty()).await;
            assert_eq!(sid.0.as_ref(), "ses_mock_1");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let notifications = collected.lock().expect("collected lock");
        assert!(notifications.iter().all(|n| &*n.session_id.0 == "ses_mock_1"));
        let pushes = commands_pushes(&notifications);
        assert_eq!(pushes.len(), 1, "one push per newSession");
        // Full replacement shape: every command, in wire order, name+
        // description; `input` omitted (opencode has no input schema).
        assert_eq!(pushes[0].available_commands.len(), 2);
        let c0 = &pushes[0].available_commands[0];
        assert_eq!(c0.name.as_str(), "review");
        assert_eq!(c0.description.as_str(), "review changes");
        assert!(c0.input.is_none());
        let c1 = &pushes[0].available_commands[1];
        assert_eq!(c1.name.as_str(), "changelog");
        assert_eq!(c1.description.as_str(), "", "missing wire description defaults to empty");
    }

    #[tokio::test]
    async fn load_and_resume_trigger_commands_push() {
        let backend = MockBackend::new();
        backend.set_commands(vec![serde_json::json!({ "name": "commit", "description": "git commit" })]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_backend, collected, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let _ = cx
                .send_request(LoadSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new(
                    "ses_mock_2",
                    "/tmp/opencode/acp-fixture-project",
                ))
                .block_task()
                .await?;
            // Wave 6b: empty catalog → the modes payload is omitted (no
            // picker); the load/resume must still succeed and push.
            assert!(resume.modes.is_none(), "empty catalog omits the modes payload");
            // Both pushes are spawned after their responses — wait for both.
            wait_for(&collected, |n| commands_pushes(n).len() >= 2).await;
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let notifications = collected.lock().expect("collected lock");
        let pushes = commands_pushes(&notifications);
        assert_eq!(pushes.len(), 2, "load and resume each push once");
        // Each push is targeted at its own lifecycle session.
        let sessions: std::collections::BTreeSet<&str> = notifications
            .iter()
            .filter_map(|n| {
                matches!(&n.update, acp::SessionUpdate::AvailableCommandsUpdate(_))
                    .then_some(&*n.session_id.0)
            })
            .collect();
        assert_eq!(
            sessions.into_iter().collect::<Vec<_>>(),
            vec!["ses_mock_1", "ses_mock_2"]
        );
        assert!(pushes.iter().all(|p| p.available_commands.len() == 1));
        assert_eq!(pushes[0].available_commands[0].name.as_str(), "commit");
    }

    #[test]
    fn epoch_ms_to_iso_formats_utc_timestamp() {
        assert_eq!(epoch_ms_to_iso(0), Some("1970-01-01T00:00:00Z".into()));
        assert_eq!(epoch_ms_to_iso(86_400_000), Some("1970-01-02T00:00:00Z".into()));
        // The live fixture value (session-create.json time.updated).
        assert_eq!(epoch_ms_to_iso(1_791_023_018_226), Some("2026-10-03T10:23:38Z".into()));
        // Sub-second precision is truncated (the schema wants seconds).
        assert_eq!(epoch_ms_to_iso(1_791_023_018_226 + 999), Some("2026-10-03T10:23:39Z".into()));
        // Leap day, pre-epoch, and epoch boundaries.
        assert_eq!(epoch_ms_to_iso(951_782_400_000), Some("2000-02-29T00:00:00Z".into()));
        assert_eq!(epoch_ms_to_iso(-1), Some("1969-12-31T23:59:59Z".into()));
    }

    // ======================= Wave 6b: modes =======================

    fn wire_agent(id: &str, mode: &str, hidden: bool, description: Option<&str>) -> dto::AgentInfo {
        dto::AgentInfo {
            id: id.into(),
            name: id.into(),
            mode: Some(mode.into()),
            hidden,
            description: description.map(str::to_string),
        }
    }

    fn agent_selected(sid: &str, agent: &str) -> dto::SessionEvent {
        dto::SessionEvent::AgentSelected(dto::SessionAgentSelected {
            sessionID: sid.into(),
            agent: agent.into(),
        })
    }

    fn wire_msg(kind: &str, agent: Option<&str>) -> dto::MessageRecord {
        dto::MessageRecord {
            kind: kind.into(),
            id: format!("{kind}-rec"),
            text: None,
            agent: agent.map(str::to_string),
            model: None,
            content: None,
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        }
    }

    fn mode_updates(n: &[SessionNotification]) -> Vec<&acp::CurrentModeUpdate> {
        n.iter()
            .filter_map(|n| match &n.update {
                acp::SessionUpdate::CurrentModeUpdate(u) => Some(u),
                _ => None,
            })
            .collect()
    }

    // ============ Release 0.3.0: config options helpers ============

    /// `initialize` declaring `clientCapabilities.session.configOptions` —
    /// exactly what Zed sends (the config-options capability gate).
    fn init_with_config_options() -> InitializeRequest {
        InitializeRequest::new(ProtocolVersion::V1).client_capabilities(
            ClientCapabilities::new().session(
                ClientSessionCapabilities::new().config_options(
                    SessionConfigOptionsCapabilities::new(),
                ),
            ),
        )
    }

    /// The canned model catalog used across the config-options tests: two
    /// providers, one model without a display name (falls back to id).
    fn catalog_models() -> Vec<dto::ModelInfo> {
        vec![
            dto::ModelInfo {
                id: "GLM-5.3-astra".into(),
                modelID: Some("GLM-5.3-astra".into()),
                providerID: "astra".into(),
                name: Some("GLM 5.3".into()),
            },
            dto::ModelInfo {
                id: "deepseek_v4_flash_code".into(),
                modelID: None,
                providerID: "astra".into(),
                name: None,
            },
            dto::ModelInfo {
                id: "gpt-6".into(),
                modelID: None,
                providerID: "openai".into(),
                name: Some("GPT-6".into()),
            },
        ]
    }

    fn config_option_pushes(n: &[SessionNotification]) -> Vec<&acp::ConfigOptionUpdate> {
        n.iter()
            .filter_map(|n| match &n.update {
                acp::SessionUpdate::ConfigOptionUpdate(u) => Some(u),
                _ => None,
            })
            .collect()
    }

    fn option_by_id<'a>(options: &'a [acp::SessionConfigOption], id: &str) -> &'a acp::SessionConfigOption {
        options
            .iter()
            .find(|o| o.id.0.as_ref() == id)
            .unwrap_or_else(|| panic!("config option {id:?} missing"))
    }

    fn as_select(option: &acp::SessionConfigOption) -> &acp::SessionConfigSelect {
        match &option.kind {
            acp::SessionConfigKind::Select(s) => s,
            _ => panic!("expected a select option, got {:?}", option.kind),
        }
    }

    fn grouped(select: &acp::SessionConfigSelect) -> &[acp::SessionConfigSelectGroup] {
        match &select.options {
            acp::SessionConfigSelectOptions::Grouped(g) => g,
            _ => panic!("expected grouped options"),
        }
    }

    fn model_selected(sid: &str, provider: &str, id: &str) -> dto::SessionEvent {
        dto::SessionEvent::ModelSelected(dto::SessionModelSelected {
            sessionID: sid.into(),
            model: dto::ModelRef {
                id: id.into(),
                providerID: provider.into(),
                variant: None,
            },
        })
    }

    fn wire_msg_model(kind: &str, agent: Option<&str>, model: Option<dto::ModelRef>) -> dto::MessageRecord {
        dto::MessageRecord {
            kind: kind.into(),
            id: format!("{kind}-rec"),
            text: None,
            agent: agent.map(str::to_string),
            model,
            content: None,
            finish: None,
            rawFinish: None,
            cost: None,
            tokens: None,
            time: None,
        }
    }

    #[tokio::test]
    async fn new_session_responds_with_filtered_modes_and_default_mode() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, Some("plans and delegates")),
            wire_agent("build", "primary", false, None),
            // subagents must not surface as modes…
            wire_agent("explorer", "subagent", false, None),
            wire_agent("fixer", "subagent", false, None),
            // …nor hidden internals…
            wire_agent("compaction", "primary", true, None),
            wire_agent("title", "primary", true, None),
            // …but `all` counts as a mode.
            wire_agent("dreamer-x", "all", false, None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let modes = ns.modes.expect("newSession carries the mode state");
            // 2.0.21 default agent.
            assert_eq!(modes.current_mode_id.0.as_ref(), "orchestrator");
            // Filtered: primary/all + !hidden, wire order preserved.
            let ids: Vec<&str> = modes
                .available_modes
                .iter()
                .map(|m| m.id.0.as_ref())
                .collect();
            assert_eq!(ids, vec!["orchestrator", "build", "dreamer-x"]);
            assert_eq!(
                modes.available_modes[0].description.as_deref(),
                Some("plans and delegates"),
                "description passthrough"
            );
            assert_eq!(modes.available_modes[1].name.as_str(), "build");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn set_mode_calls_wire_emits_single_update_and_rejects_unknown_session() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            // Unknown session → invalid_params, no wire call.
            let err = cx
                .send_request(acp::SetSessionModeRequest::new("ses_unknown", "build"))
                .block_task()
                .await
                .expect_err("unknown session must be rejected");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            // Known session → empty response.
            let _ = cx
                .send_request(acp::SetSessionModeRequest::new(sid, "build"))
                .block_task()
                .await?;
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert_eq!(
            backend.recorded_set_agent_calls(),
            vec![("ses_mock_1".to_string(), "build".to_string())],
            "exactly one wire switch, for the known session"
        );
        let notifications = collected.lock().expect("collected lock");
        let updates = mode_updates(&notifications);
        assert_eq!(updates.len(), 1, "exactly one current_mode_update on set_mode");
        assert_eq!(updates[0].current_mode_id.0.as_ref(), "build");
    }

    #[tokio::test]
    async fn agent_selected_remote_switch_emits_update_then_echo_stays_suppressed() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |backend, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            // Turn 1: a REMOTE switch to build (agent differs from the
            // tracked default) → one current_mode_update.
            backend.push(dto::SessionEvent::ExecutionStarted(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            backend.push(agent_selected(&sid.0, "build"));
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            let prompt = cx
                .send_request(PromptRequest::new(sid.clone(), vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            // Turn 2: the own-switch echo (same agent) → suppressed.
            backend.push(dto::SessionEvent::ExecutionStarted(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            backend.push(agent_selected(&sid.0, "build"));
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            let prompt = cx
                .send_request(PromptRequest::new(sid, vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let notifications = collected.lock().expect("collected lock");
        let updates = mode_updates(&notifications);
        assert_eq!(
            updates.len(),
            1,
            "remote switch emits once; the same-agent echo stays suppressed"
        );
        assert_eq!(updates[0].current_mode_id.0.as_ref(), "build");
    }

    #[tokio::test]
    async fn step_started_self_heals_desynced_tracked_mode() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |backend, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            let started = |agent: Option<&str>| {
                dto::SessionEvent::StepStarted(dto::StepStarted {
                    session: dto::SessionRef { sessionID: sid.0.to_string() },
                    agent: agent.map(str::to_string),
                    model: None,
                    assistantMessageID: "msg_x".into(),
                    snapshot: None,
                    started: None,
                })
            };
            // Turn 1: the server runs `build` although the tracked mode is
            // the default (config default agent) → self-heal update.
            backend.push(dto::SessionEvent::ExecutionStarted(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            backend.push(started(Some("build")));
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            let prompt = cx
                .send_request(PromptRequest::new(sid.clone(), vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            // Turn 2: still build → the tracked value matches, no update.
            backend.push(dto::SessionEvent::ExecutionStarted(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            backend.push(started(Some("build")));
            backend.push(dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                sessionID: sid.0.to_string(),
            }));
            let prompt = cx
                .send_request(PromptRequest::new(sid, vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let notifications = collected.lock().expect("collected lock");
        let updates = mode_updates(&notifications);
        assert_eq!(updates.len(), 1, "self-heal fires once on the desync");
        assert_eq!(updates[0].current_mode_id.0.as_ref(), "build");
    }

    #[tokio::test]
    async fn load_and_resume_use_last_assistant_agent_as_current_mode() {
        let backend = MockBackend::new();
        // A stock catalog so the modes payload is present (the current mode
        // assertions below are about the last-assistant-agent rule, not
        // about the empty-catalog omitted-payload path).
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_messages(vec![
            wire_msg("user", None),
            wire_msg("assistant", Some("build")),
            wire_msg("idle", None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |backend, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            // Load: the LAST assistant message's agent.
            let load = cx
                .send_request(LoadSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let modes = load.modes.expect("load carries the mode state");
            assert_eq!(modes.current_mode_id.0.as_ref(), "build");
            // Resume: same rule (fresh message fetch — the load consumed the
            // mock store, so set it again; newest assistant wins).
            backend.set_messages(vec![
                wire_msg("user", None),
                wire_msg("assistant", Some("build")),
                wire_msg("assistant", Some("orchestrator")),
            ]);
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let modes = resume.modes.expect("resume carries the mode state");
            assert_eq!(
                modes.current_mode_id.0.as_ref(),
                "orchestrator",
                "newest assistant message wins"
            );
            // No assistant messages at all → default.
            backend.set_messages(vec![wire_msg("user", None)]);
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let modes = resume.modes.expect("resume carries the mode state");
            assert_eq!(modes.current_mode_id.0.as_ref(), "orchestrator", "no assistant -> default");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[test]
    fn default_mode_id_follows_catalog_semantics() {
        // First visible primary in wire order wins (no hardcoded name).
        let agents = vec![
            wire_agent("build", "primary", false, None),
            wire_agent("orchestrator", "primary", false, None),
        ];
        assert_eq!(default_mode_id(&agents).as_deref(), Some("build"));
        // Hidden primaries never win.
        let agents = vec![
            wire_agent("compaction", "primary", true, None),
            wire_agent("build", "primary", false, None),
        ];
        assert_eq!(default_mode_id(&agents).as_deref(), Some("build"));
        // No primary at all → the first mode-eligible agent (`all`).
        let agents = vec![
            wire_agent("dreamer-x", "all", false, None),
            wire_agent("build", "all", false, None),
        ];
        assert_eq!(default_mode_id(&agents).as_deref(), Some("dreamer-x"));
        // Nothing eligible → None (callers omit the modes payload).
        let agents = vec![
            wire_agent("explorer", "subagent", false, None),
            wire_agent("compaction", "primary", true, None),
        ];
        assert_eq!(default_mode_id(&agents), None);
    }

    #[tokio::test]
    async fn new_session_derives_default_from_catalog_without_orchestrator() {
        // The user's server: no "orchestrator" agent, "build" is the first
        // visible primary → newSession must not hardcode "orchestrator".
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("explorer", "subagent", false, None),
            wire_agent("build", "primary", false, None),
            wire_agent("dreamer-x", "all", false, None),
            wire_agent("compaction", "primary", true, None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let modes = ns.modes.expect("newSession carries the mode state");
            assert_eq!(modes.current_mode_id.0.as_ref(), "build");
            let ids: Vec<&str> = modes
                .available_modes
                .iter()
                .map(|m| m.id.0.as_ref())
                .collect();
            assert_eq!(ids, vec!["build", "dreamer-x"]);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn new_session_without_pickable_modes_omits_the_modes_payload() {
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            // Empty catalog (no set_agents) → no modes payload at all.
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            assert!(ns.modes.is_none(), "empty catalog omits the modes payload");
            // A failing agents fetch degrades the same way (warn + omit).
            b.fail_agents();
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            assert!(ns.modes.is_none(), "failed agents fetch omits the modes payload");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn load_and_resume_use_derived_default_without_orchestrator() {
        let backend = MockBackend::new();
        // No "orchestrator" in the catalog: the derived default is "build".
        backend.set_agents(vec![
            wire_agent("build", "primary", false, None),
            wire_agent("explorer", "subagent", false, None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            // No assistant messages → the derived default, not "orchestrator".
            let load = cx
                .send_request(LoadSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let modes = load.modes.expect("load carries the mode state");
            assert_eq!(modes.current_mode_id.0.as_ref(), "build");
            // Resume follows the same rule.
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let modes = resume.modes.expect("resume carries the mode state");
            assert_eq!(modes.current_mode_id.0.as_ref(), "build");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    // ================ Release 0.3.0: config options ================

    #[tokio::test]
    async fn new_session_config_options_both_pickers_capability_gated() {
        // Capability NOT declared → no config_options in the lifecycle
        // response (the modes payload keeps working for other clients).
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            assert!(ns.modes.is_some(), "modes payload stays for non-declaring clients");
            assert!(ns.config_options.is_none(), "no capability → no config options");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");

        // Capability declared (Zed) → both pickers, real current values.
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let options = ns.config_options.expect("config options present");
            assert_eq!(options.len(), 2, "agent + model options");

            // Agent option: category Mode, ungrouped visible agents, current
            // = the derived default (first visible primary).
            let agent = option_by_id(&options, "agent");
            assert_eq!(agent.category, Some(acp::SessionConfigOptionCategory::Mode));
            assert_eq!(agent.name, "Agent");
            let select = as_select(agent);
            assert_eq!(select.current_value.0.as_ref(), "orchestrator");
            let acp::SessionConfigSelectOptions::Ungrouped(values) = &select.options else {
                panic!("agent options must be ungrouped");
            };
            let ids: Vec<&str> = values.iter().map(|v| v.value.0.as_ref()).collect();
            assert_eq!(ids, vec!["orchestrator", "build"]);

            // Model option: category Model, grouped by provider (first-seen
            // order), value scheme <provider>/<model>, display name falls
            // back to modelID then id. newSession model is UNKNOWN →
            // current "__default__" with Default PREPENDED to the first
            // group.
            let model = option_by_id(&options, "model");
            assert_eq!(model.category, Some(acp::SessionConfigOptionCategory::Model));
            assert_eq!(model.name, "Model");
            let select = as_select(model);
            assert_eq!(select.current_value.0.as_ref(), "__default__");
            let groups = grouped(select);
            assert_eq!(groups.len(), 2);
            assert_eq!(groups[0].group.0.as_ref(), "astra");
            assert_eq!(groups[0].name, "astra");
            assert_eq!(groups[1].group.0.as_ref(), "openai");
            assert_eq!(groups[1].name, "openai");
            let astra: Vec<(String, String)> = groups[0]
                .options
                .iter()
                .map(|o| (o.value.0.as_ref().to_string(), o.name.clone()))
                .collect();
            assert_eq!(
                astra,
                vec![
                    ("__default__".to_string(), "Default".to_string()),
                    ("astra/GLM-5.3-astra".to_string(), "GLM 5.3".to_string()),
                    (
                        "astra/deepseek_v4_flash_code".to_string(),
                        "deepseek_v4_flash_code".to_string()
                    ),
                ]
            );
            assert_eq!(groups[1].options.len(), 1);
            assert_eq!(groups[1].options[0].value.0.as_ref(), "openai/gpt-6");
            assert_eq!(groups[1].options[0].name, "GPT-6");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn config_options_degrade_per_option_and_together() {
        // Model catalog unavailable (fetch failed/absent) → agent option
        // only.
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let options = ns.config_options.expect("agent option present");
            assert_eq!(options.len(), 1);
            assert_eq!(options[0].id.0.as_ref(), "agent");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");

        // Agents fetch failed → model option only (Default listed: unknown
        // current model).
        let backend = MockBackend::new();
        backend.set_models(catalog_models());
        backend.fail_agents();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let options = ns.config_options.expect("model option present");
            assert_eq!(options.len(), 1);
            let model = option_by_id(&options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "__default__");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");

        // Both unavailable → no config_options field at all.
        let backend = MockBackend::new();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            assert!(ns.config_options.is_none(), "nothing to render → omit the field");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }

    #[tokio::test]
    async fn set_config_option_model_calls_wire_and_tracks() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            // Unknown session → invalid_params, no wire call.
            let err = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    "ses_unknown",
                    "model",
                    "astra/GLM-5.3-astra",
                ))
                .block_task()
                .await
                .expect_err("unknown session must be rejected");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            // "astra/GLM-5.3-astra" → set_model wire; the response carries the
            // full state with the concrete current model and NO Default
            // option anymore (once concrete, Default is not listed).
            let resp = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "model",
                    "astra/GLM-5.3-astra",
                ))
                .block_task()
                .await?;
            let model = option_by_id(&resp.config_options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "astra/GLM-5.3-astra");
            let groups = grouped(as_select(model));
            let first = &groups[0].options[0];
            assert_ne!(
                first.value.0.as_ref(),
                "__default__",
                "concrete model → Default option is gone"
            );
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let calls = backend.recorded_set_model_calls();
        assert_eq!(calls.len(), 1, "exactly one wire model switch");
        assert_eq!(calls[0].0, "ses_mock_1");
        assert_eq!(calls[0].1.id, "GLM-5.3-astra");
        assert_eq!(calls[0].1.providerID, "astra");
        assert!(calls[0].1.variant.is_none());
        // The handler pushes the full state (both options).
        let notifications = collected.lock().expect("collected lock");
        let pushes = config_option_pushes(&notifications);
        assert_eq!(pushes.len(), 1, "one config_option_update after the switch");
    }

    #[tokio::test]
    async fn set_config_option_default_noop_and_invalid_values_push_state() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            // "__default__" → success no-op (no set-model wire), current
            // state echoed (Default still listed while unknown).
            let resp = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "model",
                    "__default__",
                ))
                .block_task()
                .await?;
            let model = option_by_id(&resp.config_options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "__default__");
            assert_eq!(
                grouped(as_select(model))[0].options[0].value.0.as_ref(),
                "__default__"
            );
            // Unmatched value (stale Zed-persisted default) → invalid-params
            // error, but the current state is still pushed (self-corrects).
            let err = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "model",
                    "codebuddy/gpt-6-sol",
                ))
                .block_task()
                .await
                .expect_err("value not in the catalog must be rejected");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            // Malformed value (no '/') → same reject path.
            let err = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "model",
                    "not-a-model-ref",
                ))
                .block_task()
                .await
                .expect_err("malformed value must be rejected");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            // Unknown config id → invalid_params.
            let err = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid,
                    "bogus_config_id",
                    "x",
                ))
                .block_task()
                .await
                .expect_err("unknown config id must be rejected");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert!(
            backend.recorded_set_model_calls().is_empty(),
            "__default__ is a no-op; invalid values never reach the wire"
        );
        let notifications = collected.lock().expect("collected lock");
        let pushes = config_option_pushes(&notifications);
        assert_eq!(
            pushes.len(),
            3,
            "no-op + two rejects each push the current state (self-correction)"
        );
    }

    #[tokio::test]
    async fn set_config_option_agent_reuses_set_mode_semantics() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            let resp = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(sid, "agent", "build"))
                .block_task()
                .await?;
            let agent = option_by_id(&resp.config_options, "agent");
            assert_eq!(as_select(agent).current_value.0.as_ref(), "build");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert_eq!(
            backend.recorded_set_agent_calls(),
            vec![("ses_mock_1".to_string(), "build".to_string())]
        );
        let notifications = collected.lock().expect("collected lock");
        let updates = mode_updates(&notifications);
        assert_eq!(updates.len(), 1, "same current_mode_update semantics as set_mode");
        assert_eq!(updates[0].current_mode_id.0.as_ref(), "build");
        assert_eq!(config_option_pushes(&notifications).len(), 1);
    }

    #[tokio::test]
    async fn set_model_failure_rejects_and_pushes_current_state() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        backend.fail_set_model();
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            let err = cx
                .send_request(acp::SetSessionConfigOptionRequest::new(
                    sid,
                    "model",
                    "astra/GLM-5.3-astra",
                ))
                .block_task()
                .await
                .expect_err("wire failure must surface as an error");
            assert_eq!(err.code, agent_client_protocol::ErrorCode::InvalidParams);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert_eq!(backend.recorded_set_model_calls().len(), 1, "wire was attempted");
        let notifications = collected.lock().expect("collected lock");
        let pushes = config_option_pushes(&notifications);
        assert_eq!(pushes.len(), 1, "current state pushed after the failed switch");
        // The tracked model stayed UNKNOWN → Default still listed.
        let model = option_by_id(&pushes[0].config_options, "model");
        assert_eq!(as_select(model).current_value.0.as_ref(), "__default__");
    }

    #[tokio::test]
    async fn model_selected_and_step_started_track_and_push_with_echo_suppression() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, collected) = run_client(svc, Arc::clone(&backend), move |backend, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let ns = cx.send_request(NewSessionRequest::new("/tmp")).block_task().await?;
            let sid = ns.session_id.clone();
            let exec_started = || {
                dto::SessionEvent::ExecutionStarted(dto::SessionRef {
                    sessionID: sid.0.to_string(),
                })
            };
            let exec_succeeded = || {
                dto::SessionEvent::ExecutionSucceeded(dto::SessionRef {
                    sessionID: sid.0.to_string(),
                })
            };
            // Turn 1: step.started carries a model different from the
            // tracked UNKNOWN → self-heal push (agent matches the tracked
            // default, so no mode noise).
            backend.push(exec_started());
            backend.push(dto::SessionEvent::StepStarted(dto::StepStarted {
                session: dto::SessionRef { sessionID: sid.0.to_string() },
                agent: Some("orchestrator".into()),
                model: Some(dto::ModelRef {
                    id: "GLM-5.3-astra".into(),
                    providerID: "astra".into(),
                    variant: None,
                }),
                assistantMessageID: "msg_1".into(),
                snapshot: None,
                started: None,
            }));
            backend.push(exec_succeeded());
            let prompt = cx
                .send_request(PromptRequest::new(sid.clone(), vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            // Turn 2: same model again → no change, no push (echo suppressed).
            backend.push(exec_started());
            backend.push(dto::SessionEvent::StepStarted(dto::StepStarted {
                session: dto::SessionRef { sessionID: sid.0.to_string() },
                agent: Some("orchestrator".into()),
                model: Some(dto::ModelRef {
                    id: "GLM-5.3-astra".into(),
                    providerID: "astra".into(),
                    variant: None,
                }),
                assistantMessageID: "msg_2".into(),
                snapshot: None,
                started: None,
            }));
            backend.push(exec_succeeded());
            let prompt = cx
                .send_request(PromptRequest::new(sid.clone(), vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            // Turn 3: `session.model.selected` remote switch → one push with
            // the new current value, Default gone again (concrete model).
            backend.push(exec_started());
            backend.push(model_selected(&sid.0, "openai", "gpt-6"));
            backend.push(exec_succeeded());
            let prompt = cx
                .send_request(PromptRequest::new(sid, vec![ContentBlock::Text(TextContent::new("hi"))]))
                .block_task()
                .await?;
            assert_eq!(prompt.stop_reason, acp::StopReason::EndTurn);
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        let notifications = collected.lock().expect("collected lock");
        let pushes = config_option_pushes(&notifications);
        assert_eq!(pushes.len(), 2, "turn 1 self-heal + turn 3 remote switch");
        // Turn 1 push: concrete model tracked from the step, Default listed
        // no more.
        let model = option_by_id(&pushes[0].config_options, "model");
        assert_eq!(as_select(model).current_value.0.as_ref(), "astra/GLM-5.3-astra");
        let groups = grouped(as_select(model));
        assert_ne!(groups[0].options[0].value.0.as_ref(), "__default__");
        // Turn 3 push: the remote model won.
        let model = option_by_id(&pushes[1].config_options, "model");
        assert_eq!(as_select(model).current_value.0.as_ref(), "openai/gpt-6");
    }

    #[tokio::test]
    async fn load_and_resume_use_session_record_agent_and_model() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        // The session record is authoritative (includes post-switch state):
        // agent build (not the derived default orchestrator) + a model.
        let mut info = wire_session("ses_mock_1", None, Some("/tmp/opencode/acp-fixture-project"), None);
        info.agent = Some("build".into());
        info.model = Some(dto::ModelRef {
            id: "GLM-5.3-astra".into(),
            providerID: "astra".into(),
            variant: None,
        });
        backend.set_session(info);
        backend.set_messages(vec![wire_msg("user", None), wire_msg("assistant", Some("orchestrator"))]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let load = cx
                .send_request(LoadSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let options = load.config_options.expect("config options present");
            let agent = option_by_id(&options, "agent");
            assert_eq!(as_select(agent).current_value.0.as_ref(), "build");
            let model = option_by_id(&options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "astra/GLM-5.3-astra");
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let options = resume.config_options.expect("config options present");
            let agent = option_by_id(&options, "agent");
            assert_eq!(as_select(agent).current_value.0.as_ref(), "build");
            let model = option_by_id(&options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "astra/GLM-5.3-astra");
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
        assert_eq!(
            backend.recorded_get_session_calls(),
            vec!["ses_mock_1".to_string(), "ses_mock_1".to_string()],
            "load + resume each fetch the session record"
        );
    }

    #[tokio::test]
    async fn load_falls_back_to_last_assistant_model_and_omits_drifted_agent_option() {
        let backend = MockBackend::new();
        backend.set_agents(vec![
            wire_agent("orchestrator", "primary", false, None),
            wire_agent("build", "primary", false, None),
        ]);
        backend.set_models(catalog_models());
        // get_session unconfigured → mock failure; the fallback chain is the
        // last assistant message's agent + model.
        backend.set_messages(vec![
            wire_msg("user", None),
            wire_msg_model(
                "assistant",
                Some("ghost-agent"),
                Some(dto::ModelRef {
                    id: "deepseek_v4_flash_code".into(),
                    providerID: "astra".into(),
                    variant: None,
                }),
            ),
        ]);
        let svc = Arc::new(AgentService::new(Arc::clone(&backend) as Arc<dyn OpenCodeBackend>));
        let (outcome, _collected) = run_client(svc, Arc::clone(&backend), move |_b, _c, cx| async move {
            let _ = cx
                .send_request(init_with_config_options())
                .block_task()
                .await?;
            let load = cx
                .send_request(LoadSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let options = load.config_options.expect("model option present");
            let model = option_by_id(&options, "model");
            assert_eq!(
                as_select(model).current_value.0.as_ref(),
                "astra/deepseek_v4_flash_code",
                "last assistant message's model"
            );
            // The last assistant agent drifted out of the visible list: the
            // agent option is omitted (never an unmatched current value).
            assert!(
                !options.iter().any(|o| o.id.0.as_ref() == "agent"),
                "drifted current agent must omit the agent option"
            );
            // No model in the messages at all → UNKNOWN → __default__ +
            // Default option.
            backend.set_messages(vec![wire_msg("user", None)]);
            let resume = cx
                .send_request(acp::ResumeSessionRequest::new("ses_mock_1", "/tmp/opencode/acp-fixture-project"))
                .block_task()
                .await?;
            let options = resume.config_options.expect("options present");
            let model = option_by_id(&options, "model");
            assert_eq!(as_select(model).current_value.0.as_ref(), "__default__");
            assert_eq!(
                grouped(as_select(model))[0].options[0].value.0.as_ref(),
                "__default__"
            );
            Ok(())
        })
        .await;
        outcome.expect("client run ok");
    }
}
