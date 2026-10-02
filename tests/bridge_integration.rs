//! Wave 2 (Lane C) integration tests. Both are opt-in via `BRIDGE_IT=1`
//! (same gate as Lane A's `integration_lane_a_flow` in src/opencode/api.rs)
//! and need the scratch 2.0.21 server on 127.0.0.1:47779 with auth
//! `opencode:test123` — restart it per the recipe at the end of
//! docs/opencode-api.md if it is down. NEVER touch 127.0.0.1:44041 / 34568.
//!
//! - [`lane_c_duplex_e2e`]: in-process — real [`HttpBackend`] + the acp crate
//!   as the client role over a duplex channel, driving the whole
//!   initialize → newSession → prompt → loadSession → delete lifecycle.
//! - [`binary_stdio_smoke`]: the compiled binary under `--attach`, fed one
//!   `initialize` over stdio and expected to answer then exit on stdin EOF —
//!   proves `main()` really assembles.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, LoadSessionRequest, NewSessionRequest, PromptRequest,
    SessionNotification, SessionUpdate, StopReason, TextContent, ToolCallContent, ToolCallStatus,
};
use agent_client_protocol::schema::v1 as acp;
use agent_client_protocol::{
    Agent, Client, ConnectionTo, Error as AcpError, Responder, on_receive_notification,
    on_receive_request,
};
use opencode_acp_bridge::acp::agent::{AgentService, BoxFuture, EventStream, OpenCodeBackend};
use opencode_acp_bridge::bridge::backend::HttpBackend;
use opencode_acp_bridge::dto;
use opencode_acp_bridge::dto::ModelRef;
use opencode_acp_bridge::opencode::api::OpencodeClient;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const SCRATCH: &str = "http://127.0.0.1:47779";
const SCRATCH_PASSWORD: &str = "test123";
const SESSION_DIR: &str = "/tmp/opencode/it-laneC";
const PROMPT_TEXT: &str =
    "Use the write tool to create it-e2e.txt with content: e2e ok. Then reply done.";
/// The scratch server's known-working model (docs/opencode-api.md).
const MODEL_ID: &str = "GLM-5.3-astra";
const MODEL_PROVIDER: &str = "astra";

fn it_enabled() -> bool {
    std::env::var("BRIDGE_IT").as_deref() == Ok("1")
}

/// Lane A accepts both the session dir and the project root for write-tool
/// files; so do we.
fn artifact_content(dir: &str) -> String {
    std::fs::read_to_string(format!("{dir}/it-e2e.txt"))
        .or_else(|_| std::fs::read_to_string("/tmp/opencode/it-e2e.txt"))
        .expect("it-e2e.txt written in the session dir or the project root")
}

fn remove_artifacts(dir: &str) {
    for p in [format!("{dir}/it-e2e.txt"), "/tmp/opencode/it-e2e.txt".to_string()] {
        if std::path::Path::new(&p).exists() {
            std::fs::remove_file(&p).expect("remove artifact");
        }
    }
}

// ============================================================
// In-process E2E: real backend + acp client over a duplex channel
// ============================================================

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1 and the scratch 2.0.21 server on 127.0.0.1:47779"]
async fn lane_c_duplex_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    std::fs::create_dir_all(SESSION_DIR).expect("mkdir it-laneC");
    remove_artifacts(SESSION_DIR);

    let client = OpencodeClient::new(SCRATCH, SCRATCH_PASSWORD).expect("valid scratch URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("scratch server {SCRATCH} unreachable (restart per docs/opencode-api.md recipe): {e}"));

    let http_backend = Arc::new(HttpBackend::new(client.clone()));
    let svc = Arc::new(AgentService::new(
        http_backend.clone() as Arc<dyn OpenCodeBackend>
    ));
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move { let _ = svc.serve(agent_side).await; }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));

    let outcome: Result<usize, AcpError> = Client
        .builder()
        .name("bridge-it-lane-c")
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            let all_updates = Arc::clone(&all_updates);
            async move |cx| {
                // 1. initialize — the agent must advertise session/load.
                let init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                assert!(init.agent_capabilities.load_session);

                // 2. newSession — ACP session id passes the opencode id through.
                let ns = cx
                    .send_request(NewSessionRequest::new(SESSION_DIR))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();
                assert!(
                    sid.0.starts_with("ses_"),
                    "opencode session id passthrough, got {}",
                    sid.0
                );

                // The ACP newSession surface carries no model — set it on the
                // wire directly (trait contract).
                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on scratch");

                // 3. prompt — the turn streams as session/update notifications
                //    and the prompt response only arrives at end_turn.
                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(PROMPT_TEXT))],
                ));
                let resp = tokio::time::timeout(Duration::from_secs(120), prompt.block_task())
                    .await
                    .expect("live turn did not finish within 120s")?;
                assert_eq!(resp.stop_reason, StopReason::EndTurn);

                // 4. loadSession — replay the persisted history (notifications
                //    arrive BEFORE the response; sequential ordering).
                let replayed_before = all_updates.lock().expect("updates lock").len();
                let load = cx
                    .send_request(LoadSessionRequest::new(sid.0.clone(), SESSION_DIR))
                    .block_task()
                    .await?;
                assert!(load.modes.is_none());

                // 5. cleanup the opencode session.
                client.delete_session(sid.0.as_ref()).await.expect("delete_session cleanup");

                Ok(replayed_before)
            }
        })
        .await;

    agent_task.abort();
    let replayed_before = outcome.expect("lane C client run ok");
    let updates = all_updates.lock().expect("updates lock");
    let (live, replayed) = updates.split_at(replayed_before);

    // ---- live turn: complete tool call WITH a diff, plus agent text ----
    let mut saw_completed_diff = false;
    let mut saw_agent_chunk = false;
    for n in live {
        match &n.update {
            SessionUpdate::ToolCallUpdate(u) => {
                if u.fields.status == Some(ToolCallStatus::Completed)
                    && let Some(content) = &u.fields.content
                        && content.iter().any(|block| {
                            matches!(
                                block,
                                ToolCallContent::Diff(d)
                                    if d.path.to_string_lossy().ends_with("it-e2e.txt")
                            )
                        }) {
                            saw_completed_diff = true;
                        }
            }
            SessionUpdate::AgentMessageChunk(_) => saw_agent_chunk = true,
            _ => {}
        }
    }
    assert!(
        saw_completed_diff,
        "live turn: ToolCallUpdate completed with a Diff block for it-e2e.txt ({} live updates)",
        live.len()
    );
    assert!(saw_agent_chunk, "live turn: AgentMessageChunk present");
    assert!(
        artifact_content(SESSION_DIR).contains("e2e ok"),
        "file was really written"
    );

    // ---- replay: full persisted history on the SAME session ----
    assert!(
        replayed.iter().any(|n| matches!(&n.update, SessionUpdate::UserMessageChunk(_))),
        "replay includes the user message"
    );
    assert!(
        replayed.iter().any(|n| matches!(&n.update, SessionUpdate::ToolCall(_))),
        "replay includes the tool call"
    );
    assert!(
        replayed.iter().any(|n| matches!(
            &n.update,
            SessionUpdate::ToolCallUpdate(u)
                if u.fields.status == Some(ToolCallStatus::Completed)
        )),
        "replay includes the completed tool call"
    );
    assert!(
        replayed.iter().any(|n| matches!(&n.update, SessionUpdate::AgentMessageChunk(_))),
        "replay includes the final assistant text"
    );

    remove_artifacts(SESSION_DIR);
}

// ============================================================
// Wave 6a E2E: session/list + resume + delete + commands push
// ============================================================

/// Test-local backend for the Wave 6a E2E: the core delegates to the real
/// [`HttpBackend`]; the Wave 6a surface (list/delete/commands) overrides
/// with live wire calls. The wave boundary keeps `src/bridge/**` frozen, so
/// the production overrides on `HttpBackend` land in a follow-up wave — this
/// wrapper exercises exactly the same wire calls the production override
/// will, through the same [`OpencodeClient`].
struct WireBackend {
    inner: Arc<HttpBackend>,
    client: OpencodeClient,
}

impl OpenCodeBackend for WireBackend {
    fn create_session(&self, cwd: &str) -> BoxFuture<'_, Result<String, anyhow::Error>> {
        self.inner.create_session(cwd)
    }

    fn prompt(&self, session_id: &str, text: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        self.inner.prompt(session_id, text)
    }

    fn interrupt(&self, session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        self.inner.interrupt(session_id)
    }

    fn messages(
        &self,
        session_id: &str,
    ) -> BoxFuture<'_, Result<Vec<dto::MessageRecord>, anyhow::Error>> {
        self.inner.messages(session_id)
    }

    fn event_stream(&self, session_id: &str) -> BoxFuture<'_, Result<EventStream, anyhow::Error>> {
        self.inner.event_stream(session_id)
    }

    fn permission_reply(
        &self,
        session_id: &str,
        request_id: &str,
        decision: dto::PermissionReply,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        self.inner.permission_reply(session_id, request_id, decision)
    }

    fn list_commands(&self) -> BoxFuture<'_, Option<Vec<serde_json::Value>>> {
        let client = self.client.clone();
        Box::pin(async move { client.commands().await.ok() })
    }

    fn list_sessions(
        &self,
        directory: Option<&str>,
        cursor: Option<&str>,
    ) -> BoxFuture<'_, Result<(Vec<dto::SessionInfo>, Option<String>), anyhow::Error>> {
        let client = self.client.clone();
        let directory = directory.map(str::to_string);
        // The ACP cursor is opaque — forward it as the `next` token.
        let cursor = cursor.map(|c| dto::Cursor { previous: None, next: Some(c.to_string()) });
        Box::pin(async move {
            let env = client.list_sessions(directory.as_deref(), cursor.as_ref()).await?;
            let next = env.cursor.as_ref().and_then(|c| c.next.clone());
            Ok((env.data, next))
        })
    }

    fn delete_session(&self, session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let client = self.client.clone();
        let session_id = session_id.to_string();
        Box::pin(async move {
            client.delete_session(&session_id).await?;
            Ok(())
        })
    }
}

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1 and the scratch 2.0.21 server on 127.0.0.1:47779"]
async fn wave6a_session_management_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    std::fs::create_dir_all(SESSION_DIR).expect("mkdir it-laneC");

    let client = OpencodeClient::new(SCRATCH, SCRATCH_PASSWORD).expect("valid scratch URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("scratch server {SCRATCH} unreachable (restart per docs/opencode-api.md recipe): {e}"));

    let wire_backend = Arc::new(WireBackend {
        inner: Arc::new(HttpBackend::new(client.clone())),
        client: client.clone(),
    });
    let svc = Arc::new(AgentService::new(
        wire_backend.clone() as Arc<dyn OpenCodeBackend>
    ));
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move { let _ = svc.serve(agent_side).await; }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));

    let outcome: Result<(), AcpError> = Client
        .builder()
        .name("bridge-it-wave6a")
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            let all_updates = Arc::clone(&all_updates);
            async move |cx| {
                // 1. initialize — the session-management ring is advertised.
                let init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let sc = &init.agent_capabilities.session_capabilities;
                assert!(sc.list.is_some() && sc.delete.is_some() && sc.resume.is_some(),
                    "list/delete/resume advertised: {sc:?}");

                // 2. newSession → initial available_commands_update push
                //    (non-empty: the scratch server has slash commands).
                let ns = cx
                    .send_request(NewSessionRequest::new(SESSION_DIR))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();
                assert!(sid.0.starts_with("ses_"), "opencode session id passthrough, got {}", sid.0);

                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                let push = loop {
                    let found = all_updates.lock().expect("updates lock").iter().find_map(|n| {
                        match &n.update {
                            acp::SessionUpdate::AvailableCommandsUpdate(u) if n.session_id == sid => {
                                Some(u.clone())
                            }
                            _ => None,
                        }
                    });
                    if let Some(found) = found {
                        break found;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "available_commands_update did not arrive within 10s"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                };
                assert!(
                    !push.available_commands.is_empty(),
                    "scratch server exposes slash commands"
                );
                assert!(push.available_commands.iter().all(|c| !c.name.is_empty()));
                assert!(
                    push.available_commands.iter().all(|c| c.input.is_none()),
                    "opencode commands carry no input schema"
                );

                // 3. Give the session real history (a live turn), so a
                //    broken resume that replays would be observable.
                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on scratch");
                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("Reply with: e2e resume ok"))],
                ));
                let resp = tokio::time::timeout(Duration::from_secs(120), prompt.block_task())
                    .await
                    .expect("live turn did not finish within 120s")?;
                assert_eq!(resp.stop_reason, StopReason::EndTurn);

                // 3b. session/list with the cwd filter sees the session,
                //     with an ISO 8601 UTC updatedAt from the wire time.
                let list = cx
                    .send_request(acp::ListSessionsRequest::new()
                        .cwd(std::path::PathBuf::from(SESSION_DIR)))
                    .block_task()
                    .await?;
                let listed = list
                    .sessions
                    .iter()
                    .find(|s| s.session_id == sid)
                    .expect("the new session appears in session/list");
                if let Some(ts) = &listed.updated_at {
                    assert_eq!(ts.len(), 20, "YYYY-MM-DDTHH:MM:SSZ: {ts}");
                    assert!(ts.as_bytes()[10] == b'T' && ts.ends_with('Z'), "ISO 8601 UTC: {ts}");
                }

                // 4. resume — zero replay. Only the resume's own commands
                //    push may arrive after the response.
                let before = all_updates.lock().expect("updates lock").len();
                let resume = cx
                    .send_request(acp::ResumeSessionRequest::new(sid.clone(), SESSION_DIR))
                    .block_task()
                    .await?;
                assert!(resume.modes.is_none());
                tokio::time::sleep(Duration::from_millis(250)).await; // stray replay would land by now
                let tail: Vec<_> = all_updates.lock().expect("updates lock")[before..].to_vec();
                for n in &tail {
                    assert!(
                        matches!(&n.update, acp::SessionUpdate::AvailableCommandsUpdate(_)),
                        "resume must not replay history, got {:?}",
                        n.update
                    );
                }

                // 5. delete — empty response; the session vanishes from list.
                let _ = cx
                    .send_request(acp::DeleteSessionRequest::new(sid.clone()))
                    .block_task()
                    .await?;
                let list = cx
                    .send_request(acp::ListSessionsRequest::new()
                        .cwd(std::path::PathBuf::from(SESSION_DIR)))
                    .block_task()
                    .await?;
                assert!(
                    !list.sessions.iter().any(|s| s.session_id == sid),
                    "deleted session gone from session/list"
                );

                Ok(())
            }
        })
        .await;

    agent_task.abort();
    // Belt and braces: never leave the scratch session behind.
    if let Ok(env) = client.list_sessions(Some(SESSION_DIR), None).await {
        for s in env.data {
            if s.id.starts_with("ses_") {
                let _ = client.delete_session(&s.id).await;
            }
        }
    }
    outcome.expect("wave 6a client run ok");
}

// ============================================================
// Subprocess smoke: the real binary over stdio
// ============================================================

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1 and the scratch 2.0.21 server on 127.0.0.1:47779"]
async fn binary_stdio_smoke() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_opencode-acp-bridge"))
        .args(["--attach", SCRATCH])
        .env("OPENCODE_PASSWORD", SCRATCH_PASSWORD)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit()) // probe/server logs visible on failure
        .kill_on_drop(true)
        .spawn()
        .expect("spawn the bridge binary");

    let mut stdin = child.stdin.take().expect("child stdin");
    let mut stdout_lines =
        tokio::io::BufReader::new(child.stdout.take().expect("child stdout")).lines();

    // A minimal ACP v1 initialize (line-delimited JSON-RPC).
    let initialize =
        b"{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"initialize\",\"params\":{\"protocolVersion\":1}}\n";
    stdin.write_all(initialize).await.expect("write initialize");
    stdin.flush().await.expect("flush stdin");

    let line = tokio::time::timeout(Duration::from_secs(30), stdout_lines.next_line())
        .await
        .expect("initialize response within 30s (server startup probe included)")
        .expect("reading child stdout")
        .expect("stdout produced a line");
    let resp: serde_json::Value = serde_json::from_str(&line).expect("stdout is JSON-RPC");
    assert_eq!(resp["id"].as_i64(), Some(0), "response correlates: {resp}");
    assert_eq!(resp["result"]["protocolVersion"].as_i64(), Some(1), "{resp}");
    assert_eq!(
        resp["result"]["agentCapabilities"]["loadSession"].as_bool(),
        Some(true),
        "{resp}"
    );
    assert_eq!(resp["result"]["agentInfo"]["name"], "opencode-acp-bridge", "{resp}");

    // stdin EOF must shut the server down gracefully.
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("bridge exits after stdin EOF (30s)")
        .expect("wait for the bridge");
    assert!(status.success(), "clean exit after stdin EOF: {status:?}");
}

// ============================================================
// Wave 3 E2E: live permission loop — the fixture project's
// `[permission] bash = "ask"` rule routes bash asks to the ACP
// client, whose reply unblocks the turn (contract verified in
// docs/opencode-api.md "Permission loop").
// ============================================================

const FIXTURE_DIR: &str = "/tmp/opencode/acp-fixture-project";
const PERM_PROMPT: &str = "Use the bash tool now to run: echo perm-e2e-ok";

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1, the scratch server, and the fixture project's ask rule"]
async fn wave3_permission_loop_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    // Make the bridge's tracing visible for live diagnosis (main() wires
    // this in production; the in-process harness needs it here).
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();

    let client = OpencodeClient::new(SCRATCH, SCRATCH_PASSWORD).expect("valid scratch URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("scratch server {SCRATCH} unreachable (restart per docs/opencode-api.md recipe): {e}"));

    let http_backend = Arc::new(HttpBackend::new(client.clone()));
    let svc = Arc::new(AgentService::new(
        http_backend.clone() as Arc<dyn OpenCodeBackend>
    ));
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move { let _ = svc.serve(agent_side).await; }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));
    let seen_asks = Arc::new(Mutex::new(Vec::new()));
    let sid_seen = Arc::new(Mutex::new(String::new()));

    let outcome: Result<(), AcpError> = Client
        .builder()
        .name("bridge-it-wave3")
        .on_receive_request(
            {
                let seen_asks = Arc::clone(&seen_asks);
                async move |req: acp::RequestPermissionRequest,
                            responder: Responder<acp::RequestPermissionResponse>,
                            _cx: ConnectionTo<Agent>| {
                    seen_asks.lock().expect("asks lock").push(req);
                    // Reply "once" — official routing: selected once → allow once.
                    responder.respond(acp::RequestPermissionResponse::new(
                        acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new("once")),
                    ))
                }
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            let sid_seen = Arc::clone(&sid_seen);
            let all_updates = Arc::clone(&all_updates);
            let seen_asks = Arc::clone(&seen_asks);
            async move |cx| {
                let _init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let ns = cx
                    .send_request(NewSessionRequest::new(FIXTURE_DIR))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();
                *sid_seen.lock().expect("sid lock") = sid.0.as_ref().to_string();

                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on scratch");

                // The turn pauses on the bash ask; the "once" reply from the
                // request handler above unblocks it.
                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(PERM_PROMPT))],
                ));
                let resp = match tokio::time::timeout(
                    Duration::from_secs(180),
                    prompt.block_task(),
                )
                .await
                {
                    Ok(r) => r?,
                    Err(_) => {
                        eprintln!(
                            "DIAG after 180s: updates received: {}, asks seen by client: {}",
                            all_updates.lock().expect("updates lock").len(),
                            seen_asks.lock().expect("asks lock").len()
                        );
                        panic!("live permission turn did not finish within 180s");
                    }
                };
                assert_eq!(resp.stop_reason, StopReason::EndTurn);

                client.delete_session(sid.0.as_ref()).await.expect("delete_session cleanup");
                Ok(())
            }
        })
        .await;

    agent_task.abort();
    outcome.expect("wave3 client run ok");

    // ---- the ask reached the client with the official shape ----
    let asks = seen_asks.lock().expect("asks lock");
    assert_eq!(asks.len(), 1, "exactly one permission ask, got {}", asks.len());
    let ask = &asks[0];
    assert_eq!(
        ask.session_id.0.as_ref(),
        sid_seen.lock().expect("sid lock").as_str(),
        "the ask targets the opencode session"
    );
    let options = &ask.options;
    assert_eq!(options.len(), 3);
    assert_eq!(options[0].option_id.0.as_ref(), "once");
    assert_eq!(options[1].option_id.0.as_ref(), "always");
    assert_eq!(options[2].option_id.0.as_ref(), "reject");

    let tc = &ask.tool_call;
    assert!(
        tc.tool_call_id.0.starts_with("call_"),
        "toolCallId is the triggering tool call, got {}",
        tc.tool_call_id.0
    );
    assert!(
        tc.fields
            .title
            .as_deref()
            .is_some_and(|t| t.contains("echo perm-e2e-ok")),
        "title carries the command: {:?}",
        tc.fields.title
    );
    assert_eq!(tc.fields.status, Some(acp::ToolCallStatus::Pending));
    let raw = tc
        .fields
        .raw_input
        .as_ref()
        .expect("input merged from the cached tool input");
    assert!(
        serde_json::to_string(raw).expect("serialize input").contains("echo perm-e2e-ok"),
        "cached bash input reaches the client: {raw}"
    );

    // ---- the reply really unblocked the turn: the asked tool completed ----
    let updates = all_updates.lock().expect("updates lock");
    assert!(
        updates.iter().any(|n| matches!(
            &n.update,
            SessionUpdate::ToolCallUpdate(u)
                if u.fields.status == Some(ToolCallStatus::Completed)
                    && u.tool_call_id == tc.tool_call_id
        )),
        "the asked tool call completed after the once-reply ({} updates)",
        updates.len()
    );
}
// ---------------------------------------------------------------------------
// Wave 4: child-session projection (#48232)
// ---------------------------------------------------------------------------

const SUBAGENT_PROMPT: &str = "Use the subagent tool with agent \"explorer\" and description \
     \"Find markdown files in project\" to find all markdown files in this project directory. \
     Then reply with a one-line summary of what the subagent found.";

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1, the scratch server, and the subagent tool (OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS)"]
async fn wave4_child_projection_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();

    // Idempotent fixture content: give the explorer child something to find.
    std::fs::write(format!("{FIXTURE_DIR}/notes-a.md"), "# Notes A\nwave4 fixture\n")
        .expect("write notes-a.md");
    std::fs::write(format!("{FIXTURE_DIR}/notes-b.md"), "# Notes B\nwave4 fixture\n")
        .expect("write notes-b.md");

    let client = OpencodeClient::new(SCRATCH, SCRATCH_PASSWORD).expect("valid scratch URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("scratch server {SCRATCH} unreachable: {e}"));

    let http_backend = Arc::new(HttpBackend::new(client.clone()));
    let svc = Arc::new(AgentService::new(
        http_backend.clone() as Arc<dyn OpenCodeBackend>,
    ));
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move {
            let _ = svc.serve(agent_side).await;
        }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));
    let sid_seen = Arc::new(Mutex::new(String::new()));

    let outcome: Result<(), AcpError> = Client
        .builder()
        .name("bridge-it-wave4")
        // A child bash ask (fixture rule bash=ask) must not hang the turn:
        // auto-allow once. The ask→reply routing to the child sessionID is
        // unit-tested; here it only must not deadlock the projection.
        .on_receive_request(
            async move |req: acp::RequestPermissionRequest,
                        responder: Responder<acp::RequestPermissionResponse>,
                        _cx: ConnectionTo<Agent>| {
                let _ = req;
                responder.respond(acp::RequestPermissionResponse::new(
                    acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                        "once",
                    )),
                ))
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            let sid_seen = Arc::clone(&sid_seen);
            let all_updates = Arc::clone(&all_updates);
            async move |cx| {
                let _init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let ns = cx
                    .send_request(NewSessionRequest::new(FIXTURE_DIR))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();
                *sid_seen.lock().expect("sid lock") = sid.0.as_ref().to_string();

                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on scratch");

                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(SUBAGENT_PROMPT))],
                ));
                let resp = match tokio::time::timeout(
                    Duration::from_secs(180),
                    prompt.block_task(),
                )
                .await
                {
                    Ok(r) => r?,
                    Err(_) => {
                        eprintln!(
                            "DIAG after 180s: updates received: {}",
                            all_updates.lock().expect("updates lock").len()
                        );
                        panic!("live subagent turn did not finish within 180s");
                    }
                };
                assert_eq!(resp.stop_reason, StopReason::EndTurn);

                client.delete_session(sid.0.as_ref()).await.expect("delete_session cleanup");
                Ok(())
            }
        })
        .await;

    agent_task.abort();
    outcome.expect("wave4 client run ok");

    let sid = sid_seen.lock().expect("sid lock").clone();
    let updates = all_updates.lock().expect("updates lock");
    assert!(!updates.is_empty(), "no session updates collected");

    // 1. Every notification is addressed to the PARENT session — child
    //    events ride the parent's ACP session (#48232 projection).
    for n in updates.iter() {
        assert_eq!(
            n.session_id.0.as_ref(),
            sid.as_str(),
            "update addressed to the parent session, got {:?}",
            n.update
        );
    }

    // 2. The parent spawned a subagent: an unprefixed tool call titled
    //    "subagent".
    assert!(
        updates.iter().any(|n| matches!(
            &n.update,
            SessionUpdate::ToolCallUpdate(u)
                if !u.tool_call_id.0.contains(':')
                    && u.fields.title.as_deref() == Some("subagent")
        )),
        "the subagent spawn is a normal parent tool call ({} updates)",
        updates.len()
    );

    // 3. Child tool events project under the `${child.id}:` namespace, and
    //    at least one child call completed.
    let child_calls: Vec<_> = updates
        .iter()
        .filter_map(|n| match &n.update {
            SessionUpdate::ToolCallUpdate(u) if u.tool_call_id.0.contains(":call_") => Some(u),
            _ => None,
        })
        .collect();
    assert!(
        !child_calls.is_empty(),
        "child tool calls projected with prefixed ids ({} updates)",
        updates.len()
    );
    assert!(
        child_calls
            .iter()
            .any(|u| u.fields.status == Some(ToolCallStatus::Completed)),
        "at least one child tool call completed"
    );

    eprintln!(
        "wave4 child projection: {} updates, {} child calls",
        updates.len(),
        child_calls.len()
    );
}

// ---------------------------------------------------------------------------
// Wave 5: aft dialect — live validation against an aft-active server
// ---------------------------------------------------------------------------

/// 1×1 PNG (70 bytes): the smallest file that makes aft's hoisted `read`
/// emit a data-URI file part.
const PIXEL_PNG: [u8; 70] = [
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
    0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 13, 73, 68, 65, 84, 120, 218, 99, 252, 207, 192, 80, 15,
    0, 4, 133, 1, 128, 132, 169, 140, 33, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];

/// Live aft-server coordinates come from the environment (never hardcode a
/// personal path or a live password in the repo):
/// `AFT_SERVER_URL`, `AFT_SERVER_PASSWORD`, `AFT_SERVER_DIR` (an aft-active
/// project directory on that server).
fn aft_server_env() -> Option<(String, String, String)> {
    let url = std::env::var("AFT_SERVER_URL").ok()?;
    let password = std::env::var("AFT_SERVER_PASSWORD").ok()?;
    let dir = std::env::var("AFT_SERVER_DIR").ok()?;
    Some((url, password, dir))
}

/// How the image-read turn ended. On this deployment the relay rejects
/// image input ("Model only supports text input; ... 'image_url'"), so a
/// turn that reads an image cannot reach EndTurn — the mapping under test
/// (file part → ACP image block) is delivered before the turn fails. A
/// vision-capable relay would end the turn normally; both are acceptable.
enum TurnOutcome {
    Ended(StopReason),
    Failed(String),
}

/// One image-read turn through the given service; returns the collected
/// session notifications and how the turn ended.
async fn run_image_read_turn(
    svc: Arc<AgentService>,
    client: OpencodeClient,
    dir: &str,
) -> (Vec<SessionNotification>, TurnOutcome) {
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move {
            let _ = svc.serve(agent_side).await;
        }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));
    let outcome_seen: Arc<Mutex<Option<TurnOutcome>>> = Arc::new(Mutex::new(None));

    let prompt_text = format!(
        "Use the read tool on {dir}/.aft-e2e/pixel.png. Do not use any other tools. \
         Then reply DONE with a one-sentence description of the image."
    );

    let outcome: Result<(), AcpError> = Client
        .builder()
        .name("bridge-it-wave5")
        .on_receive_request(
            async move |req: acp::RequestPermissionRequest,
                        responder: Responder<acp::RequestPermissionResponse>,
                        _cx: ConnectionTo<Agent>| {
                let _ = req;
                responder.respond(acp::RequestPermissionResponse::new(
                    acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                        "once",
                    )),
                ))
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            let prompt_text = prompt_text.clone();
            let outcome_seen = Arc::clone(&outcome_seen);
            async move |cx| {
                let _init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let ns = cx
                    .send_request(NewSessionRequest::new(dir.to_string()))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();

                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on the aft server");

                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(prompt_text))],
                ));
                let resp =
                    match tokio::time::timeout(Duration::from_secs(180), prompt.block_task()).await
                    {
                        Ok(r) => r,
                        Err(_) => panic!("live image-read turn did not finish within 180s"),
                    };
                let seen = match resp {
                    Ok(r) => TurnOutcome::Ended(r.stop_reason),
                    Err(e) => TurnOutcome::Failed(format!("{e}")),
                };
                *outcome_seen.lock().expect("outcome lock") = Some(seen);

                client.delete_session(sid.0.as_ref()).await.expect("delete_session cleanup");
                Ok(())
            }
        })
        .await;

    agent_task.abort();
    outcome.expect("wave5 client run ok");
    let updates = all_updates.lock().expect("updates lock").clone();
    assert!(!updates.is_empty(), "no session updates collected");
    let seen = outcome_seen
        .lock()
        .expect("outcome lock")
        .take()
        .expect("turn outcome recorded");
    (updates, seen)
}

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1 plus AFT_SERVER_URL/AFT_SERVER_PASSWORD/AFT_SERVER_DIR (aft-active opencode)"]
async fn wave5_aft_image_passthrough_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }
    let Some((url, password, dir)) = aft_server_env() else {
        eprintln!("skipped: AFT_SERVER_* not set (aft-active server required)");
        return;
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();

    let png_dir = format!("{dir}/.aft-e2e");
    std::fs::create_dir_all(&png_dir).expect("create .aft-e2e");
    std::fs::write(format!("{png_dir}/pixel.png"), PIXEL_PNG).expect("write pixel.png");

    let client = OpencodeClient::new(&url, &password).expect("valid aft server URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("aft server {url} unreachable: {e}"));

    // ---- phase 1: aft adaptation ON — the image reaches the ACP client ----
    let http_backend = Arc::new(HttpBackend::new(client.clone()));
    let svc = Arc::new(AgentService::new(
        http_backend.clone() as Arc<dyn OpenCodeBackend>,
    ));
    let (updates, outcome) = run_image_read_turn(Arc::clone(&svc), client.clone(), &dir).await;
    match &outcome {
        TurnOutcome::Ended(reason) => assert_eq!(*reason, StopReason::EndTurn),
        // This relay rejects image input, so the turn fails AFTER the image
        // block was delivered — the only acceptable failure mode here.
        TurnOutcome::Failed(err) => assert!(
            err.contains("text input") || err.contains("image_url"),
            "unexpected turn failure: {err}"
        ),
    }

    let has_image = |updates: &[SessionNotification]| {
        updates.iter().any(|n| {
            let SessionUpdate::ToolCallUpdate(u) = &n.update else {
                return false;
            };
            u.fields.status == Some(ToolCallStatus::Completed)
                && u.fields
                    .content
                    .as_ref()
                    .is_some_and(|blocks| {
                        blocks.iter().any(|b| {
                            matches!(
                                b,
                                acp::ToolCallContent::Content(c)
                                    if matches!(c.content, acp::ContentBlock::Image(_))
                            )
                        })
                    })
        })
    };
    let census = |updates: &[SessionNotification]| {
        updates
            .iter()
            .filter_map(|n| {
                let SessionUpdate::ToolCallUpdate(u) = &n.update else {
                    return None;
                };
                let blocks = u
                    .fields
                    .content
                    .as_ref()
                    .map(|bs| {
                        bs.iter()
                            .map(|b| match b {
                                acp::ToolCallContent::Content(c) => match c.content {
                                    acp::ContentBlock::Image(_) => "image",
                                    acp::ContentBlock::Text(_) => "text",
                                    _ => "other-content",
                                },
                                _ => "non-content",
                            })
                            .collect::<Vec<_>>()
                            .join("+")
                    })
                    .unwrap_or_default();
                Some(format!(
                    "[{}] {} content=[{}]",
                    u.tool_call_id.0,
                    u.fields.title.as_deref().unwrap_or("?"),
                    blocks
                ))
            })
            .collect::<Vec<_>>()
    };
    assert!(
        has_image(&updates),
        "aft image read must map to an ACP image block ({} updates); tool calls seen:\n  {}",
        updates.len(),
        census(&updates).join("\n  ")
    );

    // ---- phase 2: --no-aft — the image block is dropped ----
    let svc = Arc::new(
        AgentService::new(http_backend.clone() as Arc<dyn OpenCodeBackend>).with_no_aft(true),
    );
    let (updates, outcome) = run_image_read_turn(svc, client.clone(), &dir).await;
    match &outcome {
        TurnOutcome::Ended(reason) => assert_eq!(*reason, StopReason::EndTurn),
        TurnOutcome::Failed(err) => assert!(
            err.contains("text input") || err.contains("image_url"),
            "unexpected turn failure: {err}"
        ),
    }
    assert!(
        !has_image(&updates),
        "--no-aft must drop the image passthrough ({} updates)",
        updates.len()
    );

    std::fs::remove_dir_all(&png_dir).expect("remove .aft-e2e");
}

// ---------------------------------------------------------------------------
// Wave 5.5: files[] structured diff rung — live apply_patch turn on scratch
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires BRIDGE_IT=1 (scratch opencode)"]
async fn wave5_5_apply_patch_files_diff_e2e() {
    if !it_enabled() {
        eprintln!("skipped: BRIDGE_IT=1 not set");
        return;
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();

    let client = OpencodeClient::new(SCRATCH, SCRATCH_PASSWORD).expect("valid scratch URL");
    client
        .config()
        .await
        .unwrap_or_else(|e| panic!("scratch {SCRATCH} unreachable: {e}"));

    let svc = Arc::new(AgentService::new(
        Arc::new(HttpBackend::new(client.clone())) as Arc<dyn OpenCodeBackend>,
    ));
    let (client_side, agent_side) = agent_client_protocol::Channel::duplex();
    let agent_task = tokio::spawn({
        let svc = Arc::clone(&svc);
        async move {
            let _ = svc.serve(agent_side).await;
        }
    });

    let all_updates = Arc::new(Mutex::new(Vec::<SessionNotification>::new()));

    let outcome: Result<(), AcpError> = Client
        .builder()
        .name("bridge-it-wave5_5")
        .on_receive_request(
            async move |req: acp::RequestPermissionRequest,
                        responder: Responder<acp::RequestPermissionResponse>,
                        _cx: ConnectionTo<Agent>| {
                let _ = req;
                responder.respond(acp::RequestPermissionResponse::new(
                    acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                        "once",
                    )),
                ))
            },
            on_receive_request!(),
        )
        .on_receive_notification(
            {
                let all_updates = Arc::clone(&all_updates);
                async move |notif: SessionNotification, _cx| {
                    all_updates.lock().expect("updates lock").push(notif);
                    Ok(())
                }
            },
            on_receive_notification!(),
        )
        .connect_with(client_side, {
            let client = client.clone();
            async move |cx| {
                let _init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                let ns = cx
                    .send_request(NewSessionRequest::new(FIXTURE_DIR.to_string()))
                    .block_task()
                    .await?;
                let sid = ns.session_id.clone();

                client
                    .set_model(
                        &sid.0,
                        &ModelRef {
                            id: MODEL_ID.into(),
                            providerID: MODEL_PROVIDER.into(),
                            variant: None,
                        },
                    )
                    .await
                    .expect("set_model on scratch");

                let prompt = cx.send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(
                        "Use the apply_patch tool to add a new file files-rung-e2e.txt \
                         containing exactly one line: rung-ok. Then reply DONE.",
                    ))],
                ));
                let resp =
                    match tokio::time::timeout(Duration::from_secs(180), prompt.block_task()).await
                    {
                        Ok(r) => r?,
                        Err(_) => panic!("apply_patch turn did not finish within 180s"),
                    };
                assert_eq!(resp.stop_reason, StopReason::EndTurn);

                client.delete_session(sid.0.as_ref()).await.expect("delete_session cleanup");
                Ok(())
            }
        })
        .await;

    agent_task.abort();
    outcome.expect("wave5_5 client run ok");
    let updates = all_updates.lock().expect("updates lock").clone();
    assert!(!updates.is_empty(), "no session updates collected");

    // ② rung: apply_patch diff blocks come from files[].filePath (absolute,
    // not inferred from the Index: header), type=add forces old_text=None.
    let mut saw_diff = false;
    for n in &updates {
        let SessionUpdate::ToolCallUpdate(u) = &n.update else {
            continue;
        };
        if u.fields.status != Some(ToolCallStatus::Completed) {
            continue;
        }
        let Some(blocks) = &u.fields.content else { continue };
        for b in blocks {
            let ToolCallContent::Diff(d) = b else { continue };
            assert!(
                d.path.to_string_lossy().ends_with("files-rung-e2e.txt"),
                "diff path must be the absolute files[].filePath, got {}",
                d.path.display()
            );
            assert!(
                d.new_text.contains("rung-ok"),
                "new_text must carry the added line, got {:?}",
                d.new_text
            );
            assert!(
                d.old_text.is_none(),
                "type=add must force old_text=None, got {:?}",
                d.old_text
            );
            saw_diff = true;
        }
    }
    assert!(
        saw_diff,
        "apply_patch turn must produce a diff block via the files[] rung ({} updates)",
        updates.len()
    );

    let _ = std::fs::remove_file(format!("{FIXTURE_DIR}/files-rung-e2e.txt"));
}
