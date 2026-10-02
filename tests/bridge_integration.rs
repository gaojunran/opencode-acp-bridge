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
use agent_client_protocol::{Client, Error as AcpError, on_receive_notification};
use opencode_acp_bridge::acp::agent::{AgentService, OpenCodeBackend};
use opencode_acp_bridge::bridge::backend::HttpBackend;
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
                if u.fields.status == Some(ToolCallStatus::Completed) {
                    if let Some(content) = &u.fields.content {
                        if content.iter().any(|block| {
                            matches!(
                                block,
                                ToolCallContent::Diff(d)
                                    if d.path.to_string_lossy().ends_with("it-e2e.txt")
                            )
                        }) {
                            saw_completed_diff = true;
                        }
                    }
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