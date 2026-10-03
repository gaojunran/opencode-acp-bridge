//! [`HttpBackend`] — the `acp::agent::OpenCodeBackend` implementation over
//! `opencode::api::OpencodeClient`. Trait method → client method is mostly
//! one-to-one; session IDs pass through verbatim (the ACP session id IS the
//! opencode `ses_*` id).
//!
//! The one structural wrinkle: the trait's [`EventStream`] must be `'static`,
//! while `opencode::sse::event_stream` borrows its client. The backend hops
//! the stream through a spawned task that *owns* the client and forwards
//! events over an unbounded channel (the task exits when the consumer drops
//! the stream). No leaks, no unsafe, no duplicated reconnect logic.

use futures_util::StreamExt;

use crate::acp::agent::{BoxFuture, EventStream, OpenCodeBackend, SessionList};
use crate::dto::{Location, SessionCreateRequest};
use crate::opencode::api::OpencodeClient;

/// Backend for one opencode server (`OpencodeClient` is cheap to clone: its
/// reqwest handle is an `Arc`).
#[derive(Debug, Clone)]
pub struct HttpBackend {
    client: OpencodeClient,
}

impl HttpBackend {
    pub fn new(client: OpencodeClient) -> Self {
        Self { client }
    }

    /// The underlying client (probe, model config, session cleanup).
    pub fn client(&self) -> &OpencodeClient {
        &self.client
    }
}

impl OpenCodeBackend for HttpBackend {
    fn create_session(&self, cwd: &str) -> BoxFuture<'_, Result<String, anyhow::Error>> {
        let req = SessionCreateRequest {
            id: None,
            title: None,
            agent: None,
            model: None,
            location: Location { directory: cwd.to_string() },
            metadata: None,
        };
        // The trait ties the future's lifetime to &self, so params are copied.
        Box::pin(async move { Ok(self.client.create_session(&req).await?.id) })
    }

    fn prompt(
        &self,
        session_id: &str,
        text: &str,
        files: &[crate::dto::PromptFile],
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let session_id = session_id.to_string();
        let req = crate::dto::PromptRequest {
            text: text.to_string(),
            files: if files.is_empty() {
                None
            } else {
                Some(files.to_vec())
            },
            agents: None,
            skills: None,
            metadata: None,
        };
        // The trait ties the future's lifetime to &self, so params are copied.
        Box::pin(async move {
            self.client.prompt(&session_id, &req).await?;
            Ok(())
        })
    }

    fn interrupt(&self, session_id: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let session_id = session_id.to_string();
        Box::pin(async move {
            self.client.interrupt(&session_id).await?;
            Ok(())
        })
    }

    fn messages(
        &self,
        session_id: &str,
    ) -> BoxFuture<'_, Result<Vec<crate::dto::MessageRecord>, anyhow::Error>> {
        let session_id = session_id.to_string();
        Box::pin(async move { Ok(self.client.messages(&session_id, None).await?.data) })
    }

    fn event_stream(&self, _session_id: &str) -> BoxFuture<'_, Result<EventStream, anyhow::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            // EAGER connect (completes before this future resolves — and
            // before the caller POSTs the prompt): the SSE subscriber is
            // attached inside `sse::event_stream`, so every turn event
            // emitted after that point is seen. A connect failure surfaces
            // as an error here instead of a silently dead channel. The
            // forwarding task then owns the connected, 'static stream and
            // pumps decoded events into the channel. Dropping the returned
            // stream drops the receiver, which ends the task.
            let mut stream = crate::opencode::sse::event_stream(client).await?;
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    if tx.send(event).is_err() {
                        break; // consumer dropped the stream
                    }
                }
            });
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
        decision: crate::dto::PermissionReply,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let session_id = session_id.to_string();
        let request_id = request_id.to_string();
        let req = crate::dto::PermissionReplyRequest { decision, message: None };
        Box::pin(async move {
            self.client.permission_reply(&session_id, &request_id, &req).await?;
            Ok(())
        })
    }

    // Wave 6a tail (authorized this wave): the session-management + catalog
    // overrides — live wire calls mirroring the trait's default-unavailable
    // contract, now in production.

    fn list_commands(&self) -> BoxFuture<'_, Option<Vec<serde_json::Value>>> {
        let client = self.client.clone();
        Box::pin(async move { client.commands().await.ok() })
    }

    fn list_sessions(
        &self,
        directory: Option<&str>,
        cursor: Option<&str>,
    ) -> BoxFuture<'_, Result<SessionList, anyhow::Error>> {
        let client = self.client.clone();
        let directory = directory.map(str::to_string);
        // ACP cursor is opaque — forward it as the wire `next` token.
        let cursor = cursor.map(|c| crate::dto::Cursor { previous: None, next: Some(c.to_string()) });
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

    fn agents(&self, directory: &str) -> BoxFuture<'_, Result<Vec<crate::dto::AgentInfo>, anyhow::Error>> {
        let client = self.client.clone();
        let directory = directory.to_string();
        Box::pin(async move { Ok(client.agents(&directory).await?) })
    }

    fn set_agent(&self, session_id: &str, agent: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let client = self.client.clone();
        let session_id = session_id.to_string();
        let agent = agent.to_string();
        Box::pin(async move {
            client.set_session_agent(&session_id, &agent).await?;
            Ok(())
        })
    }

    // Release 0.3.0: config-options support — the model catalog, the session
    // record (authoritative agent/model for load/resume) and the model
    // switch wire (the model config-option resolution).

    fn list_models(&self) -> BoxFuture<'_, Option<Vec<crate::dto::ModelInfo>>> {
        let client = self.client.clone();
        Box::pin(async move { client.models().await.ok() })
    }

    fn get_session(
        &self,
        session_id: &str,
    ) -> BoxFuture<'_, Result<crate::dto::SessionInfo, anyhow::Error>> {
        let client = self.client.clone();
        let session_id = session_id.to_string();
        Box::pin(async move { Ok(client.get_session(&session_id).await?) })
    }

    fn set_model(
        &self,
        session_id: &str,
        model: &crate::dto::ModelRef,
    ) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let client = self.client.clone();
        let session_id = session_id.to_string();
        let model = model.clone();
        Box::pin(async move {
            client.set_model(&session_id, &model).await?;
            Ok(())
        })
    }
}
