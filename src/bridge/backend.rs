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

use crate::acp::agent::{BoxFuture, EventStream, OpenCodeBackend};
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

    fn prompt(&self, session_id: &str, text: &str) -> BoxFuture<'_, Result<(), anyhow::Error>> {
        let session_id = session_id.to_string();
        let req = crate::dto::PromptRequest {
            text: text.to_string(),
            files: None,
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
            // `sse::event_stream` borrows its client; the task owns it and
            // forwards every decoded event into the channel. Dropping the
            // returned stream drops the receiver, which ends the task.
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                let mut stream = crate::opencode::sse::event_stream(&client);
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
}