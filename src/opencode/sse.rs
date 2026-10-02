//! SSE event stream client for `GET /api/event`.
//!
//! The server speaks `text/event-stream`: `data: {json}` frames, keep-alive
//! `: heartbeat` comment lines, no Last-Event-ID replay (see
//! `docs/opencode-api.md`). Connection loss is retried forever with
//! exponential backoff (1 s → 2 s → 4 s → capped at 5 s, reset after a
//! successful connection).
//!
//! Frames are decoded in two steps (per the contract): `dto::EventEnvelope`,
//! then [`dto::decode_event`]. Unconsumed kinds (plugin `rpc.*`, future
//! types) are skipped silently; malformed frames are logged and skipped.
//!
//! All logging goes through `tracing` (stderr) — stdout is reserved for the
//! ACP JSON-RPC channel, so this module never prints to stdout.
//!
//! `#![allow(dead_code)]`: the stream is consumed by the ACP mapping in
//! Wave 2; until then the binary crate flags it as unused. Remove when wired.

#![allow(dead_code)]

use crate::dto::{decode_event, EventEnvelope, SessionEvent};
use crate::opencode::api::{ApiError, OpencodeClient};
use eventsource_stream::{Event, EventStreamError, Eventsource};
use futures_util::stream::{self, Stream, StreamExt};
use std::pin::Pin;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Backoff cap: 5 s between reconnect attempts.
const BACKOFF_CAP_SECS: u64 = 5;

/// Two-step decode of one SSE `data:` frame payload.
///
/// Returns `None` both for unconsumed event kinds (`rpc.*`, unknown types —
/// skipped silently by design) and for unparseable frames. The pure function
/// is also the unit-test seam: fixtures feed raw payloads straight in.
pub fn decode_frame(payload: &str) -> Option<SessionEvent> {
    let envelope: EventEnvelope = serde_json::from_str(payload).ok()?;
    decode_event(&envelope.kind, &envelope.data)
}

/// Like [`decode_frame`], but logs malformed envelopes at warn level.
/// Unknown-but-valid kinds are still skipped silently.
fn decode_frame_traced(payload: &str) -> Option<SessionEvent> {
    match serde_json::from_str::<EventEnvelope>(payload) {
        Ok(envelope) => decode_event(&envelope.kind, &envelope.data),
        Err(e) => {
            warn!(error = %e, "malformed event frame");
            None
        }
    }
}

/// The transport-error parameter for [`EventStreamError`]: reqwest failures
/// surface through this variant.
type StreamError = EventStreamError<reqwest::Error>;

/// Open one SSE connection. No per-request timeout (the stream is
/// long-lived); the client-level connect timeout still applies.
async fn open_event_stream(
    client: &OpencodeClient,
) -> Result<Pin<Box<dyn Stream<Item = Result<Event, StreamError>> + Send>>, ApiError> {
    let resp = client
        .event_request()
        .send()
        .await
        .map_err(|source| ApiError::Transport {
            method: "GET".to_string(),
            path: "/api/event".to_string(),
            source,
        })?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default().chars().take(500).collect();
        return Err(ApiError::Http {
            status,
            method: "GET".to_string(),
            path: "/api/event".to_string(),
            body,
        });
    }
    Ok(Box::pin(resp.bytes_stream().eventsource()))
}

/// Reconnect delay for the n-th attempt: 1 s, 2 s, 4 s, then 5 s cap.
fn backoff(attempt: u32) -> Duration {
    let secs = 2u64.saturating_pow(attempt.saturating_sub(1)).min(BACKOFF_CAP_SECS);
    Duration::from_secs(secs.max(1))
}

/// State machine for the reconnect loop.
enum State {
    /// Not connected. `attempt` = consecutive failed connect attempts since
    /// the last established connection (0 = very first connect, no delay).
    Connect { attempt: u32 },
    /// Connected; consuming frames from the live stream.
    Streaming {
        inner: Pin<Box<dyn Stream<Item = Result<Event, StreamError>> + Send>>,
    },
}

/// Stream of typed session events from `GET /api/event`, with automatic
/// reconnects.
///
/// The stream never terminates on its own: transient failures and server
/// disconnects trigger backoff + reconnect, newly delivered `server.connected`
/// frames re-appear as `None` (unconsumed kind, skipped by
/// [`dto::decode_event`]). Callers should filter events by `sessionID`.
pub fn event_stream(client: &OpencodeClient) -> Pin<Box<dyn Stream<Item = SessionEvent> + Send + '_>> {
    let client = client;
    // unfold produces Option<SessionEvent>; None fills "no event this step"
    // (connect/reconnect progress, skipped frames) and is filtered below.
    Box::pin(stream::unfold(State::Connect { attempt: 0 }, move |state| async move {
        {
            let (next_state, item) = match state {
                State::Connect { attempt } => {
                    if attempt > 0 {
                        let delay = backoff(attempt);
                        warn!(attempt, delay_ms = delay.as_millis(), "event stream reconnect");
                        sleep(delay).await;
                    }
                    match open_event_stream(client).await {
                        Ok(inner) => {
                            info!("connected to opencode event stream");
                            (State::Streaming { inner }, None)
                        }
                        Err(e) => {
                            warn!(attempt, error = %e, "event stream connect failed");
                            (State::Connect { attempt: attempt + 1 }, None)
                        }
                    }
                }
                State::Streaming { mut inner } => match inner.next().await {
                    Some(Ok(frame)) => {
                        let item = decode_frame_traced(&frame.data);
                        (State::Streaming { inner }, item)
                    }
                    Some(Err(e)) => {
                        warn!(error = %e, "event stream error, reconnecting");
                        (State::Connect { attempt: 1 }, None)
                    }
                    None => {
                        warn!("event stream closed, reconnecting");
                        (State::Connect { attempt: 1 }, None)
                    }
                },
            };
            Some((item, next_state))
        }
    })
    .filter_map(|item| async move { item }))
}

// ============================================================
// Unit tests (hermetic: fixtures only, no network)
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{SessionEvent::*, TextDelta};

    fn event_kind(ev: &SessionEvent) -> &'static str {
        match ev {
            ExecutionStarted(_) => "session.execution.started",
            ExecutionSucceeded(_) => "session.execution.succeeded",
            ExecutionFailed(_) => "session.execution.failed",
            ExecutionInterrupted(_) => "session.execution.interrupted",
            PermissionAsked(_) => "permission.asked",
            PermissionReplied(_) => "permission.replied",
            ToolFailed(_) => "session.tool.failed",
            StepFailed(_) => "session.step.failed",
            RetryScheduled(_) => "session.retry.scheduled",
            CompactionStarted(_) => "session.compaction.started",
            CompactionEnded(_) => "session.compaction.ended",
            CompactionFailed(_) => "session.compaction.failed",
            ModelUpdated(_) => "model.updated",
            ProviderUpdated(_) => "provider.updated",
            StepStarted(_) => "session.step.started",
            StepStreamed(_) => "session.step.streamed",
            StepEnded(_) => "session.step.ended",
            TextStarted(_) => "session.text.started",
            TextDelta(_) => "session.text.delta",
            TextEnded(_) => "session.text.ended",
            ReasoningStarted(_) => "session.reasoning.started",
            ReasoningDelta(_) => "session.reasoning.delta",
            ReasoningEnded(_) => "session.reasoning.ended",
            ToolInputStarted(_) => "session.tool.input.started",
            ToolInputEnded(_) => "session.tool.input.ended",
            ToolCalled(_) => "session.tool.called",
            ToolProgress(_) => "session.tool.progress",
            ToolSuccess(_) => "session.tool.success",
            UsageUpdated(_) => "session.usage.updated",
            Renamed(_) => "session.renamed",
            SessionCreated(_) => "session.created",
        }
    }

    /// Feed the raw captured turn through the same path the live stream
    /// uses: `data:` payload → decode_frame.
    fn decode_fixture(raw: &str) -> Vec<SessionEvent> {
        raw.lines()
            .filter_map(|line| {
                let line = line.trim();
                line.strip_prefix("data: ").and_then(decode_frame)
            })
            .collect()
    }

    #[test]
    fn parses_successful_tool_turn_fixture() {
        let raw = include_str!("../../tests/fixtures/sse-tool-turn.sse");
        let events = decode_fixture(raw);
        let kinds: Vec<&str> = events.iter().map(event_kind).collect();

        assert!(kinds.contains(&"session.text.delta"), "text delta present");
        assert!(kinds.contains(&"session.tool.success"), "tool success present");
        assert!(kinds.contains(&"session.execution.succeeded"), "turn gate present");
        // the turn the fixture captured: reasoning → write tool → text reply
        assert!(kinds.contains(&"session.reasoning.delta"));
        assert!(kinds.contains(&"session.tool.input.started"));
        assert!(kinds.contains(&"session.step.ended"));
        assert!(!kinds.contains(&"session.execution.failed"));

        // tool.success carries metadata.filediff (the #52636 diff-fix source)
        let success = events
            .iter()
            .find_map(|ev| match ev {
                ToolSuccess(t) if t.metadata.as_ref().and_then(|m| m.filediff.as_ref()).is_some() => Some(t),
                _ => None,
            })
            .expect("tool success with filediff");
        let md = success.metadata.as_ref().unwrap();
        let fd = md.filediff.as_ref().unwrap();
        assert!(fd.file.starts_with('/'), "filediff.file is absolute");
        assert!(fd.patch.starts_with("Index: "));
        assert_eq!(fd.additions, Some(1));
        assert!(md.title.is_some());

        // text deltas carry actual content
        let saw_done = events.iter().any(|ev| matches!(
            ev,
            TextDelta(TextDelta { base: _, delta }) if delta == "done"
        ));
        assert!(saw_done, "text.delta with payload");

        // sanity: a full turn produced a healthy event count
        assert!(kinds.len() > 20, "expected a real turn, got {}", kinds.len());
    }

    #[test]
    fn parses_failed_turn_fixture() {
        let raw = include_str!("../../tests/fixtures/sse-failed-turn.sse");
        let events = decode_fixture(raw);
        let kinds: Vec<&str> = events.iter().map(event_kind).collect();

        let failed = events
            .iter()
            .find_map(|ev| match ev {
                ExecutionFailed(f) => Some(f),
                _ => None,
            })
            .expect("execution.failed present");
        assert!(failed.error.message.is_some(), "failure carries an error message");
        assert!(!kinds.contains(&"session.execution.succeeded"));
    }

    #[test]
    fn unknown_and_malformed_frames_are_skipped() {
        // plugin traffic flows on the same stream — must vanish silently
        assert!(decode_frame(r#"{"type":"rpc.aft.statusInvalidated","data":{}}"#).is_none());
        assert!(decode_frame(r#"{"type":"rpc.aft.indexProgress","data":{"index":"search","status":"loading"}}"#).is_none());
        // future / unverified kinds
        assert!(decode_frame(r#"{"type":"session.deleted","data":{}}"#).is_none());
        // malformed JSON
        assert!(decode_frame("not json").is_none());
        assert!(decode_frame("").is_none());
    }

    #[test]
    fn backoff_escalates_to_cap() {
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(2), Duration::from_secs(2));
        assert_eq!(backoff(3), Duration::from_secs(4));
        assert_eq!(backoff(4), Duration::from_secs(5));
        assert_eq!(backoff(100), Duration::from_secs(5));
    }
}
