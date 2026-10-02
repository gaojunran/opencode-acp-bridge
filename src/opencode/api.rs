//! REST client for the opencode 2.0.21 `/api/*` surface.
//!
//! Contract: `docs/opencode-api.md` (verified wire dialect) +
//! `tests/fixtures/openapi-2021.json` (authoritative schemas), both validated
//! against a live 2.0.21 server. Response shapes were probed live on
//! `127.0.0.1:47779` (scratch server, `opencode:test123`) before this file was
//! written:
//!
//! - Most endpoints answer `{"data": …, "location"?: …, "cursor"?: …}` →
//!   parsed via [`crate::dto::Envelope`].
//! - `PATCH /session/{id}`-style mutations answer **204 no body**
//!   (PATCH session, set model, delete session, delete message).
//! - `POST /session/{id}/interrupt` answers **bare** `{"interrupted": bool}`
//!   (not enveloped) → [`InterruptResponse`].
//! - `GET /config` answers a **bare array** (not enveloped).
//! - `POST …/compact` and `POST …/fork` reject a missing body with
//!   `InvalidRequestError "Expected object"` — always send `{}`-shaped objects.
//!
//! Request types that live only inside this lane (not yet in `dto.rs`) are
//! defined here: [`CommandRequest`], [`SessionPatchRequest`], [`ForkRequest`],
//! [`InterruptResponse`] — see the lane report for the dto additive list.
//!
//! All errors are [`ApiError`]; response bodies are truncated to the first 500
//! characters in error messages.
//!
//! `#![allow(dead_code)]`: this is a binary crate and the lane is not wired
//! into `main.rs` until Wave 2 — every method below is part of the public
//! surface the ACP mapping consumes. Remove this attribute when that wiring
//! lands.

#![allow(dead_code)]

use crate::dto::{
    Cursor, Envelope, InboxUserMessage, MessageRecord, MessagesEnvelope, ModelInfo, ModelRef,
    PermissionReplyRequest, PromptRequest, ProviderInfo, SessionCreateRequest, SessionInfo,
};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, Response, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

/// Longest a single API call waits for the server (prompt/compact can take a
/// while; the event stream deliberately sets no timeout).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// TCP connect timeout for every request, including the event stream.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Basic-auth username (fixed by the server; the password is the server's
/// `OPENCODE_PASSWORD`, passed in by the caller).
const AUTH_USER: &str = "opencode";

// ============================================================
// Errors
// ============================================================

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("invalid base URL `{given}`: {detail}")]
    BadBase {
        given: String,
        detail: String,
    },
    #[error("HTTP {status} on {method} {path}: {body}")]
    Http {
        status: u16,
        method: String,
        path: String,
        /// First 500 chars of the response body (error JSON like
        /// `{"_tag":"InvalidRequestError",…}` fits comfortably).
        body: String,
    },
    #[error("transport error on {method} {path}: {source}")]
    Transport {
        method: String,
        path: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to serialize request body for {method} {path}: {source}")]
    Encode {
        method: String,
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("malformed response on {method} {path}: {source}")]
    Decode {
        method: String,
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Truncate an error body to the first 500 characters.
fn truncate_body(s: String) -> String {
    s.chars().take(500).collect()
}

// ============================================================
// Client
// ============================================================

/// HTTP client for one opencode 2.0.21 server instance.
///
/// `base_url` is the server **root** (e.g. `http://127.0.0.1:44041`); the
/// `/api` prefix is appended internally — never pass a URL that already ends
/// in `/api`. Password parsing from the environment belongs to the caller.
#[derive(Debug, Clone)]
pub struct OpencodeClient {
    http: Client,
    /// `{root}/api` — where every endpoint is mounted.
    api_base: String,
    /// Basic-auth password (the server's `OPENCODE_PASSWORD`).
    password: String,
}

impl OpencodeClient {
    pub fn new(base_url: impl Into<String>, password: impl Into<String>) -> Result<Self, ApiError> {
        let base = base_url.into();
        Url::parse(&base).map_err(|e| ApiError::BadBase {
            given: base.clone(),
            detail: e.to_string(),
        })?;
        let api_base = format!("{}/api", base.trim_end_matches('/'));
        Ok(Self {
            http: Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .expect("reqwest ClientBuilder::build only fails on malformed TLS config"),
            api_base,
            password: password.into(),
        })
    }

    /// Build the URL for a path under `/api` (path segments must already be
    /// percent-encoded via [`encode_segment`]).
    fn endpoint_url(&self, path: &str) -> Url {
        Url::parse(&format!("{}{}", self.api_base, path))
            .expect("api_base is a valid URL root; literal path appends stay valid")
    }

    /// Start a request with basic auth and a default timeout.
    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.http
            .request(method, url)
            .basic_auth(AUTH_USER, Some(&self.password))
            .timeout(REQUEST_TIMEOUT)
    }

    /// SSE request for `sse.rs`: GET /api/event, authenticated, **no**
    /// per-request timeout (the stream is long-lived; reconnects happen in the
    /// stream layer). A connect timeout still applies from the client.
    pub(crate) fn event_request(&self) -> RequestBuilder {
        self.http
            .request(Method::GET, self.endpoint_url("/event"))
            .basic_auth(AUTH_USER, Some(&self.password))
    }

    /// Execute a request, map transport failures, and surface non-2xx as
    /// [`ApiError::Http`] (body truncated to 500 chars).
    async fn execute(&self, method: Method, url: Url, body: Option<String>) -> Result<Response, ApiError> {
        let method_name = method.to_string();
        let path = url.path().to_string();
        let mut rb = self.request(method, url);
        if let Some(json) = body {
            rb = rb.header(CONTENT_TYPE, HeaderValue::from_static("application/json")).body(json);
        }
        let resp = rb.send().await.map_err(|source| ApiError::Transport {
            method: method_name.clone(),
            path: path.clone(),
            source,
        })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = truncate_body(resp.text().await.unwrap_or_default());
            return Err(ApiError::Http {
                status,
                method: method_name,
                path,
                body,
            });
        }
        Ok(resp)
    }

    /// Execute and parse the `dto::Envelope<T>` JSON response.
    async fn send_json<T: DeserializeOwned>(
        &self,
        method: Method,
        url: Url,
        body: Option<String>,
    ) -> Result<Envelope<T>, ApiError> {
        let resp = self.execute(method.clone(), url, body).await?;
        let path = resp.url().path().to_string();
        let bytes = resp.bytes().await.map_err(|source| ApiError::Transport {
            method: method.to_string(),
            path: path.clone(),
            source,
        })?;
        serde_json::from_slice(&bytes).map_err(|source| ApiError::Decode {
            method: method.to_string(),
            path,
            source,
        })
    }

    /// Execute and ignore the body (204-style mutations; also tolerates 200).
    async fn send_unit(&self, method: Method, url: Url, body: Option<String>) -> Result<(), ApiError> {
        self.execute(method, url, body).await?;
        Ok(())
    }

    /// Serialize a request body, mapping failures to [`ApiError::Encode`].
    fn encode<T: Serialize + ?Sized>(&self, method: &Method, url: &Url, value: &T) -> Result<String, ApiError> {
        serde_json::to_string(value).map_err(|source| ApiError::Encode {
            method: method.to_string(),
            path: url.path().to_string(),
            source,
        })
    }

    // ------------------------------------------------------------
    // Sessions
    // ------------------------------------------------------------

    /// `POST /api/session` — create a session in `req.location.directory`.
    pub async fn create_session(&self, req: &SessionCreateRequest) -> Result<SessionInfo, ApiError> {
        let url = self.endpoint_url("/session");
        let body = self.encode(&Method::POST, &url, req)?;
        Ok(self.send_json::<SessionInfo>(Method::POST, url, Some(body)).await?.data)
    }

    /// `GET /api/session?directory=&cursor=` — list sessions, newest first.
    ///
    /// `cursor` is the `{previous,next}` object from a prior page envelope;
    /// `next` is sent when both are present (tokens are self-describing).
    /// Returns the full envelope so the caller can continue paging.
    pub async fn list_sessions(
        &self,
        directory: Option<&str>,
        cursor: Option<&Cursor>,
    ) -> Result<Envelope<Vec<SessionInfo>>, ApiError> {
        let mut url = self.endpoint_url("/session");
        {
            let mut q = url.query_pairs_mut();
            if let Some(dir) = directory {
                q.append_pair("directory", dir);
            }
            if let Some(token) = cursor_token(cursor) {
                q.append_pair("cursor", &token);
            }
        }
        self.send_json::<Vec<SessionInfo>>(Method::GET, url, None).await
    }

    /// `GET /api/session/{id}`.
    pub async fn get_session(&self, session_id: &str) -> Result<SessionInfo, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}", encode_segment(session_id)));
        Ok(self.send_json::<SessionInfo>(Method::GET, url, None).await?.data)
    }

    /// `PATCH /api/session/{id}` — updates title (204, no body returned).
    ///
    /// OpenAPI also allows `metadata` / `permissions` on this endpoint; add
    /// fields to [`SessionPatchRequest`] when the mapping lane needs them.
    pub async fn patch_session(&self, session_id: &str, title: Option<&str>) -> Result<(), ApiError> {
        let url = self.endpoint_url(&format!("/session/{}", encode_segment(session_id)));
        let body = self.encode(&Method::PATCH, &url, &SessionPatchRequest {
            title: title.map(str::to_string),
        })?;
        self.send_unit(Method::PATCH, url, Some(body)).await
    }

    /// `DELETE /api/session/{id}` (204).
    pub async fn delete_session(&self, session_id: &str) -> Result<(), ApiError> {
        let url = self.endpoint_url(&format!("/session/{}", encode_segment(session_id)));
        self.send_unit(Method::DELETE, url, None).await
    }

    // ------------------------------------------------------------
    // Turns
    // ------------------------------------------------------------

    /// `POST /api/session/{id}/prompt` — enqueue a user message.
    ///
    /// Returns **immediately** with the inbox user message; the turn plays out
    /// on the event stream (`sse::event_stream`).
    pub async fn prompt(&self, session_id: &str, req: &PromptRequest) -> Result<InboxUserMessage, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/prompt", encode_segment(session_id)));
        let body = self.encode(&Method::POST, &url, req)?;
        Ok(self.send_json::<InboxUserMessage>(Method::POST, url, Some(body)).await?.data)
    }

    /// `POST /api/session/{id}/command` — run a registered agent command by
    /// name (mirrors prompt; response is the enqueued inbox user message).
    pub async fn command(&self, session_id: &str, req: &CommandRequest) -> Result<InboxUserMessage, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/command", encode_segment(session_id)));
        let body = self.encode(&Method::POST, &url, req)?;
        Ok(self.send_json::<InboxUserMessage>(Method::POST, url, Some(body)).await?.data)
    }

    /// `POST /api/session/{id}/compact` — request compaction of the session.
    ///
    /// Response is a compaction inbox record (parses through the loose
    /// [`InboxUserMessage`] shape; the server rejects a missing object body).
    pub async fn compact(&self, session_id: &str) -> Result<InboxUserMessage, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/compact", encode_segment(session_id)));
        self.send_json::<InboxUserMessage>(Method::POST, url, Some("{}".to_string()))
            .await
            .map(|env| env.data)
    }

    /// `POST /api/session/{id}/interrupt` — cancel an in-flight turn.
    ///
    /// Unlike the rest, this answers a **bare** `{"interrupted": bool}`.
    pub async fn interrupt(&self, session_id: &str) -> Result<InterruptResponse, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/interrupt", encode_segment(session_id)));
        let resp = self.execute(Method::POST, url, None).await?;
        let path = resp.url().path().to_string();
        let bytes = resp.bytes().await.map_err(|source| ApiError::Transport {
            method: "POST".to_string(),
            path: path.clone(),
            source,
        })?;
        serde_json::from_slice(&bytes).map_err(|source| ApiError::Decode {
            method: "POST".to_string(),
            path,
            source,
        })
    }

    /// `POST /api/session/{id}/fork` — fork the session, optionally at a
    /// boundary message.
    pub async fn fork(&self, session_id: &str, before: Option<&str>) -> Result<SessionInfo, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/fork", encode_segment(session_id)));
        let body = self.encode(&Method::POST, &url, &ForkRequest {
            before: before.map(str::to_string),
        })?;
        Ok(self.send_json::<SessionInfo>(Method::POST, url, Some(body)).await?.data)
    }

    /// `POST /api/session/{id}/model` — switch the session model (204).
    pub async fn set_model(&self, session_id: &str, model: &ModelRef) -> Result<(), ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/model", encode_segment(session_id)));
        let body = self.encode(&Method::POST, &url, &serde_json::json!({ "model": model }))?;
        self.send_unit(Method::POST, url, Some(body)).await
    }

    // ------------------------------------------------------------
    // Messages
    // ------------------------------------------------------------

    /// `GET /api/session/{id}/message?cursor=` — persisted message records,
    /// newest first. Returns the full envelope; the caller continues paging
    /// with the returned `cursor` (`{previous,next}` object).
    pub async fn messages(&self, session_id: &str, cursor: Option<&Cursor>) -> Result<MessagesEnvelope, ApiError> {
        let mut url = self.endpoint_url(&format!("/session/{}/message", encode_segment(session_id)));
        {
            let mut q = url.query_pairs_mut();
            if let Some(token) = cursor_token(cursor) {
                q.append_pair("cursor", &token);
            }
        }
        self.send_json::<Vec<MessageRecord>>(Method::GET, url, None).await
    }

    /// `GET /api/session/{id}/message/{messageID}` — one record.
    pub async fn message_detail(&self, session_id: &str, message_id: &str) -> Result<MessageRecord, ApiError> {
        let url = self.endpoint_url(&format!(
            "/session/{}/message/{}",
            encode_segment(session_id),
            encode_segment(message_id)
        ));
        Ok(self.send_json::<MessageRecord>(Method::GET, url, None).await?.data)
    }

    /// `DELETE /api/session/{id}/message/{messageID}` (204).
    pub async fn delete_message(&self, session_id: &str, message_id: &str) -> Result<(), ApiError> {
        let url = self.endpoint_url(&format!(
            "/session/{}/message/{}",
            encode_segment(session_id),
            encode_segment(message_id)
        ));
        self.send_unit(Method::DELETE, url, None).await
    }

    // ------------------------------------------------------------
    // Permissions
    // ------------------------------------------------------------

    /// `GET /api/session/{id}/permission` — pending permission requests.
    ///
    /// Untyped until the permission event type is verified live (see
    /// docs/opencode-api.md "Unverified event names").
    pub async fn list_permissions(&self, session_id: &str) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/permission", encode_segment(session_id)));
        Ok(self.send_json::<Vec<Value>>(Method::GET, url, None).await?.data)
    }

    /// `POST /api/session/{id}/permission/{requestID}/reply`.
    pub async fn permission_reply(
        &self,
        session_id: &str,
        request_id: &str,
        req: &PermissionReplyRequest,
    ) -> Result<(), ApiError> {
        let url = self.endpoint_url(&format!(
            "/session/{}/permission/{}/reply",
            encode_segment(session_id),
            encode_segment(request_id)
        ));
        let body = self.encode(&Method::POST, &url, req)?;
        self.send_unit(Method::POST, url, Some(body)).await
    }

    // ------------------------------------------------------------
    // Catalog & config
    // ------------------------------------------------------------

    /// `GET /api/agent` → `{location, data: [...]}` (untyped for now).
    pub async fn agents(&self) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url("/agent");
        Ok(self.send_json::<Vec<Value>>(Method::GET, url, None).await?.data)
    }

    /// `GET /api/command` → `{location, data: [...]}`.
    pub async fn commands(&self) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url("/command");
        Ok(self.send_json::<Vec<Value>>(Method::GET, url, None).await?.data)
    }

    /// `GET /api/skill` → `{location, data: [...]}`.
    pub async fn skills(&self) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url("/skill");
        Ok(self.send_json::<Vec<Value>>(Method::GET, url, None).await?.data)
    }

    /// `GET /api/model` → `{data: [{id, modelID, providerID, name}…]}`.
    pub async fn models(&self) -> Result<Vec<ModelInfo>, ApiError> {
        let url = self.endpoint_url("/model");
        Ok(self.send_json::<Vec<ModelInfo>>(Method::GET, url, None).await?.data)
    }

    /// `GET /api/provider` → `{data: [{id, name, activation, package}…]}`
    /// (settings deliberately not modeled — may hold credentials).
    pub async fn providers(&self) -> Result<Vec<ProviderInfo>, ApiError> {
        let url = self.endpoint_url("/provider");
        Ok(self.send_json::<Vec<ProviderInfo>>(Method::GET, url, None).await?.data)
    }

    /// `GET /api/config` — configuration documents. **Bare array**, not
    /// enveloped (verified live).
    pub async fn config(&self) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url("/config");
        let resp = self.execute(Method::GET, url, None).await?;
        let path = resp.url().path().to_string();
        let bytes = resp.bytes().await.map_err(|source| ApiError::Transport {
            method: "GET".to_string(),
            path: path.clone(),
            source,
        })?;
        serde_json::from_slice(&bytes).map_err(|source| ApiError::Decode {
            method: "GET".to_string(),
            path,
            source,
        })
    }

    /// `GET /api/session/{id}/diff` — `{data: [...]}`.
    pub async fn diff(&self, session_id: &str) -> Result<Vec<Value>, ApiError> {
        let url = self.endpoint_url(&format!("/session/{}/diff", encode_segment(session_id)));
        Ok(self.send_json::<Vec<Value>>(Method::GET, url, None).await?.data)
    }
}

/// Pick the pagination token to send: `next` wins, else `previous`.
fn cursor_token(cursor: Option<&Cursor>) -> Option<String> {
    cursor
        .and_then(|c| c.next.clone().or_else(|| c.previous.clone()))
}

/// Percent-encode one URL path segment (RFC 3986 unreserved chars only).
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ============================================================
// Lane-local request/response types
// ============================================================

/// `POST /api/session/{id}/command` body (openapi: name + text required).
#[derive(Debug, Clone, Serialize)]
pub struct CommandRequest {
    pub name: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
}

/// `PATCH /api/session/{id}` body (subset the bridge needs today).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SessionPatchRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// `POST /api/session/{id}/fork` body (openapi: `before` only).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ForkRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
}

/// `POST /api/session/{id}/interrupt` response — **bare**, not enveloped
/// (verified live: `{"interrupted":true}`).
#[derive(Debug, Clone, Deserialize)]
pub struct InterruptResponse {
    pub interrupted: bool,
}

// ============================================================
// Unit tests (hermetic: no network)
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::AUTHORIZATION;
    use serde_json::json;

    fn client() -> OpencodeClient {
        OpencodeClient::new("http://127.0.0.1:44041", "test123").expect("valid base url")
    }

    #[test]
    fn rejects_malformed_base_url() {
        let err = OpencodeClient::new("not a url", "x").unwrap_err();
        assert!(matches!(err, ApiError::BadBase { .. }), "got {err:?}");
    }

    #[test]
    fn url_construction_appends_api_prefix() {
        let c = client();
        assert_eq!(
            c.endpoint_url("/session/ses_abc/prompt").as_str(),
            "http://127.0.0.1:44041/api/session/ses_abc/prompt"
        );
        // trailing-slash base must not double the separator
        let c2 = OpencodeClient::new("http://127.0.0.1:44041/", "x").expect("valid");
        assert_eq!(c2.endpoint_url("/model").as_str(), "http://127.0.0.1:44041/api/model");
        // existing base path is preserved
        let c3 = OpencodeClient::new("http://127.0.0.1:44041/proxy", "x").expect("valid");
        assert_eq!(c3.endpoint_url("/session").as_str(), "http://127.0.0.1:44041/proxy/api/session");
    }

    #[test]
    fn basic_auth_header_is_fixed_user_plus_password() {
        let c = client();
        let req = c.request(Method::GET, c.endpoint_url("/session")).build().expect("build");
        let auth = req.headers().get(AUTHORIZATION).expect("auth header").to_str().unwrap();
        // base64("opencode:test123")
        assert_eq!(auth, "Basic b3BlbmNvZGU6dGVzdDEyMw==");
    }

    #[test]
    fn request_timeout_and_content_type() {
        let c = client();
        let url = c.endpoint_url("/session");
        let req = c
            .request(Method::POST, url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .build()
            .expect("build");
        assert_eq!(req.timeout(), Some(&REQUEST_TIMEOUT));
    }

    #[test]
    fn path_segments_are_percent_encoded() {
        assert_eq!(encode_segment("ses_abc123"), "ses_abc123");
        assert_eq!(encode_segment("a b/c?d"), "a%20b%2Fc%3Fd");
        assert_eq!(encode_segment("msg_x~1"), "msg_x~1");
    }

    #[test]
    fn cursor_token_prefers_next() {
        let both = Cursor { previous: Some("prev".into()), next: Some("next".into()) };
        assert_eq!(cursor_token(Some(&both)).as_deref(), Some("next"));
        let only_prev = Cursor { previous: Some("prev".into()), next: None };
        assert_eq!(cursor_token(Some(&only_prev)).as_deref(), Some("prev"));
        assert_eq!(cursor_token(None), None);
    }

    // ------------------------------------------------------------
    // Fixture deserialization (real captured responses)
    // ------------------------------------------------------------

    #[test]
    fn deserializes_session_create_fixture() {
        let raw = include_str!("../../tests/fixtures/session-create.json");
        let env: Envelope<SessionInfo> = serde_json::from_str(raw).expect("fixture parses");
        assert!(env.data.id.starts_with("ses_"));
        assert_eq!(
            env.data.location.as_ref().expect("location").directory,
            "/tmp/opencode/acp-fixture-project"
        );
    }

    #[test]
    fn deserializes_models_fixture() {
        let raw = include_str!("../../tests/fixtures/models.json");
        let env: Envelope<Vec<ModelInfo>> = serde_json::from_str(raw).expect("fixture parses");
        assert!(!env.data.is_empty());
        for m in &env.data {
            assert!(!m.id.is_empty());
            assert!(!m.providerID.is_empty());
        }
        // spec says a working model with credentials exists on the scratch server
        assert!(
            env.data.iter().any(|m| m.id == "GLM-5.3-astra"),
            "expected GLM-5.3-astra in models fixture"
        );
    }

    #[test]
    fn deserializes_providers_fixture() {
        let raw = include_str!("../../tests/fixtures/providers.json");
        let env: Envelope<Vec<ProviderInfo>> = serde_json::from_str(raw).expect("fixture parses");
        assert!(!env.data.is_empty());
        assert!(env.data.iter().any(|p| p.id == "astra"));
    }

    #[test]
    fn deserializes_agents_fixture() {
        let raw = include_str!("../../tests/fixtures/agents.json");
        let env: Envelope<Vec<Value>> = serde_json::from_str(raw).expect("fixture parses");
        assert!(!env.data.is_empty());
        assert_eq!(
            env.data[0].as_object().expect("agent object")["id"],
            json!("orchestrator")
        );
    }

    #[test]
    fn deserializes_messages_fixture() {
        let raw = include_str!("../../tests/fixtures/messages-tool-turn.json");
        let env: MessagesEnvelope = serde_json::from_str(raw).expect("fixture parses");
        assert!(!env.data.is_empty());
        // newest first: the first record is the idle outcome of the turn
        assert_eq!(env.data[0].kind, "idle");
        // cursor present (fixture carries base64 tokens for paging)
        let cursor = env.cursor.expect("fixture has a cursor");
        assert!(cursor.previous.as_deref().is_some_and(|t| !t.is_empty()));
        assert!(cursor.next.as_deref().is_some_and(|t| !t.is_empty()));
    }

    #[test]
    fn deserializes_empty_list_shape() {
        // A bare `{data: []}` (as /api/session/{id}/permission answers) must
        // parse with the optional location/cursor fields absent.
        let env: Envelope<Vec<Value>> = serde_json::from_str(r#"{"data":[]}"#).expect("parses");
        assert!(env.data.is_empty());
    }

    // ------------------------------------------------------------
    // Integration test (opt-in): BRIDGE_IT=1 cargo test -- --ignored
    // ------------------------------------------------------------

    /// Full lane-A flow against the scratch 2.0.21 server:
    /// create → set model → prompt → stream until `execution.succeeded`
    /// (120 s cap) → verify the write tool landed a filediff → cleanup.
    ///
    /// Requires env `BRIDGE_IT=1` and the scratch server on 127.0.0.1:47779
    /// (auth opencode:test123). Test artifacts live under /tmp/opencode/.
    #[tokio::test]
    #[ignore = "requires BRIDGE_IT=1 and a scratch 2.0.21 server on 127.0.0.1:47779"]
    async fn integration_lane_a_flow() {
        if std::env::var("BRIDGE_IT").as_deref() != Ok("1") {
            eprintln!("skipped: BRIDGE_IT=1 not set");
            return;
        }
        use crate::dto::{Location, SessionEvent};
        use crate::opencode::sse::event_stream;
        use futures_util::StreamExt;

        let dir = "/tmp/opencode/it-laneA";
        std::fs::create_dir_all(dir).expect("mkdir it-laneA");
        // Remove leftovers from a previous partial run (this test's own files).
        for p in [format!("{dir}/it-test.txt"), "/tmp/opencode/it-test.txt".to_string()] {
            if std::path::Path::new(&p).exists() {
                std::fs::remove_file(&p).expect("remove leftover it-test.txt");
            }
        }

        let client = OpencodeClient::new("http://127.0.0.1:47779", "test123").expect("client");
        let info = client
            .create_session(&SessionCreateRequest {
                id: None,
                title: None,
                agent: None,
                model: None,
                location: Location { directory: dir.to_string() },
                metadata: None,
            })
            .await
            .expect("create_session");
        let session_id = info.id.clone();

        client
            .set_model(
                &session_id,
                &ModelRef { id: "GLM-5.3-astra".to_string(), providerID: "astra".to_string(), variant: None },
            )
            .await
            .expect("set_model");

        client
            .prompt(
                &session_id,
                &PromptRequest {
                    text: "Use the write tool to create a file named it-test.txt with content: laneA ok. Then reply done."
                        .to_string(),
                    files: None,
                    agents: None,
                    skills: None,
                    metadata: None,
                },
            )
            .await
            .expect("prompt");

        // Stream until this session's terminal event, 120 s cap.
        let mut stream = event_stream(&client);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut succeeded = false;
        let mut failed: Option<String> = None;
        let mut tool_successes: Vec<crate::dto::FileDiff> = Vec::new();

        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some(ev)) => match &ev {
                    SessionEvent::ExecutionSucceeded(s) if s.sessionID == session_id => {
                        succeeded = true;
                        break;
                    }
                    SessionEvent::ExecutionFailed(f) if f.session.sessionID == session_id => {
                        failed = f.error.message.clone();
                        break;
                    }
                    SessionEvent::ToolSuccess(t) if t.base.sessionID == session_id => {
                        if let Some(fd) = t.metadata.as_ref().and_then(|m| m.filediff.clone()) {
                            tool_successes.push(fd);
                        }
                    }
                    _ => {}
                },
                Ok(None) => break,
                Err(_) => break, // deadline elapsed
            }
        }

        // Cleanup the session regardless of outcome.
        let deleted = client.delete_session(&session_id).await;

        assert!(succeeded, "execution.succeeded within 120s (failed={failed:?})");
        let filediff = tool_successes.first().expect("saw session.tool.success with filediff");
        // The write tool resolves relative paths against the project root, so
        // accept either the session dir or the project root.
        assert!(
            filediff.file == format!("{dir}/it-test.txt")
                || filediff.file == "/tmp/opencode/it-test.txt",
            "written file path {} in expected location",
            filediff.file
        );
        let content = std::fs::read_to_string(&filediff.file).expect("written file readable");
        assert!(content.contains("laneA ok"), "content = {content:?}");
        // Clean up the artifact we created (and any root-level variant), then
        // the session.
        for p in [format!("{dir}/it-test.txt"), "/tmp/opencode/it-test.txt".to_string()] {
            if std::path::Path::new(&p).exists() {
                std::fs::remove_file(&p).expect("remove it-test.txt");
            }
        }
        deleted.expect("delete_session cleanup");
    }
}
