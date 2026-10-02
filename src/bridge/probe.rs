//! Startup probe: one lightweight `GET /api/config` before serving, so
//! misconfiguration fails fast with a *classified* message — a stale
//! service.json (server moved/restarted) reads differently from a dead
//! network or rejected credentials.

use thiserror::Error;

use crate::opencode::api::{ApiError, OpencodeClient};

/// Classified probe failure (user-facing Display).
#[derive(Debug, Error)]
pub enum ProbeError {
    /// TCP/DNS-level failure: nothing answered on the socket.
    #[error("cannot reach the opencode server at {url}: {detail}")]
    Unreachable { url: String, detail: String },
    /// The server answered but rejected the credentials.
    #[error("the opencode server at {url} rejected the credentials (HTTP {status}): {detail}")]
    Auth { url: String, status: u16, detail: String },
    /// The server answered with another HTTP error.
    #[error("the opencode server at {url} answered HTTP {status}: {detail}")]
    Http { url: String, status: u16, detail: String },
    /// Anything else (bad URL, decode failure).
    #[error("connection probe failed at {url}: {detail}")]
    Other { url: String, detail: String },
}

/// Classify an [`ApiError`] from the probe call. Pure, so the
/// stale-service-file vs dead-network distinction is unit-testable.
pub fn classify_probe_error(url: &str, err: &ApiError) -> ProbeError {
    match err {
        ApiError::Transport { .. } => ProbeError::Unreachable {
            url: url.into(),
            detail: err.to_string(),
        },
        ApiError::Http { status, .. } if *status == 401 || *status == 403 => ProbeError::Auth {
            url: url.into(),
            status: *status,
            detail: err.to_string(),
        },
        ApiError::Http { status, .. } => ProbeError::Http {
            url: url.into(),
            status: *status,
            detail: err.to_string(),
        },
        _ => ProbeError::Other { url: url.into(), detail: err.to_string() },
    }
}

/// Probe the server once. `url` is the user-facing base URL (for messages).
pub async fn probe_server(client: &OpencodeClient, url: &str) -> Result<(), ProbeError> {
    client
        .config()
        .await
        .map(|_| ())
        .map_err(|e| classify_probe_error(url, &e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn transport_error_is_unreachable() {
        // A real transport error: connection refused on a closed local port.
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let err: ApiError = rt.block_on(async {
            let e = reqwest::Client::new()
                .get("http://127.0.0.1:1/")
                .send()
                .await
                .expect_err("nothing listens on port 1");
            ApiError::Transport {
                method: "GET".into(),
                path: "/api/config".into(),
                source: e,
            }
        });
        let e = classify_probe_error("http://127.0.0.1:1", &err);
        match e {
            ProbeError::Unreachable { url, .. } => assert_eq!(url, "http://127.0.0.1:1"),
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }

    #[test]
    fn http_401_is_auth() {
        let err = ApiError::Http {
            status: 401,
            method: "GET".into(),
            path: "/api/config".into(),
            body: json!({"_tag": "UnauthorizedError"}).to_string(),
        };
        match classify_probe_error("http://h:1", &err) {
            ProbeError::Auth { status, .. } => assert_eq!(status, 401),
            other => panic!("expected Auth, got {other:?}"),
        }
    }

    #[test]
    fn http_500_is_generic_http() {
        let err = ApiError::Http {
            status: 500,
            method: "GET".into(),
            path: "/api/config".into(),
            body: "boom".into(),
        };
        match classify_probe_error("http://h:1", &err) {
            ProbeError::Http { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Http, got {other:?}"),
        }
    }

    #[test]
    fn decode_failure_is_other() {
        let err = ApiError::Decode {
            method: "GET".into(),
            path: "/api/config".into(),
            source: serde_json::from_str::<u32>("nope").expect_err("invalid json"),
        };
        match classify_probe_error("http://h:1", &err) {
            ProbeError::Other { url, .. } => assert_eq!(url, "http://h:1"),
            other => panic!("expected Other, got {other:?}"),
        }
    }
}