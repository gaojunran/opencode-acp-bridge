//! opencode HTTP/SSE client lane (Wave 1, Lane A).
//!
//! Owns: `src/opencode/**` — `api.rs` (REST client) and `sse.rs` (event stream).
//! Contract: `crate::dto` + `docs/opencode-api.md`. Wire target: opencode 2.0.21
//! (`/api/*` mount, Basic auth, `GET /api/event` SSE).

pub mod api;
pub mod sse;
