//! opencode-acp-bridge — ACP agent that bridges Zed (and any ACP client) to a
//! shared opencode server over its HTTP API.
//!
//! Library surface so integration tests (`tests/`) and the thin binary
//! (`main.rs`) share one compilation unit per lane:
//! - `dto`       — opencode 2.0.21 wire contract (shared, governance: docs/opencode-api.md)
//! - `opencode`  — HTTP/SSE client lane (owns src/opencode/**)
//! - `acp`       — ACP agent + event mapping lane (owns src/acp/**)
//! - `bridge`    — Wave 2 wiring lane: args, connection config, probe, backend

pub mod acp;
pub mod bridge;
pub mod dto;
pub mod git_add;
pub mod opencode;
