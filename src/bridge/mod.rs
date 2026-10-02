//! Wave 2 (Lane C) wiring: `main.rs`-adjacent pieces that make the ACP server
//! executable — argument parsing ([`args`]), connection config resolution
//! ([`config`]), the startup probe ([`probe`]) and the HTTP backend
//! ([`backend`]) implementing `acp::agent::OpenCodeBackend` over the
//! `opencode::api::OpencodeClient`.

pub mod args;
pub mod backend;
pub mod config;
pub mod probe;