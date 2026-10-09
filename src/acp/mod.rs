//! ACP agent & mapping lane (Wave 1, Lane B).
//!
//! Owns: `src/acp/**` — agent wiring, opencode-event → ACP SessionUpdate
//! mapping, the #52636 diff fix, and session/load history replay.
//! Contract: `crate::dto` + `docs/opencode-api.md` + `docs/acp-notes.md`.
//! ACP dialect: v1 + unstable extensions (agent-client-protocol =2.0.0,
//! schema 1.5.0 — matching the user's Zed checkout).

pub mod agent;
pub mod diff;
pub mod form;
pub mod replay;
pub mod updates;
