//! opencode-acp-bridge — ACP agent that bridges Zed (and any ACP client) to a
//! shared opencode server over its HTTP API.
//!
//! Module layout (see docs/):
//! - `dto`       — opencode 2.0.21 wire contract (shared, governance: docs/opencode-api.md)
//! - `opencode`  — HTTP/SSE client lane (owns src/opencode/**)
//! - `acp`       — ACP agent + event mapping lane (owns src/acp/**)

mod acp;
mod dto;
mod opencode;

fn main() {
    // Wired up in the integration phase (Wave 2): reads connection config from
    // env (OPENCODE_URL / OPENCODE_PASSWORD), builds the ACP Agent on stdio.
    println!("opencode-acp-bridge scaffold");
}
