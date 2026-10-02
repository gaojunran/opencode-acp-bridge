//! opencode-acp-bridge — ACP agent that bridges Zed (and any ACP client) to a
//! shared opencode server over its HTTP API.
//!
//! Binary entry point (Wave 2 wiring): parse args → resolve the connection →
//! fail-fast probe → serve the ACP agent on stdio. **stdout is the JSON-RPC
//! channel — all logging goes to stderr.**

use std::process::ExitCode;
use std::sync::Arc;

use agent_client_protocol::Stdio;
use opencode_acp_bridge::acp::agent::{AgentService, OpenCodeBackend};
use opencode_acp_bridge::bridge::args::{ConnectMode, ParseOutcome, USAGE, parse_args};
use opencode_acp_bridge::bridge::backend::HttpBackend;
use opencode_acp_bridge::bridge::config::{
    SERVICE_FILE_STALE_HINT, resolve_config,
};
use opencode_acp_bridge::bridge::probe::{ProbeError, probe_server};
use opencode_acp_bridge::opencode::api::{ApiError, OpencodeClient};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    match parse_args(std::env::args()) {
        ParseOutcome::Help => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        ParseOutcome::Version => {
            println!("opencode-acp-bridge {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ParseOutcome::Error(msg) => {
            eprintln!("error: {msg}");
            ExitCode::from(2)
        }
        ParseOutcome::Run(opts) => run(opts.mode, opts.no_aft).await,
    }
}

async fn run(mode: ConnectMode, no_aft: bool) -> ExitCode {
    init_tracing();

    let cfg = match resolve_config(&mode, &opencode_acp_bridge::bridge::config::RealEnv) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            if matches!(mode, ConnectMode::ServiceFile) {
                eprintln!("{SERVICE_FILE_STALE_HINT}");
            }
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(base_url = %cfg.base_url, source = %cfg.source, "connecting to opencode server");

    let client = match OpencodeClient::new(&cfg.base_url, &cfg.password) {
        Ok(client) => client,
        Err(ApiError::BadBase { given, detail }) => {
            eprintln!("error: invalid server URL '{given}': {detail}");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Fail fast — and tell the user WHY: a stale service registration is a
    // different problem from a dead server or wrong password. The stale-file
    // hint applies only when the connection actually came from the service
    // file (cfg.source names the real origin), not from env or an explicit URL.
    if let Err(e) = probe_server(&client, &cfg.base_url).await {
        eprintln!("error: {e}");
        match &e {
            ProbeError::Unreachable { .. } if cfg.from_service_file() => {
                eprintln!("{SERVICE_FILE_STALE_HINT}");
            }
            ProbeError::Unreachable { .. } => {
                eprintln!("is the server running and reachable? Without --attach the default reads \
                           ~/.config/opencode/service.json; alternatively pass --attach <url> or \
                           set OPENCODE_URL.");
            }
            ProbeError::Auth { .. } if cfg.from_service_file() => {
                eprintln!("{SERVICE_FILE_STALE_HINT}");
            }
            ProbeError::Auth { .. } => {
                eprintln!("check OPENCODE_PASSWORD / OPENCODE_SERVER_PASSWORD.");
            }
            _ => {}
        }
        return ExitCode::FAILURE;
    }
    tracing::info!("probe ok — serving ACP on stdio");

    let backend: Arc<dyn OpenCodeBackend> = Arc::new(HttpBackend::new(client));
    let service = Arc::new(AgentService::new(backend).with_no_aft(no_aft));

    // Runs until the stdio connection closes (stdin EOF → clean exit).
    match service.serve(Stdio::new()).await {
        Ok(()) => {
            tracing::info!("stdio connection closed, exiting");
            ExitCode::SUCCESS
        }
        Err(e) => {
            tracing::error!(error = %e, "ACP server exited with an error");
            ExitCode::FAILURE
        }
    }
}

/// Logs go to **stderr** only — stdout is the JSON-RPC channel and must stay
/// byte-clean. Level from `RUST_LOG`, default `info`.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).init();
}