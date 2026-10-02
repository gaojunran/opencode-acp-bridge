//! Command-line argument parsing — hand-rolled over `std::env::args()`
//! (deliberately no clap: the surface is three flags).
//!
//! Parsing is a pure function over the argument iterator, so every branch is
//! unit-testable. Connection *resolution* (env vars, service.json) lives in
//! [`super::config`]; this module only decides *how* to connect.

/// `--help` output.
pub const USAGE: &str = "\
opencode-acp-bridge — ACP agent bridging Zed (and any ACP client) to a shared
opencode server over its HTTP API.

USAGE:
    opencode-acp-bridge [--attach <url> | --attach] [--help] [--version]

OPTIONS:
    --attach <url>   Connect to the opencode server at <url>
                     (e.g. http://127.0.0.1:44041). The password is read from
                     the OPENCODE_PASSWORD or OPENCODE_SERVER_PASSWORD env.
    --attach         Read the connection from ~/.config/opencode/service.json
                     ({port, password, hostname} — written by `opencode serve`).
    --version        Print the version and exit.
    --help           Print this help and exit.

Without --attach, the OPENCODE_URL env var is used (password from the same
env vars as above). If nothing is configured, the program exits with
instructions for all three connection modes.";

/// The three connection modes, shown when nothing is configured.
pub const CONNECTION_EXAMPLES: &str = "\
  1) explicit URL + password env:
       OPENCODE_PASSWORD=<password> opencode-acp-bridge --attach http://127.0.0.1:44041
  2) service registration file (bare --attach):
       opencode-acp-bridge --attach
     — reads ~/.config/opencode/service.json ({\"port\", \"password\", \"hostname\"})
  3) environment variables:
       OPENCODE_URL=http://127.0.0.1:44041 OPENCODE_PASSWORD=<password> opencode-acp-bridge";

/// Which connection source to use (decided by the flags alone).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectMode {
    /// `--attach <url>`: the explicit URL; the password must come from env.
    ExplicitUrl(String),
    /// Bare `--attach`: read `~/.config/opencode/service.json`.
    ServiceFile,
    /// No `--attach`: `OPENCODE_URL` (+ password env).
    Env,
}

/// Result of parsing the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    /// Serve with this connection mode.
    Run(ConnectMode),
    /// `--version` was requested.
    Version,
    /// `--help` was requested.
    Help,
    /// Usage error; the message is already user-facing (includes a --help hint).
    Error(String),
}

/// Parse the argument list (argv[0] included, like `std::env::args()`).
///
/// Rules:
/// - `--help` / `--version` win immediately (first one seen).
/// - `--attach <url>`: the next token, when it does not start with `-`, is the
///   URL; otherwise (next flag or end of args) `--attach` is treated as bare.
/// - any other token is a usage error; a repeated `--attach` is a usage error.
pub fn parse_args<I>(args: I) -> ParseOutcome
where
    I: IntoIterator<Item = String>,
{
    let mut iter = args.into_iter();
    let _prog = iter.next(); // argv[0]: program name
    let rest: Vec<String> = iter.collect();

    // None = not seen; Some(Some(url)) = --attach with URL; Some(None) = bare.
    let mut attach: Option<Option<String>> = None;

    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--help" => return ParseOutcome::Help,
            "--version" => return ParseOutcome::Version,
            "--attach" => {
                if attach.is_some() {
                    return ParseOutcome::Error(
                        "duplicate --attach (run with --help for usage)".to_string(),
                    );
                }
                // A following token that does not start with `-` is the URL;
                // anything else (next flag, end of args) means the bare form.
                match rest.get(i + 1) {
                    Some(next) if !next.starts_with('-') => {
                        attach = Some(Some(next.clone()));
                        i += 1; // consume the URL token
                    }
                    _ => attach = Some(None),
                }
            }
            other => {
                return ParseOutcome::Error(format!(
                    "unexpected argument '{other}' (run with --help for usage)"
                ));
            }
        }
        i += 1;
    }

    let mode = match attach {
        Some(Some(url)) => ConnectMode::ExplicitUrl(url),
        Some(None) => ConnectMode::ServiceFile,
        None => ConnectMode::Env,
    };
    ParseOutcome::Run(mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> ParseOutcome {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn help_and_version_win_over_everything() {
        assert_eq!(parse(&["prog", "--help"]), ParseOutcome::Help);
        assert_eq!(parse(&["prog", "--version"]), ParseOutcome::Version);
        // Even when combined with --attach arguments.
        assert_eq!(parse(&["prog", "--attach", "http://x", "--help"]), ParseOutcome::Help);
        assert_eq!(parse(&["prog", "--attach", "--version"]), ParseOutcome::Version);
    }

    #[test]
    fn attach_with_url_is_explicit_mode() {
        assert_eq!(
            parse(&["prog", "--attach", "http://127.0.0.1:44041"]),
            ParseOutcome::Run(ConnectMode::ExplicitUrl("http://127.0.0.1:44041".into()))
        );
    }

    #[test]
    fn bare_attach_is_service_file_mode() {
        assert_eq!(parse(&["prog", "--attach"]), ParseOutcome::Run(ConnectMode::ServiceFile));
        // Followed by another flag: still the bare form.
        assert_eq!(
            parse(&["prog", "--attach", "--help"]),
            ParseOutcome::Help,
            "attach before an unrelated flag"
        );
    }

    #[test]
    fn no_attach_means_env_mode() {
        assert_eq!(parse(&["prog"]), ParseOutcome::Run(ConnectMode::Env));
    }

    #[test]
    fn unknown_token_is_a_usage_error() {
        match parse(&["prog", "--frobnicate"]) {
            ParseOutcome::Error(msg) => {
                assert!(msg.contains("--frobnicate"), "message names the token: {msg}");
                assert!(msg.contains("--help"), "message points at --help: {msg}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_attach_is_rejected() {
        match parse(&["prog", "--attach", "--attach", "http://x"]) {
            ParseOutcome::Error(msg) => assert!(msg.contains("duplicate"), "got {msg}"),
            other => panic!("expected Error, got {other:?}"),
        }
    }
}