//! Connection config resolution: turn a [`ConnectMode`] into
//! [`ConnectionConfig`] (base URL + password + human-readable source).
//!
//! The environment is accessed through the [`EnvLike`] facade so every error
//! path is unit-testable without mutating the process environment.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

use super::args::{CONNECTION_EXAMPLES, ConnectMode};

/// Password env vars, in preference order (all connection modes).
pub const PASSWORD_ENV_VARS: [&str; 2] = ["OPENCODE_PASSWORD", "OPENCODE_SERVER_PASSWORD"];
/// URL env var for the env fallback (used when no service file is present).
pub const URL_ENV_VAR: &str = "OPENCODE_URL";
/// service.json location relative to $HOME.
pub const SERVICE_FILE_REL: [&str; 3] = [".config", "opencode", "service.json"];

/// Hint printed alongside service-file errors: a running server keeps its old
/// registration on disk, so the file can be stale.
pub const SERVICE_FILE_STALE_HINT: &str =
    "note: a running opencode server keeps its OLD port/password registration on disk; \
     if the file looks stale, restart the server (or pass --attach <url> explicitly)";

/// Fully resolved connection parameters.
#[derive(Debug)]
pub struct ConnectionConfig {
    /// Server root, e.g. `http://127.0.0.1:44041` (never with `/api`).
    pub base_url: String,
    /// Basic-auth password (the server's `OPENCODE_PASSWORD`).
    pub password: String,
    /// Human-readable origin, for logs and error messages. One of:
    /// - the service file path (connection came from `service.json`),
    /// - `OPENCODE_URL=<url>` (env fallback),
    /// - `--attach <url>` (explicit URL).
    pub source: String,
}

impl ConnectionConfig {
    /// Whether the connection was read from `~/.config/opencode/service.json`
    /// (as opposed to `--attach <url>` or the `OPENCODE_URL` env fallback).
    ///
    /// The discriminator is [`ConnectionConfig::source`]'s format: the file
    /// path is the only form that is neither label-prefixed.
    pub fn from_service_file(&self) -> bool {
        !self.source.starts_with("OPENCODE_URL=") && !self.source.starts_with("--attach ")
    }
}

/// Environment facade: the real process env for production, a map for tests.
pub trait EnvLike {
    fn get(&self, key: &str) -> Option<String>;
    fn home_dir(&self) -> Option<PathBuf>;
}

/// The real process environment.
pub struct RealEnv;

impl EnvLike for RealEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }

    fn home_dir(&self) -> Option<PathBuf> {
        std::env::var_os("HOME").map(PathBuf::from)
    }
}

/// Test facade: an explicit var map + home dir.
#[derive(Debug, Clone)]
pub struct MapEnv {
    pub vars: HashMap<String, String>,
    pub home: Option<PathBuf>,
}

impl MapEnv {
    pub fn new(vars: impl IntoIterator<Item = (String, String)>, home: Option<PathBuf>) -> Self {
        Self { vars: vars.into_iter().collect(), home }
    }
}

impl EnvLike for MapEnv {
    fn get(&self, key: &str) -> Option<String> {
        self.vars.get(key).cloned()
    }

    fn home_dir(&self) -> Option<PathBuf> {
        self.home.clone()
    }
}

/// Resolution failure; every message is user-facing.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(
        "no connection configuration found.\n\
         Pass one of:\n{examples}\n\
         (run with --help for the full usage)"
    )]
    NoConfig { examples: String },
    #[error("password missing for {mode}: set OPENCODE_PASSWORD or OPENCODE_SERVER_PASSWORD")]
    MissingPassword { mode: String },
    #[error("service file {path} not found or unreadable: {source}")]
    ServiceFileIo { path: String, source: io::Error },
    #[error("service file {path} is not valid JSON: {source}")]
    ServiceFileJson { path: String, source: serde_json::Error },
}

/// Resolve the connection for a mode.
///
/// - `--attach <url>`: URL verbatim, password from env (mandatory).
/// - `--attach <url>`: explicit URL; the password comes from the password env.
/// - default (no flags): try the service file; when it is *absent* (not found,
///   or HOME unset) fall back to `OPENCODE_URL` + password env. A present but
///   broken file is surfaced as-is — a corrupt registration must not be masked.
pub fn resolve_config(mode: &ConnectMode, env: &dyn EnvLike) -> Result<ConnectionConfig, ConfigError> {
    match mode {
        ConnectMode::ExplicitUrl(url) => {
            let password = find_password(env).ok_or_else(|| ConfigError::MissingPassword {
                mode: format!("--attach {url}"),
            })?;
            Ok(ConnectionConfig {
                base_url: url.clone(),
                password,
                source: format!("--attach {url}"),
            })
        }

        ConnectMode::Default => match resolve_service_file(env) {
            ServiceFileOutcome::Resolved(cfg) => Ok(cfg),
            // Absent (not found / HOME unset): the env fallback is the point
            // of the default mode.
            ServiceFileOutcome::NoHome | ServiceFileOutcome::Missing => resolve_from_env(env),
            // Present but broken: surface it — do not mask a corrupt registration.
            ServiceFileOutcome::Failed(e) => Err(e),
        },
    }
}

/// First set password env var wins.
fn find_password(env: &dyn EnvLike) -> Option<String> {
    PASSWORD_ENV_VARS.iter().find_map(|key| env.get(key))
}

/// `OPENCODE_URL` (+ password env) — the fallback for the default mode.
fn resolve_from_env(env: &dyn EnvLike) -> Result<ConnectionConfig, ConfigError> {
    let url = env.get(URL_ENV_VAR).ok_or_else(|| ConfigError::NoConfig {
        examples: CONNECTION_EXAMPLES.to_string(),
    })?;
    let password = find_password(env).ok_or_else(|| ConfigError::MissingPassword {
        mode: format!("OPENCODE_URL={url}"),
    })?;
    Ok(ConnectionConfig {
        base_url: url.clone(),
        password,
        source: format!("OPENCODE_URL={url}"),
    })
}

/// `~/.config/opencode/service.json` — the registration `opencode serve`
/// writes for its clients.
#[derive(Debug, Deserialize)]
struct ServiceFile {
    port: u16,
    password: String,
    #[serde(default)]
    hostname: Option<String>,
}

/// What the service-file lookup produced. The `Default` mode falls back to
/// env on `NoHome`/`Missing`.
#[derive(Debug)]
enum ServiceFileOutcome {
    /// Read and parsed; ready to use.
    Resolved(ConnectionConfig),
    /// HOME is unset, so the path cannot even be located.
    NoHome,
    /// The file does not exist (`io::ErrorKind::NotFound`).
    Missing,
    /// The file exists but is unreadable or corrupt — must be surfaced.
    Failed(ConfigError),
}

/// Read and parse `~/.config/opencode/service.json` under `$HOME`.
fn resolve_service_file(env: &dyn EnvLike) -> ServiceFileOutcome {
    let Some(home) = env.home_dir() else {
        return ServiceFileOutcome::NoHome;
    };
    let path = SERVICE_FILE_REL.iter().fold(home, |p, part| p.join(part));
    let path_s = path.display().to_string();

    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return ServiceFileOutcome::Missing;
        }
        Err(source) => {
            return ServiceFileOutcome::Failed(ConfigError::ServiceFileIo {
                path: path_s,
                source,
            });
        }
    };
    let file: ServiceFile = match serde_json::from_str(&text) {
        Ok(file) => file,
        Err(source) => {
            return ServiceFileOutcome::Failed(ConfigError::ServiceFileJson {
                path: path_s,
                source,
            });
        }
    };

    // `opencode serve --hostname 0.0.0.0` registers 0.0.0.0, which is not
    // connectable from a client — map it to loopback.
    let hostname = match file.hostname.as_deref() {
        Some("0.0.0.0") => "127.0.0.1".to_string(),
        Some(h) => h.to_string(),
        None => "127.0.0.1".to_string(),
    };
    ServiceFileOutcome::Resolved(ConnectionConfig {
        base_url: format!("http://{hostname}:{}", file.port),
        password: file.password,
        source: path_s,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// A fresh isolated home dir per test (cargo runs tests in parallel).
    fn temp_home(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("acp-bridge-cfg-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir temp home");
        dir
    }

    fn write_service_file(home: &Path, json: &str) -> PathBuf {
        let path = SERVICE_FILE_REL.iter().fold(home.to_path_buf(), |p, part| p.join(part));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir .config/opencode");
        }
        std::fs::write(&path, json).expect("write service.json");
        path
    }

    #[test]
    fn explicit_url_needs_a_password_env() {
        let env = MapEnv::new([], None);
        let err = resolve_config(&ConnectMode::ExplicitUrl("http://h:1".into()), &env)
            .expect_err("password must come from env");
        let msg = err.to_string();
        assert!(msg.contains("OPENCODE_PASSWORD") && msg.contains("OPENCODE_SERVER_PASSWORD"), "{msg}");
    }

    #[test]
    fn password_env_preference_and_fallback() {
        let mut vars = HashMap::new();
        vars.insert("OPENCODE_PASSWORD".into(), "primary".into());
        vars.insert("OPENCODE_SERVER_PASSWORD".into(), "secondary".into());
        let env = MapEnv::new(vars, None);
        let cfg = resolve_config(&ConnectMode::ExplicitUrl("http://h:1".into()), &env)
            .expect("resolves");
        assert_eq!(cfg.password, "primary", "OPENCODE_PASSWORD wins");

        let env2 = MapEnv::new(
            [("OPENCODE_SERVER_PASSWORD".to_string(), "secondary".to_string())],
            None,
        );
        let cfg2 = resolve_config(&ConnectMode::ExplicitUrl("http://h:1".into()), &env2)
            .expect("resolves");
        assert_eq!(cfg2.password, "secondary");
    }

    #[test]
    fn default_mode_falls_back_to_env_without_home() {
        // No HOME ⇒ the service file path cannot be located ⇒ env fallback.
        let env = MapEnv::new(
            [
                ("OPENCODE_URL".to_string(), "http://127.0.0.1:44041".to_string()),
                ("OPENCODE_PASSWORD".to_string(), "pw".to_string()),
            ],
            None,
        );
        let cfg = resolve_config(&ConnectMode::Default, &env).expect("resolves");
        assert_eq!(cfg.base_url, "http://127.0.0.1:44041");
        assert_eq!(cfg.password, "pw");
        assert!(cfg.source.contains("OPENCODE_URL"));
        assert!(!cfg.from_service_file(), "env fallback is not the service file");
    }

    #[test]
    fn default_mode_without_env_lists_all_options() {
        let env = MapEnv::new([], None);
        let err = resolve_config(&ConnectMode::Default, &env).expect_err("nothing configured");
        let msg = err.to_string();
        for needle in ["--attach http://127.0.0.1:44041", "service.json", "OPENCODE_URL"] {
            assert!(msg.contains(needle), "expected '{needle}' in:\n{msg}");
        }
    }

    #[test]
    fn default_mode_prefers_service_file_over_env() {
        // (a) File present wins, even with OPENCODE_URL also set.
        let home = temp_home("def-file");
        let path = write_service_file(&home, r#"{"port":47779,"password":"pw","hostname":"0.0.0.0"}"#);
        let env = MapEnv::new(
            [("OPENCODE_URL".to_string(), "http://127.0.0.1:44041".to_string())],
            Some(home.clone()),
        );
        let cfg = resolve_config(&ConnectMode::Default, &env).expect("resolves");
        assert_eq!(cfg.base_url, "http://127.0.0.1:47779", "file wins over env");
        assert_eq!(cfg.password, "pw");
        assert_eq!(cfg.source, path.display().to_string(), "source names the actual path");
        assert!(cfg.from_service_file());
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn default_mode_falls_back_to_env_when_file_absent() {
        // (b) Absent file + OPENCODE_URL ⇒ env fallback.
        let home = temp_home("def-absent");
        let env = MapEnv::new(
            [
                ("OPENCODE_URL".to_string(), "http://127.0.0.1:44041".to_string()),
                ("OPENCODE_PASSWORD".to_string(), "pw".to_string()),
            ],
            Some(home.clone()),
        );
        let cfg = resolve_config(&ConnectMode::Default, &env).expect("resolves");
        assert_eq!(cfg.base_url, "http://127.0.0.1:44041");
        assert_eq!(cfg.source, "OPENCODE_URL=http://127.0.0.1:44041");
        assert!(!cfg.from_service_file(), "env fallback is not the service file");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn default_mode_absent_file_and_no_env_shows_all_connection_options() {
        // (c) Absent file + no env ⇒ NoConfig listing every option.
        let home = temp_home("def-nocfg");
        let env = MapEnv::new([], Some(home.clone()));
        let err = resolve_config(&ConnectMode::Default, &env).expect_err("nothing configured");
        let msg = err.to_string();
        for needle in ["--attach http://127.0.0.1:44041", "service.json", "OPENCODE_URL"] {
            assert!(msg.contains(needle), "expected '{needle}' in:\n{msg}");
        }
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn default_mode_corrupt_file_is_not_masked() {
        // (d) Present-but-broken file surfaces, even with env fully set.
        let home = temp_home("def-bad");
        write_service_file(&home, "{not json");
        let env = MapEnv::new(
            [
                ("OPENCODE_URL".to_string(), "http://127.0.0.1:44041".to_string()),
                ("OPENCODE_PASSWORD".to_string(), "pw".to_string()),
            ],
            Some(home.clone()),
        );
        let err = resolve_config(&ConnectMode::Default, &env).expect_err("bad json must surface");
        let msg = err.to_string();
        assert!(msg.contains("service.json"), "{msg}");
        assert!(msg.contains("not valid JSON"), "{msg}");
        assert!(!msg.contains("OPENCODE_URL="), "no env fallback for a corrupt file:\n{msg}");

        // Wrong shape (valid JSON, missing port) is equally fatal.
        write_service_file(&home, r#"{"password":"pw"}"#);
        let err2 = resolve_config(&ConnectMode::Default, &env).expect_err("bad shape must surface");
        assert!(err2.to_string().contains("missing field `port`"), "{err2}");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn service_file_happy_path_and_hostname_mapping() {
        let home = temp_home("happy");
        let path = write_service_file(&home, r#"{"port":47779,"password":"pw","hostname":"0.0.0.0"}"#);
        let env = MapEnv::new([], Some(home.clone()));
        let cfg = resolve_config(&ConnectMode::Default, &env).expect("resolves");
        assert_eq!(cfg.base_url, "http://127.0.0.1:47779", "0.0.0.0 maps to loopback");
        assert_eq!(cfg.password, "pw");
        assert_eq!(cfg.source, path.display().to_string(), "source names the actual path");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn service_file_hostname_is_optional() {
        let home = temp_home("nohost");
        write_service_file(&home, r#"{"port":44041,"password":"pw"}"#);
        let env = MapEnv::new([], Some(home.clone()));
        let cfg = resolve_config(&ConnectMode::Default, &env).expect("resolves");
        assert_eq!(cfg.base_url, "http://127.0.0.1:44041");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn service_file_bad_json_reports_the_path() {
        let home = temp_home("badjson");
        write_service_file(&home, "{not json");
        let env = MapEnv::new([], Some(home.clone()));
        let err = resolve_config(&ConnectMode::Default, &env).expect_err("bad json");
        let msg = err.to_string();
        assert!(msg.contains("service.json"), "{msg}");
        assert!(msg.contains("not valid JSON"), "{msg}");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }

    #[test]
    fn service_file_bad_shape_reports_the_missing_field() {
        let home = temp_home("badshape");
        write_service_file(&home, r#"{"password":"pw"}"#);
        let env = MapEnv::new([], Some(home.clone()));
        let err = resolve_config(&ConnectMode::Default, &env).expect_err("no port");
        let msg = err.to_string();
        assert!(msg.contains("missing field `port`"), "{msg}");
        std::fs::remove_dir_all(&home).expect("cleanup");
    }
}