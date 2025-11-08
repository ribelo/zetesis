//! Configuration loading and XDG path helpers.

use std::{env, path::PathBuf};

use axum::http::Method;
use config::{Config, Environment, File, FileFormat};
use directories::ProjectDirs;
use serde::Deserialize;
use thiserror::Error;
use url::Url;
pub use zetesis_server::config::{
    CorsConfig, ProxyMode, RateLimitConfig, RouteLimitConfig, ServerConfig,
};
use zetesis_server::config::{parse_header, parse_method};

const LOCAL_CONFIG_PATH: &str = "config/settings.toml";
const ETC_CONFIG_ENV: &str = "ZETESIS_ETC_CONFIG_DIR";
const CONFIG_OVERRIDE_ENV: &str = "ZETESIS_CONFIG_FILE";
const ETC_CONFIG_DEFAULT: &str = "/etc/xdg";
const CORS_MAX_LIST_SIZE: usize = 64;
const CORS_MAX_ENTRY_LEN: usize = 128;
const CORS_MAX_AGE_LIMIT: u64 = 86_400;

#[derive(Debug, Error)]
pub enum AppConfigError {
    #[error("unable to resolve project directories")]
    MissingProjectDirs,
    #[error(transparent)]
    Build(#[from] config::ConfigError),
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub ingest: IngestConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    /// Backend type: "fs" (default) or "s3"
    #[serde(default = "default_backend")]
    pub backend: String,
    /// Path for filesystem storage (used when backend = "fs")
    pub path: PathBuf,
    /// S3 configuration (used when backend = "s3")
    #[serde(default)]
    pub s3: Option<S3Config>,
}

fn default_backend() -> String {
    "fs".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// Optional custom endpoint URL (e.g., for Hetzner: https://nbg1.your-objectstorage.com)
    pub endpoint_url: Option<String>,
    /// Optional AWS region (defaults to behavior chain if not specified)
    pub region: Option<String>,
    /// Force path-style addressing (default: true for compatibility with non-AWS S3)
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
    /// Optional root prefix for all keys (e.g., "zetesis/")
    pub root_prefix: Option<String>,
}

fn default_force_path_style() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct IngestConfig {
    /// Maximum files to process in a single ingest run.
    pub max_files: usize,
}

pub fn load() -> Result<AppConfig, AppConfigError> {
    let dirs = project_dirs()?;
    let default_storage = dirs.data_dir().to_path_buf();
    let mut builder = Config::builder()
        .set_default("server.listen_addr", "127.0.0.1:8080")?
        .set_default("server.rate_limit.enabled", true)?
        .set_default("server.rate_limit.window_ms", 1_000)?
        .set_default("server.rate_limit.keyword.max_requests", 10)?
        .set_default("server.rate_limit.keyword.burst", 20)?
        .set_default("server.rate_limit.vector.max_requests", 3)?
        .set_default("server.rate_limit.vector.burst", 6)?
        .set_default("server.rate_limit.proxy_mode", "off")?
        .set_default("server.rate_limit.trusted_proxies", Vec::<String>::new())?
        .set_default("storage.backend", "fs")?
        .set_default(
            "storage.path",
            default_storage.to_string_lossy().to_string(),
        )?
        .set_default("ingest.max_files", 10000)?;

    for path in base_config_paths(&dirs) {
        debug_assert!(!path.as_os_str().is_empty());
        builder = builder.add_source(File::from(path).format(FileFormat::Toml).required(false));
    }

    if let Some(override_path) = config_override_path()? {
        debug_assert!(!override_path.as_os_str().is_empty());
        builder = builder.add_source(
            File::from(override_path)
                .format(FileFormat::Toml)
                .required(true),
        );
    }

    builder = builder.add_source(Environment::with_prefix("ZETESIS").separator("__"));

    let cfg: AppConfig = builder.build()?.try_deserialize()?;
    validate_config(&cfg)?;
    Ok(cfg)
}

pub fn project_dirs() -> Result<ProjectDirs, AppConfigError> {
    ProjectDirs::from("dev", "ribelo", "zetesis").ok_or(AppConfigError::MissingProjectDirs)
}

fn base_config_paths(dirs: &ProjectDirs) -> [PathBuf; 3] {
    let etc_root = etc_config_dir();
    debug_assert!(
        dirs.config_dir()
            .join("settings.toml")
            .starts_with(dirs.config_dir())
    );
    [
        etc_root.join("zetesis").join("settings.toml"),
        dirs.config_dir().join("settings.toml"),
        PathBuf::from(LOCAL_CONFIG_PATH),
    ]
}

fn config_override_path() -> Result<Option<PathBuf>, AppConfigError> {
    match env::var_os(CONFIG_OVERRIDE_ENV) {
        None => Ok(None),
        Some(raw) => {
            let path = PathBuf::from(raw);
            if path.as_os_str().is_empty() {
                return Err(invalid_config("ZETESIS_CONFIG_FILE must not be empty"));
            }
            Ok(Some(path))
        }
    }
}

fn etc_config_dir() -> PathBuf {
    match env::var_os(ETC_CONFIG_ENV) {
        Some(raw) => {
            let path = PathBuf::from(&raw);
            if path.as_os_str().is_empty() {
                PathBuf::from(ETC_CONFIG_DEFAULT)
            } else {
                path
            }
        }
        None => PathBuf::from(ETC_CONFIG_DEFAULT),
    }
}

fn validate_config(config: &AppConfig) -> Result<(), AppConfigError> {
    validate_cors(&config.server.cors)?;
    Ok(())
}

fn validate_cors(cors: &CorsConfig) -> Result<(), AppConfigError> {
    debug_assert!(CORS_MAX_LIST_SIZE >= 1);

    ensure_list_bounds("allow_origins", &cors.allow_origins)?;
    ensure_list_bounds("allow_methods", &cors.allow_methods)?;
    ensure_list_bounds("allow_headers", &cors.allow_headers)?;
    ensure_list_bounds("expose_headers", &cors.expose_headers)?;

    if cors.max_age_secs > CORS_MAX_AGE_LIMIT {
        return Err(invalid_config(
            "CORS max_age_secs exceeds 86400 second ceiling",
        ));
    }

    if cors.enabled && cors.allow_origins.is_empty() {
        return Err(invalid_config("CORS enabled but allow_origins is empty"));
    }

    if cors.enabled && cors.allow_methods.is_empty() {
        return Err(invalid_config("CORS enabled but allow_methods is empty"));
    }

    for origin in &cors.allow_origins {
        validate_origin(origin)?;
    }

    let mut has_options = false;
    for method in &cors.allow_methods {
        let parsed = parse_method(method).map_err(invalid_config)?;
        if parsed == Method::OPTIONS {
            has_options = true;
        }
    }

    if cors.enabled && !has_options {
        return Err(invalid_config(
            "CORS allow_methods must include OPTIONS when enabled",
        ));
    }

    for header in &cors.allow_headers {
        parse_header(header).map_err(invalid_config)?;
    }

    for header in &cors.expose_headers {
        parse_header(header).map_err(invalid_config)?;
    }

    Ok(())
}

fn ensure_list_bounds(name: &str, values: &[String]) -> Result<(), AppConfigError> {
    debug_assert!(!name.is_empty());
    if values.len() > CORS_MAX_LIST_SIZE {
        return Err(invalid_config(format!(
            "CORS {name} supports at most {CORS_MAX_LIST_SIZE} entries"
        )));
    }

    for value in values {
        debug_assert!(value.len() <= usize::MAX);
        if value.is_empty() {
            return Err(invalid_config(format!(
                "CORS {name} entries must not be empty"
            )));
        }
        if value.len() > CORS_MAX_ENTRY_LEN {
            return Err(invalid_config(format!(
                "CORS {name} entry `{value}` exceeds {CORS_MAX_ENTRY_LEN} characters"
            )));
        }
        if value.contains('\n') {
            return Err(invalid_config(format!(
                "CORS {name} entry `{value}` must not contain newlines"
            )));
        }
    }

    Ok(())
}

fn validate_origin(raw: &str) -> Result<(), AppConfigError> {
    debug_assert!(!raw.contains('\r'));
    let url =
        Url::parse(raw).map_err(|_| invalid_config(format!("invalid CORS origin `{raw}`")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(invalid_config(format!(
                "CORS origin `{raw}` must use http or https (found {other})"
            )));
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid_config(format!(
            "CORS origin `{raw}` must not include userinfo"
        )));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(invalid_config(format!(
            "CORS origin `{raw}` must not include path, query, or fragment"
        )));
    }
    if url.host_str().is_none() {
        return Err(invalid_config(format!(
            "CORS origin `{raw}` must include a host"
        )));
    }
    Ok(())
}

fn invalid_config<S: Into<String>>(message: S) -> AppConfigError {
    AppConfigError::Build(config::ConfigError::Message(message.into()))
}
