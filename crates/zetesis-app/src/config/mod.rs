//! Configuration loading and XDG path helpers.

use std::{
    net::IpAddr,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
};

use config::{Config, Environment, File};
use directories::ProjectDirs;
use serde::Deserialize;
use thiserror::Error;

const CONFIG_FILE: &str = "config/settings";

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
pub struct ServerConfig {
    pub listen_addr: String,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default = "RateLimitConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "RateLimitConfig::default_window_ms")]
    pub window_ms: NonZeroU64,
    #[serde(default = "RateLimitConfig::default_keyword_limit")]
    pub keyword: RouteLimitConfig,
    #[serde(default = "RateLimitConfig::default_vector_limit")]
    pub vector: RouteLimitConfig,
    #[serde(default)]
    pub proxy_mode: ProxyMode,
    #[serde(default)]
    pub trusted_proxies: Vec<IpAddr>,
}

impl RateLimitConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_window_ms() -> NonZeroU64 {
        NonZeroU64::new(1_000).expect("non-zero window defaults to 1000ms")
    }

    fn default_keyword_limit() -> RouteLimitConfig {
        RouteLimitConfig::keyword_defaults()
    }

    fn default_vector_limit() -> RouteLimitConfig {
        RouteLimitConfig::vector_defaults()
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            window_ms: Self::default_window_ms(),
            keyword: Self::default_keyword_limit(),
            vector: Self::default_vector_limit(),
            proxy_mode: ProxyMode::Off,
            trusted_proxies: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteLimitConfig {
    #[serde(default = "RouteLimitConfig::default_requests")]
    pub max_requests: NonZeroU32,
    #[serde(default = "RouteLimitConfig::default_burst")]
    pub burst: NonZeroU32,
}

impl RouteLimitConfig {
    fn default_requests() -> NonZeroU32 {
        NonZeroU32::new(1).expect("default requests bound must be non-zero")
    }

    fn default_burst() -> NonZeroU32 {
        NonZeroU32::new(1).expect("default burst bound must be non-zero")
    }

    fn keyword_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(10).expect("keyword default must be non-zero"),
            burst: NonZeroU32::new(20).expect("keyword burst must be non-zero"),
        }
    }

    fn vector_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(3).expect("vector default must be non-zero"),
            burst: NonZeroU32::new(6).expect("vector burst must be non-zero"),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    Off,
    XForwardedFor,
    Forwarded,
}

impl Default for ProxyMode {
    fn default() -> Self {
        ProxyMode::Off
    }
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
    let default_storage = default_storage_path()?;
    let builder = Config::builder()
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
        .set_default("ingest.max_files", 10000)?
        .add_source(File::with_name(CONFIG_FILE).required(false))
        .add_source(Environment::with_prefix("ZETESIS").separator("__"));

    let cfg = builder.build()?.try_deserialize()?;
    Ok(cfg)
}

pub fn project_dirs() -> Result<ProjectDirs, AppConfigError> {
    ProjectDirs::from("dev", "ribelo", "zetesis").ok_or(AppConfigError::MissingProjectDirs)
}

fn default_storage_path() -> Result<PathBuf, AppConfigError> {
    Ok(project_dirs()?.data_dir().to_path_buf())
}
