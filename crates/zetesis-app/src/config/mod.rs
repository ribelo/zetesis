//! Configuration loading and XDG path helpers.

use std::path::PathBuf;

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
