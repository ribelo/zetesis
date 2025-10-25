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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub path: PathBuf,
}

pub fn load() -> Result<AppConfig, AppConfigError> {
    let default_storage = default_storage_path()?;
    let builder = Config::builder()
        .set_default("server.listen_addr", "127.0.0.1:8080")?
        .set_default(
            "storage.path",
            default_storage.to_string_lossy().to_string(),
        )?
        .add_source(File::with_name(CONFIG_FILE).required(false))
        .add_source(Environment::with_prefix("ZETESIS").separator("__"));

    let cfg = builder.build()?.try_deserialize()?;
    Ok(cfg)
}

pub fn project_dirs() -> Result<ProjectDirs, AppConfigError> {
    ProjectDirs::from("dev", "ribelo", "zetesis").ok_or(AppConfigError::MissingProjectDirs)
}

fn default_storage_path() -> Result<PathBuf, AppConfigError> {
    Ok(project_dirs()?.data_dir().join("lmdb"))
}
