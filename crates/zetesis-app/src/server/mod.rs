//! Web server entrypoints will live here.
//! Axum routes and middleware will be introduced once we flesh out the HTTP surface.

use thiserror::Error;

use crate::config::AppConfig;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("server bootstrap is not implemented yet")]
    NotImplemented,
}

pub async fn serve(_config: AppConfig) -> Result<(), ServerError> {
    tracing::info!("Server bootstrap is not implemented yet.");
    Ok(())
}
