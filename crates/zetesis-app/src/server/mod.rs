//! Web server entrypoints live here.

use std::{future::Future, net::SocketAddr, time::Duration};

use axum::{Json, Router, response::IntoResponse, routing::get};
use serde::Serialize;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};

use crate::config::AppConfig;

const HEALTHZ_PATH: &str = "/v1/healthz";
const HEALTHZ_STATUS: &str = "ok";
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Copy, Clone, PartialEq, Eq)]
struct HealthzResponse {
    status: &'static str,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ShutdownEvent {
    Pending,
    CtrlC,
    SigTerm,
    ListenerFailed,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("listen address may not be empty")]
    EmptyListenAddr,
    #[error("invalid listen address `{address}`: {source}")]
    InvalidListenAddr {
        address: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("failed to bind to {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to determine local address: {source}")]
    LocalAddr {
        #[source]
        source: std::io::Error,
    },
    #[error("axum server error: {source}")]
    Serve {
        #[source]
        source: std::io::Error,
    },
}

pub fn build_api_router() -> Router {
    debug_assert!(HEALTHZ_PATH.starts_with("/v1/"));
    debug_assert!(HEALTHZ_PATH.ends_with("healthz"));

    Router::new().route(HEALTHZ_PATH, get(healthz))
}

pub async fn serve(config: AppConfig) -> Result<(), ServerError> {
    debug_assert!(config.server.listen_addr.len() <= 128);
    debug_assert!(!config.server.listen_addr.contains('\n'));

    let listen_addr = parse_listen_addr(&config.server.listen_addr)?;

    let listener = bind_listener(listen_addr).await?;

    let local_addr = listener
        .local_addr()
        .map_err(|source| ServerError::LocalAddr { source })?;
    tracing::info!(%local_addr, "zetesis server listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownEvent::Pending);

    let shutdown_future = broadcast_shutdown(shutdown_tx);

    let app = build_app_router();

    let mut server_future = Box::pin(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_future)
            .await
    });
    debug_assert!(DRAIN_TIMEOUT.as_secs() == 10);

    let drain_rx = shutdown_rx.clone();
    let mut drain_timeout = Box::pin(drain_timeout_future(drain_rx));

    tokio::select! {
        result = server_future.as_mut() => {
            if let Err(source) = result {
                return Err(ServerError::Serve { source });
            }
        }
        _ = drain_timeout.as_mut() => {
            // Timeout elapsed; dropping the server future forces termination.
        }
    }

    let final_event = *shutdown_rx.borrow();
    if final_event == ShutdownEvent::Pending {
        tracing::info!("server stopped without external shutdown signal");
    } else {
        tracing::info!(?final_event, "server shutdown complete");
    }

    Ok(())
}

fn build_app_router() -> Router {
    debug_assert!(HEALTHZ_PATH.starts_with('/'));
    debug_assert_eq!(HEALTHZ_STATUS, "ok");
    Router::new().merge(build_api_router())
    // Future `/ui/*` routes will be nested alongside the API router.
}

async fn healthz() -> impl IntoResponse {
    debug_assert_eq!(HEALTHZ_STATUS, "ok");
    debug_assert!(HEALTHZ_STATUS.chars().all(|c| c.is_ascii_lowercase()));

    Json(HealthzResponse {
        status: HEALTHZ_STATUS,
    })
}

async fn wait_for_shutdown() -> ShutdownEvent {
    debug_assert!(DRAIN_TIMEOUT >= Duration::from_secs(1));
    debug_assert!(HEALTHZ_PATH.starts_with('/'));

    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => ShutdownEvent::CtrlC,
            Err(error) => {
                tracing::warn!(%error, "failed to capture Ctrl+C signal");
                ShutdownEvent::ListenerFailed
            }
        }
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut term) => match term.recv().await {
                Some(_) => ShutdownEvent::SigTerm,
                None => ShutdownEvent::ListenerFailed,
            },
            Err(error) => {
                tracing::warn!(%error, "failed to capture SIGTERM");
                ShutdownEvent::ListenerFailed
            }
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending();

    tokio::select! {
        event = ctrl_c => event,
        event = sigterm => event,
    }
}

fn parse_listen_addr(addr: &str) -> Result<SocketAddr, ServerError> {
    debug_assert!(addr.len() <= 128);
    debug_assert!(!addr.contains('\n'));

    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return Err(ServerError::EmptyListenAddr);
    }

    trimmed
        .parse()
        .map_err(|source| ServerError::InvalidListenAddr {
            address: trimmed.to_string(),
            source,
        })
}

async fn bind_listener(addr: SocketAddr) -> Result<TcpListener, ServerError> {
    debug_assert!(addr.port() > 0);
    debug_assert!(addr.ip().is_ipv4() || addr.ip().is_ipv6());

    TcpListener::bind(addr)
        .await
        .map_err(|source| ServerError::Bind {
            address: addr.to_string(),
            source,
        })
}

fn broadcast_shutdown(
    sender: watch::Sender<ShutdownEvent>,
) -> impl Future<Output = ()> + Send + 'static {
    debug_assert!(!sender.is_closed());
    debug_assert!(DRAIN_TIMEOUT.as_secs() <= 10);
    async move {
        let event = wait_for_shutdown().await;
        debug_assert!(event != ShutdownEvent::Pending);
        if let Err(error) = sender.send(event) {
            tracing::warn!(?event, %error, "failed to broadcast shutdown event");
        }
    }
}

fn drain_timeout_future(
    mut receiver: watch::Receiver<ShutdownEvent>,
) -> impl Future<Output = ()> + Send + 'static {
    debug_assert!(DRAIN_TIMEOUT.as_secs() >= 1);
    debug_assert!(DRAIN_TIMEOUT.as_secs() <= 60);
    async move {
        if receiver.changed().await.is_ok() {
            let event = *receiver.borrow_and_update();
            debug_assert!(event != ShutdownEvent::Pending);
            tracing::info!(?event, "shutdown signal received; draining connections");
            tokio::time::sleep(DRAIN_TIMEOUT).await;
            tracing::warn!(
                ?event,
                seconds = DRAIN_TIMEOUT.as_secs(),
                "graceful shutdown timed out; continuing shutdown"
            );
        }
    }
}
