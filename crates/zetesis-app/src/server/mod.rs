//! Web server entrypoints live here.

use std::{future::Future, net::SocketAddr, time::Duration};

use axum::{Json, Router, extract::Query, http::StatusCode, response::IntoResponse, routing::get};
use serde::{Deserialize, Serialize, de::Deserializer};
use serde_json::Value;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};

use crate::config::AppConfig;
use crate::error::AppError;
use crate::services::{KeywordSearchParams, VectorSearchParams, keyword, vector};

const HEALTHZ_PATH: &str = "/v1/healthz";
const KEYWORD_PATH: &str = "/v1/search/keyword";
const VECTOR_PATH: &str = "/v1/search/vector";
const HEALTHZ_STATUS: &str = "ok";
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FIELD_COUNT: usize = 64;
const MAX_SORT_COUNT: usize = 8;
const MAX_FIELD_LEN: usize = 128;
const ERROR_INVALID_PARAMETER: &str = "invalid_parameter";
const ERROR_NOT_FOUND: &str = "not_found";
const ERROR_INTERNAL: &str = "internal_server_error";

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

#[derive(Debug, Deserialize)]
struct KeywordQuery {
    index: String,
    #[serde(rename = "q")]
    query: String,
    filter: Option<String>,
    limit: usize,
    offset: usize,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    sort: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct VectorQuery {
    index: String,
    #[serde(rename = "q")]
    query: String,
    embedder: Option<String>,
    filter: Option<String>,
    #[serde(rename = "k")]
    top_k: usize,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    fields: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<String>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ApiErrorBody,
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

impl KeywordQuery {
    fn into_params(self) -> Result<KeywordSearchParams, ApiError> {
        debug_assert!(self.limit <= usize::MAX - 1);
        let KeywordQuery {
            index,
            query,
            filter,
            limit,
            offset,
            sort,
            fields,
        } = self;
        validate_limit(limit)?;
        let index = trim_non_empty("index", index)?;
        let query = trim_non_empty("q", query)?;
        let sort = expand_csv(sort);
        validate_list("sort", &sort, MAX_SORT_COUNT)?;
        let fields = expand_csv(fields);
        validate_list("fields", &fields, MAX_FIELD_COUNT)?;
        let filter = sanitize_optional(filter);
        Ok(KeywordSearchParams {
            index,
            query,
            filter,
            sort,
            fields,
            limit,
            offset,
        })
    }
}

impl VectorQuery {
    fn into_params(self) -> Result<VectorSearchParams, ApiError> {
        debug_assert!(self.top_k <= usize::MAX - 1);
        let VectorQuery {
            index,
            query,
            embedder,
            filter,
            top_k,
            fields,
        } = self;
        validate_top_k(top_k)?;
        let index = trim_non_empty("index", index)?;
        let query = trim_non_empty("q", query)?;
        let embedder = sanitize_optional(embedder);
        let fields = expand_csv(fields);
        validate_list("fields", &fields, MAX_FIELD_COUNT)?;
        let filter = sanitize_optional(filter);
        Ok(VectorSearchParams {
            index,
            query,
            embedder,
            filter,
            fields,
            top_k,
        })
    }
}

impl ApiError {
    fn invalid_param(field: &str, message: impl Into<String>) -> Self {
        debug_assert!(!field.is_empty());
        ApiError {
            status: StatusCode::BAD_REQUEST,
            body: ApiErrorBody {
                error: ERROR_INVALID_PARAMETER,
                message: message.into(),
                field: Some(field.to_string()),
            },
        }
    }

    fn not_found(field: &str, message: impl Into<String>) -> Self {
        debug_assert!(!field.is_empty());
        ApiError {
            status: StatusCode::NOT_FOUND,
            body: ApiErrorBody {
                error: ERROR_NOT_FOUND,
                message: message.into(),
                field: Some(field.to_string()),
            },
        }
    }

    fn internal() -> Self {
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: ApiErrorBody {
                error: ERROR_INTERNAL,
                message: "internal server error".to_string(),
                field: None,
            },
        }
    }
}

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        match error {
            AppError::MissingIndex { index, .. } => {
                ApiError::not_found("index", format!("index `{index}` not found"))
            }
            AppError::InvalidIndexName { name } => {
                ApiError::invalid_param("index", format!("invalid index `{name}`"))
            }
            AppError::InvalidSort { spec, reason } => {
                ApiError::invalid_param("sort", format!("invalid sort `{spec}`: {reason}"))
            }
            AppError::InvalidFilter { expression, reason } => ApiError::invalid_param(
                "filter",
                format!("invalid filter `{expression}`: {reason}"),
            ),
            AppError::EmbedderNotFound { name, .. } => ApiError::invalid_param(
                "embedder",
                format!("embedder `{name}` not configured for index"),
            ),
            AppError::UnsupportedEmbedder { name, .. } => ApiError::invalid_param(
                "embedder",
                format!("embedder `{name}` does not support vector search"),
            ),
            other => {
                tracing::error!(error = %other, "search request failed");
                ApiError::internal()
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.body)).into_response()
    }
}

fn trim_non_empty(field: &str, value: String) -> Result<String, ApiError> {
    debug_assert!(!field.is_empty());
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_param(field, "must not be empty"));
    }
    Ok(trimmed.to_string())
}

fn sanitize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(value) => Ok(vec![value]),
        OneOrMany::Many(values) => Ok(values),
    }
}

fn expand_csv(values: Vec<String>) -> Vec<String> {
    debug_assert!(values.len() < 512);
    let mut out = Vec::new();
    for value in values {
        for part in value.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(trimmed.to_string());
        }
    }
    out
}

fn validate_list(field: &str, values: &[String], max_len: usize) -> Result<(), ApiError> {
    debug_assert!(!field.is_empty());
    debug_assert!(max_len > 0);
    if values.len() > max_len {
        return Err(ApiError::invalid_param(
            field,
            format!("must include at most {max_len} entries"),
        ));
    }
    for value in values {
        if value.len() > MAX_FIELD_LEN {
            return Err(ApiError::invalid_param(
                field,
                format!("entries must be at most {MAX_FIELD_LEN} characters"),
            ));
        }
    }
    Ok(())
}

fn ensure_range(field: &str, value: usize, min: usize, max: usize) -> Result<(), ApiError> {
    debug_assert!(!field.is_empty());
    debug_assert!(min > 0);
    debug_assert!(max >= min);
    if value < min || value > max {
        return Err(ApiError::invalid_param(
            field,
            format!("must be between {min} and {max}"),
        ));
    }
    Ok(())
}

fn validate_limit(limit: usize) -> Result<(), ApiError> {
    ensure_range("limit", limit, 1, 100)
}

fn validate_top_k(k: usize) -> Result<(), ApiError> {
    ensure_range("k", k, 1, 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_query_allows_empty_fields() {
        let query = KeywordQuery {
            index: "kio".to_string(),
            query: "foo".to_string(),
            filter: None,
            limit: 10,
            offset: 0,
            sort: Vec::new(),
            fields: Vec::new(),
        };
        let params = query.into_params().expect("fields should be optional");
        assert!(params.fields.is_empty(), "fields should remain empty");
    }

    #[test]
    fn vector_query_defaults_embedder() {
        let query = VectorQuery {
            index: "kio".to_string(),
            query: "foo".to_string(),
            embedder: None,
            filter: None,
            top_k: 10,
            fields: vec!["id".to_string()],
        };
        let params = query
            .into_params()
            .expect("embedder should default to configured value");
        assert!(params.embedder.is_none(), "embedder should remain None");
    }

    #[test]
    fn expand_csv_splits_commas() {
        let values = expand_csv(vec!["a,b".to_string(), "c".to_string()]);
        assert_eq!(values, vec!["a", "b", "c"]);
    }

    #[test]
    fn validate_list_limits_length() {
        let entries: Vec<String> = (0..(MAX_FIELD_COUNT + 1))
            .map(|idx| format!("field{idx}"))
            .collect();
        let error = validate_list("fields", &entries, MAX_FIELD_COUNT)
            .expect_err("too many fields must error");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }
}

pub fn build_api_router() -> Router {
    debug_assert!(HEALTHZ_PATH.starts_with("/v1/"));
    debug_assert!(HEALTHZ_PATH.ends_with("healthz"));

    Router::new()
        .route(HEALTHZ_PATH, get(healthz))
        .route(KEYWORD_PATH, get(keyword_search))
        .route(VECTOR_PATH, get(vector_search))
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

async fn keyword_search(Query(query): Query<KeywordQuery>) -> Result<Json<Vec<Value>>, ApiError> {
    debug_assert!(!KEYWORD_PATH.is_empty());
    let params = query.into_params()?;
    let rows = keyword(&params).map_err(ApiError::from)?;
    Ok(Json(rows))
}

async fn vector_search(Query(query): Query<VectorQuery>) -> Result<Json<Vec<Value>>, ApiError> {
    debug_assert!(!VECTOR_PATH.is_empty());
    let params = query.into_params()?;
    let rows = vector(&params).await.map_err(ApiError::from)?;
    Ok(Json(rows))
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
