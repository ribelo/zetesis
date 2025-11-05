//! Web server entrypoints live here.

use std::{
    collections::HashSet,
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{connect_info::ConnectInfo, MatchedPath, Query, State},
    http::{header::RETRY_AFTER, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize, de::Deserializer};
use serde_json::Value;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};
use tower_http::{
    classify::ServerErrorsFailureClass,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use governor::{clock::DefaultClock, DefaultKeyedRateLimiter, Quota};
use std::num::NonZeroU32;
use governor::clock::Clock;

use crate::config::{AppConfig, RateLimitConfig, RouteLimitConfig, ServerConfig};
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
const ERROR_METHOD_NOT_ALLOWED: &str = "method_not_allowed";
const ERROR_RATE_LIMITED: &str = "rate_limited";
const ERROR_INTERNAL: &str = "internal_server_error";
const REQUEST_ID_HEADER: &str = "x-request-id";

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
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
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
    #[error("invalid rate limit configuration for {route}: {reason}")]
    RateLimitConfig {
        route: &'static str,
        reason: String,
    },
}

#[derive(Clone)]
struct RateLimitState {
    keyword: Arc<DefaultKeyedRateLimiter<String>>,
    vector: Arc<DefaultKeyedRateLimiter<String>>,
    window_ms: u64,
    proxy_mode: crate::config::ProxyMode,
    trusted: Arc<HashSet<IpAddr>>,
}

impl RateLimitState {
    fn try_new(cfg: &RateLimitConfig) -> Result<Arc<Self>, ServerError> {
        debug_assert!(cfg.window_ms.get() > 0);
        let window_ms = cfg.window_ms.get();
        let kw = build_keyed_limiter(&cfg.keyword, window_ms);
        let vec = build_keyed_limiter(&cfg.vector, window_ms);
        Ok(Arc::new(Self {
            keyword: Arc::new(kw),
            vector: Arc::new(vec),
            window_ms,
            proxy_mode: cfg.proxy_mode,
            trusted: Arc::new(cfg.trusted_proxies.iter().copied().collect()),
        }))
    }
}

fn build_keyed_limiter(route: &RouteLimitConfig, window_ms: u64) -> DefaultKeyedRateLimiter<String> {
    // Model "max_requests per window_ms" by setting replenish interval to window_ms / max_requests
    // and max burst to route.burst. This allows up to `burst` immediate requests, replenishing
    // `max_requests` cells per `window_ms` on average.
    let per = std::time::Duration::from_millis(window_ms);
    let n = route.max_requests.get();
    debug_assert!(n > 0);
    // Use deprecated constructor to set replenish_1_per = per / n precisely.
    #[allow(deprecated)]
    let mut quota = Quota::new(NonZeroU32::new(n).expect("nonzero"), per)
        .expect("window_ms must be > 0");
    quota = quota.allow_burst(NonZeroU32::new(route.burst.get()).expect("burst>0"));
    DefaultKeyedRateLimiter::<String>::keyed(quota)
}

async fn rate_limit_middleware(
    State(state): State<Arc<RateLimitState>>,
    req: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let path = matched_path_or_uri(&req);
    if path == HEALTHZ_PATH {
        return next.run(req).await;
    }

    let ip = extract_client_ip(&req, state.proxy_mode, state.trusted.as_ref())
        .unwrap_or_else(|| IpAddr::from([0, 0, 0, 0]));
    let bucket = if path == KEYWORD_PATH {
        "keyword"
    } else if path == VECTOR_PATH {
        "vector"
    } else {
        // For any unknown route, apply no limiter here; fallback will render 404.
        return next.run(req).await;
    };
    let key = format!("{bucket}:{ip}");

    let limiter = if bucket == "keyword" {
        &state.keyword
    } else {
        &state.vector
    };

    match limiter.check_key(&key) {
        Ok(()) => next.run(req).await,
        Err(negative) => {
            let now = DefaultClock::default().now();
            let wait = negative.wait_time_from(now);
            let mut response = ApiError::rate_limited(wait.as_millis() as u64)
                .with_request_id(header_request_id(req.headers()).as_deref())
                .into_response();
            // Set Retry-After in seconds, minimum 1 second.
            let secs = std::cmp::max(1u64, (wait.as_millis() as u64 + 999) / 1000);
            if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
                let headers = response.headers_mut();
                headers.insert(RETRY_AFTER, value);
            }
            response
        }
    }
}

fn extract_client_ip(
    req: &Request<Body>,
    mode: crate::config::ProxyMode,
    trusted: &HashSet<IpAddr>,
) -> Option<IpAddr> {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    let Some(peer_ip) = peer else { return None; };

    match mode {
        crate::config::ProxyMode::Off => Some(peer_ip),
        crate::config::ProxyMode::XForwardedFor => {
            // Only trust headers from known proxy addresses.
            if trusted.contains(&peer_ip) {
                parse_xff(req.headers()).or(Some(peer_ip))
            } else {
                Some(peer_ip)
            }
        }
        crate::config::ProxyMode::Forwarded => {
            if trusted.contains(&peer_ip) {
                parse_forwarded(req.headers()).or(Some(peer_ip))
            } else {
                Some(peer_ip)
            }
        }
    }
}

fn parse_xff(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(str::trim)
        .and_then(|ip| ip.parse::<IpAddr>().ok())
}

fn parse_forwarded(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("forwarded")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            // very simple parser: look for "for=VALUE"; VALUE may be quoted
            s.split(';')
                .flat_map(|part| part.split(','))
                .find_map(|kv| {
                    let kv = kv.trim();
                    if let Some(rest) = kv.strip_prefix("for=") {
                        let val = rest.trim_matches('"');
                        return val.parse::<IpAddr>().ok();
                    }
                    None
                })
        })
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
    fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            body: ApiErrorBody {
                error,
                message: message.into(),
                field: None,
                retry_after_ms: None,
                request_id: None,
            },
        }
    }

    fn with_field(mut self, field: &str) -> Self {
        debug_assert!(!field.is_empty());
        self.body.field = Some(field.to_string());
        self
    }

    fn with_request_id(mut self, request_id: Option<&str>) -> Self {
        if let Some(id) = request_id {
            debug_assert!(!id.is_empty());
            self.body.request_id = Some(id.to_string());
        }
        self
    }

    fn with_retry_after(mut self, retry_after_ms: u64) -> Self {
        debug_assert!(retry_after_ms > 0);
        self.body.retry_after_ms = Some(retry_after_ms);
        self
    }

    fn invalid_param(field: &str, message: impl Into<String>) -> Self {
        debug_assert!(!field.is_empty());
        ApiError::new(StatusCode::BAD_REQUEST, ERROR_INVALID_PARAMETER, message)
            .with_field(field)
    }

    fn not_found(field: &str, message: impl Into<String>) -> Self {
        debug_assert!(!field.is_empty());
        ApiError::new(StatusCode::NOT_FOUND, ERROR_NOT_FOUND, message).with_field(field)
    }

    fn internal() -> Self {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ERROR_INTERNAL,
            "internal server error",
        )
    }

    fn resource_not_found(path: &str) -> Self {
        debug_assert!(path.starts_with('/'));
        ApiError::new(
            StatusCode::NOT_FOUND,
            ERROR_NOT_FOUND,
            format!("resource `{path}` not found"),
        )
    }

    fn method_not_allowed(method: &str, path: &str) -> Self {
        debug_assert!(!method.is_empty());
        debug_assert!(path.starts_with('/'));
        ApiError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            ERROR_METHOD_NOT_ALLOWED,
            format!("method `{method}` not allowed for `{path}`"),
        )
    }

    fn rate_limited(retry_after_ms: u64) -> Self {
        debug_assert!(retry_after_ms > 0);
        ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            ERROR_RATE_LIMITED,
            "rate limit exceeded; retry after backoff",
        )
        .with_retry_after(retry_after_ms)
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
        .route(
            HEALTHZ_PATH,
            get(healthz).fallback(method_not_allowed_handler),
        )
        .route(
            KEYWORD_PATH,
            get(keyword_search).fallback(method_not_allowed_handler),
        )
        .route(
            VECTOR_PATH,
            get(vector_search).fallback(method_not_allowed_handler),
        )
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

    let app = build_app_router(&config.server)?;
    let make_service = app.into_make_service_with_connect_info::<SocketAddr>();

    let mut server_future = Box::pin(async move {
        axum::serve(listener, make_service)
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

fn build_app_router(config: &ServerConfig) -> Result<Router, ServerError> {
    debug_assert!(HEALTHZ_PATH.starts_with('/'));
    debug_assert_eq!(HEALTHZ_STATUS, "ok");

    let mut router = Router::new()
        .merge(build_api_router())
        .fallback(not_found_handler);

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            let path = matched_path_or_uri(request);
            let request_id = header_request_id(request.headers()).unwrap_or_else(|| "-".to_string());
            tracing::info_span!(
                "http.request",
                method = %request.method(),
                path = %path,
                request_id = %request_id
            )
        })
        .on_response(|response: &axum::response::Response, latency: Duration, span: &tracing::Span| {
            let status = response.status().as_u16();
            let latency_ms = latency.as_millis().min(u128::from(u64::MAX)) as u64;
            tracing::info!(parent: span, status, latency_ms, "request completed");
        })
        .on_failure(|error: ServerErrorsFailureClass, latency: Duration, span: &tracing::Span| {
            let latency_ms = latency.as_millis().min(u128::from(u64::MAX)) as u64;
            tracing::error!(parent: span, latency_ms, error = %error, "request failed");
        });

    router = router.layer(trace_layer);

    if config.rate_limit.enabled {
        let limiter_state = RateLimitState::try_new(&config.rate_limit)?;
        let rate_layer = middleware::from_fn_with_state(limiter_state, rate_limit_middleware);
        router = router.layer(rate_layer);
    }

    let request_id_header = HeaderName::from_static(REQUEST_ID_HEADER);
    let make_request_id = MakeRequestUuid::default();
    router = router
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, make_request_id));

    Ok(router)
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

async fn method_not_allowed_handler(request: Request<Body>) -> axum::response::Response {
    debug_assert!(request.uri().path().starts_with('/'));
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let request_id = header_request_id(request.headers());
    ApiError::method_not_allowed(&method, &path)
        .with_request_id(request_id.as_deref())
        .into_response()
}

async fn not_found_handler(request: Request<Body>) -> axum::response::Response {
    debug_assert!(request.uri().path().starts_with('/'));
    let path = request.uri().path().to_string();
    let request_id = header_request_id(request.headers());
    ApiError::resource_not_found(&path)
        .with_request_id(request_id.as_deref())
        .into_response()
}

fn matched_path_or_uri<B>(request: &Request<B>) -> String {
    if let Some(path) = request.extensions().get::<MatchedPath>() {
        let resolved = path.as_str();
        debug_assert!(resolved.starts_with('/'));
        return resolved.to_string();
    }
    let fallback = request.uri().path().to_string();
    debug_assert!(fallback.starts_with('/'));
    fallback
}

fn header_request_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
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
