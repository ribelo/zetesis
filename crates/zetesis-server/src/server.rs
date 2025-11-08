//! Web server entrypoints live here.

use std::{
    collections::HashSet,
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{MatchedPath, Query, State, connect_info::ConnectInfo},
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, header::RETRY_AFTER},
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
};
use governor::clock::Clock;
use governor::{DefaultKeyedRateLimiter, Quota, clock::DefaultClock};
use moka::future::Cache;
use serde::{Deserialize, Serialize, de::Deserializer};
use serde_json::Value;
use std::num::NonZeroU32;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};
use tower_http::{
    add_extension::AddExtensionLayer,
    classify::ServerErrorsFailureClass,
    cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer, ExposeHeaders},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::config::{CorsConfig, ProxyMode, RateLimitConfig, RouteLimitConfig, ServerConfig};
use crate::search::{
    HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WEIGHT, HYBRID_PER_SOURCE_LIMIT_MAX,
    HYBRID_RESULT_LIMIT_MAX, HybridFusion, HybridSearchParams, KeywordSearchParams, SearchError,
    SearchErrorKind, SearchProvider, VectorSearchParams, normalize_hybrid_weights,
};

const HEALTHZ_PATH: &str = "/v1/healthz";
const KEYWORD_PATH: &str = "/v1/search/keyword";
const VECTOR_PATH: &str = "/v1/search/vector";
const TYPEAHEAD_PATH: &str = "/v1/search/typeahead";
const HYBRID_PATH: &str = "/v1/search/hybrid";
const HEALTHZ_STATUS: &str = "ok";
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FIELD_COUNT: usize = 64;
const MAX_SORT_COUNT: usize = 8;
const MAX_FIELD_LEN: usize = 128;
const TYPEAHEAD_FIELD_WHITELIST: [&str; 5] =
    ["id", "doc_id", "decision_date", "summary_short", "section"];
const TYPEAHEAD_CACHE_CAPACITY: u64 = 10_000;
const TYPEAHEAD_CACHE_TTL: Duration = Duration::from_secs(2);
const TYPEAHEAD_LIMIT_DEFAULT: usize = 10;
const TYPEAHEAD_LIMIT_MAX: usize = 10;
const TYPEAHEAD_MIN_QUERY_LEN: usize = 2;
const ERROR_INVALID_PARAMETER: &str = "invalid_parameter";
const ERROR_NOT_FOUND: &str = "not_found";
const ERROR_METHOD_NOT_ALLOWED: &str = "method_not_allowed";
const ERROR_RATE_LIMITED: &str = "rate_limited";
const ERROR_INTERNAL: &str = "internal_server_error";
const REQUEST_ID_HEADER: &str = "x-request-id";
const CACHE_HEADER_NAME: &str = "x-cache";
const CACHE_HIT_VALUE: &str = "hit";
const CACHE_MISS_VALUE: &str = "miss";

static TYPEAHEAD_FIELDS_VEC: OnceLock<Vec<String>> = OnceLock::new();

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

#[derive(Debug, Deserialize)]
struct HybridQuery {
    index: String,
    #[serde(rename = "q")]
    query: String,
    filter: Option<String>,
    embedder: Option<String>,
    #[serde(default = "HybridQuery::default_limit")]
    limit: usize,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    fields: Vec<String>,
    #[serde(default)]
    fusion: HybridFusionQuery,
    #[serde(default)]
    keyword_weight: Option<f32>,
    #[serde(default)]
    vector_weight: Option<f32>,
}

#[derive(Debug, Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
enum HybridFusionQuery {
    Rrf,
    Weighted,
}

impl Default for HybridFusionQuery {
    fn default() -> Self {
        HybridFusionQuery::Rrf
    }
}

#[derive(Debug, Deserialize)]
struct TypeaheadQuery {
    index: String,
    #[serde(rename = "q")]
    query: String,
    filter: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct TypeaheadParams {
    index: String,
    query: String,
    normalized_query: String,
    filter: Option<String>,
    limit: usize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TypeaheadCacheKey {
    index: String,
    query: String,
    filter: Option<String>,
    limit: usize,
}

#[derive(Clone)]
struct TypeaheadState {
    cache: Cache<TypeaheadCacheKey, Arc<Vec<Value>>>,
}

type DynSearchProvider = Arc<dyn SearchProvider>;
type ApiStateHandle = Arc<ApiState>;

#[derive(Clone)]
struct ApiState {
    search: DynSearchProvider,
    typeahead: Arc<TypeaheadState>,
}

impl ApiState {
    fn new(search: DynSearchProvider) -> Self {
        Self {
            search,
            typeahead: Arc::new(TypeaheadState::new()),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TypeaheadCacheStatus {
    Hit,
    Miss,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone)]
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
    RateLimitConfig { route: &'static str, reason: String },
    #[error("invalid CORS configuration: {reason}")]
    CorsConfig { reason: String },
}

#[derive(Clone)]
struct RateLimitState {
    keyword: Arc<DefaultKeyedRateLimiter<String>>,
    vector: Arc<DefaultKeyedRateLimiter<String>>,
    hybrid: Arc<DefaultKeyedRateLimiter<String>>,
    typeahead: Arc<DefaultKeyedRateLimiter<String>>,
    proxy_mode: ProxyMode,
    trusted: Arc<HashSet<IpAddr>>,
}

impl RateLimitState {
    fn try_new(cfg: &RateLimitConfig) -> Result<Arc<Self>, ServerError> {
        debug_assert!(cfg.window_ms.get() > 0);
        debug_assert!(cfg.keyword.max_requests.get() > 0);
        let window_ms = cfg.window_ms.get();
        let kw = build_keyed_limiter(&cfg.keyword, window_ms);
        let vec = build_keyed_limiter(&cfg.vector, window_ms);
        let hybrid = build_keyed_limiter(&cfg.hybrid, window_ms);
        let ta = build_keyed_limiter(&cfg.typeahead, window_ms);
        Ok(Arc::new(Self {
            keyword: Arc::new(kw),
            vector: Arc::new(vec),
            hybrid: Arc::new(hybrid),
            typeahead: Arc::new(ta),
            proxy_mode: cfg.proxy_mode,
            trusted: Arc::new(cfg.trusted_proxies.iter().copied().collect()),
        }))
    }
}

fn build_keyed_limiter(
    route: &RouteLimitConfig,
    window_ms: u64,
) -> DefaultKeyedRateLimiter<String> {
    // Model "max_requests per window_ms" by setting replenish interval to window_ms / max_requests
    // and max burst to route.burst. This allows up to `burst` immediate requests, replenishing
    // `max_requests` cells per `window_ms` on average.
    let per = std::time::Duration::from_millis(window_ms);
    let n = route.max_requests.get();
    debug_assert!(n > 0);
    // Use deprecated constructor to set replenish_1_per = per / n precisely.
    #[allow(deprecated)]
    let mut quota =
        Quota::new(NonZeroU32::new(n).expect("nonzero"), per).expect("window_ms must be > 0");
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
    let (bucket, limiter) = if path == KEYWORD_PATH {
        ("keyword", &state.keyword)
    } else if path == VECTOR_PATH {
        ("vector", &state.vector)
    } else if path == HYBRID_PATH {
        ("hybrid", &state.hybrid)
    } else if path == TYPEAHEAD_PATH {
        ("typeahead", &state.typeahead)
    } else {
        // For any unknown route, apply no limiter here; fallback will render 404.
        return next.run(req).await;
    };
    let key = format!("{bucket}:{ip}");

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
    mode: ProxyMode,
    trusted: &HashSet<IpAddr>,
) -> Option<IpAddr> {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    let Some(peer_ip) = peer else {
        return None;
    };

    match mode {
        ProxyMode::Off => Some(peer_ip),
        ProxyMode::XForwardedFor => {
            // Only trust headers from known proxy addresses.
            if trusted.contains(&peer_ip) {
                parse_xff(req.headers()).or(Some(peer_ip))
            } else {
                Some(peer_ip)
            }
        }
        ProxyMode::Forwarded => {
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

impl HybridQuery {
    fn into_params(self) -> Result<HybridSearchParams, ApiError> {
        let HybridQuery {
            index,
            query,
            filter,
            embedder,
            limit,
            fields,
            fusion,
            keyword_weight,
            vector_weight,
        } = self;
        let limit = limit;
        ensure_range("limit", limit, 1, HYBRID_RESULT_LIMIT_MAX)?;
        let index = trim_non_empty("index", index)?;
        let query = trim_non_empty("q", query)?;
        let filter = sanitize_optional(filter);
        let embedder = sanitize_optional(embedder);
        let fields = expand_csv(fields);
        validate_list("fields", &fields, MAX_FIELD_COUNT)?;
        let per_branch_limit = limit.min(HYBRID_PER_SOURCE_LIMIT_MAX).max(1);
        let keyword_params = KeywordSearchParams {
            index: index.clone(),
            query: query.clone(),
            filter: filter.clone(),
            sort: Vec::new(),
            fields: fields.clone(),
            limit: per_branch_limit,
            offset: 0,
        };
        let vector_params = VectorSearchParams {
            index,
            query,
            embedder,
            filter,
            fields,
            top_k: per_branch_limit,
        };
        let fusion_mode = match fusion {
            HybridFusionQuery::Rrf => HybridFusion::Rrf {
                k: HYBRID_DEFAULT_RRF_K,
            },
            HybridFusionQuery::Weighted => {
                let kw = keyword_weight.unwrap_or(HYBRID_DEFAULT_WEIGHT);
                let vw = vector_weight.unwrap_or(HYBRID_DEFAULT_WEIGHT);
                validate_weight_param("keyword_weight", kw)?;
                validate_weight_param("vector_weight", vw)?;
                let (kw_norm, vw_norm) = normalize_hybrid_weights(kw, vw)
                    .map_err(|msg| ApiError::invalid_param("fusion", msg))?;
                HybridFusion::Weighted {
                    keyword_weight: kw_norm,
                    vector_weight: vw_norm,
                }
            }
        };
        Ok(HybridSearchParams {
            keyword: keyword_params,
            vector: vector_params,
            limit,
            fusion: fusion_mode,
        })
    }

    const fn default_limit() -> usize {
        20
    }
}

impl TypeaheadQuery {
    fn into_params(self) -> Result<TypeaheadParams, ApiError> {
        debug_assert!(TYPEAHEAD_MIN_QUERY_LEN >= 1);
        debug_assert!(TYPEAHEAD_LIMIT_DEFAULT <= TYPEAHEAD_LIMIT_MAX);
        let TypeaheadQuery {
            index,
            query,
            filter,
            limit,
        } = self;
        let index = trim_non_empty("index", index)?;
        let query = trim_non_empty("q", query)?;
        ensure_min_length("q", &query, TYPEAHEAD_MIN_QUERY_LEN)?;
        let filter = sanitize_optional(filter);
        let limit = limit.unwrap_or(TYPEAHEAD_LIMIT_DEFAULT);
        ensure_range("limit", limit, 1, TYPEAHEAD_LIMIT_MAX)?;
        let normalized_query = normalize_typeahead_query(&query);
        Ok(TypeaheadParams {
            index,
            query,
            normalized_query,
            filter,
            limit,
        })
    }
}

impl TypeaheadParams {
    fn cache_key(&self) -> TypeaheadCacheKey {
        debug_assert!(!self.index.is_empty());
        debug_assert!(self.limit <= TYPEAHEAD_LIMIT_MAX);
        TypeaheadCacheKey::new(
            &self.index,
            &self.normalized_query,
            self.filter.as_deref(),
            self.limit,
        )
    }

    fn to_keyword_params(&self) -> KeywordSearchParams {
        debug_assert!(self.limit <= TYPEAHEAD_LIMIT_MAX);
        debug_assert!(self.normalized_query.len() >= TYPEAHEAD_MIN_QUERY_LEN);
        KeywordSearchParams {
            index: self.index.clone(),
            query: self.query.clone(),
            filter: self.filter.clone(),
            sort: Vec::new(),
            fields: typeahead_fields().clone(),
            limit: self.limit,
            offset: 0,
        }
    }
}

impl TypeaheadCacheKey {
    fn new(index: &str, normalized_query: &str, filter: Option<&str>, limit: usize) -> Self {
        debug_assert!(!index.trim().is_empty());
        debug_assert!(!normalized_query.is_empty());
        debug_assert!(limit > 0);
        Self {
            index: index.to_string(),
            query: normalized_query.to_string(),
            filter: filter.map(|value| value.to_string()),
            limit,
        }
    }
}

impl TypeaheadState {
    fn new() -> Self {
        debug_assert!(TYPEAHEAD_CACHE_CAPACITY > 0);
        debug_assert!(TYPEAHEAD_CACHE_TTL >= Duration::from_secs(1));
        let cache = Cache::builder()
            .max_capacity(TYPEAHEAD_CACHE_CAPACITY)
            .time_to_live(TYPEAHEAD_CACHE_TTL)
            .build();
        Self { cache }
    }

    async fn lookup(
        &self,
        provider: DynSearchProvider,
        key: TypeaheadCacheKey,
        params: &TypeaheadParams,
    ) -> Result<(Arc<Vec<Value>>, TypeaheadCacheStatus), ApiError> {
        debug_assert!(params.limit <= TYPEAHEAD_LIMIT_MAX);
        debug_assert!(params.normalized_query.len() >= TYPEAHEAD_MIN_QUERY_LEN);

        if let Some(rows) = self.cache.get(&key).await {
            return Ok((rows, TypeaheadCacheStatus::Hit));
        }

        let keyword_params = params.to_keyword_params();
        let loader = async move {
            let rows = provider
                .keyword(keyword_params)
                .await
                .map_err(ApiError::from)?;
            Ok::<Arc<Vec<Value>>, ApiError>(Arc::new(rows))
        };

        match self.cache.try_get_with(key, loader).await {
            Ok(rows) => Ok((rows, TypeaheadCacheStatus::Miss)),
            Err(error) => {
                let owned = Arc::try_unwrap(error).unwrap_or_else(|arc| arc.as_ref().clone());
                Err(owned)
            }
        }
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
        ApiError::new(StatusCode::BAD_REQUEST, ERROR_INVALID_PARAMETER, message).with_field(field)
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

impl From<SearchError> for ApiError {
    fn from(error: SearchError) -> Self {
        match error.kind {
            SearchErrorKind::InvalidParameter => {
                let field = error.field.unwrap_or_else(|| "parameter".to_string());
                ApiError::invalid_param(&field, error.message)
            }
            SearchErrorKind::NotFound { resource } => ApiError::not_found(&resource, error.message),
            SearchErrorKind::RateLimited { retry_after_ms } => {
                let mut api = ApiError::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    ERROR_RATE_LIMITED,
                    error.message,
                );
                if let Some(delay) = retry_after_ms {
                    api = api.with_retry_after(delay);
                }
                api
            }
            SearchErrorKind::Internal => {
                tracing::error!(message = %error.message, "search request failed");
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

fn typeahead_fields() -> &'static Vec<String> {
    TYPEAHEAD_FIELDS_VEC.get_or_init(|| {
        TYPEAHEAD_FIELD_WHITELIST
            .iter()
            .map(|value| value.to_string())
            .collect()
    })
}

fn ensure_min_length(field: &str, value: &str, min: usize) -> Result<(), ApiError> {
    debug_assert!(!field.is_empty());
    debug_assert!(min >= 1);
    if value.chars().count() < min {
        return Err(ApiError::invalid_param(
            field,
            format!("must include at least {min} characters"),
        ));
    }
    Ok(())
}

fn normalize_typeahead_query(value: &str) -> String {
    debug_assert!(!value.trim().is_empty());
    debug_assert!(TYPEAHEAD_MIN_QUERY_LEN >= 1);
    let mut normalized = String::with_capacity(value.len());
    let mut pending_space = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space && !normalized.is_empty() {
            normalized.push(' ');
            pending_space = false;
        }
        normalized.extend(ch.to_lowercase());
    }
    if normalized.ends_with(' ') {
        normalized.pop();
    }
    normalized
}

fn take_arc_vec(rows: Arc<Vec<Value>>) -> Vec<Value> {
    debug_assert!(rows.len() <= TYPEAHEAD_LIMIT_MAX);
    debug_assert!(TYPEAHEAD_LIMIT_MAX <= 64);
    match Arc::try_unwrap(rows) {
        Ok(vec) => vec,
        Err(shared) => shared.as_ref().clone(),
    }
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

fn validate_weight_param(field: &str, value: f32) -> Result<(), ApiError> {
    debug_assert!(!field.is_empty());
    if !value.is_finite() {
        return Err(ApiError::invalid_param(field, "must be finite"));
    }
    if !(0.0..=1.0).contains(&value) {
        return Err(ApiError::invalid_param(
            field,
            "must be between 0.0 and 1.0",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;
    use tower::ServiceExt;

    struct MockSearchProvider;

    #[async_trait::async_trait]
    impl SearchProvider for MockSearchProvider {
        async fn keyword(&self, _params: KeywordSearchParams) -> Result<Vec<Value>, SearchError> {
            Ok(Vec::new())
        }

        async fn vector(&self, _params: VectorSearchParams) -> Result<Vec<Value>, SearchError> {
            Ok(Vec::new())
        }

        async fn hybrid(&self, _params: HybridSearchParams) -> Result<Vec<Value>, SearchError> {
            Ok(Vec::new())
        }
    }

    fn mock_api_state() -> ApiStateHandle {
        Arc::new(ApiState::new(Arc::new(MockSearchProvider)))
    }

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
    fn hybrid_query_defaults_limit_and_weights() {
        let query = HybridQuery {
            index: "kio".to_string(),
            query: "foo".to_string(),
            filter: None,
            embedder: None,
            limit: 20,
            fields: Vec::new(),
            fusion: HybridFusionQuery::Rrf,
            keyword_weight: None,
            vector_weight: None,
        };
        let params = query.into_params().expect("defaults must be valid");
        assert_eq!(params.limit, 20);
        if let HybridFusion::Rrf { k } = params.fusion {
            assert_eq!(k, HYBRID_DEFAULT_RRF_K);
        } else {
            panic!("expected RRF for default fusion");
        }
        assert_eq!(params.keyword.limit, 20);
        assert_eq!(params.vector.top_k, 20);
    }

    #[test]
    fn hybrid_query_rejects_invalid_weight() {
        let query = HybridQuery {
            index: "kio".to_string(),
            query: "foo".to_string(),
            filter: None,
            embedder: None,
            limit: 10,
            fields: Vec::new(),
            fusion: HybridFusionQuery::Weighted,
            keyword_weight: Some(-0.1),
            vector_weight: Some(0.5),
        };
        assert!(query.into_params().is_err());
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

    fn server_config_with(cors: CorsConfig) -> ServerConfig {
        ServerConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            rate_limit: RateLimitConfig::default(),
            cors,
        }
    }

    #[tokio::test]
    async fn cors_disabled_yields_no_headers() {
        let config = server_config_with(CorsConfig::default());
        let state = mock_api_state();
        let router = build_app_router(&config, state).expect("router builds");

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(HEALTHZ_PATH)
                    .header(header::ORIGIN, "http://example.com")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "CORS disabled must not emit ACAO"
        );
    }

    #[tokio::test]
    async fn cors_enabled_allows_explicit_origin() {
        let cors = CorsConfig {
            enabled: true,
            allow_origins: vec!["http://localhost:5173".to_string()],
            ..CorsConfig::default()
        };
        let config = server_config_with(cors);
        let state = mock_api_state();
        let router = build_app_router(&config, state).expect("router builds");

        let origin = "http://localhost:5173";
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(HEALTHZ_PATH)
                    .header(header::ORIGIN, origin)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        let header_value = response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("ACAO header present when enabled");
        assert_eq!(header_value, origin);
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none(),
            "credentials header remains absent when disabled"
        );
    }

    #[tokio::test]
    async fn cors_preflight_includes_configured_headers() {
        let cors = CorsConfig {
            enabled: true,
            allow_origins: vec!["http://localhost:5173".to_string()],
            allow_headers: vec!["authorization".to_string(), "content-type".to_string()],
            expose_headers: vec!["x-total-count".to_string()],
            allow_credentials: true,
            max_age_secs: 720,
            ..CorsConfig::default()
        };
        let config = server_config_with(cors);
        let state = mock_api_state();
        let router = build_app_router(&config, state).expect("router builds");

        let origin = "http://localhost:5173";
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(HEALTHZ_PATH)
                    .header(header::ORIGIN, origin)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("simple request succeeds");

        assert_eq!(get_response.status(), StatusCode::OK);
        let get_headers = get_response.headers();
        assert_eq!(
            get_headers
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("origin header present"),
            origin
        );
        assert_eq!(
            get_headers
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .expect("credentials header present"),
            "true"
        );
        let expose_headers = get_headers
            .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .expect("expose headers present")
            .to_str()
            .expect("expose headers ascii");
        assert!(
            expose_headers.contains("x-total-count"),
            "expose headers must echo configured entries: {expose_headers}"
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri(KEYWORD_PATH)
                    .header(header::ORIGIN, origin)
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("preflight succeeds");

        assert!(
            response.status() == StatusCode::NO_CONTENT || response.status() == StatusCode::OK,
            "preflight returns success status"
        );
        let headers = response.headers();
        assert_eq!(
            headers
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("origin header present"),
            origin
        );

        let methods = headers
            .get(header::ACCESS_CONTROL_ALLOW_METHODS)
            .expect("allow-methods present")
            .to_str()
            .expect("allow-methods ascii");
        assert!(
            methods.contains("GET"),
            "methods list should include configured GET: {methods}"
        );

        let allow_headers = headers
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .expect("allow-headers present")
            .to_str()
            .expect("allow-headers ascii");
        assert!(
            allow_headers.contains("authorization") && allow_headers.contains("content-type"),
            "allow headers must echo configured entries: {allow_headers}"
        );

        let credentials = headers
            .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .expect("credentials header present");
        assert_eq!(credentials, "true");

        let max_age = headers
            .get(header::ACCESS_CONTROL_MAX_AGE)
            .expect("max-age header present")
            .to_str()
            .expect("max-age ascii");
        assert_eq!(max_age, "720");
    }
}

pub fn build_api_router() -> Router {
    debug_assert!(HEALTHZ_PATH.starts_with("/v1/"));
    debug_assert!(HEALTHZ_PATH.ends_with("healthz"));
    debug_assert!(TYPEAHEAD_LIMIT_DEFAULT <= TYPEAHEAD_LIMIT_MAX);

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
        .route(
            HYBRID_PATH,
            get(hybrid_search).fallback(method_not_allowed_handler),
        )
        .route(
            TYPEAHEAD_PATH,
            get(typeahead_search).fallback(method_not_allowed_handler),
        )
}

pub async fn serve(config: ServerConfig, search: DynSearchProvider) -> Result<(), ServerError> {
    debug_assert!(config.listen_addr.len() <= 128);
    debug_assert!(!config.listen_addr.contains('\n'));

    let api_state: ApiStateHandle = Arc::new(ApiState::new(search));
    let listen_addr = parse_listen_addr(&config.listen_addr)?;

    let listener = bind_listener(listen_addr).await?;

    let local_addr = listener
        .local_addr()
        .map_err(|source| ServerError::LocalAddr { source })?;
    tracing::info!(%local_addr, "zetesis server listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownEvent::Pending);

    let shutdown_future = broadcast_shutdown(shutdown_tx);

    let app = build_app_router(&config, api_state.clone())?;
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

fn build_app_router(config: &ServerConfig, state: ApiStateHandle) -> Result<Router, ServerError> {
    debug_assert!(HEALTHZ_PATH.starts_with('/'));
    debug_assert_eq!(HEALTHZ_STATUS, "ok");

    let mut router = Router::new()
        .merge(build_api_router())
        .fallback(not_found_handler);

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            let path = matched_path_or_uri(request);
            let request_id =
                header_request_id(request.headers()).unwrap_or_else(|| "-".to_string());
            tracing::info_span!(
                "http.request",
                method = %request.method(),
                path = %path,
                request_id = %request_id
            )
        })
        .on_response(
            |response: &axum::response::Response, latency: Duration, span: &tracing::Span| {
                let status = response.status().as_u16();
                let latency_ms = latency.as_millis().min(u128::from(u64::MAX)) as u64;
                tracing::info!(parent: span, status, latency_ms, "request completed");
            },
        )
        .on_failure(
            |error: ServerErrorsFailureClass, latency: Duration, span: &tracing::Span| {
                let latency_ms = latency.as_millis().min(u128::from(u64::MAX)) as u64;
                tracing::error!(parent: span, latency_ms, error = %error, "request failed");
            },
        );

    if config.rate_limit.enabled {
        let limiter_state = RateLimitState::try_new(&config.rate_limit)?;
        let rate_layer = middleware::from_fn_with_state(limiter_state, rate_limit_middleware);
        router = router.layer(rate_layer);
    }

    if config.cors.enabled {
        let cors_layer = build_cors_layer(&config.cors)?;
        router = router.layer(cors_layer);
    }

    router = router.layer(trace_layer);

    let request_id_header = HeaderName::from_static(REQUEST_ID_HEADER);
    let make_request_id = MakeRequestUuid::default();
    router = router
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, make_request_id));

    Ok(router.layer(AddExtensionLayer::new(state)))
}

fn build_cors_layer(config: &CorsConfig) -> Result<CorsLayer, ServerError> {
    debug_assert!(!config.allow_origins.is_empty());
    let origins: Vec<HeaderValue> = config
        .allow_origins
        .iter()
        .map(|origin| {
            HeaderValue::from_str(origin).map_err(|err| ServerError::CorsConfig {
                reason: format!("origin `{origin}` is not a valid header value: {err}"),
            })
        })
        .collect::<Result<_, _>>()?;

    let methods: Vec<Method> = config
        .allow_methods
        .iter()
        .map(|method| {
            Method::from_bytes(method.as_bytes()).map_err(|_| ServerError::CorsConfig {
                reason: format!("method `{method}` failed to parse post-validation"),
            })
        })
        .collect::<Result<_, _>>()?;

    let allow_headers: Vec<HeaderName> = config
        .allow_headers
        .iter()
        .map(|name| {
            HeaderName::from_bytes(name.as_bytes()).map_err(|err| ServerError::CorsConfig {
                reason: format!("header `{name}` is invalid: {err}"),
            })
        })
        .collect::<Result<_, _>>()?;

    let expose_headers: Vec<HeaderName> = config
        .expose_headers
        .iter()
        .map(|name| {
            HeaderName::from_bytes(name.as_bytes()).map_err(|err| ServerError::CorsConfig {
                reason: format!("expose-header `{name}` is invalid: {err}"),
            })
        })
        .collect::<Result<_, _>>()?;

    let mut cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(AllowMethods::list(methods))
        .allow_credentials(config.allow_credentials)
        .max_age(Duration::from_secs(config.max_age_secs));

    if !allow_headers.is_empty() {
        cors = cors.allow_headers(AllowHeaders::list(allow_headers));
    }

    if !expose_headers.is_empty() {
        cors = cors.expose_headers(ExposeHeaders::list(expose_headers));
    }

    Ok(cors)
}

async fn typeahead_search(
    Extension(state): Extension<ApiStateHandle>,
    Query(query): Query<TypeaheadQuery>,
) -> Result<axum::response::Response, ApiError> {
    debug_assert!(!TYPEAHEAD_PATH.is_empty());
    debug_assert!(TYPEAHEAD_LIMIT_MAX >= TYPEAHEAD_MIN_QUERY_LEN);
    let params = query.into_params()?;
    let key = params.cache_key();
    let (rows, status) = state
        .typeahead
        .lookup(Arc::clone(&state.search), key, &params)
        .await?;
    let payload = take_arc_vec(rows);
    let mut response = Json(payload).into_response();
    let headers = response.headers_mut();
    let name = HeaderName::from_static(CACHE_HEADER_NAME);
    let value = match status {
        TypeaheadCacheStatus::Hit => HeaderValue::from_static(CACHE_HIT_VALUE),
        TypeaheadCacheStatus::Miss => HeaderValue::from_static(CACHE_MISS_VALUE),
    };
    headers.insert(name, value);
    Ok(response)
}

async fn keyword_search(
    Extension(state): Extension<ApiStateHandle>,
    Query(query): Query<KeywordQuery>,
) -> Result<Json<Vec<Value>>, ApiError> {
    debug_assert!(!KEYWORD_PATH.is_empty());
    let params = query.into_params()?;
    let rows = state.search.keyword(params).await.map_err(ApiError::from)?;
    Ok(Json(rows))
}

async fn vector_search(
    Extension(state): Extension<ApiStateHandle>,
    Query(query): Query<VectorQuery>,
) -> Result<Json<Vec<Value>>, ApiError> {
    debug_assert!(!VECTOR_PATH.is_empty());
    let params = query.into_params()?;
    let rows = state.search.vector(params).await.map_err(ApiError::from)?;
    Ok(Json(rows))
}

async fn hybrid_search(
    Extension(state): Extension<ApiStateHandle>,
    Query(query): Query<HybridQuery>,
) -> Result<Json<Vec<Value>>, ApiError> {
    debug_assert!(!HYBRID_PATH.is_empty());
    let params = query.into_params()?;
    let rows = state.search.hybrid(params).await.map_err(ApiError::from)?;
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
