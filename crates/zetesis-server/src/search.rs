use async_trait::async_trait;
use serde_json::Value;
use std::fmt;

pub const HYBRID_RESULT_LIMIT_MAX: usize = 100;
pub const HYBRID_PER_SOURCE_LIMIT_MAX: usize = 50;
pub const HYBRID_DEFAULT_RRF_K: usize = 60;
pub const HYBRID_DEFAULT_WEIGHT: f32 = 0.5;
const HYBRID_WEIGHT_EPSILON: f32 = f32::EPSILON;

#[derive(Debug, Clone)]
pub struct KeywordSearchParams {
    pub index: String,
    pub query: String,
    pub filter: Option<String>,
    pub sort: Vec<String>,
    pub fields: Vec<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub struct VectorSearchParams {
    pub index: String,
    pub query: String,
    pub embedder: Option<String>,
    pub filter: Option<String>,
    pub fields: Vec<String>,
    pub top_k: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum HybridFusion {
    Rrf {
        k: usize,
    },
    Weighted {
        keyword_weight: f32,
        vector_weight: f32,
    },
}

#[derive(Debug, Clone)]
pub struct HybridSearchParams {
    pub keyword: KeywordSearchParams,
    pub vector: VectorSearchParams,
    pub limit: usize,
    pub fusion: HybridFusion,
}

#[async_trait]
pub trait SearchProvider: Send + Sync + 'static {
    async fn keyword(&self, params: KeywordSearchParams) -> Result<Vec<Value>, SearchError>;
    async fn vector(&self, params: VectorSearchParams) -> Result<Vec<Value>, SearchError>;
    async fn hybrid(&self, params: HybridSearchParams) -> Result<Vec<Value>, SearchError>;
}

#[derive(Debug, Clone)]
pub struct SearchError {
    pub kind: SearchErrorKind,
    pub message: String,
    pub field: Option<String>,
}

impl SearchError {
    pub fn invalid_param(field: impl Into<String>, message: impl Into<String>) -> Self {
        SearchError {
            kind: SearchErrorKind::InvalidParameter,
            message: message.into(),
            field: Some(field.into()),
        }
    }

    pub fn not_found(resource: impl Into<String>, message: impl Into<String>) -> Self {
        SearchError {
            kind: SearchErrorKind::NotFound {
                resource: resource.into(),
            },
            message: message.into(),
            field: None,
        }
    }

    pub fn rate_limited(message: impl Into<String>, retry_after_ms: Option<u64>) -> Self {
        SearchError {
            kind: SearchErrorKind::RateLimited { retry_after_ms },
            message: message.into(),
            field: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SearchError {
            kind: SearchErrorKind::Internal,
            message: message.into(),
            field: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SearchErrorKind {
    InvalidParameter,
    NotFound { resource: String },
    RateLimited { retry_after_ms: Option<u64> },
    Internal,
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SearchError {}

pub fn normalize_hybrid_weights(
    keyword_weight: f32,
    vector_weight: f32,
) -> Result<(f32, f32), &'static str> {
    if !keyword_weight.is_finite() || !vector_weight.is_finite() {
        return Err("weights must be finite");
    }
    if keyword_weight < 0.0 || vector_weight < 0.0 {
        return Err("weights must be non-negative");
    }
    let sum = keyword_weight + vector_weight;
    if sum <= HYBRID_WEIGHT_EPSILON {
        return Err("weights must not both be zero");
    }
    Ok((keyword_weight / sum, vector_weight / sum))
}
