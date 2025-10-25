use std::sync::Arc;

use backon::ExponentialBuilder;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::direct::NotKeyed;
use thiserror::Error;

use crate::index::milli::{MilliBootstrapError, ensure_index};
use crate::paths::{AppPaths, PathError};
use crate::services::processor::Silo;

pub type GenericRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct EmbedService {
    pub embedder_key: String,
    pub dim: usize,
    pub client: Arc<dyn EmbedClient>,
}

impl EmbedService {
    pub fn embed_batch(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        self.client.embed_batch(texts)
    }
}

pub trait EmbedClient: Send + Sync {
    fn embed_batch(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>>;
}

pub struct Governors {
    pub io: Option<Arc<GenericRateLimiter>>,
    pub embed: Option<Arc<GenericRateLimiter>>,
}

impl Default for Governors {
    fn default() -> Self {
        Self {
            io: None,
            embed: None,
        }
    }
}

pub struct PipelineContext {
    pub paths: AppPaths,
    pub embed: EmbedService,
    pub backoff: ExponentialBuilder,
    pub governors: Governors,
}

impl PipelineContext {
    pub fn index_for(&self, silo: Silo) -> PipelineResult<milli::Index> {
        ensure_index(&self.paths, silo.slug()).map_err(PipelineError::from)
    }
}

pub type PipelineResult<T> = Result<T, PipelineError>;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Milli(#[from] MilliBootstrapError),
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl PipelineError {
    pub fn message(msg: impl Into<String>) -> Self {
        PipelineError::Message(msg.into())
    }
}
