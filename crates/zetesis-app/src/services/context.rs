use std::sync::Arc;

use ai_ox::embedding::EmbeddingError;
use async_trait::async_trait;
use backon::ExponentialBuilder;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::direct::NotKeyed;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::index::milli::{MilliBootstrapError, ensure_index};
use crate::index::writer::IndexWriteError;
use crate::paths::{AppPaths, PathError};
use crate::pipeline::Silo;
use crate::services::jobs::{EmbeddingJobStore, EmbeddingJobStoreError};
use gemini_ox::GeminiRequestError;

pub type GenericRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct EmbedService {
    pub embedder_key: String,
    pub dim: usize,
    pub client: Arc<dyn EmbedClient>,
    pub job_client: Option<Arc<dyn EmbeddingJobClient>>,
    pub runtime: EmbedRuntimeOptions,
}

impl EmbedService {
    pub async fn embed_batch(
        &self,
        texts: &[&str],
        task: EmbedBatchTask,
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(texts.len() < usize::MAX);
        debug_assert!(self.dim > 0);
        self.client.embed_batch(texts, task, mode).await
    }

    pub async fn embed_documents(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(texts.len() < usize::MAX);
        debug_assert!(self.dim > 0);
        self.embed_batch(texts, EmbedBatchTask::Document, self.runtime.documents_mode)
            .await
    }

    pub async fn embed_queries(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(texts.len() < usize::MAX);
        debug_assert!(self.dim > 0);
        self.embed_batch(texts, EmbedBatchTask::Query, self.runtime.queries_mode)
            .await
    }

    pub fn provider(&self) -> Option<Arc<dyn EmbeddingJobClient>> {
        self.job_client.as_ref().map(Arc::clone)
    }
}

#[async_trait]
pub trait EmbedClient: Send + Sync {
    async fn embed_batch(
        &self,
        texts: &[&str],
        task: EmbedBatchTask,
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>>;
}

#[async_trait]
pub trait EmbeddingJobClient: Send + Sync {
    async fn submit_job(
        &self,
        doc_id: &str,
        texts: &[String],
        task: EmbedBatchTask,
        mode: EmbedMode,
    ) -> PipelineResult<SubmittedJob>;

    async fn job_state(&self, provider_job_id: &str) -> PipelineResult<JobMetadata>;

    async fn fetch_job_result(&self, provider_job_id: &str) -> PipelineResult<Vec<Vec<f32>>>;
}

#[derive(Debug, Clone)]
pub struct SubmittedJob {
    pub provider_job_id: String,
    pub batch_count: u32,
}

#[derive(Debug, Clone)]
pub struct JobMetadata {
    pub state: ProviderJobState,
    pub failed_count: usize,
}

#[derive(Debug, Clone)]
pub enum ProviderJobState {
    Pending,
    Running,
    Succeeded,
    Failed {
        message: String,
        details: Option<Vec<String>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmbedBatchTask {
    Document,
    Query,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbedMode {
    Sequential,
    Batched,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct EmbedRuntimeOptions {
    pub documents_mode: EmbedMode,
    pub queries_mode: EmbedMode,
    pub max_batch: usize,
}

impl EmbedRuntimeOptions {
    pub const fn new(documents_mode: EmbedMode, queries_mode: EmbedMode, max_batch: usize) -> Self {
        Self {
            documents_mode,
            queries_mode,
            max_batch,
        }
    }
}

impl Default for EmbedRuntimeOptions {
    fn default() -> Self {
        Self {
            documents_mode: EmbedMode::Batched,
            queries_mode: EmbedMode::Sequential,
            max_batch: 32,
        }
    }
}

#[derive(Default)]
pub struct Governors {
    pub io: Option<Arc<GenericRateLimiter>>,
    pub embed: Option<Arc<GenericRateLimiter>>,
}

pub struct PipelineContext {
    pub paths: AppPaths,
    pub embed: EmbedService,
    pub jobs: Arc<EmbeddingJobStore>,
    pub backoff: ExponentialBuilder,
    pub governors: Governors,
}

impl PipelineContext {
    pub fn index_for(&self, silo: Silo) -> PipelineResult<milli::Index> {
        ensure_index(
            &self.paths,
            silo.slug(),
            &self.embed.embedder_key,
            self.embed.dim,
        )
        .map_err(PipelineError::from)
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
    #[error("missing GOOGLE_AI_API_KEY or GEMINI_API_KEY environment variable")]
    MissingGeminiApiKey,
    #[error(transparent)]
    Gemini(#[from] Box<GeminiRequestError>),
    #[error(transparent)]
    IndexWrite(#[from] IndexWriteError),
    #[error(transparent)]
    Embedding(#[from] EmbeddingError),
    #[error(transparent)]
    Jobs(#[from] EmbeddingJobStoreError),
}

impl PipelineError {
    pub fn message(msg: impl Into<String>) -> Self {
        PipelineError::Message(msg.into())
    }
}

impl From<GeminiRequestError> for PipelineError {
    fn from(e: GeminiRequestError) -> Self {
        PipelineError::Gemini(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingClient {
        calls: Arc<Mutex<Vec<EmbedBatchTask>>>,
        modes: Arc<Mutex<Vec<EmbedMode>>>,
    }

    #[async_trait]
    impl EmbedClient for RecordingClient {
        async fn embed_batch(
            &self,
            texts: &[&str],
            task: EmbedBatchTask,
            mode: EmbedMode,
        ) -> PipelineResult<Vec<Vec<f32>>> {
            debug_assert!(texts.len() < usize::MAX);
            self.calls
                .lock()
                .expect("call log mutex poisoned")
                .push(task);
            self.modes
                .lock()
                .expect("mode log mutex poisoned")
                .push(mode);
            Ok(vec![vec![0.0; 1]; texts.len()])
        }
    }

    #[tokio::test]
    async fn embed_documents_uses_document_task() {
        let client = Arc::new(RecordingClient::default());
        let calls = client.calls.clone();
        let modes = client.modes.clone();
        let service = EmbedService {
            embedder_key: "test".to_string(),
            dim: 1,
            client,
            job_client: None,
            runtime: EmbedRuntimeOptions::default(),
        };

        let texts = ["a"];
        let result = service.embed_documents(&texts).await;
        assert!(result.is_ok(), "document embedding should succeed");
        let calls = calls.lock().expect("call log mutex poisoned");
        assert_eq!(calls.len(), 1, "expected a single embed call");
        assert_eq!(calls[0], EmbedBatchTask::Document, "wrong task recorded");
        let modes = modes.lock().expect("mode log mutex poisoned");
        assert_eq!(
            modes[0],
            EmbedMode::Batched,
            "documents should default to batched mode"
        );
    }

    #[tokio::test]
    async fn embed_queries_uses_query_task() {
        let client = Arc::new(RecordingClient::default());
        let calls = client.calls.clone();
        let modes = client.modes.clone();
        let service = EmbedService {
            embedder_key: "test".to_string(),
            dim: 1,
            client,
            job_client: None,
            runtime: EmbedRuntimeOptions::default(),
        };

        let texts = ["q"];
        let result = service.embed_queries(&texts).await;
        assert!(result.is_ok(), "query embedding should succeed");
        let calls = calls.lock().expect("call log mutex poisoned");
        assert_eq!(calls.len(), 1, "expected a single embed call");
        assert_eq!(calls[0], EmbedBatchTask::Query, "wrong task recorded");
        let modes = modes.lock().expect("mode log mutex poisoned");
        assert_eq!(
            modes[0],
            EmbedMode::Sequential,
            "queries should default to sequential mode"
        );
    }
}
