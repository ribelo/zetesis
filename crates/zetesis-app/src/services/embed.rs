use std::sync::Arc;

use ai_ox::{BatchStrategy, EmbeddingModel, GeminiEmbedder};
use async_trait::async_trait;
use gemini_ox::{
    Gemini,
    batches::{BatchJobState, InlineEmbeddingsBatch},
    content::Content,
    embedding::TaskType,
};

use crate::services::context::{
    EmbedBatchTask, EmbedClient, EmbedMode, EmbeddingJobClient, GenericRateLimiter, JobMetadata,
    PipelineError, PipelineResult, ProviderJobState, SubmittedJob,
};

const ZERO_VECTOR_VALUE: f32 = 0.0;

#[derive(Clone)]
pub struct GeminiEmbedClient {
    client: Gemini,
    model: String,
    dim: usize,
    limiter: Option<Arc<GenericRateLimiter>>,
    max_batch: usize,
}

impl GeminiEmbedClient {
    pub fn from_env(
        model: impl Into<String>,
        dim: usize,
        limiter: Option<Arc<GenericRateLimiter>>,
        max_batch: usize,
    ) -> Result<Self, PipelineError> {
        if dim == 0 {
            return Err(PipelineError::message(
                "embedding dimension must be greater than zero",
            ));
        }

        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| PipelineError::MissingGeminiApiKey)?;
        let bounded_batch = max_batch.max(1);

        Ok(Self {
            client: Gemini::new(api_key),
            model: model.into(),
            dim,
            limiter,
            max_batch: bounded_batch,
        })
    }

    async fn run_embedding(
        &self,
        payloads: &[&str],
        task_type: TaskType,
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(!self.model.is_empty());
        debug_assert!(self.dim > 0);
        debug_assert!(payloads.len() < usize::MAX);

        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let strategy = match mode {
            EmbedMode::Batched => BatchStrategy::Auto,
            EmbedMode::Sequential => BatchStrategy::ForceSingle,
        };
        let mut embedder = GeminiEmbedder::new(self.client.clone(), self.model.clone())
            .with_task_type(task_type)
            .with_output_dimensionality(self.dim as u32);

        let max_chunk = match mode {
            EmbedMode::Batched => self.max_batch,
            EmbedMode::Sequential => 1,
        };
        embedder = embedder.with_strategy(strategy).with_max_batch(max_chunk);

        let mut outputs: Vec<Vec<f32>> = Vec::with_capacity(payloads.len());
        let mut offset = 0_usize;
        while offset < payloads.len() {
            let end = (offset + max_chunk).min(payloads.len());
            if let Some(limiter) = &self.limiter {
                limiter.until_ready().await;
            }
            let chunk_slice = &payloads[offset..end];
            let vectors = embedder
                .embed(chunk_slice)
                .await
                .map_err(PipelineError::from)?;
            if vectors.len() != chunk_slice.len() {
                return Err(PipelineError::message(format!(
                    "embedding count mismatch: expected {}, got {}",
                    chunk_slice.len(),
                    vectors.len()
                )));
            }
            outputs.extend(vectors);
            offset = end;
        }

        Ok(outputs)
    }
}

#[async_trait]
impl EmbedClient for GeminiEmbedClient {
    async fn embed_batch(
        &self,
        texts: &[&str],
        task: EmbedBatchTask,
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(texts.len() < usize::MAX);
        debug_assert!(self.dim > 0);

        let task_type = match task {
            EmbedBatchTask::Document => TaskType::RetrievalDocument,
            EmbedBatchTask::Query => TaskType::RetrievalQuery,
        };
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<Option<Vec<f32>>> = Vec::with_capacity(texts.len());
        let mut non_empty_indices: Vec<usize> = Vec::new();
        let mut non_empty_payloads: Vec<&str> = Vec::new();

        for (idx, text) in texts.iter().enumerate() {
            if text.trim().is_empty() {
                results.push(Some(vec![ZERO_VECTOR_VALUE; self.dim]));
                continue;
            }
            results.push(None);
            non_empty_indices.push(idx);
            non_empty_payloads.push(*text);
        }

        if !non_empty_payloads.is_empty() {
            let vectors = self
                .run_embedding(&non_empty_payloads, task_type, mode)
                .await?;
            if vectors.len() != non_empty_payloads.len() {
                return Err(PipelineError::message(format!(
                    "embedding count mismatch: expected {}, got {}",
                    non_empty_payloads.len(),
                    vectors.len()
                )));
            }

            for (idx, vector) in non_empty_indices.into_iter().zip(vectors.into_iter()) {
                if vector.len() != self.dim {
                    return Err(PipelineError::message(format!(
                        "expected embedding dimension {}, got {}",
                        self.dim,
                        vector.len()
                    )));
                }
                results[idx] = Some(vector);
            }
        }

        let mut finalized: Vec<Vec<f32>> = Vec::with_capacity(results.len());
        for value in results.into_iter() {
            match value {
                Some(vector) => finalized.push(vector),
                None => {
                    return Err(PipelineError::message(
                        "internal error: missing embedding result",
                    ));
                }
            }
        }

        Ok(finalized)
    }
}

#[async_trait]
impl EmbeddingJobClient for GeminiEmbedClient {
    async fn submit_job(
        &self,
        doc_id: &str,
        texts: &[String],
        task: EmbedBatchTask,
        _mode: EmbedMode,
    ) -> PipelineResult<SubmittedJob> {
        debug_assert!(!doc_id.is_empty());
        debug_assert!(self.dim > 0);

        if texts.is_empty() {
            return Err(PipelineError::message("embedding job has no payloads"));
        }

        let task_type = match task {
            EmbedBatchTask::Document => TaskType::RetrievalDocument,
            EmbedBatchTask::Query => TaskType::RetrievalQuery,
        };

        let inline_batch = InlineEmbeddingsBatch::builder()
            .contents(
                texts
                    .iter()
                    .map(|body| Content::from(body.as_str()))
                    .collect::<Vec<Content>>(),
            )
            .task_type(task_type)
            .output_dimensionality(self.dim as u32)
            .build();

        let display_name = format!("embed-{}", doc_id);
        let batch = self
            .client
            .submit_embeddings_job(self.model.clone(), inline_batch, display_name)
            .await?;
        let job_id = batch
            .name
            .clone()
            .ok_or_else(|| PipelineError::message("batch submission missing job name"))?;

        Ok(SubmittedJob {
            provider_job_id: job_id,
            batch_count: 1,
        })
    }

    async fn job_state(&self, provider_job_id: &str) -> PipelineResult<JobMetadata> {
        debug_assert!(!provider_job_id.is_empty());

        let job = self.client.get_batch_job(provider_job_id).await?;
        let state = job.state.unwrap_or(BatchJobState::JobStateUnspecified);
        let failed_count = job
            .dest
            .as_ref()
            .and_then(|dest| dest.inlined_embed_content_responses.as_ref())
            .map(|responses| responses.iter().filter(|resp| resp.error.is_some()).count())
            .unwrap_or(0);

        let provider_state = match state {
            BatchJobState::JobStateQueued
            | BatchJobState::JobStatePending
            | BatchJobState::JobStateUnspecified => ProviderJobState::Pending,
            BatchJobState::JobStateRunning
            | BatchJobState::JobStateUpdating
            | BatchJobState::JobStatePaused => ProviderJobState::Running,
            BatchJobState::JobStateSucceeded => ProviderJobState::Succeeded,
            BatchJobState::JobStatePartiallySucceeded => {
                let message = job
                    .error
                    .as_ref()
                    .and_then(|err| err.message.clone())
                    .unwrap_or_else(|| "batch partially succeeded".to_string());
                ProviderJobState::Failed {
                    message,
                    details: job.error.as_ref().and_then(|err| err.details.clone()),
                }
            }
            BatchJobState::JobStateFailed
            | BatchJobState::JobStateCancelled
            | BatchJobState::JobStateCancelling
            | BatchJobState::JobStateExpired => {
                let message = job
                    .error
                    .as_ref()
                    .and_then(|err| err.message.clone())
                    .unwrap_or_else(|| format!("batch state {:?}", state));
                ProviderJobState::Failed {
                    message,
                    details: job.error.as_ref().and_then(|err| err.details.clone()),
                }
            }
        };

        Ok(JobMetadata {
            state: provider_state,
            failed_count,
        })
    }

    async fn fetch_job_result(&self, provider_job_id: &str) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(!provider_job_id.is_empty());

        let result = self.client.get_batch_result(provider_job_id).await?;
        let vectors = result
            .embeddings
            .into_iter()
            .map(|vector| vector.values)
            .collect::<Vec<Vec<f32>>>();
        Ok(vectors)
    }
}
