use std::sync::Arc;

use async_trait::async_trait;
use rig::client::EmbeddingsClient;
use rig::embeddings::{EmbeddingError, EmbeddingModel};
use rig::providers::gemini;

use crate::services::context::{
    EmbedBatchTask, EmbedClient, EmbedMode, GenericRateLimiter, PipelineError, PipelineResult,
};

const ZERO_VECTOR_VALUE: f32 = 0.0;

#[derive(Clone)]
pub struct RigEmbedClient {
    client: Arc<gemini::Client>,
    model_name: String,
    dim: usize,
    limiter: Option<Arc<GenericRateLimiter>>,
    max_batch: usize,
}

impl RigEmbedClient {
    pub fn from_env(
        model_name: impl Into<String>,
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

        let client = gemini::Client::new(&api_key);

        Ok(Self {
            client: Arc::new(client),
            model_name: model_name.into(),
            dim,
            limiter,
            max_batch: max_batch.max(1),
        })
    }

    async fn run_embedding(
        &self,
        payloads: &[&str],
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let max_chunk = match mode {
            EmbedMode::Sequential => 1,
            EmbedMode::Batched => self.max_batch,
        };

        let mut outputs: Vec<Vec<f32>> = Vec::with_capacity(payloads.len());
        let mut offset = 0usize;

        while offset < payloads.len() {
            let end = (offset + max_chunk).min(payloads.len());
            if let Some(limiter) = &self.limiter {
                limiter.until_ready().await;
            }
            let slice = &payloads[offset..end];
            let vectors = self.embed_slice(slice).await?;
            if vectors.len() != slice.len() {
                return Err(PipelineError::message(format!(
                    "embedding count mismatch: expected {}, got {}",
                    slice.len(),
                    vectors.len()
                )));
            }
            outputs.extend(vectors);
            offset = end;
        }

        Ok(outputs)
    }

    async fn embed_slice(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        let documents: Vec<String> = texts.iter().map(|s| s.to_string()).collect();

        let model = self.client.as_ref().embedding_model_with_ndims(&self.model_name, self.dim);

        let result: Vec<rig::embeddings::Embedding> = model
            .embed_texts(documents)
            .await
            .map_err(|err| match err {
                EmbeddingError::HttpError(http_err) => {
                    PipelineError::message(format!("HTTP error: {}", http_err))
                }
                EmbeddingError::JsonError(json_err) => PipelineError::Json(json_err),
                EmbeddingError::ProviderError(msg) => PipelineError::message(msg),
                _ => PipelineError::message(format!("Embedding error: {}", err)),
            })?;

        let vectors: Vec<Vec<f32>> = result
            .into_iter()
            .map(|emb| emb.vec.into_iter().map(|v| v as f32).collect())
            .collect();
        Ok(vectors)
    }
}

#[async_trait]
impl EmbedClient for RigEmbedClient {
    async fn embed_batch(
        &self,
        texts: &[&str],
        _task: EmbedBatchTask,
        mode: EmbedMode,
    ) -> PipelineResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<Option<Vec<f32>>> = Vec::with_capacity(texts.len());
        let mut non_empty_indices = Vec::new();
        let mut non_empty_payloads = Vec::new();

        for (idx, text) in texts.iter().enumerate() {
            if text.trim().is_empty() {
                results.push(Some(vec![ZERO_VECTOR_VALUE; self.dim]));
            } else {
                results.push(None);
                non_empty_indices.push(idx);
                non_empty_payloads.push(*text);
            }
        }

        if !non_empty_payloads.is_empty() {
            let vectors = self.run_embedding(&non_empty_payloads, mode).await?;
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

        let mut finalized = Vec::with_capacity(results.len());
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
