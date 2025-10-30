use crate::model::gemini::GeminiError;
use gemini_ox::{
    content::Content as GeminiContent,
    embedding::{BatchEmbedContentsResponse, ContentEmbedding, EmbedContentRequest, TaskType},
    Gemini, GeminiRequestError,
};

/// Controls how the embedder attempts to batch embedding requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStrategy {
    /// Try batches first, fall back to individual calls when not supported.
    Auto,
    /// Always use the batch endpoint (fail if unavailable).
    ForceBatch,
    /// Always call the single-item endpoint.
    ForceSingle,
}

/// High-level Gemini embedding helper with batch-aware behavior.
#[derive(Debug, Clone)]
pub struct GeminiEmbedder {
    client: Gemini,
    model: String,
    task_type: Option<TaskType>,
    output_dimensionality: Option<u32>,
    strategy: BatchStrategy,
    max_batch: usize,
}

const DEFAULT_MAX_BATCH: usize = 64;

impl GeminiEmbedder {
    /// Create a new embedder for a specific Gemini embedding model.
    pub fn new(client: Gemini, model: impl Into<String>) -> Self {
        Self {
            client,
            model: model.into(),
            task_type: None,
            output_dimensionality: None,
            strategy: BatchStrategy::Auto,
            max_batch: DEFAULT_MAX_BATCH,
        }
    }

    /// Override the retrieval task type (documents vs. queries).
    pub fn with_task_type(mut self, task_type: TaskType) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Override the embedding dimensionality (must not exceed the model's native size).
    pub fn with_output_dimensionality(mut self, dimensionality: u32) -> Self {
        self.output_dimensionality = Some(dimensionality);
        self
    }

    /// Configure batch behavior.
    pub fn with_strategy(mut self, strategy: BatchStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Configure the maximum number of items sent in a single batch request.
    ///
    /// Values outside the supported range are clamped automatically.
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        let clamped = max_batch.max(1);
        self.max_batch = clamped;
        self
    }

    /// Returns the effective batch size limit.
    pub fn max_batch(&self) -> usize {
        self.max_batch
    }

    /// Generate embeddings for the provided texts.
    pub async fn embed_texts(
        &self,
        texts: &[impl AsRef<str>],
    ) -> Result<Vec<Vec<f32>>, GeminiError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let payloads: Vec<String> = texts.iter().map(|t| t.as_ref().to_string()).collect();
        self.embed_chunks(&payloads).await
    }

    async fn embed_chunks(&self, payloads: &[String]) -> Result<Vec<Vec<f32>>, GeminiError> {
        let chunk_size = self.max_batch.max(1);
        let mut outputs: Vec<Vec<f32>> = Vec::with_capacity(payloads.len());
        let mut start = 0;
        let mut allow_batch = self.strategy != BatchStrategy::ForceSingle;

        while start < payloads.len() {
            let end = (start + chunk_size).min(payloads.len());
            let chunk = &payloads[start..end];
            let vectors = if allow_batch {
                match self.try_batch(chunk).await {
                    Ok(result) => result,
                    Err(GeminiRequestError::BatchNotAvailable { .. })
                        if self.strategy == BatchStrategy::Auto =>
                    {
                        allow_batch = false;
                        self.embed_sequential(chunk).await?
                    }
                    Err(err) => return Err(GeminiError::from(err)),
                }
            } else {
                self.embed_sequential(chunk).await?
            };

            outputs.extend(vectors);
            start = end;
        }

        Ok(outputs)
    }

    async fn try_batch(
        &self,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, GeminiRequestError> {
        if self.strategy == BatchStrategy::ForceSingle {
            return Err(GeminiRequestError::BatchNotAvailable {
                message: "batch usage disabled by strategy".to_string(),
            });
        }

        let requests = inputs
            .iter()
            .map(|text| self.build_single_request(text))
            .collect();
        let response = self
            .client
            .batch_embed_contents()
            .model(self.model.clone())
            .requests(requests)
            .build()
            .send()
            .await?;
        Self::collect_embeddings(response, inputs.len())
            .map_err(GeminiRequestError::JsonDeserializationError)
    }

    async fn embed_sequential(
        &self,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, GeminiError> {
        let mut out = Vec::with_capacity(inputs.len());
        for text in inputs {
            let request = self.build_single_request(text);
            let response = request.send().await.map_err(GeminiError::from)?;
            out.push(response.embedding.values);
        }
        Ok(out)
    }

    fn collect_embeddings(
        response: BatchEmbedContentsResponse,
        expected: usize,
    ) -> Result<Vec<Vec<f32>>, serde_json::Error> {
        if response.embeddings.len() != expected {
            let msg = format!(
                "batch response length mismatch: expected {}, got {}",
                expected,
                response.embeddings.len()
            );
            return Err(serde_json::Error::custom(msg));
        }
        Ok(response
            .embeddings
            .into_iter()
            .map(|embedding: ContentEmbedding| embedding.values)
            .collect())
    }

    fn build_single_request(&self, text: &str) -> EmbedContentRequest {
        let mut builder = self
            .client
            .embed_content()
            .model(self.model.clone())
            .content(GeminiContent::from(text));

        if let Some(task_type) = self.task_type.clone() {
            builder = builder.task_type(task_type);
        }

        if let Some(dim) = self.output_dimensionality {
            builder = builder.output_dimensionality(dim);
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_batch_clamps_to_range() {
        let client = Gemini::new("test");
        let embedder = GeminiEmbedder::new(client.clone(), "text-embedding-004")
            .with_max_batch(0);
        assert_eq!(embedder.max_batch(), 1);

        let embedder = GeminiEmbedder::new(client.clone(), "text-embedding-004")
            .with_max_batch(10_000);
        assert_eq!(embedder.max_batch(), 10_000);
    }

    #[test]
    fn embed_empty_input_returns_empty_result() {
        let client = Gemini::new("test");
        let embedder = GeminiEmbedder::new(client, "text-embedding-004");
        let result = futures_lite::future::block_on(embedder.embed_texts::<&str>(&[]));
        assert!(result.unwrap().is_empty());
    }
}
