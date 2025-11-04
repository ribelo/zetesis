use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client as HttpClient;

use crate::services::context::{
    GenericRateLimiter, JobMetadata, PipelineError, PipelineResult, StructuredBatchRequest,
    StructuredBatchResponse, StructuredJobClient, SubmittedJob,
};

#[derive(Clone)]
pub struct GeminiBatchStructuredClient {
    http: HttpClient,
    api_key: String,
    model: String,
    limiter: Option<Arc<GenericRateLimiter>>,
}

impl GeminiBatchStructuredClient {
    pub fn from_env(
        model: impl Into<String>,
        limiter: Option<Arc<GenericRateLimiter>>,
    ) -> Result<Self, PipelineError> {
        let model = model.into();
        debug_assert!(!model.trim().is_empty());
        debug_assert!(model.len() < 128);

        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| PipelineError::MissingGeminiApiKey)?;

        Ok(Self {
            http: HttpClient::new(),
            api_key,
            model,
            limiter,
        })
    }
}

#[async_trait]
impl StructuredJobClient for GeminiBatchStructuredClient {
    async fn submit_job(&self, request: &StructuredBatchRequest) -> PipelineResult<SubmittedJob> {
        request.validate();
        let _ = self.http.clone();
        debug_assert!(self.api_key.len() < 256);
        if let Some(limiter) = &self.limiter {
            debug_assert!(Arc::strong_count(limiter) >= 1);
        }
        debug_assert!(!self.model.trim().is_empty());
        debug_assert!(request.items.len() <= u32::MAX as usize);
        Err(PipelineError::message(
            "Gemini structured batch API integration pending",
        ))
    }

    async fn job_state(&self, provider_job_id: &str) -> PipelineResult<JobMetadata> {
        debug_assert!(!provider_job_id.trim().is_empty());
        debug_assert!(provider_job_id.len() < 512);
        Err(PipelineError::message(
            "Gemini structured batch job polling not yet implemented",
        ))
    }

    async fn fetch_job_result(
        &self,
        provider_job_id: &str,
    ) -> PipelineResult<StructuredBatchResponse> {
        debug_assert!(!provider_job_id.trim().is_empty());
        debug_assert!(provider_job_id.len() < 512);
        Err(PipelineError::message(
            "Gemini structured batch result retrieval not yet implemented",
        ))
    }
}
