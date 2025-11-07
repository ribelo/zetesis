use std::sync::Arc;

use async_trait::async_trait;
use gemini_ox::{
    Gemini,
    batches::{
        BatchJob, BatchJobState, GenerateContentResponsePayload, GenerateContentSource,
        InlineGenerateContentBatch,
    },
    content::{Content, Part, Role},
    generate_content::{GenerationConfig, request::GenerateContentRequest},
};
use serde_json;

use crate::{
    pipeline::structured::StructuredDecision,
    services::{
        context::{
            GenericRateLimiter, JobMetadata, PipelineError, PipelineResult, ProviderJobState,
            StructuredBatchInput, StructuredBatchRequest, StructuredBatchResponse,
            StructuredJobClient, SubmittedJob,
        },
        structured::STRUCTURED_SYSTEM_PROMPT,
    },
};

#[derive(Clone)]
pub struct GeminiBatchStructuredClient {
    gemini: Gemini,
    model: String,
    limiter: Option<Arc<GenericRateLimiter>>,
}

impl GeminiBatchStructuredClient {
    pub fn from_env(
        model: impl Into<String>,
        limiter: Option<Arc<GenericRateLimiter>>,
    ) -> Result<Self, PipelineError> {
        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| PipelineError::MissingGeminiApiKey)?;
        let gemini = Gemini::builder().api_key(api_key).build();
        let model = model.into();
        if model.trim().is_empty() {
            return Err(PipelineError::message(
                "generation model key must not be empty",
            ));
        }
        Ok(Self {
            gemini,
            model,
            limiter,
        })
    }

    fn build_request(
        &self,
        input: &StructuredBatchInput,
    ) -> Result<GenerateContentRequest, PipelineError> {
        let attachment = Part::from_bytes_with_name(
            &input.bytes,
            input.mime_type.clone(),
            input.display_name.clone(),
        );
        let mut parts = Vec::new();
        parts.push(attachment);
        parts.push(Part::from(input.text.clone()));

        let user_content = Content::builder().role(Role::User).parts(parts).build();
        let system_instruction = Content::builder()
            .role(Role::User)
            .text(STRUCTURED_SYSTEM_PROMPT)
            .build();
        let generation_config = GenerationConfig::builder()
            .response_mime_type("application/json")
            .candidate_count(1)
            .temperature(0.0)
            .build();

        Ok(GenerateContentRequest::builder()
            .model(self.model.clone())
            .content(user_content)
            .system_instruction(system_instruction)
            .generation_config(generation_config)
            .build())
    }
}

#[async_trait]
impl StructuredJobClient for GeminiBatchStructuredClient {
    async fn submit_job(&self, request: StructuredBatchRequest) -> PipelineResult<SubmittedJob> {
        request.validate()?;
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }
        let StructuredBatchRequest { inputs } = request;
        let mut generate_requests = Vec::with_capacity(inputs.len());
        for input in &inputs {
            generate_requests.push(self.build_request(input)?);
        }
        let batch = InlineGenerateContentBatch {
            requests: generate_requests,
        };
        let display_name = sanitize_display_name(
            inputs
                .first()
                .map(|input| input.doc_id.as_str())
                .unwrap_or("job"),
        );

        let job = self
            .gemini
            .submit_generate_content_job(
                self.model.clone(),
                GenerateContentSource::Inline(batch),
                display_name,
            )
            .await
            .map_err(PipelineError::from)?;

        let provider_job_id = job
            .name
            .clone()
            .ok_or_else(|| PipelineError::message("Gemini batch submission missing job name"))?;

        Ok(SubmittedJob {
            provider_job_id,
            batch_count: inputs.len() as u32,
        })
    }

    async fn job_state(&self, provider_job_id: &str) -> PipelineResult<JobMetadata> {
        if provider_job_id.trim().is_empty() {
            return Err(PipelineError::message("provider job id must not be empty"));
        }
        let job = self
            .gemini
            .get_batch_job(provider_job_id)
            .await
            .map_err(PipelineError::from)?;
        let state = map_state(&job);
        let failed_count = count_failed_items(&job);
        Ok(JobMetadata {
            state,
            failed_count,
        })
    }

    async fn fetch_job_result(
        &self,
        provider_job_id: &str,
    ) -> PipelineResult<StructuredBatchResponse> {
        if provider_job_id.trim().is_empty() {
            return Err(PipelineError::message("provider job id must not be empty"));
        }
        let result = self
            .gemini
            .get_batch_content_result(provider_job_id)
            .await
            .map_err(PipelineError::from)?;
        let mut decisions = Vec::with_capacity(result.responses.len());
        for payload in &result.responses {
            decisions.push(parse_decision(payload)?);
        }
        Ok(StructuredBatchResponse { decisions })
    }
}

fn sanitize_display_name(doc_id: &str) -> String {
    const MAX_LEN: usize = 40;
    let mut normalized: String = doc_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    if normalized.is_empty() {
        normalized = "job".to_string();
    }
    if normalized.len() > MAX_LEN {
        normalized.truncate(MAX_LEN);
    }
    format!("structured-{normalized}")
}

fn map_state(job: &BatchJob) -> ProviderJobState {
    match job.state {
        Some(BatchJobState::JobStateQueued)
        | Some(BatchJobState::JobStatePending)
        | Some(BatchJobState::JobStateUnspecified) => ProviderJobState::Pending,
        Some(BatchJobState::JobStateRunning)
        | Some(BatchJobState::JobStateUpdating)
        | Some(BatchJobState::JobStatePaused) => ProviderJobState::Running,
        Some(BatchJobState::JobStateSucceeded) => ProviderJobState::Succeeded,
        Some(BatchJobState::JobStatePartiallySucceeded) => ProviderJobState::Failed {
            message: "Gemini batch partially succeeded".to_string(),
            details: job.error.as_ref().and_then(|err| err.details.clone()),
        },
        Some(
            BatchJobState::JobStateFailed
            | BatchJobState::JobStateCancelled
            | BatchJobState::JobStateCancelling
            | BatchJobState::JobStateExpired,
        ) => ProviderJobState::Failed {
            message: job
                .error
                .as_ref()
                .and_then(|err| err.message.clone())
                .unwrap_or_else(|| "Gemini batch job failed".to_string()),
            details: job.error.as_ref().and_then(|err| err.details.clone()),
        },
        None => ProviderJobState::Pending,
    }
}

fn count_failed_items(job: &BatchJob) -> usize {
    job.dest
        .as_ref()
        .and_then(|dest| dest.inlined_responses.as_ref())
        .map(|responses| responses.iter().filter(|resp| resp.error.is_some()).count())
        .unwrap_or(0)
}

fn parse_decision(
    payload: &GenerateContentResponsePayload,
) -> Result<StructuredDecision, PipelineError> {
    if let Some(feedback) = payload.prompt_feedback.as_ref()
        && feedback.block_reason.is_some()
    {
        return Err(PipelineError::message(
            "Gemini batch rejected the structured prompt for safety reasons",
        ));
    }
    let candidates = payload
        .candidates
        .as_ref()
        .ok_or_else(|| PipelineError::message("batch response missing candidates"))?;
    let candidate = candidates
        .first()
        .ok_or_else(|| PipelineError::message("batch response missing candidate payload"))?;

    let mut buffer = String::new();
    for part in candidate.content.parts() {
        if let Some(text) = part.data.as_text() {
            buffer.push_str(text.as_str());
        }
    }
    if buffer.trim().is_empty() {
        return Err(PipelineError::message(
            "batch response did not include structured JSON output",
        ));
    }

    let decision: StructuredDecision = serde_json::from_str(buffer.trim()).map_err(|err| {
        PipelineError::message(format!("failed to parse structured decision JSON: {err}"))
    })?;
    decision
        .validate()
        .map_err(|err| PipelineError::message(err.to_string()))?;
    Ok(decision)
}
