use std::{sync::Arc, time::Duration};

use ai_ox::{
    agent::Agent,
    content::{
        message::{Message, MessageRole},
        part::Part,
    },
    model::gemini::GeminiModel,
    usage::Usage,
};
use thiserror::Error;

use crate::pipeline::structured::{StructuredDecision, StructuredValidationError};

pub(crate) const STRUCTURED_SYSTEM_PROMPT: &str = "Jesteś ekspertem analizującym orzeczenia KIO. Odpowiadasz wyłącznie w formacie JSON zgodnym ze schematem przekazanym przez API. Pole `chunks` musi zawierać wszystkie fragmenty tekstu w kolejności i obejmować pełny tekst dokumentu.";
const RETRY_SUFFIX: &str =
    "\n\nUWAGA: Poprzednia odpowiedź była niepoprawna. Zwróć wyłącznie poprawny JSON.";
const DEFAULT_MAX_ATTEMPTS: u8 = 2;

#[derive(Debug, Clone)]
pub struct StructuredExtraction {
    pub decision: StructuredDecision,
    pub usage: Usage,
}

#[derive(Clone)]
pub struct StructuredExtractor {
    agent: Arc<Agent>,
    max_attempts: u8,
}

impl StructuredExtractor {
    pub fn from_env(model: impl Into<String>) -> Result<Self, StructuredExtractError> {
        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| StructuredExtractError::MissingApiKey)?;

        let model = GeminiModel::builder()
            .api_key(api_key)
            .model(model.into())
            .build();

        let agent = Agent::model(model)
            .system_instruction(STRUCTURED_SYSTEM_PROMPT)
            .build();

        Ok(Self {
            agent: Arc::new(agent),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }

    pub async fn extract(
        &self,
        text: &str,
    ) -> Result<StructuredExtraction, StructuredExtractError> {
        self.extract_with_context(text, &[], Some(text)).await
    }

    pub async fn extract_with_context(
        &self,
        message_text: &str,
        attachments: &[Part],
        coverage_text: Option<&str>,
    ) -> Result<StructuredExtraction, StructuredExtractError> {
        let mut attempt = 0;
        let mut last_error: Option<StructuredExtractError> = None;
        let attachments = attachments.to_vec();

        while attempt < self.max_attempts {
            let mut user_parts = attachments.clone();
            let mut payload_text = message_text.to_owned();
            if attempt > 0 {
                payload_text.push_str(RETRY_SUFFIX);
            }
            user_parts.push(Part::text(payload_text));

            let messages = vec![Message::new(MessageRole::User, user_parts)];

            match self
                .agent
                .generate_typed::<StructuredDecision>(messages)
                .await
            {
                Ok(response) => {
                    let decision = response.data;
                    decision.validate()?;
                    if let Some(text) = coverage_text
                        && !text.trim().is_empty()
                    {
                        ensure_chunk_coverage(&decision, text)?;
                    }
                    return Ok(StructuredExtraction {
                        decision,
                        usage: response.usage,
                    });
                }
                Err(err) => {
                    last_error = Some(StructuredExtractError::Agent(Box::new(err)));
                }
            }

            attempt += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Err(last_error.expect("at least one attempt executed"))
    }
}

#[derive(Debug, Error)]
pub enum StructuredExtractError {
    #[error("missing GOOGLE_AI_API_KEY or GEMINI_API_KEY environment variable")]
    MissingApiKey,
    #[error(transparent)]
    Model(#[from] Box<ai_ox::model::gemini::GeminiError>),
    #[error(transparent)]
    Agent(#[from] Box<ai_ox::agent::error::AgentError>),
    #[error(transparent)]
    Validation(#[from] StructuredValidationError),
}

impl From<ai_ox::model::gemini::GeminiError> for StructuredExtractError {
    fn from(e: ai_ox::model::gemini::GeminiError) -> Self {
        StructuredExtractError::Model(Box::new(e))
    }
}

impl From<ai_ox::agent::error::AgentError> for StructuredExtractError {
    fn from(e: ai_ox::agent::error::AgentError) -> Self {
        StructuredExtractError::Agent(Box::new(e))
    }
}

fn ensure_chunk_coverage(
    decision: &StructuredDecision,
    source_text: &str,
) -> Result<(), StructuredExtractError> {
    let source_tokens = count_tokens(source_text);
    if source_tokens == 0 {
        return Ok(());
    }

    let chunk_tokens: usize = decision
        .chunks
        .iter()
        .map(|chunk| count_tokens(&chunk.body))
        .sum();

    if chunk_tokens == 0 {
        return Err(StructuredExtractError::Validation(
            StructuredValidationError::with_issue(
                "chunk coverage mismatch: chunks contain no text",
            ),
        ));
    }

    let source_len = source_tokens as f32;
    let chunk_len = chunk_tokens as f32;
    if source_len == 0.0 {
        return Ok(());
    }

    let diff_ratio = ((source_len - chunk_len).abs()) / source_len;
    const MAX_ALLOWED_DIFF: f32 = 0.15; // allow up to 15% difference

    if diff_ratio > MAX_ALLOWED_DIFF {
        return Err(StructuredExtractError::Validation(
            StructuredValidationError::with_issue(format!(
                "chunk coverage mismatch: {:.1}% difference between source and chunks",
                diff_ratio * 100.0
            )),
        ));
    }

    Ok(())
}

fn count_tokens(text: &str) -> usize {
    text.split_whitespace().count()
}
