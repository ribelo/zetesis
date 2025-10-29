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

const SYSTEM_PROMPT: &str = "Jesteś ekspertem analizującym orzeczenia KIO. Odpowiadasz wyłącznie w formacie JSON zgodnym ze schematem przekazanym przez API.";
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
            .system_instruction(SYSTEM_PROMPT)
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
        let mut attempt = 0;
        let mut last_error: Option<StructuredExtractError> = None;

        while attempt < self.max_attempts {
            let user_text = if attempt == 0 {
                text.to_owned()
            } else {
                format!("{text}{RETRY_SUFFIX}")
            };

            let messages = vec![Message::new(MessageRole::User, vec![Part::text(user_text)])];

            match self
                .agent
                .generate_typed::<StructuredDecision>(messages)
                .await
            {
                Ok(response) => {
                    let decision = response.data;
                    decision.validate()?;
                    return Ok(StructuredExtraction {
                        decision,
                        usage: response.usage,
                    });
                }
                Err(err) => {
                    last_error = Some(StructuredExtractError::Agent(err));
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
    Model(#[from] ai_ox::model::gemini::GeminiError),
    #[error(transparent)]
    Agent(#[from] ai_ox::agent::error::AgentError),
    #[error(transparent)]
    Validation(#[from] StructuredValidationError),
}
