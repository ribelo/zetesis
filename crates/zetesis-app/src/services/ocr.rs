//! Multi-provider OCR service orchestrating bounded concurrency with strict limits.

use std::{
    io::Cursor,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use ai_ox::{
    content::{
        message::{Message, MessageRole},
        part::Part,
    },
    errors::GenerateContentError,
    model::{Model, gemini::GeminiModel, request::ModelRequest, response::ModelResponse},
    usage::Usage as AiUsage,
};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bon::Builder;
use deepinfra_ox::{
    ChatRequest as DeepInfraChatRequest, DeepInfra, ImageUrl, Message as DeepInfraMessage,
    MessageContent as DeepInfraMessageContent, MessageContentPart as DeepInfraMessageContentPart,
    TokenUsage,
};
use futures_concurrency::{concurrent_stream::IntoConcurrentStream, prelude::ConcurrentStream};
use gemini_ox::generate_content::GenerationConfig;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::{PngDecoder, PngEncoder};
use image::imageops::FilterType;
use image::{ColorType, GenericImageView, ImageDecoder, ImageEncoder};
use regex::Regex;
use serde::Serialize;
use thiserror::Error;
use tracing::{debug, warn};

use crate::pdf::{PdfPageImage, render_pdf_to_png_images};

type OcrRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

const MAX_BATCH_INPUTS: usize = 64;
const MAX_PAGES_PER_DOCUMENT: usize = 1024;
const GEMINI_SYSTEM_INSTRUCTION: &str = "You are a meticulous OCR engine. Return faithful plain text transcription and preserve layout with Markdown where appropriate. Avoid inventing bounding boxes or extra metadata.";

fn deepinfra_rate_limiter() -> &'static Arc<OcrRateLimiter> {
    static LIMITER: OnceLock<Arc<OcrRateLimiter>> = OnceLock::new();
    LIMITER.get_or_init(|| {
        let quota = Quota::per_second(NonZeroU32::new(200).expect("non-zero quota"))
            .allow_burst(NonZeroU32::new(200).expect("non-zero burst"));
        Arc::new(RateLimiter::direct(quota))
    })
}

fn gemini_rate_limiter() -> &'static Arc<OcrRateLimiter> {
    static LIMITER: OnceLock<Arc<OcrRateLimiter>> = OnceLock::new();
    LIMITER.get_or_init(|| {
        let quota = Quota::per_second(NonZeroU32::new(120).expect("non-zero quota"))
            .allow_burst(NonZeroU32::new(60).expect("non-zero burst"));
        Arc::new(RateLimiter::direct(quota))
    })
}

#[async_trait]
pub trait OcrService: Send + Sync {
    async fn run_batch(
        &self,
        inputs: Vec<OcrInput>,
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError>;

    async fn run_single(
        &self,
        input: &OcrInput,
        config: &OcrConfig,
    ) -> Result<OcrDocumentResult, OcrError> {
        let results = self.run_batch(vec![input.clone()], config).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| OcrError::UnsupportedInput {
                id: input.id.clone(),
                mime_type: input.mime_type.to_string(),
            })
    }
}

#[derive(Clone)]
pub struct OcrEngine<P> {
    provider: P,
}

impl<P> OcrEngine<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> OcrService for OcrEngine<P>
where
    P: PageProvider,
{
    async fn run_batch(
        &self,
        inputs: Vec<OcrInput>,
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError> {
        validate_batch_limits(inputs.len(), config);

        let cfg = Arc::new(config.clone());
        let engine = Arc::new(self.clone());
        let collected = Arc::new(Mutex::new(Vec::with_capacity(inputs.len())));

        let engine_for_docs = Arc::clone(&engine);
        let cfg_for_docs = Arc::clone(&cfg);
        let collected_for_docs = Arc::clone(&collected);

        inputs
            .into_co_stream()
            .limit(Some(config.docs_concurrency))
            .enumerate()
            .try_for_each(move |(ordinal, input)| {
                Arc::clone(&engine_for_docs).process_document(
                    ordinal,
                    input,
                    Arc::clone(&cfg_for_docs),
                    Arc::clone(&collected_for_docs),
                )
            })
            .await?;

        let mut guard = collected.lock().expect("document collection lock poisoned");
        let mut ordered = std::mem::take(&mut *guard);
        drop(guard);

        ordered.sort_by_key(|(ordinal, _)| *ordinal);
        Ok(ordered.into_iter().map(|(_, doc)| doc).collect())
    }
}

impl<P> OcrEngine<P>
where
    P: PageProvider,
{
    async fn process_document(
        self: Arc<Self>,
        ordinal: usize,
        input: OcrInput,
        cfg: Arc<OcrConfig>,
        collected: Arc<Mutex<Vec<(usize, OcrDocumentResult)>>>,
    ) -> Result<(), OcrError> {
        assert!(
            !input.id.is_empty(),
            "document identifier must not be empty"
        );

        let input = Arc::new(input);

        let pages = tokio::task::spawn_blocking({
            let input = Arc::clone(&input);
            let cfg = Arc::clone(&cfg);
            move || collect_pages(input.as_ref(), cfg.as_ref())
        })
        .await
        .map_err(OcrError::TaskJoin)??;

        assert!(
            pages.len() <= MAX_PAGES_PER_DOCUMENT,
            "document {} has too many pages: {} > {}",
            input.id,
            pages.len(),
            MAX_PAGES_PER_DOCUMENT
        );

        let doc_id = input.id.clone();
        let accumulator = Arc::new(Mutex::new(Vec::with_capacity(pages.len())));
        let provider = self.provider.clone();

        pages
            .into_co_stream()
            .limit(Some(cfg.pages_concurrency))
            .try_for_each({
                let provider = provider.clone();
                let cfg = Arc::clone(&cfg);
                let input = Arc::clone(&input);
                let accumulator = Arc::clone(&accumulator);
                move |page| {
                    let provider = provider.clone();
                    let cfg = Arc::clone(&cfg);
                    let input = Arc::clone(&input);
                    let accumulator = Arc::clone(&accumulator);
                    async move {
                        let result = provider.process_page(input, cfg, page).await?;
                        let mut guard = accumulator.lock().expect("page collection lock poisoned");
                        guard.push(result);
                        Ok::<(), OcrError>(())
                    }
                }
            })
            .await?;

        let mut guard = accumulator.lock().expect("page collection lock poisoned");
        let mut page_results = std::mem::take(&mut *guard);
        drop(guard);

        page_results.sort_by_key(|page| page.page_index);

        let mut guard = collected.lock().expect("document collection lock poisoned");
        guard.push((
            ordinal,
            OcrDocumentResult {
                id: doc_id,
                pages: page_results,
            },
        ));

        Ok::<(), OcrError>(())
    }
}

fn validate_batch_limits(input_count: usize, config: &OcrConfig) {
    assert!(
        input_count <= MAX_BATCH_INPUTS,
        "batch too large: {} > {}",
        input_count,
        MAX_BATCH_INPUTS
    );
    assert!(
        config.docs_concurrency.get() > 0 && config.pages_concurrency.get() > 0,
        "concurrency limits must be non-zero"
    );
    let max_inflight = config.max_inflight_requests;
    let doc_limit = config.docs_concurrency;
    let page_limit = config.pages_concurrency;

    assert!(
        doc_limit.get() <= max_inflight.get() && page_limit.get() <= max_inflight.get(),
        "concurrency limits exceed inflight budget"
    );
    assert!(
        doc_limit.get().saturating_mul(page_limit.get()) <= max_inflight.get(),
        "combined concurrency exceeds inflight budget"
    );
}

#[async_trait]
pub trait PageProvider: Send + Sync + Clone + 'static {
    async fn process_page(
        &self,
        input: Arc<OcrInput>,
        config: Arc<OcrConfig>,
        page: PdfPageImage,
    ) -> Result<OcrPageResult, OcrError>;
}

/// Errors produced by the OCR service.
#[derive(Debug, Error)]
pub enum OcrError {
    #[error(transparent)]
    Pdf(#[from] crate::pdf::PdfTextError),
    #[error(transparent)]
    PdfRender(#[from] crate::pdf::PdfRenderError),
    #[error(transparent)]
    DeepInfra(#[from] deepinfra_ox::DeepInfraRequestError),
    #[error(transparent)]
    Gemini(#[from] GenerateContentError),
    #[error("failed to read input file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported input format for {id}: {mime_type}")]
    UnsupportedInput { id: String, mime_type: String },
    #[error("failed to decode image data for {id} (page {page_index}): {source}")]
    ImageDecode {
        id: String,
        page_index: usize,
        #[source]
        source: image::ImageError,
    },
    #[error("failed to encode image data for {id} (page {page_index}): {source}")]
    ImageEncode {
        id: String,
        page_index: usize,
        #[source]
        source: image::ImageError,
    },
    #[error("missing DEEPINFRA_API_KEY environment variable")]
    MissingDeepInfraApiKey,
    #[error("missing GOOGLE_AI_API_KEY or GEMINI_API_KEY environment variable")]
    MissingGeminiApiKey,
    #[error("OCR task join failed: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
}

/// Result produced for each OCR-processed page.
#[derive(Debug, Clone, Serialize)]
pub struct OcrPageResult {
    pub page_index: usize,
    pub render_width: u32,
    pub render_height: u32,
    pub ocr_width: u32,
    pub ocr_height: u32,
    pub mime_type: &'static str,
    pub plain_text: Option<String>,
    pub spans: Vec<OcrSpan>,
    pub usage: Option<TokenUsage>,
}

/// A single OCR span with bounding box metadata.
#[derive(Debug, Clone, Serialize)]
pub struct OcrSpan {
    pub kind: String,
    pub bbox: Option<[u32; 4]>,
    pub bbox_norm: Option<[f32; 4]>,
    pub text: String,
}

#[derive(Debug, Clone)]
struct PreparedOcrImage {
    data: Vec<u8>,
    width: u32,
    height: u32,
    mime_type: &'static str,
}

/// Parameters controlling OCR execution.
#[derive(Debug, Clone, Builder)]
pub struct OcrConfig {
    #[builder(into)]
    pub model: String,
    #[builder(default = 2048)]
    pub render_width: u32,
    #[builder(default = 1280)]
    pub image_max_edge: u32,
    pub detail: Option<String>,
    #[builder(default = 4096)]
    pub max_tokens: u32,
    #[builder(default = NonZeroUsize::new(2).unwrap())]
    pub docs_concurrency: NonZeroUsize,
    #[builder(default = NonZeroUsize::new(8).unwrap())]
    pub pages_concurrency: NonZeroUsize,
    #[builder(default = NonZeroUsize::new(16).unwrap())]
    pub max_inflight_requests: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OcrMimeType {
    Pdf,
    Png,
    Jpeg,
}

impl OcrMimeType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pdf => "application/pdf",
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "pdf" => Some(Self::Pdf),
            "png" => Some(Self::Png),
            "jpg" | "jpeg" => Some(Self::Jpeg),
            _ => None,
        }
    }
}

impl std::fmt::Display for OcrMimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct OcrInput {
    pub id: String,
    pub bytes: Arc<[u8]>,
    pub mime_type: OcrMimeType,
}

#[derive(Debug, Clone, Serialize)]
pub struct OcrDocumentResult {
    pub id: String,
    pub pages: Vec<OcrPageResult>,
}

#[derive(Clone)]
struct DeepInfraProvider {
    client: Arc<DeepInfra>,
    limiter: Arc<OcrRateLimiter>,
    backoff: ExponentialBuilder,
}

impl DeepInfraProvider {
    fn from_env() -> Result<Self, OcrError> {
        let client = DeepInfra::load_from_env().map_err(|_| OcrError::MissingDeepInfraApiKey)?;
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(5)
            .with_jitter();

        Ok(Self {
            client: Arc::new(client),
            limiter: Arc::clone(deepinfra_rate_limiter()),
            backoff,
        })
    }

    async fn call_with_backoff(
        &self,
        prepared: &PreparedOcrImage,
        prompt: &str,
        max_tokens: u32,
        model: &str,
    ) -> Result<deepinfra_ox::ChatCompletionResponse, OcrError> {
        let attempt = || async { self.invoke_model(prepared, prompt, max_tokens, model).await };
        attempt.retry(self.backoff.clone()).await
    }

    async fn invoke_model(
        &self,
        prepared: &PreparedOcrImage,
        prompt: &str,
        max_tokens: u32,
        model: &str,
    ) -> Result<deepinfra_ox::ChatCompletionResponse, OcrError> {
        let mut parts = Vec::with_capacity(2);
        let mut image_url = ImageUrl::new(format!(
            "data:{};base64,{}",
            prepared.mime_type,
            BASE64_STANDARD.encode(&prepared.data)
        ));
        image_url = image_url.with_detail("auto");

        parts.push(DeepInfraMessageContentPart::ImageUrl { image_url });
        parts.push(DeepInfraMessageContentPart::Text {
            text: prompt.to_string(),
        });

        let message = DeepInfraMessage::user(DeepInfraMessageContent::Parts(parts));
        let mut request = DeepInfraChatRequest::new(model.to_string(), vec![message]);
        request.max_tokens = Some(max_tokens);
        request.temperature = Some(0.0);

        self.limiter.until_ready().await;
        let response = self.client.send(&request).await?;
        if let Some(usage) = response.usage.as_ref() {
            debug!(
                prompt = prompt,
                max_tokens,
                completion_tokens = usage.completion_tokens,
                prompt_tokens = usage.prompt_tokens,
                total_tokens = usage.total_tokens,
                "deepinfra ocr invocation usage"
            );
        }
        Ok(response)
    }
}

#[async_trait]
impl PageProvider for DeepInfraProvider {
    async fn process_page(
        &self,
        input: Arc<OcrInput>,
        config: Arc<OcrConfig>,
        page: PdfPageImage,
    ) -> Result<OcrPageResult, OcrError> {
        let page_index = page.page_index;
        let render_width = page.width;
        let render_height = page.height;
        let max_edge = config.image_max_edge;
        let input_id = input.id.clone();
        let page_image = page;

        let prepared = tokio::task::spawn_blocking({
            let input_id = input_id.clone();
            move || prepare_image_for_ocr(&input_id, page_index, &page_image, max_edge)
        })
        .await
        .map_err(OcrError::TaskJoin)??;

        assert!(
            prepared.width > 0 && prepared.height > 0,
            "prepared image must be non-empty"
        );

        let default_prompt = build_grounded_prompt(
            config
                .detail
                .as_deref()
                .unwrap_or("<|grounding|>Convert the document to markdown."),
        );
        let fallback_prompt = build_grounded_prompt("<|grounding|>OCR this image.");

        let mut prompt = default_prompt.clone();
        let mut max_tokens = config.max_tokens;

        for attempt in 0..=1 {
            let response = self
                .call_with_backoff(&prepared, &prompt, max_tokens, &config.model)
                .await?;

            let usage = response.usage.clone();
            let content = response
                .choices
                .first()
                .and_then(|choice| choice.message.content.clone());

            let raw_text = content.as_ref().and_then(flatten_message_content_text);
            let (plain_text_str, spans) = match raw_text.as_deref() {
                Some(raw) => parse_deepseek_output(raw, prepared.width, prepared.height),
                None => (String::new(), Vec::new()),
            };

            let plain_text = if plain_text_str.trim().is_empty() {
                None
            } else {
                Some(plain_text_str.clone())
            };

            let truncated = usage
                .as_ref()
                .and_then(|u| u.completion_tokens)
                .map_or(false, |tokens| tokens >= u64::from(max_tokens));
            let looks_noise = plain_text.as_deref().map_or(false, text_is_numeric_noise);

            if attempt == 0 && (truncated || looks_noise) {
                warn!(
                    doc_id = %input.id,
                    page = page_index,
                    truncated,
                    looks_noise,
                    "retrying OCR page with fallback prompt",
                );
                prompt = fallback_prompt.clone();
                max_tokens = max_tokens.min(2048).max(512);
                continue;
            }

            return Ok(OcrPageResult {
                page_index,
                render_width,
                render_height,
                ocr_width: prepared.width,
                ocr_height: prepared.height,
                mime_type: prepared.mime_type,
                plain_text,
                spans,
                usage,
            });
        }

        Err(OcrError::UnsupportedInput {
            id: input.id.clone(),
            mime_type: input.mime_type.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct DeepInfraOcr {
    engine: OcrEngine<DeepInfraProvider>,
}

impl DeepInfraOcr {
    pub fn from_env() -> Result<Self, OcrError> {
        let provider = DeepInfraProvider::from_env()?;
        Ok(Self {
            engine: OcrEngine::new(provider),
        })
    }
}

#[async_trait]
impl OcrService for DeepInfraOcr {
    async fn run_batch(
        &self,
        inputs: Vec<OcrInput>,
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError> {
        self.engine.run_batch(inputs, config).await
    }
}

#[derive(Clone)]
struct GeminiProvider {
    primary: GeminiModel,
    fallback: GeminiModel,
    limiter: Arc<OcrRateLimiter>,
    backoff: ExponentialBuilder,
    model_name: String,
    primary_tokens: u32,
    fallback_tokens: u32,
}

impl GeminiProvider {
    fn from_env(model: impl Into<String>, max_output_tokens: u32) -> Result<Self, OcrError> {
        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| OcrError::MissingGeminiApiKey)?;
        let model_name = model.into();
        let fallback_tokens = fallback_token_target(max_output_tokens);

        let primary = GeminiModel::builder()
            .api_key(api_key.clone())
            .model(model_name.clone())
            .maybe_generation_config(Some(build_generation_config(max_output_tokens)))
            .build();

        let fallback = GeminiModel::builder()
            .api_key(api_key)
            .model(model_name.clone())
            .maybe_generation_config(Some(build_generation_config(fallback_tokens)))
            .build();

        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(5)
            .with_jitter();

        Ok(Self {
            primary,
            fallback,
            limiter: Arc::clone(gemini_rate_limiter()),
            backoff,
            model_name,
            primary_tokens: max_output_tokens,
            fallback_tokens,
        })
    }

    async fn call_with_backoff(
        &self,
        model: &GeminiModel,
        encoded_image: &str,
        mime_type: &'static str,
        prompt: &str,
    ) -> Result<ModelResponse, OcrError> {
        let attempt = || async {
            self.invoke_model(model, encoded_image, mime_type, prompt)
                .await
        };
        attempt.retry(self.backoff.clone()).await
    }

    async fn invoke_model(
        &self,
        model: &GeminiModel,
        encoded_image: &str,
        mime_type: &'static str,
        prompt: &str,
    ) -> Result<ModelResponse, OcrError> {
        let mut parts = Vec::with_capacity(2);
        parts.push(Part::blob_base64(encoded_image.to_string(), mime_type));
        parts.push(Part::text(prompt.to_string()));

        let user_message = Message::new(MessageRole::User, parts);
        let system_message = Message::new(
            MessageRole::System,
            vec![Part::text(GEMINI_SYSTEM_INSTRUCTION)],
        );

        let request = ModelRequest::builder()
            .messages([user_message])
            .system_message(system_message)
            .build();

        self.limiter.until_ready().await;
        let response = model.request(request).await?;
        Ok(response)
    }
}

#[async_trait]
impl PageProvider for GeminiProvider {
    async fn process_page(
        &self,
        input: Arc<OcrInput>,
        config: Arc<OcrConfig>,
        page: PdfPageImage,
    ) -> Result<OcrPageResult, OcrError> {
        assert!(
            config.max_tokens <= self.primary_tokens,
            "configured max_tokens {} exceeds provider budget {}",
            config.max_tokens,
            self.primary_tokens
        );

        let page_index = page.page_index;
        let render_width = page.width;
        let render_height = page.height;
        let max_edge = config.image_max_edge;
        let input_id = input.id.clone();
        let page_image = page;

        let prepared = tokio::task::spawn_blocking({
            let input_id = input_id.clone();
            move || prepare_image_for_ocr(&input_id, page_index, &page_image, max_edge)
        })
        .await
        .map_err(OcrError::TaskJoin)??;

        assert!(
            prepared.width > 0 && prepared.height > 0,
            "prepared image must be non-empty"
        );

        let encoded_image = BASE64_STANDARD.encode(&prepared.data);
        let default_prompt = build_grounded_prompt(
            config
                .detail
                .as_deref()
                .unwrap_or("<|grounding|>Convert the document to markdown."),
        );
        let fallback_prompt = build_grounded_prompt("<|grounding|>OCR this image.");

        let attempts = [
            (&self.primary, &default_prompt, self.primary_tokens),
            (&self.fallback, &fallback_prompt, self.fallback_tokens),
        ];

        for (idx, (model, prompt, budget)) in attempts.into_iter().enumerate() {
            let response = self
                .call_with_backoff(model, &encoded_image, prepared.mime_type, prompt)
                .await?;

            let usage = usage_to_token_usage(&response.usage);
            let raw_text = response.to_string();
            let plain_text = raw_text.as_ref().and_then(|text| {
                let trimmed = text.trim_end();
                if trimmed.trim().is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            });
            let looks_noise = plain_text.as_deref().map_or(false, text_is_numeric_noise);
            let truncated = usage
                .completion_tokens
                .map_or(false, |tokens| tokens >= u64::from(budget));
            let missing_text = plain_text.is_none();

            if idx == 0 && (truncated || looks_noise || missing_text) {
                warn!(
                    doc_id = %input.id,
                    page = page_index,
                    truncated,
                    looks_noise,
                    missing_text,
                    model = %self.model_name,
                    "retrying OCR page with fallback prompt for Gemini",
                );
                continue;
            }

            if plain_text.is_some() {
                debug!(
                    doc_id = %input.id,
                    page = page_index,
                    model = %self.model_name,
                    prompt_tokens = usage.prompt_tokens,
                    completion_tokens = usage.completion_tokens,
                    total_tokens = usage.total_tokens,
                    "gemini ocr invocation usage"
                );
            }

            if let Some(text) = plain_text {
                return Ok(OcrPageResult {
                    page_index,
                    render_width,
                    render_height,
                    ocr_width: prepared.width,
                    ocr_height: prepared.height,
                    mime_type: prepared.mime_type,
                    plain_text: Some(text),
                    spans: Vec::new(),
                    usage: Some(usage),
                });
            }
        }

        Err(OcrError::UnsupportedInput {
            id: input.id.clone(),
            mime_type: input.mime_type.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct GeminiOcr {
    engine: OcrEngine<GeminiProvider>,
}

impl GeminiOcr {
    pub fn from_env(model: impl Into<String>, max_output_tokens: u32) -> Result<Self, OcrError> {
        let provider = GeminiProvider::from_env(model, max_output_tokens)?;
        Ok(Self {
            engine: OcrEngine::new(provider),
        })
    }
}

#[async_trait]
impl OcrService for GeminiOcr {
    async fn run_batch(
        &self,
        inputs: Vec<OcrInput>,
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError> {
        self.engine.run_batch(inputs, config).await
    }
}

fn fallback_token_target(max_tokens: u32) -> u32 {
    max_tokens.min(2048).max(512)
}

fn build_generation_config(max_tokens: u32) -> GenerationConfig {
    GenerationConfig::builder()
        .temperature(0.0)
        .max_output_tokens(max_tokens)
        .build()
}

fn usage_to_token_usage(usage: &AiUsage) -> TokenUsage {
    let prompt = usage.input_tokens();
    let completion = usage.output_tokens();
    let total = usage.total_tokens();
    let tool_tokens = usage.tool_tokens_by_modality.values().copied().sum::<u64>();

    TokenUsage {
        prompt_tokens: (prompt > 0).then_some(prompt),
        completion_tokens: (completion > 0).then_some(completion),
        total_tokens: (total > 0).then_some(total),
        cache_creation_tokens: usage.cache_creation_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        reasoning_tokens: usage.thoughts_tokens,
        tool_prompt_tokens: (tool_tokens > 0).then_some(tool_tokens),
        thoughts_tokens: usage.thoughts_tokens,
    }
}

fn collect_pages(input: &OcrInput, config: &OcrConfig) -> Result<Vec<PdfPageImage>, OcrError> {
    match input.mime_type {
        OcrMimeType::Pdf => Ok(render_pdf_to_png_images(
            input.bytes.as_ref(),
            config.render_width,
        )?),
        OcrMimeType::Png => Ok(vec![png_bytes_to_page(&input.id, input.bytes.as_ref())?]),
        OcrMimeType::Jpeg => Ok(vec![jpeg_bytes_to_page(&input.id, input.bytes.as_ref())?]),
    }
}

fn prepare_image_for_ocr(
    input_id: &str,
    page_index: usize,
    image: &PdfPageImage,
    max_edge: u32,
) -> Result<PreparedOcrImage, OcrError> {
    let mut dyn_image =
        image::load_from_memory(&image.png_data).map_err(|source| OcrError::ImageDecode {
            id: input_id.to_string(),
            page_index,
            source,
        })?;

    let max_edge = max_edge.max(1);
    let (width, height) = dyn_image.dimensions();
    let longest_edge = width.max(height);

    if longest_edge > max_edge {
        let scale = max_edge as f32 / longest_edge as f32;
        let target_width = ((width as f32 * scale).round() as u32).max(1);
        let target_height = ((height as f32 * scale).round() as u32).max(1);
        dyn_image = dyn_image.resize(target_width, target_height, FilterType::CatmullRom);
    }

    let (final_width, final_height) = dyn_image.dimensions();

    let mut buffer = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut buffer, 95);
    encoder
        .encode_image(&dyn_image)
        .map_err(|source| OcrError::ImageEncode {
            id: input_id.to_string(),
            page_index,
            source,
        })?;

    Ok(PreparedOcrImage {
        data: buffer,
        width: final_width,
        height: final_height,
        mime_type: "image/jpeg",
    })
}

fn flatten_message_content_text(content: &DeepInfraMessageContent) -> Option<String> {
    match content {
        DeepInfraMessageContent::Text(text) => Some(text.clone()),
        DeepInfraMessageContent::Parts(parts) => {
            let mut buffer = String::new();
            for part in parts {
                if let DeepInfraMessageContentPart::Text { text } = part {
                    if !buffer.is_empty() {
                        buffer.push('\n');
                    }
                    buffer.push_str(text);
                }
            }

            if buffer.is_empty() {
                None
            } else {
                Some(buffer)
            }
        }
    }
}

fn parse_deepseek_output(raw: &str, width: u32, height: u32) -> (String, Vec<OcrSpan>) {
    static BBOX_RE: OnceLock<Regex> = OnceLock::new();
    let bbox_re = BBOX_RE.get_or_init(|| {
        Regex::new(r"^([A-Za-z_]+)\[\[(\d+),\s*(\d+),\s*(\d+),\s*(\d+)\]\]$").unwrap()
    });

    let mut plain_lines = Vec::new();
    let mut spans = Vec::new();
    let mut current: Option<SpanBuilder> = None;

    for line in raw.lines() {
        let trimmed = line.trim_end();
        if trimmed.trim().is_empty() {
            plain_lines.push(String::new());
            if let Some(span) = current.as_mut() {
                span.lines.push(String::new());
            }
            continue;
        }

        if let Some(caps) = bbox_re.captures(trimmed.trim()) {
            if let Some(span) = current.take() {
                finalize_span(span, &mut spans, width, height);
            }

            let coords = (
                caps.get(2).and_then(|v| v.as_str().parse::<u32>().ok()),
                caps.get(3).and_then(|v| v.as_str().parse::<u32>().ok()),
                caps.get(4).and_then(|v| v.as_str().parse::<u32>().ok()),
                caps.get(5).and_then(|v| v.as_str().parse::<u32>().ok()),
            );

            let bbox = match coords {
                (Some(x1), Some(y1), Some(x2), Some(y2)) => Some([x1, y1, x2, y2]),
                _ => None,
            };

            current = Some(SpanBuilder {
                kind: caps[1].to_string(),
                bbox,
                lines: Vec::new(),
            });

            continue;
        }

        plain_lines.push(trimmed.to_string());

        if let Some(span) = current.as_mut() {
            span.lines.push(trimmed.to_string());
        } else {
            current = Some(SpanBuilder {
                kind: "text".to_string(),
                bbox: None,
                lines: vec![trimmed.to_string()],
            });
        }
    }

    if let Some(span) = current.take() {
        finalize_span(span, &mut spans, width, height);
    }

    let plain_text = plain_lines.join("\n");

    (plain_text, spans)
}

struct SpanBuilder {
    kind: String,
    bbox: Option<[u32; 4]>,
    lines: Vec<String>,
}

fn finalize_span(span: SpanBuilder, spans: &mut Vec<OcrSpan>, width: u32, height: u32) {
    let joined = span.lines.join("\n");
    let text = joined.trim_matches('\n').trim_end().to_string();
    if text.trim().is_empty() {
        return;
    }

    let bbox_norm = span.bbox.map(|[x1, y1, x2, y2]| {
        [
            x1 as f32 / width.max(1) as f32,
            y1 as f32 / height.max(1) as f32,
            x2 as f32 / width.max(1) as f32,
            y2 as f32 / height.max(1) as f32,
        ]
    });

    spans.push(OcrSpan {
        kind: span.kind,
        bbox: span.bbox,
        bbox_norm,
        text,
    });
}

fn png_bytes_to_page(id: &str, bytes: &[u8]) -> Result<PdfPageImage, OcrError> {
    let cursor = Cursor::new(bytes);
    let decoder = PngDecoder::new(cursor).map_err(|source| OcrError::ImageDecode {
        id: id.to_string(),
        page_index: 0,
        source,
    })?;
    let (width, height) = decoder.dimensions();

    Ok(PdfPageImage {
        page_index: 0,
        width,
        height,
        png_data: bytes.to_vec(),
    })
}

fn jpeg_bytes_to_page(id: &str, bytes: &[u8]) -> Result<PdfPageImage, OcrError> {
    let image = image::load_from_memory(bytes).map_err(|source| OcrError::ImageDecode {
        id: id.to_string(),
        page_index: 0,
        source,
    })?;

    let (width, height) = image.dimensions();
    let rgba = image.to_rgba8();
    let mut png_data = Vec::new();
    {
        let encoder = PngEncoder::new(&mut png_data);
        encoder
            .write_image(rgba.as_raw(), width, height, ColorType::Rgba8.into())
            .map_err(|source| OcrError::ImageEncode {
                id: id.to_string(),
                page_index: 0,
                source,
            })?;
    }

    Ok(PdfPageImage {
        page_index: 0,
        width,
        height,
        png_data,
    })
}

fn text_is_numeric_noise(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut has_alpha = false;
    let mut has_digit = false;

    for ch in trimmed.chars() {
        if ch.is_ascii_alphabetic() {
            has_alpha = true;
            break;
        } else if ch.is_ascii_digit() {
            has_digit = true;
        } else if !(ch.is_whitespace() || matches!(ch, '.' | ',' | '-' | '_' | ':' | ';')) {
            return false;
        }
    }

    !has_alpha && has_digit
}

fn build_grounded_prompt(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.starts_with("<image>") {
        trimmed.to_string()
    } else if trimmed.is_empty() {
        "<image>".to_string()
    } else {
        format!("<image>\n{}", trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_ox::usage::Modality;

    #[test]
    fn grounded_prompt_prepends_tag_once() {
        assert_eq!(
            build_grounded_prompt("Extract table data."),
            "<image>\nExtract table data."
        );
        assert_eq!(
            build_grounded_prompt("<image>\nAlready tagged."),
            "<image>\nAlready tagged."
        );
        assert_eq!(build_grounded_prompt(""), "<image>");
    }

    #[test]
    fn usage_conversion_accumulates_totals() {
        let mut usage = AiUsage::new();
        usage.requests = 1;
        usage.input_tokens_by_modality.insert(Modality::Text, 42);
        usage.output_tokens_by_modality.insert(Modality::Text, 21);
        usage
            .tool_tokens_by_modality
            .insert(Modality::Other("tools".into()), 5);
        usage.cache_creation_tokens = Some(3);
        usage.cache_read_tokens = Some(2);
        usage.thoughts_tokens = Some(4);

        let converted = usage_to_token_usage(&usage);
        assert_eq!(converted.prompt_tokens, Some(42));
        assert_eq!(converted.completion_tokens, Some(21));
        assert_eq!(converted.total_tokens, Some(42 + 21 + 4));
        assert_eq!(converted.tool_prompt_tokens, Some(5));
        assert_eq!(converted.cache_creation_tokens, Some(3));
        assert_eq!(converted.cache_read_tokens, Some(2));
        assert_eq!(converted.thoughts_tokens, Some(4));
    }
}
