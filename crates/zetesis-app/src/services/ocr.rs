use std::{
    io::Cursor,
    num::NonZeroU32,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bon::Builder;
use deepinfra_ox::{
    ChatRequest as DeepInfraChatRequest, DeepInfra, ImageUrl, Message as DeepInfraMessage,
    MessageContent as DeepInfraMessageContent, MessageContentPart as DeepInfraMessageContentPart,
    TokenUsage,
};
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

use super::{PdfPageImage, render_pdf_to_png_images};

type OcrRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

fn deepinfra_rate_limiter() -> &'static Arc<OcrRateLimiter> {
    static LIMITER: OnceLock<Arc<OcrRateLimiter>> = OnceLock::new();
    LIMITER.get_or_init(|| {
        let quota = Quota::per_second(NonZeroU32::new(200).expect("non-zero quota"))
            .allow_burst(NonZeroU32::new(200).expect("non-zero burst"));
        Arc::new(RateLimiter::direct(quota))
    })
}

#[async_trait]
pub trait OcrService: Send + Sync {
    async fn run_batch(
        &self,
        inputs: &[OcrInput],
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError>;

    async fn run_single(
        &self,
        input: &OcrInput,
        config: &OcrConfig,
    ) -> Result<OcrDocumentResult, OcrError> {
        let results = self.run_batch(std::slice::from_ref(input), config).await?;
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
pub struct DeepInfraOcr {
    client: Arc<DeepInfra>,
    limiter: Arc<OcrRateLimiter>,
}

impl DeepInfraOcr {
    pub fn from_env() -> Result<Self, OcrError> {
        let client = DeepInfra::load_from_env().map_err(|_| OcrError::MissingApiKey)?;
        Ok(Self {
            client: Arc::new(client),
            limiter: Arc::clone(deepinfra_rate_limiter()),
        })
    }
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
    pub bytes: Vec<u8>,
    pub mime_type: OcrMimeType,
}

#[derive(Debug, Clone, Serialize)]
pub struct OcrDocumentResult {
    pub id: String,
    pub pages: Vec<OcrPageResult>,
}

/// Errors produced by the OCR service.
#[derive(Debug, Error)]
pub enum OcrError {
    #[error(transparent)]
    Pdf(#[from] crate::services::PdfTextError),
    #[error(transparent)]
    PdfRender(#[from] crate::services::PdfRenderError),
    #[error(transparent)]
    DeepInfra(#[from] deepinfra_ox::DeepInfraRequestError),
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
    MissingApiKey,
}

#[async_trait]
impl OcrService for DeepInfraOcr {
    async fn run_batch(
        &self,
        inputs: &[OcrInput],
        config: &OcrConfig,
    ) -> Result<Vec<OcrDocumentResult>, OcrError> {
        let mut docs = Vec::with_capacity(inputs.len());

        for input in inputs {
            let pages = collect_pages(input, config)?;
            let mut page_results = Vec::with_capacity(pages.len());

            for page in pages {
                let result = self.process_page(input, config, &page).await?;
                page_results.push(result);
            }

            docs.push(OcrDocumentResult {
                id: input.id.clone(),
                pages: page_results,
            });
        }

        Ok(docs)
    }
}

impl DeepInfraOcr {
    async fn process_page(
        &self,
        input: &OcrInput,
        config: &OcrConfig,
        page: &PdfPageImage,
    ) -> Result<OcrPageResult, OcrError> {
        let prepared =
            prepare_image_for_ocr(&input.id, page.page_index, page, config.image_max_edge)?;

        let default_prompt = config
            .detail
            .clone()
            .unwrap_or_else(|| "<|grounding|>Convert the document to markdown.".to_string());
        let fallback_prompt = "<|grounding|>OCR this image.".to_string();

        let mut attempt = 0;
        let mut prompt = default_prompt.clone();
        let mut max_tokens = config.max_tokens;

        loop {
            let response = self
                .invoke_model(&prepared, &prompt, max_tokens, &config.model)
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
                    page = page.page_index,
                    truncated,
                    looks_noise,
                    "retrying OCR page with fallback prompt",
                );
                attempt += 1;
                prompt = fallback_prompt.clone();
                max_tokens = max_tokens.min(2048).max(512);
                continue;
            }

            return Ok(OcrPageResult {
                page_index: page.page_index,
                render_width: page.width,
                render_height: page.height,
                ocr_width: prepared.width,
                ocr_height: prepared.height,
                mime_type: prepared.mime_type,
                plain_text,
                spans,
                usage,
            });
        }
    }

    async fn invoke_model(
        &self,
        prepared: &PreparedOcrImage,
        prompt: &str,
        max_tokens: u32,
        model: &str,
    ) -> Result<deepinfra_ox::ChatCompletionResponse, OcrError> {
        let mut parts = Vec::with_capacity(2);
        let image_url = ImageUrl::new(format!(
            "data:{};base64,{}",
            prepared.mime_type,
            BASE64_STANDARD.encode(&prepared.data)
        ));

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

fn collect_pages(input: &OcrInput, config: &OcrConfig) -> Result<Vec<PdfPageImage>, OcrError> {
    match input.mime_type {
        OcrMimeType::Pdf => Ok(render_pdf_to_png_images(&input.bytes, config.render_width)?),
        OcrMimeType::Png => Ok(vec![png_bytes_to_page(&input.id, &input.bytes)?]),
        OcrMimeType::Jpeg => Ok(vec![jpeg_bytes_to_page(&input.id, &input.bytes)?]),
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
            // presence of other symbols means it's probably real text
            return false;
        }
    }

    !has_alpha && has_digit
}
