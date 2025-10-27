use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bon::Builder;
use deepinfra_ox::{
    ChatRequest as DeepInfraChatRequest, DeepInfra, ImageUrl, Message as DeepInfraMessage,
    MessageContent as DeepInfraMessageContent, MessageContentPart as DeepInfraMessageContentPart,
    TokenUsage,
};
use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::PngDecoder;
use image::imageops::FilterType;
use image::{GenericImageView, ImageDecoder};
use regex::Regex;
use serde::Serialize;
use std::io::Cursor;
use std::sync::OnceLock;
use thiserror::Error;

use super::{PdfPageImage, render_pdf_to_png_images};

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
    #[error("unsupported input format for OCR: {0}")]
    UnsupportedInput(PathBuf),
    #[error("failed to decode image data for {path} (page {page_index}): {source}")]
    ImageDecode {
        path: PathBuf,
        page_index: usize,
        #[source]
        source: image::ImageError,
    },
    #[error("failed to encode image data for {path} (page {page_index}): {source}")]
    ImageEncode {
        path: PathBuf,
        page_index: usize,
        #[source]
        source: image::ImageError,
    },
    #[error("missing DEEPINFRA_API_KEY environment variable")]
    MissingApiKey,
}

/// Run OCR for the given document path using the supplied configuration.
pub async fn run_ocr_document(
    path: &Path,
    config: &OcrConfig,
) -> Result<Vec<OcrPageResult>, OcrError> {
    let bytes = std::fs::read(path).map_err(|source| OcrError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());

    let pages = match ext.as_deref() {
        Some("pdf") => render_pdf_to_png_images(&bytes, config.render_width)?,
        Some("png") => vec![png_bytes_to_page(path, bytes)?],
        _ => return Err(OcrError::UnsupportedInput(path.to_path_buf())),
    };

    let client = DeepInfra::load_from_env().map_err(|_| OcrError::MissingApiKey)?;

    let mut results = Vec::with_capacity(pages.len());

    for page in pages {
        let prepared = prepare_image_for_ocr(path, page.page_index, &page, config.image_max_edge)?;

        let mut parts = Vec::with_capacity(2);
        let mut image_url = ImageUrl::new(format!(
            "data:{};base64,{}",
            prepared.mime_type,
            BASE64_STANDARD.encode(&prepared.data)
        ));

        if let Some(detail) = config.detail.as_ref() {
            image_url = image_url.with_detail(detail.to_string());
        }

        parts.push(DeepInfraMessageContentPart::ImageUrl { image_url });
        parts.push(DeepInfraMessageContentPart::Text {
            text: "<|grounding|>Convert the document to markdown.".to_string(),
        });

        let message = DeepInfraMessage::user(DeepInfraMessageContent::Parts(parts));
        let mut request = DeepInfraChatRequest::new(config.model.clone(), vec![message]);
        request.max_tokens = Some(config.max_tokens);
        request.temperature = Some(0.0);

        let response = client.send(&request).await?;

        let content = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.clone());

        let raw_text = content
            .as_ref()
            .and_then(|content| flatten_message_content_text(content));

        let (plain_text_str, spans) = match raw_text.as_deref() {
            Some(raw) => parse_deepseek_output(raw, prepared.width, prepared.height),
            None => (String::new(), Vec::new()),
        };

        let plain_text = if plain_text_str.trim().is_empty() {
            None
        } else {
            Some(plain_text_str)
        };

        results.push(OcrPageResult {
            page_index: page.page_index,
            render_width: page.width,
            render_height: page.height,
            ocr_width: prepared.width,
            ocr_height: prepared.height,
            mime_type: prepared.mime_type,
            plain_text,
            spans,
            usage: response.usage.clone(),
        });
    }

    Ok(results)
}

fn prepare_image_for_ocr(
    input_path: &Path,
    page_index: usize,
    image: &PdfPageImage,
    max_edge: u32,
) -> Result<PreparedOcrImage, OcrError> {
    let mut dyn_image =
        image::load_from_memory(&image.png_data).map_err(|source| OcrError::ImageDecode {
            path: input_path.to_path_buf(),
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
            path: input_path.to_path_buf(),
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

fn png_bytes_to_page(path: &Path, bytes: Vec<u8>) -> Result<PdfPageImage, OcrError> {
    let cursor = Cursor::new(bytes.as_slice());
    let decoder = PngDecoder::new(cursor).map_err(|source| OcrError::ImageDecode {
        path: path.to_path_buf(),
        page_index: 0,
        source,
    })?;
    let (width, height) = decoder.dimensions();

    Ok(PdfPageImage {
        page_index: 0,
        width,
        height,
        png_data: bytes,
    })
}
