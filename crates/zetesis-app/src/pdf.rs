//! PDF helpers for extracting text and rendering page images.

use std::cmp::Ordering;
use std::env;
use std::path::{Path, PathBuf};

use image::codecs::png::PngEncoder;
use image::{ColorType, ImageEncoder};
use pdfium_render::prelude::{PdfRect, PdfRenderConfig, Pdfium, PdfiumError};
use thiserror::Error;

/// Errors emitted while extracting text from PDF documents.
#[derive(Debug, Error)]
pub enum PdfTextError {
    #[error("failed to load Pdfium runtime: {0}")]
    Library(#[from] PdfiumError),

    #[error("failed to load PDF document: {0}")]
    Document(#[source] PdfiumError),

    #[error("failed to extract text for page {page_index}: {source}")]
    PageText {
        page_index: usize,
        #[source]
        source: PdfiumError,
    },
}

/// Errors emitted while rendering PDF pages into PNG images.
#[derive(Debug, Error)]
pub enum PdfRenderError {
    #[error("failed to load Pdfium runtime: {0}")]
    Library(#[from] PdfiumError),

    #[error("failed to load PDF document: {0}")]
    Document(#[source] PdfiumError),

    #[error("failed to render page {page_index}: {source}")]
    PageRender {
        page_index: usize,
        #[source]
        source: PdfiumError,
    },

    #[error("failed to encode page {page_index} as PNG: {source}")]
    Encode {
        page_index: usize,
        #[source]
        source: image::ImageError,
    },
}

/// In-memory representation of a rendered PDF page.
#[derive(Debug, Clone)]
pub struct PdfPageImage {
    pub page_index: usize,
    pub width: u32,
    pub height: u32,
    pub png_data: Vec<u8>,
}

/// Extracts UTF-8 text from a PDF byte slice using Pdfium while preserving Polish diacritics.
pub fn extract_text_from_pdf(bytes: &[u8]) -> Result<String, PdfTextError> {
    let pdfium = load_pdfium()?;
    let document = pdfium
        .load_pdf_from_byte_slice(bytes, None)
        .map_err(PdfTextError::Document)?;

    let mut buffer = String::new();

    for (page_index, page) in document.pages().iter().enumerate() {
        let mut page_text = String::new();
        render_page(&mut page_text, page_index, &page)?;

        if page_text.is_empty() {
            continue;
        }

        if buffer.is_empty() {
            buffer.push_str(&page_text);
        } else {
            buffer.push_str("\n\n");
            buffer.push_str(&page_text);
        }
    }

    Ok(buffer)
}

/// Renders each page of a PDF into a PNG image with the requested target width.
pub fn render_pdf_to_png_images(
    bytes: &[u8],
    target_width: u32,
) -> Result<Vec<PdfPageImage>, PdfRenderError> {
    let pdfium = load_pdfium()?;
    let document = pdfium
        .load_pdf_from_byte_slice(bytes, None)
        .map_err(PdfRenderError::Document)?;

    let mut images = Vec::with_capacity(document.pages().len() as usize);

    for (page_index, page) in document.pages().iter().enumerate() {
        let render_config = PdfRenderConfig::new().set_target_width(target_width as i32);

        let bitmap = page
            .render_with_config(&render_config)
            .map_err(|source| PdfRenderError::PageRender { page_index, source })?;

        let width = bitmap.width() as u32;
        let height = bitmap.height() as u32;
        let rgba = bitmap.as_rgba_bytes();

        let mut encoded = Vec::new();
        let encoder = PngEncoder::new(&mut encoded);
        encoder
            .write_image(&rgba, width, height, ColorType::Rgba8.into())
            .map_err(|source| PdfRenderError::Encode { page_index, source })?;

        images.push(PdfPageImage {
            page_index,
            width,
            height,
            png_data: encoded,
        });
    }

    Ok(images)
}

fn render_page(
    out: &mut String,
    page_index: usize,
    page: &pdfium_render::prelude::PdfPage,
) -> Result<(), PdfTextError> {
    let text = page
        .text()
        .map_err(|source| PdfTextError::PageText { page_index, source })?;

    let chars = text.chars();
    if chars.is_empty() {
        return Ok(());
    }

    let mut glyphs = Vec::with_capacity(chars.len());

    for ch in chars.iter() {
        let Some(value) = ch.unicode_char() else {
            continue;
        };

        // Skip nulls and carriage returns; they do not carry visible content.
        if value == '\u{0}' || value == '\r' {
            continue;
        }

        let rect = ch
            .tight_bounds()
            .or_else(|primary| ch.loose_bounds().map_err(|_| primary))
            .map_err(|source| PdfTextError::PageText { page_index, source })?;

        let baseline = ch
            .origin_y()
            .map_err(|source| PdfTextError::PageText { page_index, source })?
            .value;

        glyphs.push(Glyph {
            ch: value,
            rect,
            baseline,
        });
    }

    if glyphs.is_empty() {
        return Ok(());
    }

    let mut heights = Vec::with_capacity(glyphs.len());
    for glyph in &glyphs {
        let height = glyph.height();
        if height.is_finite() && height > 0.0 {
            heights.push(height);
        }
    }

    let median_height = median(heights).unwrap_or(8.0);
    let line_threshold = (median_height * 0.8).max(1.0);
    let paragraph_threshold = (median_height * 1.8).max(line_threshold * 1.5);

    let mut gap_samples = Vec::new();
    for pair in glyphs.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        if same_line(next, prev, line_threshold) {
            let gap = next.left() - prev.right();
            if gap.is_finite() && gap > 0.0 {
                gap_samples.push(gap);
            }
        }
    }

    let median_gap = median(gap_samples);
    let default_gap = (median_height * 0.45).max(0.75);
    let space_threshold = median_gap
        .map(|gap| gap.max(default_gap * 0.8))
        .unwrap_or(default_gap);

    let mut prev: Option<&Glyph> = None;

    for glyph in &glyphs {
        if glyph.ch == '\n' {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            prev = None;
            continue;
        }

        if let Some(prev_glyph) = prev {
            let baseline_delta = (glyph.baseline - prev_glyph.baseline).abs();
            if baseline_delta > line_threshold {
                if !out.ends_with('\n') {
                    out.push('\n');
                }
                if baseline_delta > paragraph_threshold && !out.ends_with("\n\n") {
                    out.push('\n');
                }
            } else {
                let gap = glyph.left() - prev_glyph.right();
                if gap > space_threshold
                    && !prev_glyph.ch.is_whitespace()
                    && !glyph.ch.is_whitespace()
                {
                    out.push(' ');
                }
            }
        }

        out.push(glyph.ch);
        prev = Some(glyph);
    }

    Ok(())
}

struct Glyph {
    ch: char,
    rect: PdfRect,
    baseline: f32,
}

impl Glyph {
    #[inline]
    fn left(&self) -> f32 {
        self.rect.left().value
    }

    #[inline]
    fn right(&self) -> f32 {
        self.rect.right().value
    }

    #[inline]
    fn height(&self) -> f32 {
        (self.rect.top().value - self.rect.bottom().value).abs()
    }
}

fn same_line(current: &Glyph, previous: &Glyph, threshold: f32) -> bool {
    (current.baseline - previous.baseline).abs() <= threshold
}

fn median(values: Vec<f32>) -> Option<f32> {
    if values.is_empty() {
        return None;
    }

    let mut values = values
        .into_iter()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();

    if values.is_empty() {
        return None;
    }

    values.sort_by(|a, b| partial_cmp(*a, *b));
    let mid = values.len() / 2;
    Some(values[mid])
}

fn partial_cmp(a: f32, b: f32) -> Ordering {
    a.partial_cmp(&b).unwrap_or_else(|| {
        if a.is_nan() && b.is_nan() {
            Ordering::Equal
        } else if a.is_nan() {
            Ordering::Greater
        } else if b.is_nan() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    })
}

fn load_pdfium() -> Result<Pdfium, PdfiumError> {
    #[cfg(target_arch = "wasm32")]
    {
        return Pdfium::bind_to_system_library().map(Pdfium::new);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Some(result) = try_bind_from_env("PDFIUM_LIBRARY_PATH") {
            return result;
        }

        for var in [
            "PDFIUM_LIB_DIR",
            "PDFIUM_DYNAMIC_LIB_PATH",
            "PDFIUM_LIBRARY_DIR",
        ] {
            if let Some(result) = try_bind_from_env(var)
                && result.is_ok()
            {
                return result;
            }
        }

        for candidate in candidate_paths() {
            if let Some(result) = try_bind_from_path(candidate)
                && result.is_ok()
            {
                return result;
            }
        }

        match Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./")) {
            Ok(bindings) => Ok(Pdfium::new(bindings)),
            Err(primary_err) => match Pdfium::bind_to_system_library() {
                Ok(bindings) => Ok(Pdfium::new(bindings)),
                Err(_) => Err(primary_err),
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn try_bind_from_env(var: &str) -> Option<Result<Pdfium, PdfiumError>> {
    let value = env::var_os(var)?;
    let path = PathBuf::from(&value);
    try_bind_from_path(path)
}

#[cfg(not(target_arch = "wasm32"))]
fn try_bind_from_path(path: impl AsRef<Path>) -> Option<Result<Pdfium, PdfiumError>> {
    let path = path.as_ref();
    if path.is_dir() {
        let lib_path = Pdfium::pdfium_platform_library_name_at_path(path);
        Some(Pdfium::bind_to_library(lib_path).map(Pdfium::new))
    } else if path.exists() {
        Some(Pdfium::bind_to_library(path).map(Pdfium::new))
    } else {
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
const DEFAULT_PDFIUM_LOCATIONS: &[&str] = &[
    "third_party/pdfium/lib/libpdfium.so",
    "third_party/pdfium/libpdfium.so",
    "pdfium/lib/libpdfium.so",
    "pdfium/libpdfium.so",
    "libpdfium.so",
];

#[cfg(not(target_arch = "wasm32"))]
fn candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf);

    for candidate in DEFAULT_PDFIUM_LOCATIONS {
        paths.push(PathBuf::from(candidate));
        if let Some(root) = &workspace_root {
            paths.push(root.join(candidate));
        }
    }

    paths
}
