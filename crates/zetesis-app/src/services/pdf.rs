use std::env;
use std::path::{Path, PathBuf};

use pdfium_render::prelude::{Pdfium, PdfiumError};
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

/// Extracts UTF-8 text from a PDF byte slice using Pdfium while preserving Polish diacritics.
pub fn extract_text_from_pdf(bytes: &[u8]) -> Result<String, PdfTextError> {
    let pdfium = load_pdfium()?;
    let document = pdfium
        .load_pdf_from_byte_slice(bytes, None)
        .map_err(PdfTextError::Document)?;

    let mut buffer = String::new();

    for (index, page) in document.pages().iter().enumerate() {
        let page_text = page
            .text()
            .map_err(|source| PdfTextError::PageText {
                page_index: index,
                source,
            })?
            .all();

        if buffer.is_empty() {
            buffer.push_str(&page_text);
        } else if !page_text.is_empty() {
            buffer.push_str("\n\n");
            buffer.push_str(&page_text);
        }
    }

    Ok(buffer)
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
            if let Some(result) = try_bind_from_env(var) {
                if result.is_ok() {
                    return result;
                }
            }
        }

        for candidate in candidate_paths() {
            if let Some(result) = try_bind_from_path(candidate) {
                if result.is_ok() {
                    return result;
                }
            }
        }

        match Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path("./")) {
            Ok(bindings) => return Ok(Pdfium::new(bindings)),
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
