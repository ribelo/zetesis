//! Application-level error type shared across binaries and services.

use std::path::PathBuf;

use inquire::InquireError;
use thiserror::Error;

use crate::config;
use crate::index::milli::MilliBootstrapError;
use crate::index::writer::IndexWriteError;
use crate::ingestion;
use crate::paths::PathError;
use crate::pdf::{PdfRenderError, PdfTextError};
use crate::server;
use crate::services::blob_store::BlobError;
use crate::services::{
    GenerationJobStoreError, OcrError, PipelineError, ReaperError, StructuredExtractError,
};

#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum AppError {
    #[error(transparent)]
    ConfigLoad(#[from] config::AppConfigError),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("blob store error: {0}")]
    BlobStore(#[from] BlobError),
    #[error(transparent)]
    Server(#[from] server::ServerError),
    #[error(transparent)]
    Ingest(#[from] ingestion::IngestorError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Pdf(#[from] PdfTextError),
    #[error(transparent)]
    PdfRender(#[from] PdfRenderError),
    #[error(transparent)]
    Ocr(#[from] OcrError),
    #[error(transparent)]
    Structured(#[from] StructuredExtractError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    Milli(#[from] Box<milli::Error>),
    #[error(transparent)]
    IndexWrite(#[from] IndexWriteError),
    #[error(transparent)]
    Heed(#[from] Box<heed::Error>),
    #[error(transparent)]
    MilliBootstrap(#[from] MilliBootstrapError),
    #[error(transparent)]
    Jobs(#[from] Box<GenerationJobStoreError>),
    #[error(transparent)]
    Reaper(#[from] ReaperError),
    #[error("failed to read input file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to resolve current working directory: {0}")]
    WorkingDir(#[source] std::io::Error),
    #[error("failed to derive document id from {path}")]
    #[allow(dead_code)]
    InvalidDocId { path: PathBuf },
    #[error("invalid index name `{name}` (empty or contains path separators)")]
    InvalidIndexName { name: String },
    #[error("index `{index}` not found at {path}")]
    MissingIndex { index: String, path: PathBuf },
    #[error("backup destination already exists: {path}")]
    BackupDestinationExists { path: PathBuf },
    #[error("backup source `{path}` does not exist or is not a directory")]
    BackupSourceMissing { path: PathBuf },
    #[error("purge cancelled for index `{index}`")]
    PurgeConfirmationCancelled { index: String },
    #[error("confirmation token mismatch for index `{index}`")]
    PurgeConfirmationRejected { index: String },
    #[error("failed to read purge confirmation input: {source}")]
    PurgePromptFailed {
        #[source]
        source: InquireError,
    },
    #[error("refusing to recover into existing index `{index}` without --force")]
    RecoverRequiresForce { index: String },
    #[error("document `{id}` not found in index `{index}`")]
    DocumentNotFound { index: String, id: String },
    #[error("embedder `{name}` not configured for index `{index}`")]
    EmbedderNotFound { index: String, name: String },
    #[error("embedder `{name}` in index `{index}` does not support vector search via CLI")]
    UnsupportedEmbedder { index: String, name: String },
    #[error("invalid filter expression `{expression}`: {reason}")]
    InvalidFilter { expression: String, reason: String },
    #[error("invalid sort specification `{spec}`: {reason}")]
    InvalidSort { spec: String, reason: String },
}

impl From<milli::Error> for AppError {
    fn from(e: milli::Error) -> Self {
        AppError::Milli(Box::new(e))
    }
}

impl From<heed::Error> for AppError {
    fn from(e: heed::Error) -> Self {
        AppError::Heed(Box::new(e))
    }
}

impl From<GenerationJobStoreError> for AppError {
    fn from(e: GenerationJobStoreError) -> Self {
        AppError::Jobs(Box::new(e))
    }
}
