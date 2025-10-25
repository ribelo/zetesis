use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use bon::Builder;
use thiserror::Error;
use url::ParseError;

/// Shared builder options for KIO scrapers.
#[derive(Debug, Clone, Builder)]
pub struct KioScrapeOptions {
    #[builder(into)]
    pub output_dir: PathBuf,
    pub limit: Option<usize>,
    #[builder(default = NonZeroUsize::new(4).unwrap())]
    pub worker_count: NonZeroUsize,
    #[builder(default = 64)]
    pub channel_capacity: usize,
}

/// Basic KIO metadata emitted during discovery.
#[derive(Debug, Clone)]
pub struct KioDocumentMetadata {
    pub doc_id: String,
    pub sygnatura: Option<String>,
    pub decision_type: Option<String>,
}

/// Stream events emitted by KIO scrapers.
#[derive(Debug, Clone)]
pub enum KioEvent {
    DiscoveryStarted {
        limit: Option<usize>,
    },
    Discovered {
        ordinal: usize,
        page: usize,
        total_hint: Option<usize>,
        metadata: KioDocumentMetadata,
    },
    WorkerStarted {
        worker: usize,
        doc_id: String,
    },
    DownloadSkipped {
        doc_id: String,
    },
    DownloadCompleted {
        doc_id: String,
        bytes: usize,
    },
    Completed {
        summary: KioScraperSummary,
    },
}

/// Final summary produced after scraping completes.
#[derive(Debug, Clone, Copy)]
pub struct KioScraperSummary {
    pub discovered: usize,
    pub downloaded: usize,
    pub skipped_existing: usize,
    pub total_available_hint: Option<usize>,
}

/// Error type shared across scrapers.
#[derive(Debug, Error, Clone)]
pub enum KioScrapeError {
    #[error("invalid base URL `{0}`")]
    InvalidBaseUrl(String),
    #[error("failed to join `{path}` onto base URL: {source}")]
    UrlJoin {
        path: String,
        #[source]
        source: Arc<ParseError>,
    },
    #[error("request error during `{stage}`: {source}")]
    Request {
        stage: &'static str,
        #[source]
        source: Arc<reqwest::Error>,
    },
    #[error("unexpected HTTP status {status} during `{stage}`")]
    HttpStatus { stage: &'static str, status: u16 },
    #[error("failed to read HTTP body during `{stage}`: {source}")]
    Body {
        stage: &'static str,
        #[source]
        source: Arc<reqwest::Error>,
    },
    #[error("parse error during `{stage}`: {message}")]
    Parse {
        stage: &'static str,
        message: String,
    },
    #[error("JSON decode error during `{stage}`: {source}")]
    Json {
        stage: &'static str,
        #[source]
        source: Arc<serde_json::Error>,
    },
    #[error("bounded document channel closed unexpectedly")]
    ChannelClosed,
    #[error("filesystem error: {source}")]
    Io {
        #[from]
        source: Arc<std::io::Error>,
    },
}

impl From<std::io::Error> for KioScrapeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(value),
        }
    }
}

impl KioScrapeError {
    pub fn request(stage: &'static str, error: reqwest::Error) -> Self {
        Self::Request {
            stage,
            source: Arc::new(error),
        }
    }

    pub fn body(stage: &'static str, error: reqwest::Error) -> Self {
        Self::Body {
            stage,
            source: Arc::new(error),
        }
    }

    pub fn parse(stage: &'static str, message: impl Into<String>) -> Self {
        Self::Parse {
            stage,
            message: message.into(),
        }
    }

    pub fn json(stage: &'static str, error: serde_json::Error) -> Self {
        Self::Json {
            stage,
            source: Arc::new(error),
        }
    }
}
