use std::{num::NonZeroUsize, sync::Arc};

use bon::Builder;
use thiserror::Error;
use url::ParseError;

use crate::services::BlobStore;

/// Shared builder options for KIO scrapers.
#[derive(Clone, Builder)]
pub struct KioScrapeOptions {
    pub blob_store: Arc<dyn BlobStore>,
    pub limit: Option<usize>,
    #[builder(default = NonZeroUsize::new(4).unwrap())]
    pub worker_count: NonZeroUsize,
    #[builder(default = 64)]
    pub channel_capacity: usize,
    #[builder(default = NonZeroUsize::new(2).unwrap())]
    pub discovery_concurrency: NonZeroUsize,
}

impl std::fmt::Debug for KioScrapeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KioScrapeOptions")
            .field("blob_store", &"<Arc<dyn BlobStore>>")
            .field("limit", &self.limit)
            .field("worker_count", &self.worker_count)
            .field("channel_capacity", &self.channel_capacity)
            .field("discovery_concurrency", &self.discovery_concurrency)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::KioScrapeOptions;
    use crate::pipeline::processor::Silo;
    use crate::services::BlobStore;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    struct TestBlobStore;

    #[async_trait::async_trait]
    impl BlobStore for TestBlobStore {
        async fn put(
            &self,
            _silo: Silo,
            _data: crate::services::ByteStream,
        ) -> Result<crate::services::PutResult, crate::services::BlobError> {
            unimplemented!()
        }
        async fn get(
            &self,
            _silo: Silo,
            _cid: &str,
        ) -> Result<crate::services::ByteStream, crate::services::BlobError> {
            unimplemented!()
        }
        async fn head(
            &self,
            _silo: Silo,
            _cid: &str,
        ) -> Result<Option<crate::services::BlobMeta>, crate::services::BlobError> {
            unimplemented!()
        }
        async fn delete(
            &self,
            _silo: Silo,
            _cid: &str,
        ) -> Result<bool, crate::services::BlobError> {
            unimplemented!()
        }
    }

    #[test]
    fn builder_defaults_discovery_concurrency() {
        let store: Arc<dyn BlobStore> = Arc::new(TestBlobStore);
        let options = KioScrapeOptions::builder().blob_store(store).build();

        assert_eq!(options.discovery_concurrency.get(), 2);
        assert_eq!(options.worker_count.get(), 4);
        assert_eq!(options.channel_capacity, 64);
    }

    #[test]
    fn builder_overrides_discovery_concurrency() {
        let store: Arc<dyn BlobStore> = Arc::new(TestBlobStore);
        let options = KioScrapeOptions::builder()
            .blob_store(store)
            .discovery_concurrency(NonZeroUsize::new(3).unwrap())
            .build();

        assert_eq!(options.discovery_concurrency.get(), 3);
    }
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
    BlobStored {
        doc_id: String,
        cid: String,
        bytes: usize,
        existed: bool,
    },
    BlobSkipped {
        doc_id: String,
    },
    Completed {
        summary: KioScraperSummary,
    },
}

/// Final summary produced after scraping completes.
#[derive(Debug, Clone, Copy)]
pub struct KioScraperSummary {
    pub discovered: usize,
    pub stored: usize,
    pub skipped: usize,
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
