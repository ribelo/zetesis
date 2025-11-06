//! Modules that fetch raw documents from upstream silos.

pub mod kio_saos;
pub mod kio_types;
pub mod kio_uzp;

pub use kio_saos::KioSaosScraper;
pub use kio_types::{
    KioDocumentMetadata, KioEvent, KioScrapeError as IngestorError, KioScrapeOptions,
    KioScraperSummary,
};
pub use kio_uzp::KioUzpScraper;
