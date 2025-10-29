//! Orchestration layer for IO-bound pipeline services.
//!
//! Modules exposed here coordinate external systems (storage, OCR, rate
//! limiting) and must avoid embedding pure transforms. Keep stateless helpers in
//! `crate::text`, `crate::pdf`, or other dedicated modules so concurrency and
//! resource accounting stay localized.

pub mod context;
pub mod ocr;
pub mod orchestrator;

pub use context::{
    EmbedClient, EmbedService, Governors, PipelineContext, PipelineError, PipelineResult,
};
pub use ocr::{
    DeepInfraOcr, GeminiOcr, OcrConfig, OcrDocumentResult, OcrError, OcrInput, OcrMimeType,
    OcrPageResult, OcrService, OcrSpan,
};
pub use orchestrator::run_for_silo;
