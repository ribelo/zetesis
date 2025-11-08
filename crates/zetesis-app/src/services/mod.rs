//! Orchestration layer for IO-bound pipeline services.
//!
//! Modules exposed here coordinate external systems (storage, OCR, rate
//! limiting) and must avoid embedding pure transforms. Keep stateless helpers in
//! `crate::text`, `crate::pdf`, or other dedicated modules so concurrency and
//! resource accounting stay localized.

pub mod batch_structured;
pub mod blob_store;
pub mod context;
pub mod embed;
pub mod indexer;
pub mod jobs;
pub mod milli_actor;
pub mod ocr;
pub mod reaper;
pub mod search;
pub mod structured;

pub use blob_store::{
    BlobError, BlobMeta, BlobStore, ByteStream, Cid, DurableWrite, FsBlobStore, PutResult,
    blake3_cid, validate_cid,
};

pub use batch_structured::GeminiBatchStructuredClient;
#[cfg(feature = "s3")]
pub use blob_store::S3BlobStore;
pub use context::{
    EmbedBatchTask, EmbedClient, EmbedMode, EmbedRuntimeOptions, EmbedService, EmbeddingJobClient,
    Governors, JobMetadata, PipelineContext, PipelineError, PipelineResult, ProviderJobState,
    StructuredBatchInput, StructuredBatchRequest, StructuredBatchResponse, StructuredJobClient,
    SubmittedJob, build_pipeline_context,
};
pub use embed::GeminiEmbedClient;
pub use indexer::{
    decision_content_hash, index_structured_decision, index_structured_with_embeddings,
    load_structured_decision,
};
pub use jobs::{
    GenerationJob, GenerationJobStatus, GenerationJobStore, GenerationJobStoreError,
    GenerationMode, GenerationProviderKind,
};
pub use milli_actor::MilliActorHandle;
pub use ocr::{
    GeminiOcr, OcrConfig, OcrDocumentResult, OcrError, OcrInput, OcrMimeType, OcrPageResult,
    OcrService, OcrSpan, OcrTokenUsage,
};
pub use reaper::{
    ReaperAction, ReaperConfig, ReaperError, ReaperReport, calculate_retry_backoff, reap_stale_jobs,
};
pub use search::{
    DefaultSearchProvider, build_search_row, hybrid, keyword, normalize_index_name,
    open_index_read_only, project_value, resolve_index_dir, set_data_dir_override, vector,
};
pub use zetesis_server::search::{
    HYBRID_DEFAULT_RRF_K, HYBRID_DEFAULT_WEIGHT, HYBRID_PER_SOURCE_LIMIT_MAX,
    HYBRID_RESULT_LIMIT_MAX, HybridFusion, HybridSearchParams, KeywordSearchParams, SearchProvider,
    VectorSearchParams, normalize_hybrid_weights,
};

pub use structured::{StructuredExtractError, StructuredExtraction, StructuredExtractor};
