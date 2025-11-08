This file is a merged representation of a subset of the codebase, containing specifically included files, combined into a single document by Repomix.

# File Summary

## Purpose
This file contains a packed representation of a subset of the repository's contents that is considered the most important context.
It is designed to be easily consumable by AI systems for analysis, code review,
or other automated processes.

## File Format
The content is organized as follows:
1. This summary section
2. Repository information
3. Directory structure
4. Repository files (if enabled)
5. Multiple file entries, each consisting of:
  a. A header with the file path (## File: path/to/file)
  b. The full contents of the file in a code block

## Usage Guidelines
- This file should be treated as read-only. Any changes should be made to the
  original repository files, not this packed version.
- When processing this file, use the file path to distinguish
  between different files in the repository.
- Be aware that this file may contain sensitive information. Handle it with
  the same level of security as you would the original repository.

## Notes
- Some files may have been excluded based on .gitignore rules and Repomix's configuration
- Binary files are not included in this packed representation. Please refer to the Repository Structure section for a complete list of file paths, including binary files
- Only files matching these patterns are included: docs/architecture.md, docs/embeddings.md, SPEC, crates/zetesis-app/src/main.rs, crates/zetesis-app/src/cli/mod.rs, crates/zetesis-app/src/services/indexer.rs, crates/zetesis-app/src/services/milli_actor.rs, crates/zetesis-app/src/services/context.rs, crates/zetesis-app/src/services/embed.rs, crates/zetesis-app/src/index/milli.rs, crates/zetesis-app/src/pipeline/index_payload.rs
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Files are sorted by Git change count (files with more changes are at the bottom)

# Directory Structure
```
crates/
  zetesis-app/
    src/
      cli/
        mod.rs
      index/
        milli.rs
      pipeline/
        index_payload.rs
      services/
        context.rs
        embed.rs
        indexer.rs
        milli_actor.rs
      main.rs
docs/
  architecture.md
  embeddings.md
SPEC
```

# Files

## File: crates/zetesis-app/src/pipeline/index_payload.rs
```rust
use chrono::Utc;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::pipeline::Silo;
use crate::pipeline::structured::{DecisionResult, StructuredDecision};

const DOC_TYPE_DOC: &str = "doc";
const DOC_TYPE_CHUNK: &str = "chunk";

#[derive(Debug, Clone, Copy)]
pub enum IngestStatus {
    Pending,
    Indexed,
    Error,
}

impl IngestStatus {
    fn as_str(self) -> &'static str {
        match self {
            IngestStatus::Pending => "pending",
            IngestStatus::Indexed => "indexed",
            IngestStatus::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestMetadata<'a> {
    pub status: IngestStatus,
    pub embedder_key: Option<&'a str>,
    pub chunk_count: usize,
    pub error_message: Option<&'a str>,
}

impl<'a> IngestMetadata<'a> {
    pub fn pending() -> Self {
        Self {
            status: IngestStatus::Pending,
            embedder_key: None,
            chunk_count: 0,
            error_message: None,
        }
    }

    pub fn indexed(embedder_key: &'a str, chunk_count: usize) -> Self {
        Self {
            status: IngestStatus::Indexed,
            embedder_key: Some(embedder_key),
            chunk_count,
            error_message: None,
        }
    }

    pub fn errored(message: &'a str) -> Self {
        Self {
            status: IngestStatus::Error,
            embedder_key: None,
            chunk_count: 0,
            error_message: Some(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedChunkRecord {
    pub ord: u32,
    pub body: String,
    pub record: JsonMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct PreparedDocumentRecords {
    pub doc: JsonMap<String, JsonValue>,
    pub chunks: Vec<PreparedChunkRecord>,
}

pub fn build_document_payload(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
    blob_cids: &[&str],
    ingest: IngestMetadata<'_>,
) -> Result<PreparedDocumentRecords, serde_json::Error> {
    debug_assert!(!doc_id.is_empty());
    debug_assert!(decision.chunks.len() < u32::MAX as usize);

    let doc_record = build_doc_record(decision, silo, doc_id, blob_cids, ingest)?;
    let chunks = make_chunk_records(decision, silo, doc_id);
    Ok(PreparedDocumentRecords {
        doc: doc_record,
        chunks,
    })
}

pub fn build_doc_record(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
    blob_cids: &[&str],
    ingest: IngestMetadata<'_>,
) -> Result<JsonMap<String, JsonValue>, serde_json::Error> {
    debug_assert!(decision.summary_short.len() > 0);
    debug_assert!(decision.panel.len() <= 16);

    let mut record = JsonMap::new();
    record.insert(
        "id".to_string(),
        JsonValue::String(format!("{}::{}::doc", silo.slug(), doc_id)),
    );
    record.insert(
        "silo".to_string(),
        JsonValue::String(silo.slug().to_string()),
    );
    record.insert(
        "doc_type".to_string(),
        JsonValue::String(DOC_TYPE_DOC.to_string()),
    );
    record.insert("doc_id".to_string(), JsonValue::String(doc_id.to_string()));
    record.insert("blob_cids".to_string(), json!(blob_cids));
    record.insert(
        "summary_short".to_string(),
        JsonValue::String(decision.summary_short.clone()),
    );
    record.insert("structured".to_string(), serde_json::to_value(decision)?);
    record.insert(
        "ingest".to_string(),
        JsonValue::Object(build_ingest_object(ingest, blob_cids)),
    );
    Ok(record)
}

fn make_chunk_records(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
) -> Vec<PreparedChunkRecord> {
    debug_assert!(!decision.decision.date.is_empty());
    debug_assert!(decision.chunks.len() > 0);

    let base_identifier = json!({
        "kio_docket": &decision.identifiers.kio_docket,
        "saos_id": decision.identifiers.saos_id.as_ref(),
    });

    let base_decision = json!({
        "date": &decision.decision.date,
        "result": decision_result_value(decision.decision.result),
    });

    let parties = json!({
        "contracting_authority": &decision.parties.contracting_authority,
        "appellant": &decision.parties.appellant,
        "awardee": decision.parties.awardee.as_ref(),
        "interested_bidders": &decision.parties.interested_bidders,
    });

    let procurement = json!({
        "subject": decision.procurement.subject.as_ref(),
        "procedure_type": decision.procurement.procedure_type.map(|pt| pt.as_ref().to_string()),
        "tender_id": decision.procurement.tender_id.as_ref(),
        "cpv_codes": &decision.procurement.cpv_codes,
    });

    let issues: Vec<JsonValue> = decision
        .issues
        .iter()
        .map(|iss| {
            json!({
                "issue_key": iss.issue_key.as_ref(),
                "confidence": iss.confidence,
            })
        })
        .collect();

    let mut out = Vec::with_capacity(decision.chunks.len());
    for chunk in &decision.chunks {
        let mut record = JsonMap::new();
        record.insert(
            "id".to_string(),
            JsonValue::String(format!(
                "{}::{}::chunk::{}",
                silo.slug(),
                doc_id,
                chunk.position
            )),
        );
        record.insert(
            "silo".to_string(),
            JsonValue::String(silo.slug().to_string()),
        );
        record.insert(
            "doc_type".to_string(),
            JsonValue::String(DOC_TYPE_CHUNK.to_string()),
        );
        record.insert("doc_id".to_string(), JsonValue::String(doc_id.to_string()));
        record.insert(
            "ord".to_string(),
            JsonValue::Number(serde_json::Number::from(u64::from(chunk.position))),
        );
        record.insert("section".to_string(), JsonValue::Null);
        record.insert("content".to_string(), JsonValue::String(chunk.body.clone()));
        record.insert("decision".to_string(), base_decision.clone());
        record.insert("identifiers".to_string(), base_identifier.clone());
        record.insert("parties".to_string(), parties.clone());
        record.insert("procurement".to_string(), procurement.clone());
        record.insert("issues".to_string(), JsonValue::Array(issues.clone()));

        out.push(PreparedChunkRecord {
            ord: chunk.position as u32,
            body: chunk.body.clone(),
            record,
        });
    }

    out
}

fn build_ingest_object(meta: IngestMetadata<'_>, blob_cids: &[&str]) -> JsonMap<String, JsonValue> {
    let mut ingest = JsonMap::new();
    ingest.insert(
        "status".to_string(),
        JsonValue::String(meta.status.as_str().to_string()),
    );
    ingest.insert(
        "updated_at".to_string(),
        JsonValue::String(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
    );
    ingest.insert(
        "embedder_key".to_string(),
        meta.embedder_key
            .map(|value| JsonValue::String(value.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    ingest.insert(
        "chunk_count".to_string(),
        JsonValue::Number(serde_json::Number::from(meta.chunk_count as u64)),
    );
    ingest.insert(
        "error".to_string(),
        meta.error_message
            .map(|msg| JsonValue::String(msg.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    ingest.insert("blob_cids".to_string(), json!(blob_cids));
    ingest
}

fn decision_result_value(result: DecisionResult) -> String {
    match result {
        DecisionResult::Unknown => "unknown".to_string(),
        other => other.as_ref().to_string(),
    }
}
```

## File: crates/zetesis-app/src/services/embed.rs
```rust
use async_trait::async_trait;
use gemini_ox::{
    Gemini,
    embedding::{EmbedContentRequest, TaskType},
};

use crate::services::context::{EmbedClient, PipelineError, PipelineResult};

const MAX_SINGLE_REQUEST: usize = 1;

#[derive(Clone)]
pub struct GeminiEmbedClient {
    client: Gemini,
    model: String,
    dim: usize,
    task_type: TaskType,
}

impl GeminiEmbedClient {
    pub fn from_env(model: impl Into<String>, dim: usize) -> Result<Self, PipelineError> {
        debug_assert!(dim > 0);

        let api_key = std::env::var("GOOGLE_AI_API_KEY")
            .or_else(|_| std::env::var("GEMINI_API_KEY"))
            .map_err(|_| PipelineError::MissingGeminiApiKey)?;

        Ok(Self {
            client: Gemini::new(api_key),
            model: model.into(),
            dim,
            task_type: TaskType::RetrievalDocument,
        })
    }

    async fn embed_single(&self, text: &str) -> PipelineResult<Vec<f32>> {
        debug_assert!(self.dim > 0);
        debug_assert!(!self.model.is_empty());

        if text.trim().is_empty() {
            return Ok(vec![0.0; self.dim]);
        }

        let request: EmbedContentRequest = self
            .client
            .embed_content()
            .model(self.model.clone())
            .content(text.into())
            .task_type(self.task_type.clone())
            .output_dimensionality(self.dim as u32)
            .build();

        let response = request.send().await.map_err(PipelineError::from)?;
        let values = response.embedding.values;
        debug_assert_eq!(values.len(), self.dim);

        Ok(values)
    }
}

#[async_trait]
impl EmbedClient for GeminiEmbedClient {
    async fn embed_batch(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        debug_assert!(texts.len() <= usize::MAX);
        debug_assert!(MAX_SINGLE_REQUEST > 0);

        let mut out: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(MAX_SINGLE_REQUEST) {
            for text in chunk {
                let embedding = self.embed_single(text).await?;
                out.push(embedding);
            }
        }

        Ok(out)
    }
}
```

## File: crates/zetesis-app/src/services/indexer.rs
```rust
use std::time::Duration;

use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::pipeline::Silo;
use crate::pipeline::index_payload::{
    IngestMetadata, PreparedChunkRecord, build_doc_record, build_document_payload,
};
use crate::pipeline::structured::StructuredDecision;
use crate::services::context::{PipelineContext, PipelineError, PipelineResult};
use crate::services::milli_actor::MilliActorHandle;

const MAX_EMBED_ATTEMPTS: u8 = 3;
const INITIAL_EMBED_DELAY_MS: u64 = 200;

pub async fn index_structured_decision(
    actor: &MilliActorHandle,
    ctx: &PipelineContext,
    silo: Silo,
    doc_id: &str,
    decision: StructuredDecision,
) -> PipelineResult<()> {
    debug_assert!(!doc_id.is_empty());
    debug_assert!(decision.chunks.len() > 0);

    let blob_refs = [doc_id];
    let pending = build_document_payload(
        &decision,
        silo,
        doc_id,
        &blob_refs,
        IngestMetadata::pending(),
    )
        .map_err(|err| PipelineError::message(err.to_string()))?;
    actor.upsert(std::slice::from_ref(&pending.doc))?;

    let chunk_records = match vectorize_chunks(ctx, &pending.chunks).await {
        Ok(records) => records,
        Err(err) => {
            let message = err.to_string();
            warn!(
                doc_id,
                silo = silo.slug(),
                error = %message,
                "structured indexing failed"
            );
            let error_doc = build_doc_record(
                &decision,
                silo,
                doc_id,
                &blob_refs,
                IngestMetadata::errored(&message),
            )
            .map_err(|err| PipelineError::message(err.to_string()))?;
            actor.upsert(std::slice::from_ref(&error_doc))?;
            return Err(err);
        }
    };

    if !chunk_records.is_empty() {
        actor.upsert(&chunk_records)?;
    }

    let indexed_doc = build_doc_record(
        &decision,
        silo,
        doc_id,
        &blob_refs,
        IngestMetadata::indexed(&ctx.embed.embedder_key, chunk_records.len()),
    )
    .map_err(|err| PipelineError::message(err.to_string()))?;
    actor.upsert(std::slice::from_ref(&indexed_doc))?;

    debug!(
        doc_id,
        silo = silo.slug(),
        chunks = chunk_records.len(),
        "structured decision indexed"
    );
    Ok(())
}

async fn embed_chunks(
    ctx: &PipelineContext,
    chunks: &[PreparedChunkRecord],
) -> PipelineResult<Vec<Vec<f32>>> {
    debug_assert!(chunks.len() < usize::MAX);
    let bodies: Vec<&str> = chunks.iter().map(|chunk| chunk.body.as_str()).collect();
    let mut attempt: u8 = 0;
    let mut delay_ms = INITIAL_EMBED_DELAY_MS;

    loop {
        match ctx.embed.embed_batch(&bodies).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempt = attempt.saturating_add(1);
                if attempt >= MAX_EMBED_ATTEMPTS {
                    return Err(err);
                }

                warn!(attempt, "embedding batch failed; retrying");
                sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = delay_ms.saturating_mul(2);
            }
        }
    }
}

async fn vectorize_chunks(
    ctx: &PipelineContext,
    chunks: &[PreparedChunkRecord],
) -> PipelineResult<Vec<JsonMap<String, JsonValue>>> {
    debug_assert!(chunks.len() < usize::MAX);

    let embeddings = embed_chunks(ctx, chunks).await?;
    let mut out: Vec<JsonMap<String, JsonValue>> = Vec::with_capacity(chunks.len());
    for (prepared, vector) in chunks.iter().zip(embeddings.iter()) {
        debug_assert_eq!(vector.len(), ctx.embed.dim);
        let mut record = prepared.record.clone();
        attach_vector(&mut record, &ctx.embed.embedder_key, vector)?;
        out.push(record);
    }

    Ok(out)
}

fn attach_vector(
    record: &mut JsonMap<String, JsonValue>,
    embedder_key: &str,
    vector: &[f32],
) -> Result<(), PipelineError> {
    debug_assert!(!embedder_key.is_empty());
    debug_assert!(vector.len() > 0);

    if vector.iter().any(|value| !value.is_finite()) {
        return Err(PipelineError::message(
            "embedding contained non-finite values",
        ));
    }

    let mut vector_values: Vec<JsonValue> = Vec::with_capacity(vector.len());
    for value in vector {
        let number = serde_json::Number::from_f64(*value as f64)
            .ok_or_else(|| PipelineError::message("embedding value out of range"))?;
        vector_values.push(JsonValue::Number(number));
    }

    let mut vectors = JsonMap::new();
    vectors.insert(embedder_key.to_string(), JsonValue::Array(vector_values));
    record.insert("_vectors".to_string(), JsonValue::Object(vectors));
    Ok(())
}
```

## File: crates/zetesis-app/src/services/milli_actor.rs
```rust
use std::thread::{self, JoinHandle};

use flume::{Receiver, Sender};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::index::writer::{IndexWriteError, IndexWriter};
use crate::services::context::{PipelineError, PipelineResult};

pub struct MilliActorHandle {
    tx: Sender<MilliCmd>,
    join: Option<JoinHandle<()>>,
}

enum MilliCmd {
    Upsert {
        records: Vec<JsonMap<String, JsonValue>>,
        resp: Sender<Result<milli::update::DocumentAdditionResult, IndexWriteError>>,
    },
    Stop,
}

impl MilliActorHandle {
    pub fn spawn(index: milli::Index, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded::<MilliCmd>(capacity.max(1));
        let join = Some(thread::spawn(move || run_actor(rx, index)));
        Self { tx, join }
    }

    pub fn upsert(
        &self,
        records: &[JsonMap<String, JsonValue>],
    ) -> PipelineResult<milli::update::DocumentAdditionResult> {
        let (resp_tx, resp_rx) = flume::bounded(1);
        let owned = records.to_vec();
        self.tx
            .send(MilliCmd::Upsert {
                records: owned,
                resp: resp_tx,
            })
            .map_err(|e| PipelineError::message(format!("actor send failed: {e}")))?;
        resp_rx
            .recv()
            .map_err(|e| PipelineError::message(format!("actor recv failed: {e}")))?
            .map_err(PipelineError::from)
    }
}

impl Drop for MilliActorHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(MilliCmd::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run_actor(rx: Receiver<MilliCmd>, index: milli::Index) {
    let writer = IndexWriter::new(index);
    while let Ok(cmd) = rx.recv() {
        match cmd {
            MilliCmd::Upsert { records, resp } => {
                let res = writer.upsert(&records);
                let _ = resp.send(res);
            }
            MilliCmd::Stop => break,
        }
    }
}
```

## File: docs/embeddings.md
```markdown
# Embedding & Storage Strategy

## 1. Goals
- Keep Milli as the single canonical store for structured documents and chunk records.
- Support both keyword and vector similarity search with minimal duplication.
- Make embedder upgrades (model swaps, dimension changes) safe and reversible.

## 2. Record Shapes
- **Document record (one per decision)**
  - `id`: `{silo}::{doc_id}::doc`
  - `doc_type`: `"doc"`
  - `doc_id`: stable identifier derived from source metadata
  - `structured`: full `StructuredDecision` payload
  - `summary_short`: short abstract for lightweight recall
  - `ingest`: status (`pending` → `indexed`), timestamps, embedder key used, error state
- **Chunk record (N per decision)**
  - `id`: `{silo}::{doc_id}::chunk::{ord}`
  - `doc_type`: `"chunk"`
  - `doc_id`: parent document identifier
  - `ord`: 1-based position
  - `section`: optional semantic label (`header`, `sentencja`, …)
  - `content`: chunk body text
  - `decision.date`, `decision.result`, `parties.contracting_authority`, `parties.appellant`, `procurement.cpv_codes`, `issues.issue_key`, `identifiers.kio_docket`: copied into flat attributes for filtering
  - `_vectors.{embedder_key}`: embedding generated by the configured model (default `gemini-embedding-001`, dim 768)

All documents and chunks reside in the same Milli index per silo; vector payloads never leave Milli.

## 3. Embedder Policy
- Default embedder: `gemini-embedding-001`, 768 dimensions, stored under `_vectors.gemini-embedding-001`.
- Embed requests are batched and throttled via `governor`; failures trigger bounded retries with exponential backoff and ingest status updates.
- Embedder upgrades create a new key (`_vectors.{new_key}`) alongside the old one. Queries switch keys once re-embedding completes; old keys can be deleted after validation.

## 4. Query Surfaces
- **Similarity search**: chunk records searched via `_vectors.{embedder_key}`; results re-hydrate document context by fetching the corresponding doc record.
- **Keyword/filter search**: `content` (chunks) and `summary_short` (docs) remain searchable; filters apply on flattened attributes listed above.

## 5. Ingest Flow (Batch or Near Real-Time)
1. OCR → structured extraction produces `StructuredDecision`.
2. Upsert document record in Milli with status `pending` and canonical payload.
3. Derive chunk records, embed `content`, and upsert with vectors.
4. Update the document record to status `indexed`, recording timings, chunk count, and embedder key.
5. On failure, set status `error` with context; retries are idempotent because Milli upserts overwrite by `id`.

## 6. Cost Controls
- Only chunk bodies are embedded by default; document-level embeddings (e.g., `summary_short`) stay behind a feature flag to avoid unnecessary costs.
- Audit tooling (`zetesis-app audit structured`) samples PDFs, compares token coverage, and surfaces drift so prompt adjustments keep chunk completeness high.
```

## File: crates/zetesis-app/src/index/milli.rs
```rust
//! Milli index bootstrap helpers.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use heed::EnvOpenOptions;
use milli::progress::{EmbedderStats, Progress};
use milli::update::{IndexerConfig, Setting as MilliSetting, Settings};
use milli::vector::VectorStoreBackend;
use milli::vector::settings::{EmbedderSource, EmbeddingSettings};
use milli::{FilterableAttributesRule, Index, Result as MilliResult};
use thiserror::Error;

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EMBEDDING_DIM};
use crate::paths::{AppPaths, PathError};

const DEFAULT_MAP_SIZE_BYTES: usize = 1 << 30; // 1 GiB
const SEARCHABLE_FIELDS: &[&str] = &["content", "summary_short"];
const FILTERABLE_FIELDS: &[&str] = &[
    "doc_type",
    "doc_id",
    "decision.date",
    "decision.result",
    "parties.contracting_authority",
    "parties.appellant",
    "procurement.cpv_codes",
    "issues.issue_key",
    "identifiers.kio_docket",
    "section",
];
const SORTABLE_FIELDS: &[&str] = &["decision.date"];

#[derive(Debug, Error)]
pub enum MilliBootstrapError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error("milli error: {0}")]
    Milli(#[from] milli::Error),
    #[error("heed error: {0}")]
    Heed(#[from] heed::Error),
}

/// Ensure the Milli index for a given silo exists and is configured with the expected
/// vector backend and embedder. Returns the opened [`Index`].
pub fn ensure_index<S: AsRef<str>>(
    paths: &AppPaths,
    silo: S,
) -> Result<Index, MilliBootstrapError> {
    let path = paths.milli_index_dir(&silo)?;
    let index = open_or_create_index(&path)?;
    configure_index(&index)?;
    Ok(index)
}

fn open_or_create_index(path: &PathBuf) -> MilliResult<Index> {
    let mut env_opts = EnvOpenOptions::new().read_txn_without_tls();
    env_opts.map_size(DEFAULT_MAP_SIZE_BYTES);
    Index::new(env_opts, path, true)
}

fn configure_index(index: &Index) -> Result<(), MilliBootstrapError> {
    let rtxn = index.read_txn()?;
    let current_backend = index.get_vector_store(&rtxn)?;
    let embedding_configs = index.embedding_configs();
    let existing_configs = embedding_configs.embedding_configs(&rtxn)?;
    let has_expected_embedder = existing_configs.iter().any(|cfg| {
        cfg.name == DEFAULT_EMBEDDER_KEY
            && matches!(
                &cfg.config.embedder_options,
                milli::vector::EmbedderOptions::UserProvided(opts)
                if opts.dimensions == DEFAULT_EMBEDDING_DIM
            )
            && !cfg.config.quantized.unwrap_or(false)
    });
    drop(rtxn);

    if current_backend == Some(VectorStoreBackend::Hannoy) && has_expected_embedder {
        return Ok(());
    }

    let mut wtxn = index.write_txn()?;
    let indexer_config = IndexerConfig::default();
    let mut settings = Settings::new(&mut wtxn, index, &indexer_config);

    if current_backend != Some(VectorStoreBackend::Hannoy) {
        settings.set_vector_store(VectorStoreBackend::Hannoy);
    }

    if !has_expected_embedder {
        let mut embedder_settings = EmbeddingSettings::default();
        embedder_settings.source = MilliSetting::Set(EmbedderSource::UserProvided);
        embedder_settings.dimensions = MilliSetting::Set(DEFAULT_EMBEDDING_DIM);
        embedder_settings.binary_quantized = MilliSetting::Set(false);

        let mut map = BTreeMap::new();
        map.insert(
            DEFAULT_EMBEDDER_KEY.to_string(),
            MilliSetting::Set(embedder_settings),
        );
        settings.set_embedder_settings(map);
    }

    let searchable: Vec<String> = SEARCHABLE_FIELDS
        .iter()
        .map(|field| field.to_string())
        .collect();
    settings.set_searchable_fields(searchable);

    let filterable: Vec<FilterableAttributesRule> = FILTERABLE_FIELDS
        .iter()
        .map(|field| FilterableAttributesRule::Field(field.to_string()))
        .collect();
    settings.set_filterable_fields(filterable);

    let sortable: HashSet<String> = SORTABLE_FIELDS
        .iter()
        .map(|field| field.to_string())
        .collect();
    settings.set_sortable_fields(sortable);

    if let Some(_congestion) = settings.execute(
        &|| false,
        &Progress::default(),
        Arc::new(EmbedderStats::default()),
    )? {
        tracing::warn!("settings update reported channel congestion; continuing");
    }

    wtxn.commit()?;
    Ok(())
}
```

## File: docs/architecture.md
```markdown
# Zetesis Architecture Overview

## 1. Purpose
Zetesis is a lightweight SaaS for full-text and similarity search over documents, starting with feature parity to https://szukio.pl/. This document captures the foundational architecture so code, docs, and operations stay aligned.

## 2. System Components
- **Server (`zetesis-app`)**: Single binary exposing both the Axum HTTP API and a Clap-powered maintenance CLI.
- **CLI Commands**: Share the binary entry point; subcommands such as `serve`, `documents add`, and `scrape ...` will be introduced once requirements solidify.
- **Storage / Search**: A single Milli index (backed by LMDB internally) stores canonical structured documents, chunk records, and user-provided vectors.
- **Blob Store**: Original PDFs are written to the filesystem using content-addressed paths under the XDG data directory.
- **AI & Embeddings**: The local `ai-ox` crate supplies LLM utilities and embedding models; treat it as the sole integration point for AI tasks.
- **Configuration**: Loaded with the `config` crate, resolved through XDG directories via `directories`; defaults live in `config/settings.example.toml`.

## 3. Data & Storage Strategy
- The Milli index is the authoritative store for structured documents and chunk records. All canonical JSON lands here and is queryable directly.
- Original PDFs are stored on disk (`${XDG_DATA_HOME}/zetesis/blobs/{silo}/{prefix}/{hash}.pdf`) with stable identifiers linking them to Milli documents.
- Auxiliary application metadata (e.g., configuration caches, counters) may live in separate LMDB environments, but documents themselves never leave Milli.

## 4. Ingest & Search Pipeline
1. **Scrape**: Fetch documents (HTML, Markdown, PDF). TODO: choose extraction tooling (e.g., `scraper`, `selectolax`, or external services).
2. **Normalize**: Convert to clean text, capture metadata (title, URL, tags).
3. **Persist**: Upsert canonical structured documents into Milli (doc record) and derived chunk records with vectors.
4. **Index**: Configure Milli searchable/filterable fields and user-provided vectors generated through `ai-ox`.
5. **Query**: HTTP endpoints and CLI utilities route queries through milli, returning ranked results with metadata pulled from LMDB.

## 5. Interfaces
- **HTTP API**: Axum routes for search, document management, and health checks. Use Tower middleware for tracing and auth when defined.
- **CLI**: Clap commands for operational tasks (document ingestion, scrapers, migrations). Matches server modules for reuse.
- **Web UI**: TBD (Svelte vs Dioxus). Scaffold will reserve a `web/` entry point once the framework decision is made.

## 6. Observability & Operations
- Use `tracing` + `tracing-subscriber` for structured logs; default to info level.
- Add metrics (e.g., `metrics` crate or OpenTelemetry) after core features land.
- Docker/OCI packaging is deferred until after API stabilizes.

## 7. Open Questions
- Preferred scraper/extractor stack and content formats to support.
- Authentication/authorization model for SaaS users.
- Vector embedding strategy for similarity search (external service vs. local models).
- Deployment environment (single binary on VPS vs. managed container).

Keep this document current as implementation decisions land; update alongside SPEC/TODO during each sprint review.
```

## File: SPEC
```
# Zetesis Specification

## Authoring Guidelines
- [REQ-ZE-SPEC-FORMAT] Write requirements as `- [slug] Sentence…` bullets grouped under clear headings. Keep slugs uppercase, hyphen-separated, and project-prefixed (e.g., `ZE-AREA-THEME`).
- [REQ-ZE-SPEC-SCOPE] Document product behavior, constraints, and UX expectations. Leave implementation notes and backlog items for `TODO`.
- [REQ-ZE-SPEC-TRACE] Reference related backlog items with their identifiers (e.g., `WP-CLI`, `T1`) to maintain traceability between `SPEC` and `TODO`.
- [REQ-ZE-SPEC-CHANGES] When requirements change, update the relevant bullet in place and append a short `(Updated YYYY-MM-DD — reason)` note for historical context.
- [REQ-ZE-SPEC-CADENCE] Review SPEC at least once per active sprint; record adjustments immediately so it never trails TODO by more than a day.

### TODO Collaboration
- [REQ-ZE-TODO-SYNC] Before adding or modifying backlog entries, scan the active requirements and ensure each TODO item points to the requirement it satisfies.
- [REQ-ZE-TODO-STATUS] Keep TODO status markers (`[ ]`, `[~]`, `[x]`) in sync with progress and avoid removing completed entries; instead mark them `[x]` for traceability.
- [REQ-ZE-TODO-REVIEWS] When TODO items complete a requirement, annotate the requirement with `(Verified via WP-…/T…)` so SPEC remains the single source of truth.

### Storage & Indexing
- [REQ-ZE-STORAGE-MILLI] Persist canonical structured documents and derived chunk records directly in Milli. Each record must remain self-contained so Milli alone can satisfy read and reindex flows. (Updated 2025-10-30 — migrated away from LMDB-backed document store)
- [REQ-ZE-STORAGE-FS] Store original KIO PDFs on the filesystem using content-addressed paths under `$XDG_DATA_HOME/zetesis/blobs/{silo}/{prefix}/{hash}.pdf`; document records reference the blob location.
- [REQ-ZE-INDEX-PER-SILO] Create one Milli index **per silo** at `$XDG_DATA_HOME/zetesis/milli/{silo}` using Milli’s LMDB-backed environment. Index paths are derived from the silo slug—do not duplicate them elsewhere.
- [REQ-ZE-INDEX-EMBEDDER] Configure each Milli index with vector backend Hannoy and a user-provided embedder defined in configuration (initially `gemini-embedding-001`, dimension 768). Manual vectors must appear under `_vectors.{embedder_key}`.
- [REQ-ZE-INDEX-FIELDS] Chunk documents shall expose flat (or shallow) fields compatible with Milli flattening: `silo`, `doc_type`, `doc_id`, `decision.date` (ISO), `decision.result`, `panel.names[]`, `parties.contracting_authority`, `parties.appellant`, `procurement.cpv_codes`, `section`, `ord`, and `content`. (Updated 2025-10-30 — aligned with Milli canonical storage)
- [REQ-ZE-INDEX-LITERAL] Store per-chunk embeddings only inside Milli; no secondary store may retain vector payloads. (Updated 2025-10-30 — clarified Milli-only vector policy)

### KIO Ingestion
- [REQ-ZE-KIO-INGESTOR] Provide a KIO scraper that enumerates judgments via `POST /Home/GetResults` on https://orzeczenia.uzp.gov.pl/ (Kind=KIO) and walks detail pages for metadata and downloads. The retired FTP flow is not supported. (Updated 2025-10-25 — UZP disabled FTP on 2025-09-30)
- [REQ-ZE-KIO-DISCOVERY] First-page requests must send `CountStats=True` to capture total hits; subsequent pages toggle `CountStats=False` for throughput. The scraper shall respect the portal’s 10-result page limit and advance with `Pg=1..n` using governed retries.
- [REQ-ZE-KIO-CONCURRENCY] Implement a two-stage concurrent pipeline: one async task performs discovery and pushes document descriptors onto a bounded channel; N worker tasks (configurable, default ≥4) pull descriptors and download judgment PDFs plus detail HTML, applying `governor` rate limits and `backon` retries.
- [REQ-ZE-KIO-DOCMETA] For each KIO document, capture multiple case numbers, ruling type (Wyrok/Postanowienie), ISO ruling date, panel names, participant names, procurement subject, case sign, EU journal reference, and extracted legal references before chunking.
- [REQ-ZE-KIO-ASSETS] Persist only the primary judgment body (`/Home/PdfContent/{id}?Kind=KIO`) to the blob store; skip metrics PDFs unless specifically requested later.
- [REQ-ZE-KIO-CHUNKS] Split KIO documents using the double-pass chunker into sections (`header`, `sentencja`, `uzasadnienie`, `other`) with stable ordering; offsets remain attached to chunk metadata inside Milli. (Updated 2025-10-30 — LMDB removed)
- [REQ-ZE-KIO-PIPELINE] The ingest pipeline must: (1) write PDFs to the blob store, (2) persist canonical structured documents in Milli with ingest status `pending`, (3) embed chunks via `ai-ox` Gemini (IO-bounded with governed concurrency), (4) upsert chunk records with `_vectors.{embedder_key}`, and (5) mark the document record `indexed` with counts and timings. (Updated 2025-10-30 — LMDB removed)
- [REQ-ZE-KIO-DELETE] Hard deletes remove Milli documents (doc + chunks) and filesystem blobs in a single operation.
```

## File: crates/zetesis-app/src/services/context.rs
```rust
use std::sync::Arc;

use async_trait::async_trait;
use backon::ExponentialBuilder;
use governor::RateLimiter;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::direct::NotKeyed;
use thiserror::Error;

use crate::index::milli::{MilliBootstrapError, ensure_index};
use crate::index::writer::IndexWriteError;
use crate::paths::{AppPaths, PathError};
use crate::pipeline::Silo;
use gemini_ox::GeminiRequestError;

pub type GenericRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub struct EmbedService {
    pub embedder_key: String,
    pub dim: usize,
    pub client: Arc<dyn EmbedClient>,
}

impl EmbedService {
    pub async fn embed_batch(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>> {
        self.client.embed_batch(texts).await
    }
}

#[async_trait]
pub trait EmbedClient: Send + Sync {
    async fn embed_batch(&self, texts: &[&str]) -> PipelineResult<Vec<Vec<f32>>>;
}

pub struct Governors {
    pub io: Option<Arc<GenericRateLimiter>>,
    pub embed: Option<Arc<GenericRateLimiter>>,
}

impl Default for Governors {
    fn default() -> Self {
        Self {
            io: None,
            embed: None,
        }
    }
}

pub struct PipelineContext {
    pub paths: AppPaths,
    pub embed: EmbedService,
    pub backoff: ExponentialBuilder,
    pub governors: Governors,
}

impl PipelineContext {
    pub fn index_for(&self, silo: Silo) -> PipelineResult<milli::Index> {
        ensure_index(&self.paths, silo.slug()).map_err(PipelineError::from)
    }
}

pub type PipelineResult<T> = Result<T, PipelineError>;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Milli(#[from] MilliBootstrapError),
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("missing GOOGLE_AI_API_KEY or GEMINI_API_KEY environment variable")]
    MissingGeminiApiKey,
    #[error(transparent)]
    Gemini(#[from] GeminiRequestError),
    #[error(transparent)]
    IndexWrite(#[from] IndexWriteError),
}

impl PipelineError {
    pub fn message(msg: impl Into<String>) -> Self {
        PipelineError::Message(msg.into())
    }
}
```

## File: crates/zetesis-app/src/cli/mod.rs
```rust
use std::path::PathBuf;

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};

use crate::constants::DEFAULT_EMBEDDER_KEY;

/// Top-level CLI entry point.
#[derive(Debug, Parser)]
#[command(
    name = "zetesis",
    version,
    author,
    about = "Zetesis document search service"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
    /// Increase logging verbosity (-v, -vv, -vvv).
    #[arg(global = true, short = 'v', long = "verbose", action = ArgAction::Count)]
    pub verbose: u8,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            command: None,
            verbose: 0,
        }
    }
}

impl Cli {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }

    pub fn print_help() {
        let mut cmd = Cli::command();
        let _ = cmd.print_help();
        println!();
    }
}

/// Supported subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the Zetesis HTTP server.
    Serve(ServeArgs),
    /// Fetch KIO documents from upstream sources.
    FetchKio(FetchKioArgs),
    /// Segment local PDF documents and inspect the resulting sentences.
    SegmentPdf(SegmentPdfArgs),
    /// Render a PDF page-by-page, OCR via a selected provider, and emit JSON results.
    OcrPdf(OcrPdfArgs),
    /// Extract structured KIO decision data from a PDF using Gemini structured output.
    ExtractStructured(ExtractStructuredArgs),
    /// Run audits and consistency checks.
    Audit(AuditArgs),
    /// Index structured documents into Milli.
    Index(IndexArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs;

/// Identify which source we are scraping for KIO judgments.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KioSource {
    Uzp,
    Saos,
}

pub const DEFAULT_KIO_UZP_URL: &str = "https://orzeczenia.uzp.gov.pl/";
pub const DEFAULT_KIO_SAOS_URL: &str = "https://www.saos.org.pl/";

/// Fetch documents from a KIO data source.
#[derive(Debug, Args)]
pub struct FetchKioArgs {
    /// Base URL of the KIO portal search interface.
    #[arg(long, default_value = DEFAULT_KIO_UZP_URL)]
    pub url: String,
    /// Limit the number of documents to download (omit to fetch everything).
    #[arg(long)]
    pub limit: Option<usize>,
    /// Directory where downloaded documents will be cached.
    #[arg(long, value_name = "DIR")]
    pub output_path: PathBuf,
    /// Number of concurrent download workers (>= 1).
    #[arg(long, default_value_t = 4)]
    pub workers: usize,
    /// Upstream source to pull from (`uzp` >= 2021, `saos` < 2021).
    #[arg(long, value_enum, default_value_t = KioSource::Uzp)]
    pub source: KioSource,
}

/// Segment local documents and print the resulting chunks.
#[derive(Debug, Args)]
pub struct SegmentPdfArgs {
    /// One or more PDF files to inspect.
    #[arg(required = true)]
    pub inputs: Vec<PathBuf>,
    /// Output rendering (human-readable text or JSON lines).
    #[arg(long, value_enum, default_value_t = SegmentOutputFormat::Text)]
    pub format: SegmentOutputFormat,
    /// Include byte offsets for each segment.
    #[arg(long)]
    pub with_offsets: bool,
}

/// Available OCR backends exposed through the CLI.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OcrProviderKind {
    Deepinfra,
    Gemini,
}

/// OCR a PDF via the selected provider and print JSON to stdout.
#[derive(Debug, Args)]
pub struct OcrPdfArgs {
    /// PDF document to process.
    #[arg(value_name = "PDF")]
    pub input: PathBuf,
    /// OCR provider used to send page renders.
    #[arg(long = "ocr-provider", value_enum, default_value_t = OcrProviderKind::Deepinfra)]
    pub provider: OcrProviderKind,
    /// Override the model identifier for the selected provider.
    #[arg(long = "ocr-model", default_value = "allenai/olmOCR-2-7B-1025")]
    pub model: String,
    /// Target width (pixels) when rasterizing each PDF page.
    #[arg(long, default_value_t = 2048)]
    pub render_width: u32,
    /// Maximum edge length (pixels) when preparing images for OCR.
    #[arg(long, default_value_t = 1280)]
    pub image_max_edge: u32,
    /// Optional image detail hint passed to the model (`low`, `high`, `auto`).
    #[arg(long)]
    pub detail: Option<String>,
    /// Maximum tokens requested from the model.
    #[arg(long, default_value_t = 4096)]
    pub max_tokens: u32,
}

/// Extract structured decision data from a PDF.
#[derive(Debug, Args)]
pub struct ExtractStructuredArgs {
    /// PDF document to process.
    #[arg(value_name = "PDF")]
    pub input: PathBuf,
    /// Gemini model identifier to use for structured extraction.
    #[arg(long, default_value = "gemini-2.5-flash-lite-preview-09-2025")]
    pub model: String,
}

/// How to render chunk output.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SegmentOutputFormat {
    Text,
    Json,
}

/// Audit command namespace.
#[derive(Debug, Args)]
pub struct AuditArgs {
    #[command(subcommand)]
    pub command: AuditCommands,
}

/// Audit subcommands.
#[derive(Debug, Subcommand)]
pub enum AuditCommands {
    /// Audit semantic chunk coverage and basic integrity signals.
    Structured(StructuredAuditArgs),
}

/// Options for the `audit structured` command.
#[derive(Debug, Args)]
pub struct StructuredAuditArgs {
    /// Directory to scan for PDFs (defaults to data/kio/raw/kio).
    #[arg(long, value_name = "DIR", default_value = "data/kio/raw/kio")]
    pub dir: PathBuf,
    /// Number of PDFs to sample.
    #[arg(short = 'n', long = "num", default_value_t = 10)]
    pub num: usize,
    /// Override Gemini model identifier for extraction.
    #[arg(long)]
    pub model: Option<String>,
    /// Output path for TSV (use '-' for stdout).
    #[arg(long = "out", value_name = "TSV", default_value = "-")]
    pub out_path: String,
}

/// Top-level index command namespace.
#[derive(Debug, Args)]
pub struct IndexArgs {
    #[command(subcommand)]
    pub command: IndexCommands,
}

/// Index subcommands.
#[derive(Debug, Subcommand)]
pub enum IndexCommands {
    /// Index structured decisions into Milli.
    Structured(StructuredIndexArgs),
}

/// Options for indexing structured documents.
#[derive(Debug, Args)]
pub struct StructuredIndexArgs {
    /// One or more PDF documents to ingest.
    #[arg(required = true, value_name = "PDF")]
    pub inputs: Vec<PathBuf>,
    /// Gemini model identifier used for structured extraction.
    #[arg(long, default_value = "gemini-2.5-flash-lite-preview-09-2025")]
    pub extractor_model: String,
    /// Embedding model identifier used for chunk vectors.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
}
```

## File: crates/zetesis-app/src/main.rs
```rust
use std::{
    env, fs, num::NonZeroUsize, ops::Range, path::Path, path::PathBuf, process, sync::Arc,
    time::Duration,
};

use backon::ExponentialBuilder;
use futures_util::stream::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde_json::json;
use thiserror::Error;
use tracing_subscriber::{filter::LevelFilter, fmt};
use zetesis_app::cli::{
    AuditArgs, AuditCommands, Cli, Commands, DEFAULT_KIO_SAOS_URL, DEFAULT_KIO_UZP_URL,
    ExtractStructuredArgs, FetchKioArgs, IndexArgs, IndexCommands, KioSource, OcrPdfArgs,
    OcrProviderKind, SegmentOutputFormat, SegmentPdfArgs, StructuredAuditArgs, StructuredIndexArgs,
};
use zetesis_app::constants::DEFAULT_EMBEDDING_DIM;
use zetesis_app::ingestion::{
    KioEvent, KioSaosScraper, KioScrapeOptions, KioScraperSummary, KioUzpScraper,
};
use zetesis_app::pdf::{PdfRenderError, PdfTextError, extract_text_from_pdf};
use zetesis_app::services::{
    DeepInfraOcr, EmbedService, GeminiEmbedClient, GeminiOcr, Governors, MilliActorHandle,
    OcrConfig, OcrError, OcrInput, OcrMimeType, OcrService, PipelineContext, PipelineError,
    StructuredExtractError, StructuredExtractor, index_structured_decision,
};
use zetesis_app::text::{PolishSentenceSegmenter, cleanup_text};
use zetesis_app::{config, ingestion, paths::AppPaths, pipeline::Silo, server};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let log_level = determine_log_level(&cli);
    init_tracing(log_level);

    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn init_tracing(level: LevelFilter) {
    let subscriber = fmt().with_max_level(level).with_target(false).finish();

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        tracing::warn!("Tracing subscriber already set; skipping re-initialization.");
    }
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] config::AppConfigError),
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
    #[error("failed to read input file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to resolve current working directory: {0}")]
    WorkingDir(#[source] std::io::Error),
    #[error("failed to derive document id from {path}")]
    InvalidDocId { path: PathBuf },
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let verbosity = cli.verbose;

    match cli.command {
        Some(Commands::Serve(_)) => {
            let config = config::load()?;
            server::serve(config).await?;
        }
        Some(Commands::FetchKio(args)) => {
            run_fetch_kio(args, verbosity).await?;
        }
        Some(Commands::SegmentPdf(args)) => {
            run_segment_pdf(args)?;
        }
        Some(Commands::OcrPdf(args)) => {
            run_ocr_pdf(args).await?;
        }
        Some(Commands::ExtractStructured(args)) => {
            run_extract_structured(args).await?;
        }
        Some(Commands::Audit(args)) => {
            run_audit(args).await?;
        }
        Some(Commands::Index(args)) => {
            run_index(args).await?;
        }
        None => {
            Cli::print_help();
        }
    }

    Ok(())
}

async fn run_fetch_kio(args: FetchKioArgs, verbosity: u8) -> Result<(), AppError> {
    let worker_count = NonZeroUsize::new(args.workers.max(1)).expect("workers must always be >= 1");
    let output_dir = resolve_output_dir(&args.output_path)?;
    let base_url = resolve_kio_base_url(&args);

    tracing::info!(
        source = ?args.source,
        url = %base_url,
        limit = ?args.limit,
        workers = worker_count.get(),
        output = %output_dir.display(),
        "starting KIO portal scrape"
    );

    let options = KioScrapeOptions::builder()
        .output_dir(output_dir.clone())
        .worker_count(worker_count)
        .maybe_limit(args.limit)
        .build();
    let progress = (verbosity == 0).then_some(make_progress_bar());
    let mut tracker = ProgressTracker::new(progress.clone(), args.limit.map(|l| l as u64));
    let summary = match args.source {
        KioSource::Uzp => {
            let scraper = KioUzpScraper::new(&base_url)?;
            let mut stream = Box::pin(scraper.scrape_stream(options.clone()));
            process_kio_stream(&mut stream, &mut tracker).await?
        }
        KioSource::Saos => {
            let scraper = KioSaosScraper::new(&base_url)?;
            let mut stream = Box::pin(scraper.scrape_stream(options));
            process_kio_stream(&mut stream, &mut tracker).await?
        }
    };
    finish_kio_progress(progress, &tracker, &summary);

    if !tracker.has_progress_bar() {
        tracing::info!(
            downloaded = summary.downloaded,
            discovered = summary.discovered,
            skipped = summary.skipped_existing,
            "KIO scrape completed successfully"
        );
    }

    Ok(())
}

fn resolve_kio_base_url(args: &FetchKioArgs) -> String {
    match args.source {
        KioSource::Uzp => args.url.clone(),
        KioSource::Saos => {
            if args.url == DEFAULT_KIO_UZP_URL {
                DEFAULT_KIO_SAOS_URL.to_string()
            } else {
                args.url.clone()
            }
        }
    }
}

async fn process_kio_stream<S>(
    stream: &mut S,
    tracker: &mut ProgressTracker,
) -> Result<KioScraperSummary, AppError>
where
    S: Stream<Item = Result<KioEvent, ingestion::IngestorError>> + Unpin,
{
    while let Some(event) = stream.next().await {
        let summary = tracker.handle_event(event?)?;
        if let Some(summary) = summary {
            return Ok(summary);
        }
    }

    Err(AppError::Ingest(ingestion::IngestorError::ChannelClosed))
}

fn finish_kio_progress(
    progress: Option<ProgressBar>,
    tracker: &ProgressTracker,
    summary: &KioScraperSummary,
) {
    if let Some(pb) = progress {
        tracker.ensure_length(&pb, summary.discovered as u64);
        pb.finish_with_message(format!(
            "Completed: {}/{} downloaded ({} skipped)",
            summary.downloaded, summary.discovered, summary.skipped_existing
        ));
    }
}

struct ProgressTracker {
    progress: Option<ProgressBar>,
    target_limit: Option<u64>,
    pb_len_set: bool,
    completed: u64,
}

impl ProgressTracker {
    fn new(progress: Option<ProgressBar>, target_limit: Option<u64>) -> Self {
        Self {
            progress,
            target_limit,
            pb_len_set: false,
            completed: 0,
        }
    }

    fn has_progress_bar(&self) -> bool {
        self.progress.is_some()
    }

    fn handle_event(&mut self, event: KioEvent) -> Result<Option<KioScraperSummary>, AppError> {
        match event {
            KioEvent::DiscoveryStarted { limit } => {
                self.update_length(limit.map(|v| v as u64));
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message("Discovering judgments".to_string());
                } else {
                    tracing::info!(limit, "discovery started");
                }
            }
            KioEvent::Discovered {
                ordinal,
                page,
                total_hint,
                metadata,
            } => {
                self.update_length(total_hint.map(|v| v as u64));
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("queued {} (page {page})", metadata.doc_id));
                } else {
                    tracing::debug!(
                        doc_id = %metadata.doc_id,
                        sygnatura = metadata.sygnatura.as_deref().unwrap_or("unknown"),
                        decision_type = metadata.decision_type.as_deref().unwrap_or("unknown"),
                        ordinal,
                        page,
                        total_hint,
                        "discovered document"
                    );
                }
            }
            KioEvent::WorkerStarted { worker, doc_id } => {
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("processing {doc_id}"));
                } else {
                    tracing::debug!(worker, doc_id = %doc_id, "worker picked up document");
                }
            }
            KioEvent::DownloadSkipped { doc_id } => {
                self.bump_position();
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("cached {doc_id}"));
                } else {
                    tracing::info!(doc_id = %doc_id, "document already cached");
                }
            }
            KioEvent::DownloadCompleted { doc_id, bytes } => {
                self.bump_position();
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("saved {doc_id} ({bytes} bytes)"));
                } else {
                    tracing::info!(doc_id = %doc_id, bytes, "downloaded document");
                }
            }
            KioEvent::Completed { summary } => return Ok(Some(summary)),
        }

        Ok(None)
    }

    fn update_length(&mut self, hint: Option<u64>) {
        if self.pb_len_set {
            return;
        }
        if let Some(pb) = self.progress.as_ref() {
            if let Some(length) = effective_length(self.target_limit, hint) {
                pb.set_length(length.max(1));
                self.pb_len_set = true;
            }
        }
    }

    fn bump_position(&mut self) {
        if let Some(pb) = self.progress.as_ref() {
            self.completed += 1;
            if self.pb_len_set {
                pb.set_position(self.completed);
            } else {
                pb.inc(1);
            }
        }
    }

    fn ensure_length(&self, pb: &ProgressBar, discovered: u64) {
        if self.pb_len_set {
            return;
        }
        let length = self.completed.max(discovered);
        if length > 0 {
            pb.set_length(length);
        }
    }
}

fn run_segment_pdf(args: SegmentPdfArgs) -> Result<(), AppError> {
    for input in &args.inputs {
        let bytes = fs::read(input).map_err(|source| AppError::Io {
            path: input.clone(),
            source,
        })?;

        let raw_text = extract_text_from_pdf(&bytes)?;
        let cleaned = cleanup_text(&raw_text);
        let segments = PolishSentenceSegmenter::ranges(&cleaned)
            .into_iter()
            .map(|range| {
                let content = cleaned[range.clone()].to_string();
                (range, content)
            })
            .filter(|(_, content)| !content.is_empty())
            .collect::<Vec<_>>();

        match args.format {
            SegmentOutputFormat::Text => render_sentences_text(input, &segments, args.with_offsets),
            SegmentOutputFormat::Json => {
                render_sentences_json(input, &segments, args.with_offsets)?
            }
        }
    }

    Ok(())
}

async fn run_ocr_pdf(args: OcrPdfArgs) -> Result<(), AppError> {
    let selected_model = if matches!(args.provider, OcrProviderKind::Gemini)
        && args.model == "allenai/olmOCR-2-7B-1025"
    {
        "gemini-2.5-flash-lite-preview-09-2025".to_string()
    } else {
        args.model.clone()
    };

    let config = OcrConfig {
        model: selected_model.clone(),
        render_width: args.render_width,
        image_max_edge: args.image_max_edge,
        detail: args.detail.clone(),
        max_tokens: args.max_tokens,
        docs_concurrency: NonZeroUsize::new(2).expect("non-zero docs concurrency"),
        pages_concurrency: NonZeroUsize::new(8).expect("non-zero pages concurrency"),
        max_inflight_requests: NonZeroUsize::new(16).expect("non-zero inflight budget"),
    };

    let bytes = fs::read(&args.input).map_err(|source| AppError::Io {
        path: args.input.clone(),
        source,
    })?;
    let bytes: Arc<[u8]> = Arc::from(bytes);

    let mime_type = args
        .input
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| OcrMimeType::from_extension(&ext.to_ascii_lowercase()))
        .ok_or_else(|| {
            AppError::Ocr(OcrError::UnsupportedInput {
                id: args.input.display().to_string(),
                mime_type: "unknown".to_string(),
            })
        })?;

    let input = OcrInput {
        id: args.input.display().to_string(),
        bytes,
        mime_type,
    };
    let inputs = vec![input];
    let mut documents = match args.provider {
        OcrProviderKind::Deepinfra => {
            let service = DeepInfraOcr::from_env()?;
            service.run_batch(inputs, &config).await?
        }
        OcrProviderKind::Gemini => {
            let service = GeminiOcr::from_env(selected_model.clone(), config.max_tokens)?;
            service.run_batch(inputs, &config).await?
        }
    };
    let document = documents.pop().ok_or_else(|| {
        AppError::Ocr(OcrError::UnsupportedInput {
            id: args.input.display().to_string(),
            mime_type: mime_type.to_string(),
        })
    })?;
    let results = document.pages;

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, &results)?;
    println!();

    Ok(())
}

async fn run_extract_structured(args: ExtractStructuredArgs) -> Result<(), AppError> {
    let bytes = fs::read(&args.input).map_err(|source| AppError::Io {
        path: args.input.clone(),
        source,
    })?;
    let text = extract_text_from_pdf(&bytes)?;

    let extractor = StructuredExtractor::from_env(&args.model)?;
    let result = extractor.extract(&text).await?;

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, &result.decision)?;
    println!();

    eprintln!(
        "prompt_tokens={} completion_tokens={} total_tokens={} chunks={}",
        result.usage.input_tokens(),
        result.usage.output_tokens(),
        result.usage.total_tokens(),
        result.decision.chunks.len()
    );

    Ok(())
}

fn render_sentences_text(path: &Path, segments: &[(Range<usize>, String)], with_offsets: bool) {
    println!("== {} ==", path.display());
    let mut first = true;
    for (range, sentence) in segments {
        if !first {
            println!();
        }
        if with_offsets {
            println!("{:>8}..{:>8} {}", range.start, range.end, sentence);
        } else {
            println!("{}", sentence);
        }
        first = false;
    }
    println!();
}

fn render_sentences_json(
    path: &Path,
    segments: &[(Range<usize>, String)],
    with_offsets: bool,
) -> Result<(), AppError> {
    let input = path.display().to_string();
    for (idx, (range, sentence)) in segments.iter().enumerate() {
        let mut payload = serde_json::Map::new();
        payload.insert("input".to_string(), json!(&input));
        payload.insert("ord".to_string(), json!(idx));
        payload.insert("content".to_string(), json!(sentence));

        if with_offsets {
            payload.insert("start".to_string(), json!(range.start));
            payload.insert("end".to_string(), json!(range.end));
        }

        println!("{}", serde_json::Value::Object(payload).to_string());
    }
    Ok(())
}

fn determine_log_level(cli: &Cli) -> LevelFilter {
    match cli.command.as_ref() {
        Some(Commands::FetchKio(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Serve(_)) => match cli.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::SegmentPdf(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::OcrPdf(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::ExtractStructured(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Audit(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Index(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        None => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
    }
}

fn make_progress_bar() -> ProgressBar {
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] {pos}/{len} docs ({eta}) {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    pb.set_draw_target(ProgressDrawTarget::stderr_with_hz(12));
    pb.enable_steady_tick(Duration::from_millis(120));
    pb
}

fn effective_length(limit: Option<u64>, hint: Option<u64>) -> Option<u64> {
    match (limit, hint) {
        (Some(l), Some(h)) => Some(l.min(h)),
        (Some(l), None) => Some(l),
        (None, Some(h)) => Some(h),
        (None, None) => None,
    }
}

fn resolve_output_dir(path: &Path) -> Result<PathBuf, AppError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd = env::current_dir().map_err(AppError::WorkingDir)?;
    Ok(cwd.join(path))
}

async fn run_audit(args: AuditArgs) -> Result<(), AppError> {
    match args.command {
        AuditCommands::Structured(sub) => run_audit_structured(sub).await,
    }
}

async fn run_audit_structured(args: StructuredAuditArgs) -> Result<(), AppError> {
    use std::io::Write;
    let paths = collect_pdfs(&args.dir)?;
    let sample = sample_paths(&paths, args.num);

    let mut out: Box<dyn Write> = if args.out_path == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(
            std::fs::File::create(&args.out_path).map_err(|e| AppError::Io {
                path: PathBuf::from(&args.out_path),
                source: e,
            })?,
        )
    };

    let model = args
        .model
        .unwrap_or_else(|| "gemini-2.5-flash-lite-preview-09-2025".to_string());

    writeln!(out, "pdf\tstatus\tsrc_tokens\tchunk_tokens\tdrift_token_pct\tsrc_chars\tchunk_chars\tdrift_char_pct\tdigits_src\tdigits_chunk\tdrift_digit_pct\tstatute_hits_src\tstatute_hits_chunk\theader_in_chunks\tchunks_count")
        .ok();

    let extractor = StructuredExtractor::from_env(&model)?;
    for path in sample {
        let metrics = audit_one_pdf(&path, &extractor).await;
        render_audit_tsv(&mut out, &path, &metrics).ok();
        // brief pause to avoid hammering upstream
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    Ok(())
}

async fn run_index(args: IndexArgs) -> Result<(), AppError> {
    match args.command {
        IndexCommands::Structured(sub) => run_index_structured(sub).await,
    }
}

async fn run_index_structured(args: StructuredIndexArgs) -> Result<(), AppError> {
    let extractor = StructuredExtractor::from_env(&args.extractor_model)?;
    let ctx = build_pipeline_context(&args.embed_model)?;

    // Spawn a per-silo Milli actor for blocking writes.
    let index = ctx.index_for(Silo::Kio)?;
    let actor = MilliActorHandle::spawn(index, 64);

    for input in &args.inputs {
        let bytes = fs::read(input).map_err(|source| AppError::Io {
            path: input.clone(),
            source,
        })?;
        let text = extract_text_from_pdf(&bytes)?;
        let structured = extractor.extract(&text).await?;
        let doc_id = derive_doc_id(input)?;
        let chunk_count = structured.decision.chunks.len();

        index_structured_decision(&actor, &ctx, Silo::Kio, &doc_id, structured.decision).await?;
        tracing::info!(
            path = %input.display(),
            doc_id,
            chunks = chunk_count,
            "indexed structured decision"
        );
    }

    Ok(())
}

fn build_pipeline_context(embed_model: &str) -> Result<PipelineContext, PipelineError> {
    debug_assert!(!embed_model.is_empty());

    let paths = AppPaths::from_project_dirs()?;
    let client = GeminiEmbedClient::from_env(embed_model.to_string(), DEFAULT_EMBEDDING_DIM)?;
    let embed_client: Arc<dyn zetesis_app::services::EmbedClient> = Arc::new(client);
    let embed = EmbedService {
        embedder_key: embed_model.to_string(),
        dim: DEFAULT_EMBEDDING_DIM,
        client: embed_client,
    };

    Ok(PipelineContext {
        paths,
        embed,
        backoff: ExponentialBuilder::default(),
        governors: Governors::default(),
    })
}

fn derive_doc_id(path: &Path) -> Result<String, AppError> {
    debug_assert!(path.components().count() > 0);

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| AppError::InvalidDocId {
            path: path.to_path_buf(),
        })?;

    if stem.is_empty() {
        return Err(AppError::InvalidDocId {
            path: path.to_path_buf(),
        });
    }

    Ok(stem.trim().to_ascii_lowercase())
}

fn collect_pdfs(dir: &Path) -> Result<Vec<PathBuf>, AppError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|source| AppError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| AppError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "pdf" {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn sample_paths(all: &[PathBuf], n: usize) -> Vec<PathBuf> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut scored: Vec<(u64, &PathBuf)> = Vec::with_capacity(all.len());
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E3779B97F4A7C15);
    for p in all {
        let mut h = DefaultHasher::new();
        seed.hash(&mut h);
        p.hash(&mut h);
        scored.push((h.finish(), p));
    }
    scored.sort_by_key(|(s, _)| *s);
    scored
        .into_iter()
        .take(n.min(all.len()))
        .map(|(_, p)| p.clone())
        .collect()
}

struct AuditMetrics {
    status_ok: bool,
    src_tokens: usize,
    chunk_tokens: usize,
    drift_token_pct: f32,
    src_chars: usize,
    chunk_chars: usize,
    drift_char_pct: f32,
    digits_src: usize,
    digits_chunk: usize,
    drift_digit_pct: f32,
    statute_hits_src: usize,
    statute_hits_chunk: usize,
    header_in_chunks: bool,
    chunks_count: usize,
}

async fn audit_one_pdf(path: &Path, extractor: &StructuredExtractor) -> AuditMetrics {
    let mut base = AuditMetrics {
        status_ok: false,
        src_tokens: 0,
        chunk_tokens: 0,
        drift_token_pct: 0.0,
        src_chars: 0,
        chunk_chars: 0,
        drift_char_pct: 0.0,
        digits_src: 0,
        digits_chunk: 0,
        drift_digit_pct: 0.0,
        statute_hits_src: 0,
        statute_hits_chunk: 0,
        header_in_chunks: false,
        chunks_count: 0,
    };

    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return base,
    };
    let raw_text = match extract_text_from_pdf(&bytes) {
        Ok(t) => t,
        Err(_) => return base,
    };
    let source_text = cleanup_text(&raw_text);

    let extraction = match extractor.extract(&source_text).await {
        Ok(v) => v,
        Err(_) => return base,
    };

    let chunks_text: String = extraction
        .decision
        .chunks
        .iter()
        .map(|c| c.body.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    base.status_ok = true;
    base.src_tokens = count_ws_tokens(&source_text);
    base.chunk_tokens = count_ws_tokens(&chunks_text);
    base.drift_token_pct = drift_pct(base.src_tokens, base.chunk_tokens);
    base.src_chars = source_text.chars().count();
    base.chunk_chars = chunks_text.chars().count();
    base.drift_char_pct = drift_pct(base.src_chars, base.chunk_chars);
    base.digits_src = count_digits(&source_text);
    base.digits_chunk = count_digits(&chunks_text);
    base.drift_digit_pct = drift_pct(base.digits_src, base.digits_chunk);
    base.statute_hits_src = count_statute_hits(&source_text);
    base.statute_hits_chunk = count_statute_hits(&chunks_text);
    base.header_in_chunks = chunks_text.contains("Sygn. akt") || chunks_text.contains("WYROK");
    base.chunks_count = extraction.decision.chunks.len();

    base
}

fn count_ws_tokens(s: &str) -> usize {
    s.split_whitespace().count()
}

fn drift_pct(src: usize, chunk: usize) -> f32 {
    if src == 0 {
        0.0
    } else {
        ((src as f32 - chunk as f32) * 100.0) / src as f32
    }
}

fn count_digits(s: &str) -> usize {
    s.chars().filter(|ch| ch.is_ascii_digit()).count()
}

fn count_statute_hits(s: &str) -> usize {
    // Rough surface cues: art., ust., §, Dz. U.
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"(?i)(art\.|ust\.|§|dz\.?\s*u\.)").expect("statute regex")
    });
    re.find_iter(s).count()
}

fn render_audit_tsv(
    out: &mut dyn std::io::Write,
    path: &Path,
    m: &AuditMetrics,
) -> std::io::Result<()> {
    writeln!(
        out,
        "{}\t{}\t{}\t{}\t{:.1}\t{}\t{}\t{:.1}\t{}\t{}\t{:.1}\t{}\t{}\t{}\t{}",
        path.display(),
        if m.status_ok { "ok" } else { "fail" },
        m.src_tokens,
        m.chunk_tokens,
        m.drift_token_pct,
        m.src_chars,
        m.chunk_chars,
        m.drift_char_pct,
        m.digits_src,
        m.digits_chunk,
        m.drift_digit_pct,
        m.statute_hits_src,
        m.statute_hits_chunk,
        if m.header_in_chunks { "yes" } else { "no" },
        m.chunks_count,
    )
}
```
