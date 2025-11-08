use std::time::Duration;

use blake3;
use milli::{Filter, Search as MilliSearch, all_obkv_to_json};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::pipeline::Silo;
use crate::pipeline::index_payload::{
    IngestMetadata, PreparedChunkRecord, PreparedDocumentRecords, build_doc_record,
    build_document_payload,
};
use crate::pipeline::structured::StructuredDecision;
use crate::services::context::{PipelineContext, PipelineError, PipelineResult};
use crate::services::milli_actor::MilliActorHandle;

const MAX_EMBED_ATTEMPTS: u8 = 3;
const INITIAL_EMBED_DELAY_MS: u64 = 200;

pub fn load_structured_decision(
    ctx: &PipelineContext,
    silo: Silo,
    doc_id: &str,
) -> PipelineResult<StructuredDecision> {
    let index = ctx.index_for(silo)?;
    let rtxn = index
        .read_txn()
        .map_err(|err| PipelineError::message(err.to_string()))?;
    let mut search = MilliSearch::new(&rtxn, &index);
    let filter_expr = format!("doc_id = \"{}\" AND doc_type = \"doc\"", doc_id);
    if let Some(filter) =
        Filter::from_str(&filter_expr).map_err(|err| PipelineError::message(err.to_string()))?
    {
        search.filter(filter);
    }
    search.limit(1);
    let result = search
        .execute()
        .map_err(|err| PipelineError::message(err.to_string()))?;
    if result.documents_ids.is_empty() {
        return Err(PipelineError::message(format!(
            "document `{}` not found in index",
            doc_id
        )));
    }
    let docid = result.documents_ids[0];
    let fields = index
        .fields_ids_map(&rtxn)
        .map_err(|err| PipelineError::message(err.to_string()))?;
    let raw = index
        .document(&rtxn, docid)
        .map_err(|err| PipelineError::message(err.to_string()))?;
    let map: JsonMap<String, JsonValue> =
        all_obkv_to_json(raw, &fields).map_err(|err| PipelineError::message(err.to_string()))?;
    let structured = map
        .get("structured")
        .cloned()
        .ok_or_else(|| PipelineError::message("document missing `structured` field"))?;
    let decision = serde_json::from_value(structured)
        .map_err(|err| PipelineError::message(err.to_string()))?;
    Ok(decision)
}

pub fn decision_content_hash(decision: &StructuredDecision) -> Result<String, serde_json::Error> {
    let serialized = serde_json::to_vec(decision)?;
    Ok(blake3::hash(&serialized).to_hex().to_string())
}

pub async fn index_structured_decision(
    actor: &MilliActorHandle,
    ctx: &PipelineContext,
    silo: Silo,
    doc_id: &str,
    decision: StructuredDecision,
) -> PipelineResult<()> {
    debug_assert!(!doc_id.is_empty());
    debug_assert!(!decision.chunks.is_empty());

    let pending = build_document_payload(
        &decision,
        silo,
        doc_id,
        &[doc_id],
        &ctx.embed.embedder_key,
        IngestMetadata::pending(),
    )
    .map_err(|err| PipelineError::message(err.to_string()))?;
    let PreparedDocumentRecords {
        doc: pending_doc,
        chunks: prepared_chunks,
    } = pending;
    actor.upsert_one(pending_doc).await?;

    let chunk_records = match vectorize_chunks(ctx, &prepared_chunks).await {
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
                &[doc_id],
                &ctx.embed.embedder_key,
                IngestMetadata::errored(&message),
            )
            .map_err(|err| PipelineError::message(err.to_string()))?;
            if let Err(write_err) = actor.upsert_one(error_doc).await {
                warn!(
                    doc_id,
                    silo = silo.slug(),
                    error = %write_err,
                    "failed to persist error state after vectorization failure"
                );
            }
            return Err(err);
        }
    };

    let chunk_count = chunk_records.len();
    if chunk_count > 0
        && let Err(err) = actor.upsert(chunk_records).await
    {
        let message = format!("chunk upsert failed: {err}");
        warn!(
            doc_id,
            silo = silo.slug(),
            error = %message,
            "structured chunk write failed"
        );
        mark_ingest_error(
            actor,
            &decision,
            silo,
            doc_id,
            &ctx.embed.embedder_key,
            &message,
        )
        .await;
        return Err(err);
    }

    let indexed_doc = build_doc_record(
        &decision,
        silo,
        doc_id,
        &[doc_id],
        &ctx.embed.embedder_key,
        IngestMetadata::indexed(&ctx.embed.embedder_key, chunk_count),
    )
    .map_err(|err| PipelineError::message(err.to_string()))?;
    if let Err(err) = actor.upsert_one(indexed_doc).await {
        let message = format!("final doc upsert failed: {err}");
        warn!(
            doc_id,
            silo = silo.slug(),
            error = %message,
            "structured doc write failed"
        );
        mark_ingest_error(
            actor,
            &decision,
            silo,
            doc_id,
            &ctx.embed.embedder_key,
            &message,
        )
        .await;
        return Err(err);
    }

    debug!(
        doc_id,
        silo = silo.slug(),
        chunks = chunk_count,
        "structured decision indexed"
    );
    Ok(())
}

pub async fn index_structured_with_embeddings(
    actor: &MilliActorHandle,
    ctx: &PipelineContext,
    silo: Silo,
    doc_id: &str,
    decision: StructuredDecision,
    embeddings: Vec<Vec<f32>>,
) -> PipelineResult<()> {
    debug_assert!(!doc_id.is_empty());
    debug_assert!(!decision.chunks.is_empty());

    let pending = build_document_payload(
        &decision,
        silo,
        doc_id,
        &[doc_id],
        &ctx.embed.embedder_key,
        IngestMetadata::pending(),
    )
    .map_err(|err| PipelineError::message(err.to_string()))?;
    let PreparedDocumentRecords {
        doc: pending_doc,
        chunks: prepared_chunks,
    } = pending;
    actor.upsert_one(pending_doc).await?;

    let chunk_records = match chunk_records_with_vectors(ctx, &prepared_chunks, &embeddings) {
        Ok(records) => records,
        Err(err) => {
            let message = err.to_string();
            warn!(
                doc_id,
                silo = silo.slug(),
                error = %message,
                "precomputed vector ingestion failed"
            );
            let error_doc = build_doc_record(
                &decision,
                silo,
                doc_id,
                &[doc_id],
                &ctx.embed.embedder_key,
                IngestMetadata::errored(&message),
            )
            .map_err(|inner| PipelineError::message(inner.to_string()))?;
            if let Err(write_err) = actor.upsert_one(error_doc).await {
                warn!(
                    doc_id,
                    silo = silo.slug(),
                    error = %write_err,
                    "failed to persist error state after precomputed vector failure"
                );
            }
            return Err(err);
        }
    };

    let chunk_count = chunk_records.len();
    if chunk_count > 0
        && let Err(err) = actor.upsert(chunk_records).await
    {
        let message = format!("chunk upsert failed: {err}");
        warn!(
            doc_id,
            silo = silo.slug(),
            error = %message,
            "structured chunk write failed"
        );
        mark_ingest_error(
            actor,
            &decision,
            silo,
            doc_id,
            &ctx.embed.embedder_key,
            &message,
        )
        .await;
        return Err(err);
    }

    let indexed_doc = build_doc_record(
        &decision,
        silo,
        doc_id,
        &[doc_id],
        &ctx.embed.embedder_key,
        IngestMetadata::indexed(&ctx.embed.embedder_key, chunk_count),
    )
    .map_err(|err| PipelineError::message(err.to_string()))?;
    if let Err(err) = actor.upsert_one(indexed_doc).await {
        let message = format!("final doc upsert failed: {err}");
        warn!(
            doc_id,
            silo = silo.slug(),
            error = %message,
            "structured doc write failed"
        );
        mark_ingest_error(
            actor,
            &decision,
            silo,
            doc_id,
            &ctx.embed.embedder_key,
            &message,
        )
        .await;
        return Err(err);
    }

    debug!(
        doc_id,
        silo = silo.slug(),
        chunks = chunk_count,
        "structured decision ingested with precomputed embeddings"
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
        match ctx.embed.embed_documents(&bodies).await {
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
    chunk_records_with_vectors(ctx, chunks, &embeddings)
}

fn chunk_records_with_vectors(
    ctx: &PipelineContext,
    chunks: &[PreparedChunkRecord],
    embeddings: &[Vec<f32>],
) -> PipelineResult<Vec<JsonMap<String, JsonValue>>> {
    debug_assert!(chunks.len() < usize::MAX);
    if chunks.len() != embeddings.len() {
        return Err(PipelineError::message(format!(
            "embedding batch size mismatch: expected {}, got {}",
            chunks.len(),
            embeddings.len()
        )));
    }

    let mut out: Vec<JsonMap<String, JsonValue>> = Vec::with_capacity(chunks.len());
    for (prepared, vector) in chunks.iter().zip(embeddings.iter()) {
        if vector.len() != ctx.embed.dim {
            return Err(PipelineError::message(format!(
                "embedding dimension mismatch: expected {}, got {}",
                ctx.embed.dim,
                vector.len()
            )));
        }
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
    debug_assert!(!vector.is_empty());

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

async fn mark_ingest_error(
    actor: &MilliActorHandle,
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
    embedder_key: &str,
    message: &str,
) {
    match build_doc_record(
        decision,
        silo,
        doc_id,
        &[doc_id],
        embedder_key,
        IngestMetadata::errored(message),
    ) {
        Ok(doc) => {
            if let Err(err) = actor.upsert_one(doc).await {
                warn!(
                    doc_id,
                    silo = silo.slug(),
                    error = %err,
                    "failed to persist ingest error status"
                );
            }
        }
        Err(err) => warn!(
            doc_id,
            silo = silo.slug(),
            error = %err,
            "failed to build ingest error payload"
        ),
    }
}
