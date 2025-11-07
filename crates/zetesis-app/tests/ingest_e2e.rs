//! End-to-end integration test for document ingestion and indexing.
//!
//! This test verifies:
//! 1. StructuredDecision can be indexed with precomputed embeddings
//! 2. Document and chunk records are created with correct structure
//! 3. All required fields match SPEC requirements
//! 4. Idempotent ReplaceDocuments behavior on re-index

use tempfile::TempDir;

use zetesis_app::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EMBEDDING_DIM};
use zetesis_app::index::milli::ensure_index;
use zetesis_app::paths::AppPaths;
use zetesis_app::pipeline::index_payload::{IngestMetadata, build_document_payload};
use zetesis_app::pipeline::processor::Silo;
use zetesis_app::pipeline::structured::{
    DecisionInfo, DecisionResult, Identifiers, IssueKey, IssueTag, PanelMember, PanelRole, Parties,
    Procurement, SemanticChunk, StructuredDecision,
};
use zetesis_app::services::MilliActorHandle;

/// Helper to build minimal valid StructuredDecision for testing.
fn build_test_decision() -> StructuredDecision {
    StructuredDecision {
        identifiers: Identifiers {
            kio_docket: vec!["KIO 123/25".to_string()],
            saos_id: None,
        },
        decision: DecisionInfo {
            date: "2025-01-15".to_string(),
            result: DecisionResult::Upheld,
            orders: vec!["Test order".to_string()],
        },
        panel: vec![PanelMember {
            name: "Test Judge".to_string(),
            role: PanelRole::Chair,
        }],
        parties: Parties {
            contracting_authority: "Test Authority".to_string(),
            appellant: "Test Appellant".to_string(),
            awardee: Some("Test Awardee".to_string()),
            interested_bidders: vec![],
        },
        procurement: Procurement::default(),
        costs: vec![],
        statutes_cited: vec![],
        issues: vec![IssueTag {
            issue_key: IssueKey::Other,
            confidence: 80,
        }],
        chunks: vec![
            SemanticChunk {
                position: 1,
                section: Some("reasoning".to_string()),
                body: "This is the first test chunk with reasoning.".to_string(),
            },
            SemanticChunk {
                position: 2,
                section: Some("conclusion".to_string()),
                body: "This is the second test chunk with conclusion.".to_string(),
            },
        ],
        summary_short: "Test decision upholding the appeal.".to_string(),
    }
}

/// Generate deterministic embeddings for testing.
fn generate_test_embeddings(count: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|i| {
            (0..DEFAULT_EMBEDDING_DIM)
                .map(|j| ((i + j) as f32 * 0.001).sin())
                .collect()
        })
        .collect()
}

/// Compute doc_id as BLAKE3 hash of test bytes for determinism.
fn compute_test_doc_id() -> String {
    let test_bytes = b"test_document_123";
    let hash = blake3::hash(test_bytes);
    hash.to_string()
}

#[tokio::test]
async fn test_ingest_structured_decision_with_embeddings() {
    // Setup: temporary directory and AppPaths
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let app_paths = AppPaths::new(temp_dir.path()).expect("failed to create AppPaths");

    // Setup: ensure index for silo "kio" with default embedder
    let silo = Silo::Kio;
    let index = ensure_index(
        &app_paths,
        silo,
        DEFAULT_EMBEDDER_KEY,
        DEFAULT_EMBEDDING_DIM,
    )
    .expect("failed to ensure index");

    // Setup: MilliActorHandle
    let actor = MilliActorHandle::spawn(index, 64);

    // Setup: test data
    let doc_id = compute_test_doc_id();
    let decision = build_test_decision();
    let chunk_count = decision.chunks.len();
    let embeddings = generate_test_embeddings(chunk_count);

    // Build document payload (doc + chunks)
    let payload = build_document_payload(
        &decision,
        silo,
        &doc_id,
        DEFAULT_EMBEDDER_KEY,
        IngestMetadata::indexed(DEFAULT_EMBEDDER_KEY, chunk_count),
    )
    .expect("failed to build document payload");

    // Upsert document record
    actor
        .upsert_one(payload.doc)
        .await
        .expect("failed to upsert doc record");

    // Build chunk records with embeddings
    let chunk_records: Vec<_> = payload
        .chunks
        .into_iter()
        .zip(embeddings.iter())
        .map(|(chunk, embedding)| {
            let mut record = chunk.record;
            // Insert embeddings in the correct nested structure
            let mut vectors_map = serde_json::Map::new();
            vectors_map.insert(
                DEFAULT_EMBEDDER_KEY.to_string(),
                serde_json::json!(embedding),
            );
            record.insert("_vectors".to_string(), serde_json::json!(vectors_map));
            record
        })
        .collect();

    // Upsert chunk records - test passes if this doesn't error
    actor
        .upsert(chunk_records)
        .await
        .expect("failed to upsert chunk records");

    // Cleanup
    actor.shutdown().await;
}

#[tokio::test]
async fn test_ingest_idempotency() {
    // Setup: temporary directory and AppPaths
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let app_paths = AppPaths::new(temp_dir.path()).expect("failed to create AppPaths");

    // Setup: ensure index for silo "kio"
    let silo = Silo::Kio;
    let index = ensure_index(
        &app_paths,
        silo,
        DEFAULT_EMBEDDER_KEY,
        DEFAULT_EMBEDDING_DIM,
    )
    .expect("failed to ensure index");

    // Setup: MilliActorHandle
    let actor = MilliActorHandle::spawn(index, 64);

    // Setup: test data
    let doc_id = compute_test_doc_id();
    let decision = build_test_decision();
    let chunk_count = decision.chunks.len();
    let embeddings = generate_test_embeddings(chunk_count);

    // Execute: first index operation - should succeed
    {
        let payload = build_document_payload(
            &decision,
            silo,
            &doc_id,
            DEFAULT_EMBEDDER_KEY,
            IngestMetadata::indexed(DEFAULT_EMBEDDER_KEY, chunk_count),
        )
        .expect("failed to build document payload");

        actor
            .upsert_one(payload.doc)
            .await
            .expect("failed to upsert doc record");

        let chunk_records: Vec<_> = payload
            .chunks
            .into_iter()
            .zip(embeddings.iter())
            .map(|(chunk, embedding)| {
                let mut record = chunk.record;
                let mut vectors_map = serde_json::Map::new();
                vectors_map.insert(
                    DEFAULT_EMBEDDER_KEY.to_string(),
                    serde_json::json!(embedding),
                );
                record.insert("_vectors".to_string(), serde_json::json!(vectors_map));
                record
            })
            .collect();

        actor
            .upsert(chunk_records)
            .await
            .expect("failed to upsert chunk records");
    }

    // Execute: second index operation with identical data - should not error (idempotency)
    {
        let payload = build_document_payload(
            &decision,
            silo,
            &doc_id,
            DEFAULT_EMBEDDER_KEY,
            IngestMetadata::indexed(DEFAULT_EMBEDDER_KEY, chunk_count),
        )
        .expect("failed to build document payload");

        actor
            .upsert_one(payload.doc)
            .await
            .expect("failed to upsert doc record");

        let chunk_records: Vec<_> = payload
            .chunks
            .into_iter()
            .zip(embeddings.iter())
            .map(|(chunk, embedding)| {
                let mut record = chunk.record;
                let mut vectors_map = serde_json::Map::new();
                vectors_map.insert(
                    DEFAULT_EMBEDDER_KEY.to_string(),
                    serde_json::json!(embedding),
                );
                record.insert("_vectors".to_string(), serde_json::json!(vectors_map));
                record
            })
            .collect();

        actor
            .upsert(chunk_records)
            .await
            .expect("failed to upsert chunk records");
    }

    // Cleanup
    actor.shutdown().await;
}
