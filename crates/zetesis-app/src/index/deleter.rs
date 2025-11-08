use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bumpalo::Bump;
use milli::progress::{EmbedderStats, Progress};
use milli::prompt::Prompt;
use milli::update::IndexerConfig;
use milli::update::new::indexer;
use milli::vector::{Embedder, RuntimeEmbedder, RuntimeEmbedders, RuntimeFragment};
use milli::{Filter, Index, Search, ThreadPoolNoAbortBuilder};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::warn;

use crate::index::writer::IndexWriteError;

const MAX_MATCHING_RECORDS: usize = 50_000;

/// Summary returned both for dry-run previews and executed deletions.
#[derive(Debug, Clone)]
pub struct DeletionInfo {
    pub record_count: usize,
    pub num_chunks: usize,
    pub blob_cids: Vec<String>,
}

/// Captures everything needed to execute a deletion transaction.
#[derive(Debug)]
pub struct DeletionPlan {
    doc_id: String,
    external_ids: Vec<String>,
    info: DeletionInfo,
}

impl DeletionPlan {
    pub fn doc_id(&self) -> &str {
        self.doc_id.as_str()
    }

    pub fn info(&self) -> &DeletionInfo {
        &self.info
    }

    pub fn blob_cids(&self) -> &[String] {
        self.info.blob_cids.as_slice()
    }

    fn into_parts(self) -> (Vec<String>, DeletionInfo) {
        (self.external_ids, self.info)
    }
}

pub struct DocumentDeleter {
    index: Index,
    indexer_config: IndexerConfig,
}

impl DocumentDeleter {
    pub fn new(index: Index) -> Self {
        debug_assert!(std::mem::size_of::<Index>() > 0);
        if let Ok(rtxn) = index.read_txn() {
            debug_assert!(index.fields_ids_map(&rtxn).is_ok());
        }
        Self {
            index,
            indexer_config: IndexerConfig::default(),
        }
    }

    /// Inspect the index and prepare a deletion plan without mutating state.
    pub fn plan(&self, doc_id: &str) -> Result<Option<DeletionPlan>, IndexWriteError> {
        debug_assert!(MAX_MATCHING_RECORDS > 0);
        if doc_id.trim().is_empty() {
            return Ok(None);
        }

        let rtxn = self.index.read_txn()?;
        let filter_expr = format!("doc_id = \"{}\"", doc_id);
        let filter =
            Filter::from_str(&filter_expr).map_err(|e| IndexWriteError::Milli(Box::new(e)))?;

        let mut search = Search::new(&rtxn, &self.index);
        if let Some(f) = filter {
            search.filter(f);
        }
        search.limit(MAX_MATCHING_RECORDS);

        let result = search.execute()?;
        if result.documents_ids.is_empty() {
            return Ok(None);
        }
        if result.documents_ids.len() == MAX_MATCHING_RECORDS {
            warn!(
                doc_id,
                limit = MAX_MATCHING_RECORDS,
                "deletion hit record limit; potential truncation"
            );
        }

        let fields_map = self.index.fields_ids_map(&rtxn)?;
        let mut external_ids = Vec::with_capacity(result.documents_ids.len());
        let mut blob_cids = Vec::new();
        let mut chunk_count = 0usize;

        for docid in result.documents_ids {
            let obkv = self.index.document(&rtxn, docid)?;
            let doc_map: JsonMap<String, JsonValue> = milli::all_obkv_to_json(obkv, &fields_map)?;
            if let Some(ext_id) = doc_map.get("id").and_then(|v| v.as_str()) {
                external_ids.push(ext_id.to_string());
            } else {
                warn!(doc_id, "skipping record without external id");
                continue;
            }

            if matches!(doc_map.get("doc_type"), Some(JsonValue::String(kind)) if kind == "chunk") {
                chunk_count = chunk_count.saturating_add(1);
            }

            collect_blob_cids(&doc_map, &mut blob_cids);
        }

        drop(rtxn);
        dedup_stable(&mut blob_cids);
        let info = DeletionInfo {
            record_count: external_ids.len(),
            num_chunks: chunk_count,
            blob_cids,
        };

        Ok(Some(DeletionPlan {
            doc_id: doc_id.to_string(),
            external_ids,
            info,
        }))
    }

    /// Execute a previously prepared plan inside a Milli write transaction.
    pub fn execute(&self, plan: DeletionPlan) -> Result<DeletionInfo, IndexWriteError> {
        let (external_ids, info) = plan.into_parts();
        if external_ids.is_empty() {
            return Ok(info);
        }
        debug_assert!(external_ids.iter().all(|id| !id.is_empty()));

        let mut wtxn = self.index.write_txn()?;
        let mut doc_operation = indexer::DocumentOperation::new();
        let ext_refs: Vec<&str> = external_ids.iter().map(|s| s.as_str()).collect();
        doc_operation.delete_documents(ext_refs.as_slice());

        let alloc = Bump::new();
        let extraction_progress = Progress::default();
        let rtxn = self.index.read_txn()?;
        let db_fields_ids_map = self.index.fields_ids_map(&rtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();
        let runtime_embedders = runtime_embedders_from_index(&self.index, &rtxn)?;
        let (document_changes, operation_stats, primary_key) = doc_operation.into_changes(
            &alloc,
            &self.index,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            extraction_progress,
            None,
        )?;

        for stat in operation_stats {
            if let Some(error) = stat.error {
                return Err(IndexWriteError::from(error));
            }
        }

        let pool = ThreadPoolNoAbortBuilder::new()
            .build()
            .expect("thread pool initialization should not fail");
        let indexing_progress = Progress::default();
        let embedder_stats = EmbedderStats::default();

        self.indexer_config
            .thread_pool
            .install(|| {
                indexer::index(
                    &mut wtxn,
                    &self.index,
                    &pool,
                    self.indexer_config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    runtime_embedders,
                    &|| false,
                    &indexing_progress,
                    &embedder_stats,
                )
            })
            .unwrap()?;

        wtxn.commit()?;
        Ok(info)
    }
}

fn runtime_embedders_from_index(
    index: &Index,
    rtxn: &heed::RoTxn<'_>,
) -> Result<RuntimeEmbedders, IndexWriteError> {
    let configs = index.embedding_configs().embedding_configs(rtxn)?;
    if configs.is_empty() {
        return Ok(RuntimeEmbedders::default());
    }

    let mut data = HashMap::with_capacity(configs.len());
    for cfg in configs {
        let milli::vector::db::IndexEmbeddingConfig { name, config, .. } = cfg;
        let milli::vector::EmbeddingConfig {
            embedder_options,
            prompt,
            quantized,
        } = config;

        let document_template = Prompt::try_from(prompt)
            .map_err(milli::Error::from)
            .map_err(IndexWriteError::from)?;
        let embedder = Arc::new(
            Embedder::new(embedder_options.clone(), 0)
                .map_err(milli::vector::Error::from)
                .map_err(milli::Error::from)
                .map_err(IndexWriteError::from)?,
        );

        let runtime = Arc::new(RuntimeEmbedder::new(
            embedder,
            document_template,
            Vec::<RuntimeFragment>::new(),
            quantized.unwrap_or_default(),
        ));
        data.insert(name, runtime);
    }

    Ok(RuntimeEmbedders::new(data))
}

fn dedup_stable(values: &mut Vec<String>) {
    let mut seen = HashSet::with_capacity(values.len());
    values.retain(|val| seen.insert(val.clone()));
}

fn collect_blob_cids(record: &JsonMap<String, JsonValue>, cids: &mut Vec<String>) {
    let initial_len = cids.len();
    if let Some(value) = record.get("blob_cids") {
        extract_blob_cids_from_value(value, cids);
    }
    if let Some(JsonValue::Object(ingest)) = record.get("ingest") {
        if let Some(value) = ingest.get("blob_cids") {
            extract_blob_cids_from_value(value, cids);
        }
        if let Some(JsonValue::Object(blobs)) = ingest.get("blobs") {
            for nested in blobs.values() {
                extract_blob_cids_from_value(nested, cids);
            }
        }
    }
    if let Some(JsonValue::Object(structured)) = record.get("structured") {
        extract_blob_cids_recursive(structured, cids);
    }
    if initial_len == cids.len() {
        if let Some(JsonValue::String(doc_id)) = record.get("doc_id") {
            push_if_valid(doc_id, cids);
        }
    }
}

fn extract_blob_cids_from_value(value: &JsonValue, cids: &mut Vec<String>) {
    match value {
        JsonValue::String(s) => push_if_valid(s, cids),
        JsonValue::Array(items) => {
            for item in items {
                extract_blob_cids_from_value(item, cids);
            }
        }
        JsonValue::Object(map) => {
            for nested in map.values() {
                extract_blob_cids_from_value(nested, cids);
            }
        }
        _ => {}
    }
}

fn extract_blob_cids_recursive(obj: &JsonMap<String, JsonValue>, cids: &mut Vec<String>) {
    for (key, value) in obj {
        let key_lower = key.to_lowercase();
        if (key_lower.contains("cid") || key_lower.contains("blob"))
            && !key_lower.contains("doc_id")
        {
            if let JsonValue::String(s) = value {
                push_if_valid(s, cids);
            }
        }

        match value {
            JsonValue::Object(nested) => extract_blob_cids_recursive(nested, cids),
            JsonValue::Array(arr) => {
                for item in arr {
                    match item {
                        JsonValue::Object(nested) => extract_blob_cids_recursive(nested, cids),
                        JsonValue::String(s) => push_if_valid(s, cids),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

fn is_valid_cid(candidate: &str) -> bool {
    let len_ok = (32..=128).contains(&candidate.len());
    let chars_ok = candidate
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase());
    len_ok && chars_ok
}

fn push_if_valid(candidate: &str, cids: &mut Vec<String>) {
    if is_valid_cid(candidate) {
        cids.push(candidate.to_string());
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map as JsonMap, Value as JsonValue, json};
    use tempfile::TempDir;

    use super::*;
    use milli::FilterableAttributesRule;
    use crate::index::writer::IndexWriter;

    fn sample_records(doc_id: &str) -> Vec<JsonMap<String, JsonValue>> {
        let cid = "0123456789abcdef0123456789abcdef";
        vec![
            serde_json::from_value(json!({
                "id": format!("kio-{doc_id}-doc"),
                "doc_id": doc_id,
                "doc_type": "doc",
                "blob_cids": [cid],
                "structured": {
                    "artifact_blob": cid,
                    "other": "value"
                }
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "id": format!("kio-{doc_id}-chunk-0001"),
                "doc_id": doc_id,
                "doc_type": "chunk",
                "section": "body",
            }))
            .unwrap(),
        ]
    }

    fn test_index() -> (TempDir, Index) {
        let dir = TempDir::new().unwrap();
        let mut env_opts = heed::EnvOpenOptions::new().read_txn_without_tls();
        env_opts.map_size(1 << 26);
        let index = Index::new(env_opts, dir.path(), true).unwrap();
        {
            let mut wtxn = index.write_txn().unwrap();
            let indexer_cfg = IndexerConfig::default();
            let mut settings = milli::update::Settings::new(&mut wtxn, &index, &indexer_cfg);
            settings.set_primary_key("id".to_string());
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("doc_id".to_string())]);
            settings
                .execute(
                    &|| false,
                    &Progress::default(),
                    Arc::new(EmbedderStats::default()),
                )
                .unwrap();
            wtxn.commit().unwrap();
        }
        (dir, index)
    }

    #[test]
    fn plan_missing_doc() {
        let (_dir, index) = test_index();
        let deleter = DocumentDeleter::new(index);
        assert!(deleter.plan("missing").unwrap().is_none());
    }

    #[test]
    fn plan_and_execute_deletes_records() {
        let (_dir, index) = test_index();
        let writer = IndexWriter::new(index.clone());
        writer.upsert(&sample_records("doc123")).unwrap();

        let deleter = DocumentDeleter::new(index.clone());
        let plan = deleter.plan("doc123").unwrap().expect("plan missing");
        assert_eq!(plan.info().record_count, 2);
        assert_eq!(plan.info().num_chunks, 1);
        assert_eq!(plan.blob_cids().len(), 1);

        let summary = deleter.execute(plan).unwrap();
        assert_eq!(summary.record_count, 2);

        // verify search returns nothing
        let rtxn = index.read_txn().unwrap();
        let mut search = Search::new(&rtxn, &index);
        let filter = Filter::from_str("doc_id = \"doc123\"").unwrap();
        if let Some(f) = filter {
            search.filter(f);
        }
        let result = search.execute().unwrap();
        assert!(result.documents_ids.is_empty());
    }

    #[test]
    fn collect_blob_cids_reads_ingest_metadata() {
        let mut record = JsonMap::new();
        let mut ingest = JsonMap::new();
        ingest.insert(
            "blob_cids".to_string(),
            json!(["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]),
        );
        record.insert("ingest".to_string(), JsonValue::Object(ingest));
        let mut values = Vec::new();
        collect_blob_cids(&record, &mut values);
        assert_eq!(values, vec!["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()]);
    }

    #[test]
    fn collect_blob_cids_falls_back_to_structured() {
        let mut record = JsonMap::new();
        record.insert(
            "structured".to_string(),
            json!({
                "assets": {
                    "primary_blob": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                }
            }),
        );
        let mut values = Vec::new();
        collect_blob_cids(&record, &mut values);
        assert_eq!(values, vec!["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()]);
    }

    #[test]
    fn collect_blob_cids_falls_back_to_doc_id() {
        let mut record = JsonMap::new();
        record.insert(
            "doc_id".to_string(),
            JsonValue::String("cccccccccccccccccccccccccccccccc".to_string()),
        );
        let mut values = Vec::new();
        collect_blob_cids(&record, &mut values);
        assert_eq!(values, vec!["cccccccccccccccccccccccccccccccc".to_string()]);
    }
}
