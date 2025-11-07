//! Shared search helpers for CLI and HTTP entrypoints.

use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
};

use milli::score_details::ScoreDetails;
use milli::vector::embedder::{Embedder, EmbedderOptions, manual};
use milli::{AscDesc, Filter, Index as MilliIndex, Search as MilliSearch, all_obkv_to_json};
use serde_json::Value;
use tokio::task;

use crate::constants::DEFAULT_EMBEDDER_KEY;
use crate::error::AppError;
use crate::paths::AppPaths;
use crate::services::{PipelineError, build_pipeline_context};

const LMDB_MAP_SIZE_BYTES: usize = 1 << 30;
static DATA_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
pub const HYBRID_RESULT_LIMIT_MAX: usize = 100;
pub const HYBRID_PER_SOURCE_LIMIT_MAX: usize = 50;
pub const HYBRID_DEFAULT_RRF_K: usize = 60;
pub const HYBRID_DEFAULT_WEIGHT: f32 = 0.5;
const HYBRID_WEIGHT_EPSILON: f32 = f32::EPSILON;

fn data_dir_override() -> &'static Mutex<Option<PathBuf>> {
    DATA_DIR_OVERRIDE.get_or_init(|| Mutex::new(None))
}

#[doc(hidden)]
pub fn set_data_dir_override(path: Option<PathBuf>) {
    let mut guard = data_dir_override()
        .lock()
        .expect("data dir override mutex poisoned");
    *guard = path;
}

#[derive(Debug, Clone)]
pub struct KeywordSearchParams {
    pub index: String,
    pub query: String,
    pub filter: Option<String>,
    pub sort: Vec<String>,
    pub fields: Vec<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub struct VectorSearchParams {
    pub index: String,
    pub query: String,
    pub embedder: Option<String>,
    pub filter: Option<String>,
    pub fields: Vec<String>,
    pub top_k: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum HybridFusion {
    Rrf {
        k: usize,
    },
    Weighted {
        keyword_weight: f32,
        vector_weight: f32,
    },
}

#[derive(Debug, Clone)]
pub struct HybridSearchParams {
    pub keyword: KeywordSearchParams,
    pub vector: VectorSearchParams,
    pub limit: usize,
    pub fusion: HybridFusion,
}

pub fn keyword(params: &KeywordSearchParams) -> Result<Vec<Value>, AppError> {
    debug_assert!(!params.index.trim().is_empty());
    debug_assert!(params.limit <= 100);

    let (_index_name, index_path) = resolve_index_dir(&params.index)?;
    let index = open_index_read_only(&index_path)?;
    let rtxn = index.read_txn()?;
    let fields_map = index.fields_ids_map(&rtxn)?;

    let mut search = MilliSearch::new(&rtxn, &index);
    search.query(&params.query);
    search.offset(params.offset);
    search.limit(params.limit);

    if let Some(filter) = parse_filter(params.filter.as_deref())? {
        search.filter(filter);
    }

    let sort_rules = parse_sort(&params.sort)?;
    if !sort_rules.is_empty() {
        search.sort_criteria(sort_rules);
    }

    let result = search.execute()?;
    let mut rows = Vec::with_capacity(result.documents_ids.len());
    for (idx, docid) in result.documents_ids.iter().enumerate() {
        let document = index.document(&rtxn, *docid)?;
        let value = Value::Object(all_obkv_to_json(document, &fields_map)?);
        let projected = project_value(&value, &params.fields);
        let score = result
            .document_scores
            .get(idx)
            .map(|details: &Vec<ScoreDetails>| ScoreDetails::global_score(details.iter()));
        rows.push(build_search_row(&value, projected, score));
    }
    drop(rtxn);
    Ok(rows)
}

pub async fn vector(params: &VectorSearchParams) -> Result<Vec<Value>, AppError> {
    debug_assert!(params.top_k <= 100);
    debug_assert!(!params.query.trim().is_empty());

    let embedder_key = params
        .embedder
        .as_ref()
        .map(|value| value.as_str())
        .unwrap_or(DEFAULT_EMBEDDER_KEY);
    let index_label = params.index.clone();

    let (_index_name, index_path) = resolve_index_dir(&params.index)?;
    let index = open_index_read_only(&index_path)?;
    let rtxn = index.read_txn()?;
    let fields_map = index.fields_ids_map(&rtxn)?;

    let configs = index.embedding_configs().embedding_configs(&rtxn)?;
    let config = configs
        .iter()
        .find(|cfg| cfg.name == embedder_key)
        .ok_or_else(|| AppError::EmbedderNotFound {
            index: index_label.clone(),
            name: embedder_key.to_string(),
        })?;

    let (manual_opts, quantized) = match &config.config.embedder_options {
        EmbedderOptions::UserProvided(options) => (options.clone(), config.config.quantized()),
        _ => {
            drop(rtxn);
            return Err(AppError::UnsupportedEmbedder {
                index: index_label,
                name: embedder_key.to_string(),
            });
        }
    };

    let ctx = build_pipeline_context(&config.name)?;
    let query_vectors = ctx.embed.embed_queries(&[params.query.as_str()]).await?;
    let vector = query_vectors
        .into_iter()
        .next()
        .unwrap_or_else(|| vec![0.0; ctx.embed.dim]);
    if vector.len() != manual_opts.dimensions {
        drop(rtxn);
        return Err(PipelineError::message(format!(
            "embedding dimension mismatch: expected {}, got {}",
            manual_opts.dimensions,
            vector.len()
        ))
        .into());
    }

    let embedder = Arc::new(Embedder::UserProvided(manual::Embedder::new(manual_opts)));
    let mut search = MilliSearch::new(&rtxn, &index);
    search.limit(params.top_k);
    if let Some(filter) = parse_filter(params.filter.as_deref())? {
        search.filter(filter);
    }
    search.semantic(config.name.clone(), embedder, quantized, Some(vector), None);

    let result = search.execute()?;
    let mut rows = Vec::with_capacity(result.documents_ids.len());
    for (idx, docid) in result.documents_ids.iter().enumerate() {
        let document = index.document(&rtxn, *docid)?;
        let value = Value::Object(all_obkv_to_json(document, &fields_map)?);
        let projected = project_value(&value, &params.fields);
        let score = result
            .document_scores
            .get(idx)
            .map(|details: &Vec<ScoreDetails>| ScoreDetails::global_score(details.iter()));
        rows.push(build_search_row(&value, projected, score));
    }
    drop(rtxn);
    Ok(rows)
}

pub async fn hybrid(params: &HybridSearchParams) -> Result<Vec<Value>, AppError> {
    debug_assert!(params.limit >= 1);
    debug_assert!(params.limit <= HYBRID_RESULT_LIMIT_MAX);
    let keyword_params = params.keyword.clone();
    let vector_params = params.vector.clone();
    let keyword_future = run_keyword_task(keyword_params);
    let vector_future = async move { vector(&vector_params).await };
    let (keyword_rows, vector_rows) = tokio::try_join!(keyword_future, vector_future)?;
    Ok(fuse_hybrid_rows(
        keyword_rows,
        vector_rows,
        params.limit,
        params.fusion,
    ))
}

pub fn normalize_index_name(raw: &str) -> Result<String, AppError> {
    debug_assert!(raw.len() < 128);
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidIndexName {
            name: raw.to_string(),
        });
    }
    Ok(trimmed.to_ascii_lowercase())
}

pub fn resolve_index_dir(raw: &str) -> Result<(String, PathBuf), AppError> {
    debug_assert!(raw.len() < 128);
    let name = normalize_index_name(raw)?;
    let override_path = {
        let guard = data_dir_override()
            .lock()
            .expect("data dir override mutex poisoned");
        guard.clone()
    };
    let paths = if let Some(dir) = override_path {
        AppPaths::new(dir)?
    } else if let Ok(dir) = env::var("ZETESIS_DATA_DIR") {
        AppPaths::new(PathBuf::from(dir))?
    } else {
        AppPaths::from_project_dirs()?
    };
    let base = paths.milli_base_dir()?;
    debug_assert!(base.is_absolute());
    let path = base.join(&name);
    if !path.is_dir() {
        return Err(AppError::MissingIndex {
            index: name.clone(),
            path,
        });
    }
    Ok((name, path))
}

pub fn open_index_read_only(path: &Path) -> Result<MilliIndex, AppError> {
    debug_assert!(path.is_absolute());
    let mut env_opts = heed::EnvOpenOptions::new().read_txn_without_tls();
    env_opts.map_size(LMDB_MAP_SIZE_BYTES);
    MilliIndex::new(env_opts, path, false).map_err(AppError::from)
}

fn parse_filter(expr: Option<&str>) -> Result<Option<Filter<'_>>, AppError> {
    if let Some(raw) = expr {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        return Filter::from_str(trimmed).map_err(|err| AppError::InvalidFilter {
            expression: trimmed.to_string(),
            reason: err.to_string(),
        });
    }
    Ok(None)
}

fn parse_sort(specs: &[String]) -> Result<Vec<AscDesc>, AppError> {
    let mut rules = Vec::new();
    for spec in specs {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            continue;
        }
        match AscDesc::from_str(trimmed) {
            Ok(rule) => rules.push(rule),
            Err(err) => {
                return Err(AppError::InvalidSort {
                    spec: trimmed.to_string(),
                    reason: err.to_string(),
                });
            }
        }
    }
    Ok(rules)
}

pub fn project_value(source: &Value, fields: &[String]) -> Value {
    debug_assert!(fields.len() < 256);
    if fields.is_empty() {
        return source.clone();
    }

    let mut projected = serde_json::Map::new();
    for field in fields {
        let key = field.trim();
        if key.is_empty() {
            continue;
        }
        if let Some(value) = extract_path(source, key) {
            projected.insert(key.to_string(), value);
        }
    }
    Value::Object(projected)
}

fn extract_path(value: &Value, path: &str) -> Option<Value> {
    let mut current = value;
    for segment in path.split('.') {
        let key = segment.trim();
        if key.is_empty() {
            return None;
        }
        match current {
            Value::Object(map) => current = map.get(key)?,
            Value::Array(items) => {
                let index = key.parse::<usize>().ok()?;
                current = items.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

fn number_value(value: f64) -> Option<Value> {
    if !value.is_finite() {
        return None;
    }
    serde_json::Number::from_f64(value).map(Value::Number)
}

pub fn build_search_row(original: &Value, projected: Value, score: Option<f64>) -> Value {
    let mut map = match projected {
        Value::Object(map) => map,
        other => {
            let mut base = serde_json::Map::new();
            base.insert("value".to_string(), other);
            base
        }
    };

    if let Some(source) = original.as_object() {
        for key in ["id", "doc_type", "doc_id"] {
            if let Some(value) = source.get(key) {
                map.entry(key.to_string()).or_insert_with(|| value.clone());
            }
        }
    }

    if let Some(value) = score.and_then(number_value) {
        map.insert("score".to_string(), value);
    }

    Value::Object(map)
}

pub fn normalize_hybrid_weights(
    keyword_weight: f32,
    vector_weight: f32,
) -> Result<(f32, f32), &'static str> {
    if !keyword_weight.is_finite() || !vector_weight.is_finite() {
        return Err("weights must be finite");
    }
    if keyword_weight < 0.0 || vector_weight < 0.0 {
        return Err("weights must be non-negative");
    }
    let sum = keyword_weight + vector_weight;
    if sum <= HYBRID_WEIGHT_EPSILON {
        return Err("weights must not both be zero");
    }
    Ok((keyword_weight / sum, vector_weight / sum))
}

async fn run_keyword_task(params: KeywordSearchParams) -> Result<Vec<Value>, AppError> {
    debug_assert!(!params.index.trim().is_empty());
    task::spawn_blocking(move || keyword(&params))
        .await
        .map_err(|err| join_error("keyword_search", err))?
}

fn fuse_hybrid_rows(
    keyword_rows: Vec<Value>,
    vector_rows: Vec<Value>,
    limit: usize,
    fusion: HybridFusion,
) -> Vec<Value> {
    debug_assert!(limit >= 1);
    debug_assert!(limit <= HYBRID_RESULT_LIMIT_MAX);
    let capacity = keyword_rows.len().saturating_add(vector_rows.len());
    let mut entries: HashMap<String, FusionEntry> = HashMap::with_capacity(capacity);
    ingest_rows(&mut entries, keyword_rows, HybridSource::Keyword);
    ingest_rows(&mut entries, vector_rows, HybridSource::Vector);
    let mut fused: Vec<(String, f64, FusionEntry)> =
        Vec::with_capacity(entries.len().min(limit + 4));
    for (doc_id, mut entry) in entries {
        entry.fused_score = match fusion {
            HybridFusion::Rrf { k } => fused_rrf_score(&entry, k),
            HybridFusion::Weighted {
                keyword_weight,
                vector_weight,
            } => fused_weighted_score(&entry, keyword_weight, vector_weight),
        };
        apply_fused_score(&mut entry.row, entry.fused_score);
        fused.push((doc_id, entry.fused_score, entry));
    }
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| rank_value(&a.2.keyword_rank).cmp(&rank_value(&b.2.keyword_rank)))
            .then_with(|| rank_value(&a.2.vector_rank).cmp(&rank_value(&b.2.vector_rank)))
            .then_with(|| a.0.cmp(&b.0))
    });
    fused.truncate(limit);
    fused.into_iter().map(|(_, _, entry)| entry.row).collect()
}

fn ingest_rows(entries: &mut HashMap<String, FusionEntry>, rows: Vec<Value>, source: HybridSource) {
    debug_assert!(rows.len() <= HYBRID_RESULT_LIMIT_MAX);
    for (rank, row) in rows.into_iter().enumerate() {
        if let Some(doc_id) = extract_doc_identifier(&row) {
            let score = read_score(&row);
            let entry = entries
                .entry(doc_id)
                .or_insert_with(|| FusionEntry::new(row.clone()));
            entry.attach(source, rank, score);
        }
    }
}

fn extract_doc_identifier(row: &Value) -> Option<String> {
    match row {
        Value::Object(map) => map
            .get("doc_id")
            .or_else(|| map.get("id"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string()),
        _ => None,
    }
}

fn read_score(row: &Value) -> Option<f64> {
    match row {
        Value::Object(map) => map.get("score").and_then(|value| value.as_f64()),
        _ => None,
    }
}

fn apply_fused_score(row: &mut Value, fused_score: f64) {
    if let Value::Object(map) = row {
        if let Some(value) = number_value(fused_score) {
            map.insert("score".to_string(), value);
        }
    }
}

fn fused_rrf_score(entry: &FusionEntry, k: usize) -> f64 {
    debug_assert!(k > 0);
    let mut total = 0.0;
    if let Some(rank) = entry.keyword_rank {
        total += reciprocal_rank_score(rank, k);
    }
    if let Some(rank) = entry.vector_rank {
        total += reciprocal_rank_score(rank, k);
    }
    total
}

fn fused_weighted_score(entry: &FusionEntry, keyword_weight: f32, vector_weight: f32) -> f64 {
    debug_assert!(keyword_weight >= 0.0);
    debug_assert!(vector_weight >= 0.0);
    let kw = normalized_component(entry.keyword_score, entry.keyword_rank);
    let vv = normalized_component(entry.vector_score, entry.vector_rank);
    f64::from(keyword_weight) * kw + f64::from(vector_weight) * vv
}

fn normalized_component(score: Option<f64>, rank: Option<usize>) -> f64 {
    if let Some(value) = score {
        if value.is_finite() && value >= 0.0 {
            return value;
        }
    }
    rank.map(|pos| 1.0 / (1.0 + pos as f64)).unwrap_or(0.0)
}

fn reciprocal_rank_score(rank: usize, k: usize) -> f64 {
    let base = (k as f64) + 1.0 + (rank as f64);
    if base <= 0.0 {
        return 0.0;
    }
    1.0 / base
}

fn rank_value(rank: &Option<usize>) -> usize {
    rank.map(|value| value + 1).unwrap_or(usize::MAX)
}

#[derive(Debug)]
struct FusionEntry {
    row: Value,
    keyword_rank: Option<usize>,
    vector_rank: Option<usize>,
    keyword_score: Option<f64>,
    vector_score: Option<f64>,
    fused_score: f64,
}

impl FusionEntry {
    fn new(row: Value) -> Self {
        Self {
            row,
            keyword_rank: None,
            vector_rank: None,
            keyword_score: None,
            vector_score: None,
            fused_score: 0.0,
        }
    }

    fn attach(&mut self, source: HybridSource, rank: usize, score: Option<f64>) {
        match source {
            HybridSource::Keyword => {
                if self.keyword_rank.is_none() {
                    self.keyword_rank = Some(rank);
                    self.keyword_score = score;
                }
            }
            HybridSource::Vector => {
                if self.vector_rank.is_none() {
                    self.vector_rank = Some(rank);
                    self.vector_score = score;
                }
            }
        }
    }
}

#[derive(Copy, Clone)]
enum HybridSource {
    Keyword,
    Vector,
}

fn join_error(task: &str, err: task::JoinError) -> AppError {
    let message = format!("{task} task panicked: {err}");
    PipelineError::message(message).into()
}

#[cfg(test)]
mod tests {
    use super::{
        HYBRID_DEFAULT_RRF_K, HybridFusion, extract_path, fuse_hybrid_rows,
        normalize_hybrid_weights, number_value, project_value,
    };
    use serde_json::{Value, json};

    #[test]
    fn project_value_returns_full_object_when_fields_empty() {
        let value = json!({"a": 1, "b": 2});
        let projected = project_value(&value, &[]);
        assert_eq!(projected, value);
    }

    #[test]
    fn extract_path_handles_nested_arrays() {
        let value = json!({"items": [{"name": "first"}]});
        let result = extract_path(&value, "items.0.name");
        assert_eq!(result, Some(json!("first")));
    }

    #[test]
    fn number_value_filters_non_finite() {
        assert!(number_value(f64::NAN).is_none());
        assert!(number_value(f64::INFINITY).is_none());
        assert_eq!(number_value(1.5), Some(json!(1.5)));
    }

    #[test]
    fn normalize_weights_rejects_zero_sum() {
        let err = normalize_hybrid_weights(0.0, 0.0).unwrap_err();
        assert!(err.contains("zero"));
    }

    #[test]
    fn hybrid_fusion_merges_duplicates() {
        let keyword_rows = vec![row("doc-a", 0.8), row("doc-b", 0.7), row("doc-c", 0.6)];
        let vector_rows = vec![row("doc-b", 0.95), row("doc-d", 0.5)];
        let fused = fuse_hybrid_rows(
            keyword_rows,
            vector_rows,
            4,
            HybridFusion::Rrf {
                k: HYBRID_DEFAULT_RRF_K,
            },
        );
        assert_eq!(fused.len(), 4);
        let ids: Vec<String> = fused
            .into_iter()
            .filter_map(|row| {
                row.get("doc_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect();
        assert!(ids.contains(&"doc-b".to_string()));
        assert!(ids.contains(&"doc-d".to_string()));
    }

    fn row(doc_id: &str, score: f64) -> Value {
        json!({
            "doc_id": doc_id,
            "score": score,
            "value": { "doc_id": doc_id }
        })
    }
}
