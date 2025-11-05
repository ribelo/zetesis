//! Shared search helpers for CLI and HTTP entrypoints.

use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
};

use milli::score_details::ScoreDetails;
use milli::vector::embedder::{Embedder, EmbedderOptions, manual};
use milli::{AscDesc, Filter, Index as MilliIndex, Search as MilliSearch, all_obkv_to_json};
use serde_json::Value;

use crate::constants::DEFAULT_EMBEDDER_KEY;
use crate::error::AppError;
use crate::paths::AppPaths;
use crate::services::{PipelineError, build_pipeline_context};

const LMDB_MAP_SIZE_BYTES: usize = 1 << 30;
static DATA_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

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

#[cfg(test)]
mod tests {
    use super::{extract_path, number_value, project_value};
    use serde_json::json;

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
}
