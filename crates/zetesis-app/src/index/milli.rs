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

use crate::paths::{AppPaths, PathError};

const DEFAULT_MAP_SIZE_BYTES: usize = 1 << 30; // 1 GiB
const SEARCHABLE_FIELDS: &[&str] = &[
    "content",
    "summary_short",
    "doc_id",
    "decision.date",
    "decision.result",
    "identifiers.kio_docket",
    "identifiers.saos_id",
    "parties.contracting_authority",
    "parties.appellant",
    "parties.awardee",
    "parties.interested_bidders",
    "panel.names",
    "panel.roles",
    "procurement.subject",
    "procurement.procedure_type",
    "procurement.tender_id",
    "procurement.cpv_codes",
    "issues.issue_key",
    "section",
];
const FILTERABLE_FIELDS: &[&str] = &[
    "silo",
    "doc_type",
    "doc_id",
    "decision_date",
    "decision_result",
    "decision.date",
    "decision.result",
    "parties_contracting_authority",
    "parties_appellant",
    "parties_awardee",
    "parties_interested_bidders",
    "parties.contracting_authority",
    "parties.appellant",
    "parties.awardee",
    "parties.interested_bidders",
    "procurement_subject",
    "procurement_procedure_type",
    "procurement_tender_id",
    "procurement_cpv_codes",
    "procurement.subject",
    "procurement.procedure_type",
    "procurement.tender_id",
    "procurement.cpv_codes",
    "identifiers_kio_docket",
    "identifiers_saos_id",
    "identifiers.kio_docket",
    "identifiers.saos_id",
    "panel_names",
    "panel_roles",
    "panel.names",
    "panel.roles",
    "issues_keys",
    "issues.issue_key",
    "section",
];
const SORTABLE_FIELDS: &[&str] = &["decision_date", "doc_id"];

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
    embedder_key: &str,
    embed_dim: usize,
) -> Result<Index, MilliBootstrapError> {
    open_index(paths, silo.as_ref(), embedder_key, embed_dim, true)
}

pub fn open_existing_index<S: AsRef<str>>(
    paths: &AppPaths,
    silo: S,
    embedder_key: &str,
    embed_dim: usize,
) -> Result<Index, MilliBootstrapError> {
    open_index(paths, silo.as_ref(), embedder_key, embed_dim, false)
}

fn open_index(
    paths: &AppPaths,
    silo: &str,
    embedder_key: &str,
    embed_dim: usize,
    create: bool,
) -> Result<Index, MilliBootstrapError> {
    let path = if create {
        paths.milli_index_dir(silo)?
    } else {
        milli_index_path(paths, silo)?
    };
    let mut env_opts = EnvOpenOptions::new().read_txn_without_tls();
    env_opts.map_size(DEFAULT_MAP_SIZE_BYTES);
    let index = Index::new(env_opts, &path, create)?;
    configure_index(&index, embedder_key, embed_dim)?;
    Ok(index)
}

fn milli_index_path(paths: &AppPaths, silo: &str) -> Result<PathBuf, PathError> {
    let mut base = paths.data_dir();
    base.push("milli");
    base.push(slugify(silo));
    Ok(base)
}

fn slugify(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn configure_index(
    index: &Index,
    embedder_key: &str,
    embed_dim: usize,
) -> Result<(), MilliBootstrapError> {
    let rtxn = index.read_txn()?;
    let current_backend = index.get_vector_store(&rtxn)?;
    let current_primary = index.primary_key(&rtxn)?.map(|pk| pk.to_owned());
    let current_searchable: Vec<String> = index
        .searchable_fields(&rtxn)?
        .into_iter()
        .map(|field| field.into_owned())
        .collect();
    let embedding_configs = index.embedding_configs();
    let existing_configs = embedding_configs.embedding_configs(&rtxn)?;
    let has_expected_embedder = existing_configs.iter().any(|cfg| {
        cfg.name == embedder_key
            && matches!(
                &cfg.config.embedder_options,
                milli::vector::EmbedderOptions::UserProvided(opts)
                if opts.dimensions == embed_dim
            )
            && !cfg.config.quantized.unwrap_or(false)
    });
    drop(rtxn);

    let needs_primary_key = current_primary
        .as_deref()
        .map(|pk| pk != "id")
        .unwrap_or(true);
    let desired_searchable: Vec<String> = SEARCHABLE_FIELDS
        .iter()
        .map(|field| field.to_string())
        .collect();

    if current_backend == Some(VectorStoreBackend::Hannoy)
        && has_expected_embedder
        && !needs_primary_key
        && current_searchable == desired_searchable
    {
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
        embedder_settings.dimensions = MilliSetting::Set(embed_dim);
        embedder_settings.binary_quantized = MilliSetting::Set(false);

        let mut map = BTreeMap::new();
        map.insert(
            embedder_key.to_string(),
            MilliSetting::Set(embedder_settings),
        );
        settings.set_embedder_settings(map);
    }

    if needs_primary_key {
        settings.set_primary_key("id".to_string());
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
