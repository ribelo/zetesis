//! Milli index bootstrap helpers.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use heed::EnvOpenOptions;
use milli::progress::{EmbedderStats, Progress};
use milli::update::{IndexerConfig, Setting as MilliSetting, Settings};
use milli::vector::VectorStoreBackend;
use milli::vector::settings::{EmbedderSource, EmbeddingSettings};
use milli::{Index, Result as MilliResult};
use thiserror::Error;

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EMBEDDING_DIM};
use crate::paths::{AppPaths, PathError};

const DEFAULT_MAP_SIZE_BYTES: usize = 1 << 30; // 1 GiB

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
