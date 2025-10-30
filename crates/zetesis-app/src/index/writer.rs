use std::io::Cursor;
use std::sync::Arc;

use milli::Index;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::progress::EmbedderStats;
use milli::update::{
    DocumentAdditionResult, IndexDocuments, IndexDocumentsConfig, IndexDocumentsMethod,
    IndexerConfig,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use thiserror::Error;

const CANCEL_INDEXING: fn() -> bool = || false;

pub struct IndexWriter {
    index: Index,
    indexer_config: IndexerConfig,
}

impl IndexWriter {
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

    pub fn upsert(
        &self,
        records: &[JsonMap<String, JsonValue>],
    ) -> Result<DocumentAdditionResult, IndexWriteError> {
        debug_assert!(records.len() <= usize::MAX);

        if records.is_empty() {
            let rtxn = self.index.read_txn()?;
            let count = self.index.number_of_documents(&rtxn)?;
            drop(rtxn);
            return Ok(DocumentAdditionResult {
                indexed_documents: 0,
                number_of_documents: count,
            });
        }

        let mut batch_builder = DocumentsBatchBuilder::new(Vec::new());
        for record in records {
            batch_builder.append_json_object(record)?;
        }
        let buffer = batch_builder.into_inner()?;
        let reader = DocumentsBatchReader::from_reader(Cursor::new(buffer))?;

        let mut wtxn = self.index.write_txn()?;
        let config = IndexDocumentsConfig {
            update_method: IndexDocumentsMethod::ReplaceDocuments,
            autogenerate_docids: false,
            ..IndexDocumentsConfig::default()
        };
        let embedder_stats: Arc<EmbedderStats> = Arc::new(EmbedderStats::default());

        let builder = IndexDocuments::new(
            &mut wtxn,
            &self.index,
            &self.indexer_config,
            config,
            |_step| {},
            CANCEL_INDEXING,
            &embedder_stats,
        )?;
        let (builder, addition) = builder.add_documents(reader)?;
        addition?;
        let result = builder.execute()?;
        wtxn.commit()?;
        Ok(result)
    }

    pub fn index(&self) -> &Index {
        &self.index
    }
}

#[derive(Debug, Error)]
pub enum IndexWriteError {
    #[error(transparent)]
    Milli(#[from] milli::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Documents(#[from] milli::documents::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    User(#[from] milli::UserError),
}
