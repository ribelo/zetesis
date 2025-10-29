use crate::pdf::{PdfTextError, extract_text_from_pdf};
use crate::text::{PolishSentenceSplit, cleanup_text};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use strum::{AsRefStr, EnumIter, EnumString};
use thiserror::Error;

/// Minimal representation of a parsed document flowing through the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Doc {
    pub doc_id: String,
    pub text: String,
    pub meta: JsonMap<String, JsonValue>,
}

impl Doc {
    pub fn new(doc_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            text: text.into(),
            meta: JsonMap::new(),
        }
    }
}

/// A silo-specific content chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub section: Option<String>,
    pub ord: u32,
    pub content: String,
    pub meta: JsonMap<String, JsonValue>,
}

impl Chunk {
    pub fn new(section: Option<String>, ord: u32, content: impl Into<String>) -> Self {
        Self {
            section,
            ord,
            content: content.into(),
            meta: JsonMap::new(),
        }
    }
}

/// Flat record to be ingested into Milli.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexRecord {
    pub id: String,
    pub silo: String,
    pub doc_id: String,
    pub section: Option<String>,
    pub ord: u32,
    pub content: String,
    pub attributes: JsonMap<String, JsonValue>,
    #[serde(skip)]
    pub vector: Option<Vec<f32>>,
}

/// Enumeration of supported silos.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsRefStr, EnumIter, Serialize, Deserialize,
)]
#[strum(serialize_all = "kebab-case")]
pub enum Silo {
    Kio,
}

impl Silo {
    pub fn slug(self) -> &'static str {
        match self {
            Silo::Kio => "kio",
        }
    }
}

/// Errors emitted by document processors.
#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("{0}")]
    Message(String),
}

impl ProcessorError {
    pub fn msg(msg: impl Into<String>) -> Self {
        ProcessorError::Message(msg.into())
    }
}

impl From<PdfTextError> for ProcessorError {
    fn from(error: PdfTextError) -> Self {
        ProcessorError::Message(error.to_string())
    }
}

/// Trait implemented by silo-specific processors. All operations are pure and
/// side-effect free.
pub trait SiloDocumentProcessor: Send + Sync {
    fn name(&self) -> &'static str;

    fn preprocess(&self, raw: &[u8]) -> Result<Doc, ProcessorError>;

    fn chunk(&self, doc: &Doc) -> Result<Vec<Chunk>, ProcessorError>;

    fn postprocess(&self, chunks: Vec<Chunk>) -> Result<Vec<Chunk>, ProcessorError> {
        Ok(chunks)
    }
}

/// Conversion from silo-specific chunk to the common index record.
pub trait ToIndexRecord {
    fn to_index_record(&self, silo_slug: &str, doc_id: &str, embedder_key: &str) -> IndexRecord;
}

/// Simple KIO processor stub.
pub struct KioProcessor;

impl KioProcessor {
    pub const fn new() -> Self {
        Self
    }
}

impl SiloDocumentProcessor for KioProcessor {
    fn name(&self) -> &'static str {
        "kio"
    }

    fn preprocess(&self, raw: &[u8]) -> Result<Doc, ProcessorError> {
        let text = extract_text_from_pdf(raw)?;
        let normalized = cleanup_text(&text);
        Ok(Doc::new("", normalized))
    }

    fn chunk(&self, doc: &Doc) -> Result<Vec<Chunk>, ProcessorError> {
        let mut chunks = Vec::new();

        for (idx, sentence) in doc.text.split_polish_sentences().into_iter().enumerate() {
            let trimmed = sentence.trim();
            if trimmed.is_empty() {
                continue;
            }
            chunks.push(Chunk::new(None::<String>, idx as u32, trimmed));
        }

        Ok(chunks)
    }
}

impl ToIndexRecord for Chunk {
    fn to_index_record(&self, silo_slug: &str, doc_id: &str, _embedder_key: &str) -> IndexRecord {
        let mut record = IndexRecord::default();
        record.silo = silo_slug.to_owned();
        record.doc_id = doc_id.to_owned();
        record.section = self.section.clone();
        record.ord = self.ord;
        record.content = self.content.clone();
        record.attributes = self.meta.clone();
        record
    }
}

/// Static processor registry.
pub fn processor_for(silo: Silo) -> &'static dyn SiloDocumentProcessor {
    match silo {
        Silo::Kio => &KIO_PROCESSOR,
    }
}

static KIO_PROCESSOR: KioProcessor = KioProcessor::new();
