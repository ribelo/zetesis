//! Cross-cutting application constants.

/// Default embedder key stored under the Milli `_vectors` field.
pub const DEFAULT_EMBEDDER_KEY: &str = "gemini-embedding-001";

/// Dimensionality of embeddings produced by the default embedder.
pub const DEFAULT_EMBEDDING_DIM: usize = 768;
