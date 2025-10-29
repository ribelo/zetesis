//! Text utilities kept pure for reuse across services and pipelines.
//!
//! Functions and types exposed here must remain side-effect free so they can
//! be composed from orchestrators without introducing hidden IO or mutable
//! state.

pub mod cleanup;
pub mod segmenter;

pub use cleanup::{CleanupOptions, cleanup_text, cleanup_text_with_options};
pub use segmenter::{PolishSentenceSegmenter, PolishSentenceSplit, SegmenterOptions};
