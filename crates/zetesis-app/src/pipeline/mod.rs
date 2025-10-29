//! Pure pipeline transformations that operate on document data.
//!
//! Modules under this namespace must remain free of IO and external side effects
//! so they can be reused across batch orchestrators and test harnesses.

pub mod processor;
pub mod structured;

pub use processor::{
    Chunk, Doc, IndexRecord, KioProcessor, ProcessorError, Silo, SiloDocumentProcessor,
    ToIndexRecord, processor_for,
};
