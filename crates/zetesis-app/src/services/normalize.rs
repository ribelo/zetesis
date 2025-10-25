use crate::services::context::{PipelineContext, PipelineResult};
use crate::services::processor::{Chunk, Doc};

/// Deprecated placeholder; consult the owner before introducing document normalizers.
#[deprecated(
    note = "DocNormalizer is currently deprecated. Ask the project owner before using or extending it."
)]
pub trait DocNormalizer: Send + Sync {
    fn name(&self) -> &'static str;

    fn apply(&self, doc: &mut Doc, ctx: &PipelineContext) -> PipelineResult<()>;
}

/// Deprecated placeholder; consult the owner before introducing chunk normalizers.
#[deprecated(
    note = "ChunkNormalizer is currently deprecated. Ask the project owner before using or extending it."
)]
pub trait ChunkNormalizer: Send + Sync {
    fn name(&self) -> &'static str;

    fn apply(&self, chunks: &mut Vec<Chunk>, ctx: &PipelineContext) -> PipelineResult<()>;
}
