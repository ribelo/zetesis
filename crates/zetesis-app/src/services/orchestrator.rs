use crate::services::context::{PipelineContext, PipelineResult};
use crate::services::processor::Silo;

/// Placeholder orchestrator; full pipeline wiring will be implemented in a later step.
pub async fn run_for_silo(silo: Silo, raw: &[u8], ctx: &PipelineContext) -> PipelineResult<()> {
    let _ = (silo, raw, ctx);
    // TODO: implement end-to-end pipeline (ingest, normalize, chunk, embed, index).
    Ok(())
}
