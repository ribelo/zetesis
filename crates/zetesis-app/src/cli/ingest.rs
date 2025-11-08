use clap::{ArgAction, Args, ValueEnum, ValueHint};

use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EXTRACTOR_MODEL};

/// Structured generation mode options for ingest.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GenModeArg {
    Sync,
    Batch,
}

/// Ingest local documents into a Milli index.
#[derive(Debug, Args)]
pub struct IngestArgs {
    /// Either `[INDEX] PATH` (legacy) or just `PATH` (preferred with `--silo`).
    #[arg(
        value_hint = ValueHint::AnyPath,
        num_args = 1..=2,
        help = "Use `INGEST <path>` with `--silo` or `INGEST <index> <path>` as legacy alias."
    )]
    pub args: Vec<String>,
    /// Structured generation mode (`sync` runs locally, `batch` submits jobs to the provider).
    #[arg(long = "gen-mode", value_enum, default_value_t = GenModeArg::Sync)]
    pub gen_mode: GenModeArg,
    /// Limit the number of files processed when ingesting a directory.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Embedding model identifier used for chunk vectors.
    #[arg(long, default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Gemini model identifier used for structured generation (alias: --extractor-model).
    #[arg(long = "gen-model", default_value = DEFAULT_EXTRACTOR_MODEL, alias = "extractor-model")]
    pub gen_model: String,
    /// Create the Milli index if it does not already exist.
    #[arg(long, action = ArgAction::SetTrue)]
    pub create_index: bool,
    /// Deprecated alias for `--gen-mode batch`.
    #[arg(long, action = ArgAction::SetTrue, hide = true)]
    pub batch: bool,
}
