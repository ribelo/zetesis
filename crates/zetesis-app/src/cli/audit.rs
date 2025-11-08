use clap::{Args, Subcommand};
use std::path::PathBuf;

/// Audit command namespace.
#[derive(Debug, Args)]
pub struct AuditArgs {
    #[command(subcommand)]
    pub command: AuditCommands,
}

/// Audit subcommands.
#[derive(Debug, Subcommand)]
pub enum AuditCommands {
    /// Audit semantic chunk coverage and basic integrity signals.
    Structured(StructuredAuditArgs),
}

/// Options for the `audit structured` command.
#[derive(Debug, Args)]
pub struct StructuredAuditArgs {
    /// Directory to scan for PDFs (defaults to data/kio/raw/kio).
    #[arg(long, value_name = "DIR", default_value = "data/kio/raw/kio")]
    pub dir: PathBuf,
    /// Number of PDFs to sample.
    #[arg(short = 'n', long = "num", default_value_t = 10)]
    pub num: usize,
    /// Override Gemini model identifier for extraction.
    #[arg(long)]
    pub model: Option<String>,
    /// Output path for TSV (use '-' for stdout).
    #[arg(long = "out", value_name = "TSV", default_value = "-")]
    pub out_path: String,
}
