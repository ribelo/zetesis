#[cfg(feature = "cli-debug")]
use clap::{Args, Subcommand};

#[cfg(feature = "cli-debug")]
use crate::cli::validators::validate_pdf_file;

/// Debug command namespace (gated by cli-debug feature).
#[cfg(feature = "cli-debug")]
#[derive(Debug, Args)]
pub struct DebugArgs {
    #[command(subcommand)]
    pub command: DebugCommands,
}

/// Debug subcommands.
#[cfg(feature = "cli-debug")]
#[derive(Debug, Subcommand)]
pub enum DebugCommands {
    /// Extract plain text from a PDF file.
    Text(DebugTextArgs),
}

/// Options for the `debug text` command.
#[cfg(feature = "cli-debug")]
#[derive(Debug, Args)]
pub struct DebugTextArgs {
    /// PDF file to extract text from.
    #[arg(value_parser = validate_pdf_file)]
    pub input: std::path::PathBuf,
}
