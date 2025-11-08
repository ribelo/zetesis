use clap::{Args, Subcommand};

use crate::cli::fetch_kio::FetchKioArgs;

/// Source pipeline commands (e.g., fetch KIO data).
#[derive(Debug, Args)]
pub struct SourceArgs {
    #[command(subcommand)]
    pub command: SourceCommands,
}

/// Supported source commands.
#[derive(Debug, Subcommand)]
pub enum SourceCommands {
    /// Work with KIO data ingestion pipelines.
    Kio(SourceKioArgs),
}

/// KIO source namespace.
#[derive(Debug, Args)]
pub struct SourceKioArgs {
    #[command(subcommand)]
    pub command: SourceKioCommands,
}

/// KIO source subcommands.
#[derive(Debug, Subcommand)]
pub enum SourceKioCommands {
    /// Fetch documents from a KIO data source.
    Fetch(FetchKioArgs),
}
