use clap::{Args, Subcommand};

use crate::cli::db::{DbBackupArgs, DbPurgeArgs, DbRecoverArgs, DbStatsArgs};
use crate::cli::validators::validate_index_slug;
use crate::constants::{DEFAULT_EMBEDDER_KEY, DEFAULT_EMBEDDING_DIM};

/// Namespace for dataset maintenance operations.
#[derive(Debug, Args)]
pub struct SiloArgs {
    #[command(subcommand)]
    pub command: SiloCommands,
}

/// Silo management commands.
#[derive(Debug, Subcommand)]
pub enum SiloCommands {
    /// List available Milli indexes and their sizes.
    List,
    /// Provision a new Milli index for the selected silo.
    Create(SiloCreateArgs),
    /// Show statistics for a specific index.
    Stats(DbStatsArgs),
    /// Create a timestamped backup of a Milli index.
    Backup(DbBackupArgs),
    /// Restore a Milli index from a backup directory.
    Recover(DbRecoverArgs),
    /// Permanently delete a Milli index directory.
    Purge(DbPurgeArgs),
}

/// Arguments for `silo create`.
#[derive(Debug, Args)]
pub struct SiloCreateArgs {
    /// Name of the index to create (e.g., `kio`).
    #[arg(value_parser = validate_index_slug)]
    pub index: String,
    /// Embedder key to configure for the index.
    #[arg(long = "embed-model", default_value_t = DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Embedding dimensionality for the index.
    #[arg(long = "embed-dim", default_value_t = DEFAULT_EMBEDDING_DIM)]
    pub embed_dim: usize,
}
