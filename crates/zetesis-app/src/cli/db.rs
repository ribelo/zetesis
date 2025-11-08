use clap::{ArgAction, Args, Subcommand};
use std::path::PathBuf;

use crate::cli::db_delete::DbDeleteArgs;
use crate::cli::validators::validate_index_slug;
use crate::constants::DEFAULT_EMBEDDING_DIM;

/// Top-level namespace for database maintenance commands.
#[derive(Debug, Args)]
pub struct DbArgs {
    #[command(subcommand)]
    pub command: DbCommands,
}

/// Database maintenance subcommands.
#[derive(Debug, Subcommand)]
pub enum DbCommands {
    /// Create an empty Milli index with optional configuration.
    Create(DbCreateArgs),
    /// List available Milli indexes and their on-disk sizes.
    List,
    /// Show summary statistics for a specific Milli index.
    Stats(DbStatsArgs),
    /// Fetch a single record by primary key.
    Get(DbGetArgs),
    /// Run an ad-hoc filtered listing of records.
    Find(DbFindArgs),
    /// Create a timestamped backup of a Milli index.
    Backup(DbBackupArgs),
    /// Permanently delete a Milli index directory.
    Purge(DbPurgeArgs),
    /// Restore a Milli index from a backup directory.
    Recover(DbRecoverArgs),
    /// Delete a document and associated chunks/blobs.
    Delete(DbDeleteArgs),
}

/// Arguments for `db create`.
#[derive(Debug, Args)]
pub struct DbCreateArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    /// Embedding model identifier to configure for the index.
    #[arg(long = "embed-model", default_value_t = crate::constants::DEFAULT_EMBEDDER_KEY.to_string())]
    pub embed_model: String,
    /// Embedding dimensionality for the index.
    #[arg(long = "embed-dim", default_value_t = DEFAULT_EMBEDDING_DIM)]
    pub embed_dim: usize,
    /// Allow overwriting the index if it already exists.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force: bool,
}

/// Arguments for `db stats`.
#[derive(Debug, Args)]
pub struct DbStatsArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
}

/// Arguments for `db get`.
#[derive(Debug, Args)]
pub struct DbGetArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    /// Primary key of the record to fetch.
    #[arg(long)]
    pub id: String,
    /// Comma-separated list of fields to include in the response.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for `db find`.
#[derive(Debug, Args)]
pub struct DbFindArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    /// Optional Milli filter expression.
    #[arg(long)]
    pub filter: Option<String>,
    /// Maximum number of records to return.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Number of records to skip from the start.
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    /// Comma-separated list of fields to include in the response.
    #[arg(long = "fields", value_delimiter = ',', num_args = 0..)]
    pub fields: Vec<String>,
    /// Pretty-print JSON output.
    #[arg(long, action = ArgAction::SetTrue)]
    pub pretty: bool,
}

/// Arguments for `db backup`.
#[derive(Debug, Args)]
pub struct DbBackupArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    /// Destination directory for backups (defaults to ./backups).
    #[arg(long = "out", value_name = "DIR")]
    pub out: Option<PathBuf>,
}

/// Arguments for `db purge`.
#[derive(Debug, Args)]
pub struct DbPurgeArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
}

/// Arguments for `db recover`.
#[derive(Debug, Args)]
pub struct DbRecoverArgs {
    /// Positional index slug (deprecated alias for `--silo`).
    #[arg(value_parser = validate_index_slug)]
    pub index: Option<String>,
    /// Directory containing the backup to restore from.
    #[arg(long = "from", value_name = "DIR")]
    pub from: PathBuf,
    /// Allow overwriting the target index directory if it already exists.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force: bool,
}
