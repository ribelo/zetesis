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
    /// Use restic-compatible encrypted backup (requires --repository and --password).
    #[arg(long, action = ArgAction::SetTrue)]
    pub restic: bool,
    /// Restic repository location (local path or S3 URI).
    #[arg(long, value_name = "PATH")]
    pub repository: Option<String>,
    /// Restic repository password.
    #[arg(long, value_name = "PASSWORD")]
    pub password: Option<String>,
    /// Verify backup integrity after completion.
    #[arg(long, action = ArgAction::SetTrue)]
    pub verify: bool,
    /// Number of daily backups to keep (default: 7).
    #[arg(long = "keep-daily", value_name = "N")]
    pub keep_daily: Option<usize>,
    /// Number of weekly backups to keep (default: 4).
    #[arg(long = "keep-weekly", value_name = "N")]
    pub keep_weekly: Option<usize>,
    /// Number of monthly backups to keep (default: 6).
    #[arg(long = "keep-monthly", value_name = "N")]
    pub keep_monthly: Option<usize>,
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
    pub from: Option<PathBuf>,
    /// Allow overwriting the target index directory if it already exists.
    #[arg(long, action = ArgAction::SetTrue)]
    pub force: bool,
    /// Use restic-compatible encrypted restore (requires --repository and --password).
    #[arg(long, action = ArgAction::SetTrue)]
    pub restic: bool,
    /// Restic repository location (local path or S3 URI).
    #[arg(long, value_name = "PATH")]
    pub repository: Option<String>,
    /// Restic repository password.
    #[arg(long, value_name = "PASSWORD")]
    pub password: Option<String>,
    /// Restic snapshot ID to restore.
    #[arg(long, value_name = "ID")]
    pub snapshot: Option<String>,
    /// Perform dry-run without actually restoring files.
    #[arg(long, action = ArgAction::SetTrue)]
    pub dry_run: bool,
}
