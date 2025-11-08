// Backup service using rustic for encrypted, deduplicated backups
//
// This module provides backup and recovery functionality for the Zetesis
// application using the rustic library (restic-compatible format).
// Supports local and S3-compatible remote storage backends.

use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use thiserror::Error;

use rustic_backend::BackendOptions;
use rustic_core::{
    BackupOptions, CheckOptions, ConfigOptions, FileDirStats, KeepOptions, KeyOptions,
    LocalDestination, LsOptions, PathList, Repository, RepositoryOptions, RestoreOptions,
    SnapshotGroupCriterion, SnapshotOptions,
};

/// Errors that can occur during backup operations.
#[derive(Debug, Error)]
pub enum BackupError {
    #[error("backup repository not initialized: {0}")]
    RepositoryNotInitialized(String),

    #[error("backup repository already exists: {0}")]
    RepositoryExists(String),

    #[error("invalid backup configuration: {0}")]
    InvalidConfiguration(String),

    #[error("backup operation failed: {0}")]
    BackupFailed(String),

    #[error("restore operation failed: {0}")]
    RestoreFailed(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("rustic error: {0}")]
    Rustic(String),
}

/// Result type for backup operations.
pub type BackupResult<T> = Result<T, BackupError>;

/// Configuration for backup repository.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Repository location (local path or S3 URI).
    pub repository: String,

    /// Password for repository encryption.
    pub password: String,

    /// Optional S3 configuration.
    pub s3_config: Option<S3Config>,
}

/// S3-compatible storage configuration.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,

    /// S3 endpoint URL (for S3-compatible services like Hetzner).
    pub endpoint: String,

    /// S3 region.
    pub region: String,

    /// Access key ID.
    pub access_key_id: String,

    /// Secret access key.
    pub secret_access_key: String,

    /// Optional root prefix within the bucket.
    pub root_prefix: Option<String>,
}

/// Retention policy for backup snapshots.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Number of daily backups to keep (default: 7).
    pub daily: usize,

    /// Number of weekly backups to keep (default: 4).
    pub weekly: usize,

    /// Number of monthly backups to keep (default: 6).
    pub monthly: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            daily: 7,
            weekly: 4,
            monthly: 6,
        }
    }
}

/// Backup service for managing encrypted, deduplicated backups.
pub struct BackupService {
    config: BackupConfig,
}

/// Summary of a restore attempt.
#[derive(Debug, Clone)]
pub struct RestoreSummary {
    /// Canonical snapshot identifier that was targeted.
    pub snapshot_id: String,
    /// Total number of bytes that will be copied.
    pub restore_size: u64,
    /// Number of bytes that can be reused from existing files.
    pub matched_size: u64,
    /// File statistics returned by rustic.
    pub files: FileDirStats,
    /// Directory statistics returned by rustic.
    pub dirs: FileDirStats,
    /// Whether the restore was only a dry-run.
    pub dry_run: bool,
}

impl BackupService {
    /// Create a new backup service with the given configuration.
    pub fn new(config: BackupConfig) -> Self {
        Self { config }
    }

    /// Initialize a new backup repository.
    ///
    /// This creates a new encrypted repository at the configured location.
    /// Returns an error if the repository already exists.
    pub fn init_repository(&self) -> BackupResult<()> {
        // Create backend options
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Create repository options with password
        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        // Use default key and config options
        let key_opts = KeyOptions::default();
        let config_opts = ConfigOptions::default();

        // Initialize the repository
        Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .init(&key_opts, &config_opts)
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        Ok(())
    }

    /// Perform a backup of the specified directories.
    ///
    /// Returns the snapshot ID of the created backup.
    pub fn backup(&self, paths: &[PathBuf]) -> BackupResult<String> {
        // Create backend options
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Create repository options with password
        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        // Open repository and transition to indexed state for backup
        let repo = Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .open()
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .to_indexed_ids()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Create path list from input paths
        // Join multiple paths with newlines for PathList::from_string
        let path_string = paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let source = PathList::from_string(&path_string)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .sanitize()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Create snapshot options
        let snap_opts = SnapshotOptions::default();
        let snap = snap_opts
            .to_snapshot()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Perform backup
        let backup_opts = BackupOptions::default();
        let snapshot = repo
            .backup(&backup_opts, &source, snap)
            .map_err(|e| BackupError::BackupFailed(e.to_string()))?;

        // Return snapshot ID
        Ok(snapshot.id.to_string())
    }

    /// List all snapshots in the repository.
    pub fn list_snapshots(&self) -> BackupResult<Vec<SnapshotInfo>> {
        // Create backend options
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Create repository options with password
        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        // Open repository
        let repo = Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .open()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Get all snapshots
        let snapshots = repo
            .get_all_snapshots()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        // Convert to our SnapshotInfo format
        let snapshot_infos = snapshots
            .into_iter()
            .map(|snap| SnapshotInfo {
                id: snap.id.to_string(),
                time: snap.time.with_timezone(&chrono::Utc),
                hostname: snap.hostname.clone(),
                paths: snap.paths.iter().map(PathBuf::from).collect(),
                size: 0, // Size info not readily available from basic snapshot metadata
            })
            .collect();

        Ok(snapshot_infos)
    }

    /// Restore a snapshot to the specified target directory.
    pub fn restore(
        &self,
        snapshot_ref: &str,
        target: &Path,
        dry_run: bool,
    ) -> BackupResult<RestoreSummary> {
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        let repo_open = Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .open()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let snapshot = repo_open
            .get_snapshot_from_str(snapshot_ref, |_| true)
            .map_err(|e| BackupError::SnapshotNotFound(e.to_string()))?;

        let repo = repo_open
            .to_indexed()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let node = repo
            .node_from_snapshot_and_path(&snapshot, "")
            .map_err(|e| BackupError::RestoreFailed(e.to_string()))?;

        let ls_opts = LsOptions::default();
        let entries = repo
            .ls(&node, &ls_opts)
            .map_err(|e| BackupError::RestoreFailed(e.to_string()))?;

        let expect_file = !node.is_dir();
        let destination_string = target.to_string_lossy().to_string();
        let dest = LocalDestination::new(destination_string.as_str(), !dry_run, expect_file)
            .map_err(|e| BackupError::RestoreFailed(e.to_string()))?;

        let restore_opts = RestoreOptions::default();
        let plan = repo
            .prepare_restore(&restore_opts, entries.clone(), &dest, dry_run)
            .map_err(|e| BackupError::RestoreFailed(e.to_string()))?;

        let summary = RestoreSummary {
            snapshot_id: snapshot.id.to_hex().to_string(),
            restore_size: plan.restore_size,
            matched_size: plan.matched_size,
            files: plan.stats.files,
            dirs: plan.stats.dirs,
            dry_run,
        };

        if dry_run {
            return Ok(summary);
        }

        repo.restore(plan, &restore_opts, entries, &dest)
            .map_err(|e| BackupError::RestoreFailed(e.to_string()))?;

        Ok(summary)
    }

    /// Apply retention policy to remove old snapshots.
    ///
    /// Returns the number of snapshots removed.
    pub fn apply_retention(&self, policy: &RetentionPolicy) -> BackupResult<usize> {
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        let repo = Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .open()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let mut keep = KeepOptions::default();
        keep.keep_daily = Some(convert_retention(policy.daily)?);
        keep.keep_weekly = Some(convert_retention(policy.weekly)?);
        keep.keep_monthly = Some(convert_retention(policy.monthly)?);

        let groups = repo
            .get_forget_snapshots(&keep, SnapshotGroupCriterion::default(), |_| true)
            .map_err(|e| BackupError::Rustic(e.to_string()))?;
        let ids = groups.into_forget_ids();
        if ids.is_empty() {
            return Ok(0);
        }

        repo.delete_snapshots(&ids)
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        Ok(ids.len())
    }

    /// Verify the integrity of a snapshot.
    pub fn verify_snapshot(&self, snapshot_id: &str) -> BackupResult<VerificationResult> {
        let backends = BackendOptions::default()
            .repository(&self.config.repository)
            .to_backends()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let repo_opts = RepositoryOptions::default().password(&self.config.password);

        let repo = Repository::new(&repo_opts, &backends)
            .map_err(|e| BackupError::Rustic(e.to_string()))?
            .open()
            .map_err(|e| BackupError::Rustic(e.to_string()))?;

        let snapshot = repo
            .get_snapshot_from_str(snapshot_id, |_| true)
            .map_err(|e| BackupError::SnapshotNotFound(e.to_string()))?;

        repo.check_with_trees(CheckOptions::default(), vec![snapshot.tree])
            .map_err(|e| BackupError::BackupFailed(e.to_string()))?;

        Ok(VerificationResult {
            valid: true,
            files_checked: 0,
            errors: Vec::new(),
        })
    }
}

/// Information about a backup snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Unique snapshot identifier.
    pub id: String,

    /// Timestamp when the snapshot was created.
    pub time: chrono::DateTime<chrono::Utc>,

    /// Hostname where the backup was created.
    pub hostname: String,

    /// Paths included in the snapshot.
    pub paths: Vec<PathBuf>,

    /// Size of the snapshot in bytes.
    pub size: u64,
}

/// Result of a snapshot verification.
#[derive(Debug)]
pub struct VerificationResult {
    /// Whether the snapshot passed verification.
    pub valid: bool,

    /// Number of files verified.
    pub files_checked: usize,

    /// Any errors encountered during verification.
    pub errors: Vec<String>,
}

fn convert_retention(value: usize) -> BackupResult<i32> {
    i32::try_from(value).map_err(|_| {
        BackupError::InvalidConfiguration(format!(
            "retention value {value} exceeds supported range"
        ))
    })
}
