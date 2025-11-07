use std::time::{SystemTime, UNIX_EPOCH};

use crate::paths::{AppPaths, PathError};
use crate::pipeline::structured::StructuredDecision;
use bincode::config;
use bincode::error::{DecodeError, EncodeError};
use bincode::serde::{decode_from_slice, encode_to_vec};
use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use serde::{Deserialize, Serialize};
use serde_json;
use thiserror::Error;

const JOB_ENV_MAP_SIZE_BYTES: usize = 1 << 28; // 256 MiB
const DEFAULT_MAX_RETRIES: u32 = 3;

fn default_max_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}

/// Lifecycle state of a structured generation job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GenerationJobStatus {
    Pending,
    Generating,
    Generated,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenerationMode {
    Sync,
    Batch,
}

/// Metadata persisted for every generation job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationJob {
    pub job_id: String,
    pub doc_id: String,
    pub silo: String,
    pub embedder_key: String,
    pub generator_key: String,
    pub status: GenerationJobStatus,
    pub generation_mode: GenerationMode,
    #[serde(default)]
    pub provider_job_id: Option<String>,
    #[serde(default)]
    pub provider_kind: GenerationProviderKind,
    #[serde(default)]
    pub submitted_batch_count: u32,
    #[serde(default)]
    pub chunk_count: u32,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub submitted_at_ms: Option<i64>,
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
    pub error: Option<String>,
    #[serde(default)]
    pub pending_decision: Option<StructuredDecision>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub next_retry_at_ms: Option<i64>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenerationProviderKind {
    #[default]
    Synchronous,
    GeminiBatch,
}

impl GenerationJob {
    #[must_use]
    pub fn new(
        doc_id: impl Into<String>,
        silo: impl Into<String>,
        embedder_key: impl Into<String>,
        generator_key: impl Into<String>,
        generation_mode: GenerationMode,
    ) -> Self {
        let doc_id = doc_id.into();
        debug_assert!(!doc_id.is_empty());
        let now_ms = current_timestamp_ms();
        Self {
            job_id: doc_id.clone(),
            doc_id,
            silo: silo.into(),
            embedder_key: embedder_key.into(),
            generator_key: generator_key.into(),
            status: GenerationJobStatus::Pending,
            generation_mode,
            provider_job_id: None,
            provider_kind: GenerationProviderKind::default(),
            submitted_batch_count: 0,
            chunk_count: 0,
            content_hash: None,
            submitted_at_ms: None,
            completed_at_ms: None,
            error: None,
            pending_decision: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            stale: false,
            retry_count: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            last_error: None,
            next_retry_at_ms: None,
        }
    }

    #[must_use]
    pub fn with_status(mut self, status: GenerationJobStatus, error: Option<String>) -> Self {
        self.status = status;
        self.error = error;
        self.updated_at_ms = current_timestamp_ms();
        self
    }

    pub fn set_status(&mut self, status: GenerationJobStatus, error: Option<String>) {
        self.status = status;
        self.error = error;
        self.updated_at_ms = current_timestamp_ms();
    }
}

pub(crate) fn current_timestamp_ms() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    since_epoch.as_millis() as i64
}

/// Errors emitted by the embedding job store.
#[derive(Debug, Error)]
pub enum GenerationJobStoreError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Encode(#[from] EncodeError),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("job `{0}` already exists")]
    Duplicate(String),
    #[error("job `{0}` not found")]
    NotFound(String),
}

/// LMDB-backed persistence for embedding jobs.
#[derive(Debug)]
pub struct GenerationJobStore {
    env: Env,
    jobs: Database<Str, Bytes>,
}

impl GenerationJobStore {
    pub fn open(paths: &AppPaths) -> Result<Self, GenerationJobStoreError> {
        let path = paths.generation_jobs_lmdb_dir()?;
        debug_assert!(path.exists());

        let mut options = EnvOpenOptions::new();
        options.max_dbs(8);
        options.map_size(JOB_ENV_MAP_SIZE_BYTES);
        let env = unsafe {
            // SAFETY: LMDB requires callers to uphold environment lifetime invariants.
            options.open(&path)?
        };
        let jobs = {
            let rtxn = env.read_txn()?;
            let opened = env.open_database::<Str, Bytes>(&rtxn, Some("jobs"))?;
            drop(rtxn);
            match opened {
                Some(existing) => existing,
                None => {
                    let mut wtxn = env.write_txn()?;
                    let db = env.create_database::<Str, Bytes>(&mut wtxn, Some("jobs"))?;
                    wtxn.commit()?;
                    db
                }
            }
        };
        Ok(Self { env, jobs })
    }

    pub fn enqueue(&self, job: &GenerationJob) -> Result<(), GenerationJobStoreError> {
        debug_assert!(!job.job_id.is_empty());
        debug_assert!(job.status == GenerationJobStatus::Pending);

        let mut wtxn = self.env.write_txn()?;
        if self.jobs.get(&wtxn, job.job_id.as_str())?.is_some() {
            return Err(GenerationJobStoreError::Duplicate(job.job_id.clone()));
        }
        let encoded = encode_to_vec(job, config::standard())?;
        self.jobs
            .put(&mut wtxn, job.job_id.as_str(), encoded.as_slice())
            .map_err(GenerationJobStoreError::from)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn get(&self, job_id: &str) -> Result<Option<GenerationJob>, GenerationJobStoreError> {
        debug_assert!(!job_id.is_empty());
        let rtxn = self.env.read_txn()?;
        let value = self.jobs.get(&rtxn, job_id)?;
        if let Some(raw) = value {
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    /// List jobs by status, respecting exponential backoff scheduling.
    ///
    /// Jobs that have `next_retry_at_ms` set to a future timestamp will be
    /// skipped, allowing the exponential backoff to take effect and preventing
    /// immediate re-processing of recently failed jobs.
    pub fn list_by_status(
        &self,
        status: GenerationJobStatus,
        limit: usize,
    ) -> Result<Vec<GenerationJob>, GenerationJobStoreError> {
        self.list_by_status_with_filter(status, limit, |_| true)
    }

    pub fn list_by_status_with_filter<F>(
        &self,
        status: GenerationJobStatus,
        limit: usize,
        mut predicate: F,
    ) -> Result<Vec<GenerationJob>, GenerationJobStoreError>
    where
        F: FnMut(&GenerationJob) -> bool,
    {
        debug_assert!(limit > 0);
        let now_ms = current_timestamp_ms();
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut out = Vec::new();
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            if job.status != status {
                continue;
            }
            if let Some(retry_at) = job.next_retry_at_ms {
                if retry_at > now_ms {
                    continue;
                }
            }
            if !predicate(&job) {
                continue;
            }
            out.push(job);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn update_status(
        &self,
        job_id: &str,
        status: GenerationJobStatus,
        error: Option<String>,
    ) -> Result<GenerationJob, GenerationJobStoreError> {
        debug_assert!(!job_id.is_empty());
        let mut wtxn = self.env.write_txn()?;
        let existing = self.jobs.get(&wtxn, job_id)?;
        let Some(raw) = existing else {
            return Err(GenerationJobStoreError::NotFound(job_id.to_string()));
        };
        let (mut job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
        job.set_status(status, error);
        let encoded = encode_to_vec(&job, config::standard())?;
        self.jobs.put(&mut wtxn, job_id, encoded.as_slice())?;
        wtxn.commit()?;
        Ok(job)
    }

    pub fn upsert(&self, job: &GenerationJob) -> Result<(), GenerationJobStoreError> {
        debug_assert!(!job.job_id.is_empty());
        let mut wtxn = self.env.write_txn()?;
        let encoded = encode_to_vec(job, config::standard())?;
        self.jobs
            .put(&mut wtxn, job.job_id.as_str(), encoded.as_slice())
            .map_err(GenerationJobStoreError::from)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn count_by_status(
        &self,
        status: GenerationJobStatus,
    ) -> Result<usize, GenerationJobStoreError> {
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut count = 0_usize;
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            if job.status == status {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// List jobs with given status that have not been updated within the threshold.
    pub fn list_stale_jobs(
        &self,
        status: GenerationJobStatus,
        age_threshold_ms: i64,
        limit: usize,
    ) -> Result<Vec<GenerationJob>, GenerationJobStoreError> {
        debug_assert!(limit > 0);
        debug_assert!(age_threshold_ms > 0);
        let now_ms = current_timestamp_ms();
        let cutoff_ms = now_ms.saturating_sub(age_threshold_ms);

        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut out = Vec::new();
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            if job.status == status && job.updated_at_ms <= cutoff_ms {
                // Skip jobs that are scheduled for retry in the future
                if let Some(retry_at) = job.next_retry_at_ms {
                    if retry_at > now_ms {
                        continue;
                    }
                }
                out.push(job);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Get the oldest timestamp (both created_at and updated_at) for a given status.
    pub fn oldest_by_status(
        &self,
        status: GenerationJobStatus,
    ) -> Result<Option<(i64, i64)>, GenerationJobStoreError> {
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut oldest_created: Option<i64> = None;
        let mut oldest_updated: Option<i64> = None;

        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            if job.status == status {
                oldest_created = Some(match oldest_created {
                    None => job.created_at_ms,
                    Some(ts) => ts.min(job.created_at_ms),
                });
                oldest_updated = Some(match oldest_updated {
                    None => job.updated_at_ms,
                    Some(ts) => ts.min(job.updated_at_ms),
                });
            }
        }

        match (oldest_created, oldest_updated) {
            (Some(c), Some(u)) => Ok(Some((c, u))),
            _ => Ok(None),
        }
    }

    /// Get counts and oldest timestamps for all statuses efficiently.
    pub fn list_all_with_counts(
        &self,
    ) -> Result<
        std::collections::HashMap<GenerationJobStatus, (usize, Option<i64>, Option<i64>)>,
        GenerationJobStoreError,
    > {
        use std::collections::HashMap;
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut stats: HashMap<GenerationJobStatus, (usize, Option<i64>, Option<i64>)> =
            HashMap::new();

        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<GenerationJob, _>(raw, config::standard())?;
            let stat_entry = stats.entry(job.status).or_insert((0, None, None));
            stat_entry.0 = stat_entry.0.saturating_add(1);
            stat_entry.1 = Some(match stat_entry.1 {
                None => job.created_at_ms,
                Some(ts) => i64::min(ts, job.created_at_ms),
            });
            stat_entry.2 = Some(match stat_entry.2 {
                None => job.updated_at_ms,
                Some(ts) => i64::min(ts, job.updated_at_ms),
            });
        }

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::AppPaths;
    use tempfile::TempDir;

    #[test]
    fn generation_job_new_sets_defaults() {
        let job = GenerationJob::new(
            "doc-123",
            "kio",
            "embed-model",
            "gen-model",
            GenerationMode::Sync,
        );

        assert_eq!(job.job_id, "doc-123");
        assert_eq!(job.doc_id, "doc-123");
        assert_eq!(job.embedder_key, "embed-model");
        assert_eq!(job.generator_key, "gen-model");
        assert_eq!(job.generation_mode, GenerationMode::Sync);
        assert_eq!(job.status, GenerationJobStatus::Pending);
        assert_eq!(job.provider_kind, GenerationProviderKind::Synchronous);
        assert_eq!(job.chunk_count, 0);
        assert!(job.content_hash.is_none());
        assert!(job.provider_job_id.is_none());
        assert!(job.pending_decision.is_none());
        assert_eq!(job.retry_count, 0);
        assert_eq!(job.max_retries, 3);
        assert!(job.last_error.is_none());
        assert!(job.next_retry_at_ms.is_none());
    }

    #[test]
    fn job_enqueue_is_idempotent() {
        let temp = TempDir::new().expect("temp dir");
        let paths = AppPaths::new(temp.path()).expect("app paths");
        let store = GenerationJobStore::open(&paths).expect("open store");

        let job = GenerationJob::new(
            "doc-id",
            "kio",
            "embed-model",
            "gen-model",
            GenerationMode::Sync,
        );

        store.enqueue(&job).expect("initial enqueue succeeds");
        let err = store.enqueue(&job).expect_err("duplicate enqueue fails");
        match err {
            GenerationJobStoreError::Duplicate(id) => assert_eq!(id, "doc-id"),
            other => panic!("expected duplicate error, got {other:?}"),
        }
    }

    #[test]
    fn generation_job_roundtrip_serialization() {
        let job = GenerationJob::new(
            "doc-rt",
            "kio",
            "embed-model",
            "gen-model",
            GenerationMode::Batch,
        );
        let encoded = encode_to_vec(&job, config::standard()).expect("encode");
        let (decoded, _) =
            decode_from_slice::<GenerationJob, _>(&encoded, config::standard()).expect("decode");
        assert_eq!(decoded.job_id, job.job_id);
        assert_eq!(decoded.silo, job.silo);
        assert_eq!(decoded.generation_mode, GenerationMode::Batch);
        assert_eq!(decoded.status, GenerationJobStatus::Pending);
    }

    #[test]
    fn update_status_persists() {
        let temp = TempDir::new().expect("temp dir");
        let paths = AppPaths::new(temp.path()).expect("app paths");
        let store = GenerationJobStore::open(&paths).expect("open store");

        let job = GenerationJob::new(
            "doc-456",
            "kio",
            "embed-model",
            "gen-model",
            GenerationMode::Sync,
        );

        store.enqueue(&job).expect("enqueue succeeds");
        let persisted_before = store
            .get(&job.job_id)
            .expect("fetch succeeds")
            .expect("job exists");
        assert_eq!(
            persisted_before.status,
            GenerationJobStatus::Pending,
            "enqueue must store pending status"
        );
        let updated = store
            .update_status(&job.job_id, GenerationJobStatus::Generating, None)
            .expect("status update succeeds");
        assert_eq!(updated.status, GenerationJobStatus::Generating);

        let fetched = store
            .get(&job.job_id)
            .expect("fetch succeeds")
            .expect("job exists");
        assert_eq!(fetched.status, GenerationJobStatus::Generating);
        assert!(
            fetched.updated_at_ms >= fetched.created_at_ms,
            "update timestamp should be >= creation"
        );
    }
}
