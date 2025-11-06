use std::time::{SystemTime, UNIX_EPOCH};

use crate::paths::{AppPaths, PathError};
use crate::pipeline::structured::StructuredDecision;
use crate::services::context::EmbedMode;
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

/// Lifecycle state of an embedding job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EmbeddingJobStatus {
    Pending,
    Embedding,
    Embedded,
    Ingesting,
    Ingested,
    Failed,
}

/// Metadata persisted for every embedding job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingJob {
    pub job_id: String,
    pub doc_id: String,
    pub silo: String,
    pub embedder_key: String,
    pub status: EmbeddingJobStatus,
    pub mode: EmbedMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_job_id: Option<String>,
    #[serde(default)]
    pub provider_kind: EmbeddingProviderKind,
    #[serde(default)]
    pub submitted_batch_count: u32,
    #[serde(default)]
    pub chunk_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_decision: Option<StructuredDecision>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_ms: Option<i64>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingProviderKind {
    #[default]
    GeminiBatch,
    Synchronous,
}

impl EmbeddingJob {
    #[must_use]
    pub fn new(
        doc_id: impl Into<String>,
        silo: impl Into<String>,
        embedder_key: impl Into<String>,
        mode: EmbedMode,
        chunk_count: u32,
        content_hash: Option<String>,
    ) -> Self {
        let doc_id = doc_id.into();
        debug_assert!(!doc_id.is_empty());
        let now_ms = current_timestamp_ms();
        Self {
            job_id: doc_id.clone(),
            doc_id,
            silo: silo.into(),
            embedder_key: embedder_key.into(),
            status: EmbeddingJobStatus::Pending,
            mode,
            provider_job_id: None,
            provider_kind: EmbeddingProviderKind::default(),
            submitted_batch_count: 0,
            chunk_count,
            content_hash,
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
    pub fn with_status(mut self, status: EmbeddingJobStatus, error: Option<String>) -> Self {
        self.status = status;
        self.error = error;
        self.updated_at_ms = current_timestamp_ms();
        self
    }

    pub fn set_status(&mut self, status: EmbeddingJobStatus, error: Option<String>) {
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
pub enum EmbeddingJobStoreError {
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
pub struct EmbeddingJobStore {
    env: Env,
    jobs: Database<Str, Bytes>,
}

impl EmbeddingJobStore {
    pub fn open(paths: &AppPaths) -> Result<Self, EmbeddingJobStoreError> {
        let path = paths.embedding_jobs_lmdb_dir()?;
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

    pub fn enqueue(&self, job: &EmbeddingJob) -> Result<(), EmbeddingJobStoreError> {
        debug_assert!(!job.job_id.is_empty());
        debug_assert!(job.status == EmbeddingJobStatus::Pending);

        let mut wtxn = self.env.write_txn()?;
        if self.jobs.get(&wtxn, job.job_id.as_str())?.is_some() {
            return Err(EmbeddingJobStoreError::Duplicate(job.job_id.clone()));
        }
        let encoded = encode_to_vec(job, config::standard())?;
        self.jobs
            .put(&mut wtxn, job.job_id.as_str(), encoded.as_slice())
            .map_err(EmbeddingJobStoreError::from)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn get(&self, job_id: &str) -> Result<Option<EmbeddingJob>, EmbeddingJobStoreError> {
        debug_assert!(!job_id.is_empty());
        let rtxn = self.env.read_txn()?;
        let value = self.jobs.get(&rtxn, job_id)?;
        if let Some(raw) = value {
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
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
        status: EmbeddingJobStatus,
        limit: usize,
    ) -> Result<Vec<EmbeddingJob>, EmbeddingJobStoreError> {
        debug_assert!(limit > 0);
        let now_ms = current_timestamp_ms();
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut out = Vec::new();
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
            if job.status == status {
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

    pub fn update_status(
        &self,
        job_id: &str,
        status: EmbeddingJobStatus,
        error: Option<String>,
    ) -> Result<EmbeddingJob, EmbeddingJobStoreError> {
        debug_assert!(!job_id.is_empty());
        let mut wtxn = self.env.write_txn()?;
        let existing = self.jobs.get(&wtxn, job_id)?;
        let Some(raw) = existing else {
            return Err(EmbeddingJobStoreError::NotFound(job_id.to_string()));
        };
        let (mut job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
        job.set_status(status, error);
        let encoded = encode_to_vec(&job, config::standard())?;
        self.jobs.put(&mut wtxn, job_id, encoded.as_slice())?;
        wtxn.commit()?;
        Ok(job)
    }

    pub fn upsert(&self, job: &EmbeddingJob) -> Result<(), EmbeddingJobStoreError> {
        debug_assert!(!job.job_id.is_empty());
        let mut wtxn = self.env.write_txn()?;
        let encoded = encode_to_vec(job, config::standard())?;
        self.jobs
            .put(&mut wtxn, job.job_id.as_str(), encoded.as_slice())
            .map_err(EmbeddingJobStoreError::from)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn count_by_status(
        &self,
        status: EmbeddingJobStatus,
    ) -> Result<usize, EmbeddingJobStoreError> {
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut count = 0_usize;
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
            if job.status == status {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// List jobs with given status that have not been updated within the threshold.
    pub fn list_stale_jobs(
        &self,
        status: EmbeddingJobStatus,
        age_threshold_ms: i64,
        limit: usize,
    ) -> Result<Vec<EmbeddingJob>, EmbeddingJobStoreError> {
        debug_assert!(limit > 0);
        debug_assert!(age_threshold_ms > 0);
        let now_ms = current_timestamp_ms();
        let cutoff_ms = now_ms.saturating_sub(age_threshold_ms);

        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut out = Vec::new();
        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
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
        status: EmbeddingJobStatus,
    ) -> Result<Option<(i64, i64)>, EmbeddingJobStoreError> {
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut oldest_created: Option<i64> = None;
        let mut oldest_updated: Option<i64> = None;

        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
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
    ) -> Result<std::collections::HashMap<EmbeddingJobStatus, (usize, Option<i64>, Option<i64>)>, EmbeddingJobStoreError> {
        use std::collections::HashMap;
        let rtxn = self.env.read_txn()?;
        let iter = self.jobs.iter(&rtxn)?;
        let mut stats: HashMap<EmbeddingJobStatus, (usize, Option<i64>, Option<i64>)> =
            HashMap::new();

        for entry in iter {
            let (_, raw) = entry?;
            let (job, _) = decode_from_slice::<EmbeddingJob, _>(raw, config::standard())?;
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

    #[test]
    fn embed_job_new_sets_defaults() {
        let job = EmbeddingJob::new(
            "doc-123",
            "kio",
            "embed-model",
            EmbedMode::Batched,
            7,
            Some("hash".to_string()),
        );

        assert_eq!(job.job_id, "doc-123");
        assert_eq!(job.doc_id, "doc-123");
        assert_eq!(job.embedder_key, "embed-model");
        assert_eq!(job.status, EmbeddingJobStatus::Pending);
        assert_eq!(job.provider_kind, EmbeddingProviderKind::GeminiBatch);
        assert_eq!(job.chunk_count, 7);
        assert_eq!(job.content_hash.as_deref(), Some("hash"));
        assert!(job.provider_job_id.is_none());
        assert!(job.pending_decision.is_none());
        assert_eq!(job.retry_count, 0);
        assert_eq!(job.max_retries, 3);
        assert!(job.last_error.is_none());
        assert!(job.next_retry_at_ms.is_none());
    }

}
