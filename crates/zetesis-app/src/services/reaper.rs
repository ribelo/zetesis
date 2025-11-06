use crate::services::jobs::{
    current_timestamp_ms, EmbeddingJob, EmbeddingJobStatus, EmbeddingJobStore,
    EmbeddingJobStoreError,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Configuration for the stale job reaper.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// Maximum age for jobs in Pending status (milliseconds)
    pub pending_max_age_ms: i64,
    /// Maximum age for jobs in Embedding status (milliseconds)
    pub embedding_max_age_ms: i64,
    /// Base delay for exponential backoff (milliseconds)
    pub base_retry_delay_ms: i64,
    /// Maximum retry delay cap (milliseconds)
    pub max_retry_delay_ms: i64,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            pending_max_age_ms: 172_800_000,    // 48 hours
            embedding_max_age_ms: 43_200_000,   // 12 hours
            base_retry_delay_ms: 300_000,       // 5 minutes
            max_retry_delay_ms: 3_600_000,      // 1 hour
        }
    }
}

/// Action to take when reaping stale jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaperAction {
    /// Requeue stale jobs with retry backoff
    Requeue,
    /// Mark stale jobs as failed
    Fail,
    /// Both requeue (if retries left) and fail (if retries exhausted)
    Both,
}

/// Report of reaper execution results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaperReport {
    /// Jobs that were requeued for retry
    pub requeued: Vec<String>,
    /// Jobs that were marked as failed
    pub failed: Vec<String>,
    /// Jobs that were skipped (not yet at retry time)
    pub skipped: usize,
}

impl ReaperReport {
    pub fn is_empty(&self) -> bool {
        self.requeued.is_empty() && self.failed.is_empty() && self.skipped == 0
    }

    pub fn total(&self) -> usize {
        self.requeued.len() + self.failed.len() + self.skipped
    }
}

#[derive(Debug, Error)]
pub enum ReaperError {
    #[error(transparent)]
    Store(#[from] EmbeddingJobStoreError),
    #[error("reaper configuration invalid: {0}")]
    InvalidConfig(String),
}

/// Calculate next retry time using exponential backoff with jitter.
pub fn calculate_retry_backoff(
    retry_count: u32,
    base_delay_ms: i64,
    max_delay_ms: i64,
) -> i64 {
    use rand::Rng;
    debug_assert!(base_delay_ms > 0);
    debug_assert!(max_delay_ms >= base_delay_ms);

    // Exponential: base * 2^retry_count
    let exponent = retry_count.min(20); // Prevent overflow
    let multiplier = 2_i64.saturating_pow(exponent);
    let delay = base_delay_ms.saturating_mul(multiplier);

    // Cap at max delay
    let capped_delay = delay.min(max_delay_ms);

    // Add jitter (±10%)
    let mut rng = rand::thread_rng();
    let jitter_factor = rng.gen_range(0.9..=1.1);
    let final_delay = ((capped_delay as f64) * jitter_factor) as i64;

    final_delay.clamp(base_delay_ms, max_delay_ms)
}

/// Reap stale jobs from the store.
pub fn reap_stale_jobs(
    store: &EmbeddingJobStore,
    config: &ReaperConfig,
    action: ReaperAction,
) -> Result<ReaperReport, ReaperError> {
    // Validate config
    if config.pending_max_age_ms <= 0 || config.embedding_max_age_ms <= 0 {
        return Err(ReaperError::InvalidConfig(
            "age thresholds must be positive".to_string(),
        ));
    }

    let mut report = ReaperReport {
        requeued: Vec::new(),
        failed: Vec::new(),
        skipped: 0,
    };

    // Reap pending jobs
    reap_status(
        store,
        config,
        action,
        EmbeddingJobStatus::Pending,
        config.pending_max_age_ms,
        &mut report,
    )?;

    // Reap embedding jobs
    reap_status(
        store,
        config,
        action,
        EmbeddingJobStatus::Embedding,
        config.embedding_max_age_ms,
        &mut report,
    )?;

    Ok(report)
}

fn reap_status(
    store: &EmbeddingJobStore,
    config: &ReaperConfig,
    action: ReaperAction,
    status: EmbeddingJobStatus,
    age_threshold_ms: i64,
    report: &mut ReaperReport,
) -> Result<(), ReaperError> {
    let stale_jobs = store.list_stale_jobs(status, age_threshold_ms, 1000)?;

    for job in stale_jobs {
        // Skip if this job is already at max retries and action is not Fail
        if action == ReaperAction::Requeue && job.retry_count >= job.max_retries {
            report.skipped = report.skipped.saturating_add(1);
            continue;
        }

        let should_requeue = match action {
            ReaperAction::Requeue => true,
            ReaperAction::Fail => false,
            ReaperAction::Both => job.retry_count < job.max_retries,
        };

        if should_requeue {
            requeue_job(store, config, &job, report)?;
        } else {
            fail_job(store, &job, report)?;
        }
    }

    Ok(())
}

fn requeue_job(
    store: &EmbeddingJobStore,
    config: &ReaperConfig,
    job: &EmbeddingJob,
    report: &mut ReaperReport,
) -> Result<(), ReaperError> {
    let mut updated_job = job.clone();
    updated_job.status = EmbeddingJobStatus::Pending;
    updated_job.stale = true;
    updated_job.retry_count = updated_job.retry_count.saturating_add(1);
    updated_job.updated_at_ms = current_timestamp_ms();

    // Store the current error as last_error before clearing
    if let Some(ref err) = updated_job.error {
        updated_job.last_error = Some(err.clone());
    }

    // Calculate next retry time with exponential backoff
    let backoff_delay = calculate_retry_backoff(
        updated_job.retry_count,
        config.base_retry_delay_ms,
        config.max_retry_delay_ms,
    );
    updated_job.next_retry_at_ms = Some(updated_job.updated_at_ms + backoff_delay);

    // Update error message
    let retry_msg = format!(
        "stale job requeued (attempt {} of {})",
        updated_job.retry_count,
        updated_job.max_retries
    );
    updated_job.error = Some(retry_msg);

    store.upsert(&updated_job)?;
    report.requeued.push(job.job_id.clone());

    Ok(())
}

fn fail_job(
    store: &EmbeddingJobStore,
    job: &EmbeddingJob,
    report: &mut ReaperReport,
) -> Result<(), ReaperError> {
    let mut updated_job = job.clone();
    updated_job.status = EmbeddingJobStatus::Failed;
    updated_job.stale = true;
    updated_job.updated_at_ms = current_timestamp_ms();

    // Store the current error as last_error
    if let Some(ref err) = updated_job.error {
        updated_job.last_error = Some(err.clone());
    }

    // Build comprehensive error message
    let error_msg = if updated_job.retry_count >= updated_job.max_retries {
        format!(
            "stale job failed: max retries ({}) exhausted. Last error: {}",
            updated_job.max_retries,
            updated_job.last_error.as_deref().unwrap_or("none")
        )
    } else {
        format!(
            "stale job failed: exceeded age threshold. Last error: {}",
            updated_job.last_error.as_deref().unwrap_or("none")
        )
    };
    updated_job.error = Some(error_msg);

    store.upsert(&updated_job)?;
    report.failed.push(job.job_id.clone());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases_exponentially() {
        let base = 300_000; // 5 minutes
        let max = 3_600_000; // 1 hour

        let delay0 = calculate_retry_backoff(0, base, max);
        let delay1 = calculate_retry_backoff(1, base, max);
        let delay2 = calculate_retry_backoff(2, base, max);
        let delay3 = calculate_retry_backoff(3, base, max);

        // Check that delays increase (accounting for jitter)
        assert!(delay0 >= (base as f64 * 0.9) as i64);
        assert!(delay1 > delay0);
        assert!(delay2 > delay1);
        assert!(delay3 > delay2);

        // Check that delays are capped
        assert!(delay0 <= max);
        assert!(delay1 <= max);
        assert!(delay2 <= max);
        assert!(delay3 <= max);
    }

    #[test]
    fn backoff_respects_max_delay() {
        let base = 300_000;
        let max = 600_000;

        // High retry count should still be capped
        let delay = calculate_retry_backoff(10, base, max);
        assert!(delay <= max);
        assert!(delay >= (max as f64 * 0.9) as i64); // Should be close to max
    }

    #[test]
    fn backoff_jitter_stays_within_bounds() {
        let base = 300_000;
        let max = 3_600_000;

        // Run multiple times to check jitter variance
        for _ in 0..100 {
            let delay = calculate_retry_backoff(1, base, max);
            // For retry_count=1: base * 2 = 600_000
            // With jitter: 540_000 to 660_000
            assert!(delay >= (base * 2) as i64 * 9 / 10);
            assert!(delay <= (base * 2) as i64 * 11 / 10);
        }
    }

    #[test]
    fn reaper_report_empty_check() {
        let empty = ReaperReport {
            requeued: Vec::new(),
            failed: Vec::new(),
            skipped: 0,
        };
        assert!(empty.is_empty());
        assert_eq!(empty.total(), 0);

        let non_empty = ReaperReport {
            requeued: vec!["job1".to_string()],
            failed: Vec::new(),
            skipped: 2,
        };
        assert!(!non_empty.is_empty());
        assert_eq!(non_empty.total(), 3);
    }

    #[test]
    fn default_config_has_sensible_values() {
        let config = ReaperConfig::default();
        assert_eq!(config.pending_max_age_ms, 172_800_000); // 48 hours
        assert_eq!(config.embedding_max_age_ms, 43_200_000); // 12 hours
        assert_eq!(config.base_retry_delay_ms, 300_000); // 5 minutes
        assert_eq!(config.max_retry_delay_ms, 3_600_000); // 1 hour
    }
}
