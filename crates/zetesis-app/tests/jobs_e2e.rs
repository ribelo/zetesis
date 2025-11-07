use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use zetesis_app::paths::AppPaths;
use zetesis_app::services::jobs::{
    GenerationJob, GenerationJobStatus, GenerationJobStore, GenerationMode, GenerationProviderKind,
};
use zetesis_app::services::reaper::{ReaperAction, ReaperConfig, reap_stale_jobs};

#[test]
fn generation_job_lifecycle_stats() {
    let temp = TempDir::new().expect("temp dir");
    let paths = AppPaths::new(temp.path()).expect("paths");
    let store = GenerationJobStore::open(&paths).expect("open store");

    let job = GenerationJob::new(
        "doc-lifecycle",
        "kio",
        "embed-model",
        "gen-model",
        GenerationMode::Sync,
    );
    store.enqueue(&job).expect("enqueue");

    let pending_stats = store
        .list_all_with_counts()
        .expect("stats after enqueue")
        .get(&GenerationJobStatus::Pending)
        .cloned()
        .unwrap_or((0, None, None));
    assert_eq!(pending_stats.0, 1);

    store
        .update_status(&job.job_id, GenerationJobStatus::Generating, None)
        .expect("update to generating");
    store
        .update_status(&job.job_id, GenerationJobStatus::Generated, None)
        .expect("update to generated");

    let stats = store.list_all_with_counts().expect("stats after generate");
    assert!(stats.get(&GenerationJobStatus::Pending).is_none());
    assert!(stats.get(&GenerationJobStatus::Generating).is_none());
    let (count, _, _) = stats
        .get(&GenerationJobStatus::Generated)
        .cloned()
        .unwrap_or((0, None, None));
    assert_eq!(count, 1);
}

#[test]
fn generation_job_reaper_enforces_retry_bounds() {
    let temp = TempDir::new().expect("temp dir");
    let paths = AppPaths::new(temp.path()).expect("paths");
    let store = GenerationJobStore::open(&paths).expect("open store");

    let mut job = GenerationJob::new(
        "doc-retry",
        "kio",
        "embed-model",
        "gen-model",
        GenerationMode::Batch,
    );
    job.status = GenerationJobStatus::Generating;
    job.provider_kind = GenerationProviderKind::GeminiBatch;
    job.retry_count = job.max_retries;
    job.error = Some("previous failure".to_string());
    job.updated_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
        - 10_000;
    store.upsert(&job).expect("store job");

    let mut config = ReaperConfig::default();
    config.generating_max_age_ms = 1;

    let report = reap_stale_jobs(&store, &config, ReaperAction::Both).expect("reaper runs");
    assert!(report.failed.contains(&job.job_id));

    let refreshed = store
        .get(&job.job_id)
        .expect("fetch job")
        .expect("job present");
    assert_eq!(refreshed.status, GenerationJobStatus::Failed);
    assert_eq!(refreshed.retry_count, job.max_retries);
}
