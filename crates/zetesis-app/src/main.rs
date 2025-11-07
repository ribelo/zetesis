use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, hash_map::Entry},
    env, fs,
    num::NonZeroUsize,
    path::Path,
    path::PathBuf,
    process,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use ai_ox::content::part::Part as AttachmentPart;
use chrono::Utc;
use futures_util::stream::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use inquire::{InquireError, Text};
use milli::Index as MilliIndex;
use milli::vector::VectorStoreBackend;
use milli::vector::db::IndexEmbeddingConfig;
use milli::vector::embedder::EmbedderOptions;
use milli::{Filter, Search as MilliSearch, all_obkv_to_json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing_subscriber::{filter::LevelFilter, fmt};
use uuid::Uuid;
use zetesis_app::cli::{
    AuditArgs, AuditCommands, Cli, Commands, DEFAULT_KIO_SAOS_URL, DEFAULT_KIO_UZP_URL, DbArgs,
    DbBackupArgs, DbCommands, DbFindArgs, DbGetArgs, DbPurgeArgs, DbRecoverArgs, DbStatsArgs,
    FetchKioArgs, GenModeArg, HybridFusionArg, HybridSearchArgs, IngestArgs, JobsArgs,
    JobsCommands, JobsGenArgs, JobsGenCommands, JobsGenFetchArgs, JobsGenSubmitArgs, JobsReapArgs,
    JobsStatusArgs, KeywordSearchArgs, KioSource, SearchArgs, SearchCommands, StructuredAuditArgs,
    VectorSearchArgs,
};
#[cfg(feature = "cli-debug")]
use zetesis_app::cli::{DebugArgs, DebugCommands};
use zetesis_app::error::AppError;
use zetesis_app::index::milli::{ensure_index, open_existing_index};
use zetesis_app::ingestion::{
    KioEvent, KioSaosScraper, KioScrapeOptions, KioScraperSummary, KioUzpScraper,
};
use zetesis_app::pdf::extract_text_from_pdf;
use zetesis_app::pipeline::structured::StructuredDecision;
use zetesis_app::services::{
    EmbedBatchTask, GeminiBatchStructuredClient, GenerationJob, GenerationJobStatus,
    GenerationJobStore, GenerationMode, GenerationProviderKind, HYBRID_DEFAULT_RRF_K,
    HYBRID_PER_SOURCE_LIMIT_MAX, HybridFusion, HybridSearchParams, KeywordSearchParams,
    MilliActorHandle, PipelineContext, PipelineError, ProviderJobState, StructuredBatchInput,
    StructuredBatchRequest, StructuredExtractor, StructuredJobClient, VectorSearchParams,
    build_pipeline_context, decision_content_hash, hybrid, index_structured_with_embeddings,
    keyword, normalize_hybrid_weights, normalize_index_name, open_index_read_only, project_value,
    resolve_index_dir, vector,
};
use zetesis_app::text::cleanup_text;
use zetesis_app::{config, ingestion, paths::AppPaths, pipeline::Silo, server};

const DOCUMENT_PROMPT: &str =
    "Przeanalizuj załączony dokument i zwróć odpowiedź w oczekiwanym formacie JSON.";
const MAX_INLINE_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;
/// Default maximum number of files to process in a single ingest when the
/// environment variable `ZETESIS_MAX_INGEST_FILES` is not set or malformed.
const DEFAULT_MAX_INGEST_FILES: usize = 10_000;

fn ingest_max_files_from_env() -> usize {
    // Prefer an explicit env var if set (backwards compatibility), otherwise
    // consult the app config (which can be populated from env/file through
    // `config::load`). Fall back to the default if everything else fails.
    if let Ok(s) = std::env::var("ZETESIS_MAX_INGEST_FILES")
        && let Ok(v) = s.parse::<usize>()
        && v > 0
    {
        return v;
    }

    // Try to load from the application config; fall back to default on error.
    match config::load() {
        Ok(cfg) => cfg.ingest.max_files,
        Err(_) => DEFAULT_MAX_INGEST_FILES,
    }
}

#[allow(clippy::result_large_err)]
fn validate_ingest_limit(limit: Option<usize>) -> Result<(), AppError> {
    validate_ingest_limit_with_max(limit, ingest_max_files_from_env())
}

#[allow(clippy::result_large_err)]
fn validate_ingest_limit_with_max(limit: Option<usize>, max: usize) -> Result<(), AppError> {
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(PipelineError::message("limit must be > 0").into());
        }
        if limit > max {
            return Err(
                PipelineError::message(format!("limit cannot exceed {} files", max)).into(),
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let log_level = determine_log_level(&cli);
    init_tracing(log_level);

    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn init_tracing(level: LevelFilter) {
    let subscriber = fmt().with_max_level(level).with_target(false).finish();

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        tracing::warn!("Tracing subscriber already set; skipping re-initialization.");
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let verbosity = cli.verbose;

    match cli.command {
        Some(Commands::Serve(_)) => {
            let config = config::load()?;
            server::serve(config).await?;
        }
        Some(Commands::FetchKio(args)) => {
            run_fetch_kio(args, verbosity).await?;
        }
        Some(Commands::Ingest(args)) => {
            run_ingest(args).await?;
        }
        Some(Commands::Audit(args)) => {
            run_audit(args).await?;
        }
        Some(Commands::Search(args)) => {
            run_search(args).await?;
        }
        Some(Commands::Db(args)) => {
            run_db(args).await?;
        }
        Some(Commands::Jobs(args)) => {
            run_jobs(args).await?;
        }
        #[cfg(feature = "cli-debug")]
        Some(Commands::Debug(args)) => {
            run_debug(args)?;
        }
        None => {
            Cli::print_help();
        }
    }

    Ok(())
}

async fn build_blob_store(
    storage_cfg: &config::StorageConfig,
) -> Result<Arc<dyn zetesis_app::services::BlobStore>, AppError> {
    use zetesis_app::paths::AppPaths;
    use zetesis_app::services::{BlobStore, DurableWrite, FsBlobStore};

    match storage_cfg.backend.as_str() {
        "fs" => {
            let paths = AppPaths::new(&storage_cfg.path)?;
            let store: Arc<dyn BlobStore> = Arc::new(
                FsBlobStore::builder()
                    .paths(paths)
                    .durability(DurableWrite::None)
                    .build(),
            );
            tracing::debug!(path = ?storage_cfg.path, "initialized FsBlobStore");
            Ok(store)
        }
        "s3" => {
            #[cfg(not(feature = "s3"))]
            {
                Err(AppError::Config(
                    "S3 backend requested but s3 feature not enabled".to_string(),
                ))
            }
            #[cfg(feature = "s3")]
            {
                use zetesis_app::services::S3BlobStore;

                let s3_cfg = storage_cfg.s3.as_ref().ok_or_else(|| {
                    AppError::Config(
                        "S3 backend selected but storage.s3 config missing".to_string(),
                    )
                })?;

                let store = S3BlobStore::from_config(
                    s3_cfg.bucket.clone(),
                    s3_cfg.endpoint_url.clone(),
                    s3_cfg.region.clone(),
                    s3_cfg.force_path_style,
                    s3_cfg.root_prefix.clone(),
                )
                .await
                .map_err(|e| {
                    AppError::Storage(format!("failed to initialize S3BlobStore: {}", e))
                })?;

                tracing::info!(
                    bucket = %s3_cfg.bucket,
                    endpoint = ?s3_cfg.endpoint_url,
                    region = ?s3_cfg.region,
                    "initialized S3BlobStore"
                );

                Ok(Arc::new(store))
            }
        }
        other => Err(AppError::Config(format!(
            "unknown storage backend '{}'; expected 'fs' or 's3'",
            other
        ))),
    }
}

async fn run_fetch_kio(args: FetchKioArgs, verbosity: u8) -> Result<(), AppError> {
    use std::sync::Arc;
    use zetesis_app::services::BlobStore;

    let worker_count = NonZeroUsize::new(args.workers.max(1)).expect("workers must always be >= 1");
    let base_url = resolve_kio_base_url(&args);

    let cfg = config::load()?;
    let blob_store: Arc<dyn BlobStore> = build_blob_store(&cfg.storage).await?;

    tracing::info!(
        source = ?args.source,
        url = %base_url,
        limit = ?args.limit,
        workers = worker_count.get(),
        backend = %cfg.storage.backend,
        "starting KIO portal scrape with BlobStore"
    );

    let options = KioScrapeOptions::builder()
        .blob_store(blob_store)
        .worker_count(worker_count)
        .maybe_limit(args.limit)
        .build();

    let progress = (verbosity == 0).then_some(make_progress_bar());
    let mut tracker = ProgressTracker::new(progress.clone(), args.limit.map(|l| l as u64));
    let summary = match args.source {
        KioSource::Uzp => {
            let scraper = KioUzpScraper::new(&base_url)?;
            let mut stream = Box::pin(scraper.scrape_stream(options.clone()));
            process_kio_stream(&mut stream, &mut tracker).await?
        }
        KioSource::Saos => {
            let scraper = KioSaosScraper::new(&base_url)?;
            let mut stream = Box::pin(scraper.scrape_stream(options));
            process_kio_stream(&mut stream, &mut tracker).await?
        }
    };
    finish_kio_progress(progress, &tracker, &summary);

    if !tracker.has_progress_bar() {
        tracing::info!(
            stored = summary.stored,
            discovered = summary.discovered,
            skipped = summary.skipped,
            "KIO scrape completed successfully"
        );
    }

    Ok(())
}

fn resolve_kio_base_url(args: &FetchKioArgs) -> String {
    match args.source {
        KioSource::Uzp => args.url.clone(),
        KioSource::Saos => {
            if args.url == DEFAULT_KIO_UZP_URL {
                DEFAULT_KIO_SAOS_URL.to_string()
            } else {
                args.url.clone()
            }
        }
    }
}

async fn process_kio_stream<S>(
    stream: &mut S,
    tracker: &mut ProgressTracker,
) -> Result<KioScraperSummary, AppError>
where
    S: Stream<Item = Result<KioEvent, ingestion::IngestorError>> + Unpin,
{
    while let Some(event) = stream.next().await {
        let summary = tracker.handle_event(event?).await?;
        if let Some(summary) = summary {
            return Ok(summary);
        }
    }

    Err(AppError::Ingest(ingestion::IngestorError::ChannelClosed))
}

fn finish_kio_progress(
    progress: Option<ProgressBar>,
    tracker: &ProgressTracker,
    summary: &KioScraperSummary,
) {
    if let Some(pb) = progress {
        tracker.ensure_length(&pb, summary.discovered as u64);
        pb.finish_with_message(format!(
            "Completed: {}/{} stored ({} skipped)",
            summary.stored, summary.discovered, summary.skipped
        ));
    }
}

#[derive(Default)]
struct IngestStats {
    total: usize,
    generated: usize,
    submitted: usize,
    queued: usize,
    failed: Vec<(PathBuf, String)>,
}

struct IngestOutcome {
    job_id: String,
    status: GenerationJobStatus,
    provider_job_id: Option<String>,
}

async fn run_ingest(args: IngestArgs) -> Result<(), AppError> {
    let ctx = build_pipeline_context(&args.embed_model)?;

    // Validate CLI arguments early and fail fast with clear error messages.
    validate_ingest_limit(args.limit)?;

    // Enforce configured maximum even when the user did not pass `--limit`.
    let config_max = ingest_max_files_from_env();
    let effective_limit = args.limit.or(Some(config_max));
    if args.limit.is_none() {
        tracing::info!(
            event = "ingest_limit_enforced",
            max = config_max,
            "no --limit provided; enforcing configured max_files"
        );
    }

    // Sanity assertions for developer/debug builds; these are side-effect-free.
    debug_assert!(!args.embed_model.is_empty(), "embed model must be present");
    debug_assert!(
        !args.gen_model.is_empty(),
        "generation model must be present"
    );

    tracing::info!(
        event = "ingest_start",
        index = %args.index,
        path = %args.path.display(),
        limit = ?args.limit,
        gen_mode = ?args.gen_mode,
        deprecated_batch_flag = args.batch,
        embed_model = %args.embed_model,
        gen_model = %args.gen_model,
        "starting ingest"
    );

    let silo = Silo::from_str(&args.index).map_err(|_| AppError::InvalidIndexName {
        name: args.index.clone(),
    })?;
    let slug = silo.slug();

    let mut gen_mode_arg = args.gen_mode;
    if args.batch {
        tracing::warn!(
            flag = "--batch",
            replacement = "--gen-mode batch",
            "deprecated flag --batch used; mapping to generation batch mode"
        );
        println!("warning: --batch is deprecated; use --gen-mode batch");
        gen_mode_arg = GenModeArg::Batch;
    }
    let gen_mode = match gen_mode_arg {
        GenModeArg::Sync => GenerationMode::Sync,
        GenModeArg::Batch => GenerationMode::Batch,
    };
    let extractor = match gen_mode {
        GenerationMode::Sync => Some(StructuredExtractor::from_env(args.gen_model.clone())?),
        GenerationMode::Batch => None,
    };
    debug_assert!(
        matches!(gen_mode, GenerationMode::Sync | GenerationMode::Batch),
        "unsupported generation mode"
    );

    let index_dir = ctx.paths.data_dir().join("milli").join(slug);
    let data_file = index_dir.join("data.mdb");
    if !data_file.exists() {
        if args.create_index {
            ensure_index(&ctx.paths, slug, &ctx.embed.embedder_key, ctx.embed.dim)?;
        } else {
            return Err(AppError::MissingIndex {
                index: args.index.clone(),
                path: index_dir,
            });
        }
    } else if args.create_index {
        ensure_index(&ctx.paths, slug, &ctx.embed.embedder_key, ctx.embed.dim)?;
    }

    let targets = collect_ingest_targets(&args.path, effective_limit)?;
    if targets.is_empty() {
        tracing::info!(event = "ingest_nothing", path = %args.path.display(), "no supported documents found");
        println!("no supported documents found at {}", args.path.display());
        return Ok(());
    }

    let mut stats = IngestStats::default();

    for path in targets {
        tracing::info!(event = "ingest_document_start", path = %path.display());
        stats.total = stats.total.saturating_add(1);
        match ingest_document(&ctx, extractor.as_ref(), silo, &path, &args, gen_mode).await {
            Ok(outcome) => match outcome.status {
                GenerationJobStatus::Generated => {
                    stats.generated = stats.generated.saturating_add(1);
                    tracing::info!(
                        event = "ingest_document_complete",
                        path = %path.display(),
                        job_id = %outcome.job_id,
                        status = ?outcome.status,
                        "document ingested"
                    );
                    println!("generated {} (job_id: {})", path.display(), outcome.job_id);
                }
                GenerationJobStatus::Generating => {
                    stats.submitted = stats.submitted.saturating_add(1);
                    if let Some(provider_job_id) = outcome.provider_job_id {
                        tracing::info!(
                            event = "ingest_document_submitted",
                            path = %path.display(),
                            job_id = %outcome.job_id,
                            provider_job_id = %provider_job_id,
                            status = ?outcome.status,
                            "document submitted to provider"
                        );
                        println!(
                            "submitted {} (job_id: {}, provider_job_id: {})",
                            path.display(),
                            outcome.job_id,
                            provider_job_id
                        );
                    } else {
                        tracing::info!(
                            event = "ingest_document_submitted",
                            path = %path.display(),
                            job_id = %outcome.job_id,
                            status = ?outcome.status,
                            "document submitted (no provider id)"
                        );
                        println!("queued {} (job_id: {})", path.display(), outcome.job_id);
                    }
                }
                GenerationJobStatus::Pending => {
                    stats.queued = stats.queued.saturating_add(1);
                    tracing::info!(
                        event = "ingest_document_queued",
                        path = %path.display(),
                        job_id = %outcome.job_id,
                        status = ?outcome.status,
                        "document queued for batch generation"
                    );
                    println!("queued {} (job_id: {})", path.display(), outcome.job_id);
                }
                _ => {
                    tracing::info!(
                        event = "ingest_document_processed",
                        path = %path.display(),
                        job_id = %outcome.job_id,
                        status = ?outcome.status,
                        "document processed"
                    );
                    println!("processed {} (job_id: {})", path.display(), outcome.job_id);
                }
            },
            Err(err) => {
                let msg = err.to_string();
                stats.failed.push((path.clone(), msg.clone()));
                tracing::warn!(event = "ingest_document_failed", document = %path.display(), error = %msg, "ingest failed");
            }
        }
    }

    tracing::info!(
        event = "ingest_complete",
        total = stats.total,
        generated = stats.generated,
        submitted = stats.submitted,
        queued = stats.queued,
        failed = stats.failed.len(),
        "ingest complete"
    );

    // Debug assertions to help catch logic regressions in development builds.
    debug_assert!(
        stats.total >= stats.generated,
        "total should be >= generated"
    );
    debug_assert!(
        stats.total >= stats.submitted,
        "total should be >= submitted"
    );
    debug_assert!(stats.total >= stats.queued, "total should be >= queued");

    println!(
        "processed {} document(s): {} generated, {} in flight, {} queued, {} failed",
        stats.total,
        stats.generated,
        stats.submitted,
        stats.queued,
        stats.failed.len()
    );

    if !stats.failed.is_empty() {
        println!("failed inputs:");
        for (path, error) in stats.failed {
            println!("  - {} :: {}", path.display(), error);
        }
    }

    Ok(())
}

#[cfg(test)]
mod ingest_tests {
    use super::*;

    #[test]
    fn validate_limit_zero_rejected() {
        let res = validate_ingest_limit_with_max(Some(0), 100);
        assert!(res.is_err());
    }

    #[test]
    fn validate_limit_exceeds_env_rejected() {
        let res = validate_ingest_limit_with_max(Some(6), 5);
        assert!(res.is_err());
    }

    #[test]
    fn validate_limit_within_env_ok() {
        let res = validate_ingest_limit_with_max(Some(3), 5);
        assert!(res.is_ok());
    }
}

async fn ingest_document(
    ctx: &PipelineContext,
    extractor: Option<&StructuredExtractor>,
    silo: Silo,
    path: &Path,
    args: &IngestArgs,
    gen_mode: GenerationMode,
) -> Result<IngestOutcome, AppError> {
    let document_kind = document_kind_for_path(path).ok_or_else(|| {
        PipelineError::message(format!("unsupported document type: {}", path.display()))
    })?;

    let bytes = fs::read(path).map_err(|source| AppError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.is_empty() {
        return Err(PipelineError::message("document is empty").into());
    }
    debug_assert!(
        !bytes.is_empty(),
        "read should produce bytes for non-empty file"
    );
    if bytes.len() > MAX_INLINE_ATTACHMENT_BYTES {
        let limit_mib = (MAX_INLINE_ATTACHMENT_BYTES / (1024 * 1024)).max(1);
        let message = format!(
            "document {} exceeds inline attachment limit of {} MiB ({} bytes)",
            path.display(),
            limit_mib,
            MAX_INLINE_ATTACHMENT_BYTES
        );
        return Err(PipelineError::message(message).into());
    }

    let doc_id = doc_id_from_bytes(&bytes);
    // Emit telemetry that ties the parsed document to the generated doc_id and
    // number of content chunks produced. This helps downstream observability and
    // deduplication tracing.
    let display_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document");

    if args.create_index {
        ensure_index(
            &ctx.paths,
            silo.slug(),
            &ctx.embed.embedder_key,
            ctx.embed.dim,
        )?;
    } else {
        open_existing_index(
            &ctx.paths,
            silo.slug(),
            &ctx.embed.embedder_key,
            ctx.embed.dim,
        )?;
    }

    debug_assert!(
        extractor.is_some() == matches!(gen_mode, GenerationMode::Sync),
        "extractor availability must match generation mode"
    );

    match gen_mode {
        GenerationMode::Batch => {
            let payload_kind = GenerationPayloadKind::from(document_kind);
            store_generation_payload(
                &ctx.paths,
                silo.slug(),
                &doc_id,
                payload_kind,
                display_name,
                &bytes,
            )?;

            let job = GenerationJob::new(
                doc_id.clone(),
                silo.slug(),
                ctx.embed.embedder_key.clone(),
                args.gen_model.clone(),
                gen_mode,
            );
            ctx.jobs.enqueue(&job)?;
            tracing::info!(
                event = "generation_job_enqueued",
                doc_id = %doc_id,
                silo = %silo.slug(),
                mode = ?gen_mode,
                "queued generation job for batch processing"
            );

            Ok(IngestOutcome {
                job_id: job.job_id.clone(),
                status: GenerationJobStatus::Pending,
                provider_job_id: None,
            })
        }
        GenerationMode::Sync => {
            let Some(extractor) = extractor else {
                return Err(
                    PipelineError::message("synchronous generation requires extractor").into(),
                );
            };
            let payload_kind = GenerationPayloadKind::from(document_kind);
            let job = GenerationJob::new(
                doc_id.clone(),
                silo.slug(),
                ctx.embed.embedder_key.clone(),
                args.gen_model.clone(),
                gen_mode,
            );
            ctx.jobs.enqueue(&job)?;

            let mut record =
                ctx.jobs
                    .update_status(&job.job_id, GenerationJobStatus::Generating, None)?;

            let decision =
                match run_structured_extraction(extractor, &payload_kind, &bytes, display_name)
                    .await
                {
                    Ok(decision) => {
                        tracing::info!(
                            event = "document_parsed",
                            doc_id = %doc_id,
                            path = %path.display(),
                            chunk_count = decision.chunks.len(),
                            "document parsed and structured"
                        );
                        decision
                    }
                    Err(err) => {
                        fail_job(ctx, &mut record, err.to_string());
                        return Err(err);
                    }
                };
            record.pending_decision = Some(decision.clone());
            record.provider_kind = GenerationProviderKind::Synchronous;
            record.provider_job_id = None;
            record.submitted_batch_count = 0;
            record.submitted_at_ms = None;

            if let Err(err) = finalize_generation(ctx, &mut record, decision).await {
                fail_job(ctx, &mut record, err.to_string());
                return Err(err);
            }

            Ok(IngestOutcome {
                job_id: record.job_id.clone(),
                status: record.status,
                provider_job_id: record.provider_job_id.clone(),
            })
        }
    }
}

fn collect_ingest_targets(path: &Path, limit: Option<usize>) -> Result<Vec<PathBuf>, AppError> {
    let metadata = fs::metadata(path).map_err(|source| AppError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_file() {
        if document_kind_for_path(path).is_some() {
            return Ok(vec![path.to_path_buf()]);
        }
        return Err(PipelineError::message(format!(
            "unsupported document type: {}",
            path.display()
        ))
        .into());
    }

    if metadata.is_dir() {
        let mut entries = Vec::new();
        for entry in fs::read_dir(path).map_err(|source| AppError::Io {
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| AppError::Io {
                path: path.to_path_buf(),
                source,
            })?;
            let entry_path = entry.path();
            if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                continue;
            }
            if document_kind_for_path(&entry_path).is_none() {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            entries.push((entry_path, modified));
        }

        entries.sort_by_key(|(_, modified)| Reverse(*modified));
        let mut files: Vec<PathBuf> = entries.into_iter().map(|(path, _)| path).collect();
        if let Some(limit) = limit
            && limit < files.len()
        {
            files.truncate(limit);
        }
        return Ok(files);
    }

    Err(PipelineError::message("ingest path must be a file or directory").into())
}

#[derive(Clone, Debug)]
enum DocumentKind {
    Pdf,
    Image(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GenerationPayloadKind {
    Pdf,
    Image { mime: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerationPayloadMeta {
    kind: GenerationPayloadKind,
    display_name: String,
}

impl From<DocumentKind> for GenerationPayloadKind {
    fn from(kind: DocumentKind) -> Self {
        match kind {
            DocumentKind::Pdf => GenerationPayloadKind::Pdf,
            DocumentKind::Image(mime) => GenerationPayloadKind::Image { mime },
        }
    }
}

fn document_kind_for_path(path: &Path) -> Option<DocumentKind> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "pdf" => Some(DocumentKind::Pdf),
        "jpg" | "jpeg" => Some(DocumentKind::Image("image/jpeg".to_string())),
        "png" => Some(DocumentKind::Image("image/png".to_string())),
        _ => None,
    }
}

fn store_generation_payload(
    paths: &AppPaths,
    silo: &str,
    doc_id: &str,
    kind: GenerationPayloadKind,
    display_name: &str,
    bytes: &[u8],
) -> Result<(), AppError> {
    let dir = paths.generation_jobs_payload_dir(silo)?;
    let data_path = dir.join(format!("{doc_id}.bin"));
    let meta_path = dir.join(format!("{doc_id}.json"));

    fs::write(&data_path, bytes).map_err(|source| AppError::Io {
        path: data_path.clone(),
        source,
    })?;

    let meta = GenerationPayloadMeta {
        kind,
        display_name: display_name.to_string(),
    };
    let meta_bytes = serde_json::to_vec(&meta)?;
    fs::write(&meta_path, meta_bytes).map_err(|source| AppError::Io {
        path: meta_path.clone(),
        source,
    })?;
    Ok(())
}

fn load_generation_payload(
    paths: &AppPaths,
    silo: &str,
    doc_id: &str,
) -> Result<(GenerationPayloadKind, String, Vec<u8>), AppError> {
    let dir = paths.generation_jobs_payload_dir(silo)?;
    let data_path = dir.join(format!("{doc_id}.bin"));
    let meta_path = dir.join(format!("{doc_id}.json"));

    let bytes = fs::read(&data_path).map_err(|source| AppError::Io {
        path: data_path.clone(),
        source,
    })?;
    let meta_bytes = fs::read(&meta_path).map_err(|source| AppError::Io {
        path: meta_path.clone(),
        source,
    })?;
    let meta: GenerationPayloadMeta = serde_json::from_slice(&meta_bytes)?;
    Ok((meta.kind, meta.display_name, bytes))
}

fn remove_generation_payload(paths: &AppPaths, silo: &str, doc_id: &str) -> Result<(), AppError> {
    let dir = paths.generation_jobs_payload_dir(silo)?;
    let data_path = dir.join(format!("{doc_id}.bin"));
    let meta_path = dir.join(format!("{doc_id}.json"));

    if let Err(source) = fs::remove_file(&data_path) {
        if source.kind() != std::io::ErrorKind::NotFound {
            return Err(AppError::Io {
                path: data_path,
                source,
            });
        }
    }
    if let Err(source) = fs::remove_file(&meta_path) {
        if source.kind() != std::io::ErrorKind::NotFound {
            return Err(AppError::Io {
                path: meta_path,
                source,
            });
        }
    }
    Ok(())
}

fn inline_part_from_bytes(bytes: &[u8], mime_type: &str, display_name: &str) -> AttachmentPart {
    debug_assert!(!bytes.is_empty());
    debug_assert!(!mime_type.is_empty());
    debug_assert!(!display_name.is_empty());

    use base64::Engine as _;

    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    AttachmentPart::Blob {
        data_ref: ai_ox::content::part::DataRef::base64(encoded),
        mime_type: mime_type.to_string(),
        name: None,
        description: None,
        ext: BTreeMap::new(),
    }
}

async fn run_structured_extraction(
    extractor: &StructuredExtractor,
    payload_kind: &GenerationPayloadKind,
    bytes: &[u8],
    display_name: &str,
) -> Result<StructuredDecision, AppError> {
    match payload_kind {
        GenerationPayloadKind::Pdf => {
            let text = extract_text_from_pdf(bytes)?;
            let attachments = vec![inline_part_from_bytes(
                bytes,
                "application/pdf",
                display_name,
            )];
            let extraction = extractor
                .extract_with_context(&text, &attachments, Some(&text))
                .await?;
            Ok(extraction.decision)
        }
        GenerationPayloadKind::Image { mime } => {
            let attachments = vec![inline_part_from_bytes(bytes, mime.as_str(), display_name)];
            let extraction = extractor
                .extract_with_context(DOCUMENT_PROMPT, &attachments, None)
                .await?;
            Ok(extraction.decision)
        }
    }
}

async fn finalize_generation(
    ctx: &PipelineContext,
    record: &mut GenerationJob,
    decision: StructuredDecision,
) -> Result<(), AppError> {
    use std::str::FromStr;

    if decision.chunks.is_empty() {
        return Err(PipelineError::message("structured decision produced zero chunks").into());
    }
    if decision.chunks.len() > u32::MAX as usize {
        return Err(
            PipelineError::message("structured decision has more chunks than supported").into(),
        );
    }

    let chunk_texts: Vec<String> = decision
        .chunks
        .iter()
        .map(|chunk| chunk.body.clone())
        .collect();
    if chunk_texts.is_empty() {
        return Err(PipelineError::message("structured decision contains no chunk text").into());
    }
    let chunk_refs: Vec<&str> = chunk_texts.iter().map(|text| text.as_str()).collect();

    let content_hash = decision_content_hash(&decision)?;
    if let Some(existing) = &record.content_hash {
        if existing != &content_hash {
            return Err(
                PipelineError::message("job content hash mismatch after generation").into(),
            );
        }
    } else {
        record.content_hash = Some(content_hash);
    }
    record.chunk_count = chunk_refs.len() as u32;

    let vectors = ctx
        .embed
        .embed_batch(
            &chunk_refs,
            EmbedBatchTask::Document,
            ctx.embed.runtime.documents_mode,
        )
        .await?;
    if vectors.len() != chunk_refs.len() {
        return Err(PipelineError::message(format!(
            "embedding count mismatch: expected {}, got {}",
            chunk_refs.len(),
            vectors.len()
        ))
        .into());
    }
    for vector in &vectors {
        if vector.len() != ctx.embed.dim {
            return Err(PipelineError::message(format!(
                "embedding dimension mismatch: expected {}, got {}",
                ctx.embed.dim,
                vector.len()
            ))
            .into());
        }
    }

    let silo = Silo::from_str(&record.silo)
        .map_err(|_| PipelineError::message(format!("unknown silo `{}`", record.silo)))?;

    let index = ctx.index_for(silo)?;
    let actor = MilliActorHandle::spawn(index, 64);
    if let Err(err) =
        index_structured_with_embeddings(&actor, ctx, silo, &record.doc_id, decision, vectors).await
    {
        actor.shutdown().await;
        return Err(AppError::from(err));
    }
    actor.shutdown().await;

    record.pending_decision = None;
    record.completed_at_ms = Some(current_timestamp_ms());
    record.set_status(GenerationJobStatus::Generated, None);
    ctx.jobs.upsert(record)?;
    Ok(())
}

struct ProgressTracker {
    progress: Option<ProgressBar>,
    target_limit: Option<u64>,
    pb_len_set: bool,
    completed: u64,
}

impl ProgressTracker {
    fn new(progress: Option<ProgressBar>, target_limit: Option<u64>) -> Self {
        Self {
            progress,
            target_limit,
            pb_len_set: false,
            completed: 0,
        }
    }

    fn has_progress_bar(&self) -> bool {
        self.progress.is_some()
    }

    async fn handle_event(
        &mut self,
        event: KioEvent,
    ) -> Result<Option<KioScraperSummary>, AppError> {
        match event {
            KioEvent::DiscoveryStarted { limit } => {
                self.update_length(limit.map(|v| v as u64));
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message("Discovering judgments".to_string());
                } else {
                    tracing::info!(limit, "discovery started");
                }
            }
            KioEvent::Discovered {
                ordinal,
                page,
                total_hint,
                metadata,
            } => {
                self.update_length(total_hint.map(|v| v as u64));
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("queued {} (page {page})", metadata.doc_id));
                } else {
                    tracing::debug!(
                        doc_id = %metadata.doc_id,
                        sygnatura = metadata.sygnatura.as_deref().unwrap_or("unknown"),
                        decision_type = metadata.decision_type.as_deref().unwrap_or("unknown"),
                        ordinal,
                        page,
                        total_hint,
                        "discovered document"
                    );
                }
            }
            KioEvent::WorkerStarted { worker, doc_id } => {
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("processing {doc_id}"));
                } else {
                    tracing::debug!(worker, doc_id = %doc_id, "worker picked up document");
                }
            }
            KioEvent::BlobSkipped { cid, upstream_id } => {
                self.bump_position();
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("cached {} (cid: {})", upstream_id, &cid[..8]));
                } else {
                    tracing::info!(
                        cid = %cid,
                        upstream_id = %upstream_id,
                        "document already cached"
                    );
                }
            }
            KioEvent::BlobStored {
                cid,
                upstream_id,
                bytes,
                existed,
            } => {
                self.bump_position();

                if let Some(pb) = self.progress.as_ref() {
                    if existed {
                        pb.set_message(format!("cached {} (cid: {})", upstream_id, &cid[..8]));
                    } else {
                        pb.set_message(format!(
                            "stored {} ({bytes} bytes, cid: {})",
                            upstream_id,
                            &cid[..8]
                        ));
                    }
                } else {
                    tracing::info!(
                        cid = %cid,
                        upstream_id = %upstream_id,
                        bytes,
                        existed,
                        "stored blob"
                    );
                }
            }
            KioEvent::Completed { summary } => return Ok(Some(summary)),
        }

        Ok(None)
    }

    fn update_length(&mut self, hint: Option<u64>) {
        if self.pb_len_set {
            return;
        }
        if let Some(pb) = self.progress.as_ref()
            && let Some(length) = effective_length(self.target_limit, hint)
        {
            pb.set_length(length.max(1));
            self.pb_len_set = true;
        }
    }

    fn bump_position(&mut self) {
        if let Some(pb) = self.progress.as_ref() {
            self.completed += 1;
            if self.pb_len_set {
                pb.set_position(self.completed);
            } else {
                pb.inc(1);
            }
        }
    }

    fn ensure_length(&self, pb: &ProgressBar, discovered: u64) {
        if self.pb_len_set {
            return;
        }
        let length = self.completed.max(discovered);
        if length > 0 {
            pb.set_length(length);
        }
    }
}

fn determine_log_level(cli: &Cli) -> LevelFilter {
    match cli.command.as_ref() {
        Some(Commands::FetchKio(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Serve(_)) => match cli.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Ingest(_)) => match cli.verbose {
            0 => LevelFilter::INFO,
            1 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Audit(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Db(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Search(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::Jobs(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        #[cfg(feature = "cli-debug")]
        Some(Commands::Debug(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        None => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
    }
}

fn make_progress_bar() -> ProgressBar {
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] {pos}/{len} docs ({eta}) {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    pb.set_draw_target(ProgressDrawTarget::stderr_with_hz(12));
    pb.enable_steady_tick(Duration::from_millis(120));
    pb
}

fn effective_length(limit: Option<u64>, hint: Option<u64>) -> Option<u64> {
    match (limit, hint) {
        (Some(l), Some(h)) => Some(l.min(h)),
        (Some(l), None) => Some(l),
        (None, Some(h)) => Some(h),
        (None, None) => None,
    }
}

fn resolve_output_dir(path: &Path) -> Result<PathBuf, AppError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd = env::current_dir().map_err(AppError::WorkingDir)?;
    Ok(cwd.join(path))
}

async fn run_audit(args: AuditArgs) -> Result<(), AppError> {
    match args.command {
        AuditCommands::Structured(sub) => run_audit_structured(sub).await,
    }
}

#[cfg(feature = "cli-debug")]
fn run_debug(args: DebugArgs) -> Result<(), AppError> {
    match args.command {
        DebugCommands::Text(sub) => run_debug_text(sub),
    }
}

#[cfg(feature = "cli-debug")]
fn run_debug_text(args: zetesis_app::cli::DebugTextArgs) -> Result<(), AppError> {
    let bytes = fs::read(&args.input).map_err(|source| AppError::Io {
        path: args.input.clone(),
        source,
    })?;

    let text = extract_text_from_pdf(&bytes)?;
    println!("{}", text);

    Ok(())
}

async fn run_audit_structured(args: StructuredAuditArgs) -> Result<(), AppError> {
    use std::io::Write;
    let paths = collect_pdfs(&args.dir)?;
    let sample = sample_paths(&paths, args.num);

    let mut out: Box<dyn Write> = if args.out_path == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(
            std::fs::File::create(&args.out_path).map_err(|e| AppError::Io {
                path: PathBuf::from(&args.out_path),
                source: e,
            })?,
        )
    };

    let model = args
        .model
        .unwrap_or_else(|| "gemini-2.5-flash-lite-preview-09-2025".to_string());

    writeln!(out, "pdf\tstatus\tsrc_tokens\tchunk_tokens\tdrift_token_pct\tsrc_chars\tchunk_chars\tdrift_char_pct\tdigits_src\tdigits_chunk\tdrift_digit_pct\tstatute_hits_src\tstatute_hits_chunk\theader_in_chunks\tchunks_count")
        .ok();

    let extractor = StructuredExtractor::from_env(&model)?;
    for path in sample {
        let metrics = audit_one_pdf(&path, &extractor).await;
        render_audit_tsv(&mut out, &path, &metrics).ok();
        // brief pause to avoid hammering upstream
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    Ok(())
}

async fn run_search(args: SearchArgs) -> Result<(), AppError> {
    match args.command {
        SearchCommands::Keyword(sub) => search_keyword(sub).await?,
        SearchCommands::Vector(sub) => search_vector(sub).await?,
        SearchCommands::Hybrid(sub) => search_hybrid(sub).await?,
    }
    Ok(())
}

async fn run_jobs(args: JobsArgs) -> Result<(), AppError> {
    match args.command {
        JobsCommands::Status(sub) => jobs_status(sub).await,
        JobsCommands::Gen(sub) => jobs_gen(sub).await,
        JobsCommands::Reap(sub) => jobs_reap(sub).await,
    }
}

async fn jobs_status(args: JobsStatusArgs) -> Result<(), AppError> {
    use comfy_table::{Table, presets};
    use zetesis_app::cli::JobsStatusFormat;

    let paths = AppPaths::from_project_dirs()?;
    let store = GenerationJobStore::open(&paths)
        .map_err(PipelineError::from)
        .map_err(AppError::from)?;

    let stats = store
        .list_all_with_counts()
        .map_err(PipelineError::from)
        .map_err(AppError::from)?;

    match args.format {
        JobsStatusFormat::Json => {
            let mut output = serde_json::Map::new();
            for (status, (count, oldest_created, oldest_updated)) in stats {
                let status_name = match status {
                    GenerationJobStatus::Pending => "pending",
                    GenerationJobStatus::Generating => "generating",
                    GenerationJobStatus::Generated => "generated",
                    GenerationJobStatus::Failed => "failed",
                };
                let mut status_obj = serde_json::Map::new();
                status_obj.insert("count".to_string(), serde_json::json!(count));
                if let Some(c) = oldest_created {
                    status_obj.insert("oldest_created_ms".to_string(), serde_json::json!(c));
                }
                if let Some(u) = oldest_updated {
                    status_obj.insert("oldest_updated_ms".to_string(), serde_json::json!(u));
                }
                output.insert(
                    status_name.to_string(),
                    serde_json::Value::Object(status_obj),
                );
            }
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        JobsStatusFormat::Table => {
            let mut table = Table::new();
            table.load_preset(presets::UTF8_FULL);
            table.set_header(vec!["Status", "Count", "Oldest Created", "Oldest Updated"]);

            for (status, (count, oldest_created, oldest_updated)) in stats {
                let status_name = match status {
                    GenerationJobStatus::Pending => "pending",
                    GenerationJobStatus::Generating => "generating",
                    GenerationJobStatus::Generated => "generated",
                    GenerationJobStatus::Failed => "failed",
                };
                let created_str = oldest_created
                    .map(|ms| format_timestamp(ms))
                    .unwrap_or_else(|| "-".to_string());
                let updated_str = oldest_updated
                    .map(|ms| format_timestamp(ms))
                    .unwrap_or_else(|| "-".to_string());
                let count_str = count.to_string();
                table.add_row(vec![
                    status_name.to_string(),
                    count_str,
                    created_str,
                    updated_str,
                ]);
            }
            println!("{table}");
        }
    }

    Ok(())
}

fn format_timestamp(ms: i64) -> String {
    use chrono::DateTime;
    let seconds = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    match DateTime::from_timestamp(seconds, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("{} ms", ms),
    }
}

async fn jobs_gen(args: JobsGenArgs) -> Result<(), AppError> {
    match args.command {
        JobsGenCommands::Submit(sub) => jobs_gen_submit(sub).await,
        JobsGenCommands::Fetch(sub) => jobs_gen_fetch(sub).await,
    }
}

async fn jobs_gen_submit(args: JobsGenSubmitArgs) -> Result<(), AppError> {
    if args.limit == 0 {
        println!("jobs gen submit: limit must be greater than zero");
        return Ok(());
    }

    let ctx = build_pipeline_context(&args.embed_model)?;
    let client =
        GeminiBatchStructuredClient::from_env(args.gen_model.clone(), ctx.governors.io.clone())
            .map_err(AppError::from)?;

    let candidates = ctx
        .jobs
        .list_by_status_with_filter(
            GenerationJobStatus::Pending,
            args.limit.saturating_mul(8).max(args.limit),
            |job| {
                job.generation_mode == GenerationMode::Batch
                    && job.generator_key == args.gen_model
                    && job.embedder_key == args.embed_model
            },
        )
        .map_err(PipelineError::from)
        .map_err(AppError::from)?;

    if candidates.is_empty() {
        println!(
            "no pending generation jobs found for generator `{}` and embedder `{}`",
            args.gen_model, args.embed_model
        );
        return Ok(());
    }

    let mut processed = 0usize;
    for job in candidates.into_iter().take(args.limit) {
        let mut record =
            match ctx
                .jobs
                .update_status(&job.job_id, GenerationJobStatus::Generating, None)
            {
                Ok(record) => record,
                Err(err) => {
                    tracing::warn!(
                        job_id = job.job_id.as_str(),
                        error = %err,
                        "failed to mark pending job as generating"
                    );
                    continue;
                }
            };
        let (payload_kind, display_name, payload_bytes) =
            match load_generation_payload(&ctx.paths, &record.silo, &record.doc_id) {
                Ok(value) => value,
                Err(err) => {
                    let msg = format!("failed to load payload: {err}");
                    fail_job(&ctx, &mut record, msg);
                    continue;
                }
            };

        let batch_input = match payload_kind {
            GenerationPayloadKind::Pdf => {
                let text = match extract_text_from_pdf(&payload_bytes) {
                    Ok(text) => text,
                    Err(err) => {
                        fail_job(&ctx, &mut record, err.to_string());
                        continue;
                    }
                };
                StructuredBatchInput {
                    doc_id: record.doc_id.clone(),
                    display_name: display_name.clone(),
                    mime_type: "application/pdf".to_string(),
                    bytes: payload_bytes,
                    text,
                }
            }
            GenerationPayloadKind::Image { mime } => StructuredBatchInput {
                doc_id: record.doc_id.clone(),
                display_name: display_name.clone(),
                mime_type: mime,
                bytes: payload_bytes,
                text: DOCUMENT_PROMPT.to_string(),
            },
        };

        let submission = match client
            .submit_job(StructuredBatchRequest::single(batch_input))
            .await
        {
            Ok(response) => response,
            Err(err) => {
                fail_job(&ctx, &mut record, err.to_string());
                continue;
            }
        };

        record.provider_kind = GenerationProviderKind::GeminiBatch;
        record.provider_job_id = Some(submission.provider_job_id.clone());
        record.submitted_batch_count = record
            .submitted_batch_count
            .saturating_add(submission.batch_count);
        record.submitted_at_ms = Some(current_timestamp_ms());
        record.error = None;
        record.last_error = None;
        record.next_retry_at_ms = None;
        record.pending_decision = None;

        if let Err(err) = ctx.jobs.upsert(&record) {
            tracing::warn!(
                job_id = record.job_id.as_str(),
                error = %err,
                "failed to persist generation job submission state"
            );
            continue;
        }

        processed = processed.saturating_add(1);
        tracing::info!(
            event = "generation_job_submitted",
            job_id = record.job_id.as_str(),
            provider_job_id = submission.provider_job_id.as_str(),
            "submitted generation job to provider"
        );
    }

    if processed == 0 {
        println!("no generation jobs submitted");
    } else {
        println!("submitted {processed} generation job(s)");
    }
    Ok(())
}

async fn jobs_gen_fetch(args: JobsGenFetchArgs) -> Result<(), AppError> {
    if args.limit == 0 {
        println!("jobs gen fetch: limit must be greater than zero");
        return Ok(());
    }

    let ctx = build_pipeline_context(&args.embed_model)?;
    let candidates = ctx
        .jobs
        .list_by_status_with_filter(
            GenerationJobStatus::Generating,
            args.limit.saturating_mul(8).max(args.limit),
            |job| {
                job.generation_mode == GenerationMode::Batch && job.embedder_key == args.embed_model
            },
        )
        .map_err(PipelineError::from)
        .map_err(AppError::from)?;

    if candidates.is_empty() {
        println!(
            "no in-flight generation jobs found for embedder `{}`",
            args.embed_model
        );
        return Ok(());
    }

    let mut client_cache: HashMap<String, Arc<GeminiBatchStructuredClient>> = HashMap::new();
    let mut processed = 0usize;
    for job in candidates.into_iter().take(args.limit) {
        let mut record = job.clone();
        let generator_key = record.generator_key.clone();
        let Some(provider_job_id) = record.provider_job_id.clone() else {
            fail_job(
                &ctx,
                &mut record,
                "provider job id missing for generating job",
            );
            continue;
        };

        let client = match client_cache.entry(generator_key.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => match GeminiBatchStructuredClient::from_env(
                generator_key.clone(),
                ctx.governors.io.clone(),
            ) {
                Ok(client) => entry.insert(Arc::new(client)).clone(),
                Err(err) => {
                    fail_job(&ctx, &mut record, err.to_string());
                    continue;
                }
            },
        };

        let metadata = match client.job_state(&provider_job_id).await {
            Ok(meta) => meta,
            Err(err) => {
                fail_job(&ctx, &mut record, err.to_string());
                continue;
            }
        };

        match metadata.state {
            ProviderJobState::Pending | ProviderJobState::Running => {
                tracing::info!(
                    event = "generation_job_inflight",
                    job_id = record.job_id.as_str(),
                    provider_job_id = provider_job_id.as_str(),
                    attempted_fetch = metadata.failed_count,
                    "generation job still running at provider"
                );
                continue;
            }
            ProviderJobState::Failed { message, details } => {
                let mut full_message = message;
                if let Some(extra) = details {
                    if !extra.is_empty() {
                        full_message.push_str(&format!(" ({})", extra.join(", ")));
                    }
                }
                fail_job(&ctx, &mut record, full_message);
                continue;
            }
            ProviderJobState::Succeeded => {}
        }

        let response = match client.fetch_job_result(&provider_job_id).await {
            Ok(resp) => resp,
            Err(err) => {
                fail_job(&ctx, &mut record, err.to_string());
                continue;
            }
        };

        let mut decisions = response.decisions;
        let Some(decision) = decisions.pop() else {
            fail_job(
                &ctx,
                &mut record,
                "provider returned no structured decision payloads",
            );
            continue;
        };

        if decision.chunks.is_empty() {
            fail_job(
                &ctx,
                &mut record,
                "structured decision produced zero chunks",
            );
            continue;
        }
        if decision.chunks.len() > u32::MAX as usize {
            fail_job(
                &ctx,
                &mut record,
                "structured decision has more chunks than supported",
            );
            continue;
        }

        let chunk_count = decision.chunks.len();
        let hash = match decision_content_hash(&decision) {
            Ok(hash) => hash,
            Err(err) => {
                fail_job(&ctx, &mut record, err.to_string());
                continue;
            }
        };

        record.pending_decision = Some(decision.clone());
        record.content_hash = Some(hash);
        record.chunk_count = chunk_count as u32;
        record.provider_kind = GenerationProviderKind::GeminiBatch;
        record.error = None;
        record.last_error = None;
        record.next_retry_at_ms = None;

        match finalize_generation(&ctx, &mut record, decision).await {
            Ok(()) => {
                processed = processed.saturating_add(1);
                if let Err(err) =
                    remove_generation_payload(&ctx.paths, &record.silo, &record.doc_id)
                {
                    tracing::warn!(
                        job_id = record.job_id.as_str(),
                        error = %err,
                        "failed to remove generation payload after completion"
                    );
                }
                tracing::info!(
                    event = "generation_job_completed",
                    job_id = record.job_id.as_str(),
                    provider_job_id = provider_job_id.as_str(),
                    "generation job finalized"
                );
            }
            Err(err) => {
                fail_job(&ctx, &mut record, err.to_string());
            }
        }
    }

    if processed == 0 {
        println!("no generation jobs completed during fetch");
    } else {
        println!("completed {processed} generation job(s)");
    }
    Ok(())
}

async fn jobs_reap(args: JobsReapArgs) -> Result<(), AppError> {
    use comfy_table::{Table, presets};
    use zetesis_app::cli::JobsReapAction;
    use zetesis_app::services::{ReaperAction, ReaperConfig, reap_stale_jobs};

    let paths = AppPaths::from_project_dirs()?;
    let store = GenerationJobStore::open(&paths)
        .map_err(PipelineError::from)
        .map_err(AppError::from)?;

    let action = match args.action {
        JobsReapAction::Requeue => ReaperAction::Requeue,
        JobsReapAction::Fail => ReaperAction::Fail,
        JobsReapAction::Both => ReaperAction::Both,
    };

    let config = ReaperConfig::default();

    if args.dry_run {
        println!("Dry run mode: no jobs will be modified");
        println!(
            "Reaper config:\n  Pending max age: {} ms ({} hours)\n  Generating max age: {} ms ({} hours)",
            config.pending_max_age_ms,
            config.pending_max_age_ms / 3_600_000,
            config.generating_max_age_ms,
            config.generating_max_age_ms / 3_600_000
        );
        return Ok(());
    }

    let report = reap_stale_jobs(&store, &config, action)?;

    if report.is_empty() {
        println!("No stale jobs found");
    } else {
        let mut table = Table::new();
        table.load_preset(presets::UTF8_FULL);
        table.set_header(vec!["Action", "Count", "Job IDs"]);

        if !report.requeued.is_empty() {
            let requeued_count = report.requeued.len().to_string();
            let requeued_ids = report.requeued.join(", ");
            table.add_row(vec!["Requeued".to_string(), requeued_count, requeued_ids]);
        }
        if !report.failed.is_empty() {
            let failed_count = report.failed.len().to_string();
            let failed_ids = report.failed.join(", ");
            table.add_row(vec!["Failed".to_string(), failed_count, failed_ids]);
        }
        if report.skipped > 0 {
            let skipped_count = report.skipped.to_string();
            table.add_row(vec!["Skipped".to_string(), skipped_count, "-".to_string()]);
        }

        println!("{table}");
        println!(
            "Total: {} jobs processed ({} requeued, {} failed, {} skipped)",
            report.total(),
            report.requeued.len(),
            report.failed.len(),
            report.skipped
        );
    }

    Ok(())
}

fn fail_job(ctx: &PipelineContext, record: &mut GenerationJob, message: impl Into<String>) {
    let msg = message.into();
    tracing::warn!(job_id = record.job_id.as_str(), error = %msg, "job failed");
    record.set_status(GenerationJobStatus::Failed, Some(msg.clone()));
    if let Err(err) = ctx.jobs.upsert(record) {
        tracing::warn!(
            job_id = record.job_id.as_str(),
            error = %err,
            "unable to persist failed job state"
        );
    }
}

fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn run_db(args: DbArgs) -> Result<(), AppError> {
    match args.command {
        DbCommands::List => db_list()?,
        DbCommands::Stats(sub) => db_stats(sub)?,
        DbCommands::Get(sub) => db_get(sub)?,
        DbCommands::Find(sub) => db_find(sub)?,
        DbCommands::Backup(sub) => db_backup(sub)?,
        DbCommands::Purge(sub) => db_purge(sub)?,
        DbCommands::Recover(sub) => db_recover(sub)?,
    }
    Ok(())
}

async fn search_keyword(args: KeywordSearchArgs) -> Result<(), AppError> {
    let KeywordSearchArgs {
        index,
        query,
        filter,
        limit,
        offset,
        sort,
        fields,
        pretty,
    } = args;
    let params = KeywordSearchParams {
        index,
        query,
        filter,
        sort,
        fields,
        limit,
        offset,
    };
    let rows = keyword(&params)?;
    emit_json_rows(&rows, pretty)?;
    Ok(())
}

async fn search_vector(args: VectorSearchArgs) -> Result<(), AppError> {
    let VectorSearchArgs {
        index,
        query,
        embedder,
        filter,
        top_k,
        fields,
        pretty,
    } = args;
    let params = VectorSearchParams {
        index,
        query,
        embedder,
        filter,
        fields,
        top_k,
    };
    let rows = vector(&params).await?;
    emit_json_rows(&rows, pretty)?;
    Ok(())
}

async fn search_hybrid(args: HybridSearchArgs) -> Result<(), AppError> {
    let HybridSearchArgs {
        index,
        query,
        filter,
        embedder,
        limit,
        fields,
        fusion,
        keyword_weight,
        vector_weight,
        pretty,
    } = args;
    let per_branch_limit = limit.min(HYBRID_PER_SOURCE_LIMIT_MAX).max(1);
    let keyword_params = KeywordSearchParams {
        index: index.clone(),
        query: query.clone(),
        filter: filter.clone(),
        sort: Vec::new(),
        fields: fields.clone(),
        limit: per_branch_limit,
        offset: 0,
    };
    let vector_params = VectorSearchParams {
        index,
        query,
        embedder,
        filter,
        fields,
        top_k: per_branch_limit,
    };
    let fusion_mode = match fusion {
        HybridFusionArg::Rrf => HybridFusion::Rrf {
            k: HYBRID_DEFAULT_RRF_K,
        },
        HybridFusionArg::Weighted => {
            let (kw, vw) = normalize_hybrid_weights(keyword_weight, vector_weight)
                .map_err(|msg| AppError::Config(msg.to_string()))?;
            HybridFusion::Weighted {
                keyword_weight: kw,
                vector_weight: vw,
            }
        }
    };
    let params = HybridSearchParams {
        keyword: keyword_params,
        vector: vector_params,
        limit,
        fusion: fusion_mode,
    };
    let rows = hybrid(&params).await?;
    emit_json_rows(&rows, pretty)?;
    Ok(())
}

fn db_list() -> Result<(), AppError> {
    let paths = AppPaths::from_project_dirs()?;
    let base = paths.milli_base_dir()?;
    let mut entries: Vec<(String, PathBuf, u64)> = Vec::new();

    for entry_res in fs::read_dir(&base).map_err(|e| io_error(&base, e))? {
        let entry = entry_res.map_err(|e| io_error(&base, e))?;
        let file_type = entry.file_type().map_err(|e| io_error(&base, e))?;
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let size = directory_size(&path)?;
        entries.push((name, path, size));
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    if entries.is_empty() {
        println!("no Milli indexes found under {}", base.display());
    } else {
        println!("index\tsize_bytes\tsize_pretty\tpath");
        for (name, path, size) in entries {
            println!(
                "{}\t{}\t{}\t{}",
                name,
                size,
                human_size(size),
                path.display()
            );
        }
    }

    Ok(())
}

fn db_stats(args: DbStatsArgs) -> Result<(), AppError> {
    let index_name = normalize_index_name(&args.index)?;
    let paths = AppPaths::from_project_dirs()?;
    let base = paths.milli_base_dir()?;
    let index_path = base.join(&index_name);
    if !index_path.is_dir() {
        return Err(AppError::MissingIndex {
            index: index_name,
            path: index_path,
        });
    }

    let size_bytes = directory_size(&index_path)?;
    let index = open_index_read_only(&index_path)?;
    let rtxn = index.read_txn()?;
    let doc_count = index.number_of_documents(&rtxn)?;
    let backend_label = format_vector_backend(index.get_vector_store(&rtxn)?);
    let doc_summary = summarize_documents(&index, &rtxn)?;
    debug_assert_eq!(doc_summary.total(), doc_count);

    let embedding_configs = index.embedding_configs();
    let embed_configs = embedding_configs.embedding_configs(&rtxn)?;
    let embed_summaries: Vec<String> = embed_configs.iter().map(describe_embedder).collect();

    let searchable_fields: Vec<String> = index
        .searchable_fields(&rtxn)?
        .into_iter()
        .map(|field| field.into_owned())
        .collect();

    let vector_stats = index.vector_store_stats(&rtxn)?;
    let vector_docs = vector_stats.documents.len();
    let vector_embeddings = vector_stats.number_of_embeddings;

    drop(rtxn);

    println!("index: {}", index_name);
    println!("path: {}", index_path.display());
    println!("size: {} bytes ({})", size_bytes, human_size(size_bytes));
    println!(
        "records: {} total (docs: {}, chunks: {}, other: {})",
        doc_count, doc_summary.doc_records, doc_summary.chunk_records, doc_summary.other_records
    );
    println!("vector_backend: {}", backend_label);
    println!(
        "vectors: {} embeddings across {} docs",
        vector_embeddings, vector_docs
    );
    if embed_summaries.is_empty() {
        println!("embedders: (none configured)");
    } else {
        println!("embedders:");
        for summary in embed_summaries {
            println!("  - {}", summary);
        }
    }
    if searchable_fields.is_empty() {
        println!("searchable_fields: (none)");
    } else {
        println!("searchable_fields: {}", searchable_fields.join(", "));
    }

    Ok(())
}

fn db_get(args: DbGetArgs) -> Result<(), AppError> {
    let (index_name, index_path) = resolve_index_dir(&args.index)?;
    let index = open_index_read_only(&index_path)?;
    let rtxn = index.read_txn()?;
    let fields_map = index.fields_ids_map(&rtxn)?;
    let Some(docid) = index.external_documents_ids().get(&rtxn, &args.id)? else {
        return Err(AppError::DocumentNotFound {
            index: index_name,
            id: args.id.clone(),
        });
    };
    let record = index.document(&rtxn, docid)?;
    let value = Value::Object(all_obkv_to_json(record, &fields_map)?);
    let projected = project_value(&value, &args.fields);
    drop(rtxn);
    emit_json_value(&projected, args.pretty)?;
    Ok(())
}

fn db_find(args: DbFindArgs) -> Result<(), AppError> {
    let (_index_name, index_path) = resolve_index_dir(&args.index)?;
    let index = open_index_read_only(&index_path)?;
    let rtxn = index.read_txn()?;
    let fields_map = index.fields_ids_map(&rtxn)?;

    let mut search = MilliSearch::new(&rtxn, &index);
    search.offset(args.offset);
    search.limit(args.limit);

    if let Some(expr) = args
        .filter
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        && let Some(filter) = Filter::from_str(expr)?
    {
        search.filter(filter);
    }

    let result = search.execute()?;
    let mut rows = Vec::with_capacity(result.documents_ids.len());
    for docid in result.documents_ids {
        let document = index.document(&rtxn, docid)?;
        let value = Value::Object(all_obkv_to_json(document, &fields_map)?);
        rows.push(project_value(&value, &args.fields));
    }
    drop(rtxn);
    emit_json_rows(&rows, args.pretty)?;
    Ok(())
}

fn db_backup(args: DbBackupArgs) -> Result<(), AppError> {
    let index_name = normalize_index_name(&args.index)?;
    let paths = AppPaths::from_project_dirs()?;
    let base = paths.milli_base_dir()?;
    let src = base.join(&index_name);
    if !src.is_dir() {
        return Err(AppError::MissingIndex {
            index: index_name,
            path: src,
        });
    }

    let dest_root = match &args.out {
        Some(dir) => resolve_output_dir(dir.as_path())?,
        None => resolve_output_dir(Path::new("backups"))?,
    };
    fs::create_dir_all(&dest_root).map_err(|e| io_error(&dest_root, e))?;

    let timestamp = Utc::now().format("%Y%m%dT%H%M%S");
    let dest = dest_root.join(format!("{}-{}", index_name, timestamp));
    if dest.exists() {
        return Err(AppError::BackupDestinationExists { path: dest });
    }

    copy_directory(&src, &dest)?;
    println!("backup complete: {} -> {}", src.display(), dest.display());
    Ok(())
}

fn db_purge(args: DbPurgeArgs) -> Result<(), AppError> {
    let index_name = normalize_index_name(&args.index)?;
    debug_assert!(!index_name.is_empty());
    let paths = AppPaths::from_project_dirs()?;
    let base = paths.milli_base_dir()?;
    let target = base.join(&index_name);
    if !target.is_dir() {
        return Err(AppError::MissingIndex {
            index: index_name.clone(),
            path: target,
        });
    }

    let token = generate_confirmation_token();
    debug_assert_eq!(token.len(), 8);
    let prompt_message = format!(
        "Type `{}` to confirm purging index `{}`",
        token.as_str(),
        index_name.as_str()
    );
    let input = match Text::new(prompt_message.as_str())
        .with_placeholder("confirmation token")
        .with_help_message("Purging removes the Milli index directory permanently.")
        .prompt()
    {
        Ok(value) => value,
        Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => {
            return Err(AppError::PurgeConfirmationCancelled { index: index_name });
        }
        Err(err) => {
            return Err(AppError::PurgePromptFailed { source: err });
        }
    };
    if input.trim() != token {
        return Err(AppError::PurgeConfirmationRejected { index: index_name });
    }

    fs::remove_dir_all(&target).map_err(|e| io_error(&target, e))?;
    println!("purged index `{}` ({})", index_name, target.display());
    Ok(())
}

fn generate_confirmation_token() -> String {
    let raw = Uuid::new_v4().simple().to_string();
    let token: String = raw.chars().take(8).collect();
    debug_assert_eq!(token.len(), 8);
    debug_assert!(token.chars().all(|ch| ch.is_ascii_hexdigit()));
    token
}

#[cfg(test)]
mod tests {
    use super::{doc_id_from_bytes, generate_confirmation_token};

    #[test]
    fn confirmation_token_format_is_consistent() {
        let token = generate_confirmation_token();
        assert_eq!(token.len(), 8);
        assert!(token.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn confirmation_token_is_whitespace_free() {
        let token = generate_confirmation_token();
        assert_eq!(token.trim(), token);
        assert!(!token.chars().any(char::is_whitespace));
    }

    #[test]
    fn blake_doc_id_is_deterministic() {
        let a = b"hello world";
        let b = b"hello world";
        let id_a = doc_id_from_bytes(a);
        let id_b = doc_id_from_bytes(b);
        assert_eq!(id_a, id_b);
        assert!(!id_a.is_empty());
    }

    #[test]
    fn blake_doc_id_changes_with_content() {
        let a = b"hello world";
        let b = b"hello world!";
        let id_a = doc_id_from_bytes(a);
        let id_b = doc_id_from_bytes(b);
        assert_ne!(id_a, id_b);
    }
}

fn db_recover(args: DbRecoverArgs) -> Result<(), AppError> {
    let index_name = normalize_index_name(&args.index)?;
    let raw_source = args.from.clone();
    let source = raw_source
        .canonicalize()
        .map_err(|e| io_error(&raw_source, e))?;
    if !source.is_dir() {
        return Err(AppError::BackupSourceMissing { path: source });
    }

    let paths = AppPaths::from_project_dirs()?;
    let base = paths.milli_base_dir()?;
    let target = base.join(&index_name);

    if target.exists() {
        if !args.force {
            return Err(AppError::RecoverRequiresForce { index: index_name });
        }
        fs::remove_dir_all(&target).map_err(|e| io_error(&target, e))?;
    }

    copy_directory(&source, &target)?;
    println!("recovered index `{}` from {}", index_name, source.display());
    Ok(())
}

fn io_error(path: &Path, source: std::io::Error) -> AppError {
    AppError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn directory_size(root: &Path) -> Result<u64, AppError> {
    if !root.exists() {
        return Ok(0);
    }
    let mut total: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    // Iteration bounded by the number of entries reachable from `root`.
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path).map_err(|e| io_error(&path, e))?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            for entry_res in fs::read_dir(&path).map_err(|e| io_error(&path, e))? {
                let entry = entry_res.map_err(|e| io_error(&path, e))?;
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

fn copy_directory(src: &Path, dst: &Path) -> Result<(), AppError> {
    let mut stack: Vec<(PathBuf, PathBuf)> = vec![(src.to_path_buf(), dst.to_path_buf())];
    // Iteration bounded by the number of entries under `src`.
    while let Some((from_dir, to_dir)) = stack.pop() {
        fs::create_dir_all(&to_dir).map_err(|e| io_error(&to_dir, e))?;
        for entry_res in fs::read_dir(&from_dir).map_err(|e| io_error(&from_dir, e))? {
            let entry = entry_res.map_err(|e| io_error(&from_dir, e))?;
            let file_type = entry.file_type().map_err(|e| io_error(&from_dir, e))?;
            let source_path = entry.path();
            let dest_path = to_dir.join(entry.file_name());
            if file_type.is_dir() {
                stack.push((source_path, dest_path));
            } else if file_type.is_file() {
                fs::copy(&source_path, &dest_path).map_err(|e| io_error(&source_path, e))?;
            }
        }
    }
    Ok(())
}

fn emit_json_value(value: &Value, pretty: bool) -> Result<(), AppError> {
    if pretty {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("{}", serde_json::to_string(value)?);
    }
    Ok(())
}

fn emit_json_rows(rows: &[Value], pretty: bool) -> Result<(), AppError> {
    if pretty {
        let array = Value::Array(rows.to_vec());
        println!("{}", serde_json::to_string_pretty(&array)?);
    } else {
        for row in rows {
            println!("{}", serde_json::to_string(row)?);
        }
    }
    Ok(())
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn describe_embedder(cfg: &IndexEmbeddingConfig) -> String {
    let (kind, dims) = match &cfg.config.embedder_options {
        EmbedderOptions::HuggingFace(_) => ("huggingface", None),
        EmbedderOptions::OpenAi(_) => ("openai", None),
        EmbedderOptions::Ollama(_) => ("ollama", None),
        EmbedderOptions::UserProvided(opts) => ("user-provided", Some(opts.dimensions)),
        EmbedderOptions::Rest(_) => ("rest", None),
        EmbedderOptions::Composite(_) => ("composite", None),
    };
    let quantized_label = if cfg.config.quantized() {
        "quantized"
    } else {
        "float32"
    };
    match dims {
        Some(dim) => format!(
            "{} (kind: {}, dims: {}, {})",
            cfg.name, kind, dim, quantized_label
        ),
        None => format!("{} (kind: {}, {})", cfg.name, kind, quantized_label),
    }
}

fn format_vector_backend(backend: Option<VectorStoreBackend>) -> &'static str {
    match backend {
        Some(VectorStoreBackend::Arroy) => "arroy",
        Some(VectorStoreBackend::Hannoy) => "hannoy",
        None => "unconfigured",
    }
}

#[derive(Default)]
struct DocBreakdown {
    doc_records: u64,
    chunk_records: u64,
    other_records: u64,
}

impl DocBreakdown {
    fn total(&self) -> u64 {
        self.doc_records + self.chunk_records + self.other_records
    }

    fn record(&mut self, doc_type: &str) {
        match doc_type {
            "doc" => self.doc_records = self.doc_records.saturating_add(1),
            "chunk" => self.chunk_records = self.chunk_records.saturating_add(1),
            _ => self.other_records = self.other_records.saturating_add(1),
        }
    }
}

fn summarize_documents(
    index: &MilliIndex,
    rtxn: &heed::RoTxn<'_>,
) -> Result<DocBreakdown, AppError> {
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let mut summary = DocBreakdown::default();
    let documents = index.all_documents(rtxn)?;
    for entry in documents {
        let (_, obkv) = entry?;
        let object = all_obkv_to_json(obkv, &fields_ids_map)?;
        let doc_type = object
            .get("doc_type")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        summary.record(doc_type);
    }
    Ok(summary)
}

fn doc_id_from_bytes(bytes: &[u8]) -> String {
    let hash = blake3::hash(bytes);
    hash.to_hex().to_string()
}

fn collect_pdfs(dir: &Path) -> Result<Vec<PathBuf>, AppError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|source| AppError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| AppError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if let Some(ext) = path.extension()
            && ext == "pdf"
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn sample_paths(all: &[PathBuf], n: usize) -> Vec<PathBuf> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut scored: Vec<(u64, &PathBuf)> = Vec::with_capacity(all.len());
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E3779B97F4A7C15);
    for p in all {
        let mut h = DefaultHasher::new();
        seed.hash(&mut h);
        p.hash(&mut h);
        scored.push((h.finish(), p));
    }
    scored.sort_by_key(|(s, _)| *s);
    scored
        .into_iter()
        .take(n.min(all.len()))
        .map(|(_, p)| p.clone())
        .collect()
}

struct AuditMetrics {
    status_ok: bool,
    src_tokens: usize,
    chunk_tokens: usize,
    drift_token_pct: f32,
    src_chars: usize,
    chunk_chars: usize,
    drift_char_pct: f32,
    digits_src: usize,
    digits_chunk: usize,
    drift_digit_pct: f32,
    statute_hits_src: usize,
    statute_hits_chunk: usize,
    header_in_chunks: bool,
    chunks_count: usize,
}

async fn audit_one_pdf(path: &Path, extractor: &StructuredExtractor) -> AuditMetrics {
    let mut base = AuditMetrics {
        status_ok: false,
        src_tokens: 0,
        chunk_tokens: 0,
        drift_token_pct: 0.0,
        src_chars: 0,
        chunk_chars: 0,
        drift_char_pct: 0.0,
        digits_src: 0,
        digits_chunk: 0,
        drift_digit_pct: 0.0,
        statute_hits_src: 0,
        statute_hits_chunk: 0,
        header_in_chunks: false,
        chunks_count: 0,
    };

    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return base,
    };
    let raw_text = match extract_text_from_pdf(&bytes) {
        Ok(t) => t,
        Err(_) => return base,
    };
    let source_text = cleanup_text(&raw_text);

    let extraction = match extractor.extract(&source_text).await {
        Ok(v) => v,
        Err(_) => return base,
    };

    let chunks_text: String = extraction
        .decision
        .chunks
        .iter()
        .map(|c| c.body.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    base.status_ok = true;
    base.src_tokens = count_ws_tokens(&source_text);
    base.chunk_tokens = count_ws_tokens(&chunks_text);
    base.drift_token_pct = drift_pct(base.src_tokens, base.chunk_tokens);
    base.src_chars = source_text.chars().count();
    base.chunk_chars = chunks_text.chars().count();
    base.drift_char_pct = drift_pct(base.src_chars, base.chunk_chars);
    base.digits_src = count_digits(&source_text);
    base.digits_chunk = count_digits(&chunks_text);
    base.drift_digit_pct = drift_pct(base.digits_src, base.digits_chunk);
    base.statute_hits_src = count_statute_hits(&source_text);
    base.statute_hits_chunk = count_statute_hits(&chunks_text);
    base.header_in_chunks = chunks_text.contains("Sygn. akt") || chunks_text.contains("WYROK");
    base.chunks_count = extraction.decision.chunks.len();

    base
}

fn count_ws_tokens(s: &str) -> usize {
    s.split_whitespace().count()
}

fn drift_pct(src: usize, chunk: usize) -> f32 {
    if src == 0 {
        0.0
    } else {
        ((src as f32 - chunk as f32) * 100.0) / src as f32
    }
}

fn count_digits(s: &str) -> usize {
    s.chars().filter(|ch| ch.is_ascii_digit()).count()
}

fn count_statute_hits(s: &str) -> usize {
    // Rough surface cues: art., ust., §, Dz. U.
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"(?i)(art\.|ust\.|§|dz\.?\s*u\.)").expect("statute regex")
    });
    re.find_iter(s).count()
}

fn render_audit_tsv(
    out: &mut dyn std::io::Write,
    path: &Path,
    m: &AuditMetrics,
) -> std::io::Result<()> {
    writeln!(
        out,
        "{}\t{}\t{}\t{}\t{:.1}\t{}\t{}\t{:.1}\t{}\t{}\t{:.1}\t{}\t{}\t{}\t{}",
        path.display(),
        if m.status_ok { "ok" } else { "fail" },
        m.src_tokens,
        m.chunk_tokens,
        m.drift_token_pct,
        m.src_chars,
        m.chunk_chars,
        m.drift_char_pct,
        m.digits_src,
        m.digits_chunk,
        m.drift_digit_pct,
        m.statute_hits_src,
        m.statute_hits_chunk,
        if m.header_in_chunks { "yes" } else { "no" },
        m.chunks_count,
    )
}
