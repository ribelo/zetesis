use std::{
    env, fs, num::NonZeroUsize, ops::Range, path::Path, path::PathBuf, process, time::Duration,
};

use futures::StreamExt;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde_json::json;
use thiserror::Error;
use tracing_subscriber::{filter::LevelFilter, fmt};
use zetesis_app::cli::{
    Cli, Commands, DEFAULT_KIO_SAOS_URL, DEFAULT_KIO_UZP_URL, FetchKioArgs, KioSource, OcrPdfArgs,
    SegmentOutputFormat, SegmentPdfArgs,
};
use zetesis_app::ingestion::{
    KioEvent, KioSaosScraper, KioScrapeOptions, KioScraperSummary, KioUzpScraper,
};
use zetesis_app::services::{
    OcrConfig, OcrError, PolishSentenceSegmenter, cleanup_text, extract_text_from_pdf,
    run_ocr_document,
};
use zetesis_app::{config, ingestion, server};

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

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] config::AppConfigError),
    #[error(transparent)]
    Server(#[from] server::ServerError),
    #[error(transparent)]
    Ingest(#[from] ingestion::IngestorError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Pdf(#[from] zetesis_app::services::PdfTextError),
    #[error(transparent)]
    PdfRender(#[from] zetesis_app::services::PdfRenderError),
    #[error(transparent)]
    Ocr(#[from] OcrError),
    #[error("failed to read input file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to resolve current working directory: {0}")]
    WorkingDir(#[source] std::io::Error),
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
        Some(Commands::SegmentPdf(args)) => {
            run_segment_pdf(args)?;
        }
        Some(Commands::OcrPdf(args)) => {
            run_ocr_pdf(args).await?;
        }
        None => {
            Cli::print_help();
        }
    }

    Ok(())
}

async fn run_fetch_kio(args: FetchKioArgs, verbosity: u8) -> Result<(), AppError> {
    let worker_count = NonZeroUsize::new(args.workers.max(1)).expect("workers must always be >= 1");
    let output_dir = resolve_output_dir(&args.output_path)?;
    let base_url = resolve_kio_base_url(&args);

    tracing::info!(
        source = ?args.source,
        url = %base_url,
        limit = ?args.limit,
        workers = worker_count.get(),
        output = %output_dir.display(),
        "starting KIO portal scrape"
    );

    let options = KioScrapeOptions::builder()
        .output_dir(output_dir.clone())
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
            downloaded = summary.downloaded,
            discovered = summary.discovered,
            skipped = summary.skipped_existing,
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
    S: futures::Stream<Item = Result<KioEvent, ingestion::IngestorError>> + Unpin,
{
    while let Some(event) = stream.next().await {
        let summary = tracker.handle_event(event?)?;
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
            "Completed: {}/{} downloaded ({} skipped)",
            summary.downloaded, summary.discovered, summary.skipped_existing
        ));
    }
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

    fn handle_event(&mut self, event: KioEvent) -> Result<Option<KioScraperSummary>, AppError> {
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
            KioEvent::DownloadSkipped { doc_id } => {
                self.bump_position();
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("cached {doc_id}"));
                } else {
                    tracing::info!(doc_id = %doc_id, "document already cached");
                }
            }
            KioEvent::DownloadCompleted { doc_id, bytes } => {
                self.bump_position();
                if let Some(pb) = self.progress.as_ref() {
                    pb.set_message(format!("saved {doc_id} ({bytes} bytes)"));
                } else {
                    tracing::info!(doc_id = %doc_id, bytes, "downloaded document");
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
        if let Some(pb) = self.progress.as_ref() {
            if let Some(length) = effective_length(self.target_limit, hint) {
                pb.set_length(length.max(1));
                self.pb_len_set = true;
            }
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

fn run_segment_pdf(args: SegmentPdfArgs) -> Result<(), AppError> {
    for input in &args.inputs {
        let bytes = fs::read(input).map_err(|source| AppError::Io {
            path: input.clone(),
            source,
        })?;

        let raw_text = extract_text_from_pdf(&bytes)?;
        let cleaned = cleanup_text(&raw_text);
        let segments = PolishSentenceSegmenter::ranges(&cleaned)
            .into_iter()
            .map(|range| {
                let content = cleaned[range.clone()].to_string();
                (range, content)
            })
            .filter(|(_, content)| !content.is_empty())
            .collect::<Vec<_>>();

        match args.format {
            SegmentOutputFormat::Text => render_sentences_text(input, &segments, args.with_offsets),
            SegmentOutputFormat::Json => {
                render_sentences_json(input, &segments, args.with_offsets)?
            }
        }
    }

    Ok(())
}

async fn run_ocr_pdf(args: OcrPdfArgs) -> Result<(), AppError> {
    let config = OcrConfig {
        model: args.model.clone(),
        render_width: args.render_width,
        image_max_edge: args.image_max_edge,
        detail: args.detail.clone(),
        max_tokens: args.max_tokens,
    };

    let results = run_ocr_document(&args.input, &config).await?;

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, &results)?;
    println!();

    Ok(())
}

fn render_sentences_text(path: &Path, segments: &[(Range<usize>, String)], with_offsets: bool) {
    println!("== {} ==", path.display());
    let mut first = true;
    for (range, sentence) in segments {
        if !first {
            println!();
        }
        if with_offsets {
            println!("{:>8}..{:>8} {}", range.start, range.end, sentence);
        } else {
            println!("{}", sentence);
        }
        first = false;
    }
    println!();
}

fn render_sentences_json(
    path: &Path,
    segments: &[(Range<usize>, String)],
    with_offsets: bool,
) -> Result<(), AppError> {
    let input = path.display().to_string();
    for (idx, (range, sentence)) in segments.iter().enumerate() {
        let mut payload = serde_json::Map::new();
        payload.insert("input".to_string(), json!(&input));
        payload.insert("ord".to_string(), json!(idx));
        payload.insert("content".to_string(), json!(sentence));

        if with_offsets {
            payload.insert("start".to_string(), json!(range.start));
            payload.insert("end".to_string(), json!(range.end));
        }

        println!("{}", serde_json::Value::Object(payload).to_string());
    }
    Ok(())
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
        Some(Commands::SegmentPdf(_)) => match cli.verbose {
            0 => LevelFilter::OFF,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        },
        Some(Commands::OcrPdf(_)) => match cli.verbose {
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
