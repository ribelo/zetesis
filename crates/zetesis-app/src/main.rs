use std::{
    env, fs, num::NonZeroUsize, path::Path, path::PathBuf, pin::Pin, process, time::Duration,
};

use futures::StreamExt;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde_json::json;
use thiserror::Error;
use tracing_subscriber::{filter::LevelFilter, fmt};
use zetesis_app::cli::{
    Cli, Commands, DEFAULT_KIO_SAOS_URL, DEFAULT_KIO_UZP_URL, FetchKioArgs, KioSource,
    SegmentOutputFormat, SegmentPdfArgs,
};
use zetesis_app::ingestion::{
    KioEvent, KioSaosScraper, KioScrapeOptions, KioScraperSummary, KioUzpScraper,
};
use zetesis_app::services::{PolishSentenceSegmenter, cleanup_text, extract_text_from_pdf};
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
        None => {
            Cli::print_help();
        }
    }

    Ok(())
}

async fn run_fetch_kio(args: FetchKioArgs, verbosity: u8) -> Result<(), AppError> {
    let worker_count = NonZeroUsize::new(args.workers.max(1)).expect("workers must always be >= 1");
    let output_dir = resolve_output_dir(&args.output_path)?;

    let base_url = match args.source {
        KioSource::Uzp => args.url.clone(),
        KioSource::Saos => {
            if args.url == DEFAULT_KIO_UZP_URL {
                DEFAULT_KIO_SAOS_URL.to_string()
            } else {
                args.url.clone()
            }
        }
    };

    tracing::info!(
        source = ?args.source,
        url = %base_url,
        limit = ?args.limit,
        workers = worker_count.get(),
        output = %output_dir.display(),
        "starting KIO portal scrape"
    );

    let builder = KioScrapeOptions::builder()
        .output_dir(output_dir.clone())
        .worker_count(worker_count);
    let options = builder.maybe_limit(args.limit).build();
    let show_progress = verbosity == 0;
    enum SelectedScraper {
        Uzp(KioUzpScraper),
        Saos(KioSaosScraper),
    }

    let scraper = match args.source {
        KioSource::Uzp => SelectedScraper::Uzp(KioUzpScraper::new(&base_url)?),
        KioSource::Saos => SelectedScraper::Saos(KioSaosScraper::new(&base_url)?),
    };

    let mut stream: Pin<
        Box<dyn futures::Stream<Item = Result<KioEvent, ingestion::IngestorError>> + Send>,
    > = match &scraper {
        SelectedScraper::Uzp(scraper) => Box::pin(scraper.scrape_stream(options.clone())),
        SelectedScraper::Saos(scraper) => Box::pin(scraper.scrape_stream(options)),
    };

    let progress = show_progress.then_some(make_progress_bar());
    let mut summary: Option<KioScraperSummary> = None;
    let target_limit = args.limit.map(|l| l as u64);
    let mut pb_len_set = false;
    let mut completed = 0u64;

    while let Some(event) = stream.next().await {
        let event = event?;
        match event {
            KioEvent::DiscoveryStarted { limit } => {
                if let Some(pb) = progress.as_ref() {
                    if !pb_len_set {
                        if let Some(length) =
                            effective_length(target_limit, limit.map(|v| v as u64))
                        {
                            pb.set_length(length.max(1));
                            pb_len_set = true;
                        }
                    }
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
                if let Some(pb) = progress.as_ref() {
                    if !pb_len_set {
                        if let Some(length) =
                            effective_length(target_limit, total_hint.map(|v| v as u64))
                        {
                            pb.set_length(length.max(1));
                            pb_len_set = true;
                        }
                    }
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
                if let Some(pb) = progress.as_ref() {
                    pb.set_message(format!("processing {doc_id}"));
                } else {
                    tracing::debug!(worker, doc_id = %doc_id, "worker picked up document");
                }
            }
            KioEvent::DownloadSkipped { doc_id } => {
                if let Some(pb) = progress.as_ref() {
                    completed += 1;
                    if pb_len_set {
                        pb.set_position(completed);
                    } else {
                        pb.inc(1);
                    }
                    pb.set_message(format!("cached {doc_id}"));
                } else {
                    tracing::info!(doc_id = %doc_id, "document already cached");
                }
            }
            KioEvent::DownloadCompleted { doc_id, bytes } => {
                if let Some(pb) = progress.as_ref() {
                    completed += 1;
                    if pb_len_set {
                        pb.set_position(completed);
                    } else {
                        pb.inc(1);
                    }
                    pb.set_message(format!("saved {doc_id} ({bytes} bytes)"));
                } else {
                    tracing::info!(doc_id = %doc_id, bytes, "downloaded document");
                }
            }
            KioEvent::Completed { summary: s } => {
                summary = Some(s);
                break;
            }
        }
    }

    let summary =
        summary.ok_or_else(|| AppError::Ingest(ingestion::IngestorError::ChannelClosed))?;

    if let Some(pb) = progress {
        if !pb_len_set {
            let length = completed.max(summary.discovered as u64);
            if length > 0 {
                pb.set_length(length);
            }
        }
        pb.finish_with_message(format!(
            "Completed: {}/{} downloaded ({} skipped)",
            summary.downloaded, summary.discovered, summary.skipped_existing
        ));
    } else {
        tracing::info!(
            downloaded = summary.downloaded,
            discovered = summary.discovered,
            skipped = summary.skipped_existing,
            "KIO scrape completed successfully"
        );
    }

    Ok(())
}

fn run_segment_pdf(args: SegmentPdfArgs) -> Result<(), AppError> {
    for input in &args.inputs {
        let bytes = fs::read(input).map_err(|source| AppError::Io {
            path: input.clone(),
            source,
        })?;

        let raw_text = extract_text_from_pdf(&bytes)?;
        let cleaned = cleanup_text(&raw_text);
        let sentences = PolishSentenceSegmenter::split(&cleaned)
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        match args.format {
            SegmentOutputFormat::Text => render_sentences_text(input, &sentences),
            SegmentOutputFormat::Json => render_sentences_json(input, &sentences)?,
        }
    }

    Ok(())
}

fn render_sentences_text(path: &Path, sentences: &[String]) {
    println!("== {} ==", path.display());
    let mut first = true;
    for sentence in sentences {
        if !first {
            println!();
        }
        println!("{}", sentence);
        first = false;
    }
    println!();
}

fn render_sentences_json(path: &Path, sentences: &[String]) -> Result<(), AppError> {
    let input = path.display().to_string();
    for (idx, sentence) in sentences.iter().enumerate() {
        let payload = json!({
            "input": input,
            "ord": idx,
            "content": sentence,
        });
        println!("{}", serde_json::to_string(&payload)?);
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
