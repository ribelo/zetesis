use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_stream::try_stream;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use futures_util::stream::Stream;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use reqwest::{Client, Url, header};
use serde::Deserialize;
use tokio::{
    fs,
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::sleep,
};
use tracing::{debug, info, warn};

use crate::ingestion::kio_types::{
    KioDocumentMetadata, KioEvent, KioScrapeError, KioScrapeOptions, KioScraperSummary,
};

const SILO_SLUG: &str = "kio-saos";
const SEARCH_ENDPOINT: &str = "api/search/judgments";
const DETAIL_ENDPOINT_PREFIX: &str = "api/judgments/";
const FILES_PREFIX: &str = "files/judgments/national_appeal_chamber";
const CUT_OFF_DATE: &str = "2020-12-31";
const PAGE_SIZE: usize = 100;

type GenericRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[derive(Debug, Clone)]
pub struct KioSaosScraper {
    base_url: Url,
    http: Client,
    rate_limiter: Arc<GenericRateLimiter>,
    backoff: ExponentialBuilder,
}

#[derive(Debug, Clone)]
struct SaosDocumentTask {
    id: u64,
    doc_id: String,
    case_number: Option<String>,
    judgment_type: Option<String>,
}

#[derive(Debug)]
struct SaosSearchPage {
    items: Vec<SaosDocumentTask>,
    total_available: Option<usize>,
}

#[derive(Debug)]
struct SaosDetail {
    judgment_id: String,
    judgment_date: String,
}

#[derive(Deserialize)]
struct SearchResponse {
    #[serde(default)]
    items: Vec<SearchItem>,
    info: Option<SearchInfo>,
}

#[derive(Deserialize)]
struct SearchInfo {
    #[serde(rename = "totalResults")]
    total_results: usize,
}

#[derive(Deserialize)]
struct SearchItem {
    id: u64,
    #[serde(rename = "judgmentType")]
    judgment_type: Option<String>,
    #[serde(rename = "courtCases", default)]
    court_cases: Vec<CourtCase>,
}

#[derive(Deserialize)]
struct CourtCase {
    #[serde(rename = "caseNumber")]
    case_number: Option<String>,
}

#[derive(Deserialize)]
struct DetailResponse {
    data: DetailData,
}

#[derive(Deserialize)]
struct DetailData {
    #[serde(rename = "judgmentDate")]
    judgment_date: Option<String>,
    #[serde(default)]
    source: Option<DetailSource>,
}

#[derive(Deserialize)]
struct DetailSource {
    #[serde(rename = "judgmentId")]
    judgment_id: Option<String>,
}

enum EventMessage {
    Event(KioEvent),
    Error(KioScrapeError),
}

type EventSender = mpsc::Sender<EventMessage>;

async fn send_event(sender: &EventSender, event: KioEvent) -> Result<(), KioScrapeError> {
    sender
        .send(EventMessage::Event(event))
        .await
        .map_err(|_| KioScrapeError::ChannelClosed)
}

impl KioSaosScraper {
    pub fn new(base_url: &str) -> Result<Self, KioScrapeError> {
        let parsed = Url::parse(base_url)
            .map_err(|_| KioScrapeError::InvalidBaseUrl(base_url.to_string()))?;

        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("zetesis-kio-saos/0.1")
            .build()
            .map_err(|err| KioScrapeError::request("build_client", err))?;

        let quota = Quota::per_second(NonZeroU32::new(4).unwrap());
        let rate_limiter = Arc::new(RateLimiter::direct(quota));
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(250))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(6)
            .with_jitter();

        Ok(Self {
            base_url: parsed,
            http,
            rate_limiter,
            backoff,
        })
    }

    pub fn scrape_stream(
        &self,
        opts: KioScrapeOptions,
    ) -> impl Stream<Item = Result<KioEvent, KioScrapeError>> {
        let capacity = opts.channel_capacity.max(1);
        let (event_tx, event_rx) = mpsc::channel(capacity);
        let scraper = self.clone();
        tokio::spawn(async move {
            if let Err(err) = scraper.run_scrape(opts, event_tx.clone()).await {
                let _ = event_tx.send(EventMessage::Error(err)).await;
            }
        });

        try_stream! {
            let mut rx = event_rx;
            while let Some(message) = rx.recv().await {
                match message {
                    EventMessage::Event(event) => yield event,
                    EventMessage::Error(err) => Err(err)?,
                }
            }
        }
    }

    async fn run_scrape(
        &self,
        opts: KioScrapeOptions,
        event_tx: EventSender,
    ) -> Result<(), KioScrapeError> {
        ensure_dir(&opts.output_dir).await?;
        send_event(&event_tx, KioEvent::DiscoveryStarted { limit: opts.limit }).await?;

        let worker_count = opts.worker_count.get();
        let channel_capacity = opts.channel_capacity.max(1);
        let mut discovery_concurrency = opts.discovery_concurrency.get();
        discovery_concurrency = discovery_concurrency.min(worker_count);
        discovery_concurrency = discovery_concurrency.min(channel_capacity);
        let (sender, receiver) = mpsc::channel(channel_capacity);

        let downloaded = Arc::new(AtomicUsize::new(0));
        let skipped_existing = Arc::new(AtomicUsize::new(0));

        let worker_stats = WorkerStats::new(
            downloaded.clone(),
            skipped_existing.clone(),
            event_tx.clone(),
        );

        assert!(
            discovery_concurrency > 0,
            "discovery concurrency must be positive"
        );

        let mut join_set = spawn_workers(
            self.clone(),
            receiver,
            opts.output_dir.clone(),
            worker_count,
            worker_stats,
        );

        let discovery = self
            .run_discovery(&sender, opts.limit, discovery_concurrency, &event_tx)
            .await?;

        drop(sender);

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(join_err) => return Err(to_scraper_join_error(join_err)),
            }
        }

        let summary = KioScraperSummary {
            discovered: discovery.discovered,
            total_available_hint: discovery.total_available,
            downloaded: downloaded.load(Ordering::Relaxed),
            skipped_existing: skipped_existing.load(Ordering::Relaxed),
        };

        send_event(&event_tx, KioEvent::Completed { summary }).await?;
        Ok(())
    }

    async fn run_discovery(
        &self,
        sender: &mpsc::Sender<SaosDocumentTask>,
        limit: Option<usize>,
        discovery_concurrency: usize,
        event_tx: &EventSender,
    ) -> Result<DiscoveryStats, KioScrapeError> {
        assert!(
            discovery_concurrency > 0,
            "discovery concurrency must be non-zero"
        );

        let mut discovered = 0usize;
        let mut target = compute_discovery_target(limit, None);
        let mut total_available = None;
        let mut next_page = 0usize;
        let mut should_stop = false;

        let mut join_set = JoinSet::new();
        let mut inflight = 0usize;

        // See kio_uzp discovery notes: JoinSet keeps bounded fan-out with early cancellation
        // when we reach the discovery target. futures-concurrency collectors would decouple
        // fetch completion from spawn decisions, so JoinSet remains the clearer choice.
        for _ in 0..discovery_concurrency {
            join_set.spawn(fetch_saos_discovery_page(self.clone(), next_page));
            next_page += 1;
            inflight += 1;
        }

        while let Some(result) = join_set.join_next().await {
            inflight -= 1;
            let (page, response) = match result {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => return Err(err),
                Err(join_err) => return Err(to_scraper_join_error(join_err)),
            };

            if !should_stop {
                if let Some(total) = response.total_available {
                    total_available = Some(total);
                    target = compute_discovery_target(limit, total_available);
                }

                if response.items.is_empty() {
                    should_stop = true;
                } else {
                    for task in response.items {
                        if discovered >= target {
                            should_stop = true;
                            break;
                        }

                        let metadata = KioDocumentMetadata {
                            doc_id: task.doc_id.clone(),
                            sygnatura: task.case_number.clone(),
                            decision_type: task.judgment_type.clone(),
                        };

                        send_event(
                            event_tx,
                            KioEvent::Discovered {
                                ordinal: discovered + 1,
                                page: page + 1,
                                total_hint: total_available,
                                metadata,
                            },
                        )
                        .await?;

                        sender
                            .send(task)
                            .await
                            .map_err(|_| KioScrapeError::ChannelClosed)?;

                        discovered += 1;
                    }
                }
            }

            if !should_stop {
                join_set.spawn(fetch_saos_discovery_page(self.clone(), next_page));
                next_page += 1;
                inflight += 1;
            }

            if inflight == 0 {
                break;
            }
        }

        Ok(DiscoveryStats {
            discovered,
            total_available,
        })
    }

    async fn fetch_results_page(&self, page: usize) -> Result<SaosSearchPage, KioScrapeError> {
        let url = self
            .base_url
            .join(SEARCH_ENDPOINT)
            .map_err(|err| KioScrapeError::UrlJoin {
                path: SEARCH_ENDPOINT.to_string(),
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "saos_search";
        let query = vec![
            ("pageSize", PAGE_SIZE.to_string()),
            ("pageNumber", page.to_string()),
            ("sortingField", "JUDGMENT_DATE".to_string()),
            ("sortingDirection", "DESC".to_string()),
            ("courtType", "NATIONAL_APPEAL_CHAMBER".to_string()),
            ("judgmentDateTo", CUT_OFF_DATE.to_string()),
        ];

        let attempt = {
            let limiter = limiter.clone();
            let client = client.clone();
            let url = url.clone();
            let query = query.clone();
            move || {
                let limiter = limiter.clone();
                let client = client.clone();
                let url = url.clone();
                let query = query.clone();
                async move {
                    limiter.until_ready().await;
                    let response = client
                        .get(url.clone())
                        .query(&query)
                        .header("Accept", "application/json")
                        .send()
                        .await
                        .map_err(|err| KioScrapeError::request(stage, err))?;

                    let status = response.status();
                    if !status.is_success() {
                        return Err(KioScrapeError::HttpStatus {
                            stage,
                            status: status.as_u16(),
                        });
                    }

                    let payload = response
                        .bytes()
                        .await
                        .map_err(|err| KioScrapeError::body(stage, err))?;

                    serde_json::from_slice::<SearchResponse>(&payload)
                        .map_err(|err| KioScrapeError::json(stage, err))
                }
            }
        };

        let response = attempt
            .retry(self.backoff)
            .sleep(sleep)
            .notify(|err: &KioScrapeError, delay: Duration| {
                warn!(
                    silo = SILO_SLUG,
                    stage,
                    page,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying SAOS search request"
                );
            })
            .await?;

        Ok(transform_search_response(response))
    }

    async fn fetch_detail(&self, id: u64) -> Result<SaosDetail, KioScrapeError> {
        let path = format!("{DETAIL_ENDPOINT_PREFIX}{id}");
        let url = self
            .base_url
            .join(&path)
            .map_err(|err| KioScrapeError::UrlJoin {
                path,
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "saos_detail";

        let attempt = {
            let limiter = limiter.clone();
            let client = client.clone();
            let url = url.clone();
            move || {
                let limiter = limiter.clone();
                let client = client.clone();
                let url = url.clone();
                async move {
                    limiter.until_ready().await;
                    let response = client
                        .get(url.clone())
                        .header("Accept", "application/json")
                        .send()
                        .await
                        .map_err(|err| KioScrapeError::request(stage, err))?;

                    let status = response.status();
                    if !status.is_success() {
                        return Err(KioScrapeError::HttpStatus {
                            stage,
                            status: status.as_u16(),
                        });
                    }

                    let payload = response
                        .bytes()
                        .await
                        .map_err(|err| KioScrapeError::body(stage, err))?;

                    serde_json::from_slice::<DetailResponse>(&payload)
                        .map_err(|err| KioScrapeError::json(stage, err))
                }
            }
        };

        let response = attempt
            .retry(self.backoff)
            .sleep(sleep)
            .notify(|err: &KioScrapeError, delay: Duration| {
                warn!(
                    silo = SILO_SLUG,
                    stage,
                    id,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying SAOS detail request"
                );
            })
            .await?;

        extract_detail(response)
    }

    async fn fetch_pdf(&self, pdf_path: &str) -> Result<Bytes, KioScrapeError> {
        let url = self
            .base_url
            .join(pdf_path)
            .map_err(|err| KioScrapeError::UrlJoin {
                path: pdf_path.to_string(),
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "saos_pdf_download";

        let attempt = {
            let limiter = limiter.clone();
            let client = client.clone();
            let url = url.clone();
            move || {
                let limiter = limiter.clone();
                let client = client.clone();
                let url = url.clone();
                async move {
                    limiter.until_ready().await;
                    let response = client
                        .get(url.clone())
                        .send()
                        .await
                        .map_err(|err| KioScrapeError::request(stage, err))?;

                    let status = response.status();
                    if !status.is_success() {
                        return Err(KioScrapeError::HttpStatus {
                            stage,
                            status: status.as_u16(),
                        });
                    }

                    response
                        .bytes()
                        .await
                        .map_err(|err| KioScrapeError::body(stage, err))
                }
            }
        };

        attempt
            .retry(self.backoff)
            .sleep(sleep)
            .notify(|err: &KioScrapeError, delay: Duration| {
                warn!(
                    silo = SILO_SLUG,
                    stage,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying SAOS pdf download"
                );
            })
            .await
    }

    async fn fetch_pdf_len(&self, pdf_path: &str) -> Result<Option<u64>, KioScrapeError> {
        let url = self
            .base_url
            .join(pdf_path)
            .map_err(|err| KioScrapeError::UrlJoin {
                path: pdf_path.to_string(),
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "saos_pdf_head";

        let attempt = {
            let limiter = limiter.clone();
            let client = client.clone();
            let url = url.clone();
            move || {
                let limiter = limiter.clone();
                let client = client.clone();
                let url = url.clone();
                async move {
                    limiter.until_ready().await;
                    let response = client
                        .head(url.clone())
                        .send()
                        .await
                        .map_err(|err| KioScrapeError::request(stage, err))?;

                    let status = response.status();
                    if !status.is_success() {
                        return Err(KioScrapeError::HttpStatus {
                            stage,
                            status: status.as_u16(),
                        });
                    }

                    let len = response
                        .headers()
                        .get(header::CONTENT_LENGTH)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    Ok(len)
                }
            }
        };

        attempt
            .retry(self.backoff)
            .sleep(sleep)
            .notify(|err: &KioScrapeError, delay: Duration| {
                warn!(
                    silo = SILO_SLUG,
                    stage,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying SAOS PDF head request"
                );
            })
            .await
    }

    async fn process_task(
        &self,
        task: &SaosDocumentTask,
        output_dir: &Path,
    ) -> Result<ProcessOutcome, KioScrapeError> {
        let output_path = output_dir.join(format!("{}.pdf", task.doc_id));
        let existing_len = self.local_pdf_len(&output_path, &task.doc_id).await?;

        let detail = self.fetch_detail_with_logging(task).await?;
        let pdf_path =
            build_pdf_path(&detail).map_err(|err| KioScrapeError::parse("pdf_path", err))?;

        if let Some(outcome) = self
            .check_cached_pdf(&pdf_path, &output_path, &task.doc_id, existing_len)
            .await?
        {
            return Ok(outcome);
        }

        self.download_pdf_to_disk(&pdf_path, &output_path, &task.doc_id)
            .await
    }

    async fn local_pdf_len(
        &self,
        output_path: &Path,
        doc_id: &str,
    ) -> Result<Option<u64>, KioScrapeError> {
        if !fs::try_exists(output_path).await? {
            return Ok(None);
        }

        match fs::metadata(output_path).await {
            Ok(meta) => Ok(Some(meta.len())),
            Err(err) => {
                warn!(
                    silo = SILO_SLUG,
                    stage = "local_metadata_failed",
                    doc_id,
                    path = %output_path.display(),
                    error = %err,
                    "failed to read local PDF metadata; re-downloading"
                );
                Ok(None)
            }
        }
    }

    async fn fetch_detail_with_logging(
        &self,
        task: &SaosDocumentTask,
    ) -> Result<SaosDetail, KioScrapeError> {
        info!(
            silo = SILO_SLUG,
            stage = "detail_fetch_start",
            doc_id = task.doc_id,
            "fetching detail JSON"
        );
        let detail = self.fetch_detail(task.id).await?;
        debug!(
            silo = SILO_SLUG,
            stage = "detail_fetched",
            doc_id = task.doc_id,
            "detail response parsed"
        );
        Ok(detail)
    }

    async fn check_cached_pdf(
        &self,
        pdf_path: &str,
        output_path: &Path,
        doc_id: &str,
        existing_len: Option<u64>,
    ) -> Result<Option<ProcessOutcome>, KioScrapeError> {
        let Some(local_len) = existing_len else {
            return Ok(None);
        };

        match self.fetch_pdf_len(pdf_path).await {
            Ok(Some(remote_len)) if remote_len == local_len => {
                info!(
                    silo = SILO_SLUG,
                    stage = "skip_existing",
                    doc_id,
                    path = %output_path.display(),
                    local_len,
                    remote_len,
                    "skipping existing PDF; size matches upstream"
                );
                Ok(Some(ProcessOutcome::SkippedExisting))
            }
            Ok(Some(remote_len)) => {
                warn!(
                    silo = SILO_SLUG,
                    stage = "size_mismatch",
                    doc_id,
                    path = %output_path.display(),
                    local_len,
                    remote_len,
                    "existing PDF size differs from upstream; re-downloading"
                );
                Ok(None)
            }
            Ok(None) => {
                warn!(
                    silo = SILO_SLUG,
                    stage = "size_unknown",
                    doc_id,
                    path = %output_path.display(),
                    local_len,
                    "remote PDF size unavailable; re-downloading"
                );
                Ok(None)
            }
            Err(err) => {
                warn!(
                    silo = SILO_SLUG,
                    stage = "size_check_failed",
                    doc_id,
                    path = %output_path.display(),
                    local_len,
                    error = %err,
                    "failed to inspect remote PDF size; re-downloading"
                );
                Ok(None)
            }
        }
    }

    async fn download_pdf_to_disk(
        &self,
        pdf_path: &str,
        output_path: &Path,
        doc_id: &str,
    ) -> Result<ProcessOutcome, KioScrapeError> {
        info!(
            silo = SILO_SLUG,
            stage = "pdf_fetch_start",
            doc_id,
            path = %pdf_path,
            "downloading judgment PDF"
        );
        let pdf = self.fetch_pdf(pdf_path).await?;
        fs::write(output_path, &pdf).await?;
        info!(
            silo = SILO_SLUG,
            stage = "pdf_stored",
            doc_id,
            bytes = pdf.len(),
            path = %output_path.display(),
            "stored judgment PDF"
        );

        Ok(ProcessOutcome::Downloaded { bytes: pdf.len() })
    }
}

#[derive(Debug)]
struct DiscoveryStats {
    discovered: usize,
    total_available: Option<usize>,
}

struct WorkerStats {
    downloaded: Arc<AtomicUsize>,
    skipped_existing: Arc<AtomicUsize>,
    event_tx: EventSender,
}

impl WorkerStats {
    fn new(
        downloaded: Arc<AtomicUsize>,
        skipped_existing: Arc<AtomicUsize>,
        event_tx: EventSender,
    ) -> Self {
        Self {
            downloaded,
            skipped_existing,
            event_tx,
        }
    }
}

impl Clone for WorkerStats {
    fn clone(&self) -> Self {
        Self {
            downloaded: Arc::clone(&self.downloaded),
            skipped_existing: Arc::clone(&self.skipped_existing),
            event_tx: self.event_tx.clone(),
        }
    }
}

#[derive(Debug)]
enum ProcessOutcome {
    Downloaded { bytes: usize },
    SkippedExisting,
}

fn spawn_workers(
    scraper: KioSaosScraper,
    receiver: mpsc::Receiver<SaosDocumentTask>,
    output_dir: PathBuf,
    worker_count: usize,
    stats: WorkerStats,
) -> JoinSet<Result<(), KioScrapeError>> {
    let shared_receiver = Arc::new(Mutex::new(receiver));
    let output_dir = Arc::new(output_dir);

    let mut join_set = JoinSet::new();
    for worker_idx in 0..worker_count {
        let rx = Arc::clone(&shared_receiver);
        let scraper = scraper.clone();
        let output_dir = Arc::clone(&output_dir);
        let stats = stats.clone();

        join_set.spawn(async move { run_worker(worker_idx, rx, scraper, output_dir, stats).await });
    }

    join_set
}

async fn run_worker(
    worker_idx: usize,
    receiver: Arc<Mutex<mpsc::Receiver<SaosDocumentTask>>>,
    scraper: KioSaosScraper,
    output_dir: Arc<PathBuf>,
    stats: WorkerStats,
) -> Result<(), KioScrapeError> {
    loop {
        let Some(task) = receive_task(&receiver).await else {
            debug!(
                silo = SILO_SLUG,
                stage = "worker_shutdown",
                worker = worker_idx,
                "worker terminating (channel closed)"
            );
            break;
        };

        process_worker_task(worker_idx, &scraper, &output_dir, &stats, task).await?;
    }
    Ok(())
}

async fn receive_task(
    receiver: &Arc<Mutex<mpsc::Receiver<SaosDocumentTask>>>,
) -> Option<SaosDocumentTask> {
    let mut guard = receiver.lock().await;
    guard.recv().await
}

async fn process_worker_task(
    worker_idx: usize,
    scraper: &KioSaosScraper,
    output_dir: &Path,
    stats: &WorkerStats,
    task: SaosDocumentTask,
) -> Result<(), KioScrapeError> {
    info!(
        silo = SILO_SLUG,
        stage = "worker_start",
        worker = worker_idx,
        doc_id = %task.doc_id,
        id = task.id,
        "worker picked up document"
    );
    send_event(
        &stats.event_tx,
        KioEvent::WorkerStarted {
            worker: worker_idx,
            doc_id: task.doc_id.clone(),
        },
    )
    .await?;

    match scraper.process_task(&task, output_dir).await {
        Ok(ProcessOutcome::Downloaded { bytes }) => {
            stats.downloaded.fetch_add(1, Ordering::Relaxed);
            debug!(
                silo = SILO_SLUG,
                stage = "worker_done",
                worker = worker_idx,
                doc_id = %task.doc_id,
                bytes,
                "worker finished download"
            );
            send_event(
                &stats.event_tx,
                KioEvent::DownloadCompleted {
                    doc_id: task.doc_id.clone(),
                    bytes,
                },
            )
            .await?;
            Ok(())
        }
        Ok(ProcessOutcome::SkippedExisting) => {
            stats.skipped_existing.fetch_add(1, Ordering::Relaxed);
            send_event(
                &stats.event_tx,
                KioEvent::DownloadSkipped {
                    doc_id: task.doc_id.clone(),
                },
            )
            .await?;
            Ok(())
        }
        Err(err) => {
            warn!(
                silo = SILO_SLUG,
                stage = "worker_error",
                worker = worker_idx,
                doc_id = %task.doc_id,
                error = %err,
                "worker encountered error"
            );
            Err(err)
        }
    }
}

fn to_scraper_join_error(err: tokio::task::JoinError) -> KioScrapeError {
    if err.is_cancelled() {
        KioScrapeError::parse("worker_join", "worker task cancelled")
    } else if err.is_panic() {
        KioScrapeError::parse("worker_join", "worker panicked")
    } else {
        KioScrapeError::parse("worker_join", "worker aborted unexpectedly")
    }
}

async fn fetch_saos_discovery_page(
    scraper: KioSaosScraper,
    page: usize,
) -> Result<(usize, SaosSearchPage), KioScrapeError> {
    let response = scraper.fetch_results_page(page).await?;
    Ok((page, response))
}

fn transform_search_response(response: SearchResponse) -> SaosSearchPage {
    let items = response
        .items
        .into_iter()
        .map(|item| SaosDocumentTask {
            id: item.id,
            doc_id: format!("saos-{}", item.id),
            case_number: item
                .court_cases
                .into_iter()
                .find_map(|case| case.case_number),
            judgment_type: item.judgment_type,
        })
        .collect::<Vec<_>>();

    let total_available = response.info.map(|info| info.total_results);

    SaosSearchPage {
        items,
        total_available,
    }
}

fn extract_detail(response: DetailResponse) -> Result<SaosDetail, KioScrapeError> {
    let judgment_date = response
        .data
        .judgment_date
        .ok_or_else(|| KioScrapeError::parse("detail_missing_date", "missing judgment_date"))?;
    let judgment_id = response
        .data
        .source
        .and_then(|source| source.judgment_id)
        .ok_or_else(|| KioScrapeError::parse("detail_missing_id", "missing source.judgmentId"))?;

    Ok(SaosDetail {
        judgment_id,
        judgment_date,
    })
}

fn build_pdf_path(detail: &SaosDetail) -> Result<String, String> {
    let (year, month, day) = split_date(&detail.judgment_date)?;
    Ok(format!(
        "{FILES_PREFIX}/{}/{}/{}/{}.pdf",
        year, month, day, detail.judgment_id
    ))
}

fn split_date(date: &str) -> Result<(u32, u32, u32), String> {
    let mut parts = date.split('-');
    let year = parts
        .next()
        .ok_or_else(|| format!("date `{date}` missing year"))?
        .parse::<u32>()
        .map_err(|err| format!("failed to parse year in `{date}`: {err}"))?;
    let month = parts
        .next()
        .ok_or_else(|| format!("date `{date}` missing month"))?
        .parse::<u32>()
        .map_err(|err| format!("failed to parse month in `{date}`: {err}"))?;
    let day = parts
        .next()
        .ok_or_else(|| format!("date `{date}` missing day"))?
        .parse::<u32>()
        .map_err(|err| format!("failed to parse day in `{date}`: {err}"))?;

    Ok((year, month, day))
}

async fn ensure_dir(dir: &Path) -> Result<(), KioScrapeError> {
    if fs::try_exists(dir).await? {
        return Ok(());
    }
    fs::create_dir_all(dir).await?;
    Ok(())
}

fn compute_discovery_target(limit: Option<usize>, total_available: Option<usize>) -> usize {
    match (limit, total_available) {
        (Some(l), Some(t)) => l.min(t),
        (Some(l), None) => l,
        (None, Some(t)) => t,
        (None, None) => usize::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::compute_discovery_target;

    #[test]
    fn target_uses_limit_when_only_limit() {
        assert_eq!(compute_discovery_target(Some(500), None), 500);
    }

    #[test]
    fn target_uses_total_when_unbounded() {
        assert_eq!(compute_discovery_target(None, Some(28860)), 28860);
    }

    #[test]
    fn target_uses_min_of_limit_and_total() {
        assert_eq!(compute_discovery_target(Some(250), Some(28860)), 250);
    }

    #[test]
    fn target_defaults_to_max_when_unknown() {
        assert_eq!(compute_discovery_target(None, None), usize::MAX);
    }
}
