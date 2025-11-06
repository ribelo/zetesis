use std::{
    fmt,
    num::NonZeroU32,
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
use scraper::{Html, Selector};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::sleep,
};
use tracing::{debug, info, warn};

use crate::ingestion::kio_types::{
    KioDocumentMetadata, KioEvent, KioScrapeError, KioScrapeOptions, KioScraperSummary,
};

const SILO_SLUG: &str = "kio-uzp";
const SEARCH_ENDPOINT: &str = "Home/GetResults";
const KIND_PARAM: &str = "KIO";
const DETAILS_PATH_PREFIX: &str = "Home/Details/";
const PDF_PATH_PREFIX: &str = "Home/PdfContent/";
const DEFAULT_RESULT_PER_PAGE: usize = 10;

type GenericRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[derive(Debug, Clone)]
pub struct KioUzpScraper {
    base_url: Url,
    http: Client,
    rate_limiter: Arc<GenericRateLimiter>,
    backoff: ExponentialBuilder,
}

#[derive(Debug, Clone)]
struct KioDocumentTask {
    doc_id: String,
    detail_path: String,
    pdf_path: String,
    sygnatura: Option<String>,
    decision_type: Option<String>,
}

#[derive(Debug)]
struct KioSearchPage {
    items: Vec<KioDocumentTask>,
    total_available: Option<usize>,
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

impl KioUzpScraper {
    pub fn new(base_url: &str) -> Result<Self, KioScrapeError> {
        let parsed = Url::parse(base_url)
            .map_err(|_| KioScrapeError::InvalidBaseUrl(base_url.to_string()))?;

        let http = Client::builder()
            .cookie_store(true)
            .timeout(Duration::from_secs(30))
            .user_agent("zetesis-kio-scraper/0.1")
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
        send_event(&event_tx, KioEvent::DiscoveryStarted { limit: opts.limit }).await?;

        let worker_count = opts.worker_count.get();
        let channel_capacity = opts.channel_capacity.max(1);
        let mut discovery_concurrency = opts.discovery_concurrency.get();
        discovery_concurrency = discovery_concurrency.min(worker_count);
        discovery_concurrency = discovery_concurrency.min(channel_capacity);
        let (sender, receiver) = mpsc::channel(channel_capacity);

        let stored = Arc::new(AtomicUsize::new(0));
        let skipped = Arc::new(AtomicUsize::new(0));

        let worker_stats = WorkerStats::new(stored.clone(), skipped.clone(), event_tx.clone());

        assert!(
            discovery_concurrency > 0,
            "discovery concurrency must be positive"
        );

        let mut join_set = spawn_workers(
            self.clone(),
            receiver,
            opts.blob_store.clone(),
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
            stored: stored.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
        };

        send_event(&event_tx, KioEvent::Completed { summary }).await?;
        Ok(())
    }

    async fn run_discovery(
        &self,
        sender: &mpsc::Sender<KioDocumentTask>,
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
        let mut next_page = 1usize;
        let mut should_stop = false;

        let mut join_set = JoinSet::new();
        let mut inflight = 0usize;

        // JoinSet keeps bounded, cancellable fan-out: async fetches are long-lived IO tasks,
        // and we want the ability to stop spawning once the discovery target or empty page is hit.
        // futures-concurrency's collectors would buffer completions without letting us interleave
        // "spawn next" decisions, so JoinSet stays here for clarity and cancellation semantics.
        for _ in 0..discovery_concurrency {
            join_set.spawn(fetch_discovery_page(self.clone(), next_page));
            next_page += 1;
            inflight += 1;
        }

        while let Some(result) = join_set.join_next().await {
            inflight -= 1;
            let (page, parsed) = match result {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => return Err(err),
                Err(join_err) => return Err(to_scraper_join_error(join_err)),
            };

            if !should_stop {
                if let Some(total) = parsed.total_available {
                    total_available = Some(total);
                    target = compute_discovery_target(limit, total_available);
                }

                if parsed.items.is_empty() {
                    should_stop = true;
                } else {
                    for task in parsed.items {
                        if discovered >= target {
                            should_stop = true;
                            break;
                        }

                        let metadata = KioDocumentMetadata {
                            doc_id: task.doc_id.clone(),
                            sygnatura: task.sygnatura.clone(),
                            decision_type: task.decision_type.clone(),
                        };

                        send_event(
                            event_tx,
                            KioEvent::Discovered {
                                ordinal: discovered + 1,
                                page,
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
                join_set.spawn(fetch_discovery_page(self.clone(), next_page));
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

    async fn fetch_results_page(
        &self,
        form: Vec<(String, String)>,
        page: usize,
    ) -> Result<String, KioScrapeError> {
        let url = self
            .base_url
            .join(SEARCH_ENDPOINT)
            .map_err(|err| KioScrapeError::UrlJoin {
                path: SEARCH_ENDPOINT.to_string(),
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "search_page";
        let attempt = {
            let limiter = limiter.clone();
            let client = client.clone();
            let url = url.clone();
            let form = form.clone();
            move || {
                let limiter = limiter.clone();
                let client = client.clone();
                let url = url.clone();
                let form = form.clone();
                async move {
                    limiter.until_ready().await;
                    let response = client
                        .post(url.clone())
                        .form(&form)
                        .header("Referer", url.as_str())
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
                        .text()
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
                    page,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "retrying search page request"
                );
            })
            .await
    }

    async fn fetch_detail_html(&self, detail_path: &str) -> Result<String, KioScrapeError> {
        let url = self
            .base_url
            .join(detail_path)
            .map_err(|err| KioScrapeError::UrlJoin {
                path: detail_path.to_string(),
                source: Arc::new(err),
            })?;

        let limiter = self.rate_limiter.clone();
        let client = self.http.clone();
        let stage = "detail_page";

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
                        .text()
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
                    "retrying detail request"
                );
            })
            .await
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
        let stage = "pdf_download";

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
                    "retrying PDF download"
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
        let stage = "pdf_head";

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
                    "retrying PDF head request"
                );
            })
            .await
    }

    async fn process_task(
        &self,
        task: &KioDocumentTask,
        blob_store: &Arc<dyn crate::services::BlobStore>,
    ) -> Result<ProcessOutcome, KioScrapeError> {
        use crate::pipeline::processor::Silo;
        use futures::stream;

        info!(
            silo = SILO_SLUG,
            stage = "detail_fetch_start",
            doc_id = task.doc_id,
            detail = task.detail_path,
            "fetching detail view"
        );
        let detail_html = self.fetch_detail_html(&task.detail_path).await?;
        debug!(
            silo = SILO_SLUG,
            stage = "detail_fetched",
            doc_id = task.doc_id,
            detail_len = detail_html.len(),
            "detail view fetched"
        );

        info!(
            silo = SILO_SLUG,
            stage = "pdf_fetch_start",
            doc_id = task.doc_id,
            "downloading judgment PDF"
        );
        let pdf_bytes = self.fetch_pdf(&task.pdf_path).await?;
        let byte_count = pdf_bytes.len();

        info!(
            silo = SILO_SLUG,
            stage = "blob_store_start",
            doc_id = task.doc_id,
            bytes = byte_count,
            "storing blob"
        );

        let byte_stream: crate::services::ByteStream =
            Box::pin(stream::once(async move { Ok(pdf_bytes) }));

        let put_result = blob_store.put(Silo::Kio, byte_stream).await.map_err(|e| {
            KioScrapeError::parse("blob_put", format!("BlobStore put failed: {}", e))
        })?;

        info!(
            silo = SILO_SLUG,
            stage = "blob_stored",
            doc_id = task.doc_id,
            cid = %put_result.cid,
            bytes = put_result.size_bytes,
            existed = put_result.existed,
            "stored judgment PDF"
        );

        Ok(ProcessOutcome::Stored {
            cid: put_result.cid,
            bytes: byte_count,
            existed: put_result.existed,
        })
    }
}

#[derive(Debug)]
struct DiscoveryStats {
    discovered: usize,
    total_available: Option<usize>,
}

struct WorkerStats {
    stored: Arc<AtomicUsize>,
    skipped: Arc<AtomicUsize>,
    event_tx: EventSender,
}

impl WorkerStats {
    fn new(stored: Arc<AtomicUsize>, skipped: Arc<AtomicUsize>, event_tx: EventSender) -> Self {
        Self {
            stored,
            skipped,
            event_tx,
        }
    }
}

impl Clone for WorkerStats {
    fn clone(&self) -> Self {
        Self {
            stored: Arc::clone(&self.stored),
            skipped: Arc::clone(&self.skipped),
            event_tx: self.event_tx.clone(),
        }
    }
}

#[derive(Debug)]
enum ProcessOutcome {
    Stored {
        cid: String,
        bytes: usize,
        existed: bool,
    },
    #[allow(dead_code)]
    Skipped {
        cid: String,
    },
}

fn spawn_workers(
    scraper: KioUzpScraper,
    receiver: mpsc::Receiver<KioDocumentTask>,
    blob_store: Arc<dyn crate::services::BlobStore>,
    worker_count: usize,
    stats: WorkerStats,
) -> JoinSet<Result<(), KioScrapeError>> {
    let shared_receiver = Arc::new(Mutex::new(receiver));

    let mut join_set = JoinSet::new();
    for worker_idx in 0..worker_count {
        let rx = Arc::clone(&shared_receiver);
        let scraper = scraper.clone();
        let blob_store = Arc::clone(&blob_store);
        let stats = stats.clone();

        join_set.spawn(async move { run_worker(worker_idx, rx, scraper, blob_store, stats).await });
    }

    join_set
}

async fn run_worker(
    worker_idx: usize,
    receiver: Arc<Mutex<mpsc::Receiver<KioDocumentTask>>>,
    scraper: KioUzpScraper,
    blob_store: Arc<dyn crate::services::BlobStore>,
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

        process_worker_task(worker_idx, &scraper, &blob_store, &stats, task).await?;
    }
    Ok(())
}

async fn receive_task(
    receiver: &Arc<Mutex<mpsc::Receiver<KioDocumentTask>>>,
) -> Option<KioDocumentTask> {
    let mut guard = receiver.lock().await;
    guard.recv().await
}

async fn process_worker_task(
    worker_idx: usize,
    scraper: &KioUzpScraper,
    blob_store: &Arc<dyn crate::services::BlobStore>,
    stats: &WorkerStats,
    task: KioDocumentTask,
) -> Result<(), KioScrapeError> {
    let doc_id = task.doc_id.clone();
    info!(
        silo = SILO_SLUG,
        stage = "worker_start",
        worker = worker_idx,
        doc_id = %doc_id,
        "worker picked up document"
    );
    send_event(
        &stats.event_tx,
        KioEvent::WorkerStarted {
            worker: worker_idx,
            doc_id: doc_id.clone(),
        },
    )
    .await?;

    match scraper.process_task(&task, blob_store).await {
        Ok(ProcessOutcome::Stored {
            cid,
            bytes,
            existed,
        }) => {
            if !existed {
                stats.stored.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.skipped.fetch_add(1, Ordering::Relaxed);
            }
            debug!(
                silo = SILO_SLUG,
                stage = "worker_done",
                worker = worker_idx,
                upstream_id = %doc_id,
                cid = %cid,
                bytes,
                existed,
                "worker finished blob storage"
            );
            send_event(
                &stats.event_tx,
                KioEvent::BlobStored {
                    cid: cid.clone(),
                    upstream_id: doc_id.clone(),
                    bytes,
                    existed,
                },
            )
            .await?;
            Ok(())
        }
        Ok(ProcessOutcome::Skipped { cid }) => {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
            send_event(&stats.event_tx, KioEvent::BlobSkipped {
                cid,
                upstream_id: doc_id,
            }).await?;
            Ok(())
        }
        Err(err) => {
            warn!(
                silo = SILO_SLUG,
                stage = "worker_error",
                worker = worker_idx,
                doc_id = %doc_id,
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

fn build_search_form(page: usize, count_stats: bool) -> Vec<(String, String)> {
    vec![
        ("Phrase".to_string(), "".to_string()),
        ("Fle".to_string(), "1".to_string()),
        ("SCnt".to_string(), "1".to_string()),
        ("Kind".to_string(), KIND_PARAM.to_string()),
        ("Srt".to_string(), "date_desc".to_string()),
        ("Pg".to_string(), page.to_string()),
        (
            "CountStats".to_string(),
            if count_stats { "True" } else { "False" }.to_string(),
        ),
        (
            "ResultPerPage".to_string(),
            DEFAULT_RESULT_PER_PAGE.to_string(),
        ),
    ]
}

fn parse_results_page(html: &str) -> Result<KioSearchPage, KioScrapeError> {
    let document = Html::parse_document(html);
    let selectors = KioResultsSelectors::new()?;

    let mut items = Vec::new();
    for element in document.select(&selectors.item) {
        items.push(parse_result_item(&selectors, element)?);
    }

    let total_available = selectors.extract_total(&document);

    Ok(KioSearchPage {
        items,
        total_available,
    })
}

struct KioResultsSelectors {
    item: Selector,
    detail: Selector,
    paragraph: Selector,
    label: Selector,
    counts: Selector,
}

impl KioResultsSelectors {
    fn new() -> Result<Self, KioScrapeError> {
        Ok(Self {
            item: Selector::parse("div.search-list-item")
                .map_err(|err| parse_err("discover", err))?,
            detail: Selector::parse("a.link-details").map_err(|err| parse_err("discover", err))?,
            paragraph: Selector::parse("p").map_err(|err| parse_err("discover", err))?,
            label: Selector::parse("label").map_err(|err| parse_err("discover", err))?,
            counts: Selector::parse("input#resultCounts")
                .map_err(|err| parse_err("discover", err))?,
        })
    }

    fn extract_total(&self, document: &Html) -> Option<usize> {
        document
            .select(&self.counts)
            .next()
            .and_then(|node| node.value().attr("value"))
            .and_then(|value| {
                value
                    .split(',')
                    .filter_map(|s| s.trim().parse::<usize>().ok())
                    .nth(1)
            })
    }
}

fn parse_result_item(
    selectors: &KioResultsSelectors,
    element: scraper::ElementRef<'_>,
) -> Result<KioDocumentTask, KioScrapeError> {
    let detail = element
        .select(&selectors.detail)
        .next()
        .and_then(|n| n.value().attr("href"))
        .ok_or_else(|| KioScrapeError::parse("discover", "result missing detail link"))?;

    let doc_id = extract_doc_id(detail)?;
    let pdf_path = format!("{PDF_PATH_PREFIX}{doc_id}?Kind={KIND_PARAM}");

    let mut sygnatura = None;
    let mut decision_type = None;

    for paragraph in element.select(&selectors.paragraph) {
        if let Some(label) = paragraph.select(&selectors.label).next() {
            let label_text = text_content(&label);
            let paragraph_text = text_content(&paragraph);
            let value = paragraph_text
                .trim_start_matches(&label_text)
                .trim_matches(|c: char| c == ':' || c.is_whitespace())
                .trim()
                .to_string();

            if label_text.contains("Sygnatura") {
                sygnatura = Some(value);
            } else if label_text.contains("Rodzaj dokumentu") {
                decision_type = Some(value);
            }
        }
    }

    let detail_path = format!("{DETAILS_PATH_PREFIX}{doc_id}");

    Ok(KioDocumentTask {
        doc_id,
        detail_path,
        pdf_path,
        sygnatura,
        decision_type,
    })
}

fn extract_doc_id(detail_href: &str) -> Result<String, KioScrapeError> {
    detail_href
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .map(|id| id.to_string())
        .ok_or_else(|| {
            KioScrapeError::parse(
                "discover",
                format!("detail link `{detail_href}` missing identifier"),
            )
        })
}

fn text_content(element: &scraper::ElementRef<'_>) -> String {
    element
        .text()
        .collect::<Vec<_>>()
        .join("")
        .trim()
        .to_string()
}

async fn fetch_discovery_page(
    scraper: KioUzpScraper,
    page: usize,
) -> Result<(usize, KioSearchPage), KioScrapeError> {
    let form = build_search_form(page, page == 1);
    let html = scraper.fetch_results_page(form, page).await?;
    let parsed = parse_results_page(&html)?;
    Ok((page, parsed))
}

fn parse_err(stage: &'static str, err: impl fmt::Display) -> KioScrapeError {
    KioScrapeError::parse(stage, err.to_string())
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
    fn target_uses_limit_only_when_total_missing() {
        assert_eq!(compute_discovery_target(Some(50), None), 50);
    }

    #[test]
    fn target_uses_total_when_unbounded() {
        assert_eq!(compute_discovery_target(None, Some(28860)), 28860);
    }

    #[test]
    fn target_uses_min_of_limit_and_total() {
        assert_eq!(compute_discovery_target(Some(100), Some(75)), 75);
    }

    #[test]
    fn target_defaults_to_max_when_unknown() {
        assert_eq!(compute_discovery_target(None, None), usize::MAX);
    }
}
