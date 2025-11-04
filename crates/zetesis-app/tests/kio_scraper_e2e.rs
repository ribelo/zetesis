//! End-to-end integration test for KIO scraping with BlobStore and manifest.
//!
//! This test verifies the complete flow:
//! 1. Discovery: scraper parses HTML search results
//! 2. Download: scraper fetches PDF content via HTTP
//! 3. Storage: BlobStore receives content and computes CID
//! 4. Manifest: ledger records doc_id → CID mappings
//!
//! Uses wiremock to simulate the KIO UZP HTTP server without external dependencies.

use futures_util::{StreamExt, pin_mut};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use zetesis_app::ingestion::{KioEvent, KioScrapeOptions, KioUzpScraper, ManifestWriter};
use zetesis_app::paths::AppPaths;
use zetesis_app::pipeline::processor::Silo;
use zetesis_app::services::{BlobStore, DurableWrite, FsBlobStore};

/// Create mock HTML for search results page containing one result.
fn mock_search_results_html(doc_id: &str) -> String {
    format!(
        r#"
<!DOCTYPE html>
<html>
<body>
    <input id="resultCounts" value="1, 1" />
    <div class="search-list-item">
        <a class="link-details" href="/Home/Details/{doc_id}">Decision {doc_id}</a>
        <p><label>Sygnatura:</label>KIO {doc_id}/2025</p>
        <p><label>Rodzaj dokumentu:</label>Wyrok</p>
    </div>
</body>
</html>
        "#
    )
}

/// Create mock HTML for detail page.
fn mock_detail_html(doc_id: &str) -> String {
    format!(
        r#"
<!DOCTYPE html>
<html>
<body>
    <h1>Decision {doc_id}</h1>
    <p>This is a sample detail page for document {doc_id}.</p>
</body>
</html>
        "#
    )
}

/// Create minimal valid PDF content (simplified PDF structure).
fn mock_pdf_bytes() -> Vec<u8> {
    // Use actual test fixture PDF
    include_bytes!("fixtures/kio_sample.pdf").to_vec()
}

#[tokio::test]
async fn test_e2e_kio_scraper_with_blobstore_and_manifest() {
    // Setup: mock HTTP server
    let mock_server = MockServer::start().await;
    let doc_id = "123456";
    let doc_id_str = doc_id.to_string();

    // Mock search results endpoint (POST Home/GetResults)
    Mock::given(method("POST"))
        .and(path("/Home/GetResults"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_search_results_html(doc_id)))
        .mount(&mock_server)
        .await;

    // Mock detail page endpoint (GET Home/Details/123456)
    Mock::given(method("GET"))
        .and(path(format!("/Home/Details/{}", doc_id)))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_detail_html(doc_id)))
        .mount(&mock_server)
        .await;

    // Mock PDF download endpoint (GET Home/PdfContent/123456?Kind=KIO)
    Mock::given(method("GET"))
        .and(path(format!("/Home/PdfContent/{}", doc_id)))
        .and(query_param("Kind", "KIO"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mock_pdf_bytes()))
        .mount(&mock_server)
        .await;

    // Setup: BlobStore (filesystem with temp directory)
    let blob_temp_dir = TempDir::new().expect("failed to create blob temp dir");
    let app_paths = AppPaths::new(blob_temp_dir.path()).expect("failed to create AppPaths");
    let blob_store: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::builder()
            .paths(app_paths)
            .durability(DurableWrite::FileOnly)
            .build(),
    );

    // Setup: ManifestWriter
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let manifest_path = temp_dir.path().join("manifest.ndjson");
    let mut manifest_writer = ManifestWriter::open(&manifest_path)
        .await
        .expect("failed to open manifest writer");

    // Setup: scraper with mock server URL
    let scraper = KioUzpScraper::new(&mock_server.uri()).expect("failed to create scraper");

    // Setup: scrape options (limit to 1 document)
    let opts = KioScrapeOptions {
        blob_store: blob_store.clone(),
        limit: Some(1),
        worker_count: std::num::NonZeroUsize::new(1).unwrap(),
        channel_capacity: 16,
        discovery_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
    };

    // Execute: run scraper and collect events
    let stream = scraper.scrape_stream(opts);
    pin_mut!(stream);
    let mut events = Vec::new();
    let mut stored_cid: Option<String> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                // Record manifest entry on BlobStored event
                if let KioEvent::BlobStored {
                    ref doc_id,
                    ref cid,
                    bytes,
                    existed: _,
                } = event
                {
                    let entry = zetesis_app::ingestion::ManifestEntry::new(
                        doc_id.clone(),
                        cid.clone(),
                        bytes,
                    );
                    manifest_writer
                        .write(&entry)
                        .await
                        .expect("failed to write manifest");
                    stored_cid = Some(cid.clone());
                }
                events.push(event);
            }
            Err(err) => panic!("scrape error: {}", err),
        }
    }

    // Close manifest
    manifest_writer
        .close()
        .await
        .expect("failed to close manifest");

    // Verify: events received
    assert!(!events.is_empty(), "expected at least one event");

    let has_discovery_started = events
        .iter()
        .any(|e| matches!(e, KioEvent::DiscoveryStarted { .. }));
    assert!(has_discovery_started, "expected DiscoveryStarted event");

    let has_discovered = events
        .iter()
        .any(|e| matches!(e, KioEvent::Discovered { .. }));
    assert!(
        has_discovered,
        "expected Discovered event for doc {}",
        doc_id
    );

    let has_blob_stored = events
        .iter()
        .any(|e| matches!(e, KioEvent::BlobStored { .. }));
    assert!(has_blob_stored, "expected BlobStored event");

    let has_completed = events
        .iter()
        .any(|e| matches!(e, KioEvent::Completed { .. }));
    assert!(has_completed, "expected Completed event");

    // Verify: BlobStore contains PDF
    let cid = stored_cid.expect("expected stored_cid to be set");
    let meta = blob_store
        .head(Silo::Kio, &cid)
        .await
        .expect("failed to check blob existence");
    assert!(
        meta.is_some(),
        "expected blob {} to exist in BlobStore",
        cid
    );

    // Verify: manifest file written
    assert!(
        manifest_path.exists(),
        "expected manifest file to exist at {:?}",
        manifest_path
    );

    // Verify: manifest content
    let mut manifest_file = tokio::fs::File::open(&manifest_path)
        .await
        .expect("failed to open manifest file");
    let mut manifest_content = String::new();
    manifest_file
        .read_to_string(&mut manifest_content)
        .await
        .expect("failed to read manifest file");

    assert!(
        !manifest_content.is_empty(),
        "expected manifest to contain entries"
    );

    let lines: Vec<&str> = manifest_content.lines().collect();
    assert_eq!(lines.len(), 1, "expected exactly 1 manifest entry");

    let entry: zetesis_app::ingestion::ManifestEntry =
        serde_json::from_str(lines[0]).expect("failed to parse manifest entry");
    assert_eq!(
        entry.doc_id, doc_id_str,
        "expected manifest doc_id to match"
    );
    assert_eq!(entry.cid, cid, "expected manifest cid to match");
    assert!(entry.size_bytes > 0, "expected manifest size_bytes > 0");
    assert!(entry.timestamp > 0, "expected manifest timestamp > 0");
}

#[tokio::test]
async fn test_e2e_kio_scraper_deduplication() {
    // This test verifies that scraping the same document twice results in:
    // 1. BlobStore reporting existed=true on second scrape
    // 2. Only one blob stored in BlobStore (content-addressable deduplication)

    let mock_server = MockServer::start().await;
    let doc_id = "789012";

    // Mock endpoints
    Mock::given(method("POST"))
        .and(path("/Home/GetResults"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_search_results_html(doc_id)))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/Home/Details/{}", doc_id)))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_detail_html(doc_id)))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/Home/PdfContent/{}", doc_id)))
        .and(query_param("Kind", "KIO"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mock_pdf_bytes()))
        .mount(&mock_server)
        .await;

    // Setup: shared BlobStore
    let blob_temp_dir = TempDir::new().expect("failed to create blob temp dir");
    let app_paths = AppPaths::new(blob_temp_dir.path()).expect("failed to create AppPaths");
    let blob_store: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::builder()
            .paths(app_paths)
            .durability(DurableWrite::FileOnly)
            .build(),
    );

    // First scrape
    let scraper = KioUzpScraper::new(&mock_server.uri()).expect("failed to create scraper");
    let opts = KioScrapeOptions {
        blob_store: blob_store.clone(),
        limit: Some(1),
        worker_count: std::num::NonZeroUsize::new(1).unwrap(),
        channel_capacity: 16,
        discovery_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
    };

    let stream = scraper.scrape_stream(opts);
    pin_mut!(stream);
    let mut first_cid: Option<String> = None;
    let mut first_existed = false;

    while let Some(result) = stream.next().await {
        if let Ok(KioEvent::BlobStored { cid, existed, .. }) = result {
            first_cid = Some(cid);
            first_existed = existed;
        }
    }

    let first_cid = first_cid.expect("expected first scrape to store blob");
    assert!(!first_existed, "expected first scrape existed=false");

    // Second scrape (same document)
    let scraper2 = KioUzpScraper::new(&mock_server.uri()).expect("failed to create scraper");
    let opts2 = KioScrapeOptions {
        blob_store: blob_store.clone(),
        limit: Some(1),
        worker_count: std::num::NonZeroUsize::new(1).unwrap(),
        channel_capacity: 16,
        discovery_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
    };

    let stream2 = scraper2.scrape_stream(opts2);
    pin_mut!(stream2);
    let mut second_cid: Option<String> = None;
    let mut second_existed = false;

    while let Some(result) = stream2.next().await {
        match result {
            Ok(event) => {
                if let KioEvent::BlobStored { cid, existed, .. } = event {
                    second_cid = Some(cid);
                    second_existed = existed;
                }
            }
            Err(err) => panic!("second scrape error: {}", err),
        }
    }

    let second_cid = second_cid.expect("expected second scrape to produce BlobStored event");
    assert!(second_existed, "expected second scrape existed=true");
    assert_eq!(
        first_cid, second_cid,
        "expected same CID for identical content"
    );

    // Verify: BlobStore contains only one blob
    // (InMemBlobStore doesn't expose count, but we verified existed=true on second put)
}
