> AI: Do not change any IDs (T-*). Only edit bullets under the item.

# TODO

Priority legend: P0 = immediate; P1 = next; P2 = later
Status: [ ] open · [~] in progress · [x] done

## WP-SPEC — Specification & Documentation

### Sprint 2025-W45 (2025-11-03 → 2025-11-14)

- [x] T-SPEC-20251104-01 — Derive SPEC from current code; add CLI, OCR, Jobs, Identity, Index details (P0)
  - Covers: [REQ-OBS-001](SPEC.md#req-obs-001), [REQ-INDEX-001](SPEC.md#req-index-001), [REQ-CLI-001](SPEC.md#req-cli-001)

- [ ] T-SPEC-20251108-01 — Add SPEC verification checklist to CI (clippy fmt test invoke, fail on drift) (P1)
  - Covers: [REQ-OBS-001](SPEC.md#req-obs-001)

## WP-STORAGE — Content-Addressed Storage

### Sprint 2025-W44 (2025-10-28 → 2025-11-08)

- [x] T-STORAGE-20251104-01 — Define `BlobStore` trait (`put`/`get`/`head`/`delete`, CID=idempotent, streaming) + property tests (P0)
  - Covers: [REQ-STORAGE-002](SPEC.md#req-storage-002), [REQ-ID-001](SPEC.md#req-id-001)

- [x] T-STORAGE-20251104-02 — Implement `FsBlobStore` over `AppPaths::blob_path` with checksum-on-write and size verification (P0)
  - Covers: [REQ-STORAGE-002](SPEC.md#req-storage-002)

- [x] T-STORAGE-20251104-03 — Implement `S3BlobStore` using an S3 library (for Hetzner) behind feature flag + config stubs (P1)
  - Covers: [REQ-STORAGE-002](SPEC.md#req-storage-002)

- [x] T-STORAGE-20251105-01 — Migrate KIO/SAOS download paths to `BlobStore` API; deprecate ad-hoc caches; E2E move test (P1)
  - Covers: [REQ-STORAGE-002](SPEC.md#req-storage-002)

## WP-CLI — Command Surface

### Sprint 2025-W45 (2025-11-03 → 2025-11-14)

- [x] T-CLI-20251104-01 — Remove dead CLI (`ocr`, `segment`) and unreachable wiring; keep services internal; update SPEC + help output tests (P0)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

- [x] T-CLI-20251104-02 — Validate index names and file size bounds at parse time; add negative tests (P1)
  - Covers: [REQ-CLI-002](SPEC.md#req-cli-002)

- [x] T-CLI-20251104-03 — Add examples and `--help` coverage for `search keyword|vector` including filters and fields (P2)
  - Covers: [REQ-CLI-003](SPEC.md#req-cli-003)

- [x] T-CLI-20251107-01 — Add `--gen-mode sync|batch` and `--gen-model` to `ingest`; show immediate deprecation warning when `--batch` is used and map it to `--gen-mode batch` (P0)
  - Covers: [REQ-CLI-006](SPEC.md#req-cli-006)

## WP-SERVER — HTTP Surface

### Sprint 2025-W44 (2025-10-28 → 2025-11-08)

- [x] T-SERVER-20251104-01 — Implement Axum POC with `/healthz` + graceful shutdown (P0)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

- [x] T-SERVER-20251106-01 — Add `/v1/search/keyword` and `/v1/search/vector` endpoints mirroring CLI options; reuse index/search code paths (P1)
  - Covers: [REQ-INDEX-005](SPEC.md#req-index-005)

- [x] T-SERVER-20251104-02 — Logging, rate limits, and error mapping to structured JSON (P1)
  - Covers: [REQ-OBS-002](SPEC.md#req-obs-002), [REQ-OBS-001](SPEC.md#req-obs-001)

- [x] T-CONFIG-20251107-01 — Config load from env/files; XDG-aware paths; CORS off by default (P2)
  - Covers: [REQ-CONFIG-001](SPEC.md#req-config-001), [REQ-CONFIG-002](SPEC.md#req-config-002)

- [x] T-SERVER-20251104-03 — Typeahead-optimized keyword endpoint: small fields, limit ≤ 10, p95 ≤ 100 ms; per-IP rate limit and 2s LRU cache (P0)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

## WP-UI — Dioxus 0.7.1 (SSR + Islands)

### Sprint 2025-W45 (2025-11-08 → 2025-11-21)

- [ ] T-UI-20251108-01 — Minimal SSR route `/ui/hello` rendered by Dioxus 0.7.1; CI check for gzipped bundle ≤ 150 KiB (P0)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-02 — Set up workspace structure with separate crates for UI/server/shared; configure Axum 0.8 compatibility (P0)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-03 — Create basic SSR component structure with `#[component]` macro and `rsx!` macro usage (P0)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-04 — Implement Axum route handler that creates VirtualDom, rebuilds in-place, and renders to HTML string (P0)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-05 — Add bundle size CI check using `cargo bloat` or similar to enforce ≤ 150 KiB gzipped limit (P0)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-06 — Wire Dioxus SSR into Axum under `/ui/*`; keep `/v1/*` JSON APIs separate (P0)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-07 — Create dedicated `/ui` router module with proper route nesting and middleware isolation (P0)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-08 — Implement route serving function that wraps Dioxus SSR rendering with proper error handling (P0)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-09 — Add CORS configuration for `/ui/*` routes if different from `/v1/*` API routes (P0)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-10 — Create integration tests verifying route separation and proper HTML/JSON content-type headers (P1)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-11 — Implement search page: SSR shell + hydrated results island with typeahead (debounce 200ms, min 2 chars, limit 10, cancellable) (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-12 — Create search page SSR component with static shell (header, search input, results container) (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-13 — Implement typeahead island component with client-side hydration using `dioxus-web` (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-14 — Add debounced input handling (200ms) with proper cleanup on unmount (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-15 — Implement search API client with cancellation support using AbortController pattern (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-16 — Add keyboard navigation support (arrow keys, enter, escape) for search results (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-17 — Create loading states and error boundary handling for failed searches (P1)
  - Covers: [REQ-UI-004](SPEC.md#req-ui-004)

- [ ] T-UI-20251108-18 — Add "Refine with semantics" action to run hybrid once (RRF, bounded k), swap results (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-19 — Add hybrid search toggle button with proper ARIA labels and accessibility (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-20 — Implement client-side hybrid search API call using existing `/v1/search/hybrid` endpoint (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-21 — Create results transition animation when switching between keyword and hybrid results (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-22 — Add visual indicator showing when hybrid search is active vs keyword-only (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-23 — Implement proper error handling for hybrid search failures with fallback to keyword (P1)
  - Covers: [REQ-UI-005](SPEC.md#req-ui-005)

- [ ] T-UI-20251108-24 — Admin status page: SSR table (index stats, job counts); optional tiny hydrated badge for live counters (P1)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-25 — Create admin dashboard SSR layout with navigation and status table structure (P1)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-26 — Implement static table rendering for index statistics (document counts, sizes) (P1)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-27 — Add job counts table with different states (pending, running, completed, failed) (P1)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-28 — Create optional live counter island using WebSocket or Server-Sent Events for real-time updates (P2)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-29 — Add proper authentication checks and role-based access control for admin routes (P2)
  - Covers: [REQ-AUTH-004](SPEC.md#req-auth-004)

- [ ] T-UI-20251108-30 — Implement responsive design for mobile admin interface (P2)
  - Covers: [REQ-UI-002](SPEC.md#req-ui-002)

- [ ] T-UI-20251108-31 — SSG build for static pages (landing/help) and static assets pipeline (public/ + caching) (P2)
  - Covers: [REQ-UI-003](SPEC.md#req-ui-003), [REQ-UI-006](SPEC.md#req-ui-006)

- [ ] T-UI-20251108-32 — Set up Dioxus SSG build process for static page generation at build time (P2)
  - Covers: [REQ-UI-003](SPEC.md#req-ui-003)

- [ ] T-UI-20251108-33 — Create landing page component with marketing content and feature highlights (P2)
  - Covers: [REQ-UI-003](SPEC.md#req-ui-003)

- [ ] T-UI-20251108-34 — Implement help/documentation pages with searchable content and navigation (P2)
  - Covers: [REQ-UI-003](SPEC.md#req-ui-003)

- [ ] T-UI-20251108-35 — Configure static asset pipeline for CSS, JS, images with proper caching headers (P2)
  - Covers: [REQ-UI-006](SPEC.md#req-ui-006)

- [ ] T-UI-20251108-36 — Add asset fingerprinting for cache busting on deployment (P2)
  - Covers: [REQ-UI-006](SPEC.md#req-ui-006)

- [ ] T-UI-20251108-37 — Implement proper 404 and error pages for static content (P2)
  - Covers: [REQ-UI-003](SPEC.md#req-ui-003)

- [ ] T-UI-20251108-38 — Set up development environment with hot reload for UI development (P1)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-39 — Set up Dioxus first-party primitives component library (Button, Dialog, Input, etc.) (P1)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-40 — Implement proper error boundaries and fallback UI for SSR failures (P1)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-41 — Configure native Tailwind CSS support via dx-cli 0.7 with proper SSR integration (P1)
  - Covers: [REQ-UI-001](SPEC.md#req-ui-001)

- [ ] T-UI-20251108-42 — Create end-to-end tests for critical user flows using headless browser (P2)
  - Covers: (none) — Test-only task

## WP-AUTH — OAuth2/OIDC Integration

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-AUTH-20251108-01 — Document oauth2-proxy setup (Hetzner), header trust model, and network placement (P0)
  - Covers: [REQ-AUTH-001](SPEC.md#req-auth-001)

- [ ] T-AUTH-20251108-02 — Optional JWT verification middleware (JWKS cache, aud/iss checks, clock skew) behind feature flag (P1)
  - Covers: [REQ-AUTH-002](SPEC.md#req-auth-002)

- [ ] T-AUTH-20251108-03 — API key path for programmatic clients (scoped routes, disabled by default) (P1)
  - Covers: [REQ-AUTH-003](SPEC.md#req-auth-003)

## WP-INGEST — Ingestion Pipeline

### Sprint 2025-W45 (2025-11-03 → 2025-11-14)

- [x] T-INGEST-20251107-01 — Route KIO/SAOS downloaded PDFs to blob store via `AppPaths::blob_path` (content-addressed) and store CID reference (P0)
  - Covers: [REQ-STORAGE-002](SPEC.md#req-storage-002), [REQ-ID-001](SPEC.md#req-id-001)

- [x] T-INGEST-20251107-02 — Add end-to-end ingest test that indexes a tiny PDF fixture and verifies `doc` + `chunk` shapes (P1)
  - Covers: [REQ-INDEX-008](SPEC.md#req-index-008), [REQ-INDEX-003](SPEC.md#req-index-003)

- [x] T-INGEST-20251107-03 — Refactor ingest pipeline: structured generation uses jobs (sync by default, optional batch); embeddings always run synchronously after generation; remove embedding batch paths (P0)
  - Covers: [REQ-EMBED-002](SPEC.md#req-embed-002), [REQ-JOBS-002](SPEC.md#req-jobs-002), [REQ-PIPELINE-001](SPEC.md#req-pipeline-001)

## WP-INDEX — Milli Configuration

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-INDEX-20251108-01 — Property test for index bootstrap: searchable/filterable/sortable sets equal SPEC; primary key=`id`; Hannoy backend; embedder dims (P1)
  - Covers: [REQ-INDEX-002](SPEC.md#req-index-002), [REQ-INDEX-005](SPEC.md#req-index-005), [REQ-INDEX-006](SPEC.md#req-index-006), [REQ-INDEX-007](SPEC.md#req-index-007)

## WP-GEN-JOBS — Structured Generation Jobs

### Sprint 2025-W45 (2025-11-03 → 2025-11-14)

- [x] T-JOBS-20251107-01 — Add stale generation job reaper and recovery (Pending/Generating with old timestamps → requeue with backoff or mark Failed) (P1)
  - Covers: [REQ-JOBS-002](SPEC.md#req-jobs-002), [REQ-JOBS-003](SPEC.md#req-jobs-003)

- [x] T-JOBS-20251107-02 — `jobs status` reports generation job counts and oldest timestamps by state (P2)
  - Covers: [REQ-JOBS-001](SPEC.md#req-jobs-001)

- [x] T-JOBS-20251107-03 — Idempotency tests for generation job enqueue/update/upsert; bounded retries (P2)
  - Covers: [REQ-JOBS-003](SPEC.md#req-jobs-003)

## WP-OCR — OCR Providers

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-OCR-20251108-01 — Add negative tests for exceeding `MAX_PAGES_PER_DOCUMENT`, zero concurrency, and inflight budget mismatches (P1)
  - Covers: [REQ-OCR-002](SPEC.md#req-ocr-002)

- [ ] T-OCR-20251108-02 — Document token usage logging and provider-specific retries in docs/ocr_costs.md (P2)
  - Covers: [REQ-OCR-004](SPEC.md#req-ocr-004)

## WP-HYBRID — Hybrid Search Fusion

### Sprint 2025-W45 (2025-11-08 → 2025-11-14)

- [x] T-HYBRID-20251108-01 — Confirm Milli lacks built-in RRF; document hybrid support scope (P0)
  - Covers: [REQ-INDEX-005](SPEC.md#req-index-005)

- [x] T-HYBRID-20251108-02 — Implement client-side RRF fusion in CLI/server (run keyword + vector, fuse with k=60, bounded k); tests for tie and overlap (P1)
  - Covers: [REQ-HYBRID-001](SPEC.md#req-hybrid-001)

- [x] T-HYBRID-20251108-03 — Add optional weighted-linear fusion with tunable weights; default to RRF (P2)
  - Covers: [REQ-HYBRID-002](SPEC.md#req-hybrid-002)

## WP-TEXT — Segmentation

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-TEXT-20251108-01 — Add more YAML regression cases for abbreviations and enumerations; include negative cases (P2)
  - Covers: [REQ-TEXT-002](SPEC.md#req-text-002)

## WP-BACKUP — Backup & Recover

### Sprint 2025-W47 (2025-11-22 → 2025-12-05)

- [ ] T-BACKUP-20251108-01 — Add rotation policy and timestamped dirs for `db backup`; verify counts/sizes (P1)
  - Covers: [REQ-BACKUP-001](SPEC.md#req-backup-001)

- [ ] T-BACKUP-20251108-02 — `db recover` integrity check (doc count, vector presence) and dry-run (P1)
  - Covers: [REQ-BACKUP-003](SPEC.md#req-backup-003)

- [ ] T-BACKUP-20251108-03 — Restic setup to Hetzner S3 (credentials, bucket policy), systemd service+timer, retention 7/4/6 (P1)
  - Covers: [REQ-BACKUP-001](SPEC.md#req-backup-001), [REQ-BACKUP-002](SPEC.md#req-backup-002)

- [ ] T-BACKUP-20251108-04 — Monthly restore rehearsal into scratch path + diff stats (P2)
  - Covers: [REQ-BACKUP-003](SPEC.md#req-backup-003)

## WP-DELETE — Hard Deletes

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-DELETE-20251108-01 — Implement atomic delete of doc+chunks+blob with rollback on failure; add dry-run (P0)
  - Covers: [REQ-KIO-008](SPEC.md#req-kio-008)

- [ ] T-DELETE-20251108-02 — Negative tests (missing doc, partial state) and idempotency checks (P1)
  - Covers: [REQ-KIO-008](SPEC.md#req-kio-008)

## WP-DEPLOY — Hetzner Basics

### Sprint 2025-W47 (2025-11-22 → 2025-12-05)

- [ ] T-DEPLOY-20251108-01 — Systemd unit, journald logs, env file; XDG base override docs (P1)
  - Covers: (none) — Deployment task

- [ ] T-DEPLOY-20251108-02 — Deployment doc for Hetzner (firewall, ulimit, backups), manual first (P2)
  - Covers: (none) — Documentation task

## WP-CLI-REDESIGN — CLI Consistency and Namespaces

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [ ] T-CLIRED-20251108-01 — Introduce `--silo <slug>` for dataset-scoped commands; keep positional `INDEX` as a deprecated alias with warning (P0)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

- [ ] T-CLIRED-20251108-02 — Add namespaces: `silo (list|create|stats|backup|recover|purge)`, `source (kio fetch)`, `index (alias for db)`, `sys (serve|init|doctor)` (P0)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

- [ ] T-CLIRED-20251108-03 — Standardize flags across commands (`--limit/--offset/--fields/--pretty/--filter`); enforce bounds (`limit,k ≤ 100`) (P1)
  - Covers: [REQ-CLI-003](SPEC.md#req-cli-003)

- [ ] T-CLIRED-20251108-04 — Implement `sys init` (create dirs, verify RW) and `sys doctor` (API keys, paths, map sizes, rate limits) with actionable output (P1)
  - Covers: [REQ-ID-002](SPEC.md#req-id-002)

- [ ] T-CLIRED-20251108-05 — Update help texts and examples to prefer `--silo`; add deprecation notes for old forms (P1)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

- [ ] T-CLIRED-20251108-06 — Docs: `docs/data-quickstart.md` with end-to-end flow (source→ingest→jobs→search→backup) (P1)
  - Covers: (none) — Documentation task

- [ ] T-CLIRED-20251108-07 — Migration: keep old `db` as alias to `index` and positional `INDEX` for one release; add warnings and a removal date (P2)
  - Covers: [REQ-CLI-001](SPEC.md#req-cli-001)

## WP-DOCS — Spec and Architecture

### Sprint 2025-W45 (2025-11-03 → 2025-11-14)

- [x] T-DOCS-20251107-01 — Update SPEC to reflect generation jobs and synchronous embeddings; add REQ-EMBED-SYNC, REQ-GEN-MODES, REQ-PIPELINE-ORDER, REQ-CLI-INGEST-GEN-MODE (P0)

- [x] T-DOCS-20251107-02 — Update `docs/architecture.md` with the new pipeline order and job scope; add deprecation note for `--batch` (P1)

- [x] T-JOBS-20251107-04 — `jobs` namespace focuses on generation: `jobs (status|gen submit|gen fetch|reap)`; update help and examples (P1)
  - Covers: [REQ-JOBS-001](SPEC.md#req-jobs-001)

## WP-LIBRARY-CLEANUP — Codebase Quality and Refactoring

### Sprint 2025-W46 (2025-11-15 → 2025-11-28)

- [x] T-CLEAN-20251108-01 — Move `DEFAULT_KIO_UZP_URL` and `DEFAULT_KIO_SAOS_URL` from `crates/zetesis-app/src/cli/mod.rs` to domain-specific modules near KIO scrapers (P0)
  - Covers: [REQ-ID-003](SPEC.md#req-id-003)
  - Status: ✅ COMPLETED - URL constants properly located in scraper modules

- [x] T-CLEAN-20251108-02 — Create `db create <index>` command and ensure `ingest` auto-creates indexes when missing (P0)
  - Covers: [REQ-CLI-004](SPEC.md#req-cli-004)
  - Status: ✅ COMPLETED - Both explicit creation and auto-creation working

- [x] T-CLEAN-20251108-03 — Refactor CLI: split `crates/zetesis-app/src/cli/mod.rs` into one file per command (P0)
  - Covers: [REQ-CLI-005](SPEC.md#req-cli-005)
  - Status: ✅ COMPLETED - CLI properly modularized with focused command files

- [x] T-CLEAN-20251108-04 — Extract server code from `crates/zetesis-app/src/server/mod.rs` into new `crates/zetesis-server` crate (P1)
  - Covers: [REQ-SERVER-001](SPEC.md#req-server-001)
  - Status: ✅ COMPLETED - Server router lives in dedicated crate; CLI now calls into it via trait-based search provider injection.

- [x] T-CLEAN-20251108-05 — Add `db delete <index> <doc_id>` command with atomic operation and `--dry-run` (P0)
  - Covers: [REQ-DB-001](SPEC.md#req-db-001)
  - Status: ✅ COMPLETED - Already fully implemented with comprehensive features

- [x] T-CLEAN-20251108-06 — Delete `crates/ai-ox/src/model/gemini` module if unused (P1)
  - Covers: [REQ-CLEAN-001](SPEC.md#req-clean-001)

- [x] T-CLEAN-20251108-07 — Remove non-Gemini OCR providers and keep only Gemini OCR (P1)
  - Covers: [REQ-OCR-001](SPEC.md#req-ocr-001)

- [x] T-CLEAN-20251108-08 — Audit and consolidate `embed.rs` and `embed_rig.rs` into single implementation (P1)
  - Covers: [REQ-EMBED-UNIFIED-001](SPEC.md#req-embed-unified-001)
  - Status: ✅ COMPLETED - Unused embed_rig.rs removed; unified on GeminiEmbedClient

- [~] T-CLEAN-20251108-09 — Remove `crates/zetesis-app/src/pdf.rs` and `crates/zetesis-app/src/text/` modules (P1)
  - Covers: [REQ-CLEAN-001](SPEC.md#req-clean-001)
  - Status: 🔄 DEFERRED — Modules are still the sole implementation of PDF rasterization/text cleanup used by OCR and structured extraction; removal would break REQ-OCR-002/REQ-PIPELINE-001. Revisit only once alternate shared crate exists.

- [x] T-CLEAN-20251108-10 — Audit `orchestrator.rs` usage: document purpose if needed or delete if unused (P1)
  - Covers: [REQ-CLEAN-001](SPEC.md#req-clean-001)
  - Status: ✅ COMPLETED - File already deleted; was unused placeholder with only TODO comment
