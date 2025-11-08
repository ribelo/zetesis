> AI: Do not change any IDs (REQ-*). Only edit bullets under the item.

# SPEC

EARS-style requirements for Zetesis. Never renumber IDs; append new ones.

## Configuration

### REQ-CONFIG-001 — Configuration loading from TOML files
- Type: ubiquitous
- EARS: The system shall load settings from TOML files only, honoring precedence `/etc/xdg/zetesis/settings.toml` (override root via `$ZETESIS_ETC_CONFIG_DIR`), `$XDG_CONFIG_HOME/zetesis/settings.toml`, `./config/settings.toml`, then `$ZETESIS_CONFIG_FILE`, before applying `ZETESIS__` environment overrides with double-underscore separators; no implicit `.env` loading.
- Status: accepted
- Trace: [T-CONFIG-20251107-01](TODO#t-config-20251107-01)
- History:
  - 2025-11-07: Created.

### REQ-CONFIG-002 — CORS configuration disabled by default
- Type: state-driven
- EARS: CORS shall remain disabled by default; enabling it requires explicit http/https origins, an `OPTIONS`-inclusive method list, header/expose lists bounded to ≤64 entries of ≤128 characters, and `max_age_secs ≤ 86400`, producing headers that mirror the configured allow/expose/credentials choices.
- Status: accepted
- Trace: [T-CONFIG-20251107-01](TODO#t-config-20251107-01)
- History:
  - 2025-11-07: Created.

## Storage & Indexing

### REQ-STORAGE-001 — Milli as primary document store
- Type: ubiquitous
- EARS: The system shall persist canonical structured documents and derived chunk records directly in Milli. Each record shall be self-contained so Milli alone can satisfy read and reindex flows.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Updated — aligned to current code.

### REQ-STORAGE-002 — Blob storage on filesystem
- Type: ubiquitous
- EARS: Blobs shall live under `$XDG_DATA_HOME/zetesis/blobs/{silo}/{prefix}/{cid}` where `cid` is lowercase hex BLAKE3 of original bytes. Callers may additionally cache KIO downloads in user-provided folders.
- Status: accepted
- Trace: [T-STORAGE-20251107-01](TODO#t-storage-20251107-01)
- History:
  - 2025-11-04: Updated.

### REQ-INDEX-001 — Per-silo Milli indexes
- Type: ubiquitous
- EARS: The system shall maintain one Milli index per silo at `$XDG_DATA_HOME/zetesis/milli/{silo}`. Paths shall be derived from a lowercase slug; do not duplicate elsewhere.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-002 — Embedder configuration
- Type: ubiquitous
- EARS: The system shall configure indexes with Hannoy vector backend and a user-provided embedder (default from config), dimension `DEFAULT_EMBEDDING_DIM`. Vectors shall be stored under `_vectors.{embedder_key}` with user-provided embeddings.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-003 — Document record shape
- Type: ubiquitous
- EARS: Document records (`doc_type=doc`) shall contain: `id`=`{silo}-{doc_id}-doc`, `silo`, `doc_type`, `doc_id`, `summary_short`, `panel_names[]`, `panel_roles[]`, `panel{names,roles}`, `parties_*` flats, `procurement_*` flats, `identifiers_*` flats, `decision_date`, `decision_result`, `issues_keys[]`, `structured` (full JSON), `ingest{status,updated_at,embedder_key,chunk_count,error}`, `_vectors.{embedder_key}`=null.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-004 — Chunk record shape
- Type: ubiquitous
- EARS: Chunk records (`doc_type=chunk`) shall contain: `id`=`{silo}-{doc_id}-chunk-{ord}`, `silo`, `doc_type`, `doc_id`, `ord`, `section`, `content`, nested `decision{date,result}`, `identifiers{kio_docket,saos_id}`, `parties{...}`, `procurement{...}`, `panel{...}`, `issues[{issue_key,confidence}]`, and flat duplicates: `panel_names`, `panel_roles`, `parties_*`, `procurement_*`, `identifiers_*`, `decision_date`, `decision_result`, `issues_keys[]`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-005 — Searchable fields
- Type: ubiquitous
- EARS: Searchable fields shall include: `content`, `summary_short`, `doc_id`, `decision.{date,result}`, `identifiers.{kio_docket,saos_id}`, `parties.{contracting_authority,appellant,awardee,interested_bidders}`, `panel.{names,roles}`, `procurement.{subject,procedure_type,tender_id,cpv_codes}`, `issues.issue_key`, `section`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-006 — Filterable fields
- Type: ubiquitous
- EARS: Filterable fields shall include flat duplicates: `silo`, `doc_type`, `doc_id`, `decision_date`, `decision_result`, `identifiers_*`, `panel_*`, `parties_*`, `procurement_*`, `issues_keys`, `section`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-007 — Sortable fields
- Type: ubiquitous
- EARS: Sortable fields shall include `decision_date` and `doc_id`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-INDEX-008 — Idempotent indexing
- Type: ubiquitous
- EARS: Indexing shall use ReplaceDocuments semantics and stable primary key `id` to ensure idempotent writes.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## KIO Ingestion

### REQ-KIO-001 — KIO scraper implementation
- Type: event-driven
- EARS: The system shall provide a KIO scraper that enumerates judgments via `POST /Home/GetResults` on https://orzeczenia.uzp.gov.pl/ (Kind=KIO) and walks detail pages for metadata and downloads.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-KIO-002 — Discovery with CountStats
- Type: event-driven
- EARS: First-page requests shall send `CountStats=True` to capture total hits; subsequent pages toggle `CountStats=False` for throughput. The scraper shall respect the portal's 10-result page limit and advance with `Pg=1..n` using governed retries.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-KIO-003 — Concurrent pipeline
- Type: event-driven
- EARS: The system shall implement a two-stage concurrent pipeline: one async task performs discovery and pushes document descriptors onto a bounded channel; N worker tasks (configurable, default ≥4) pull descriptors and download judgment PDFs plus detail HTML, applying `governor` rate limits and `backon` retries.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-KIO-004 — Document metadata capture
- Type: event-driven
- EARS: For each KIO document, the system shall capture multiple case numbers, ruling type (Wyrok/Postanowienie), ISO ruling date, panel names, participant names, procurement subject, case sign, EU journal reference, and extracted legal references before chunking.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-KIO-005 — Asset storage
- Type: ubiquitous
- EARS: The system shall persist only the primary judgment body (`/Home/PdfContent/{id}?Kind=KIO`) to the blob store; skip metrics PDFs unless specifically requested later.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-KIO-006 — Document chunking
- Type: event-driven
- EARS: The system shall split KIO documents using the double-pass chunker into sections (`header`, `sentencja`, `uzasadnienie`, `other`) with stable ordering; offsets remain attached to chunk metadata inside Milli.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Updated — LMDB removed.

### REQ-KIO-007 — Ingest pipeline
- Type: event-driven
- EARS: The ingest pipeline shall: (1) write PDFs to the blob store, (2) persist canonical structured documents in Milli with ingest status `pending`, (3) embed chunks via `ai-ox` Gemini (IO-bounded with governed concurrency), (4) upsert chunk records with `_vectors.{embedder_key}`, and (5) mark the document record `indexed` with counts and timings.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Updated — LMDB removed.

### REQ-KIO-008 — Hard delete support
- Type: ubiquitous
- EARS: Hard deletes shall remove Milli documents (doc + chunks) and filesystem blobs in a single operation.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## SAOS Ingestion

### REQ-SAOS-001 — SAOS scraper implementation
- Type: event-driven
- EARS: The system shall provide a SAOS scraper for pre-2021 KIO judgments via `GET /api/search/judgments` and `GET /api/judgments/{id}` under https://www.saos.org.pl/. Respect `CUT_OFF_DATE` semantics to avoid overlap with UZP.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-SAOS-002 — Paging strategy
- Type: event-driven
- EARS: The system shall use page size 100; advance with `page=0..n` until exhaustion or limit reached; compute totals from response `info.totalResults`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-SAOS-003 — Rate limiting
- Type: ubiquitous
- EARS: The system shall apply a 4 rps limiter with exponential backoff and jitter across search, detail, and file downloads.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-SAOS-004 — File download
- Type: ubiquitous
- EARS: The system shall download PDFs from `files/judgments/national_appeal_chamber/<...>` and cache under caller-provided directory or blob storage.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## OCR Service

### REQ-OCR-001 — OCR providers
- Type: state-driven
- EARS: The system shall expose OCR exclusively via the Gemini provider (model configurable) and error fast when the Gemini API key is missing.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.
  - 2025-11-08: Updated to deprecate DeepInfra/olmOCR implementations per REQ-CLEAN-001.

### REQ-OCR-002 — OCR limits
- Type: ubiquitous
- EARS: The system shall enforce bounds: max batch inputs 64, max pages per document 1024, explicit non-zero concurrency for documents/pages, and inflight budget consistency checks.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-OCR-003 — Image processing
- Type: event-driven
- EARS: The system shall render PDF pages to PNG via pdfium, then downscale to `image_max_edge` preserving aspect ratio; encode PNG/JPEG as required by provider.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-OCR-004 — Retry policy
- Type: event-driven
- EARS: The system shall apply provider-specific rate limiting and exponential backoff; log usage tokens when available.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## Text Processing

### REQ-TEXT-001 — Text cleanup
- Type: ubiquitous
- EARS: The system shall normalize extracted text (token separation, whitespace, diacritics intact) prior to segmentation.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-TEXT-002 — Polish text segmentation
- Type: event-driven
- EARS: The system shall segment Polish legal text with ICU segmenter plus abbreviation/ordinal stitching and enumeration splitting. Provide deterministic ranges and strings with no empty outputs.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## Generation, Embedding & Jobs

### REQ-EMBED-001 — Embedder models
- Type: ubiquitous
- EARS: The system shall use Gemini embeddings for `RetrievalDocument`/`RetrievalQuery` with configured output dimension. Replace empty texts with all-zero vectors for shape stability.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-EMBED-002 — Synchronous embeddings
- Type: state-driven
- EARS: The system shall always compute embeddings synchronously after structured generation completes (whether generation was sync or async). Enforce vector count = chunk count and dimension = `DEFAULT_EMBEDDING_DIM`; apply a rate limiter.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Updated — embeddings no longer use jobs.

### REQ-JOBS-001 — Job storage
- Type: ubiquitous
- EARS: The system shall persist structured generation jobs in LMDB under `$XDG_DATA_HOME/zetesis/lmdb/jobs` with status lifecycle: Pending → Generating → Generated | Failed; `job_id` equals `doc_id`. Store provider metadata (`provider_job_id`, usage, error) and timestamps.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Updated — migrated from embedding jobs.

### REQ-JOBS-002 — Generation modes
- Type: event-driven
- EARS: The system shall support two generation modes: `sync` (default) and `batch` via Gemini Batch API. Sync runs execute immediately and record a completed job for observability. Batch runs submit to provider, record `provider_job_id`, and finalize upon result fetch; include bounds and provider rate limits.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Created.

## CLI Surface

### REQ-CLI-001 — CLI commands
- Type: ubiquitous
- EARS: The system shall provide subcommands: `serve`, `fetch-kio` (uzp|saos), `ingest`, `audit structured`, `search (keyword|vector)`, `db (list|stats|get|find|backup|purge|recover)`, `jobs (status|gen submit|gen fetch|reap)`.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-CLI-002 — Input validation
- Type: ubiquitous
- EARS: The system shall validate inputs and enforce bounds: positive worker counts, non-empty index names without separators, file size limits for inline attachments (≤ 20 MiB), and supported document types (PDF, common images).
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-CLI-003 — Search options
- Type: event-driven
- EARS: Keyword/vector search shall accept filters, pagination, field selection, and pretty output; vector search shall embed the query with index embedder when not provided.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-CLI-004 — Index existence checks
- Type: ubiquitous
- EARS: The system shall ensure target index exists before `ingest` or `db` operations and provide an explicit command to create indexes when they do not exist.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-CLI-005 — CLI structure
- Type: ubiquitous
- EARS: The CLI code shall be organized with one module file per command to improve maintainability and clarity.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-CLI-006 — Ingest generation mode
- Type: state-driven
- EARS: `ingest` shall accept `--gen-mode sync|batch` (default `sync`) and `--gen-model <id>`. The old `--batch` flag is deprecated immediately; show a warning and map it to `--gen-mode batch` for one release window before removal.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Created.

## Identity & Paths

### REQ-ID-001 — Canonical document IDs
- Type: ubiquitous
- EARS: Canonical `doc_id` shall be lowercase hex BLAKE3 of exact original content bytes. Never derive from filenames, URLs, or upstream IDs.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-ID-002 — Application paths
- Type: ubiquitous
- EARS: The system shall resolve data paths under `$XDG_DATA_HOME/zetesis`: `milli/{silo}`, `lmdb/{app,jobs}`, `blobs/{silo}/{prefix}/{cid}`, and generation job staging under `jobs/{payloads,results}/{silo}`. Ensure directories exist before use.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-ID-003 — URL strategy
- Type: ubiquitous
- EARS: Default source URLs shall be defined in domain-specific modules, not in CLI modules.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## Observability & Safety

### REQ-OBS-001 — Assertion usage
- Type: ubiquitous
- EARS: The system shall use assertions to enforce invariants (non-empty ids, bounded sizes, non-zero concurrency) and fail fast on configuration errors.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

### REQ-OBS-002 — Structured logging
- Type: ubiquitous
- EARS: The system shall emit structured logs for retries, indexing phases, and ingestion status transitions; prefer human-actionable messages.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-04: Created.

## HTTP & UI

### REQ-UI-001 — Dioxus SSR as rendering mode
- Type: state-driven
- EARS: The system shall use Dioxus 0.7 Fullstack with SSR as the primary rendering mode. Keep hydration to minimal islands (e.g., search results/typeahead). Do not use a client router. Enforce gzip JS budget ≤ 150 KiB per page.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-UI-002 — Axum integration with route separation
- Type: state-driven
- EARS: The system shall integrate Dioxus SSR into Axum under `/ui/*` routes; keep JSON APIs under `/v1/*`. UI and API share types; avoid leaking service/internal types across layers.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-UI-003 — Static site generation
- Type: state-driven
- EARS: The system shall use Dioxus SSG to pre-render static pages (landing, docs/help) at build time; serve as static assets.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-UI-004 — Typeahead search functionality
- Type: event-driven
- EARS: The system shall provide “search as you type” via keyword-only queries: debounce 200 ms, min 2 chars, limit ≤ 10, fields whitelisted, cancellable requests. Fall back to full SSR submit when JS is disabled.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-UI-005 — Hybrid search refinement
- Type: state-driven
- EARS: The system shall trigger vector/hybrid refine only on explicit action (Enter/idle > 600 ms/toggle). Never embed on every keystroke.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-UI-006 — Asset serving
- Type: ubiquitous
- EARS: The system shall serve static assets under `public/` (or embedded) with immutable caching; version via hashed filenames when practical.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

## Authn/Authz

### REQ-AUTH-001 — OAuth2 proxy authentication
- Type: state-driven
- EARS: The system shall terminate OAuth2/OIDC at a reverse proxy (e.g., oauth2-proxy) and trust identity headers only from a private network. Application remains stateless and holds no user database.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-AUTH-002 — JWT verification
- Type: state-driven
- EARS: The system shall optionally accept OIDC JWTs directly: verify signature via JWKS, enforce audience/issuer, allow small clock skew, cache keys, and reject on failure.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-AUTH-003 — API key support
- Type: state-driven
- EARS: The system shall support static API keys for programmatic access; scope keys to limited routes; disable by default.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-AUTH-004 — RBAC roles
- Type: state-driven
- EARS: The system shall implement simple roles (`user`, `admin`) via header/claims; admin-only routes include maintenance and backups.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

## Backup & Restore

### REQ-BACKUP-001 — Restic backup implementation
- Type: event-driven
- EARS: The system shall perform encrypted backups with restic to Hetzner S3 (or equivalent). Schedule daily backups with systemd timers and enforce retention (7 daily, 4 weekly, 6 monthly).
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-BACKUP-002 — Backup scope
- Type: ubiquitous
- EARS: Backup shall include `milli/*`, `lmdb/jobs`, and `blobs/*`. Run integrity checks post-backup: document counts, presence of vectors, and index health.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-BACKUP-003 — Recovery with dry-run
- Type: event-driven
- EARS: The system shall provide `db recover --dry-run` to validate a restore target; block in-place recoveries unless `--force` is set.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

## Job Queue & Reliability

### REQ-JOBS-001 — LMDB-backed job store
- Type: state-driven
- EARS: The system shall use the LMDB-backed job store as the primary queue for structured generation jobs; persist status transitions and timestamps; no external broker required.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Updated.

### REQ-JOBS-002 — Job reaper
- Type: event-driven
- EARS: The system shall add a stale job reaper (age-based) for generation jobs and idempotent retry policy with bounded attempts and exponential backoff. Target Pending/Generating states.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Updated.

### REQ-JOBS-003 — Job limits
- Type: ubiquitous
- EARS: The system shall enforce bounded concurrency and back-pressure via rate limiters on provider calls; expose `jobs status` to report counts and oldest age by state.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Updated.

## HTTP & Server

### REQ-SERVER-001 — Modular server architecture
- Type: ubiquitous
- EARS: The system shall provide a modular HTTP server architecture that supports separation of concerns and reusability.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-DB-001 — Document deletion
- Type: event-driven
- EARS: The system shall support deletion of individual documents with atomic removal of all associated data including index entries, chunks, and blobs, with dry-run capability.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-OCR-001 — OCR provider requirement
- Type: ubiquitous
- EARS: The system shall support OCR operations exclusively through the Gemini provider, removing legacy OCR implementations.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-EMBED-UNIFIED-001 — Unified embedding implementation
- Type: ubiquitous
- EARS: The system shall provide a single, unified embedding implementation without duplicate modules.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-CLEAN-001 — Codebase cleanliness
- Type: ubiquitous
- EARS: The system shall maintain a clean codebase without unused or ambiguous modules such as orchestrator implementations or legacy PDF parsing code.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

## Pipeline Order

### REQ-PIPELINE-001 — Ingestion flow order
- Type: event-driven
- EARS: Ingest flow shall be: fetch blob → structured generation (sync or batch job) → synchronous embedding of chunks → index in Milli → update status/usage metrics.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-06: Created.

## Hybrid Search

### REQ-HYBRID-001 — Client-side RRF fusion
- Type: state-driven
- EARS: The system shall perform RRF fusion client-side with bounded k=60 and max 50 hits per branch, since Milli lacks native RRF support.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.

### REQ-HYBRID-002 — Weighted linear fusion option
- Type: state-driven
- EARS: The system shall support optional weighted-linear fusion with tunable weights as alternative to RRF, defaulting to RRF.
- Status: accepted
- Trace: (none)
- History:
  - 2025-11-08: Created.
