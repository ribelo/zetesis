# Zetesis Architecture Overview

## 1. Purpose
Zetesis is a lightweight SaaS for full-text and similarity search over documents, starting with feature parity to https://szukio.pl/. This document captures the foundational architecture so code, docs, and operations stay aligned.

## 2. System Components
- **Server (`zetesis-app`)**: Single binary exposing both the Axum HTTP API and a Clap-powered maintenance CLI.
- **CLI Commands**: Share the binary entry point; subcommands such as `serve`, `documents add`, and `scrape ...` will be introduced once requirements solidify.
- **Storage / Search**: A single Milli index (backed by LMDB internally) stores canonical structured documents, chunk records, and user-provided vectors.
- **Blob Store**: Original PDFs are written to the filesystem using content-addressed paths under the XDG data directory.
- **AI & Embeddings**: The local `ai-ox` crate supplies LLM utilities and embedding models; treat it as the sole integration point for AI tasks.
- **Configuration**: Loaded with the `config` crate as TOML only, using XDG precedence (`/etc/xdg` → `$XDG_CONFIG_HOME` → `./config/settings.toml` → `$ZETESIS_CONFIG_FILE`) before applying `ZETESIS__` env overrides; defaults live in `config/settings.example.toml` and CORS stays off unless explicitly configured.

## 3. Data & Storage Strategy
- The Milli index is the authoritative store for structured documents and chunk records. All canonical JSON lands here and is queryable directly.
- Original PDFs are stored on disk (`${XDG_DATA_HOME}/zetesis/blobs/{silo}/{prefix}/{hash}.pdf`) with stable identifiers linking them to Milli documents.
- Auxiliary application metadata (e.g., configuration caches, counters) may live in separate LMDB environments, but documents themselves never leave Milli.

## 4. Ingest & Search Pipeline
1. **Scrape / Collect**: Fetch upstream documents (PDF or images) using the SAOS/UZP scrapers or manual ingest.
2. **Stage Payload**: Compute the canonical `doc_id` (BLAKE3 of bytes), persist the original blob under `${XDG_DATA_HOME}/zetesis/blobs`, and enqueue a generation job in LMDB.
3. **Structured Generation**: Run the Gemini extractor either synchronously (default) or queue the request for batch submission. Batch mode stores payloads under `jobs/payloads/{silo}` and is processed via `jobs gen submit`.
4. **Embedding**: Once structured data exists (immediately for sync, or during `jobs gen fetch` for batch), embed semantic chunks synchronously via the configured Gemini embedder; enforce vector count/dimension invariants.
5. **Index**: Write structured decision + chunk records (with vectors) into Milli; clear job `pending_decision` as the job transitions to `Generated`.
6. **Query**: HTTP endpoints and CLI utilities route keyword/vector searches through Milli, returning ranked results with metadata pulled from LMDB.

## 4.1 Hybrid Search (WP-HYBRID)
- **No native RRF in Milli (T32):** Milli v1.24.0 exposes lexical and semantic searches only; `rg -n "rrf"` / `rg -n "reciprocal"` across the vendored sources returned no hits, so Zetesis performs RRF/weighted fusion client-side. This keeps the call graph acyclic and bounded—two Milli searches per hybrid query, nothing more.
- **Default fusion (T33):** Hybrid search runs keyword + vector queries in parallel, caps each branch at 50 hits (bounded work), applies RRF with `k=60`, deduplicates by `doc_id`, and emits at most 100 fused rows. Fused scores overwrite base scores so downstream consumers do not need to infer which ranking strategy ran.
- **Weighted alternative (T34):** Requests may opt into a linear fusion that normalizes caller-provided weights (default 0.5 keyword / 0.5 vector, documented in CLI/HTTP help). When the weighted mode is chosen both weights must be non-negative and not both zero; the code renormalizes them to keep the math stable.

## 5. Interfaces
- **HTTP API**: Axum routes for search, document management, and health checks. Use Tower middleware for tracing and auth when defined.
- **CLI**: Clap commands for operational tasks (document ingestion, scrapers, migrations, job orchestration). Notable flows: `ingest --gen-mode sync|batch` (legacy `--batch` prints a deprecation warning and forwards to `--gen-mode batch`), `jobs status`, `jobs gen submit|gen fetch`, and `jobs reap`.
- **Web UI**: Dioxus 0.7.1 with SSR + Islands architecture, served under `/ui/*` routes separate from `/v1/*` API routes.

## 5.1 Web UI Architecture (Dioxus 0.7.1 + SSR + Islands)

### Technology Stack
- **Framework**: Dioxus 0.7.1 with SSR and Web features
- **Server**: Axum 0.8 integration via `dioxus-fullstack`
- **CSS**: Native Tailwind CSS support via dx-cli 0.7
- **Components**: Dioxus first-party primitives (Radix-like headless components)

### Crate Structure
```
crates/
├── zetesis-ui/          # Dioxus UI components (SSR + Web features)
├── zetesis-server/      # Axum 0.8 server (API + SSR serving)
├── zetesis-shared/      # Shared types, utilities, API types
└── zetesis-app/         # Main binary (minimal launcher)
```

### Architecture Pattern
- **SSR Strategy**: Server-side rendering with selective hydration
- **Islands Pattern**: Only interactive components get client-side JavaScript
- **Bundle Optimization**: ≤ 150 KiB gzipped initial bundle size limit
- **Asset Pipeline**: Static asset optimization with fingerprinting and caching

### Performance Targets
- **Bundle Size**: ≤ 150 KiB gzipped for initial page load
- **Time to Interactive**: ≤ 100ms for typeahead functionality
- **First Contentful Paint**: ≤ 1.5s on 3G connection
- **Hydration Time**: ≤ 50ms for individual islands

## 6. Observability & Operations
- Use `tracing` + `tracing-subscriber` for structured logs; default to info level.
- Add metrics (e.g., `metrics` crate or OpenTelemetry) after core features land.
- Docker/OCI packaging is deferred until after API stabilizes.

## 7. Open Questions
- Preferred scraper/extractor stack and content formats to support.
- Authentication/authorization model for SaaS users.
- Vector embedding strategy for similarity search (external service vs. local models).
- Deployment environment (single binary on VPS vs. managed container).

Keep this document current as implementation decisions land; update alongside SPEC/TODO during each sprint review.
