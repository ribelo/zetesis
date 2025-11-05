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
1. **Scrape**: Fetch documents (HTML, Markdown, PDF). TODO: choose extraction tooling (e.g., `scraper`, `selectolax`, or external services).
2. **Normalize**: Convert to clean text, capture metadata (title, URL, tags).
3. **Persist**: Upsert canonical structured documents into Milli (doc record) and derived chunk records with vectors.
4. **Index**: Configure Milli searchable/filterable fields and user-provided vectors generated through `ai-ox`.
5. **Query**: HTTP endpoints and CLI utilities route queries through milli, returning ranked results with metadata pulled from LMDB.

## 5. Interfaces
- **HTTP API**: Axum routes for search, document management, and health checks. Use Tower middleware for tracing and auth when defined.
- **CLI**: Clap commands for operational tasks (document ingestion, scrapers, migrations). Matches server modules for reuse.
- **Web UI**: TBD (Svelte vs Dioxus). Scaffold will reserve a `web/` entry point once the framework decision is made.

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
