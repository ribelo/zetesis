# Zetesis Architecture Overview

## 1. Purpose
Zetesis is a lightweight SaaS for full-text and similarity search over documents, starting with feature parity to https://szukio.pl/. This document captures the foundational architecture so code, docs, and operations stay aligned.

## 2. System Components
- **Server (`zetesis-app`)**: Single binary exposing both the Axum HTTP API and a Clap-powered maintenance CLI.
- **CLI Commands**: Share the binary entry point; subcommands such as `serve`, `documents add`, and `scrape ...` will be introduced once requirements solidify.
- **Storage**: LMDB via the `heed` crate stores canonical documents, metadata, and index snapshots.
- **Search Index**: The `milli` engine (from Meilisearch) provides full-text and vector-similarity search, embedded alongside LMDB.
- **AI & Embeddings**: The local `ai-ox` crate supplies LLM utilities and embedding models; treat it as the sole integration point for AI tasks.
- **Configuration**: Loaded with the `config` crate, resolved through XDG directories via `directories`; defaults live in `config/settings.example.toml`.

## 3. Data & Storage Strategy
- LMDB is the authoritative store: one environment under `${XDG_DATA_HOME}/zetesis/lmdb`.
- Document ingestion will normalize content, persist the canonical form in LMDB, and emit an index update into milli.
- Index snapshots mirror LMDB transactions to keep writes atomic; cross-reference keys should remain simple strings (`doc:{id}`).

## 4. Ingest & Search Pipeline
1. **Scrape**: Fetch documents (HTML, Markdown, PDF). TODO: choose extraction tooling (e.g., `scraper`, `selectolax`, or external services).
2. **Normalize**: Convert to clean text, capture metadata (title, URL, tags).
3. **Persist**: Store normalized documents in LMDB.
4. **Index**: Push records into milli, configuring searchable fields, ranking rules, and embedding vectors generated through `ai-ox`.
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
