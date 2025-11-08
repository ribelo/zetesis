> AI: Do not change any IDs (D-*). Only edit bullets under the item.

# DECISIONS

Short Architecture Decision Records (ADRs) capturing why we chose specific approaches.

## D-20251108-01 — Use Dioxus 0.7.1 with SSR + Islands architecture
- Why: Need performant web UI with minimal JavaScript payload and modern SSR capabilities.
- Decision: Adopt Dioxus 0.7.1 with SSR + Islands pattern, Axum 0.8 integration.
- Consequence: Requires multi-crate workspace structure, new build pipeline with dx-cli.
- Links: [REQ-UI-SSR-001](SPEC.md#req-ui-ssr-001), [T-UI-20251108-01](TODO#t-ui-20251108-01)

## D-20251108-02 — Multi-crate workspace for UI/server separation
- Why: Separate concerns, enable independent testing, and cleaner dependencies.
- Decision: Split into zetesis-ui, zetesis-server, zetesis-shared, zetesis-app crates.
- Consequence: More complex workspace setup but better modularity and compile times.
- Links: [T-UI-20251108-02](TODO#t-ui-20251108-02)

## D-20251108-03 — Use Dioxus first-party primitives component library
- Why: Consistent API, accessibility built-in, modeled after Radix/shadcn.
- Decision: Use dioxus-primitives for all UI components instead of custom impl.
- Consequence: Learning curve for primitives, but faster development and better UX.
- Links: [T-UI-20251108-03](TODO#t-ui-20251108-03)

## D-20251108-04 — Native Tailwind CSS support via dx-cli
- Why: Built-in tooling, no extra dependencies, fast iteration.
- Decision: Use dx-cli 0.7's native Tailwind integration instead of manual setup.
- Consequence: Must use dx serve for development; CSS purging handled automatically.
- Links: [T-UI-20251108-04](TODO#t-ui-20251108-04)

## D-20251108-05 — Defer server extraction to prioritize CLI cleanup
- Why: Server extraction requires extensive refactoring with high risk; CLI improvements provide immediate value.
- Decision: Focus on CLI modularization and database management features; defer multi-crate separation.
- Consequence: Maintains production stability while delivering operational improvements.
- Links: [T-CLEAN-20251108-01](BACKLOG.md#t-clean-20251108-01), [T-CLEAN-20251108-08](BACKLOG.md#t-clean-20251108-08)

## D-20251108-05 — Client-side hybrid search RRF fusion
- Why: Milli lacks native RRF; client-side keeps search logic simple.
- Decision: Perform RRF fusion in CLI/server, bounded k=60, max 50 hits per branch.
- Consequence: More network traffic but bounded, simpler search implementation.
- Links: [REQ-HYBRID-001](SPEC.md#req-hybrid-001), [T-SEARCH-20251108-01](TODO#t-search-20251108-01)

## D-20251108-06 — Synchronous embeddings after structured generation
- Why: Pipeline order guarantees vectors exist when documents are indexed.
- Decision: Always embed synchronously after generation completes (sync or batch).
- Consequence: Simpler state machine, no async embedding jobs needed.
- Links: [REQ-PIPELINE-001](SPEC.md#req-pipeline-001), [T-PIPELINE-20251108-01](TODO#t-pipeline-20251108-01)

## D-20251108-07 — Gemini-only OCR service
- Why: REQ-OCR-001 now mandates a single OCR provider; DeepInfra added code churn, duplicate prompts, and separate concurrency pools without improving accuracy.
- Decision: Delete the DeepInfra provider, token accounting glue, and dependency surface so the OCR service exclusively targets Gemini (sync now, batch later).
- Consequence: All OCR invocations share one set of rate limits and configuration knobs; any future provider addition must justify itself with a new SPEC slug.
- Links: [REQ-OCR-001](SPEC.md#req-ocr-001), [T-CLEAN-20251108-07](BACKLOG.md#t-clean-20251108-07)

## D-20251108-08 — Keep `pdf.rs`/`text` utilities until shared crate exists
- Why: These modules power PDF rasterization for OCR and text cleanup/sentence segmentation for structured extraction; no replacement exists yet.
- Decision: Defer deletion of `crate::pdf` and `crate::text` until a reusable crate emerges, documenting the dependency to avoid repeated churn.
- Consequence: Cleanup work focuses elsewhere; future restructuring must include alternate implementations before removing today’s modules.
- Links: [REQ-OCR-002](SPEC.md#req-ocr-002), [REQ-PIPELINE-001](SPEC.md#req-pipeline-001), [T-CLEAN-20251108-09](BACKLOG.md#t-clean-20251108-09)

## D-20251108-09 — Extract HTTP server into dedicated crate
- Why: REQ-SERVER-001 demands clear modular boundaries; keeping Axum wiring inside `zetesis-app` tied server behavior to CLI and prevented trait-based injection.
- Decision: Move the server implementation (router, rate limiting, API types) into the new `zetesis-server` crate, define the `SearchProvider` trait there, and have the CLI inject its own provider implementation.
- Consequence: The server builds without touching CLI code, CLI `sys serve` reuses the same binary logic via dependency injection, and future work can publish the crate independently or embed it in other runtimes.
- Links: [REQ-SERVER-001](SPEC.md#req-server-001), [T-CLEAN-20251108-04](BACKLOG.md#t-clean-20251108-04)
