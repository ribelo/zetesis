> AI: Do not change any IDs (D-*). Only edit bullets under the item.

# DECISIONS

Short Architecture Decision Records (ADRs) capturing why we chose specific approaches.

## D-20251108-01 — Use Dioxus 0.7.1 with SSR + Islands architecture
- Why: Need performant web UI with minimal JavaScript payload and modern SSR capabilities.
- Decision: Adopt Dioxus 0.7.1 with SSR + Islands pattern, Axum 0.8 integration.
- Consequence: Requires multi-crate workspace structure, new build pipeline with dx-cli.
- Links: [REQ-UI-SSR-001](SPEC.md#req-ui-ssr-001), [T-UI-20251108-01](TASKS.md#t-ui-20251108-01)

## D-20251108-02 — Multi-crate workspace for UI/server separation
- Why: Separate concerns, enable independent testing, and cleaner dependencies.
- Decision: Split into zetesis-ui, zetesis-server, zetesis-shared, zetesis-app crates.
- Consequence: More complex workspace setup but better modularity and compile times.
- Links: [T-UI-20251108-02](TASKS.md#t-ui-20251108-02)

## D-20251108-03 — Use Dioxus first-party primitives component library
- Why: Consistent API, accessibility built-in, modeled after Radix/shadcn.
- Decision: Use dioxus-primitives for all UI components instead of custom impl.
- Consequence: Learning curve for primitives, but faster development and better UX.
- Links: [T-UI-20251108-03](TASKS.md#t-ui-20251108-03)

## D-20251108-04 — Native Tailwind CSS support via dx-cli
- Why: Built-in tooling, no extra dependencies, fast iteration.
- Decision: Use dx-cli 0.7's native Tailwind integration instead of manual setup.
- Consequence: Must use dx serve for development; CSS purging handled automatically.
- Links: [T-UI-20251108-04](TASKS.md#t-ui-20251108-04)

## D-20251108-05 — Client-side hybrid search RRF fusion
- Why: Milli lacks native RRF; client-side keeps search logic simple.
- Decision: Perform RRF fusion in CLI/server, bounded k=60, max 50 hits per branch.
- Consequence: More network traffic but bounded, simpler search implementation.
- Links: [REQ-HYBRID-001](SPEC.md#req-hybrid-001), [T-SEARCH-20251108-01](TASKS.md#t-search-20251108-01)

## D-20251108-06 — Synchronous embeddings after structured generation
- Why: Pipeline order guarantees vectors exist when documents are indexed.
- Decision: Always embed synchronously after generation completes (sync or batch).
- Consequence: Simpler state machine, no async embedding jobs needed.
- Links: [REQ-PIPELINE-001](SPEC.md#req-pipeline-001), [T-PIPELINE-20251108-01](TASKS.md#t-pipeline-20251108-01)
