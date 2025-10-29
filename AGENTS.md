# Engineering Workflow and Safety Guidelines

## 1) Safety Rules

- **Simple control flow only:** no `goto`, recursion, or equivalents. Maintain an acyclic call graph for provable boundedness.
- **Bound every loop:** Provide static justification for upper bounds. Enforce hard limits if iteration counts are uncertain.
- **No dynamic allocation after initialization** in critical/long-running paths; design memory upfront with arenas/pools/fixed capacities.
- **Small functions:** keep below ~60 logical lines. Larger functions often signal unclear structure.
- **Average 2+ side-effect-free assertions per function; trigger explicit recovery on failure.**
- **Declare variables in narrowest possible scope. Don't reuse variables for multiple purposes.**
- **Always check results, validate inputs, and propagate errors explicitly. Never ignore results without clear justification.**
- **Limit metaprogramming and macros. Avoid complex conditional compilation or macro-based DSLs.**
- **Avoid raw pointers/function-style indirection on critical paths; prefer static dispatch/generics.**
- **Zero warnings, continuous strict static analysis. Rewrite code for clarity if analysis/tools incorrectly warn.**

> **Favor strict, safety-conscious guidelines over idiomatic shortcuts that sacrifice resilience.**

---

## 2) Extensions

- **Prioritize:** Safety → Performance → Developer Experience.
- **Pair assertions at caller/callee to enforce contracts; check required/forbidden behaviors at compile time when possible.**
- **Centralize control/state at parent; keep leaf functions pure; batch ops to amortize cost.**
- **Sketch performance: estimate bandwidth/latency for network, disk, memory, CPU. Optimize the slowest/highest-frequency resource.**
- **Buffer/batch to bound work; don't react immediately to interrupts.**
- **Prefer explicit call-site options over defaults to avoid subtle behavior.**
- **Favor explicitly sized types (e.g., `u32`) for protocols/storage over arch-dependent ones.**

---

## 3) Pragmatism and Simplicity

- **Avoid unnecessary features/abstraction; pursue the 80/20 solution.**
- **Abstractions must earn their keep—prefer boring, simple solutions.**
- **Delete unjustified complexity and legacy shims. Ship the simplest design.**
- **Respect data/layout/hardware constraints. Minimize indirection and do IO at boundaries.**
- **Favor referentially transparent/pure functions over hidden state or mutation.**
- **If confused, stop and fix (Kill FOLD).**
- **Prototype minimal demos when stuck to expose real constraints.**
- **Invest in robust, human-friendly logging; debugging is a major time expense.**
- **Use generics moderately; avoid over-complex type-level tricks.**

---

## 4) Language-Specific Conventions

- **Only use unsafe when necessary; isolate and document invariants/assertions/tests in minimal modules.**
- **Prefer borrowing to cloning; limit ownership/lifetime complexity.**
- **Pre-allocate buffers, arrays, slabs, arenas; avoid heap growth post-init on critical paths.**
- **Default to static dispatch; restrict trait objects on hot paths unless strictly needed and justified.**
- **Keep macros trivial; don't stack `cfg` attributes/explode test matrix.**
- **Idiomatic, explicit names with unit suffixes for clarity (e.g., `_ms`).**

---

## 5) Testing, Verification, and Observability

- **Test negative as rigorously as positive cases: model boundaries, use property/fuzz testing to find bugs.**
- **Fix bugs by first writing failing regression tests.**
- **Handle all errors. Never ignore Results. Use structured, context-rich logs for debugging.**
- **After edits or tests, validate outcomes and document next steps; self-correct if validation fails.**

---

## 6) Communication & Collaboration

- **Ask clarifying questions if any aspect is unclear or ambiguous before proceeding.**
- **Propose or advise simpler, more maintainable solutions when requested approach is overly complex; reject unnecessary complexity.**
- **Use external resources instead of reinventing solutions when unfamiliar requirements arise.**

---

## 7) PR Format & Pre-Merge Checklist

- **PR template:** Reasoning → Decision → Plan (tests/telemetry) → Result.
- **Checklist:**
  - [ ] Bounds everywhere
  - [ ] ≥2 assertions/function
  - [ ] ≤60 lines/function
  - [ ] Zero warnings
  - [ ] Static analysis clean
  - [ ] Negative tests present
  - [ ] Actionable logs
- **Conventional prefixes (`feat:`, `fix:`, `docs:`, `chore:`); single-purpose commits.**
 - **PR hygiene:** Link relevant TODO items, include validation commands, and record follow-ups in TODO instead of the PR thread.

---

## Project Structure & Module Organization
- Keep `SPEC` ahead of code; mirror slugs in `TODO` and review daily on active work.
- Shared crates live under `crates/`; add only once code proves reusable.
- Layout: CLI in `src/cli/`; orchestration-only services in `src/services/`; pure transforms/utilities in `src/text/`, `src/pdf.rs`, and `src/pipeline/`; integration suites in `tests/`; docs in `docs/`. New code must respect this boundary.
- OCR providers plug into `services::ocr` via the `PageProvider` trait and `OcrEngine`; reuse the shared concurrency/rate-limiting pipeline instead of cloning ad-hoc loops.
- Gemini OCR pricing (Gemini API paid tier, per 1 M tokens): input text/image/video $0.10, output (incl. thinking) $0.40. Batch API halves this ($0.05 / $0.20). Free tier is zero-cost but rate-limited.
- DeepSeek-OCR on DeepInfra (per 1 M tokens): input $0.03, output $0.10. OlmOCR pricing is not currently published on DeepInfra—confirm with dashboards before committing workloads.

## Communication Defaults
- Lead with the answer, then evidence; numbered questions keep alignment and force plain speech.

## Build, Test, and Development Commands
- `cargo build` compiles debug binaries; `cargo run -- <cmd>` invokes CLI helpers.
- `cargo fmt` (or `-- --check`) and `cargo clippy --all-targets --all-features` enforce style.
- `cargo test` runs unit/integration suites; narrow scope with module paths while iterating.
- We use `jj` (Jujutsu) for version control; prefer `jj st`, `jj commit`, and `jj squash` flows over direct Git porcelain. Keep the Git mirror clean by syncing via `jj git push`.

## Style & Boundaries
- Trust rustfmt (4 spaces): `snake_case` functions, `CamelCase` types, SCREAMING_SNAKE_CASE constants.
- Prefer structural tools (`ast-grep`, `fastmod`); never invent sample data.
- Keep solutions boring; abstractions must earn their keep.
- Model errors with `thiserror` and lean on `strum` macros instead of hand-coded enum glue.
- Reach for `tap` to label flows instead of temporary locals.
- Builders are first-class tools—always derive them with the `bon` crate; hand-rolled builders are banned.
- Keep ingestion/services UI-free—produce structured data/events only; presentation (progress bars, logs formatting, ANSI) lives strictly in CLI/front-end layers.
- Never expose callbacks or raw channels across module boundaries—use `Stream` to surface asynchronous sequences.

### bon Builder Notes
- Use `#[derive(Builder)]` from `bon::Builder`; annotate optional fields with `.maybe_<field>` in code via `maybe_field` setter instead of manual `if let`.
- Provide defaults with `#[builder(default = EXPR)]`; use raw expressions (no string literal), e.g., `NonZeroUsize::new(4).unwrap()`.
- Add `#[builder(into)]` when builder should accept types convertible into the field (e.g., `PathBuf`).
- When building, chain setters then call `.build()`; for optional values, prefer `.maybe_field(option)` to keep the builder state consistent.
- Re-export builder types when useful so callers can type hint them; keep constructors in sync with SPEC requirements.

## Quality & Design Tenets
- Ship the simplest design; delete unjustified complexity and legacy shims.
- Keep documentation accurate or delete it.
- Keep code where the work happens, name it literally, and prove assumptions with tests.
- Respect hardware realities—data-oriented layout, minimal indirection, side effects at IO boundaries.

## Functional Discipline
- Keep functions referentially transparent—no hidden state or implicit mutation; prefer pure transformations that return new values when practical.
- Compose behavior through traits with explicit implementations instead of runtime lookups; every abstraction must be obvious from the call site.
- Reject shortcuts that smuggle complexity; make data flow and ownership visible in types.

## Complexity Discipline
- Cut scope before adding knobs; every line must earn its keep.
- Say "no" to features/abstractions unless simplification is obvious.
- Kill FOLD: if it feels confusing, stop and fix it.

## Testing Guidelines
- Co-locate fast tests via `#[cfg(test)]`; scenario tests live in `tests/<feature>_spec.rs` using `tokio::test`.
- Reference SPEC slugs (e.g., `covers REQ-ZE-CLI-FIRST`) to keep traceability.



## Preferred Libraries
- Async & concurrency: Tokio with `futures-concurrency`, `async-trait`.
- Config & storage: `config`, LMDB/`heed`, `bincode`, `serde_json`, `rustc-hash`, `blake3`.
- Interfaces & UX: `clap`, `owo-colors`, `comfy-table`, `bon`.
- Text & search: `scraper`, `pulldown-cmark`, `itertools`, `indoc`, `strum`, `unicode-*`, `regex`, `milli`, `tokenizers`.
- Networking & resilience: `reqwest` (rustls), `governor`, `backon`, `rayon`, `csv`, `chrono`, `uuid`, `bytes`, `tempfile`, `mime`, `nalgebra`, `pdfium-render`, `base64`, `bcrypt`.
- AI & embeddings: reuse `../ai-ox`.

## Async Stack Defaults
- **Purpose:** pick async tools that match Tokio-first apps without smuggling extra complexity.
- **Tokio runtime:** use Tokio end-to-end for scheduling, timers, I/O, channels, cancellations, and multithreaded spawning (`Send + 'static`). Keep `tokio`, `tokio-util`, and `tokio-stream` in scope; add `futures-util` only when you need adapters like `for_each_concurrent`.
- **Prefer futures-concurrency:** default to `futures-concurrency` for orchestration whenever you do not need explicit parallel task spawns. Use its `join`, `try_join`, and `race` utilities for fixed sets; reach for `into_co_stream` + `.limit()` when you must process collections with bounded concurrency.
- **Parallel hot paths:** when true parallelism or task spawning is required, stay on Tokio primitives (`tokio::try_join!`, `tokio::task::JoinSet`) and ensure spawned futures are `Send + 'static`. Combine with `futures-concurrency` only where borrowing-friendly combinators help.
