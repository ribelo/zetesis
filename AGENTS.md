# Repository Guidelines

## Project Structure & Module Organization
- Keep `SPEC` ahead of code; mirror slugs in `TODO` and review daily on active work.
- Shared crates live under `crates/`; add only once code proves reusable.
- Layout: CLI in `src/cli/`, services in `src/services/`, shared logic in `src/lib.rs`, integration suites in `tests/`, docs in `docs/`.

## Communication Defaults
- Lead with the answer, then evidence; numbered questions keep alignment and force plain speech.

## Build, Test, and Development Commands
- `cargo build` compiles debug binaries; `cargo run -- <cmd>` invokes CLI helpers.
- `cargo fmt` (or `-- --check`) and `cargo clippy --all-targets --all-features` enforce style.
- `cargo test` runs unit/integration suites; narrow scope with module paths while iterating.

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
- Say “no” to features/abstractions unless simplification is obvious.
- Kill FOLD: if it feels confusing, stop and fix it.

## Testing Guidelines
- Co-locate fast tests via `#[cfg(test)]`; scenario tests live in `tests/<feature>_spec.rs` using `tokio::test`.
- Reference SPEC slugs (e.g., `covers REQ-ZE-CLI-FIRST`) to keep traceability.

## Commit & Pull Request Guidelines
- Use conventional prefixes (`feat:`, `fix:`, `docs:`, `chore:`) and single-purpose commits.
- PRs must link TODO items, list validation commands, and capture follow-ups in TODO instead of the PR thread.

## Preferred Libraries
- Async & concurrency: Tokio with `futures-concurrency`, `async-trait`.
- Config & storage: `config`, LMDB/`heed`, `bincode`, `serde_json`, `rustc-hash`, `blake3`.
- Interfaces & UX: `clap`, `owo-colors`, `comfy-table`, `bon`.
- Text & search: `scraper`, `pulldown-cmark`, `itertools`, `indoc`, `strum`, `unicode-*`, `regex`, `milli`, `tokenizers`.
- Networking & resilience: `reqwest` (rustls), `governor`, `backon`, `rayon`, `csv`, `chrono`, `uuid`, `bytes`, `tempfile`, `mime`, `nalgebra`, `pdfium-render`, `base64`, `bcrypt`.
- AI & embeddings: reuse `../ai-ox`.
