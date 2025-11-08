# Data Quickstart

This quickstart walks through the dataset lifecycle using the redesigned CLI hierarchy.
All commands share the global `--silo <slug>` flag (default `kio`), so you can scope
operations without repeating an `INDEX` argument. The legacy positional `INDEX`
is still accepted for now but prints a warning pointing you to `--silo` plus the
new namespace paths.

## 1. Prepare the system

Ensure the directory layout, LMDB environments, and blob stores exist before doing
anything else:

```
zetesis sys init                # creates milli/blobs/jobs directories
zetesis sys doctor --verbose     # validates config, rate limits, API keys, and paths
```

`sys init` touches the expected storage roots for every registered silo; use
`--skip-nonessential` if you only want the bare minimum (LMDB + milli). `sys
doctor` reports the active config, storage backend, and whether a Gemini/Google
AI key is present.

## 2. Fetch new source data

Download KIO cases through the dedicated source namespace:

```
zetesis source kio fetch --limit 50 --workers 8
```

This mirrors the old `fetch-kio` command (which still works, but prints a
deprecation warning directing you here). The command honors `--silo` for
multi-silo setups, although all existing data lives in `kio` today.

## 3. Ingest documents

Put the captured PDFs into the in-memory Milli index:

```
zetesis ingest --silo kio kio-pdfs/2025 --gen-mode sync --gen-model gemini-2.5-flash
```

The `--silo` flag drives dataset selection, while the positional index slug is still
recognized as a legacy alias. Prefer `--silo` when scripting so the warning
stays hidden.

## 4. Drive generation jobs

The jobs namespace now exposes status/processing actions:

```
zetesis jobs status
zetesis jobs gen submit
zetesis jobs gen fetch
zetesis jobs reap --action both
```

`jobs gen submit` and `jobs gen fetch` correspond to batching structured generation
requests through the configured provider, while `jobs reap` cleans up stale jobs.

## 5. Query indexes

Search commands continue to live under `search` but inherit the same `--silo`
semantics:

```
zetesis search keyword --silo=kio --q "public procurement" --limit 20 --pretty
zetesis search vector --silo=kio --q "contracts" --k 15 --fields id,decision_date
```

Use `--pretty`, `--fields`, and filters consistently as before; the CLI now warns
when you pass an index positionally instead of taking it from `--silo` (the warning
mentions the 2025-12-31 removal date so you can update scripts proactively).

## 6. Inspect and protect data

Use the `silo` namespace to operate on individual indexes (list/backup/purge):

```
zetesis silo list
zetesis silo stats kio-2025
zetesis silo backup kio-2025 --out backups/kio
zetesis silo purge old-backup
```

The legacy `db` command continues to work (prints a warning) but will be removed
after 2025-12-31. `zetesis index` is the preferred alias to the same subcommands.

You can also run `zetesis index list`/`index backup`/`index purge` if you prefer the
new namespace name, and rely on the alias while you migrate older scripts.

## 7. Restore & recover

When you need to recover data or migrate indexes:

```
zetesis silo recover --index kio-2025 --from backups/kio/latest
```

This runs the same code paths as `db recover`, but the `silo` namespace groups it
with the rest of the dataset lifecycle.

## 8. Monitoring & diagnostics

Run `zetesis sys doctor` periodically or after configuration changes to ensure the
pipeline can reach Gemini/Google AI, write to the blob store, and honor the
configured rate limits.

```
zetesis sys doctor --verbose
```

When you see a warning that an API key is missing, set `GEMINI_API_KEY` or
`GOOGLE_AI_API_KEY` and rerun; the command will exit successfully once it can
read the config and the important directories.
