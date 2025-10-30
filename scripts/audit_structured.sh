#!/usr/bin/env bash
set -euo pipefail

# Audits semantic chunk coverage for random PDFs.
# - Picks N random PDFs from data/kio/raw/kio/
# - Runs `segment-pdf` and `extract-structured`
# - Computes multiple drift metrics and simple signal checks
#
# Usage:
#   bash scripts/audit_structured.sh [-n NUM] [--model MODEL] [-o OUT.tsv]
# Env:
#   OUT (optional): output TSV path (defaults to /tmp/structured_audit.tsv)

N=5
MODEL=""
OUT="${OUT:-/tmp/structured_audit.tsv}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--num)
      N="${2:-5}"; shift 2 ;;
    --model)
      MODEL="${2:-}"; shift 2 ;;
    -o|--out)
      OUT="${2:-/tmp/structured_audit.tsv}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [-n NUM] [--model MODEL] [-o OUT.tsv]"; exit 0 ;;
    *)
      echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

mapfile -t PDFS < <(ls data/kio/raw/kio/*.pdf | shuf -n "$N")

printf "pdf\tstatus\tsrc_tokens\tchunk_tokens\tdrift_token_pct\tsrc_chars\tchunk_chars\tdrift_char_pct\tdigits_src\tdigits_chunk\tdrift_digit_pct\tstatute_hits_src\tstatute_hits_chunk\theader_in_chunks\tchunks_count\n" > "$OUT"

for pdf in "${PDFS[@]}"; do
  base=$(basename "$pdf" .pdf)
  src_txt=/tmp/${base}_src.txt
  struct_json=/tmp/${base}_struct.json
  struct_log=/tmp/${base}_struct.log
  chunks_txt=/tmp/${base}_chunks.txt

  # Source text
  cargo run --quiet --bin zetesis-app -- segment-pdf --format text "$pdf" > "$src_txt"

  # Extract structured
  set +e
  if [[ -n "$MODEL" ]]; then
    cargo run --quiet --bin zetesis-app -- extract-structured --model "$MODEL" "$pdf" > "$struct_json" 2> "$struct_log"
  else
    cargo run --quiet --bin zetesis-app -- extract-structured "$pdf" > "$struct_json" 2> "$struct_log"
  fi
  status=$?
  set -e

  if [[ $status -ne 0 ]]; then
    # Failed extraction line
    printf "%s\tfail\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\tno\t0\n" "$pdf" >> "$OUT"
    continue
  fi

  # Chunk text
  jq -r '.chunks[]?.body' < "$struct_json" > "$chunks_txt"

  # Token and char counts
  src_tokens=$(tr -s '[:space:]' '\n' < "$src_txt" | sed '/^$/d' | wc -l | tr -d ' ')
  chunk_tokens=$(tr -s '[:space:]' '\n' < "$chunks_txt" | sed '/^$/d' | wc -l | tr -d ' ')
  src_chars=$(wc -m < "$src_txt" | tr -d ' ')
  chunk_chars=$(wc -m < "$chunks_txt" | tr -d ' ')

  # Drift helpers
  drift_pct() { awk -v s="$1" -v c="$2" 'BEGIN{ if(s==0){print 0}else{ printf "%.1f", (s-c)*100.0/s }}'; }
  tok_drift=$(drift_pct "$src_tokens" "$chunk_tokens")
  chr_drift=$(drift_pct "$src_chars" "$chunk_chars")

  # Numeric fingerprints
  digits_src=$(grep -ao '[0-9]' "$src_txt" | wc -l | tr -d ' ' || true)
  digits_chunk=$(grep -ao '[0-9]' "$chunks_txt" | wc -l | tr -d ' ' || true)
  dig_drift=$(drift_pct "$digits_src" "$digits_chunk")

  # Legal citation surface cues (rough): art., ust., §, Dz. U.
  statute_hits_src=$(grep -aioE 'art\.|ust\.|§|dz\.?\s*u\.' "$src_txt" | wc -l | tr -d ' ' || true)
  statute_hits_chunk=$(grep -aioE 'art\.|ust\.|§|dz\.?\s*u\.' "$chunks_txt" | wc -l | tr -d ' ' || true)

  # Header markers presence in chunks
  if grep -aq 'Sygn\. akt\|WYROK' "$chunks_txt"; then
    header_in_chunks=yes
  else
    header_in_chunks=no
  fi

  chunks_count=$(jq '(.chunks // []) | length' < "$struct_json")

  printf "%s\tok\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$pdf" "$src_tokens" "$chunk_tokens" "$tok_drift" "$src_chars" "$chunk_chars" "$chr_drift" \
    "$digits_src" "$digits_chunk" "$dig_drift" "$statute_hits_src" "$statute_hits_chunk" "$header_in_chunks" "$chunks_count" >> "$OUT"

  # Don’t hammer the API
  sleep 0.2
done

echo "Wrote: $OUT"
