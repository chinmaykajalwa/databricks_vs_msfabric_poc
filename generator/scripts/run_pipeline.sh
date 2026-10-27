#!/usr/bin/env bash
# Run the whole data pipeline (steps 1-4), on-disk validation, and the step-5 dry-run gate.
#   bash scripts/run_pipeline.sh [--profile lite|baseline] [--out DIR]
# Fails fast: every step is itself gated and refuses to run on stale inputs.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE=lite
OUT=output
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
PY="${PYTHON:-python3}"
CSV=""
[ "$PROFILE" = "baseline" ] && CSV="--no-csv"   # baseline CSVs are large; Parquet is the platform format

step() {
  local name="$1"; shift
  local t0; t0=$(date +%s)
  echo "=== [$name] $* ==="
  "$@"
  echo "=== [$name] OK in $(( $(date +%s) - t0 ))s ==="
}

step master       "$PY" scripts/gen_master.py       --profile "$PROFILE" --out "$OUT" $CSV --cross-process
step transactions "$PY" scripts/gen_transactions.py --profile "$PROFILE" --out "$OUT" $CSV
step telemetry    "$PY" scripts/gen_history.py      --profile "$PROFILE" --out "$OUT" --no-csv
step documents    "$PY" scripts/gen_docs.py         --profile "$PROFILE" --out "$OUT"
step validate     "$PY" scripts/validate.py         --profile "$PROFILE" --out "$OUT" --digests
step dryrun       "$PY" scripts/publish_live.py     --profile "$PROFILE" --out "$OUT" --dry-run --duration-min 60 --time-scale 0
echo "PIPELINE OK profile=$PROFILE out=$OUT"
