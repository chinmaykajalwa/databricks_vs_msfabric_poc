#!/usr/bin/env bash
# Upload generated data to ADLS Gen2 with the VM's managed identity (no keys on the VM).
#   upload.sh all          landing data + ground truth
#   upload.sh landing      batch data the platforms load
#   upload.sh groundtruth  scoring files only (also run after every live publisher run)
#
# Ground truth goes to a SEPARATE container on purpose: grant the platforms access to
# `landing` only, so no agent can read the answer key.
set -euo pipefail
conf() { sed -n "s/^$1=//p" /etc/p2p/setup.conf | tail -1; }
WHAT="${1:-all}"
ACCT=$(conf STORAGE_ACCOUNT)
LANDING=$(conf LANDING_CONTAINER)
GT=$(conf GROUNDTRUTH_CONTAINER)
PROFILE=$(conf DATA_PROFILE); PROFILE=${PROFILE:-lite}
OUT=/opt/p2p/output/$PROFILE
BASE="https://$ACCT.blob.core.windows.net"
[ -n "$ACCT" ] || { echo "[upload] no STORAGE_ACCOUNT configured; skipping"; exit 0; }
export AZCOPY_AUTO_LOGIN_TYPE=MSI
export AZCOPY_LOG_LOCATION=/var/log/azcopy

sync_dir() {  # $1 local dir, $2 container, $3 remote path
  [ -d "$1" ] || { echo "[upload] $1 missing; skipped"; return 0; }
  # Role assignments take minutes to propagate after the VM is created; retry instead of failing.
  for i in 1 2 3 4 5 6 7 8 9 10; do
    if azcopy sync "$1" "$BASE/$2/$3" --recursive --delete-destination=true --log-level=ERROR >/dev/null; then
      echo "[upload] $1 -> $2/$3"; return 0
    fi
    echo "[upload] attempt $i failed (RBAC may still be propagating); retrying in 60 s" >&2
    sleep 60
  done
  echo "[upload] FAILED: $1 -> $2/$3" >&2
  return 1
}

if [ "$WHAT" = "all" ] || [ "$WHAT" = "landing" ]; then
  for d in master transactions telemetry documents reference live_samples; do
    sync_dir "$OUT/$d" "$LANDING" "p2p/$PROFILE/$d"
  done
fi
if [ "$WHAT" = "all" ] || [ "$WHAT" = "groundtruth" ]; then
  sync_dir "$OUT/ground_truth" "$GT" "p2p/$PROFILE/ground_truth"
fi
