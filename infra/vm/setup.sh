#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# P2P control tower: VM setup (runs from the cloned repo; called by bootstrap.sh)
# ---------------------------------------------------------------------------
#   1. venv + install the generator
#   2. run the data pipeline (steps 1-4 + validation + dry-run gate)
#   3. upload batch data to ADLS Gen2 (landing) and ground truth (separate container)
#   4. register the live publisher as a systemd unit (started manually for the demo)
#
# Settings come from /etc/p2p/setup.conf (non-secret, written by cloud-init).
# The Event Hubs connection string lives only in /etc/p2p/publisher.env (0600)
# and is read by publish_live.py itself -- never shell-sourced, because the ';'
# in a connection string would truncate it.
#
# Lessons carried over from the Novelis VM: no top-level `set -e` (one failing
# stage must not hide the others); each stage runs in a subshell WITH `set -e`
# and records a status; a single SUMMARY line states what actually happened;
# non-zero exit if anything requested failed, so `cloud-init status` is truthful.
set -uo pipefail

conf() {  # read KEY from a KEY=VALUE file without shell-sourcing it
  sed -n "s/^$1=//p" "${2:-/etc/p2p/setup.conf}" | tail -1
}

ROOT=/opt/p2p
REPO=$ROOT/repo
SUBDIR=$(conf GENERATOR_SUBDIR); SUBDIR=${SUBDIR:-generator}
GEN=$REPO/$SUBDIR
VENV=$ROOT/venv
PROFILE=$(conf DATA_PROFILE); PROFILE=${PROFILE:-lite}
OUT=$ROOT/output/$PROFILE
RUN_PIPELINE=$(conf RUN_PIPELINE)
UPLOAD=$(conf UPLOAD_TO_STORAGE)
AUTOSTART=$(conf PUBLISHER_AUTOSTART)
DUR=$(conf PUBLISHER_DURATION_MIN)
TS=$(conf PUBLISHER_TIME_SCALE)
FLUSH=$(conf PUBLISHER_FLUSH_MS)

S_INSTALL=skipped; S_PIPELINE=skipped; S_UPLOAD=skipped; S_UNIT=skipped
echo "=== p2p setup starting $(date -u) profile=$PROFILE commit=$(cat $ROOT/COMMIT 2>/dev/null) ==="

# ---- 1. install -----------------------------------------------------------
( set -e
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet -e "$GEN"
  "$VENV/bin/python" -c "import p2p_gen, kafka; print('[install] p2p_gen', p2p_gen.__version__)"
) && S_INSTALL=ok || S_INSTALL=FAILED
echo "[install] status: $S_INSTALL"

# ---- 2. pipeline ----------------------------------------------------------
if [ "$RUN_PIPELINE" = "true" ] && [ "$S_INSTALL" = "ok" ]; then
  ( set -e
    mkdir -p "$OUT"
    PYTHON="$VENV/bin/python" bash "$GEN/scripts/run_pipeline.sh" --profile "$PROFILE" --out "$OUT"
  ) && S_PIPELINE=ok || S_PIPELINE=FAILED
fi
echo "[pipeline] status: $S_PIPELINE"

# ---- 3. upload ------------------------------------------------------------
if [ "$UPLOAD" = "true" ] && [ "$S_PIPELINE" = "ok" ]; then
  ( set -e; bash "$REPO/infra/vm/upload.sh" all ) && S_UPLOAD=ok || S_UPLOAD=FAILED
fi
echo "[upload] status: $S_UPLOAD"

# ---- 4. publisher unit ----------------------------------------------------
# Restart=no on purpose: a restart would replay the 60-minute story from minute
# 0 with fresh timestamps, which looks like a second incident on the dashboards.
# Re-run deliberately with `systemctl start p2p-publisher`.
if [ "$S_INSTALL" = "ok" ]; then
  ( set -e
    cat > /etc/systemd/system/p2p-publisher.service <<UNIT
[Unit]
Description=P2P control tower live publisher (Event Hubs), ${DUR}-minute story run
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$GEN
Environment=PYTHONUNBUFFERED=1
ExecStart=$VENV/bin/python scripts/publish_live.py --profile $PROFILE --out $OUT --env-file /etc/p2p/publisher.env --duration-min $DUR --time-scale $TS --flush-ms $FLUSH
ExecStopPost=/bin/bash $REPO/infra/vm/upload.sh groundtruth
Restart=no
StandardOutput=journal
StandardError=journal
UNIT
    systemctl daemon-reload
    if [ "$AUTOSTART" = "true" ] && [ "$S_PIPELINE" = "ok" ]; then
      systemctl start p2p-publisher.service
      echo "[unit] publisher started (autostart)"
    else
      echo "[unit] publisher registered; start the demo with: sudo systemctl start p2p-publisher"
    fi
  ) && S_UNIT=ok || S_UNIT=FAILED
fi
echo "[unit] status: $S_UNIT"

echo
echo "=================================================================="
echo " SETUP SUMMARY  install=$S_INSTALL pipeline=$S_PIPELINE upload=$S_UPLOAD unit=$S_UNIT profile=$PROFILE"
echo "=================================================================="
echo "=== p2p setup complete $(date -u) ==="
for s in "$S_INSTALL" "$S_PIPELINE" "$S_UPLOAD" "$S_UNIT"; do
  [ "$s" = "FAILED" ] && { echo "ERROR: at least one stage failed; see above." >&2; exit 1; }
done
exit 0
