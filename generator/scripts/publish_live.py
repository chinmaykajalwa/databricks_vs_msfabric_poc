#!/usr/bin/env python
"""
Step 5: live publisher (Azure Event Hubs, Kafka endpoint).

Dry run first -- no namespace, no credentials, no spend; measures cost and
runs the stream checks:
    python scripts/publish_live.py --dry-run --duration-min 60 --time-scale 0

Live demo (real time, 60 min, timestamps rebased to now):
    python scripts/publish_live.py --env-file /etc/p2p/publisher.env --duration-min 60

Env file (KEY=VALUE lines; parsed by this script, never `source`d):
    EVENTHUB_BOOTSTRAP=<namespace>.servicebus.windows.net:9093
    EVENTHUB_CONNECTION_STRING=Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...   (namespace-level, no EntityPath)

Each run writes output/ground_truth/live_run_<run_id>.json: the time offset,
story events in sim AND wall time, injected defects, the firmware tag map.
Score each platform against it. Do not load it into either platform.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.checks import format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import digests  # noqa: E402
from p2p_gen.publisher import (BILLING_UNIT, MAX_EVENT, TOPICS, DryRunSink, KafkaSink, LiveStream,  # noqa: E402
                               preflight, publish, utc_now_ms)
from p2p_gen.telemetry import ISO_VIBRATION_ALARM, MIN, Context  # noqa: E402
from p2p_gen.transactions import build_transactions  # noqa: E402
from p2p_gen.world import build_world  # noqa: E402


def read_env_file(path: str) -> dict:
    """KEY=VALUE per line, split on the FIRST '='. Never shell-sourced: the ';' inside an
    Event Hubs connection string would terminate the statement and truncate the secret."""
    env = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def stream_checks(stream: LiveStream, sink: DryRunSink, ctx: Context) -> list:
    res = []
    add = lambda n, ok, d="": res.append((n, bool(ok), d))  # noqa: E731
    recs, sizes = [], []
    for topic, key, body in sink.messages:
        sizes.append(len(body))
        env = json.loads(body)
        for r in env["records"]:
            recs.append((topic, key, r["t"], r["g"], r["v"], r["q"], "fw" in r))
    df = pd.DataFrame(recs, columns=["topic", "site", "t", "g", "v", "q", "fw"])
    add("every generated record published (+ duplicates)",
        len(df) == stream.n_generated + stream.truth["duplicate_records"], f"{len(df):,}")
    add("every message within one 64 KB billing unit", max(sizes) <= BILLING_UNIT, f"max {max(sizes):,} B")
    add("no message exceeds the 1 MB event limit", max(sizes) <= MAX_EVENT)
    t_by_tag = df.groupby("g")["t"].apply(lambda s: int((np.diff(s.to_numpy()) < 0).sum()))
    add("out-of-order records present (late delivery)", t_by_tag.sum() > 0, f"{int(t_by_tag.sum())} inversions")
    dup = df.duplicated(["g", "t", "v"]).sum()
    add("duplicate records present", dup >= stream.truth["duplicate_records"] * 0.9, f"{dup}")
    live = ctx.live
    m = ctx.cfg.story_minutes

    def tag_of(scn):
        return next(s for s in ctx.scenarios if s["scenario"] == scn)

    def first_alarm(scn, thr, sustain_min=0):
        """First time the alarm condition holds (value >= thr, sustained for sustain_min)."""
        s = tag_of(scn)
        d = df[df["g"] == s["tag_id"]].sort_values("t")
        t, v = d["t"].to_numpy(), d["v"].to_numpy(dtype=float)
        above = v >= thr
        for i in np.flatnonzero(above):
            j = np.searchsorted(t, t[i] + sustain_min * MIN)
            if j <= len(t) and (j == i + 1 or above[i:j].all()) and (sustain_min == 0 or j < len(t)):
                return (t[i] + sustain_min * MIN - live) / MIN
        return None

    for scn, thr, key, sustain, tol in (("rh_probe_failure", 80, "rh_probe_failure", 0, 1),
                                        ("bearing_degradation", ISO_VIBRATION_ALARM, "sorter_vibration_alarm", 0, 3),
                                        ("reefer_failure", 20, "reefer_failure", 15, 2)):
        at = first_alarm(scn, thr, sustain)
        add(f"story: {scn} first alarms at minute {m[key]} (+/-{tol})", at is not None and abs(at - m[key]) <= tol,
            f"first alarm at minute {at:.1f}" if at is not None else "never")
    o = tag_of("dc_outage")
    silent = df[(df["site"] == o["asset_id"]) & (df["topic"] == "dc-asset-telemetry")
                & (df["t"] >= o["start_ms"]) & (df["t"] < o["end_ms"])]
    add(f"story: {o['asset_id']} silent from minute {m['dc_outage']}", silent.empty, f"{len(silent)} records")
    after = df[df["t"] >= stream.fw_t]
    add("firmware 4.3: old melt-temp tag IDs stop on upgraded machines",
        not after["g"].isin(list(stream.fw_map)).any())
    add("firmware 4.3: new tag IDs appear, unknown to tag_dim, carrying 'fw'",
        after["g"].isin(list(stream.fw_map.values())).any() and not set(stream.fw_map.values()) & set(ctx.tdim["tag_id"])
        and after[after["g"].isin(list(stream.fw_map.values()))]["fw"].all())
    sk = df[(df["site"] == stream.skew_site) & (df["topic"] == "dc-asset-telemetry")]
    add(f"clock skew: {stream.skew_site} reports time ahead", len(sk) > 0)
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    ap.add_argument("--out", default="output")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--duration-min", type=float, default=60)
    ap.add_argument("--from-minute", type=float, default=0, help="Start the story clock later (e.g. resume).")
    ap.add_argument("--time-scale", type=float, default=1.0, help="Plant seconds per wall second; 0 = flat out.")
    ap.add_argument("--flush-ms", type=int, default=5000, help="Plant-time flush window: latency vs cost.")
    ap.add_argument("--topics", nargs="+", default=list(TOPICS), choices=TOPICS)
    ap.add_argument("--rebase", dest="rebase", action="store_true", default=None,
                    help="Shift record times so live start = now (default for live runs).")
    ap.add_argument("--no-rebase", dest="rebase", action="store_false")
    ap.add_argument("--no-preflight", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(message)s")

    cfg = P2PConfig(profile=a.profile, seed=a.seed, out_root=a.out)
    t0 = time.monotonic()
    world = build_world(cfg)
    tx, _ = build_transactions(cfg, world)
    man = Path(cfg.out_root) / "transactions" / "_manifest.json"
    if man.exists():
        on_disk = {k: v["digest"] for k, v in json.loads(man.read_text())["tables"].items()}
        if any(on_disk.get(k) != d for k, d in digests(tx).items()):
            print("[p2p] ABORT: output/transactions is stale; re-run steps 1-4.", file=sys.stderr)
            return 4
    ctx = Context(cfg, world, tx)
    t_start = ctx.live + int(a.from_minute * MIN)
    t_end = t_start + int(a.duration_min * MIN)
    stream = LiveStream(ctx, t_start, t_end, topics=a.topics)
    print(f"[p2p] stream: {stream.n_generated:,} records + {stream.truth['duplicate_records']} duplicates, "
          f"{stream.truth['late_records']} late, {a.duration_min:g} plant-min ({time.monotonic() - t0:.0f}s to build)")

    rebase = (not a.dry_run) if a.rebase is None else a.rebase
    wall0 = utc_now_ms()
    offset = (wall0 - t_start) if rebase else 0
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + ("-dry" if a.dry_run else "")
    gt = Path(cfg.out_root) / "ground_truth"
    gt.mkdir(parents=True, exist_ok=True)
    ref = Path(cfg.out_root) / "reference"
    ref.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(stream.tag_dim_fw43()).to_csv(ref / "tag_dim_fw43.csv", index=False)

    def wall(ms):
        return pd.Timestamp(int(ms + offset), unit="ms", tz="UTC").isoformat()

    story = [{"anomaly_id": s["anomaly_id"], "scenario": s["scenario"], "asset_id": s["asset_id"], "tag_id": s["tag_id"],
              "alarm_sim": pd.Timestamp(s["alarm_ms"], unit="ms", tz="UTC").isoformat(), "alarm_record_time": wall(s["alarm_ms"]),
              "start_record_time": wall(s["start_ms"])}
             for s in ctx.scenarios if s["window"] != "history" and s["alarm_ms"] and t_start <= s["alarm_ms"] < t_end]
    run = {"run_id": run_id, "dry_run": a.dry_run, "profile": cfg.profile, "topics": a.topics,
           "sim_window": [pd.Timestamp(t_start, unit="ms", tz="UTC").isoformat(), pd.Timestamp(t_end, unit="ms", tz="UTC").isoformat()],
           "record_time_offset_ms": offset, "rebased": rebase, "time_scale": a.time_scale, "flush_ms": a.flush_ms,
           "story_events": story, "injected": stream.manifest()}
    run_path = gt / f"live_run_{run_id}.json"
    run_path.write_text(json.dumps(run, indent=2, default=str))
    print(f"[p2p] run ground truth -> {run_path}")

    if a.dry_run:
        sink = DryRunSink()
    else:
        env = {**os.environ, **(read_env_file(a.env_file) if a.env_file else {})}
        metrics = {"send_failures": 0}
        sink = KafkaSink(env["EVENTHUB_BOOTSTRAP"], env["EVENTHUB_CONNECTION_STRING"], metrics)
        if not a.no_preflight:
            preflight(sink, a.topics)
            print("[p2p] preflight OK: all hubs reachable")
    summary = publish(stream, sink, time_scale=a.time_scale, flush_ms=a.flush_ms, offset_ms=offset)
    run["summary"] = summary
    ok = True
    if a.dry_run:
        res = stream_checks(stream, sink, ctx)
        print("[p2p] stream checks:\n" + format_results(res))
        run["checks"] = [{"name": n, "ok": o, "detail": d} for n, o, d in res]
        ok = all(o for _, o, _ in res)
        samples = Path(cfg.out_root) / "live_samples"
        samples.mkdir(exist_ok=True)
        seen = set()
        for topic, key, body in sink.messages:
            if topic not in seen:
                seen.add(topic)
                (samples / f"{topic}.json").write_text(json.dumps(json.loads(body), indent=1)[:4000])
    run_path.write_text(json.dumps(run, indent=2, default=str))
    s = summary
    print(f"[p2p] {s['records']:,} records in {s['messages']:,} messages ({s['billable_units']:,} billable units, "
          f"{s['billing_unit_utilisation_pct']}% unit utilisation), ~{s['records_per_plant_sec']} records/s")
    print(f"[p2p] projected {s['projected_billable_events_per_month']:,} billable events/month; peak "
          f"{s['peak_ingress_events_per_s']} events/s, {s['peak_ingress_mb_per_s']} MB/s -> "
          f"{s['throughput_units_needed']} throughput unit(s); median latency {s['median_plant_latency_ms'] / 1000:.1f} s")
    return 0 if ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
