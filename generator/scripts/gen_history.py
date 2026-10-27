#!/usr/bin/env python
"""
Step 3: telemetry history + reference dims + ground truth.

    python scripts/gen_history.py            # lite: 30 days of aggregates + last 2 days raw

Writes
  output/reference/asset_dim, tag_dim                    (load into both platforms)
  output/telemetry/agg/topic=<t>/date=<d>/part.parquet   (1-min; 5-min for DC assets)
  output/telemetry/raw/topic=<t>/date=<d>/part.parquet   (last N days, full rate, incl. duplicates)
  output/ground_truth/anomaly_log, hero_true_rh_by_load, hero_blast_radius.json (+ confirmed lots)
Ground truth is for scoring the platforms. Do not load it.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.blast_radius import hero_blast_radius, write_ground_truth  # noqa: E402
from p2p_gen.checks import format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import digests, rng_for  # noqa: E402
from p2p_gen.telemetry import (AGG_MS, BAD, CHUNK, DAYMS, HOUR, ISO_VIBRATION_ALARM, MIN, QUALITY_NAMES,  # noqa: E402
                               Context, aggregate, generate, steriliser_profile)
from p2p_gen.transactions import build_transactions  # noqa: E402
from p2p_gen.world import build_world  # noqa: E402
from p2p_gen.writers import write_data_dictionary, write_manifest, write_tables  # noqa: E402


def _ts(ms):
    return pa.array(np.asarray(ms, dtype="int64"), type=pa.int64()).cast(pa.timestamp("ms", tz="UTC"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    ap.add_argument("--out", default="output")
    ap.add_argument("--no-csv", action="store_true")
    ap.add_argument("--days", type=int, default=None, help="Only the last N history days (quick test).")
    a = ap.parse_args(argv)
    cfg = P2PConfig(profile=a.profile, seed=a.seed, out_root=a.out, write_csv=not a.no_csv)
    out = Path(cfg.out_root)
    t0 = time.monotonic()
    print(f"[p2p] step 3 profile={cfg.profile}")

    world = build_world(cfg)
    tx, _ = build_transactions(cfg, world)
    man = out / "transactions" / "_manifest.json"
    if man.exists():
        on_disk = {k: v["digest"] for k, v in json.loads(man.read_text())["tables"].items()}
        stale = sorted(k for k, d in digests(tx).items() if on_disk.get(k) != d)
        if stale:
            print(f"[p2p] ABORT: output/transactions is stale for {stale}; re-run steps 1-2.", file=sys.stderr)
            return 4
        print("[p2p] transactions on disk match this code + seed")
    ctx = Context(cfg, world, tx)
    print(f"[p2p] {len(ctx.adim)} assets, {len(ctx.tdim)} tags, {len(ctx.scenarios)} scenarios "
          f"({time.monotonic() - t0:.0f}s)")

    # ---- generation loop ---------------------------------------------------
    days = cfg.p.history_days if a.days is None else a.days
    first_day = ctx.live - days * DAYMS
    raw_from = ctx.live - cfg.p.raw_tail_days * DAYMS
    hot_tags = {s["tag_id"] for s in ctx.scenarios if s["tag_id"]} | \
        set(ctx.tdim[ctx.tdim["asset_id"].isin({s["asset_id"] for s in ctx.scenarios})]["tag_id"])
    tvalid = ctx.tdim.set_index("tag_id")[["min_valid", "max_valid"]]
    lo_v, hi_v = tvalid["min_valid"].to_dict(), tvalid["max_valid"].to_dict()
    stats = defaultdict(int)
    cadence = defaultdict(list)
    sizes = defaultdict(int)
    assets = list(ctx.adim[["asset_id", "asset_kind", "topic"]].itertuples(index=False))
    for day in range(first_day, ctx.live, DAYMS):
        date = pd.Timestamp(day, unit="ms", tz="UTC").date()
        agg = defaultdict(lambda: defaultdict(list))
        raw = defaultdict(lambda: defaultdict(list))
        for c0 in range(day, day + DAYMS, CHUNK):
            for asset_id, kind, topic in assets:
                for tag, (tt, v, q) in generate(ctx, asset_id, kind, c0, c0 + CHUNK).items():
                    r = aggregate(tt, v, q, AGG_MS[topic])
                    if r is None:
                        continue
                    b, n, mean, mn, mx, last, nbad = r
                    A = agg[topic]
                    A["bucket_ts"].append(b); A["tag_id"].append(np.full(len(b), tag, np.int32))
                    A["n"].append(n); A["mean"].append(mean); A["min"].append(mn); A["max"].append(mx)
                    A["last"].append(last); A["n_bad"].append(nbad)
                    if tag not in hot_tags:
                        ok = np.isnan(mn) | ((mn >= lo_v[tag]) & (mx <= hi_v[tag]))
                        stats["range_rows"] += len(ok); stats["range_ok"] += int(ok.sum())
                    if len(cadence[tag]) < 3:
                        cadence[tag].append(float(np.median(n)))
                    if c0 >= raw_from:
                        R = raw[topic]
                        R["ts"].append(tt); R["tag_id"].append(np.full(len(tt), tag, np.int32))
                        R["value"].append(v); R["quality"].append(q)
        for topic, A in agg.items():
            cols = {k: np.concatenate(v) for k, v in A.items()}
            o = np.lexsort((cols["tag_id"], cols["bucket_ts"]))
            tbl = pa.table({"bucket_ts": _ts(cols["bucket_ts"][o]), "tag_id": cols["tag_id"][o],
                            "n": cols["n"][o].astype(np.int32), "mean": cols["mean"][o].astype(np.float32),
                            "min": cols["min"][o].astype(np.float32), "max": cols["max"][o].astype(np.float32),
                            "last": cols["last"][o].astype(np.float32), "n_bad": cols["n_bad"][o].astype(np.int32)})
            p = out / "telemetry" / "agg" / f"topic={topic}" / f"date={date}"
            p.mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, p / "part.parquet", compression="zstd")
            sizes[f"agg/{topic}"] += tbl.num_rows
        for topic, R in raw.items():
            cols = {k: np.concatenate(v) for k, v in R.items()}
            o = np.lexsort((cols["tag_id"], cols["ts"]))
            cols = {k: v[o] for k, v in cols.items()}
            # duplicate delivery: ~0.1% of rows re-sent (a real gateway/at-least-once artefact)
            rng = rng_for(cfg.seed, "tel", "dup", topic, str(date))
            dup = np.sort(rng.choice(len(cols["ts"]), size=max(1, len(cols["ts"]) // 1000), replace=False))
            for k in cols:
                cols[k] = np.insert(cols[k], dup + 1, cols[k][dup])
            stats[f"duplicates/{topic}"] += len(dup)
            tbl = pa.table({"ts": _ts(cols["ts"]), "tag_id": cols["tag_id"],
                            "value": cols["value"].astype(np.float32),
                            "quality": pa.array(QUALITY_NAMES[cols["quality"]]).dictionary_encode()})
            p = out / "telemetry" / "raw" / f"topic={topic}" / f"date={date}"
            p.mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, p / "part.parquet", compression="zstd")
            sizes[f"raw/{topic}"] += tbl.num_rows
        print(f"  {date}  ({time.monotonic() - t0:.0f}s)", flush=True)

    # ---- reference + ground truth ----------------------------------------
    info = write_tables(cfg, {"asset_dim": ctx.adim, "tag_dim": ctx.tdim}, "reference")
    log = pd.DataFrame(ctx.scenarios)
    for c in ("start_ms", "end_ms", "alarm_ms"):
        log[c.replace("_ms", "_ts")] = pd.to_datetime(log[c], unit="ms", utc=True)
    log = log.drop(columns=["start_ms", "end_ms", "alarm_ms"])
    gt = out / "ground_truth"
    gt.mkdir(parents=True, exist_ok=True)
    log.to_parquet(gt / "anomaly_log.parquet", index=False)
    log.to_csv(gt / "anomaly_log.csv", index=False)

    S = ctx.STER[ctx.hero]
    rows = []
    for i in range(len(S["s"])):
        if S["ce"][i] < ctx.drift_start - 2 * DAYMS:
            continue
        te = np.arange(S["pe"][i], min(S["ee"][i], S["ce"][i]), 10_000, dtype=np.int64)
        tp = np.arange(S["s"][i] + 20 * MIN, S["s"][i] + 60 * MIN, 5000, dtype=np.int64)
        P, Pp = steriliser_profile(ctx, ctx.hero, te), steriliser_profile(ctx, ctx.hero, tp)
        rows.append({"load_id": S["load"][i], "start_ts": pd.Timestamp(int(S["s"][i]), unit="ms", tz="UTC"),
                     "in_drift_window": bool(S["s"][i] >= ctx.drift_start),
                     "true_rh_min_exposure": round(float(P["rh"].min()), 2),
                     "reported_rh_mean_exposure": round(float((P["rh"] + ctx.hero_drift(te)).mean()), 2),
                     "steam_kg_h_mean_conditioning": round(float(Pp["steam"].mean()), 2),
                     "out_of_spec_true_rh_below_50": bool(P["rh"].min() < 50)})
    hero_df = pd.DataFrame(rows)
    hero_df.to_csv(gt / "hero_true_rh_by_load.csv", index=False)
    br = hero_blast_radius(cfg, world, tx, ctx.hero)
    oos = set(hero_df.loc[hero_df["out_of_spec_true_rh_below_50"], "load_id"])
    lc = tx["load_contents"]
    br["confirmed_out_of_spec"] = {
        "loads": sorted(oos), "lots": sorted(lc[lc["load_id"].isin(oos)]["lot_id"].dropna().unique().tolist()),
        "note": "True RH < 50% validated minimum during exposure. Only inferable from steam consumption; "
                "reported RH looked normal. 'suspect_*' = everything since drift onset (what QA must hold)."}
    write_ground_truth(cfg, br)

    # ---- checks ------------------------------------------------------------
    res = []
    add = lambda n, ok, d="": res.append((n, bool(ok), d))  # noqa: E731
    td = ctx.tdim
    add("tag_dim: tag_id and tag_name unique", not td["tag_id"].duplicated().any() and not td["tag_name"].duplicated().any())
    add("every asset has tags", set(ctx.adim["asset_id"]) <= set(td["asset_id"]))
    probe = {}
    for kind in ("molding_machine", "sterilizer", "dc_asset", "vehicle"):
        aid = ctx.adim[ctx.adim["asset_kind"] == kind]["asset_id"].iloc[0]
        x, y = generate(ctx, aid, kind, ctx.live - CHUNK, ctx.live), generate(ctx, aid, kind, ctx.live - CHUNK, ctx.live)
        probe[kind] = all(np.array_equal(x[k][1], y[k][1], equal_nan=True) and np.array_equal(x[k][0], y[k][0]) for k in x)
    add("chunk regeneration is identical (batch == live publisher)", all(probe.values()), str(probe))
    add("aggregate min/max within valid range (non-scenario tags) >= 99.9%",
        stats["range_ok"] >= 0.999 * stats["range_rows"], f"{stats['range_ok']}/{stats['range_rows']}")
    melt = td[td["signal"] == "melt_temp_c"]["tag_id"]
    med = np.median([np.median(cadence[t]) for t in melt if cadence[t]])
    add("5 s tags deliver 12 samples per minute", med == 12, f"median {med}")
    done = hero_df[hero_df["in_drift_window"] & (hero_df["start_ts"] < pd.Timestamp(cfg.live_start) - pd.Timedelta(hours=12))]
    add("hero: reported RH looks normal on every suspect load (62-68%)",
        done["reported_rh_mean_exposure"].between(62, 68).all(), f"{done['reported_rh_mean_exposure'].min()}-{done['reported_rh_mean_exposure'].max()}")
    drop = 1 - done["steam_kg_h_mean_conditioning"].iloc[-1] / hero_df[~hero_df["in_drift_window"]]["steam_kg_h_mean_conditioning"].mean()
    add("hero: steam consumption reveals the drift (>= 15% drop)", drop >= 0.15, f"{drop:.0%}")
    add("hero: confirmed out-of-spec loads exist and are a subset of suspect loads",
        0 < len(oos) and oos <= set(br["suspect_loads"]) | set(hero_df["load_id"]), f"{len(oos)} of {len(br['suspect_loads'])}")
    # live scenarios fire at their story-clock times
    def two_chunks(a_, k):  # last history chunk + first live chunk, stitched
        x, y = generate(ctx, a_, k, ctx.live - CHUNK, ctx.live), generate(ctx, a_, k, ctx.live, ctx.live + CHUNK)
        return {t: tuple(np.concatenate([x[t][i], y[t][i]]) for i in range(3)) for t in y}

    live = {a_: two_chunks(a_, k) for a_, k in
            [(ctx.hero, "sterilizer")] + [(s["asset_id"], "dc_asset") for s in ctx.scenarios if s["scenario"] == "bearing_degradation"]
            + [(s["asset_id"], "vehicle") for s in ctx.scenarios if s["scenario"] == "reefer_failure"]}
    for s in ctx.scenarios:
        if s["window"] == "history" or s["scenario"] == "rh_probe_drift":
            continue
        if s["scenario"] == "dc_outage":
            dca = ctx.adim[(ctx.adim["site_id"] == s["asset_id"]) & (ctx.adim["asset_kind"] == "dc_asset")]["asset_id"].iloc[0]
            g = generate(ctx, dca, "dc_asset", ctx.live, ctx.live + CHUNK)
            silent = all(not ((tt >= s["start_ms"]) & (tt < s["end_ms"])).any() for tt, _, _ in g.values())
            add("live: DC outage silences the site", silent)
            continue
        tt, v, q = live[s["asset_id"]][s["tag_id"]]
        w = (tt >= s["alarm_ms"]) & (tt < s["alarm_ms"] + 5 * MIN)
        thr = {"rh_probe_failure": 80, "bearing_degradation": ISO_VIBRATION_ALARM, "reefer_failure": 20}[s["scenario"]]
        before = (tt >= s["alarm_ms"] - 30 * MIN) & (tt < s["alarm_ms"] - 20 * MIN)
        add(f"live: {s['scenario']} crosses its alarm at story time, not before",
            w.any() and np.nanmax(v[w]) >= thr and np.nanmax(v[before]) < thr,
            f"max at alarm {np.nanmax(v[w]):.1f}, before {np.nanmax(v[before]):.1f}, thr {thr}")
    # history precursors visible
    vis, tot = 0, 0
    for s in ctx.scenarios:
        if s["category"] != "process" or s["window"] != "history" or s["asset_id"] not in ctx.MOLD:
            continue
        c0 = (s["alarm_ms"] // CHUNK) * CHUNK
        tt, v, _ = generate(ctx, s["asset_id"], "molding_machine", c0, c0 + CHUNK)[s["tag_id"]]
        base = generate(ctx, s["asset_id"], "molding_machine", c0, c0 + CHUNK)
        M = ctx.MOLD[s["asset_id"]]
        k = np.searchsorted(M["ws"], s["alarm_ms"], "right") - 1
        sp = {"melt_temp_c": M["melt"][k], "mold_temp_c": M["mold"][k], "inj_pressure_peak_bar": M["inj"][k]}[s["signal"]]
        w = (tt >= s["alarm_ms"]) & (tt < s["end_ms"])
        if w.any():
            tot += 1
            vis += abs(np.nanmean(v[w]) - sp) > {"melt_temp_c": 6, "mold_temp_c": 3, "inj_pressure_peak_bar": 0.03 * sp}[s["signal"]]
    add("history: molding precursors visible vs setpoint before the deviation", tot == 0 or vis >= 0.8 * tot, f"{vis}/{tot}")
    srt = [s for s in ctx.scenarios if s["scenario"] == "bearing_degradation"][0]
    tag = srt["tag_id"]
    agg_dir = out / "telemetry" / "agg" / "topic=dc-asset-telemetry"
    vib = pd.concat([pd.read_parquet(p, filters=[("tag_id", "==", tag)]).assign(date=p.parent.name[5:])
                     for p in sorted(agg_dir.glob("date=*/part.parquet"))])
    dm = vib[vib["n"] > 0].groupby("date")["mean"].mean()
    add("history: sorter vibration trend visible before the live alarm, never alarmed in history",
        dm.iloc[-1] - dm.iloc[0] > 0.8 and vib["max"].max() < ISO_VIBRATION_ALARM,
        f"first day {dm.iloc[0]:.2f} -> last day {dm.iloc[-1]:.2f}, max {vib['max'].max():.2f}")
    sh = tx["shipments"].merge(tx["customer_orders"][["order_id", "customer_id"]]).merge(world["customers"][["customer_id", "lat", "lon"]])
    sh = sh.merge(world["routes"][["route_id", "vehicle_id"]]).sample(60, random_state=1)
    near = 0
    for _, r in sh.iterrows():
        F = ctx.FLEET[r["vehicle_id"]]
        tms = (r["delivered_ts"] - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(milliseconds=1) + 60_000
        near += abs(np.interp(tms, F["t"], F["lat"]) - r["lat"]) < 0.01 and abs(np.interp(tms, F["t"], F["lon"]) - r["lon"]) < 0.01
    add("fleet: truck is at the customer when the delivery is recorded", near >= 57, f"{near}/60")

    print("[p2p] checks:\n" + format_results(res))
    write_manifest(cfg, "telemetry", {"days": days, "raw_tail_days": cfg.p.raw_tail_days, "rows": dict(sizes),
                                      "stats": dict(stats), "reference": info,
                                      "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in res]}) \
        if (out / "telemetry").exists() else None
    write_data_dictionary(cfg, {"asset_dim": ctx.adim, "tag_dim": ctx.tdim}, "reference",
                          {"asset_dim": "Every telemetry-emitting asset, its site and Event Hubs topic.",
                           "tag_dim": "One row per sensor tag: unit, sampling, validity bounds, max rate of change, frozen threshold, criticality. "
                                      "The lean stream carries only (t, tag_id, value, quality); join this for everything else."})
    print(f"[p2p] rows: {dict(sizes)}")
    hero = br["confirmed_out_of_spec"]
    print(f"[p2p] hero: {len(br['suspect_loads'])} suspect loads, {len(hero['loads'])} confirmed out of spec "
          f"({len(hero['lots'])} lots); done in {time.monotonic() - t0:.0f}s")
    return 0 if all(ok for _, ok, _ in res) else 3


if __name__ == "__main__":
    raise SystemExit(main())
