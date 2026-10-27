#!/usr/bin/env python
"""
Step 4: documents + calibration records.

    python scripts/gen_docs.py

Writes output/documents/{sops,manuals,deviation_reports,supplier_notices}/*.md,
docs_index, records/calibration_records, and ground_truth/{rag_eval.jsonl,
prompt_injection.json}. Refuses to run if steps 1-3 outputs are stale.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.checks import format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import digests  # noqa: E402
from p2p_gen.docs import INJECTION_TEXT, DocBuilder, rag_eval_set, write_docs  # noqa: E402
from p2p_gen.telemetry import Context  # noqa: E402
from p2p_gen.transactions import build_transactions  # noqa: E402
from p2p_gen.world import build_world  # noqa: E402
from p2p_gen.writers import write_data_dictionary, write_manifest  # noqa: E402


def build(cfg):
    world = build_world(cfg)
    tx, _ = build_transactions(cfg, world)
    ctx = Context(cfg, world, tx)
    log = pd.DataFrame(ctx.scenarios)
    log["start_ts"] = pd.to_datetime(log["start_ms"], unit="ms", utc=True)
    b = DocBuilder(cfg, world, tx, log, ctx.tdim)
    cal = b.build()
    return world, tx, ctx, b, cal


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    ap.add_argument("--out", default="output")
    a = ap.parse_args(argv)
    cfg = P2PConfig(profile=a.profile, seed=a.seed, out_root=a.out)
    t0 = time.monotonic()
    world, tx, ctx, b, cal = build(cfg)
    man = Path(cfg.out_root) / "transactions" / "_manifest.json"
    if man.exists():
        on_disk = {k: v["digest"] for k, v in json.loads(man.read_text())["tables"].items()}
        if any(on_disk.get(k) != d for k, d in digests(tx).items()):
            print("[p2p] ABORT: output/transactions is stale; re-run steps 1-3.", file=sys.stderr)
            return 4
    idx = write_docs(cfg, b, cal)

    res = []
    add = lambda n, ok, d="": res.append((n, bool(ok), d))  # noqa: E731
    _, _, _, b2, cal2 = build(cfg)
    add("deterministic: identical documents and calibration records on rebuild",
        [d["text"] for d in b.docs] == [d["text"] for d in b2.docs] and cal.equals(cal2))
    ids = set(idx["doc_id"])
    add("doc_id unique", not idx["doc_id"].duplicated().any())
    devs = set(tx["deviations"]["deviation_id"])
    add("every deviation has a report", {f"DR-{d}" for d in devs} <= ids, f"{len(devs)} deviations")
    known = ids | devs | set(tx["capas"]["capa_id"])
    refs = {r for x in idx["related_ids"] for r in x.split(",") if r}
    add("every cross-reference resolves", refs <= known, str(sorted(refs - known)[:3]))
    assets = {a_ for x in idx["asset_ids"] for a_ in x.split(",") if a_}
    known_assets = (set(ctx.adim["asset_id"]) | set(world["production_lines"]["line_id"])
                    | set(tx["component_lots"]["component_lot_id"]) | set(world["suppliers"]["supplier_id"]))
    add("every referenced asset exists", assets <= known_assets, str(sorted(assets - known_assets)[:3]))
    text = {d["doc_id"]: d["text"] for d in b.docs}
    misses = [q["question_id"] for q in rag_eval_set(b)
              if not set(q["expected_doc_ids"]) <= ids or not any(q["must_contain"] in text[x] for x in q["expected_doc_ids"])]
    add("RAG eval: expected docs exist and contain the answer fact", not misses, str(misses))
    add("exactly one prompt-injection test document", sum(INJECTION_TEXT in t for t in text.values()) == 1)
    h = world["sterilizers"].set_index("sterilizer_id").loc[b.hero]
    add("SOP-STR-201 ranges match the sterilizers table",
        f"{h['exposure_rh_low_pct']:.0f}-{h['exposure_rh_high_pct']:.0f} %RH" in text["SOP-STR-201"]
        and f"{h['eto_conc_low_mg_l']:.0f}-{h['eto_conc_high_mg_l']:.0f} mg/L" in text["SOP-STR-201"])
    rh = cal[(cal["asset_id"] == b.hero) & (cal["instrument"] == "chamber_rh_pct")].sort_values("performed_ts")
    drift = pd.Timestamp(cfg.live_start) - pd.Timedelta(days=cfg.hero_drift_days)
    last = rh[rh["performed_ts"] < drift].iloc[-1]
    add("hero: last RH verification before drift onset PASSED, within 12 h of onset",
        last["result"] == "PASS" and drift - last["performed_ts"] <= pd.Timedelta(hours=12),
        f"{last['result']} at {last['performed_ts']}")
    fails = rh[rh["result"] == "FAIL"]
    add("hero: failed verifications match the archive precedents",
        set(fails["deviation_id"]) == set(b.precedents["deviation_id"]) and len(fails) == len(b.precedents))
    add("hero: no verification between drift onset and live start (window is derivable, not given)",
        not ((rh["performed_ts"] >= drift) & (rh["performed_ts"] < pd.Timestamp(cfg.live_start))).any())
    gt = json.loads((Path(cfg.out_root) / "ground_truth" / "hero_blast_radius.json").read_text())
    # The answer must be derived from data. Suspect loads never appear in documents; a suspect lot may appear
    # only in a report of its OWN, unrelated deviation (a real fact recorded in deviation_lots).
    leak = [l for l in gt["suspect_loads"] if any(l in t for t in text.values())]
    own = set(tx["deviation_lots"]["lot_id"])
    leak += [l for l in gt["suspect_lots"] if l not in own and any(l in t for t in text.values())]
    add("no ground-truth answer leaks into documents (loads never; lots only via their own deviation)",
        not leak, str(leak[:2]))
    add("deviation report numbers agree with telemetry (precursor hours present for molding)",
        all("Telemetry review" in text[f"DR-{d}"] for d in
            tx["deviations"].query("category == 'molding' and source == 'simulation'")["deviation_id"]))

    print("[p2p] checks:\n" + format_results(res))
    write_manifest(cfg, "documents", {"documents": len(idx), "by_type": idx["doc_type"].value_counts().to_dict(),
                                      "words": int(idx["words"].sum()), "calibration_records": len(cal),
                                      "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in res]})
    write_data_dictionary(cfg, {"docs_index": idx, "calibration_records": cal}, "documents",
                          {"docs_index": "One row per document: type, revision, effective date, sites, assets, related IDs (for metadata filtering in retrieval).",
                           "calibration_records": "Instrument verifications: as-found / as-left error, tolerance, PASS/FAIL, linked deviation."})
    print(f"[p2p] {len(idx)} documents ({idx['doc_type'].value_counts().to_dict()}), "
          f"{int(idx['words'].sum()):,} words, {len(cal)} calibration records in {time.monotonic() - t0:.0f}s")
    return 0 if all(ok for _, ok, _ in res) else 3


if __name__ == "__main__":
    raise SystemExit(main())
