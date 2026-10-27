#!/usr/bin/env python
"""
Step 2: simulate the supply chain -> transactions, lots, genealogy, inventory.

    python scripts/gen_transactions.py                 # lite
    python scripts/gen_transactions.py --profile baseline
    python scripts/gen_transactions.py --skip-selftest # skip the run-twice determinism test

Master data is rebuilt in memory from the same seed (and checked against
output/master/_manifest.json if present), so transactions can never be
generated against a different master than the one on disk.

Writes output/transactions/ and output/ground_truth/hero_blast_radius.json.
The ground truth is for SCORING the platforms' agents -- do not load it.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.blast_radius import hero_blast_radius, summary_text, write_ground_truth  # noqa: E402
from p2p_gen.checks import check_transactions, format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import digests  # noqa: E402
from p2p_gen.transactions import build_transactions  # noqa: E402
from p2p_gen.world import build_world  # noqa: E402
from p2p_gen.writers import write_data_dictionary, write_manifest, write_tables  # noqa: E402

DESCRIPTIONS = {
    "customer_formulary": "Each customer's contracted assortment and its share of order lines.",
    "component_lots": "Raw-material / packaging / EtO gas lots received at plants. One resin lot is nonconforming.",
    "work_orders": "Production runs on a molding machine or line. `is_opening_stock` = pre-history stock build.",
    "sterilization_loads": "EtO chamber cycles with phase timestamps, result, BI and release. `in_cycle` = in the chamber at live start.",
    "load_contents": "Which work order's product (cases, pallets) went into each load, including failed cycles.",
    "lots": "Finished-goods lots. Sterile make lot = one work order's product sterilised in one load. Includes UDI-PI.",
    "lot_genealogy": "Edges: component lot -> WO, kit-content lot -> kit WO, WO -> lot, load -> lot, EtO lot -> load.",
    "stock_transfers": "Plant -> DC shipments of released lots. `in_transit` = on the road at live start.",
    "purchase_orders": "Buy-SKU POs to suppliers, received straight into DCs as supplier lots.",
    "customer_orders": "Orders per delivery day. `open` = requested after live start, soft-allocated.",
    "order_lines": "Order lines with shipped / backordered / allocated quantities and contract price per case.",
    "shipments": "Route deliveries with planned and actual arrival.",
    "shipment_lines": "Which lot each shipped case came from (FEFO). The recall trace to customers.",
    "order_allocations": "Soft allocations of OPEN order lines to DC lots at live start: what the hero scenario puts at risk.",
    "inventory_ledger": "Every receipt (+) and shipment (-) per DC x lot.",
    "inventory_snapshot_daily": "End-of-day on-hand per DC x lot over the history window.",
    "inventory_on_hand": "On hand / allocated / available per DC x lot at live start.",
    "deviations": "Quality deviations: 18-month archive (precedent library) + events during the simulation.",
    "deviation_lots": "Lots touched by a deviation and their disposition.",
    "capas": "Corrective and preventive actions raised from major/critical deviations.",
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    ap.add_argument("--out", default="output")
    ap.add_argument("--no-csv", action="store_true")
    ap.add_argument("--skip-selftest", action="store_true")
    a = ap.parse_args(argv)
    cfg = P2PConfig(profile=a.profile, seed=a.seed, out_root=a.out, write_csv=not a.no_csv)
    print(f"[p2p] step 2 profile={cfg.profile} seed={cfg.seed}")
    t0 = time.monotonic()

    world = build_world(cfg)
    man = Path(cfg.out_root) / "master" / "_manifest.json"
    if man.exists():
        on_disk = {k: v["digest"] for k, v in json.loads(man.read_text())["tables"].items()}
        now = digests(world)
        stale = sorted(k for k in now if on_disk.get(k) != now[k])
        if stale:
            print(f"[p2p] ABORT: output/master differs from this code+seed for {stale}. "
                  f"Re-run scripts/gen_master.py first.", file=sys.stderr)
            return 4
        print("[p2p] master on disk matches this code + seed")

    tx, sim = build_transactions(cfg, world)
    print(f"[p2p] simulated {cfg.prehistory_days + cfg.p.history_days} days in {time.monotonic() - t0:.0f}s")
    selftest = None
    if not a.skip_selftest:
        tx2, _ = build_transactions(cfg, world)
        d1, d2 = digests(tx), digests(tx2)
        bad = sorted(k for k in d1 if d1[k] != d2[k])
        selftest = {"mode": "in-process", "deterministic": not bad, "mismatched": bad}
        print(f"[p2p] determinism (in-process): {'OK' if not bad else 'FAILED ' + str(bad)}")
        if bad:
            return 2

    br = hero_blast_radius(cfg, world, tx, sim.hero_ster)
    results = check_transactions(cfg, world, tx, br)
    print("[p2p] integrity checks:\n" + format_results(results))
    print(f"[p2p] hero blast radius:\n{summary_text(br)}")
    if not all(ok for _, ok, _ in results):
        print("[p2p] ABORT: checks failed; nothing written.", file=sys.stderr)
        return 3

    info = write_tables(cfg, tx, "transactions")
    write_manifest(cfg, "transactions", {"tables": info, "selftest": selftest,
                                         "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in results]})
    write_data_dictionary(cfg, tx, "transactions", DESCRIPTIONS)
    gt = write_ground_truth(cfg, br)
    print(f"\n[p2p] wrote {len(info)} tables in {time.monotonic() - t0:.0f}s -> {Path(cfg.out_root) / 'transactions'}")
    for name, i in info.items():
        print(f"  {name:<26} {i['rows']:>9,} rows")
    print(f"[p2p] hero ground truth -> {gt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
