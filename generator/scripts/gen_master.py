#!/usr/bin/env python
"""
Step 1: generate master data.

    python scripts/gen_master.py                     # lite profile
    python scripts/gen_master.py --profile baseline
    python scripts/gen_master.py --cross-process     # also prove determinism across interpreters

Gated: refuses to write anything if the determinism self-test or any
integrity check fails.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.checks import check_master, format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import cross_process_selftest, determinism_selftest  # noqa: E402
from p2p_gen.world import TABLE_DESCRIPTIONS, build_world  # noqa: E402
from p2p_gen.writers import write_data_dictionary, write_manifest, write_tables  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    ap.add_argument("--out", default="output")
    ap.add_argument("--no-csv", action="store_true", help="Parquet only.")
    ap.add_argument("--outage-dc", default=None, help="Override the outage-scenario DC (e.g. DC01).")
    ap.add_argument("--cross-process", action="store_true",
                    help="Also run the cross-interpreter determinism test (different PYTHONHASHSEEDs).")
    ap.add_argument("--skip-selftest", action="store_true")
    a = ap.parse_args(argv)

    cfg = P2PConfig(profile=a.profile, seed=a.seed, out_root=a.out,
                    write_csv=not a.no_csv, outage_dc_id=a.outage_dc)
    print(f"[p2p] profile={cfg.profile} seed={cfg.seed} out={cfg.out_root}")
    t0 = time.monotonic()

    selftests = []
    if not a.skip_selftest:
        st = determinism_selftest(build_world, cfg)
        selftests.append(st)
        print(f"[p2p] determinism (in-process): {'OK' if st['deterministic'] else 'FAILED'} over {st['tables']} tables")
        if a.cross_process:
            xp = cross_process_selftest(cfg.profile, cfg.seed)
            selftests.append(xp)
            print(f"[p2p] determinism (cross-process, PYTHONHASHSEED={xp['pythonhashseeds']}): "
                  f"{'OK' if xp['deterministic'] else 'FAILED'}")
        if not all(s["deterministic"] for s in selftests):
            print(f"[p2p] ABORT: non-deterministic tables: {[s['mismatched'] for s in selftests]}", file=sys.stderr)
            return 2

    tables = build_world(cfg)
    results = check_master(cfg, tables)
    print("[p2p] integrity checks:\n" + format_results(results))
    if not all(ok for _, ok, _ in results):
        print("[p2p] ABORT: integrity checks failed; nothing written.", file=sys.stderr)
        return 3

    info = write_tables(cfg, tables, "master")
    manifest = write_manifest(cfg, "master", {"tables": info, "selftests": selftests,
                                              "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in results]})
    print(f"\n[p2p] wrote {len(info)} tables in {time.monotonic() - t0:.1f}s -> {Path(cfg.out_root) / 'master'}")
    for name, i in info.items():
        print(f"  {name:<22} {i['rows']:>7,} rows  {i['columns']:>3} cols  digest {i['digest']}")
    print(f"[p2p] manifest: {manifest}")
    print(f"[p2p] data dictionary: {write_data_dictionary(cfg, tables, 'master', TABLE_DESCRIPTIONS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
