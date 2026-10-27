#!/usr/bin/env python
"""
Re-run every integrity check against files ON DISK (not in memory).

Use after copying output/ to the VM, a UC Volume or a Fabric Lakehouse, to
prove what the platforms will load is exactly what was generated:

    python scripts/validate.py --profile lite --out output
    python scripts/validate.py --profile lite --out output --digests   # compare to manifest
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from p2p_gen.checks import check_master, format_results  # noqa: E402
from p2p_gen.config import PROFILES, P2PConfig  # noqa: E402
from p2p_gen.determinism import table_digest  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=sorted(PROFILES), default="lite")
    ap.add_argument("--out", default="output")
    ap.add_argument("--digests", action="store_true", help="Also compare table digests with the manifest.")
    a = ap.parse_args(argv)

    cfg = P2PConfig(profile=a.profile, out_root=a.out)
    root = Path(a.out) / "master"
    tables = {p.stem: pd.read_parquet(p) for p in sorted(root.glob("*.parquet"))}
    results = check_master(cfg, tables)
    if a.digests:
        manifest = json.loads((root / "_manifest.json").read_text())
        for name, info in manifest["tables"].items():
            got = table_digest(tables[name])
            results.append((f"digest matches manifest: {name}", got == info["digest"],
                            f"manifest {info['digest']} vs disk {got}"))
    print(format_results(results))
    return 0 if all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
