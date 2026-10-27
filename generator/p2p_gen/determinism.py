"""
Determinism: the gate every generation run passes through.
===========================================================
Lesson carried over from the Novelis POC: the upstream engine seeded with the
builtin hash(), which Python salts per process, so two runs produced
different data. For a comparison PoC that is fatal -- any difference
between Databricks and Fabric could be blamed on different inputs.

Rules enforced in this package:
  * never call the builtin hash() for seeding -- use stable_hash()
  * never iterate over a set of strings (its order depends on the hash salt)
  * every table gets its own RNG from (seed, key), so tables are independent

`determinism_selftest` proves it in-process; `cross_process_selftest` proves
it across interpreters with DIFFERENT PYTHONHASHSEED values, which is the
failure mode that actually bit last time.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import zlib
from pathlib import Path

import numpy as np
import pandas as pd


def stable_hash(*parts: object) -> int:
    """Process-independent replacement for hash(). CRC32 of the joined parts."""
    return zlib.crc32("|".join(str(p) for p in parts).encode("utf-8"))


def rng_for(seed: int, *keys: object) -> np.random.Generator:
    """Independent, reproducible RNG for one table / entity / signal."""
    ss = np.random.SeedSequence([int(seed) & 0xFFFFFFFF, stable_hash(*keys)])
    return np.random.default_rng(ss)


def table_digest(df: pd.DataFrame) -> str:
    """Content hash of a table: values, column names and order."""
    h = hashlib.sha256()
    h.update(",".join(map(str, df.columns)).encode("utf-8"))
    if len(df):
        h.update(pd.util.hash_pandas_object(df, index=False).to_numpy().tobytes())
    return h.hexdigest()[:16]


def digests(tables: dict[str, pd.DataFrame]) -> dict[str, str]:
    return {name: table_digest(df) for name, df in sorted(tables.items())}


def determinism_selftest(build_fn, cfg) -> dict:
    """Build twice in one process; every table digest must match."""
    a = digests(build_fn(cfg))
    b = digests(build_fn(cfg))
    mismatched = [k for k in a if a[k] != b.get(k)]
    return {"mode": "in-process", "tables": len(a),
            "deterministic": not mismatched, "mismatched": mismatched}


def cross_process_selftest(profile: str, seed: int,
                           hash_seeds: tuple[str, ...] = ("0", "4242")) -> dict:
    """Build in separate interpreters with different PYTHONHASHSEED values.

    Catches the subtle cases the in-process test cannot: set iteration
    order, builtin hash() sneaking back in, dict-of-set constructions.
    """
    root = Path(__file__).resolve().parent.parent
    runs = []
    for hs in hash_seeds:
        env = dict(os.environ, PYTHONHASHSEED=hs, PYTHONPATH=str(root))
        out = subprocess.run(
            [sys.executable, "-m", "p2p_gen.digest", "--profile", profile,
             "--seed", str(seed)],
            capture_output=True, text=True, env=env, cwd=str(root), check=True,
        )
        runs.append(json.loads(out.stdout))
    first = runs[0]
    mismatched = sorted({k for r in runs[1:] for k in first if r.get(k) != first[k]})
    return {"mode": "cross-process", "pythonhashseeds": list(hash_seeds),
            "tables": len(first), "deterministic": not mismatched,
            "mismatched": mismatched}
