"""Print per-table digests as JSON. Used by the cross-process self-test."""
from __future__ import annotations

import argparse
import json

from .config import P2PConfig
from .determinism import digests
from .world import build_world


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="lite")
    ap.add_argument("--seed", type=int, default=P2PConfig().seed)
    a = ap.parse_args()
    print(json.dumps(digests(build_world(P2PConfig(profile=a.profile, seed=a.seed)))))


if __name__ == "__main__":
    main()
