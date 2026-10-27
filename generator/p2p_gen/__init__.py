"""
Plant-to-Provider control tower: synthetic data generator.
==========================================================
Platform-neutral synthetic data for a Databricks vs Microsoft Fabric PoC,
modelled on a FICTIONAL vertically integrated medical-surgical manufacturer
and distributor.

Build steps (each gated on determinism + integrity checks):
  1. master data          scripts/gen_master.py      <- this release
  2. transactions/lots    scripts/gen_transactions.py
  3. telemetry history    scripts/gen_history.py
  4. documents            scripts/gen_docs.py
  5. live publisher       scripts/publish_live.py
"""
from .config import PROFILES, P2PConfig, Profile
from .determinism import (cross_process_selftest, determinism_selftest, digests,
                          rng_for, stable_hash, table_digest)
from .world import build_world

__version__ = "0.1.0"
__all__ = ["P2PConfig", "Profile", "PROFILES", "build_world", "rng_for", "stable_hash",
           "table_digest", "digests", "determinism_selftest", "cross_process_selftest"]
