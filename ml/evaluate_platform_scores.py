#!/usr/bin/env python
"""
Evaluate both platforms' molding scores with IDENTICAL code, offline, against ground truth.

    python ml/evaluate_platform_scores.py --ground-truth ./ground_truth_lite90 \
        --scores molding_scores_databricks.parquet molding_scores_fabric.parquet

Download the raw score exports from each platform (Databricks volume ml/exports, Fabric Files/exports) and
the lite90 ground truth from the groundtruth container (p2p/lite90/ground_truth). The same alert rule
(15-min smoothing, 99.9th percentile of smoothed training scores) is applied to every file.
"""
import argparse, os, sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import p2p_ml  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--ground-truth", type=Path, required=True)
ap.add_argument("--scores", nargs="+", required=True)
ap.add_argument("--train-days", type=int, default=45)
a = ap.parse_args()
log = pd.read_parquet(a.ground_truth / "anomaly_log.parquet")
out = []
for f in a.scores:
    S = pd.read_parquet(f)
    S["bucket_ts"] = pd.to_datetime(S["bucket_ts"], utc=True)
    cut = S["bucket_ts"].min() + pd.Timedelta(days=a.train_days)
    A = p2p_ml.alerts(S, cut)
    r = p2p_ml.evaluate(A, log, after=cut)
    r.insert(0, "platform", os.path.basename(f).replace("molding_scores_", "").replace(".parquet", ""))
    out.append(r)
res = pd.concat(out, ignore_index=True)
print(res.to_string(index=False))
res.to_csv("ml_evaluation.csv", index=False)
print("-> ml_evaluation.csv")
