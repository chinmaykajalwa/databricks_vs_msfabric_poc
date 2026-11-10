# ML comparison: molding anomaly detection

Same code on both platforms (`p2p_ml.py`); the comparison is about data access, experiment tracking,
registry, batch and real-time serving, effort and cost -- not about model code.

| File | Purpose |
|---|---|
| `p2p_ml.py` | Shared module: `FEATURE_SQL` (Spark SQL, runs on both), `finish_features`, baseline / Isolation Forest / Keras autoencoder, `alerts` (shared alert rule), MLflow `log_and_register`, offline `evaluate` |
| `evaluate_platform_scores.py` | Scores BOTH platforms' exports with identical code against ground truth, on your laptop |
| `../platforms/databricks/ml/30_ml_molding.py` | Databricks notebook (Runtime ML 15.4 LTS+): features, train, MLflow + Unity Catalog registry, `spark_udf`, Model Serving latency |
| `../platforms/fabric/ml/10_ml_molding.py` | Fabric notebook: same steps with Fabric experiments, ML model items, PREDICT, ML model endpoints (preview) |

## Data: the `lite90` profile

Same entities as lite, 90 days of history, deviation rate 12% (19 labelled molding precursors; 11 in the test half).
Separate output and landing path (`p2p/lite90/`), so the lite data used by the agents is untouched. On the VM
(Run Command, in the background; ~4 minutes):

```bash
nohup bash -c 'cd /opt/p2p/repo/generator && PYTHON=/opt/p2p/venv/bin/python bash scripts/run_pipeline.sh --profile lite90 --out /opt/p2p/output/lite90 && P2P_PROFILE=lite90 bash /opt/p2p/repo/infra/vm/upload.sh all' > /var/log/p2p-lite90.log 2>&1 &
```

## Design decisions (each is also a finding)

- **Features in SQL, not pandas.** Pivoting the 21M-row 90-day aggregate in pandas ran out of memory; the Spark SQL
  query hands pandas ~2M rows.
- **Alerting needs persistence.** Raw per-minute flags gave 3-5 false alarms per machine-day; 15-minute smoothing with a
  threshold at the 99.9th percentile of smoothed training scores (same rule for every model) brings that to ~0.
- **Keras autoencoder outputs ONE score** (reconstruction error computed inside the network), and is logged with the
  tensor signature MLflow's Keras flavour requires. Fabric's preview endpoints document scalar/column schemas: whether
  they serve it is a test to run, not an assumption.
- **scikit-learn models saved with cloudpickle** (MLflow 3 defaults to skops): identical artefacts whatever the runtime.

## Local preview (lite90, Keras on JAX, test period = last 45 days)

| Model | Recall | Median warning before the deviation | False alarms (20 machines, 45 days) |
|---|---|---|---|
| Autoencoder | 11/11 | ~5.3 h | 1 |
| Baseline (robust z-score vs setpoints) | 11/11 | ~4.1 h | 0 |
| Isolation Forest | 4/11 | ~5.0 h | 0 |

The autoencoder warns ~70 minutes earlier than simple rules; Isolation Forest misses most slow drifts. Platform runs
should reproduce these numbers (same code, same data); differences would come from the runtime.

## ML scorecard (record per platform)

Wall time: features / train / log+register / batch scoring / endpoint activation; endpoint median latency (20 calls);
which models each serving path accepted; capacity units or DBUs consumed (Capacity Metrics app / system.billing.usage);
effort hours and friction; evaluation from `evaluate_platform_scores.py`.
