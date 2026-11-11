# ML comparison: molding anomaly detection (MLOps structure)

Same code on both platforms (`p2p_ml.py`, uploaded next to the Databricks notebooks / to Lakehouse `Files/code/`).
Data: the `lite90` profile (`landing/p2p/lite90/`, read directly as files; never via Event Hubs or bronze tables).

| Notebook (both platforms) | Stage | Runs as | Writes |
|---|---|---|---|
| `30_ml_features` | Feature table, built once; training and inference both read it | job | `ml.molding_features` / `ml_molding_features` |
| `31_ml_train_experiment` | One MLflow run per candidate (baseline, Isolation Forest, Keras autoencoder); validation against the quality system's molding deviations; champion registered with alias `@champion` | manual / retraining trigger | MLflow runs, registered model `molding_anomaly`, score export for the offline check |
| `32_ml_batch_inference` | Loads `@champion` from the registry, scores a window, applies the champion's logged threshold | scheduled job (`score_start` / `score_end`) | `molding_scores`, `molding_alerts` |
| `33_ml_stream_inference` | Live: 1-minute aggregates (watermark 2 min) -> shared `stream_microbatch` -> scores and alerts, with latency | continuous | `molding_stream_scores`, `molding_stream_alerts` |

Databricks streams from `silver.telemetry` (start 02a/02b first). Fabric reads Event Hubs directly (Spark Kafka connector)
through consumer group `fabric-ml`: add it with `consumer_groups = ["databricks", "fabric", "fabric-ml"]` in
`terraform.tfvars` and `terraform apply` (creates the group and a listen rule; the VM is not rebuilt). Store the
`fabric-listen` connection string in Azure Key Vault (secret `eventhub-fabric-listen`) and set `KEY_VAULT` in Fabric 33.

Live input for 33: publish with the lite90 profile (same machines and tag ids as lite; its work orders give the setpoints):
`publish_live.py --profile lite90 --out /opt/p2p/output/lite90 --env-file /etc/p2p/publisher.env --time-scale 1 --duration-min 30`.
The live story has no molding incident: 33 measures false alarms, latency, and the firmware 4.3 tag change at minute 20
(`machines_scored` drops for the second plant's machines: an unmonitored model degrades silently; monitoring is a later step).

## Hyperparameter tuning (notebook 31, parameter `tuning_mode`)

| Mode | Databricks | Fabric |
|---|---|---|
| `single` | fixed hyperparameters | fixed hyperparameters |
| `optuna` | Optuna TPE, sequential on the driver | same (may need `%pip install optuna`) |
| `distributed` | **Ray Tune on Spark** (`setup_ray_cluster`; Databricks' recommended distributed tuner since Hyperopt left DBR ML after 16.4 LTS). Use a cluster with workers | **FLAML tune, `use_spark=True`** (Fabric's built-in, preview): one trial per executor |

Shared across modes: search spaces (`SEARCH_SPACE`), the objective (`tuning_objective`: 100 x recall + lead hours - 50 x
false alarms per machine-day on business labels), and driver-side MLflow logging (a parent run per tuned model, one child
run per trial; no worker credentials needed). Time split: trials fit days 1-30 (sampled) and are scored on days 31-45; the
final fit uses days 1-45; the champion is chosen on days 46-90, never seen by tuning. Compare per mode and platform:
wall-clock time, trials completed, best objective, and the effort to set it up. Caveat: the tuning window holds only 4
deviations, so trial scores are noisy; use this to compare platform mechanics, not to claim optimal hyperparameters.

Local test (lite90): Optuna and FLAML (local, no Spark) paths, trial logging (3 parents, 9 child trials) and the tuned final
fit all ran; Ray was not run locally (test it on Databricks). Tuning lifted Isolation Forest's objective markedly
(small `max_samples`), worth checking on the untouched test period.

## Design decisions

- One MLflow run per candidate; champion rule fixed before running: at most 0.1 false alarms per machine-day, then
  highest recall, then longest lead time. Only the champion is registered (alias; a version tag where aliases are unsupported).
- Every candidate is served through one pyfunc interface (`AnomalyScorer`: features in, `anomaly_score` out) that packages
  `p2p_ml.py` with the model, so inference loads "the champion" without knowing its type. The alert threshold is logged
  with the run and read back at inference: model and threshold are versioned together.
- Validation uses on-platform BUSINESS labels (molding deviations, 12-hour horizon). The generator's answer key stays
  offline for the independent check (`evaluate_platform_scores.py`).
- Features in SQL (pandas ran out of memory on the 21M-row history); alerts need 15 minutes of persistence.

## Local test results (lite90; Keras on JAX locally, TensorFlow on the platforms)

| Candidate | Recall (11 deviations) | Median warning | False alarms per machine-day |
|---|---|---|---|
| Autoencoder (champion) | 11/11 | ~6.7 h | 0.02 |
| Baseline | 11/11 | ~4.1 h | 0 |
| Isolation Forest | 4/11 | ~5.0 h | 0 |

Streaming path vs batch path on the same 8 hours (7,933 machine-minutes): score correlation 0.9999, median difference 0:
no training/serving skew. Exact figures vary slightly with training epochs and the runtime's Keras backend.
