# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · ML experiment: (optional) hyperparameter tuning, one MLflow run per candidate, register the champion
# MAGIC **tuning_mode**: `single` (fixed hyperparameters) | `optuna` (sequential trials on the driver) |
# MAGIC `distributed` (Ray Tune on the Spark cluster, Databricks' recommended distributed tuner; use a cluster WITH workers).
# MAGIC Time split: tuning fits days 1-30 and scores trials on days 31-45; the final fit uses days 1-45; the champion is
# MAGIC chosen on days 46-90 (never seen by tuning). Labels: the quality system's molding deviations.
# MAGIC Champion rule: at most 0.1 false alarms per machine-day, then recall, then lead time. Only the champion is registered.

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("landing_ml_path", "abfss://landing@p2ppocstxbohi5.dfs.core.windows.net/p2p/lite90/")
dbutils.widgets.dropdown("tuning_mode", "single", ["single", "optuna", "distributed"])
dbutils.widgets.text("n_trials", "8")
dbutils.widgets.text("concurrency", "4")
CATALOG, L = dbutils.widgets.get("catalog"), dbutils.widgets.get("landing_ml_path").rstrip("/") + "/"
MODE, N, CONC = dbutils.widgets.get("tuning_mode"), int(dbutils.widgets.get("n_trials")), int(dbutils.widgets.get("concurrency"))
import os, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
import mlflow, pandas as pd, p2p_ml
mlflow.set_registry_uri("databricks-uc")
EXPERIMENT, MODEL = "/Shared/p2p-molding-anomaly", f"{CATALOG}.ml.molding_anomaly"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ml.exports")
F = spark.table(f"{CATALOG}.ml.molding_features").toPandas()
F["bucket_ts"] = pd.to_datetime(F["bucket_ts"], utc=True)
train, test, cut = p2p_ml.split(F, 45)
dev = spark.read.parquet(L + "transactions/deviations.parquet").toPandas()

# COMMAND ----------

# MAGIC %md ## 1. Hyperparameter tuning (skipped when tuning_mode = single)

# COMMAND ----------

def tune_ray(kind, fit, val, dev, n, conc):
    """Ray on Spark: trials run as Ray tasks on the cluster's workers; p2p_ml ships with the job (py_modules)."""
    import ray
    from ray import tune
    try:
        from ray.util.spark import setup_ray_cluster
        try:
            setup_ray_cluster(max_worker_nodes=conc)
        except TypeError:                                  # older Ray API
            setup_ray_cluster(num_worker_nodes=conc)
    except Exception as exc:
        print(f"[ray] no Ray-on-Spark cluster ({type(exc).__name__}: {exc}); running Ray locally on the driver")
    ray.init(ignore_reinit_error=True, runtime_env={"py_modules": [p2p_ml]})

    def trainable(config, fit=None, val=None, dev=None):
        import os
        os.environ.setdefault("KERAS_BACKEND", "tensorflow")
        import p2p_ml as m
        return m.tuning_objective(kind, config, fit, val, dev)
    res = tune.Tuner(tune.with_parameters(trainable, fit=fit, val=val, dev=dev), param_space=p2p_ml.ray_space(kind),
                     tune_config=tune.TuneConfig(num_samples=n, max_concurrent_trials=conc, metric="objective", mode="max")).fit()
    keys = ("events", "detected", "recall", "median_lead_min", "alerts", "false_alarms", "false_alarms_per_machine_day", "objective")
    return [{"params": {k: r.config[k] for k in p2p_ml.SEARCH_SPACE[kind]}, "metrics": {k: r.metrics[k] for k in keys}}
            for r in res if r.metrics]

best_params, tuning = {}, []
if MODE != "single":
    fit, val, tcut = p2p_ml.tuning_split(train)
    for kind in p2p_ml.TUNABLE:
        t = time.time()
        trials = p2p_ml.tune_optuna(kind, fit, val, dev, N) if MODE == "optuna" else tune_ray(kind, fit, val, dev, N, CONC)
        wall = time.time() - t
        best = p2p_ml.log_trials(kind, trials, EXPERIMENT, MODE, wall)
        best_params[kind] = best["params"]
        tuning.append({"candidate": kind, "mode": MODE, "trials": len(trials), "wall_seconds": round(wall, 1),
                       "best_objective": best["metrics"]["objective"], **{f"best_{k}": v for k, v in best["params"].items()}})
    display(pd.DataFrame(tuning))

# COMMAND ----------

# MAGIC %md ## 2. Final fit per candidate (best hyperparameters), test-period evaluation, champion

# COMMAND ----------

results, exports = [], []
for kind in p2p_ml.CANDIDATES:
    t = time.time()
    m = p2p_ml.fit_candidate(kind, train, params=best_params.get(kind))
    thr = p2p_ml.alert_threshold(kind, m, train)
    s_all = pd.DataFrame({"asset_id": F["asset_id"], "bucket_ts": F["bucket_ts"], "score": p2p_ml.score_candidate(kind, m, F)})
    metrics = p2p_ml.evaluate_vs_deviations(p2p_ml.smooth_flags(s_all, thr), dev, cut, F["bucket_ts"].max())
    metrics["train_seconds"] = round(time.time() - t, 1)
    uri = p2p_ml.log_candidate(kind, m, train, metrics, thr, cut, EXPERIMENT, best_params.get(kind), MODE)
    results.append({"kind": kind, "uri": uri, "metrics": metrics})
    exports.append(s_all.assign(model=kind))
champ = p2p_ml.pick_champion(results)
version = p2p_ml.register_champion(champ["uri"], MODEL)
print(f"CHAMPION ({MODE}): {champ['kind']} -> {MODEL} v{version}")
pd.concat(exports).to_parquet(f"/Volumes/{CATALOG}/ml/exports/molding_scores_databricks_{MODE}.parquet")
display(pd.DataFrame([{"candidate": r["kind"], **r["metrics"]} for r in results]))
