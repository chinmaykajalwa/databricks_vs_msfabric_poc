# Fabric notebook. Same as Databricks 31. tuning_mode: single | optuna | distributed
# distributed = FLAML tune with use_spark=True (Fabric's built-in, preview): one trial per Spark executor by default.
# Ray is not used here: Fabric's documented distributed tuner is FLAML. If `import optuna` fails: %pip install optuna.
# If `import keras` fails: %pip install "tensorflow>=2.16".
tuning_mode, n_trials, concurrency = "single", 8, 4     # parameter cell
import os, sys, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
sys.path.append("/lakehouse/default/Files/code")
import mlflow, pandas as pd, p2p_ml
mlflow.autolog(disable=True)                            # trials are logged explicitly, identically on both platforms
L, EXPERIMENT, MODEL = "Files/landing/p2p/lite90/", "p2p-molding-anomaly", "molding_anomaly"
F = spark.table("ml_molding_features").toPandas()
F["bucket_ts"] = pd.to_datetime(F["bucket_ts"], utc=True)
train, test, cut = p2p_ml.split(F, 45)
dev = spark.read.parquet(L + "transactions/deviations.parquet").toPandas()

def tune_flaml(kind, fit, val, dev, n, conc):
    from flaml import tune
    spark.sparkContext.addPyFile("/lakehouse/default/Files/code/p2p_ml.py")     # executors import the same module

    def evaluate(config):
        import os
        os.environ.setdefault("KERAS_BACKEND", "tensorflow")
        import p2p_ml as m
        return m.tuning_objective(kind, config, fit, val, dev)
    a = tune.run(evaluate, config=p2p_ml.flaml_space(kind), metric="objective", mode="max",
                 num_samples=n, use_spark=True, n_concurrent_trials=conc)
    keys = ("events", "detected", "recall", "median_lead_min", "alerts", "false_alarms", "false_alarms_per_machine_day", "objective")
    return [{"params": {k: t.config[k] for k in p2p_ml.SEARCH_SPACE[kind]}, "metrics": {k: t.last_result[k] for k in keys}}
            for t in a.trials if t.last_result and "objective" in t.last_result]

best_params, tuning = {}, []
if tuning_mode != "single":
    fit, val, tcut = p2p_ml.tuning_split(train)
    for kind in p2p_ml.TUNABLE:
        t = time.time()
        trials = p2p_ml.tune_optuna(kind, fit, val, dev, n_trials) if tuning_mode == "optuna" else tune_flaml(kind, fit, val, dev, n_trials, concurrency)
        wall = time.time() - t
        best = p2p_ml.log_trials(kind, trials, EXPERIMENT, tuning_mode, wall)
        best_params[kind] = best["params"]
        tuning.append({"candidate": kind, "mode": tuning_mode, "trials": len(trials), "wall_seconds": round(wall, 1),
                       "best_objective": best["metrics"]["objective"], **{f"best_{k}": v for k, v in best["params"].items()}})
    display(pd.DataFrame(tuning))

results, exports = [], []
for kind in p2p_ml.CANDIDATES:
    t = time.time()
    m = p2p_ml.fit_candidate(kind, train, params=best_params.get(kind))
    thr = p2p_ml.alert_threshold(kind, m, train)
    s_all = pd.DataFrame({"asset_id": F["asset_id"], "bucket_ts": F["bucket_ts"], "score": p2p_ml.score_candidate(kind, m, F)})
    metrics = p2p_ml.evaluate_vs_deviations(p2p_ml.smooth_flags(s_all, thr), dev, cut, F["bucket_ts"].max())
    metrics["train_seconds"] = round(time.time() - t, 1)
    results.append({"kind": kind, "uri": p2p_ml.log_candidate(kind, m, train, metrics, thr, cut, EXPERIMENT, best_params.get(kind), tuning_mode), "metrics": metrics})
    exports.append(s_all.assign(model=kind))
champ = p2p_ml.pick_champion(results)
print(f"CHAMPION ({tuning_mode}): {champ['kind']} -> {MODEL} v{p2p_ml.register_champion(champ['uri'], MODEL)}")
os.makedirs("/lakehouse/default/Files/exports", exist_ok=True)
pd.concat(exports).to_parquet(f"/lakehouse/default/Files/exports/molding_scores_fabric_{tuning_mode}.parquet")
display(pd.DataFrame([{"candidate": r["kind"], **r["metrics"]} for r in results]))
