# Databricks notebook source
# MAGIC %md
# MAGIC # 32 · Batch inference (scheduled job)
# MAGIC Loads the CURRENT champion from the registry (by alias), scores a time window of the feature table, applies the
# MAGIC champion's own alert threshold (logged with its run), writes scores and alerts. Never trains.
# MAGIC Schedule it: Workflows -> Create job -> this notebook, parameters `score_start` / `score_end` (e.g. yesterday).

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("score_start", "")      # empty = last 7 days of the feature table
dbutils.widgets.text("score_end", "")
CATALOG = dbutils.widgets.get("catalog")
import os, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
import mlflow, pandas as pd, p2p_ml
from pyspark.sql import functions as Fn
mlflow.set_registry_uri("databricks-uc")
MODEL = f"{CATALOG}.ml.molding_anomaly"
t = time.time()
model, thr, version = p2p_ml.load_champion(MODEL)
feats = spark.table(f"{CATALOG}.ml.molding_features")
end = pd.Timestamp(dbutils.widgets.get("score_end") or feats.agg(Fn.max("bucket_ts")).first()[0], tz="UTC")
start = pd.Timestamp(dbutils.widgets.get("score_start"), tz="UTC") if dbutils.widgets.get("score_start") else end - pd.Timedelta(days=7)
look = start - pd.Timedelta(minutes=20)                     # history needed by the 15-minute smoothing
X = feats.filter((Fn.col("bucket_ts") >= look.to_pydatetime()) & (Fn.col("bucket_ts") <= end.to_pydatetime())).toPandas()
flagged = p2p_ml.smooth_flags(p2p_ml.score_features(model, X), thr)
flagged = flagged[flagged["bucket_ts"] >= start].assign(model_version=version)
runs = p2p_ml._alert_runs(flagged).assign(model_version=version)
spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.ml.molding_scores (asset_id STRING, bucket_ts TIMESTAMP, score DOUBLE, score_smooth DOUBLE, flag BOOLEAN, model_version STRING)")
spark.sql(f"DELETE FROM {CATALOG}.ml.molding_scores WHERE bucket_ts >= '{start}' AND bucket_ts <= '{end}'")   # idempotent re-runs
spark.createDataFrame(flagged[["asset_id", "bucket_ts", "score", "score_smooth", "flag", "model_version"]]).write.mode("append").saveAsTable(f"{CATALOG}.ml.molding_scores")
if len(runs):
    spark.createDataFrame(runs).write.mode("append").saveAsTable(f"{CATALOG}.ml.molding_alerts")
print(f"champion v{version}: scored {len(flagged):,} machine-minutes {start} -> {end}, {len(runs)} alerts, {time.time() - t:.0f}s")
