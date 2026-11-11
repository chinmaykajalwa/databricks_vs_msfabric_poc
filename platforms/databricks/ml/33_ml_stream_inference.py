# Databricks notebook source
# MAGIC %md
# MAGIC # 33 · Streaming inference (continuous)
# MAGIC silver.telemetry (live, from 02a/02b) -> 1-minute aggregates per machine and signal (watermark 2 min) -> shared
# MAGIC `p2p_ml.stream_microbatch` (setpoints, features, champion score, 15-min smoothing) -> live scores and alerts.
# MAGIC Start 02a and 02b first; then publish with the lite90 profile (runbook "ML run"). The live story has no molding
# MAGIC incident, so this measures FALSE ALARMS, latency, and what happens at minute 20 (firmware 4.3 renames melt-temp
# MAGIC tags: watch `machines_scored` drop).

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("landing_ml_path", "abfss://landing@p2ppocstxbohi5.dfs.core.windows.net/p2p/lite90/")
CATALOG, L = dbutils.widgets.get("catalog"), dbutils.widgets.get("landing_ml_path").rstrip("/") + "/"
import os
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
import mlflow, pandas as pd, p2p_ml
from pyspark.sql import functions as Fn
mlflow.set_registry_uri("databricks-uc")
MODEL = f"{CATALOG}.ml.molding_anomaly"
SCORES, ALERTS = f"{CATALOG}.ml.molding_stream_scores", f"{CATALOG}.ml.molding_stream_alerts"
model, thr, version = p2p_ml.load_champion(MODEL)
wo = spark.read.parquet(L + "transactions/work_orders.parquet").toPandas()
molds = spark.read.parquet(L + "master/molds.parquet").toPandas()
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ml.checkpoints")

agg = (spark.readStream.table(f"{CATALOG}.silver.telemetry")
       .filter(Fn.col("asset_type") == "injection_molding_machine").filter(Fn.col("signal").isin(p2p_ml.SIGNALS))
       .withWatermark("event_time", "2 minutes")
       .groupBy("asset_id", "signal", Fn.window("event_time", "1 minute").alias("w"))
       .agg(Fn.avg("value").alias("mean_v"), Fn.min("value").alias("min_v"), Fn.max("value").alias("max_v"))
       .select("asset_id", Fn.col("w.start").alias("minute"), "signal", "mean_v", "min_v", "max_v"))

def process(batch_df, batch_id):
    a = batch_df.toPandas()
    if a.empty:
        return
    a["minute"] = pd.to_datetime(a["minute"], utc=True)
    try:
        recent = spark.table(SCORES).filter(Fn.col("bucket_ts") >= (a["minute"].min() - pd.Timedelta(minutes=30)).to_pydatetime()).toPandas()
    except Exception:
        recent = pd.DataFrame(columns=["asset_id", "bucket_ts", "score"])
    new, runs, stats = p2p_ml.stream_microbatch(a, recent, model, thr, wo, molds)
    now = pd.Timestamp.now(tz="UTC")
    if len(new):
        out = new.assign(model_version=version, scored_at=now, latency_s=(now - new["bucket_ts"]).dt.total_seconds())
        spark.createDataFrame(out[["asset_id", "bucket_ts", "score", "score_smooth", "flag", "model_version", "scored_at", "latency_s"]]).write.mode("append").saveAsTable(SCORES)
    if len(runs):
        spark.createDataFrame(runs.assign(model_version=version, raised_at=now)).write.mode("append").saveAsTable(ALERTS)
    print(batch_id, stats, "alerts:", len(runs))

q = (agg.writeStream.outputMode("append").foreachBatch(process).queryName("ml_stream_inference")
     .option("checkpointLocation", f"/Volumes/{CATALOG}/ml/checkpoints/stream_inference")
     .trigger(processingTime="30 seconds").start())

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS scored_minutes, count(DISTINCT asset_id) AS machines, round(percentile(latency_s, 0.5)) AS median_latency_s, sum(cast(flag AS INT)) AS flagged_minutes FROM {SCORES}"))
