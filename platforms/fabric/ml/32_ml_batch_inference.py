# Fabric notebook. Same as Databricks 32. Schedule it: notebook Run -> Schedule (or a Data pipeline notebook activity),
# parameters score_start / score_end (mark the cell below as a parameter cell).
score_start, score_end = "", ""   # parameters; empty = last 7 days of the feature table
import os, sys, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
sys.path.append("/lakehouse/default/Files/code")
import pandas as pd, p2p_ml
from pyspark.sql import functions as Fn
t = time.time()
model, thr, version = p2p_ml.load_champion("molding_anomaly")
feats = spark.table("ml_molding_features")
end = pd.Timestamp(score_end or feats.agg(Fn.max("bucket_ts")).first()[0], tz="UTC")
start = pd.Timestamp(score_start, tz="UTC") if score_start else end - pd.Timedelta(days=7)
X = feats.filter((Fn.col("bucket_ts") >= (start - pd.Timedelta(minutes=20)).to_pydatetime()) & (Fn.col("bucket_ts") <= end.to_pydatetime())).toPandas()
flagged = p2p_ml.smooth_flags(p2p_ml.score_features(model, X), thr)
flagged = flagged[flagged["bucket_ts"] >= start].assign(model_version=version)
runs = p2p_ml._alert_runs(flagged).assign(model_version=version)
spark.sql("CREATE TABLE IF NOT EXISTS ml_molding_scores (asset_id STRING, bucket_ts TIMESTAMP, score DOUBLE, score_smooth DOUBLE, flag BOOLEAN, model_version STRING)")
spark.sql(f"DELETE FROM ml_molding_scores WHERE bucket_ts >= '{start}' AND bucket_ts <= '{end}'")
spark.createDataFrame(flagged[["asset_id", "bucket_ts", "score", "score_smooth", "flag", "model_version"]]).write.mode("append").saveAsTable("ml_molding_scores")
if len(runs):
    spark.createDataFrame(runs).write.mode("append").saveAsTable("ml_molding_alerts")
print(f"champion v{version}: scored {len(flagged):,} machine-minutes {start} -> {end}, {len(runs)} alerts, {time.time() - t:.0f}s")
