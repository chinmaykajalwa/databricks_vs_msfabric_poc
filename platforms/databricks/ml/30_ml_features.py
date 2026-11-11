# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · ML features (job)
# MAGIC Builds the molding feature table ONCE; training (31) and batch inference (32) both read it, so training and
# MAGIC serving use identical features. Streaming (33) computes the same features with the same shared functions.
# MAGIC Needs `p2p_ml.py` in this folder and a Databricks Runtime ML cluster (15.4 LTS ML+).

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("landing_ml_path", "abfss://landing@p2ppocstxbohi5.dfs.core.windows.net/p2p/lite90/")
CATALOG, L = dbutils.widgets.get("catalog"), dbutils.widgets.get("landing_ml_path").rstrip("/") + "/"
import time, p2p_ml
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ml")
t = time.time()
spark.read.parquet(L + "telemetry/agg/topic=molding-telemetry").createOrReplaceTempView("agg")
spark.read.parquet(L + "reference/tag_dim.parquet").createOrReplaceTempView("tag_dim")
spark.read.parquet(L + "transactions/work_orders.parquet").createOrReplaceTempView("work_orders")
spark.read.parquet(L + "master/molds.parquet").createOrReplaceTempView("molds")
F = p2p_ml.finish_features(spark.sql(p2p_ml.feature_sql("agg", "tag_dim", "work_orders", "molds")).toPandas())
(spark.createDataFrame(F).write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.ml.molding_features"))
print(f"{len(F):,} machine-minutes, {F.asset_id.nunique()} machines, {time.time() - t:.0f}s")
