# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · ML: molding anomaly detection (Isolation Forest vs Keras autoencoder vs baseline)
# MAGIC Shared code: `ml/p2p_ml.py` -- upload it to the SAME workspace folder as this notebook (it is then
# MAGIC importable). Identical code runs on Fabric (`platforms/fabric/ml/10_ml_molding.py`); only data access,
# MAGIC tracking, registry and serving differ -- which is what we compare.
# MAGIC
# MAGIC **Compute:** a Databricks Runtime **ML** cluster (15.4 LTS ML or newer: TensorFlow/Keras 3, scikit-learn, MLflow
# MAGIC preinstalled), single node is enough. Data: the `lite90` profile in the landing zone (`.../p2p/lite90/`).
# MAGIC
# MAGIC Record in the ML scorecard: wall time of each step (printed), effort, and any friction.

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("landing_ml_path", "abfss://landing@<storage-account>.dfs.core.windows.net/p2p/lite90/")
dbutils.widgets.text("experiment", "/Shared/p2p-molding-anomaly")
CATALOG = dbutils.widgets.get("catalog")
L = dbutils.widgets.get("landing_ml_path").rstrip("/") + "/"
EXPERIMENT = dbutils.widgets.get("experiment")

import os, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
import mlflow, pandas as pd
import p2p_ml
mlflow.set_registry_uri("databricks-uc")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ml")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ml.exports")
T = {}

# COMMAND ----------

# MAGIC %md ## 1. Features: heavy reshaping in Spark SQL, finishing in pandas

# COMMAND ----------

t = time.time()
spark.read.parquet(L + "telemetry/agg/topic=molding-telemetry").createOrReplaceTempView("agg")
spark.read.parquet(L + "reference/tag_dim.parquet").createOrReplaceTempView("tag_dim")
spark.read.parquet(L + "transactions/work_orders.parquet").createOrReplaceTempView("work_orders")
spark.read.parquet(L + "master/molds.parquet").createOrReplaceTempView("molds")
wide = spark.sql(p2p_ml.feature_sql("agg", "tag_dim", "work_orders", "molds")).toPandas()
F = p2p_ml.finish_features(wide); del wide
T["features_s"] = round(time.time() - t, 1)
print(len(F), "machine-minutes,", F.asset_id.nunique(), "machines;", T)

# COMMAND ----------

# MAGIC %md ## 2. Train (first 45 days), log to MLflow, register in Unity Catalog

# COMMAND ----------

t = time.time()
M = p2p_ml.train_all(F, train_days=45, epochs=6)
T["train_s"] = round(time.time() - t, 1)
t = time.time()
versions = p2p_ml.log_and_register(M, F, {"iforest": f"{CATALOG}.ml.molding_iforest",
                                          "autoenc": f"{CATALOG}.ml.molding_autoenc"}, EXPERIMENT)
T["log_register_s"] = round(time.time() - t, 1)
print(versions, M["thresholds"], T)

# COMMAND ----------

# MAGIC %md ## 3. Score everything (shared code) and apply the shared alert rule

# COMMAND ----------

t = time.time()
S = p2p_ml.score_all(M, F)
A = p2p_ml.alerts(S, M["train_cut"])
T["score_s"] = round(time.time() - t, 1)
(spark.createDataFrame(A[["asset_id", "bucket_ts", "model", "score", "score_smooth", "flag"]])
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.molding_scores"))
# raw scores for OFFLINE evaluation on your laptop (ground truth never comes here)
S.to_parquet(f"/Volumes/{CATALOG}/ml/exports/molding_scores_databricks.parquet")
print(A.groupby("model")["flag"].sum(), T)

# COMMAND ----------

# MAGIC %md ## 4. Platform-native batch scoring: MLflow `spark_udf` (measure, compare with Fabric PREDICT)

# COMMAND ----------

t = time.time()
sdf = spark.createDataFrame(F[p2p_ml.FEATURES].astype("float64"))
udf = mlflow.pyfunc.spark_udf(spark, f"models:/{CATALOG}.ml.molding_iforest/{versions['iforest']}", result_type="double")
n = sdf.withColumn("prediction", udf(*p2p_ml.FEATURES)).filter("prediction = -1").count()
T["spark_udf_iforest_s"] = round(time.time() - t, 1)
print("iforest -1 labels:", n, T)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Real-time serving (Model Serving), then measure latency
# MAGIC Serving -> Create serving endpoint -> entity `<catalog>.ml.molding_autoenc` (and `molding_iforest`), the version
# MAGIC above, smallest size, scale to zero ON. Then run the cell below 20 times and record median latency.
# MAGIC The autoencoder returns `anomaly_score`; Isolation Forest returns -1 (anomaly) / 1.

# COMMAND ----------

import requests, statistics
ENDPOINT = "molding-autoenc"     # your endpoint name
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
row = F[p2p_ml.FEATURES].astype("float64").head(1)
lat = []
for _ in range(20):
    t = time.time()
    r = requests.post(f"https://{host}/serving-endpoints/{ENDPOINT}/invocations",
                      headers={"Authorization": f"Bearer {token}"},
                      # autoencoder: tensor input; Isolation Forest endpoint: {"dataframe_split": row.to_dict(orient="split")}
                      json={"inputs": row.astype("float32").values.tolist()}, timeout=60)
    lat.append((time.time() - t) * 1000)
print(r.status_code, r.text[:200], "| median latency ms:", round(statistics.median(lat)))
