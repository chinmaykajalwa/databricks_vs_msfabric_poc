# Fabric notebook (PySpark). Identical ML code to Databricks (ml/p2p_ml.py); compare data access, tracking,
# registry, PREDICT and ML model endpoints.
#
# Setup: upload ml/p2p_ml.py to the attached Lakehouse under Files/code/. The `landing` shortcut must show
# Files/landing/p2p/lite90/ (generated and uploaded on the VM with the lite90 profile).
# Environment: if `import keras` fails, run `%pip install "tensorflow>=2.16"` (Keras 3) in the first cell.
# Record wall time of each step (printed) and any friction in the ML scorecard.

import os, sys, time
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
sys.path.append("/lakehouse/default/Files/code")
import mlflow, pandas as pd
import p2p_ml

L = "Files/landing/p2p/lite90/"
EXPERIMENT = "p2p-molding-anomaly"
T = {}

# 1. Features: Spark SQL, then pandas ----------------------------------------------------------
t = time.time()
spark.read.parquet(L + "telemetry/agg/topic=molding-telemetry").createOrReplaceTempView("agg")
spark.read.parquet(L + "reference/tag_dim.parquet").createOrReplaceTempView("tag_dim")
spark.read.parquet(L + "transactions/work_orders.parquet").createOrReplaceTempView("work_orders")
spark.read.parquet(L + "master/molds.parquet").createOrReplaceTempView("molds")
wide = spark.sql(p2p_ml.feature_sql("agg", "tag_dim", "work_orders", "molds")).toPandas()
F = p2p_ml.finish_features(wide); del wide
T["features_s"] = round(time.time() - t, 1)
print(len(F), "machine-minutes;", T)

# 2. Train, log, register (Fabric experiment + ML model items) ----------------------------------
t = time.time()
M = p2p_ml.train_all(F, train_days=45, epochs=6)
T["train_s"] = round(time.time() - t, 1)
t = time.time()
versions = p2p_ml.log_and_register(M, F, {"iforest": "molding_iforest", "autoenc": "molding_autoenc"}, EXPERIMENT)
T["log_register_s"] = round(time.time() - t, 1)
print(versions, M["thresholds"], T)

# 3. Score with shared code, shared alert rule; save table + export raw scores for offline evaluation
t = time.time()
S = p2p_ml.score_all(M, F)
A = p2p_ml.alerts(S, M["train_cut"])
T["score_s"] = round(time.time() - t, 1)
(spark.createDataFrame(A[["asset_id", "bucket_ts", "model", "score", "score_smooth", "flag"]])
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ml_molding_scores"))
os.makedirs("/lakehouse/default/Files/exports", exist_ok=True)
S.to_parquet("/lakehouse/default/Files/exports/molding_scores_fabric.parquet")
print(A.groupby("model")["flag"].sum(), T)

# 4. Platform-native batch scoring: PREDICT (MLFlowTransformer) -------------------------------------
from synapse.ml.predict import MLFlowTransformer
t = time.time()
sdf = spark.createDataFrame(F[p2p_ml.FEATURES].astype("float64"))
pred = MLFlowTransformer(inputCols=p2p_ml.FEATURES, outputCol="prediction",
                         modelName="molding_iforest", modelVersion=int(versions["iforest"]))
n = pred.transform(sdf).filter("prediction = -1").count()
T["predict_iforest_s"] = round(time.time() - t, 1)
print("iforest -1 labels:", n, T)
# Try the same for the autoencoder (modelName="molding_autoenc"); note if PREDICT rejects the Keras flavour.

# 5. Real-time: ML model endpoints (preview) --------------------------------------------------------
# EXPECTATION TO TEST: Isolation Forest (scikit-learn, column schema) should activate. The Keras autoencoder is
# logged with a TENSOR signature (required by MLflow's Keras flavour); Fabric documents scalar/column schemas for
# endpoints, so it may refuse -- record exactly what happens (that is a comparison finding).
# Open the ML model item molding_autoenc (and molding_iforest) -> version -> Activate endpoint (the tenant switch
# for ML model endpoints must be on). Use the endpoint's sample request from the UI 20 times; record median latency,
# activation time, and any schema/flavour errors. Deactivate afterwards: active endpoints consume capacity units.
