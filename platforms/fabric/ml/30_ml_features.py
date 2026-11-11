# Fabric notebook (PySpark). Upload ml/p2p_ml.py to the Lakehouse under Files/code/. Same as Databricks 30_ml_features.
import sys, time
sys.path.append("/lakehouse/default/Files/code")
import p2p_ml
L = "Files/landing/p2p/lite90/"
t = time.time()
spark.read.parquet(L + "telemetry/agg/topic=molding-telemetry").createOrReplaceTempView("agg")
spark.read.parquet(L + "reference/tag_dim.parquet").createOrReplaceTempView("tag_dim")
spark.read.parquet(L + "transactions/work_orders.parquet").createOrReplaceTempView("work_orders")
spark.read.parquet(L + "master/molds.parquet").createOrReplaceTempView("molds")
F = p2p_ml.finish_features(spark.sql(p2p_ml.feature_sql("agg", "tag_dim", "work_orders", "molds")).toPandas())
spark.createDataFrame(F).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ml_molding_features")
print(f"{len(F):,} machine-minutes, {F.asset_id.nunique()} machines, {time.time() - t:.0f}s")
