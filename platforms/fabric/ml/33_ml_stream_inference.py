# Fabric notebook. Same as Databricks 33, reading Event Hubs DIRECTLY (Spark Kafka connector) through its own consumer
# group `fabric-ml` (so it does not compete with the Eventstream on `fabric`; add it via terraform consumer_groups).
# Secret: store the fabric-listen connection string in Azure Key Vault and read it with notebookutils (below).
import os, sys
os.environ.setdefault("KERAS_BACKEND", "tensorflow")
sys.path.append("/lakehouse/default/Files/code")
import pandas as pd, p2p_ml
from pyspark.sql import functions as Fn
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType
NAMESPACE, KEY_VAULT, SECRET = "p2ppoc-ehns-xbohi5", "https://<your-key-vault>.vault.azure.net/", "eventhub-fabric-listen"
conn = notebookutils.credentials.getSecret(KEY_VAULT, SECRET)
L = "Files/landing/p2p/lite90/"
model, thr, version = p2p_ml.load_champion("molding_anomaly")
wo = spark.read.parquet(L + "transactions/work_orders.parquet").toPandas()
molds = spark.read.parquet(L + "master/molds.parquet").toPandas()
tags = spark.table("ref_tag_dim").filter(Fn.col("asset_kind") == "molding_machine").filter(Fn.col("signal").isin(p2p_ml.SIGNALS)).select("tag_id", "asset_id", "signal")
ENV = StructType([StructField("site", StringType()), StructField("records", ArrayType(StructType([
    StructField("t", LongType()), StructField("g", IntegerType()), StructField("v", DoubleType()), StructField("q", StringType())])))])
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", f"{NAMESPACE}.servicebus.windows.net:9093").option("subscribe", "molding-telemetry")
       .option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.mechanism", "PLAIN")
       .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{conn}";')
       .option("kafka.group.id", "fabric-ml").option("startingOffsets", "latest").option("failOnDataLoss", "false").load())
readings = (raw.select(Fn.from_json(Fn.col("value").cast("string"), ENV).alias("e")).select(Fn.explode("e.records").alias("r"))
            .select(Fn.expr("timestamp_millis(r.t)").alias("event_time"), Fn.col("r.g").alias("tag_id"), Fn.col("r.v").alias("value"))
            .withWatermark("event_time", "2 minutes").dropDuplicatesWithinWatermark(["tag_id", "event_time"])
            .join(Fn.broadcast(tags), "tag_id"))
agg = (readings.groupBy("asset_id", "signal", Fn.window("event_time", "1 minute").alias("w"))
       .agg(Fn.avg("value").alias("mean_v"), Fn.min("value").alias("min_v"), Fn.max("value").alias("max_v"))
       .select("asset_id", Fn.col("w.start").alias("minute"), "signal", "mean_v", "min_v", "max_v"))

def process(batch_df, batch_id):
    a = batch_df.toPandas()
    if a.empty:
        return
    a["minute"] = pd.to_datetime(a["minute"], utc=True)
    try:
        recent = spark.table("ml_molding_stream_scores").filter(Fn.col("bucket_ts") >= (a["minute"].min() - pd.Timedelta(minutes=30)).to_pydatetime()).toPandas()
    except Exception:
        recent = pd.DataFrame(columns=["asset_id", "bucket_ts", "score"])
    new, runs, stats = p2p_ml.stream_microbatch(a, recent, model, thr, wo, molds)
    now = pd.Timestamp.now(tz="UTC")
    if len(new):
        out = new.assign(model_version=version, scored_at=now, latency_s=(now - new["bucket_ts"]).dt.total_seconds())
        spark.createDataFrame(out[["asset_id", "bucket_ts", "score", "score_smooth", "flag", "model_version", "scored_at", "latency_s"]]).write.mode("append").saveAsTable("ml_molding_stream_scores")
    if len(runs):
        spark.createDataFrame(runs.assign(model_version=version, raised_at=now)).write.mode("append").saveAsTable("ml_molding_stream_alerts")
    print(batch_id, stats, "alerts:", len(runs))

q = (agg.writeStream.outputMode("append").foreachBatch(process).option("checkpointLocation", "Files/checkpoints/ml_stream_inference")
     .trigger(processingTime="30 seconds").start())
# For production, run this as a Spark job definition with a retry policy rather than a notebook.
