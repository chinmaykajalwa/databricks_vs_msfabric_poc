# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · Streaming: bronze.eventhub_raw -> silver.telemetry
# MAGIC One row per reading; 15-min event-time watermark (late data); de-duplicated on (tag_id, event_time);
# MAGIC enriched from `ref_tag_dim`; unknown tags (firmware 4.3) kept and flagged; `arrival_lag_ms` exposes the
# MAGIC clock-skewed gateway. Separate from 02a so it can be restarted alone (minute 20) without touching
# MAGIC ingestion. Needs DBR 14.3 LTS+.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType)

ENVELOPE = StructType([
    StructField("src", StringType()), StructField("topic", StringType()), StructField("site", StringType()),
    StructField("sent_ms", LongType()), StructField("n", IntegerType()),
    StructField("records", ArrayType(StructType([
        StructField("t", LongType()), StructField("g", IntegerType()), StructField("v", DoubleType()),
        StructField("q", StringType()),
        StructField("fw", StringType()),      # only present after the firmware 4.3 upgrade -> null before
    ]))),
])

# MAGIC %md ## Silver: one row per reading

# COMMAND ----------

tag_dim = spark.table(f"{BRONZE}.ref_tag_dim").select(
    "tag_id", "tag_name", "asset_id", "asset_type", "site_id", "signal", "unit", "criticality", "min_valid", "max_valid")

silver = (spark.readStream.table(f"{BRONZE}.eventhub_raw")
    .withColumn("env", F.from_json("body", ENVELOPE))
    .select("eh_topic", "enqueued_time", "ingest_time", F.col("env.site").alias("site"),
            F.expr("timestamp_millis(env.sent_ms)").alias("sent_time"), F.explode("env.records").alias("r"))
    .select("eh_topic", "site", "sent_time", "enqueued_time", "ingest_time",
            F.expr("timestamp_millis(r.t)").alias("event_time"),
            F.col("r.g").alias("tag_id"), F.col("r.v").alias("value"), F.col("r.q").alias("quality"),
            F.col("r.fw").alias("fw"),
            (F.expr("unix_millis(enqueued_time)") - F.col("r.t")).alias("arrival_lag_ms"))
    # Late records arrive up to 10 min after their event time; 15 min watermark keeps them.
    .withWatermark("event_time", "15 minutes")
    # ~0.1% duplicates from at-least-once delivery.
    .dropDuplicatesWithinWatermark(["tag_id", "event_time"])
    .join(F.broadcast(tag_dim), "tag_id", "left")
    .withColumn("known_tag", F.col("tag_name").isNotNull())
    .withColumn("event_date", F.to_date("event_time")))

silver_q = (silver.writeStream.queryName("silver_telemetry").option("checkpointLocation", f"{CHECKPOINTS}/silver_telemetry")
    .partitionBy("eh_topic", "event_date").trigger(processingTime="10 seconds")
    .toTable(f"{SILVER}.telemetry"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Firmware 4.3 (demo minute 20): schema evolution
# MAGIC New melt-temperature tag IDs appear with `known_tag = false`. Run the next cell to merge
# MAGIC `ref_tag_dim_fw43_pending` into `ref_tag_dim`, then restart ONLY this stream: `silver_q.stop()` and re-run the
# MAGIC silver cell (stream-static joins read the static side at start). Bronze keeps ingesting. Rows ingested before the merge can be back-filled with a MERGE.

# COMMAND ----------

# spark.sql(f"""
#   MERGE INTO {BRONZE}.ref_tag_dim t
#   USING (SELECT * EXCEPT (replaces_tag_id, firmware) FROM {BRONZE}.ref_tag_dim_fw43_pending) s
#   ON t.tag_id = s.tag_id
#   WHEN NOT MATCHED THEN INSERT *""")

# COMMAND ----------

display(spark.sql(f"""
  SELECT eh_topic, count(*) AS readings, max(event_time) AS latest_event, sum(CAST(NOT known_tag AS INT)) AS unknown_tag_rows
  FROM {SILVER}.telemetry GROUP BY eh_topic ORDER BY eh_topic"""))
