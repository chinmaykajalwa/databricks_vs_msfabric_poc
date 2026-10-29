# Databricks notebook source
# MAGIC %md
# MAGIC # 02a · Streaming: Event Hubs (Kafka) -> bronze.eventhub_raw
# MAGIC One row per Event Hubs message, body untouched (replayable, auditable). Runs in the background:
# MAGIC this cell returns immediately; the query keeps running until stopped or the cluster stops.
# MAGIC
# MAGIC **Offsets:** `starting_offsets` applies only the FIRST time a checkpoint is created. After that the
# MAGIC checkpoint decides. `latest` = only messages published after the first start (demo day);
# MAGIC `earliest` = replay the 7-day retention (testing). To switch, run `99_reset_streams` first.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Bronze: raw messages

# COMMAND ----------

bronze_q = (spark.readStream.format("kafka").options(**kafka_options()).load()
    .select(F.col("topic").alias("eh_topic"), "partition", "offset",
            F.col("timestamp").alias("enqueued_time"), F.col("key").cast("string").alias("site_key"),
            F.col("value").cast("string").alias("body"), F.current_timestamp().alias("ingest_time"))
    .writeStream.queryName("bronze_eventhub_raw").option("checkpointLocation", f"{CHECKPOINTS}/bronze_eventhub_raw")
    .trigger(processingTime="5 seconds").toTable(f"{BRONZE}.eventhub_raw"))

# COMMAND ----------


# COMMAND ----------

# MAGIC %md ## Monitor (re-run any time)

# COMMAND ----------

import json
for q in spark.streams.active:
    p = q.lastProgress or {}
    print(q.name, "| active:", q.isActive, "| status:", q.status["message"],
          "| last batch rows:", p.get("numInputRows"), "| rows/s:", p.get("processedRowsPerSecond"))
    for s in p.get("sources", []):
        print("   offsets:", json.dumps(s.get("endOffset"))[:300])
print("eventhub_raw rows:", spark.table(f"{BRONZE}.eventhub_raw").count())
