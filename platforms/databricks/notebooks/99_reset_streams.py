# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset streams (stop, clear checkpoints, empty stream tables)
# MAGIC Use to switch `starting_offsets` (latest <-> earliest), or to start demo day from a clean slate so
# MAGIC rehearsal runs do not appear as extra incidents. Batch tables from notebook 01 are NOT touched.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("confirm_reset", "no", ["no", "yes"])
assert dbutils.widgets.get("confirm_reset") == "yes", "Set confirm_reset = yes to proceed."

for q in spark.streams.active:          # streams on THIS cluster; stop jobs on other clusters separately
    print("stopping", q.name); q.stop()
for name in ("bronze_eventhub_raw", "silver_telemetry"):
    dbutils.fs.rm(f"{CHECKPOINTS}/{name}", True); print("removed checkpoint", name)
for t in (f"{BRONZE}.eventhub_raw", f"{SILVER}.telemetry"):
    spark.sql(f"DROP TABLE IF EXISTS {t}"); print("dropped", t)
print("Reset done. Set starting_offsets, then start 02a, then 02b.")
