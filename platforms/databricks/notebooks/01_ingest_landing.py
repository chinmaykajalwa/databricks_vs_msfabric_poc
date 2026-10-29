# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Batch ingest from the ADLS Gen2 landing zone -> bronze Delta tables
# MAGIC Reads exactly the files Fabric reads (`landing/p2p/<profile>/`). Parquet only (CSV copies are for humans).
# MAGIC Ends with a **reconciliation against the generator's manifests**: same row counts on both platforms is
# MAGIC the fairness condition of the comparison.
# MAGIC
# MAGIC Prerequisite: a Unity Catalog external location covering the landing container (see README).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json, re
from pyspark.sql import functions as F

for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")

def parquet_files(folder):
    return sorted(f.path for f in dbutils.fs.ls(LANDING + folder) if f.path.endswith(".parquet"))

def load_table(path, table):
    df = spark.read.parquet(path)
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))
    return spark.table(table).count()

loaded = {}

# COMMAND ----------

# MAGIC %md ## Master data, transactions, reference dims

# COMMAND ----------

for folder, prefix in (("master/", "master_"), ("transactions/", "txn_"), ("reference/", "ref_")):
    for path in parquet_files(folder):
        name = path.rsplit("/", 1)[-1].removesuffix(".parquet")
        loaded[(folder.strip("/"), name)] = load_table(path, f"{BRONZE}.{prefix}{name}")
        print(f"{BRONZE}.{prefix}{name:<28} {loaded[(folder.strip('/'), name)]:>10,}")

# tag_dim_fw43 is CSV and deliberately NOT merged into ref_tag_dim: load it as a pending table and
# merge it during the demo, after the firmware 4.3 change appears at minute 20 (schema-evolution moment).
(spark.read.option("header", True).option("inferSchema", True).csv(LANDING + "reference/tag_dim_fw43.csv")
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.ref_tag_dim_fw43_pending"))

# COMMAND ----------

# MAGIC %md ## Documents (Markdown, for retrieval) and their index + calibration records

# COMMAND ----------

loaded[("documents", "docs_index")] = load_table(LANDING + "documents/docs_index.parquet", f"{BRONZE}.docs_index")
loaded[("documents", "calibration_records")] = load_table(LANDING + "documents/records/calibration_records.parquet",
                                                          f"{BRONZE}.calibration_records")
docs = (spark.read.format("binaryFile").option("pathGlobFilter", "*.md").option("recursiveFileLookup", "true")
        .load(LANDING + "documents/")
        .select(F.regexp_extract("path", r"/([^/]+)\.md$", 1).alias("doc_id"),
                F.regexp_extract("path", r"/documents/([^/]+)/", 1).alias("folder"),
                F.col("content").cast("string").alias("text"), "path"))
docs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE}.documents_text")
print("documents:", spark.table(f"{BRONZE}.documents_text").count())

# COMMAND ----------

# MAGIC %md ## Telemetry history (Hive-partitioned `topic=…/date=…`)

# COMMAND ----------

for kind in ("agg", "raw"):
    base = LANDING + f"telemetry/{kind}/"
    df = spark.read.option("basePath", base).parquet(base)        # adds `topic` and `date` columns
    (df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("topic", "date")
       .saveAsTable(f"{BRONZE}.telemetry_{kind}_history"))
    print(kind, spark.table(f"{BRONZE}.telemetry_{kind}_history").count())

# COMMAND ----------

# MAGIC %md ## Reconciliation against the generator's manifests

# COMMAND ----------

bad = []
tm = json.loads(dbutils.fs.head(LANDING + "telemetry/_manifest.json", 10 * 1024 * 1024))
for folder in ("master", "transactions", "reference"):
    if folder == "reference":          # reference row counts are recorded in the telemetry manifest
        tables = tm["reference"]
    else:
        tables = json.loads(dbutils.fs.head(LANDING + f"{folder}/_manifest.json", 10 * 1024 * 1024))["tables"]
    for name, info in tables.items():
        got = loaded.get((folder, name))
        status = "OK" if got == info["rows"] else "MISMATCH"
        if status != "OK":
            bad.append((folder, name, info["rows"], got))
        print(f"{status:<8} {folder}/{name:<28} manifest {info['rows']:>10,}  delta {got}")
for kind in ("agg", "raw"):
    want = sum(v for k, v in tm["rows"].items() if k.startswith(kind + "/"))
    got = spark.table(f"{BRONZE}.telemetry_{kind}_history").count()
    print(f"{'OK' if want == got else 'MISMATCH':<8} telemetry/{kind:<24} manifest {want:>10,}  delta {got:,}")
    if want != got:
        bad.append(("telemetry", kind, want, got))
assert not bad, f"Row counts differ from the generator manifests: {bad}"
print("Reconciliation OK: Databricks holds exactly what the generator wrote.")
