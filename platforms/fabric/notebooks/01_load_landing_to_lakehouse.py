# Fabric notebook (PySpark). Attach the Lakehouse that holds the `landing` OneLake shortcut (see README).
# Mirrors platforms/databricks/notebooks/01_ingest_landing.py: same files, same table names, same
# reconciliation against the generator's manifests -- the fairness condition.

import json
from pyspark.sql import functions as F

PROFILE = "lite"
LANDING = f"Files/landing/p2p/{PROFILE}/"          # shortcut name "landing" under Files/

def ls_parquet(folder):
    return sorted(f.path for f in notebookutils.fs.ls(LANDING + folder) if f.path.endswith(".parquet"))

def load_table(path, table):
    spark.read.parquet(path).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    return spark.table(table).count()

loaded = {}
for folder, prefix in (("master/", "master_"), ("transactions/", "txn_"), ("reference/", "ref_")):
    for path in ls_parquet(folder):
        name = path.rsplit("/", 1)[-1].removesuffix(".parquet")
        loaded[(folder.strip("/"), name)] = load_table(path, f"{prefix}{name}")
        print(f"{prefix}{name:<28} {loaded[(folder.strip('/'), name)]:>10,}")

(spark.read.option("header", True).option("inferSchema", True).csv(LANDING + "reference/tag_dim_fw43.csv")
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("ref_tag_dim_fw43_pending"))

loaded[("documents", "docs_index")] = load_table(LANDING + "documents/docs_index.parquet", "docs_index")
loaded[("documents", "calibration_records")] = load_table(LANDING + "documents/records/calibration_records.parquet",
                                                          "calibration_records")
(spark.read.format("binaryFile").option("pathGlobFilter", "*.md").option("recursiveFileLookup", "true")
      .load(LANDING + "documents/")
      .select(F.regexp_extract("path", r"/([^/]+)\.md$", 1).alias("doc_id"),
              F.regexp_extract("path", r"/documents/([^/]+)/", 1).alias("folder"),
              F.col("content").cast("string").alias("text"), "path")
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("documents_text"))

for kind in ("agg", "raw"):
    base = LANDING + f"telemetry/{kind}/"
    (spark.read.option("basePath", base).parquet(base)
          .write.mode("overwrite").option("overwriteSchema", "true").partitionBy("topic", "date")
          .saveAsTable(f"telemetry_{kind}_history"))

def head_json(path):
    return json.loads("".join(r.value for r in spark.read.text(path).collect()))

bad = []
tm = head_json(LANDING + "telemetry/_manifest.json")
for folder in ("master", "transactions", "reference"):
    tables = tm["reference"] if folder == "reference" else head_json(LANDING + f"{folder}/_manifest.json")["tables"]
    for name, info in tables.items():
        if loaded.get((folder, name)) != info["rows"]:
            bad.append((folder, name, info["rows"], loaded.get((folder, name))))
for kind in ("agg", "raw"):
    want = sum(v for k, v in tm["rows"].items() if k.startswith(kind + "/"))
    got = spark.table(f"telemetry_{kind}_history").count()
    if want != got:
        bad.append(("telemetry", kind, want, got))
assert not bad, f"Row counts differ from the generator manifests: {bad}"
print("Reconciliation OK: the Lakehouse holds exactly what the generator wrote.")
