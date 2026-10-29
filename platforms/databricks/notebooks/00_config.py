# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuration (shared)
# MAGIC Included by every other notebook with `%run ./00_config`. Set the widgets once per cluster/job.
# MAGIC
# MAGIC | Widget | Where to get it |
# MAGIC |---|---|
# MAGIC | `landing_path` | `terraform output landing_abfss_path` |
# MAGIC | `eventhub_namespace` | `terraform output eventhub_namespace` |
# MAGIC | `secret_scope` / `secret_key` | the scope holding the **databricks-listen** connection string (see README) |
# MAGIC | `starting_offsets` | `latest` for the demo run (ignores rehearsals); `earliest` to replay the 7-day retention |

# COMMAND ----------

dbutils.widgets.text("catalog", "p2p_poc")
dbutils.widgets.text("profile", "lite")
dbutils.widgets.text("landing_path", "abfss://landing@<storage-account>.dfs.core.windows.net/p2p/lite/")
dbutils.widgets.text("eventhub_namespace", "<eventhub-namespace>")
dbutils.widgets.text("secret_scope", "p2p")
dbutils.widgets.text("secret_key", "eventhub-databricks-listen")
dbutils.widgets.dropdown("starting_offsets", "latest", ["latest", "earliest"])

CATALOG = dbutils.widgets.get("catalog")
PROFILE = dbutils.widgets.get("profile")
LANDING = dbutils.widgets.get("landing_path").rstrip("/") + "/"
NAMESPACE = dbutils.widgets.get("eventhub_namespace")
BRONZE, SILVER, GOLD = f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold"
CHECKPOINTS = f"/Volumes/{CATALOG}/bronze/checkpoints"
TOPICS = ["molding-telemetry", "sterilizer-cycles", "dc-asset-telemetry", "fleet-telemetry"]
CONSUMER_GROUP = "databricks"          # Event Hubs consumer group == Kafka group.id


def kafka_options() -> dict:
    """Spark Kafka source options for the Event Hubs Kafka endpoint.

    Spark tracks offsets in the checkpoint; kafka.group.id pins the Event Hubs consumer
    group so Databricks never competes with Fabric (consumer group "fabric")."""
    conn = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key")).strip()
    problems = [msg for bad, msg in (
        (not conn.startswith("Endpoint=sb://"), "does not start with 'Endpoint=sb://' (did you store the whole JSON map from terraform output?)"),
        ('"' in conn, "contains double quotes, which break the JAAS config"),
        ("SharedAccessKey=" not in conn, "has no SharedAccessKey (truncated?)"),
        ("EntityPath=" in conn, "is scoped to one hub (EntityPath); use the namespace-level databricks-listen rule"),
    ) if bad]
    if problems:
        raise ValueError("Event Hubs secret " + "; ".join(problems))
    jaas = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{conn}";')
    return {
        "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
        "subscribe": ",".join(TOPICS),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.group.id": CONSUMER_GROUP,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
        "startingOffsets": dbutils.widgets.get("starting_offsets"),
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "20000",
    }

print(f"catalog={CATALOG} landing={LANDING} namespace={NAMESPACE}")
