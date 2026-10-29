# Databricks: ingest the landing zone and the four Event Hubs topics

| Notebook | Run | Produces |
|---|---|---|
| `00_config` | included by all others (`%run`) | widgets, Kafka options for Event Hubs |
| `01_ingest_landing` | once (and after each data regeneration) | `bronze.master_*`, `txn_*`, `ref_*`, `docs_index`, `calibration_records`, `documents_text`, `telemetry_{agg,raw}_history`; **reconciles row counts against the generator manifests** |
| `02a_stream_bronze` | start **before** the demo run; leave running | `bronze.eventhub_raw` (raw messages); monitor cell shows rows per batch and offsets |
| `02b_stream_silver` | start after 02a; leave running | `silver.telemetry` (one row per reading, watermark, de-dup, tag enrichment, unknown tags flagged). Restartable alone |
| `99_reset_streams` | to switch latest/earliest or before demo day | stops streams, clears checkpoints, drops the two stream tables |
| `03_gold_alerts_and_hero` | once (views stay current) | alert and data-quality views; hero steam-vs-RH analysis; SOP-derived blast radius |

Import: Workspace -> Import -> the `.py` files (Databricks source format), or connect this repo as a Git folder.

## One-time setup

1. **Unity Catalog access to the landing container.** Create an *Access Connector for Azure Databricks*,
   grant its managed identity **Storage Blob Data Reader** on the storage account
   (`terraform output storage_account`), create a storage credential from it, and an external location on
   `abfss://landing@<account>.dfs.core.windows.net/`. Do **not** create one for `groundtruth`.
2. **Catalog:** `CREATE CATALOG IF NOT EXISTS p2p_poc;` (notebook 01 creates the schemas and the checkpoint volume).
3. **Secret for the listen connection string**: store ONLY the databricks value, not the JSON map (its
   quotes break the Kafka JAAS config: "Value not specified for key 'databricks'"). PowerShell:
   ```powershell
   $conn = (terraform output -json platform_listen_connection_strings | ConvertFrom-Json).databricks
   databricks secrets create-scope p2p
   databricks secrets put-secret p2p eventhub-databricks-listen --string-value $conn
   ```
4. **Compute:** a classic all-purpose or job cluster, DBR **14.3 LTS or newer** (Spark 3.5 for
   `dropDuplicatesWithinWatermark`); single node is enough for lite. Kafka sources on serverless depend on
   your workspace's serverless features; use classic compute unless you have confirmed support.
5. Set the `00_config` widgets: `landing_path` = `terraform output landing_abfss_path`,
   `eventhub_namespace` = `terraform output eventhub_namespace`.

## Demo run

1. Run `99_reset_streams` (clean slate), set `starting_offsets = latest`, start `02a`, then `02b`.
   For a production-like setup, run 02a and 02b as two tasks of one Databricks Job (continuous, with retries).
2. Start the publisher on the VM (`sudo systemctl start p2p-publisher`).
3. Watch the `gold.v_alert_*` views (Databricks SQL dashboard / SQL Alerts):
   minute 5 sorter, 15 RH probe, 30 reefer (sustained > 15 min), 45 site silence.
4. **Minute 20, schema evolution:** new tag IDs show in `gold.v_unknown_tags`. Run the commented MERGE cell in
   notebook 02b, then restart only the silver stream (a stream-static join reads the static side at start).
   Note this honestly in the comparison: Fabric's KQL lookups resolve new tags at query time, no restart.

## What is verified and what is not

The hero blast-radius SQL was executed (DuckDB, same SQL) against the generated files and matches
`hero_blast_radius.json` exactly. The Spark/Kafka/Unity Catalog code has **not** been run on a Databricks
workspace from here: expect to adjust small details (permissions, widget values) on the first run.
## Troubleshooting: stream running but `eventhub_raw` empty
`starting_offsets = latest` reads only messages published after the stream's FIRST start; anything published
earlier (e.g. rehearsals) is skipped by design, and changing the widget later has no effect because the
checkpoint decides. Either start the publisher (rows appear within seconds), or run `99_reset_streams`, set
`earliest`, and restart 02a to replay the 7-day retention. The monitor cell in 02a shows rows per batch and the
offsets reached per partition.
