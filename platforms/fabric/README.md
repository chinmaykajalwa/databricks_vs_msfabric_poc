# Microsoft Fabric: Lakehouse for the batch data, Eventhouse (KQL) for the stream

```
ADLS Gen2 landing ──OneLake shortcut──> Lakehouse ──notebook 01──> Delta tables (+ reconciliation)
                                                    └─notebook 02──> hero steam analysis, blast radius
Event Hubs (4 hubs, consumer group "fabric") ──Eventstream──> Eventhouse: RawEnvelope
      ──update policy──> Telemetry ──materialized views──> TelemetryDedup, TelemetryMinute
      ──KQL queries──> Real-Time Dashboard + Activator alerts
```

Same files, same stream and the same rules as the Databricks side, so differences are the platform's.

> **Ingestion from Event Hubs is configured in the Eventstream UI (section 2, step 4), not in the KQL files.**
> The KQL files define where data lands, how it is transformed, and how it is queried. (Databricks defines
> ingestion in code; worth noting in the comparison.)
## 1. Lakehouse (batch)

1. Create a workspace on your Fabric capacity (same region as the storage account) and a Lakehouse, e.g.
   `p2p_lakehouse`.
2. **Files -> New shortcut -> Azure Data Lake Storage Gen2.** URL `https://<account>.dfs.core.windows.net`
   (`terraform output storage_account`); authenticate with an organizational account or service principal that
   has **Storage Blob Data Reader** on the account; select the `landing` container; name the shortcut `landing`.
   Never shortcut `groundtruth`.
3. Import and run `notebooks/01_load_landing_to_lakehouse.py` (attach the Lakehouse). It loads the same tables as
   Databricks, with the same names, and asserts the row counts match the generator manifests.
4. Run `notebooks/02_hero_analytics.py`: steam vs reported RH per load, and the SOP-derived blast radius.

## 2. Eventhouse (stream)

1. Create an **Eventhouse** (with its KQL database), e.g. `p2p_eventhouse`.
2. Open a **KQL queryset** on the database (KQL does not run in notebooks) and run `kql/01_setup.kql` **one command
   at a time** (cursor in the block, Shift+Enter): tables, JSON mapping, update policy, `TagDim`, the two
   materialized views. Verify with `.show tables` and `.show materialized-views`.
3. **Load `TagDim`:** download `landing/p2p/lite/reference/tag_dim.csv` from the storage account (Portal ->
   Containers -> Download), then Get data -> **Local file** -> existing table `TagDim`, CSV, first row is header.
   `TagDim | count` = 815. (The OneLake picker may not list files behind a shortcut.) Download
   `tag_dim_fw43.csv` too, for minute 20 of the demo.
4. **Eventstream:** create one, add **four Azure Event Hubs sources** (one per hub: `molding-telemetry`,
   `sterilizer-cycles`, `dc-asset-telemetry`, `fleet-telemetry`) with
   - namespace: `terraform output eventhub_namespace`
   - shared access key name `fabric-listen`, key from:
     `az eventhubs namespace authorization-rule keys list -g <rg> --namespace-name <ns> --authorization-rule-name fabric-listen --query primaryKey -o tsv`
   - consumer group **`fabric`**, data format **JSON**.
   All four sources in ONE Eventstream, all connected to the stream node.
5. Add an **Eventhouse destination** in **Direct ingestion** mode (not "event processing before ingestion"), choose
   the Eventhouse and KQL database, JSON. **Publish.** The table and mapping are chosen AFTER publishing:
6. **Start a publisher run on the VM first** (the wizard samples live events; with nothing flowing it shows "No data
   to preview"). Then in **Live view** click **Configure** on the Eventhouse node: existing table `RawEnvelope`,
   JSON, existing mapping `RawEnvelopeMapping` (or auto-generated, with `records` as `dynamic`). Finish; the node
   turns Active.
7. Verify: `RawEnvelope | count`, then `Telemetry | summarize readings = count() by topic` (all four topics).

## 3. Dashboards and alerts

Pin the queries in `kql/02_alerts_and_monitors.kql` to a **Real-Time Dashboard**. Attach **Activator** alerts to
the four marked `[ALERT]`: RH probe (minute 15), sorter vibration (5), reefer sustained > 15 min (30),
site silence (45). The data-quality tiles show the clock-skewed gateway, late arrivals and removed duplicates.

**Minute 20, schema evolution:** the unknown-tags query lists the firmware 4.3 tag IDs. Append
`reference/tag_dim_fw43.csv` to `TagDim` (Get data, existing table; extra columns `replaces_tag_id`, `firmware`
are ignored by the column mapping) and the list empties immediately: lookups resolve at query time.

## What is verified and what is not

The hero blast-radius SQL was executed (DuckDB, same SQL) against the generated files and matches the ground
truth exactly. The KQL and the Fabric notebooks have **not** been run in a Fabric workspace from here, and
Eventstream's connector screens change between releases: follow the intent of each step if a label differs.
