# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: live alerts, data-quality monitors, and the hero investigation
# MAGIC Views (not materialised tables), so dashboards and Databricks SQL Alerts always read current data.
# MAGIC Every rule here mirrors a documented one (SOPs / manuals) and has a KQL twin in the Fabric folder.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

T = f"{SILVER}.telemetry"
views = {
"v_minute": f"""
  SELECT tag_id, tag_name, asset_id, site, signal, date_trunc('minute', event_time) AS minute,
         count(*) AS n, avg(value) AS mean_v, min(value) AS min_v, max(value) AS max_v,
         sum(CAST(quality <> 'G' AS INT)) AS n_not_good
  FROM {T} GROUP BY ALL""",
# MAN-STZ-EX alarm A-104 / A-107: RH above high limit or signal quality degraded
"v_alert_rh_probe": f"""
  SELECT asset_id, event_time, value, quality FROM {T}
  WHERE signal = 'chamber_rh_pct' AND (value > 80 OR quality <> 'G')""",
# SOP-DC-501 zone D
"v_alert_sorter_vibration": f"""
  SELECT asset_id, site, event_time, value FROM {T}
  WHERE signal = 'vibration_mm_s' AND asset_type = 'crossbelt_sorter' AND value > 7.1""",
# SOP-LOG-601: above 20 C for MORE than 15 minutes (sustained, not a door-opening bump)
"v_alert_reefer_sustained": f"""
  WITH m AS (SELECT asset_id, minute, min_v FROM {GOLD}.v_minute WHERE signal = 'reefer_temp_c'),
       g AS (SELECT *, sum(CASE WHEN min_v > 20 THEN 0 ELSE 1 END) OVER (PARTITION BY asset_id ORDER BY minute) AS grp FROM m)
  SELECT asset_id, min(minute) AS above_since, max(minute) AS last_minute, count(*) AS minutes_above
  FROM g WHERE min_v > 20 GROUP BY asset_id, grp HAVING count(*) > 15""",
# SOP-DC-510: a silent site is an incident, not "zero"
"v_site_silence": f"""
  SELECT site, eh_topic, max(ingest_time) AS last_seen,
         (unix_timestamp(current_timestamp()) - unix_timestamp(max(ingest_time))) AS silent_seconds
  FROM {T} GROUP BY site, eh_topic HAVING silent_seconds > 120""",
"v_clock_skew": f"""
  SELECT site, eh_topic, percentile_approx(arrival_lag_ms, 0.5) AS median_arrival_lag_ms, count(*) AS n
  FROM {T} GROUP BY site, eh_topic HAVING percentile_approx(arrival_lag_ms, 0.5) < -30000""",
"v_unknown_tags": f"""
  SELECT tag_id, site, fw, min(event_time) AS first_seen, count(*) AS n FROM {T}
  WHERE NOT known_tag GROUP BY tag_id, site, fw""",
}
for name, sql in views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {GOLD}.{name} AS {sql}")
    print("view", f"{GOLD}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hero investigation (what the agent must derive): steam reveals the drift
# MAGIC Per load on a chamber: reported RH during exposure vs conditioning steam (first 20-60 min of the cycle).
# MAGIC Reported RH stays flat; steam falls. MAN-STZ-EX §4.3: >10 % sustained drop = suspected RH probe drift.

# COMMAND ----------

dbutils.widgets.text("sterilizer_id", "STZ-P01-01")
STZ = dbutils.widgets.get("sterilizer_id")
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.v_steam_vs_reported_rh AS
WITH tags AS (SELECT tag_id, asset_id, signal FROM {BRONZE}.ref_tag_dim
              WHERE asset_kind = 'sterilizer' AND signal IN ('steam_flow_kg_h', 'chamber_rh_pct')),
     h AS (SELECT a.bucket_ts, a.mean, t.asset_id, t.signal
           FROM {BRONZE}.telemetry_agg_history a JOIN tags t USING (tag_id))
SELECT l.load_id, l.sterilizer_id, l.start_ts,
       avg(CASE WHEN h.signal = 'steam_flow_kg_h' AND h.bucket_ts >= l.start_ts + INTERVAL 20 MINUTES
                 AND h.bucket_ts <  l.start_ts + INTERVAL 60 MINUTES THEN h.mean END) AS conditioning_steam_kg_h,
       avg(CASE WHEN h.signal = 'chamber_rh_pct' AND h.bucket_ts >= l.precond_end_ts
                 AND h.bucket_ts < l.exposure_end_ts THEN h.mean END) AS reported_rh_exposure
FROM {BRONZE}.txn_sterilization_loads l
JOIN h ON h.asset_id = l.sterilizer_id AND h.bucket_ts BETWEEN l.start_ts AND l.exposure_end_ts
WHERE l.status = 'completed'
GROUP BY l.load_id, l.sterilizer_id, l.start_ts""")
display(spark.sql(f"""SELECT * FROM {GOLD}.v_steam_vs_reported_rh WHERE sterilizer_id = '{STZ}'
                      ORDER BY start_ts DESC LIMIT 40"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hero blast radius, derived the SOP way (SOP-STR-204 §4 + SOP-QA-110)
# MAGIC Suspect window = since the **last passing RH verification** (calibration records) -> loads -> lots ->
# MAGIC where the stock is -> open orders -> customers. Compare with `groundtruth/.../hero_blast_radius.json`
# MAGIC offline (never load the ground truth into the platform).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.v_hero_suspect_lots AS
WITH last_pass AS (
  SELECT asset_id, max(performed_ts) AS since FROM {BRONZE}.calibration_records
  WHERE instrument = 'chamber_rh_pct' AND result = 'PASS' GROUP BY asset_id)
SELECT l.sterilizer_id, lp.since AS suspect_since, l.load_id, l.status AS load_status, c.lot_id, c.sku_id, c.cases
FROM {BRONZE}.txn_sterilization_loads l
JOIN last_pass lp ON lp.asset_id = l.sterilizer_id AND l.start_ts >= lp.since
JOIN {BRONZE}.txn_load_contents c ON c.load_id = l.load_id""")

display(spark.sql(f"""
WITH s AS (SELECT DISTINCT lot_id FROM {GOLD}.v_hero_suspect_lots WHERE sterilizer_id = '{STZ}' AND lot_id IS NOT NULL),
     onhand AS (SELECT dc_id, sum(qty_cases) AS cases FROM {BRONZE}.txn_inventory_on_hand JOIN s USING (lot_id) GROUP BY dc_id),
     orders AS (SELECT o.order_id, o.customer_id, c.customer_type, c.idn_id, sum(a.qty_cases) AS cases
                FROM {BRONZE}.txn_order_allocations a JOIN s USING (lot_id)
                JOIN {BRONZE}.txn_order_lines ol USING (order_line_id)
                JOIN {BRONZE}.txn_customer_orders o USING (order_id)
                JOIN {BRONZE}.master_customers c USING (customer_id)
                GROUP BY ALL)
SELECT 'suspect lots' AS measure, CAST(count(*) AS STRING) AS value FROM s
UNION ALL SELECT 'cases on hand by DC', to_json(map_from_entries(collect_list(struct(dc_id, cases)))) FROM onhand
UNION ALL SELECT 'open orders at risk', CAST(count(*) AS STRING) FROM orders
UNION ALL SELECT 'customers at risk', CAST(count(DISTINCT customer_id) AS STRING) FROM orders
UNION ALL SELECT 'hospitals at risk', CAST(count(DISTINCT CASE WHEN customer_type = 'hospital' THEN customer_id END) AS STRING) FROM orders
UNION ALL SELECT 'IDNs at risk', CAST(count(DISTINCT idn_id) AS STRING) FROM orders"""))
