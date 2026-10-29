# Fabric notebook (PySpark + Spark SQL). Same logic as the hero cells of the Databricks notebook 03,
# against the Lakehouse tables loaded by 01_load_landing_to_lakehouse.

STZ = "STZ-P01-01"

# Steam reveals the drift: reported RH flat, conditioning steam falling (MAN-STZ-EX section 4.3)
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_steam_vs_reported_rh AS
WITH tags AS (SELECT tag_id, asset_id, signal FROM ref_tag_dim
              WHERE asset_kind = 'sterilizer' AND signal IN ('steam_flow_kg_h', 'chamber_rh_pct')),
     h AS (SELECT a.bucket_ts, a.mean, t.asset_id, t.signal FROM telemetry_agg_history a JOIN tags t USING (tag_id))
SELECT l.load_id, l.sterilizer_id, l.start_ts,
       avg(CASE WHEN h.signal = 'steam_flow_kg_h' AND h.bucket_ts >= l.start_ts + INTERVAL 20 MINUTES
                 AND h.bucket_ts <  l.start_ts + INTERVAL 60 MINUTES THEN h.mean END) AS conditioning_steam_kg_h,
       avg(CASE WHEN h.signal = 'chamber_rh_pct' AND h.bucket_ts >= l.precond_end_ts
                 AND h.bucket_ts < l.exposure_end_ts THEN h.mean END) AS reported_rh_exposure
FROM txn_sterilization_loads l
JOIN h ON h.asset_id = l.sterilizer_id AND h.bucket_ts BETWEEN l.start_ts AND l.exposure_end_ts
WHERE l.status = 'completed'
GROUP BY l.load_id, l.sterilizer_id, l.start_ts""")
display(spark.sql(f"SELECT * FROM v_steam_vs_reported_rh WHERE sterilizer_id = '{STZ}' ORDER BY start_ts DESC LIMIT 40"))

# Blast radius, derived the SOP way (since the last PASSING RH verification)
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_hero_suspect_lots AS
WITH last_pass AS (SELECT asset_id, max(performed_ts) AS since FROM calibration_records
                   WHERE instrument = 'chamber_rh_pct' AND result = 'PASS' GROUP BY asset_id)
SELECT l.sterilizer_id, lp.since AS suspect_since, l.load_id, l.status AS load_status, c.lot_id, c.sku_id, c.cases
FROM txn_sterilization_loads l
JOIN last_pass lp ON lp.asset_id = l.sterilizer_id AND l.start_ts >= lp.since
JOIN txn_load_contents c ON c.load_id = l.load_id""")
display(spark.sql(f"""
WITH s AS (SELECT DISTINCT lot_id FROM v_hero_suspect_lots WHERE sterilizer_id = '{STZ}' AND lot_id IS NOT NULL),
     onhand AS (SELECT dc_id, sum(qty_cases) AS cases FROM txn_inventory_on_hand JOIN s USING (lot_id) GROUP BY dc_id),
     orders AS (SELECT o.order_id, o.customer_id, c.customer_type, c.idn_id, sum(a.qty_cases) AS cases
                FROM txn_order_allocations a JOIN s USING (lot_id)
                JOIN txn_order_lines ol USING (order_line_id)
                JOIN txn_customer_orders o USING (order_id)
                JOIN master_customers c USING (customer_id)
                GROUP BY o.order_id, o.customer_id, c.customer_type, c.idn_id)
SELECT 'suspect lots' AS measure, CAST(count(*) AS STRING) AS value FROM s
UNION ALL SELECT 'cases on hand by DC', to_json(map_from_entries(collect_list(struct(dc_id, cases)))) FROM onhand
UNION ALL SELECT 'open orders at risk', CAST(count(*) AS STRING) FROM orders
UNION ALL SELECT 'customers at risk', CAST(count(DISTINCT customer_id) AS STRING) FROM orders
UNION ALL SELECT 'hospitals at risk', CAST(count(DISTINCT CASE WHEN customer_type = 'hospital' THEN customer_id END) AS STRING) FROM orders
UNION ALL SELECT 'IDNs at risk', CAST(count(DISTINCT idn_id) AS STRING) FROM orders"""))
