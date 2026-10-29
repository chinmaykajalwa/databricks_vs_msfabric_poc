# Fabric notebook (PySpark). Attach the Lakehouse loaded by 01_load_landing_to_lakehouse.
# 03 · Agent-ready layer (Fabric data agent data model)
# GENERATED from platforms/shared/agent_layer.py -- edit that file, then run
# python platforms/shared/build_agent_notebooks.py. Identical to Databricks' agents/10_agent_ready_layer.
#
# Creates agent_* Delta tables (visible in the Lakehouse SQL analytics endpoint, which the data agent
# queries). Column comments are stored in Delta; paste platforms/shared/agent_table_descriptions.md into the
# data agent's data-source instructions as well, since the agent may not read Delta comments.

import json
TABLES = json.loads(r'''[
 {
  "name": "sterilization_loads",
  "description": "One row per EtO sterilization cycle (load) on a chamber. load_status 'in_cycle' = in the chamber at the data snapshot. Join to load_lots on load_id for what the load contained.",
  "columns": {
   "load_id": "Sterilization load (cycle) identifier.",
   "sterilizer_id": "Chamber that ran the cycle, e.g. STZ-P01-01.",
   "plant_id": "Plant where the chamber is.",
   "cycle_start_ts": "Cycle start (UTC): vacuum and conditioning begin.",
   "exposure_start_ts": "EtO gas exposure start (UTC); conditioning ends here.",
   "exposure_end_ts": "EtO exposure end (UTC).",
   "cycle_end_ts": "Cycle end (UTC) after nitrogen washes.",
   "load_status": "completed or in_cycle.",
   "cycle_result": "pass, fail, or null while in cycle. Failed cycles are re-sterilized in a later load.",
   "pallets": "Pallets in the load.",
   "release_ts": "When QA released the load (UTC), after aeration and biological-indicator results."
  },
  "sql": "\nSELECT load_id, sterilizer_id, plant_id, start_ts AS cycle_start_ts, precond_end_ts AS exposure_start_ts,\n       exposure_end_ts, cycle_end_ts, status AS load_status, cycle_result, pallets, release_ts\nFROM {txn_sterilization_loads}"
 },
 {
  "name": "load_lots",
  "description": "Which finished-goods lots each sterilization load produced (a sterile lot is one work order's product sterilized in one load). Rows with in_chamber = true are product still in the chamber at the snapshot (no lot yet). Failed cycles are excluded: their product appears in the load that re-sterilized it.",
  "columns": {
   "load_id": "Sterilization load.",
   "lot_id": "Finished-goods lot produced by this load (null while in the chamber).",
   "work_order_id": "Production work order the product came from.",
   "sku_id": "Product SKU.",
   "sku_description": "Product description.",
   "product_family": "Product family, e.g. procedure_kits, molded_disposables, textiles.",
   "cases_in_load": "Cases of this work order in the load.",
   "in_chamber": "True if the load was still in the chamber at the snapshot.",
   "lot_status": "released, quarantine (awaiting release), on_hold (deviation), or rejected.",
   "lot_release_ts": "Lot release time (UTC).",
   "expiry_date": "Lot expiry date."
  },
  "sql": "\nSELECT c.load_id, c.lot_id, c.work_order_id, c.sku_id, p.description AS sku_description,\n       p.family AS product_family, c.cases AS cases_in_load,\n       (c.lot_id IS NULL AND c.cycle_result IS NULL) AS in_chamber,\n       l.status AS lot_status, l.release_ts AS lot_release_ts, l.expiry_date\nFROM {txn_load_contents} c\nJOIN {master_products} p ON p.sku_id = c.sku_id\nLEFT JOIN {txn_lots} l ON l.lot_id = c.lot_id\nWHERE c.cycle_result IS NULL OR c.cycle_result <> 'fail'"
 },
 {
  "name": "lot_locations",
  "description": "Where every lot's cases are at the snapshot. location_type: plant (made, not yet shipped), in_transit (plant -> DC), dc_on_hand (at a distribution centre; allocated_cases = reserved for open orders), delivered (location_id is the customer). Use this to answer 'where is lot X now'.",
  "columns": {
   "lot_id": "Finished-goods lot.",
   "sku_id": "Product SKU.",
   "location_type": "plant, in_transit, dc_on_hand or delivered.",
   "location_id": "Plant id, destination DC id, DC id, or customer id (for delivered).",
   "cases": "Cases at this location.",
   "allocated_cases": "For dc_on_hand: cases reserved for open customer orders; otherwise 0."
  },
  "sql": "\nSELECT lot_id, sku_id, 'dc_on_hand' AS location_type, dc_id AS location_id,\n       CAST(qty_cases AS BIGINT) AS cases, CAST(qty_allocated_cases AS BIGINT) AS allocated_cases\nFROM {txn_inventory_on_hand}\nUNION ALL\nSELECT lot_id, sku_id, 'in_transit', to_dc_id, CAST(qty_cases AS BIGINT), CAST(0 AS BIGINT)\nFROM {txn_stock_transfers} WHERE status = 'in_transit'\nUNION ALL\nSELECT sl.lot_id, l.sku_id, 'delivered', o.customer_id, CAST(SUM(sl.qty_cases) AS BIGINT), CAST(0 AS BIGINT)\nFROM {txn_shipment_lines} sl\nJOIN {txn_shipments} s ON s.shipment_id = sl.shipment_id\nJOIN {txn_customer_orders} o ON o.order_id = s.order_id\nJOIN {txn_lots} l ON l.lot_id = sl.lot_id\nGROUP BY sl.lot_id, l.sku_id, o.customer_id\nUNION ALL\nSELECT l.lot_id, l.sku_id, 'plant', w.plant_id,\n       CAST(l.qty_cases - COALESCE(t.cases, 0) AS BIGINT), CAST(0 AS BIGINT)\nFROM {txn_lots} l\nJOIN {txn_work_orders} w ON w.work_order_id = l.work_order_id\nLEFT JOIN (SELECT lot_id, SUM(qty_cases) AS cases FROM {txn_stock_transfers} GROUP BY lot_id) t ON t.lot_id = l.lot_id\nWHERE l.source = 'make' AND l.status <> 'rejected' AND l.qty_cases - COALESCE(t.cases, 0) > 0"
 },
 {
  "name": "open_order_exposure",
  "description": "Open customer order lines and the specific DC lots soft-allocated to them at the snapshot. Join to load_lots on lot_id to find which open orders and customers depend on a given load.",
  "columns": {
   "order_line_id": "Open order line.",
   "order_id": "Customer order.",
   "customer_id": "Customer.",
   "customer_name": "Customer name (fictional).",
   "customer_type": "hospital, ambulatory_surgery_center, extended_care, physician_office or home_health.",
   "idn_id": "Integrated delivery network (health system), if any.",
   "idn_name": "Health system name.",
   "dc_id": "Distribution centre the order ships from.",
   "requested_delivery_date": "Requested delivery date.",
   "sku_id": "Product SKU.",
   "lot_id": "Lot allocated to this line.",
   "allocated_cases": "Cases of this lot allocated to the line."
  },
  "sql": "\nSELECT a.order_line_id, ol.order_id, o.customer_id, c.name AS customer_name, c.customer_type, c.idn_id,\n       i.name AS idn_name, a.dc_id, a.requested_delivery_date, ol.sku_id, a.lot_id,\n       CAST(a.qty_cases AS BIGINT) AS allocated_cases\nFROM {txn_order_allocations} a\nJOIN {txn_order_lines} ol ON ol.order_line_id = a.order_line_id\nJOIN {txn_customer_orders} o ON o.order_id = ol.order_id\nJOIN {master_customers} c ON c.customer_id = o.customer_id\nLEFT JOIN {master_idns} i ON i.idn_id = c.idn_id"
 },
 {
  "name": "calibration_history",
  "description": "Instrument verification records. Per SOP-STR-204, when an instrument fails verification, every load since that instrument's LAST PASSING verification is suspect. For chamber humidity use instrument = 'chamber_rh_pct'.",
  "columns": {
   "calibration_id": "Verification record.",
   "asset_id": "Equipment, e.g. a sterilizer STZ-..., molding machine IMM-..., or truck TRK-....",
   "instrument": "Signal verified: chamber_rh_pct, chamber_temp_c, chamber_pressure_kpa, eto_conc_mg_l, melt_temp_c, reefer_temp_c.",
   "performed_ts": "When the verification was done (UTC).",
   "tolerance": "Allowed as-found error (+/-).",
   "unit": "Unit of the error.",
   "as_found_error": "Error found before any adjustment.",
   "result": "PASS or FAIL.",
   "as_left_error": "Error after adjustment.",
   "deviation_id": "Deviation opened for a FAIL, if any.",
   "next_due_ts": "Next verification due (UTC)."
  },
  "sql": "\nSELECT calibration_id, asset_id, instrument, performed_ts, tolerance, unit, as_found_error, result,\n       as_left_error, deviation_id, next_due_ts\nFROM {calibration_records}"
 },
 {
  "name": "steam_vs_rh_per_load",
  "description": "Per completed load: conditioning steam consumption (20-60 min into the cycle) and the REPORTED chamber humidity during exposure, with the chamber's own steam baseline. Per MAN-STZ-EX 4.3, a sustained steam drop of more than 10% with unchanged reported RH indicates RH probe drift (the probe reads high, so true humidity was lower than reported).",
  "columns": {
   "load_id": "Sterilization load.",
   "sterilizer_id": "Chamber.",
   "cycle_start_ts": "Cycle start (UTC).",
   "conditioning_steam_kg_h": "Mean steam flow during conditioning (kg/h).",
   "reported_rh_exposure_pct": "Mean REPORTED chamber RH during exposure (%RH).",
   "chamber_baseline_steam_kg_h": "Chamber's mean conditioning steam over the first half of the history window.",
   "steam_vs_baseline_pct": "Percent difference of this load's conditioning steam from the chamber baseline (negative = less steam)."
  },
  "sql": "\nWITH tags AS (\n  SELECT tag_id, asset_id, signal FROM {ref_tag_dim}\n  WHERE asset_kind = 'sterilizer' AND signal IN ('steam_flow_kg_h', 'chamber_rh_pct')),\nh AS (\n  SELECT a.bucket_ts, a.mean, t.asset_id, t.signal\n  FROM {telemetry_agg_history} a JOIN tags t ON t.tag_id = a.tag_id),\nper_load AS (\n  SELECT l.load_id, l.sterilizer_id, l.start_ts AS cycle_start_ts,\n         AVG(CASE WHEN h.signal = 'steam_flow_kg_h' AND h.bucket_ts >= l.start_ts + INTERVAL 20 MINUTES\n                   AND h.bucket_ts < l.start_ts + INTERVAL 60 MINUTES THEN h.mean END) AS conditioning_steam_kg_h,\n         AVG(CASE WHEN h.signal = 'chamber_rh_pct' AND h.bucket_ts >= l.precond_end_ts\n                   AND h.bucket_ts < l.exposure_end_ts THEN h.mean END) AS reported_rh_exposure_pct\n  FROM {txn_sterilization_loads} l\n  JOIN h ON h.asset_id = l.sterilizer_id AND h.bucket_ts >= l.start_ts AND h.bucket_ts < l.exposure_end_ts\n  WHERE l.status = 'completed'\n  GROUP BY l.load_id, l.sterilizer_id, l.start_ts),\nbase AS (\n  SELECT sterilizer_id, AVG(conditioning_steam_kg_h) AS chamber_baseline_steam_kg_h\n  FROM per_load\n  WHERE conditioning_steam_kg_h IS NOT NULL\n    AND cycle_start_ts < (SELECT MAX(cycle_start_ts) FROM per_load) - INTERVAL 15 DAYS\n  GROUP BY sterilizer_id)\nSELECT p.load_id, p.sterilizer_id, p.cycle_start_ts, ROUND(p.conditioning_steam_kg_h, 2) AS conditioning_steam_kg_h,\n       ROUND(p.reported_rh_exposure_pct, 2) AS reported_rh_exposure_pct,\n       ROUND(b.chamber_baseline_steam_kg_h, 2) AS chamber_baseline_steam_kg_h,\n       ROUND(100 * (p.conditioning_steam_kg_h / b.chamber_baseline_steam_kg_h - 1), 1) AS steam_vs_baseline_pct\nFROM per_load p LEFT JOIN base b ON b.sterilizer_id = p.sterilizer_id\nWHERE p.conditioning_steam_kg_h IS NOT NULL"
 },
 {
  "name": "deviations_and_capas",
  "description": "Quality deviations (18-month archive plus recent events) with their corrective and preventive actions (one row per deviation x CAPA; deviations without a CAPA have null CAPA columns). Use for 'has this happened before' questions on an asset.",
  "columns": {
   "deviation_id": "Deviation.",
   "opened_ts": "Opened (UTC).",
   "deviation_status": "open or closed.",
   "closed_ts": "Closed (UTC).",
   "severity": "minor, major or critical.",
   "category": "sterilization, molding, packaging, textiles, kit_assembly, supplier, distribution.",
   "type_key": "Deviation type, e.g. eto_rh_probe_drift, mold_short_shot.",
   "title": "Short description.",
   "plant_id": "Site.",
   "asset_type": "Kind of asset involved.",
   "asset_id": "Asset involved, e.g. STZ-P01-01.",
   "root_cause_category": "equipment, calibration, process, material or human.",
   "supplier_id": "Supplier, for supplier deviations.",
   "capa_id": "Corrective/preventive action.",
   "capa_action": "What the CAPA does.",
   "capa_status": "open, overdue or closed.",
   "capa_due_ts": "CAPA due date (UTC).",
   "capa_closed_ts": "CAPA closed (UTC).",
   "capa_effectiveness_verified": "Whether the CAPA's effectiveness was verified."
  },
  "sql": "\nSELECT d.deviation_id, d.opened_ts, d.status AS deviation_status, d.closed_ts, d.severity, d.category, d.type_key,\n       d.title, d.plant_id, d.asset_type, d.asset_id, d.root_cause_category, d.supplier_id,\n       c.capa_id, c.action AS capa_action, c.status AS capa_status, c.due_ts AS capa_due_ts,\n       c.closed_ts AS capa_closed_ts, c.effectiveness_verified AS capa_effectiveness_verified\nFROM {txn_deviations} d LEFT JOIN {txn_capas} c ON c.deviation_id = d.deviation_id"
 },
 {
  "name": "customers",
  "description": "Customer sites (all names fictional) with type, health system (IDN), serving DC and location.",
  "columns": {
   "customer_id": "Customer.",
   "customer_name": "Name.",
   "customer_type": "hospital, ambulatory_surgery_center, extended_care, physician_office or home_health.",
   "idn_id": "Health system, if any.",
   "idn_name": "Health system name.",
   "dc_id": "Distribution centre serving the customer.",
   "city": "City.",
   "state": "State.",
   "bed_count": "Beds (hospitals and extended care)."
  },
  "sql": "\nSELECT c.customer_id, c.name AS customer_name, c.customer_type, c.idn_id, i.name AS idn_name, c.dc_id,\n       c.city, c.state, c.bed_count\nFROM {master_customers} c LEFT JOIN {master_idns} i ON i.idn_id = c.idn_id"
 },
 {
  "name": "products",
  "description": "Product catalogue: SKU, family, sterility, and storage temperature limits (temperature-sensitive products have storage_temp_min_c / storage_temp_max_c).",
  "columns": {
   "sku_id": "Product SKU.",
   "sku_description": "Description.",
   "product_family": "Family.",
   "make_or_buy": "make (own plants) or buy (supplier).",
   "sterile": "Sterile product.",
   "temp_sensitive": "Requires temperature control.",
   "storage_temp_min_c": "Minimum storage temperature (C).",
   "storage_temp_max_c": "Maximum storage temperature (C).",
   "abc_class": "A = fast-moving."
  },
  "sql": "\nSELECT sku_id, description AS sku_description, family AS product_family, make_or_buy, sterile, temp_sensitive,\n       storage_temp_min_c, storage_temp_max_c, abc_class\nFROM {master_products}"
 }
]''')
SRC = {name: name for name in ["txn_sterilization_loads", "txn_load_contents", "txn_lots", "txn_work_orders", "txn_inventory_on_hand", "txn_stock_transfers", "txn_shipment_lines", "txn_shipments", "txn_customer_orders", "txn_order_lines", "txn_order_allocations", "txn_deviations", "txn_capas", "master_customers", "master_idns", "master_products", "calibration_records", "ref_tag_dim", "telemetry_agg_history"]}

def target_name(name):
    return f"agent_{name}"

def sql_str(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

for t in TABLES:
    target = target_name(t["name"])
    body = t["sql"].format(**SRC)
    spark.sql(f"CREATE OR REPLACE TABLE {target} COMMENT {sql_str(t['description'])} AS {body}")
    for col, comment in t["columns"].items():
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{col}` COMMENT {sql_str(comment)}")
    print(f"{target:<45} {spark.table(target).count():>10,} rows")
