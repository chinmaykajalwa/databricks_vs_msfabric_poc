"""
Example question -> query pairs for the agents, in each platform's dialect.
Deliberately about chamber STZ-P02-01 / DC01, NOT the hero chamber: they teach the
patterns (suspect window from calibration, steam vs baseline, lot locations, open-order
exposure) without handing the agent the answers it is scored on.

Databricks Genie: Spark SQL over <catalog>.agent.<table>   (paste into "SQL queries" / example queries)
Fabric data agent (Lakehouse source): T-SQL over agent_<table>  (paste into the Lakehouse's example queries)
"""
EXAMPLES = [
    ("Which loads on chamber STZ-P02-01 ran since its humidity probe last passed verification?",
     """SELECT l.load_id, l.cycle_start_ts, l.load_status, l.cycle_result
FROM {T}sterilization_loads l
WHERE l.sterilizer_id = 'STZ-P02-01'
  AND l.cycle_start_ts >= (SELECT MAX(c.performed_ts) FROM {T}calibration_history c
                           WHERE c.asset_id = 'STZ-P02-01' AND c.instrument = 'chamber_rh_pct' AND c.result = 'PASS')
ORDER BY l.cycle_start_ts"""),
    ("Is conditioning steam on STZ-P02-01 drifting while reported humidity stays constant?",
     """SELECT load_id, cycle_start_ts, conditioning_steam_kg_h, chamber_baseline_steam_kg_h, steam_vs_baseline_pct,
       reported_rh_exposure_pct
FROM {T}steam_vs_rh_per_load
WHERE sterilizer_id = 'STZ-P02-01'
ORDER BY cycle_start_ts DESC"""),
    ("Where is the product from STZ-P02-01 loads since 25 September now?",
     """SELECT x.location_type, x.location_id, SUM(x.cases) AS cases, SUM(x.allocated_cases) AS allocated_cases
FROM {T}load_lots ll
JOIN {T}lot_locations x ON x.lot_id = ll.lot_id
JOIN {T}sterilization_loads l ON l.load_id = ll.load_id
WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25'
GROUP BY x.location_type, x.location_id
ORDER BY x.location_type, cases DESC"""),
    ("Which open orders and customers depend on lots from STZ-P02-01 loads since 25 September?",
     """SELECT e.customer_type, COUNT(DISTINCT e.order_id) AS open_orders, COUNT(DISTINCT e.customer_id) AS customers,
       COUNT(DISTINCT e.idn_id) AS health_systems, SUM(e.allocated_cases) AS cases
FROM {T}open_order_exposure e
WHERE e.lot_id IN (SELECT ll.lot_id FROM {T}load_lots ll JOIN {T}sterilization_loads l ON l.load_id = ll.load_id
                   WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25')
GROUP BY e.customer_type
ORDER BY open_orders DESC"""),
    ("What deviations and CAPAs has STZ-P02-01 had?",
     """SELECT deviation_id, opened_ts, severity, type_key, title, root_cause_category, capa_id, capa_action, capa_status
FROM {T}deviations_and_capas
WHERE asset_id = 'STZ-P02-01'
ORDER BY opened_ts DESC"""),
    ("Which temperature-sensitive products are on hand at DC01?",
     """SELECT p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c, SUM(x.cases) AS cases_on_hand
FROM {T}lot_locations x
JOIN {T}products p ON p.sku_id = x.sku_id
WHERE p.temp_sensitive = {TRUE} AND x.location_type = 'dc_on_hand' AND x.location_id = 'DC01'
GROUP BY p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c
ORDER BY cases_on_hand DESC"""),
]

KQL_EXAMPLES = [
    ("What are the latest readings for chamber STZ-P02-01?",
     """TelemetryDedup
| where event_time > ago(30m)
| lookup kind = inner (TagDim | where asset_id == "STZ-P02-01" | project tag_id, signal, unit) on tag_id
| summarize arg_max(event_time, value, quality) by signal, unit"""),
    ("Is any sterilizer humidity probe alarming or reporting bad quality right now?",
     """TelemetryDedup
| where event_time > ago(1h)
| lookup kind = inner (TagDim | where signal == "chamber_rh_pct" | project tag_id, asset_id) on tag_id
| where value > 80 or quality != "G"
| summarize first_alarm = min(event_time), readings = count() by asset_id"""),
    ("Which sites have stopped sending data?",
     """Telemetry
| where ingest_time > ago(6h)
| summarize last_seen = max(ingest_time) by site, topic
| where datetime_diff('second', now(), last_seen) > 120"""),
]


def render(dialect: str) -> list[tuple[str, str]]:
    t, true = {"spark": ("agent.", "true"), "tsql": ("agent_", "1"), "duckdb": ("", "true")}[dialect]
    return [(q, sql.replace("{T}", t).replace("{TRUE}", true)) for q, sql in EXAMPLES]
