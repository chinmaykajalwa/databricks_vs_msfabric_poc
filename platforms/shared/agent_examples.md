# Example questions and queries for the agents

GENERATED from `example_queries.py`. They use chamber STZ-P02-01 / DC01 on purpose: they teach the
agent the patterns without giving away the hero answers it is scored on.

## Databricks Genie Agent (Spark SQL)

**Which loads on chamber STZ-P02-01 ran since its humidity probe last passed verification?**

```sql
SELECT l.load_id, l.cycle_start_ts, l.load_status, l.cycle_result
FROM agent.sterilization_loads l
WHERE l.sterilizer_id = 'STZ-P02-01'
  AND l.cycle_start_ts >= (SELECT MAX(c.performed_ts) FROM agent.calibration_history c
                           WHERE c.asset_id = 'STZ-P02-01' AND c.instrument = 'chamber_rh_pct' AND c.result = 'PASS')
ORDER BY l.cycle_start_ts
```

**Is conditioning steam on STZ-P02-01 drifting while reported humidity stays constant?**

```sql
SELECT load_id, cycle_start_ts, conditioning_steam_kg_h, chamber_baseline_steam_kg_h, steam_vs_baseline_pct,
       reported_rh_exposure_pct
FROM agent.steam_vs_rh_per_load
WHERE sterilizer_id = 'STZ-P02-01'
ORDER BY cycle_start_ts DESC
```

**Where is the product from STZ-P02-01 loads since 25 September now?**

```sql
SELECT x.location_type, x.location_id, SUM(x.cases) AS cases, SUM(x.allocated_cases) AS allocated_cases
FROM agent.load_lots ll
JOIN agent.lot_locations x ON x.lot_id = ll.lot_id
JOIN agent.sterilization_loads l ON l.load_id = ll.load_id
WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25'
GROUP BY x.location_type, x.location_id
ORDER BY x.location_type, cases DESC
```

**Which open orders and customers depend on lots from STZ-P02-01 loads since 25 September?**

```sql
SELECT e.customer_type, COUNT(DISTINCT e.order_id) AS open_orders, COUNT(DISTINCT e.customer_id) AS customers,
       COUNT(DISTINCT e.idn_id) AS health_systems, SUM(e.allocated_cases) AS cases
FROM agent.open_order_exposure e
WHERE e.lot_id IN (SELECT ll.lot_id FROM agent.load_lots ll JOIN agent.sterilization_loads l ON l.load_id = ll.load_id
                   WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25')
GROUP BY e.customer_type
ORDER BY open_orders DESC
```

**What deviations and CAPAs has STZ-P02-01 had?**

```sql
SELECT deviation_id, opened_ts, severity, type_key, title, root_cause_category, capa_id, capa_action, capa_status
FROM agent.deviations_and_capas
WHERE asset_id = 'STZ-P02-01'
ORDER BY opened_ts DESC
```

**Which temperature-sensitive products are on hand at DC01?**

```sql
SELECT p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c, SUM(x.cases) AS cases_on_hand
FROM agent.lot_locations x
JOIN agent.products p ON p.sku_id = x.sku_id
WHERE p.temp_sensitive = true AND x.location_type = 'dc_on_hand' AND x.location_id = 'DC01'
GROUP BY p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c
ORDER BY cases_on_hand DESC
```

## Fabric data agent, Lakehouse source (T-SQL)

**Which loads on chamber STZ-P02-01 ran since its humidity probe last passed verification?**

```sql
SELECT l.load_id, l.cycle_start_ts, l.load_status, l.cycle_result
FROM agent_sterilization_loads l
WHERE l.sterilizer_id = 'STZ-P02-01'
  AND l.cycle_start_ts >= (SELECT MAX(c.performed_ts) FROM agent_calibration_history c
                           WHERE c.asset_id = 'STZ-P02-01' AND c.instrument = 'chamber_rh_pct' AND c.result = 'PASS')
ORDER BY l.cycle_start_ts
```

**Is conditioning steam on STZ-P02-01 drifting while reported humidity stays constant?**

```sql
SELECT load_id, cycle_start_ts, conditioning_steam_kg_h, chamber_baseline_steam_kg_h, steam_vs_baseline_pct,
       reported_rh_exposure_pct
FROM agent_steam_vs_rh_per_load
WHERE sterilizer_id = 'STZ-P02-01'
ORDER BY cycle_start_ts DESC
```

**Where is the product from STZ-P02-01 loads since 25 September now?**

```sql
SELECT x.location_type, x.location_id, SUM(x.cases) AS cases, SUM(x.allocated_cases) AS allocated_cases
FROM agent_load_lots ll
JOIN agent_lot_locations x ON x.lot_id = ll.lot_id
JOIN agent_sterilization_loads l ON l.load_id = ll.load_id
WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25'
GROUP BY x.location_type, x.location_id
ORDER BY x.location_type, cases DESC
```

**Which open orders and customers depend on lots from STZ-P02-01 loads since 25 September?**

```sql
SELECT e.customer_type, COUNT(DISTINCT e.order_id) AS open_orders, COUNT(DISTINCT e.customer_id) AS customers,
       COUNT(DISTINCT e.idn_id) AS health_systems, SUM(e.allocated_cases) AS cases
FROM agent_open_order_exposure e
WHERE e.lot_id IN (SELECT ll.lot_id FROM agent_load_lots ll JOIN agent_sterilization_loads l ON l.load_id = ll.load_id
                   WHERE l.sterilizer_id = 'STZ-P02-01' AND l.cycle_start_ts >= '2026-09-25')
GROUP BY e.customer_type
ORDER BY open_orders DESC
```

**What deviations and CAPAs has STZ-P02-01 had?**

```sql
SELECT deviation_id, opened_ts, severity, type_key, title, root_cause_category, capa_id, capa_action, capa_status
FROM agent_deviations_and_capas
WHERE asset_id = 'STZ-P02-01'
ORDER BY opened_ts DESC
```

**Which temperature-sensitive products are on hand at DC01?**

```sql
SELECT p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c, SUM(x.cases) AS cases_on_hand
FROM agent_lot_locations x
JOIN agent_products p ON p.sku_id = x.sku_id
WHERE p.temp_sensitive = 1 AND x.location_type = 'dc_on_hand' AND x.location_id = 'DC01'
GROUP BY p.sku_id, p.sku_description, p.storage_temp_min_c, p.storage_temp_max_c
ORDER BY cases_on_hand DESC
```

## Fabric data agent, Eventhouse source (KQL)

**What are the latest readings for chamber STZ-P02-01?**

```kusto
TelemetryDedup
| where event_time > ago(30m)
| lookup kind = inner (TagDim | where asset_id == "STZ-P02-01" | project tag_id, signal, unit) on tag_id
| summarize arg_max(event_time, value, quality) by signal, unit
```

**Is any sterilizer humidity probe alarming or reporting bad quality right now?**

```kusto
TelemetryDedup
| where event_time > ago(1h)
| lookup kind = inner (TagDim | where signal == "chamber_rh_pct" | project tag_id, asset_id) on tag_id
| where value > 80 or quality != "G"
| summarize first_alarm = min(event_time), readings = count() by asset_id
```

**Which sites have stopped sending data?**

```kusto
Telemetry
| where ingest_time > ago(6h)
| summarize last_seen = max(ingest_time) by site, topic
| where datetime_diff('second', now(), last_seen) > 120
```
