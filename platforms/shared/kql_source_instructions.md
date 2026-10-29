# Live-telemetry instructions

## 1. Fabric data agent: KQL database data-source instructions

Select these four objects in the KQL database: `Telemetry`, `TelemetryDedup`, `TelemetryMinute`, `TagDim`.

```text
Live sensor telemetry from plants, distribution centres and trucks (streamed from Azure Event Hubs).
Use this source for "right now / last minutes / last hours" questions about equipment readings and alarms.
For loads, lots, inventory, orders, customers, calibrations and deviations, use the Lakehouse instead.

Tables:
- TelemetryDedup: one row per reading, duplicates removed. PREFER this for values and alarms.
  Columns: topic, site, event_time (when measured, UTC), tag_id, value, quality, fw, ingest_time, arrival_lag_ms.
- TelemetryMinute: 1-minute aggregates per tag (n, mean_v, min_v, max_v, n_not_good, minute). Use for trends and
  for "sustained" rules (e.g. above a limit for more than 15 minutes).
- Telemetry: raw readings including duplicates. Use only for data-quality questions (duplicates, late data,
  silent sites, clock skew).
- TagDim: what each tag_id means. Readings carry only tag_id: ALWAYS join on tag_id to get asset_id, signal,
  unit, site_id, asset_type, min_valid, max_valid, criticality. Use `lookup kind=inner (TagDim | ...) on tag_id`.

Conventions:
- quality: G = good, U = uncertain, B = bad. Treat U/B readings as instrument problems, not process values.
- Filter on event_time for process questions; on ingest_time for "is data still arriving" questions.
- Default to the last 1 hour unless the user gives a time range.
- Asset ids: sterilizers STZ-..., molding machines IMM-..., DC equipment <DC>-SRT/CNV/AMR-..., trucks TRK-....
- Key signals: chamber_rh_pct, chamber_temp_c, chamber_pressure_kpa, eto_conc_mg_l, steam_flow_kg_h (sterilizers);
  vibration_mm_s, bearing_temp_c (sorters/conveyors); reefer_temp_c, gps_lat, gps_lon (trucks); melt_temp_c (molding).

Documented alarm rules:
- Sterilizer humidity: chamber_rh_pct > 80 or quality U/B (probe failure).
- Sorter bearing: vibration_mm_s > 7.1 on a crossbelt_sorter (ISO zone D).
- Reefer: reefer_temp_c above 20 C for MORE than 15 minutes (shorter rises are door openings).
- Site outage: no data from a site for more than 2 minutes (compare max(ingest_time) with now()).

Tag ids not found in TagDim come from equipment on new firmware (fw = 4.3.0); report them as unknown tags rather
than ignoring them.
```

## 2. Databricks Genie Agent: the equivalent block (parity)

Attach `p2p_poc.silver.telemetry` and append this to the Genie Agent's instructions:

```text
Live telemetry is in silver.telemetry: one row per reading, already de-duplicated and joined to the tag
dictionary (asset_id, signal, unit, site_id, asset_type, min_valid, max_valid, criticality). known_tag = false
marks tags from equipment on new firmware (fw = 4.3.0); report them as unknown tags rather than ignoring them.

Conventions:
- quality: G = good, U = uncertain, B = bad. Treat U/B readings as instrument problems, not process values.
- Filter on event_time for process questions; on ingest_time for "is data still arriving" questions.
- Default to the last 1 hour unless the user gives a time range.

Documented alarm rules:
- Sterilizer humidity: chamber_rh_pct > 80 or quality U/B (probe failure).
- Sorter bearing: vibration_mm_s > 7.1 on a crossbelt_sorter (ISO zone D).
- Reefer: reefer_temp_c above 20 C for MORE than 15 minutes (shorter rises are door openings).
- Site outage: no data from a site for more than 2 minutes.
```
