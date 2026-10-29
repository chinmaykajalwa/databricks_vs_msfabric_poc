# Agent-ready tables: descriptions

Paste into the Fabric data agent's **data source instructions** for the Lakehouse (Databricks reads the
same text from Unity Catalog comments). Table names: Databricks `<catalog>.agent.<name>`, Fabric `agent_<name>`.

## sterilization_loads

One row per EtO sterilization cycle (load) on a chamber. load_status 'in_cycle' = in the chamber at the data snapshot. Join to load_lots on load_id for what the load contained.

- `load_id`: Sterilization load (cycle) identifier.
- `sterilizer_id`: Chamber that ran the cycle, e.g. STZ-P01-01.
- `plant_id`: Plant where the chamber is.
- `cycle_start_ts`: Cycle start (UTC): vacuum and conditioning begin.
- `exposure_start_ts`: EtO gas exposure start (UTC); conditioning ends here.
- `exposure_end_ts`: EtO exposure end (UTC).
- `cycle_end_ts`: Cycle end (UTC) after nitrogen washes.
- `load_status`: completed or in_cycle.
- `cycle_result`: pass, fail, or null while in cycle. Failed cycles are re-sterilized in a later load.
- `pallets`: Pallets in the load.
- `release_ts`: When QA released the load (UTC), after aeration and biological-indicator results.

## load_lots

Which finished-goods lots each sterilization load produced (a sterile lot is one work order's product sterilized in one load). Rows with in_chamber = true are product still in the chamber at the snapshot (no lot yet). Failed cycles are excluded: their product appears in the load that re-sterilized it.

- `load_id`: Sterilization load.
- `lot_id`: Finished-goods lot produced by this load (null while in the chamber).
- `work_order_id`: Production work order the product came from.
- `sku_id`: Product SKU.
- `sku_description`: Product description.
- `product_family`: Product family, e.g. procedure_kits, molded_disposables, textiles.
- `cases_in_load`: Cases of this work order in the load.
- `in_chamber`: True if the load was still in the chamber at the snapshot.
- `lot_status`: released, quarantine (awaiting release), on_hold (deviation), or rejected.
- `lot_release_ts`: Lot release time (UTC).
- `expiry_date`: Lot expiry date.

## lot_locations

Where every lot's cases are at the snapshot. location_type: plant (made, not yet shipped), in_transit (plant -> DC), dc_on_hand (at a distribution centre; allocated_cases = reserved for open orders), delivered (location_id is the customer). Use this to answer 'where is lot X now'.

- `lot_id`: Finished-goods lot.
- `sku_id`: Product SKU.
- `location_type`: plant, in_transit, dc_on_hand or delivered.
- `location_id`: Plant id, destination DC id, DC id, or customer id (for delivered).
- `cases`: Cases at this location.
- `allocated_cases`: For dc_on_hand: cases reserved for open customer orders; otherwise 0.

## open_order_exposure

Open customer order lines and the specific DC lots soft-allocated to them at the snapshot. Join to load_lots on lot_id to find which open orders and customers depend on a given load.

- `order_line_id`: Open order line.
- `order_id`: Customer order.
- `customer_id`: Customer.
- `customer_name`: Customer name (fictional).
- `customer_type`: hospital, ambulatory_surgery_center, extended_care, physician_office or home_health.
- `idn_id`: Integrated delivery network (health system), if any.
- `idn_name`: Health system name.
- `dc_id`: Distribution centre the order ships from.
- `requested_delivery_date`: Requested delivery date.
- `sku_id`: Product SKU.
- `lot_id`: Lot allocated to this line.
- `allocated_cases`: Cases of this lot allocated to the line.

## calibration_history

Instrument verification records. Per SOP-STR-204, when an instrument fails verification, every load since that instrument's LAST PASSING verification is suspect. For chamber humidity use instrument = 'chamber_rh_pct'.

- `calibration_id`: Verification record.
- `asset_id`: Equipment, e.g. a sterilizer STZ-..., molding machine IMM-..., or truck TRK-....
- `instrument`: Signal verified: chamber_rh_pct, chamber_temp_c, chamber_pressure_kpa, eto_conc_mg_l, melt_temp_c, reefer_temp_c.
- `performed_ts`: When the verification was done (UTC).
- `tolerance`: Allowed as-found error (+/-).
- `unit`: Unit of the error.
- `as_found_error`: Error found before any adjustment.
- `result`: PASS or FAIL.
- `as_left_error`: Error after adjustment.
- `deviation_id`: Deviation opened for a FAIL, if any.
- `next_due_ts`: Next verification due (UTC).

## steam_vs_rh_per_load

Per completed load: conditioning steam consumption (20-60 min into the cycle) and the REPORTED chamber humidity during exposure, with the chamber's own steam baseline. Per MAN-STZ-EX 4.3, a sustained steam drop of more than 10% with unchanged reported RH indicates RH probe drift (the probe reads high, so true humidity was lower than reported).

- `load_id`: Sterilization load.
- `sterilizer_id`: Chamber.
- `cycle_start_ts`: Cycle start (UTC).
- `conditioning_steam_kg_h`: Mean steam flow during conditioning (kg/h).
- `reported_rh_exposure_pct`: Mean REPORTED chamber RH during exposure (%RH).
- `chamber_baseline_steam_kg_h`: Chamber's mean conditioning steam over the first half of the history window.
- `steam_vs_baseline_pct`: Percent difference of this load's conditioning steam from the chamber baseline (negative = less steam).

## deviations_and_capas

Quality deviations (18-month archive plus recent events) with their corrective and preventive actions (one row per deviation x CAPA; deviations without a CAPA have null CAPA columns). Use for 'has this happened before' questions on an asset.

- `deviation_id`: Deviation.
- `opened_ts`: Opened (UTC).
- `deviation_status`: open or closed.
- `closed_ts`: Closed (UTC).
- `severity`: minor, major or critical.
- `category`: sterilization, molding, packaging, textiles, kit_assembly, supplier, distribution.
- `type_key`: Deviation type, e.g. eto_rh_probe_drift, mold_short_shot.
- `title`: Short description.
- `plant_id`: Site.
- `asset_type`: Kind of asset involved.
- `asset_id`: Asset involved, e.g. STZ-P01-01.
- `root_cause_category`: equipment, calibration, process, material or human.
- `supplier_id`: Supplier, for supplier deviations.
- `capa_id`: Corrective/preventive action.
- `capa_action`: What the CAPA does.
- `capa_status`: open, overdue or closed.
- `capa_due_ts`: CAPA due date (UTC).
- `capa_closed_ts`: CAPA closed (UTC).
- `capa_effectiveness_verified`: Whether the CAPA's effectiveness was verified.

## customers

Customer sites (all names fictional) with type, health system (IDN), serving DC and location.

- `customer_id`: Customer.
- `customer_name`: Name.
- `customer_type`: hospital, ambulatory_surgery_center, extended_care, physician_office or home_health.
- `idn_id`: Health system, if any.
- `idn_name`: Health system name.
- `dc_id`: Distribution centre serving the customer.
- `city`: City.
- `state`: State.
- `bed_count`: Beds (hospitals and extended care).

## products

Product catalogue: SKU, family, sterility, and storage temperature limits (temperature-sensitive products have storage_temp_min_c / storage_temp_max_c).

- `sku_id`: Product SKU.
- `sku_description`: Description.
- `product_family`: Family.
- `make_or_buy`: make (own plants) or buy (supplier).
- `sterile`: Sterile product.
- `temp_sensitive`: Requires temperature control.
- `storage_temp_min_c`: Minimum storage temperature (C).
- `storage_temp_max_c`: Maximum storage temperature (C).
- `abc_class`: A = fast-moving.
