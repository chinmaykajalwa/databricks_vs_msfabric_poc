"""
Integrity checks for master data.
=================================
The hero demo traces sensor -> load -> lot -> DC -> order. If any join in
that chain is broken, the agent on stage gives a wrong answer, so these
checks gate every run rather than being an afterthought.
"""
from __future__ import annotations

import pandas as pd

from .config import P2PConfig
from .world import gtin_is_valid


def _fk(child: pd.DataFrame, col: str, parent: pd.DataFrame, pcol: str, allow_null=False):
    vals = child[col].dropna() if allow_null else child[col]
    missing = sorted(set(vals.astype(str)) - set(parent[pcol].astype(str)))
    nulls = 0 if allow_null else int(child[col].isna().sum())
    return (not missing and nulls == 0), f"{len(missing)} orphan(s){', e.g. ' + missing[0] if missing else ''}; {nulls} null(s)"


def check_master(cfg: P2PConfig, t: dict[str, pd.DataFrame]) -> list[tuple[str, bool, str]]:
    r: list[tuple[str, bool, str]] = []

    def add(name, ok, detail=""):
        r.append((name, bool(ok), detail))

    # Primary keys
    for table, pk in [("plants", "plant_id"), ("production_lines", "line_id"), ("molding_machines", "machine_id"),
                      ("sterilizers", "sterilizer_id"), ("suppliers", "supplier_id"), ("components", "component_id"),
                      ("products", "sku_id"), ("molds", "mold_id"), ("distribution_centers", "dc_id"),
                      ("idns", "idn_id"), ("customers", "customer_id"), ("fleet_vehicles", "vehicle_id"),
                      ("routes", "route_id"), ("dc_assets", "asset_id")]:
        dup = int(t[table][pk].duplicated().sum())
        add(f"pk unique: {table}.{pk}", dup == 0, f"{dup} duplicate(s)")

    # Profile counts
    p = cfg.p
    for table, expected in [("plants", p.n_plants), ("molding_machines", p.n_molding_machines),
                            ("sterilizers", p.n_sterilizers), ("suppliers", p.n_suppliers),
                            ("products", p.n_skus), ("distribution_centers", p.n_dcs), ("idns", p.n_idns),
                            ("customers", p.n_customers), ("fleet_vehicles", p.n_trucks),
                            ("dc_assets", p.n_dc_assets)]:
        add(f"count: {table} == {expected}", len(t[table]) == expected, f"got {len(t[table])}")

    # Foreign keys
    for name, args in [
        ("lines -> plants", ("production_lines", "plant_id", "plants", "plant_id", False)),
        ("machines -> lines", ("molding_machines", "line_id", "production_lines", "line_id", False)),
        ("sterilizers -> plants", ("sterilizers", "plant_id", "plants", "plant_id", False)),
        ("components -> suppliers", ("components", "primary_supplier_id", "suppliers", "supplier_id", False)),
        ("products(make) -> plants", ("products", "producing_plant_id", "plants", "plant_id", True)),
        ("products(sterile make) -> steriliser plant", ("products", "sterilization_plant_id", "plants", "plant_id", True)),
        ("products(buy) -> suppliers", ("products", "primary_supplier_id", "suppliers", "supplier_id", True)),
        ("molds -> products", ("molds", "sku_id", "products", "sku_id", False)),
        ("idns -> dcs", ("idns", "home_dc_id", "distribution_centers", "dc_id", False)),
        ("customers -> dcs", ("customers", "dc_id", "distribution_centers", "dc_id", False)),
        ("customers -> idns", ("customers", "idn_id", "idns", "idn_id", True)),
        ("customers -> routes", ("customers", "route_id", "routes", "route_id", False)),
        ("fleet -> dcs", ("fleet_vehicles", "home_dc_id", "distribution_centers", "dc_id", False)),
        ("routes -> fleet", ("routes", "vehicle_id", "fleet_vehicles", "vehicle_id", False)),
        ("route_stops -> customers", ("route_stops", "customer_id", "customers", "customer_id", False)),
        ("dc_assets -> dcs", ("dc_assets", "dc_id", "distribution_centers", "dc_id", False)),
    ]:
        c, cc, par, pc, nullable = args
        ok, detail = _fk(t[c], cc, t[par], pc, nullable)
        add(f"fk: {name}", ok, detail)

    prod = t["products"]
    make, buy = prod[prod["make_or_buy"] == "make"], prod[prod["make_or_buy"] == "buy"]
    add("make SKUs have a producing plant", make["producing_plant_id"].notna().all())
    add("buy SKUs have a supplier", buy["primary_supplier_id"].notna().all())
    sterile_make = make[make["sterile"]]
    add("sterile make SKUs have an EtO steriliser site", sterile_make["sterilization_plant_id"].notna().all(),
        f"{int(sterile_make['sterilization_plant_id'].isna().sum())} missing")

    # Plant capability matches what it makes
    caps = t["plants"].set_index("plant_id")["capabilities"].str.split(",")
    need = {"molded_disposables": "molding", "textiles": "textiles", "procedure_kits": "kit_assembly",
            "wound_care": "wound_care"}
    bad = [s for _, s in make.iterrows() if need[s["family"]] not in caps[s["producing_plant_id"]]]
    add("producing plant has the capability", not bad, f"{len(bad)} mismatch(es)")

    # UDI
    add("UDI-DI GTIN-14 check digits valid", prod["udi_di"].map(gtin_is_valid).all())
    add("case GTIN-14 check digits valid", prod["case_gtin"].map(gtin_is_valid).all())
    add("UDI-DI unique", not prod["udi_di"].duplicated().any())
    add("descriptions unique", not prod["description"].duplicated().any(),
        f"{int(prod['description'].duplicated().sum())} duplicate(s)")

    # Molds and machines
    molded_make = make[make["family"] == "molded_disposables"]
    add("every in-house molded SKU has a mold", set(molded_make["sku_id"]) <= set(t["molds"]["sku_id"]))
    m = t["molding_machines"]
    no_machine = [mold["mold_id"] for _, mold in t["molds"].iterrows()
                  if not ((m["plant_id"] == mold["plant_id"]) & (m["clamp_tonnage"] >= mold["min_clamp_tonnage"])).any()]
    add("every mold has a compatible machine at its plant", not no_machine, f"{len(no_machine)} without")

    # BOM
    bom = t["bom"]
    add("bom parents are make SKUs", set(bom["parent_sku_id"]) <= set(make["sku_id"]))
    add("every make SKU has a BOM", set(make["sku_id"]) <= set(bom["parent_sku_id"]))
    comp_refs = bom.loc[bom["component_kind"] == "component", "component_ref"]
    sku_refs = bom.loc[bom["component_kind"] == "sku", "component_ref"]
    add("bom component refs exist", set(comp_refs) <= set(t["components"]["component_id"]))
    add("bom sku refs exist (kit contents)", set(sku_refs) <= set(prod["sku_id"]))
    add("kits contain no kits", not (set(sku_refs) & set(prod.loc[prod["family"] == "procedure_kits", "sku_id"])))
    add("bom quantities positive", (bom["qty_per_each"] > 0).all())

    # Network
    dcs = t["distribution_centers"]
    add("exactly one outage-scenario DC", int(dcs["is_outage_scenario_dc"].sum()) == 1)
    stops = t["route_stops"]
    add("every customer on exactly one route stop",
        len(stops) == len(t["customers"]) and not stops["customer_id"].duplicated().any())
    fleet = t["fleet_vehicles"]
    reefer_dcs = set(fleet.loc[fleet["has_reefer"], "home_dc_id"])
    add("every DC has a reefer truck", set(dcs["dc_id"]) <= reefer_dcs)
    sorter_dcs = set(t["dc_assets"].loc[t["dc_assets"]["asset_type"] == "crossbelt_sorter", "dc_id"])
    add("every DC has a crossbelt sorter", set(dcs["dc_id"]) <= sorter_dcs)
    cust = t["customers"]
    add("customer coordinates inside continental US",
        cust["lat"].between(24, 50).all() and cust["lon"].between(-125, -66).all())
    add("customer names unique", not cust["name"].duplicated().any())

    # Suppliers: every category used by components has an alternate
    alt_missing = t["components"]["alternate_supplier_id"].isna().sum()
    add("every component has an alternate supplier", alt_missing == 0, f"{int(alt_missing)} single-sourced")

    # Kits: content plausibility (no wound-care biologics, gels or paper in a sterile pack)
    kit_skus = set(prod.loc[prod["family"] == "procedure_kits", "sku_id"])
    contents = bom[(bom["component_kind"] == "sku") & bom["parent_sku_id"].isin(kit_skus)]
    bases = set(prod.set_index("sku_id").loc[contents["component_ref"], "base_name"])
    from .catalogs import KIT_CONTENTS
    allowed = {b for v in KIT_CONTENTS.values() for b, _ in v}
    kit_prof = prod[prod["family"] == "procedure_kits"].set_index("sku_id")["base_name"]
    from .catalogs import SKU_TEMPLATES
    prof_of = {b: a["contents"] for b, _, a in SKU_TEMPLATES["procedure_kits"]}
    wrong = [(k, b) for k, b in zip(contents["parent_sku_id"],
                                     prod.set_index("sku_id").loc[contents["component_ref"], "base_name"])
             if b not in {x for x, _ in KIT_CONTENTS[prof_of[kit_prof[k]]]}]
    add("each kit's contents match its kit type", not wrong, f"{len(wrong)} misplaced, e.g. {wrong[:1]}")
    add("kit contents drawn from the allowed list", bases <= allowed, f"unexpected: {sorted(bases - allowed)[:3]}")
    add("every kit has contents", kit_skus <= set(contents["parent_sku_id"]))
    r.extend(check_schedule(t))
    cp = t["capacity_plan"]
    hot = cp[cp["forecast_utilisation"] > 0.85]
    add("forecast utilisation <= 85% everywhere", hot.empty,
        "; ".join(f"{a}/{b} {u:.0%}" for a, b, u in hot[["plant_id", "capability", "forecast_utilisation"]].values))
    return r


def format_results(results) -> str:
    width = max(len(n) for n, _, _ in results)
    lines = [f"  {'PASS' if ok else 'FAIL'}  {n.ljust(width)}  {d if not ok else ''}".rstrip()
             for n, ok, d in results]
    passed = sum(ok for _, ok, _ in results)
    lines.append(f"  {passed}/{len(results)} checks passed")
    return "\n".join(lines)


def check_schedule(t: dict[str, pd.DataFrame]) -> list[tuple[str, bool, str]]:
    """No truck runs two routes on the same day; every route fits a 14 h on-duty window."""
    r = t["routes"]
    same_day = int(r.duplicated(["vehicle_id", "service_days"]).sum())
    longest = r["planned_duration_min"].max() / 60
    c = t["customers"].merge(r[["route_id", "deliveries_per_week"]], on="route_id", suffixes=("", "_route"))
    return [("no truck runs two routes on the same day", same_day == 0, f"{same_day} clash(es)"),
            ("every route fits a 14h on-duty window", longest <= 14, f"longest {longest:.1f}h"),
            ("customer delivery frequency matches its route",
             bool((c["deliveries_per_week"] == c["deliveries_per_week_route"]).all()), "")]


# ---------------------------------------------------------------------------
# Step 2: transactions
# ---------------------------------------------------------------------------
def _no_overlap(df, key, start, end) -> int:
    d = df.sort_values([key, start])
    nxt = d.groupby(key)[start].shift(-1)
    return int((nxt < d[end]).sum())


def check_transactions(cfg: P2PConfig, w: dict, t: dict, br: dict | None = None) -> list[tuple[str, bool, str]]:
    r: list[tuple[str, bool, str]] = []

    def add(name, ok, detail=""):
        r.append((name, bool(ok), detail))

    for table, pk in [("lots", "lot_id"), ("work_orders", "work_order_id"), ("sterilization_loads", "load_id"),
                      ("stock_transfers", "transfer_id"), ("purchase_orders", "po_id"), ("customer_orders", "order_id"),
                      ("order_lines", "order_line_id"), ("shipments", "shipment_id"), ("deviations", "deviation_id"),
                      ("capas", "capa_id"), ("component_lots", "component_lot_id")]:
        add(f"pk unique: {table}.{pk}", not t[table][pk].duplicated().any())

    res = set(w["molding_machines"]["machine_id"]) | set(w["production_lines"]["line_id"])
    for name, args in [
        ("lots -> products", ("lots", "sku_id", w["products"], "sku_id", False)),
        ("work_orders -> resources", None),
        ("loads -> sterilizers", ("sterilization_loads", "sterilizer_id", w["sterilizers"], "sterilizer_id", False)),
        ("load_contents -> work_orders", ("load_contents", "work_order_id", t["work_orders"], "work_order_id", False)),
        ("lots -> work_orders", ("lots", "work_order_id", t["work_orders"], "work_order_id", True)),
        ("lots -> loads", ("lots", "sterilization_load_id", t["sterilization_loads"], "load_id", True)),
        ("transfers -> lots", ("stock_transfers", "lot_id", t["lots"], "lot_id", False)),
        ("transfers -> dcs", ("stock_transfers", "to_dc_id", w["distribution_centers"], "dc_id", False)),
        ("POs -> suppliers", ("purchase_orders", "supplier_id", w["suppliers"], "supplier_id", False)),
        ("orders -> customers", ("customer_orders", "customer_id", w["customers"], "customer_id", False)),
        ("order_lines -> orders", ("order_lines", "order_id", t["customer_orders"], "order_id", False)),
        ("shipment_lines -> lots", ("shipment_lines", "lot_id", t["lots"], "lot_id", False)),
        ("shipment_lines -> order_lines", ("shipment_lines", "order_line_id", t["order_lines"], "order_line_id", False)),
        ("allocations -> lots", ("order_allocations", "lot_id", t["lots"], "lot_id", False)),
        ("on_hand -> lots", ("inventory_on_hand", "lot_id", t["lots"], "lot_id", False)),
        ("deviation_lots -> deviations", ("deviation_lots", "deviation_id", t["deviations"], "deviation_id", False)),
        ("capas -> deviations", ("capas", "deviation_id", t["deviations"], "deviation_id", False)),
    ]:
        if args is None:
            bad = set(t["work_orders"]["resource_id"]) - res
            add(f"fk: {name}", not bad, f"{len(bad)} orphan(s)")
            continue
        c, cc, par, pc, nullable = args
        ok, detail = _fk(t[c], cc, par, pc, nullable)
        add(f"fk: {name}", ok, detail)

    # Physical scheduling constraints
    wo = t["work_orders"]
    add("no resource runs two work orders at once", _no_overlap(wo, "resource_id", "start_ts", "end_ts") == 0)
    ld = t["sterilization_loads"]
    add("no chamber runs two loads at once", _no_overlap(ld, "sterilizer_id", "start_ts", "cycle_end_ts") == 0)
    add("loads within pallet capacity", (ld["pallets"] <= ld["pallet_capacity"]).all())
    add("no completed load after live start", (ld[ld["status"] == "completed"]["cycle_end_ts"] <= pd.Timestamp(cfg.live_start)).all())

    # Lot / genealogy rules
    lots = t["lots"].merge(w["products"][["sku_id", "sterile", "make_or_buy"]], on="sku_id")
    mk = lots[lots["source"] == "make"]
    add("every sterile make lot came from a load", mk[mk["sterile"]]["sterilization_load_id"].notna().all())
    add("no non-sterile lot has a load", mk[~mk["sterile"]]["sterilization_load_id"].isna().all())
    passed = set(ld[ld["cycle_result"] == "pass"]["load_id"])
    add("sterile lots only from passing loads", set(mk["sterilization_load_id"].dropna()) <= passed)
    g = t["lot_genealogy"]
    produced = set(g[g["relation"] == "produced"]["to_id"])
    add("every make lot has a 'produced' genealogy edge", set(mk["lot_id"]) <= produced)
    hist_wo = wo[(wo["status"] == "completed") & (~wo["is_opening_stock"])]
    consumed = set(g[(g["relation"] == "consumed_by") & (g["to_type"] == "work_order")]["to_id"])
    add("every completed history WO consumed traceable component lots", set(hist_wo["work_order_id"]) <= consumed,
        f"{len(set(hist_wo['work_order_id']) - consumed)} without")
    add("every buy lot has a received PO", t["lots"][t["lots"]["source"] == "buy"]["purchase_order_id"].isin(
        t["purchase_orders"][t["purchase_orders"]["status"] == "received"]["po_id"]).all())

    # Conservation: ledger == on hand, never negative
    led = t["inventory_ledger"].groupby(["dc_id", "lot_id"])["qty_delta_cases"].sum()
    onh = t["inventory_on_hand"].set_index(["dc_id", "lot_id"])["qty_cases"]
    led_pos = led[led > 0]
    add("ledger balance == on-hand for every DC x lot", led_pos.sort_index().equals(onh.sort_index().astype(led.dtype)),
        f"{len(led_pos)} vs {len(onh)}")
    add("no negative stock", (led >= 0).all())
    tr = t["stock_transfers"].groupby("lot_id")["qty_cases"].sum()
    lq = t["lots"].set_index("lot_id")["qty_cases"]
    add("transferred qty <= lot qty", (tr <= lq.reindex(tr.index)).all())
    rej = set(t["lots"][t["lots"]["status"] == "rejected"]["lot_id"])
    add("rejected lots never shipped or transferred",
        not (rej & set(t["stock_transfers"]["lot_id"])) and not (rej & set(t["shipment_lines"]["lot_id"])))
    al = t["order_allocations"].groupby(["dc_id", "lot_id"])["qty_cases"].sum()
    add("open allocations <= on hand", (al <= onh.reindex(al.index).fillna(0)).all())

    # Shipments respect availability and expiry
    sl = t["shipment_lines"].merge(t["shipments"][["shipment_id", "ship_date"]], on="shipment_id") \
        .merge(t["lots"][["lot_id", "expiry_date", "release_ts"]], on="lot_id")
    add("nothing shipped before lot release", (pd.to_datetime(sl["ship_date"]).dt.tz_localize("UTC") + pd.Timedelta(hours=2)
                                               >= sl["release_ts"]).all())
    exp = sl.dropna(subset=["expiry_date"])
    add("nothing shipped within 30 days of expiry",
        (pd.to_datetime(exp["expiry_date"]) > pd.to_datetime(exp["ship_date"]) + pd.Timedelta(days=30)).all())

    # Service and stock levels
    o = t["customer_orders"]
    hist = t["order_lines"][t["order_lines"]["order_id"].isin(o[o["status"] != "open"]["order_id"])]
    fill = hist["qty_shipped_cases"].sum() / hist["qty_ordered_cases"].sum()
    add("case fill rate >= 97%", fill >= 0.97, f"{fill:.3%}")
    oh = t["inventory_on_hand"].groupby("sku_id")["qty_cases"].sum()
    ship_rate = t["shipment_lines"].merge(t["lots"][["lot_id", "sku_id"]], on="lot_id").groupby("sku_id")["qty_cases"].sum() / cfg.p.history_days
    doh = (oh / ship_rate).dropna()
    kits = set(w["products"][w["products"]["family"] == "procedure_kits"]["sku_id"])
    lo, hi = cfg.cover_days
    other = doh[~doh.index.isin(kits)]
    add(f"median days on hand (non-pack) within {lo - 15}-{hi + 15}", lo - 15 <= other.median() <= hi + 15,
        f"median {other.median():.0f}")
    klo, khi = cfg.kit_cover_days
    kd = doh[doh.index.isin(kits)]
    add(f"median days on hand (packs) within {max(1, klo - 4)}-{khi + 10}", max(1, klo - 4) <= kd.median() <= khi + 10,
        f"median {kd.median():.0f}")

    # Quality
    held = set(t["lots"][t["lots"]["status"] == "on_hold"]["lot_id"])
    add("every on-hold lot is linked to a deviation", held <= set(t["deviation_lots"]["lot_id"]))
    add("archive holds RH-probe precedent on the hero steriliser",
        ((t["deviations"]["type_key"] == "eto_rh_probe_drift") & (t["deviations"]["source"] == "archive")).sum() >= 2)

    # Hero chain -- the whole demo depends on this
    if br is not None:
        add("hero: >= 2 suspect loads in drift window", len(br["suspect_loads"]) >= 2, f"{len(br['suspect_loads'])}")
        add("hero: suspect stock on hand at a DC", br["on_hand_at_dcs"]["cases"] > 0)
        add("hero: suspect stock allocated to open orders", br["open_orders_at_risk"]["cases"] > 0)
        add("hero: >= 1 hospital among customers at risk", len(br["open_orders_at_risk"]["hospitals"]) >= 1)
    return r
