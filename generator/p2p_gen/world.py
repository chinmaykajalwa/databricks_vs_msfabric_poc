"""
Master data: one consistent, seeded model of the business.
===========================================================
Everything downstream (transactions, telemetry, scenarios, documents) hangs
off the IDs created here, so this module is built for referential integrity
first and realism second.

Each table draws from its own RNG, keyed by table name, so changing the
number of customers never changes a single SKU or machine.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pandas as pd

from . import catalogs as C
from .config import P2PConfig
from .demand import (STERILIZER_CYCLES_PER_DAY, customer_models, expected_demand,
                     lines_needed, required_capacity)
from .determinism import rng_for

GS1_EXAMPLE_PREFIX = "0614141"
"""GS1's documented example company prefix -- guaranteed not to be a real
manufacturer's prefix, so UDI-DIs look authentic but are obviously synthetic."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def allocate(n: int, weights) -> list[int]:
    """Split n into integers proportional to weights (largest remainder,
    ties broken by position, so the result is deterministic)."""
    w = np.asarray(weights, dtype=float)
    if n <= 0 or w.sum() <= 0:
        return [0] * len(w)
    raw = n * w / w.sum()
    base = np.floor(raw).astype(int)
    rem = n - int(base.sum())
    order = sorted(range(len(w)), key=lambda i: (-(raw[i] - base[i]), i))
    for i in order[:rem]:
        base[i] += 1
    return base.tolist()


def gtin14(indicator: int, item_ref: int) -> str:
    body = f"{indicator}{GS1_EXAMPLE_PREFIX}{item_ref:05d}"
    assert len(body) == 13
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return body + str((10 - total % 10) % 10)


def gtin_is_valid(code: str) -> bool:
    return len(code) == 14 and code.isdigit() and gtin14(int(code[0]), int(code[8:13])) == code


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _caps(plant_row) -> tuple[str, ...]:
    return tuple(plant_row["capabilities"].split(","))


# ---------------------------------------------------------------------------
# Plants, lines, machines, sterilisers
# ---------------------------------------------------------------------------
def build_plants(cfg: P2PConfig) -> pd.DataFrame:
    rows = []
    for p in C.PLANTS[: cfg.p.n_plants]:
        r = {k: v for k, v in p.items() if k != "capabilities"}
        r["capabilities"] = ",".join(p["capabilities"])
        rows.append(r)
    return pd.DataFrame(rows)


def build_lines_and_machines(cfg: P2PConfig, plants: pd.DataFrame, hours: dict):
    """Machines and lines sized to demand (see demand.py).

    Molding machines: the profile's total is split across molding plants in
    proportion to required molding hours. Other lines: enough to run at
    ~70% utilisation, minimum two per capability.
    """
    rng = rng_for(cfg.seed, "world", "molding_machines")
    molding = [r for _, r in plants.iterrows() if "molding" in _caps(r)]
    need = [max(hours.get((r["plant_id"], "molding"), 0.0), 1.0) for r in molding]
    counts = allocate(cfg.p.n_molding_machines - len(molding), need)
    counts = [c + 1 for c in counts]  # every molding plant keeps at least one machine

    lines, machines = [], []
    mpl = cfg.p.machines_per_molding_line
    for plant, n in zip(molding, counts):
        pid = plant["plant_id"]
        for j in range(math.ceil(n / mpl) if n else 0):
            lines.append({"line_id": f"{pid}-MLD-{j + 1:02d}", "plant_id": pid,
                          "line_type": "molding", "name": f"Molding Cell {j + 1}"})
        for k in range(n):
            tonnage = 500 if k == 0 else int(rng.choice(C.MOLDING_TONNAGES, p=[.10, .20, .30, .25, .15]))
            mid = f"IMM-{pid}-{k + 1:03d}"
            machines.append({
                "machine_id": mid, "plant_id": pid, "line_id": f"{pid}-MLD-{k // mpl + 1:02d}",
                "make_model": str(rng.choice(C.MOLDING_MAKES)), "clamp_tonnage": tonnage,
                "install_year": int(rng.integers(2008, 2025)), "controller_fw": "4.2.1",
                "telemetry_device_id": f"GW-{mid}",
            })

    for _, plant in plants.iterrows():
        for cap, code, label in (("kit_assembly", "KIT", "Kit Assembly"),
                                 ("textiles", "TXT", "Textile Conversion"),
                                 ("wound_care", "WND", "Wound Care Converting")):
            if cap in _caps(plant):
                for j in range(lines_needed(hours.get((plant["plant_id"], cap), 0.0))):
                    lines.append({"line_id": f"{plant['plant_id']}-{code}-{j + 1:02d}",
                                  "plant_id": plant["plant_id"], "line_type": cap,
                                  "name": f"{label} Line {j + 1}"})
    return pd.DataFrame(lines), pd.DataFrame(machines)


def build_sterilizers(cfg: P2PConfig, plants: pd.DataFrame, pallets: dict) -> pd.DataFrame:
    """Chambers split across EtO plants in proportion to sterile pallets/day."""
    rng = rng_for(cfg.seed, "world", "sterilizers")
    eto = [r for _, r in plants.iterrows() if "eto_sterilization" in _caps(r)]
    rows = []
    counts = [c + 1 for c in allocate(cfg.p.n_sterilizers - len(eto),
                                      [max(pallets.get(r["plant_id"], 0.0), 1.0) for r in eto])]
    for plant, n in zip(eto, counts):
        pid = plant["plant_id"]
        for k in range(n):
            sid = f"STZ-{pid}-{k + 1:02d}"
            vol = int(rng.choice([84, 112, 140, 168]))  # 12-24 pallet industrial chambers
            rows.append({
                "sterilizer_id": sid, "plant_id": pid, "method": "EtO",
                "chamber_volume_m3": vol, "pallet_capacity": vol // 7,
                # Validated cycle windows (setpoint, low, high).
                "precond_temp_setpoint_c": 46.0, "precond_rh_low_pct": 50.0, "precond_rh_high_pct": 80.0,
                "exposure_temp_setpoint_c": 52.0, "exposure_temp_low_c": 48.0, "exposure_temp_high_c": 56.0,
                "exposure_rh_setpoint_pct": 65.0, "exposure_rh_low_pct": 50.0, "exposure_rh_high_pct": 80.0,
                "eto_conc_setpoint_mg_l": 550.0, "eto_conc_low_mg_l": 450.0, "eto_conc_high_mg_l": 650.0,
                "vacuum_setpoint_kpa": 8.0,
                "exposure_minutes": int(rng.choice([240, 300, 360])),
                "aeration_hours": int(rng.choice([12, 16, 24])),
                "last_requalification_date": cfg.history_start.date() - timedelta(days=int(rng.integers(30, 330))),
                "controller_fw": "7.1.0", "telemetry_device_id": f"GW-{sid}",
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Suppliers and components
# ---------------------------------------------------------------------------
def build_suppliers(cfg: P2PConfig) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "suppliers")
    cats = C.SUPPLIER_CATEGORIES
    n = cfg.p.n_suppliers
    # Two per category first (alternates must exist), then weighted extras.
    seq = [c for c in cats for _ in range(2)][:n]
    extra_w = np.array([3 if c[0] in ("finished_goods_oem", "packaging", "glove_oem") else 1 for c in cats], float)
    while len(seq) < n:
        seq.append(cats[int(rng.choice(len(cats), p=extra_w / extra_w.sum()))])

    adj = list(rng.permutation(C.SUPPLIER_ADJ))
    used: list[str] = []
    rows = []
    for i, (cat, countries, nouns) in enumerate(seq):
        country = str(rng.choice(countries))
        name = None
        for attempt in range(50):
            cand = f"{adj[(i + attempt) % len(adj)]} {rng.choice(nouns)} {rng.choice(C.COUNTRY_SUFFIX[country])}"
            if cand not in used:
                name = cand
                break
        name = name or f"{adj[i % len(adj)]} {nouns[0]} {i}"
        used.append(name)
        lo, hi = C.COUNTRY_LEAD_DAYS[country]
        rows.append({
            "supplier_id": f"SUP-{i + 1:04d}", "name": name, "category": cat, "country": country,
            "lead_time_days": int(rng.integers(lo, hi + 1)),
            "on_time_delivery_pct": round(float(rng.uniform(86, 99.5)), 1),
            "risk_tier": "low" if country in ("US", "DE") else ("medium" if country in ("MX", "KR", "MY", "TH") else "high"),
            "approved_since": cfg.history_start.date() - timedelta(days=int(rng.integers(365, 365 * 15))),
        })
    return pd.DataFrame(rows)


def build_components(cfg: P2PConfig, suppliers: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "components")
    rows = []
    for cid, desc, cat, uom, cost in C.COMPONENTS:
        pool = suppliers.loc[suppliers["category"] == cat, "supplier_id"].tolist()
        primary = str(rng.choice(pool))
        alts = [s for s in pool if s != primary]
        rows.append({"component_id": cid, "description": desc, "category": cat, "uom": uom,
                     "unit_cost_usd": cost, "primary_supplier_id": primary,
                     "alternate_supplier_id": str(rng.choice(alts)) if alts else None})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Products, molds, BOM
# ---------------------------------------------------------------------------
TIERS = ["", "SelectLine", "ValueLine", "ProLine", "UltraLine", "EcoLine", "PlusLine", "MaxLine",
         "CoreLine", "PrimeLine"]


def _family_combos(family: str, rng, quota: int):
    """Distinct (template, variant, sterile, each_per_case, tier) combos."""
    templates = C.SKU_TEMPLATES[family]
    out = []
    for tier in TIERS:
        rnd = []
        for t_idx, (base, variants, attrs) in enumerate(templates):
            epcs = attrs.get("epc", C.FAMILIES[family]["each_per_case"])
            sterile_opts = [True, False] if attrs.get("sterile") == "both" else [bool(attrs.get("sterile"))]
            for v in variants:
                for st in sterile_opts:
                    for epc in epcs:
                        rnd.append((t_idx, v, st, epc, tier))
        out.extend(rnd[i] for i in rng.permutation(len(rnd)))
        if len(out) >= quota:
            break
    if len(out) < quota:
        raise ValueError(f"family {family}: only {len(out)} combos for quota {quota}")
    return out[:quota]


def build_products(cfg: P2PConfig, plants: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "products")
    fams = list(C.FAMILIES)
    quotas = allocate(cfg.p.n_skus, [C.FAMILIES[f]["share"] for f in fams])

    eto_plants = [r for _, r in plants.iterrows() if "eto_sterilization" in _caps(r)]

    def steriliser_site(prod_plant):
        if prod_plant is None:
            return None
        pr = plants.set_index("plant_id").loc[prod_plant]
        if "eto_sterilization" in pr["capabilities"].split(","):
            return prod_plant
        if not eto_plants:
            return None
        return min(eto_plants, key=lambda e: haversine_km(pr["lat"], pr["lon"], e["lat"], e["lon"]))["plant_id"]

    rows, idx = [], 0
    for fam, quota in zip(fams, quotas):
        spec = C.FAMILIES[fam]
        makers = [r["plant_id"] for _, r in plants.iterrows()
                  if spec["make_capability"] and spec["make_capability"] in _caps(r)]
        vendor_pool = suppliers.loc[suppliers["category"] == spec["supplier_category"], "supplier_id"].tolist()
        lo_cost, hi_cost = spec["cost"]
        for t_idx, variant, sterile, epc, tier in _family_combos(fam, rng, quota):
            base, _, attrs = C.SKU_TEMPLATES[fam][t_idx]
            make = bool(makers)
            prod_plant = str(rng.choice(makers)) if make else None
            label_st = ", Sterile" if (sterile and attrs.get("sterile") == "both") else (
                ", Non-Sterile" if attrs.get("sterile") == "both" else "")
            desc = f"{base}{' ' + tier if tier else ''}, {variant}{label_st}, {epc}/cs"
            temp = attrs.get("temp")
            part_g = round(float(rng.uniform(*attrs["part_g"])), 1) if "part_g" in attrs else None
            if sterile:
                method = "EtO" if make else ("Gamma (supplier)" if fam in ("exam_gloves", "general_supplies") else "EtO (supplier)")
                shelf = 24 if temp else int(rng.choice([36, 48, 60]))
            else:
                method = "none"
                shelf = 24 if temp else (60 if fam == "exam_gloves" else None)
            c_lo, c_hi = attrs.get("cost", (lo_cost, hi_cost))
            cost = float(np.exp(rng.uniform(np.log(c_lo), np.log(c_hi))))
            item_ref = 10000 + idx
            rows.append({
                "sku_id": f"SKU-{100000 + idx}",
                "description": desc, "family": fam, "base_name": base, "variant": variant,
                "product_line_tier": tier or "Standard",
                "make_or_buy": "make" if make else "buy",
                "producing_plant_id": prod_plant,
                "sterilization_plant_id": steriliser_site(prod_plant) if (make and sterile) else None,
                "primary_supplier_id": None if make else str(rng.choice(vendor_pool)),
                "sterile": bool(sterile), "sterilization_method": method,
                "device_class": attrs.get("cls", "I"),
                "resin": attrs.get("resin"), "part_weight_g": part_g,
                "udi_di": gtin14(0, item_ref), "case_gtin": gtin14(1, item_ref),
                "uom": "EA", "each_per_case": int(epc),
                "shelf_life_months": shelf,
                "temp_sensitive": bool(temp),
                "storage_temp_min_c": float(temp[0]) if temp else None,
                "storage_temp_max_c": float(temp[1]) if temp else None,
                "unit_cost_usd": round(cost, 4),
                "list_price_usd": round(cost * float(rng.uniform(1.35, 1.9)), 4),
                "demand_weight": round(float(rng.lognormal(0.0, 1.7)), 4),
            })
            idx += 1

    df = pd.DataFrame(rows)
    # ABC class by demand weight (A = top 80% of volume).
    order = df["demand_weight"].sort_values(ascending=False)
    cum = order.cumsum() / order.sum()
    abc = pd.Series(np.where(cum <= 0.80, "A", np.where(cum <= 0.95, "B", "C")), index=order.index)
    df["abc_class"] = abc.reindex(df.index)
    df["shelf_life_months"] = df["shelf_life_months"].astype("Int64")
    return df


def build_molds(cfg: P2PConfig, products: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "molds")
    rows = []
    made = products[(products["family"] == "molded_disposables") & (products["make_or_buy"] == "make")]
    for _, p in made.iterrows():
        g = float(p["part_weight_g"])
        cav_opts = [16, 24, 32] if g < 10 else [8, 12, 16] if g < 40 else [4, 6, 8] if g < 100 else [1, 2, 4]
        cav = int(rng.choice(cav_opts))
        shot = round(g * cav * 1.05, 1)
        need = 0.55 * shot
        tonnage = next((t for t in sorted(C.MOLDING_TONNAGES) if t >= need), 500)
        cycle = round(max(6.0, 6.0 + 0.12 * g + float(rng.uniform(-1, 1))), 1)
        r = C.RESINS[p["resin"]]
        inj = int(rng.integers(650, 1400))
        rows.append({
            "mold_id": f"MLD-{p['sku_id'][4:]}", "sku_id": p["sku_id"], "plant_id": p["producing_plant_id"],
            "resin": p["resin"], "cavities": cav, "shot_weight_g": shot, "min_clamp_tonnage": tonnage,
            "nominal_cycle_s": cycle,
            "melt_temp_setpoint_c": float(r["melt_c"][0] + int(rng.integers(-5, 6))), "melt_temp_tol_c": float(r["melt_c"][1]),
            "mold_temp_setpoint_c": float(r["mold_c"][0] + int(rng.integers(-3, 4))), "mold_temp_tol_c": float(r["mold_c"][1]),
            "injection_pressure_setpoint_bar": float(inj), "injection_pressure_tol_bar": round(inj * 0.06, 1),
            "hold_pressure_setpoint_bar": round(inj * 0.6, 1),
            "cooling_time_s": round(cycle * 0.5, 1),
            "cavity_pressure_alarm_bar": round(inj * 0.45, 1),
        })
    return pd.DataFrame(rows)


def build_bom(cfg: P2PConfig, products: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "bom")
    rows = []

    def add(parent, kind, ref, qty, uom):
        rows.append({"parent_sku_id": parent, "line_no": 0, "component_kind": kind,
                     "component_ref": ref, "qty_per_each": round(float(qty), 5), "uom": uom})

    def pack(p, pouch, carton):
        if p["sterile"] and pouch:
            add(p["sku_id"], "component", pouch, 1, "ea")
        add(p["sku_id"], "component", "PKG-LBL-P", 1, "ea")
        add(p["sku_id"], "component", carton, 1 / p["each_per_case"], "ea")
        add(p["sku_id"], "component", "PKG-LBL-C", 1 / p["each_per_case"], "ea")

    non_kit = products[products["family"] != "procedure_kits"]
    by_base = {b: g for b, g in non_kit.groupby("base_name", sort=True)}

    def kit_contents(profile):
        allowed = [(b, q) for b, q in C.KIT_CONTENTS[profile] if b in by_base]
        lo, hi = C.KIT_LINES[profile]
        k = min(len(allowed), int(rng.integers(lo, hi + 1)))
        picked = [allowed[i] for i in sorted(rng.choice(len(allowed), size=k, replace=False))]
        if profile != "surgical_L":  # one glove type per small kit
            gloves = [x for x in picked if "Gloves" in x[0]]
            picked = [x for x in picked if "Gloves" not in x[0]] + gloves[:1]
        out = []
        for base, (qlo, qhi) in picked:
            cands = by_base[base]
            if cands["sterile"].any():          # contents go in sterile, prefer sterile variants
                cands = cands[cands["sterile"]]
            out.append((str(rng.choice(cands["sku_id"].tolist())), int(rng.integers(qlo, qhi + 1))))
        return out

    for _, p in products[products["make_or_buy"] == "make"].iterrows():
        sid, fam, base = p["sku_id"], p["family"], p["base_name"]
        attrs = next(a for b, _, a in C.SKU_TEMPLATES[fam] if b == base)
        if fam == "molded_disposables":
            add(sid, "component", C.RESINS[p["resin"]]["component_id"], p["part_weight_g"] / 1000 * 1.03, "kg")
            pack(p, "PKG-POU-L" if p["part_weight_g"] > 60 else "PKG-POU-S", "PKG-CTN-M")
        elif fam == "textiles":
            area = (2.6 if "Gown" in base else 0.15 if ("Cap" in base or "Shoe" in base)
                    else 1.2 if ("Cover" in base) else 4.5)
            add(sid, "component", attrs["fabric"], area * float(rng.uniform(0.92, 1.08)), "m2")
            if "Surgical Gown" in base:
                add(sid, "component", "TX-CUFF", 1, "pair")
                add(sid, "component", "TX-TIE", 2.5, "m")
                add(sid, "component", "TX-HOOK", 1, "ea")
                if "Reinforced" in base and "Non" not in base:
                    add(sid, "component", "NW-SPL-RF", 1, "ea")
                if p["sterile"]:
                    add(sid, "component", "PKG-CSR-36", 1, "ea")
            if "Laparotomy" in base:
                add(sid, "component", "NW-ABS-RF", 1, "ea")
            pack(p, "PKG-POU-L", "PKG-CTN-L")
        elif fam == "procedure_kits":
            for comp, qty in kit_contents(attrs["contents"]):
                add(sid, "sku", comp, qty, "ea")
            add(sid, "component", attrs["tray"], 1, "ea")
            add(sid, "component", "PKG-CSR-45", 1, "ea")
            add(sid, "component", "PKG-TYV-L", 1, "ea")
            add(sid, "component", "PKG-IFU", 1, "ea")
            pack(p, None, "PKG-CTN-L")
        elif fam == "wound_care":
            mat = attrs["mat"]
            uom = "kg" if mat == "WC-HYD" else ("ea" if mat == "WC-GAU" else "m2")
            qty = 0.03 if uom == "kg" else (1 if uom == "ea" else float(rng.uniform(0.003, 0.03)))
            add(sid, "component", mat, qty, uom)
            if mat in ("WC-PUF", "WC-AG"):
                add(sid, "component", "WC-SIL", qty, "m2")
            pack(p, "PKG-POU-S", "PKG-CTN-S")

    df = pd.DataFrame(rows)
    df["line_no"] = df.groupby("parent_sku_id").cumcount() + 1
    return df


# ---------------------------------------------------------------------------
# Distribution network, customers, fleet, routes, DC assets
# ---------------------------------------------------------------------------
def build_dcs(cfg: P2PConfig) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "dcs")
    chosen = C.DCS[: cfg.p.n_dcs]
    ids = [d["dc_id"] for d in chosen]
    outage = cfg.outage_dc_id or next((d["dc_id"] for d in chosen if d["outage_default"]), ids[-1])
    if outage not in ids:
        raise ValueError(f"outage_dc_id {outage} not in profile DCs {ids}")
    rows = []
    for i, d in enumerate(chosen):
        rows.append({
            "dc_id": d["dc_id"], "name": f"{d['metro']} Distribution Center", "metro": d["metro"],
            "city": d["city"], "state": d["state"], "lat": d["lat"], "lon": d["lon"],
            "region": d["region"], "size_sqft": d["size_sqft"], "automation_level": d["automation"],
            "dock_doors": d["size_sqft"] // 9000,
            "has_cold_room": bool(i == 0 or rng.random() < 0.5),
            "opened_year": int(rng.integers(2005, 2023)),
            "is_outage_scenario_dc": d["dc_id"] == outage,
        })
    return pd.DataFrame(rows)


def build_idns(cfg: P2PConfig, dcs: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "idns")
    homes = []
    for dc_id, n in zip(dcs["dc_id"], allocate(cfg.p.n_idns, dcs["size_sqft"].tolist())):
        homes.extend([dc_id] * n)
    names: list[str] = []
    rows = []
    for i, home in enumerate(homes):
        for attempt in range(100):
            cand = f"{rng.choice(C.NATURE_WORDS)} Health {rng.choice(['System', 'Partners', 'Network', 'Alliance'])}"
            if cand not in names:
                break
        else:
            cand = f"{C.NATURE_WORDS[i % len(C.NATURE_WORDS)]} Health System {i}"
        names.append(cand)
        rows.append({"idn_id": f"IDN-{i + 1:03d}", "name": cand, "home_dc_id": home,
                     "contract_tier": str(rng.choice(["Tier 1", "Tier 2", "Tier 3"], p=[.25, .45, .30])),
                     "contract_renewal_date": cfg.history_start.date() + timedelta(days=int(rng.integers(30, 900)))})
    return pd.DataFrame(rows)


def _customer_name(rng, ctype, town) -> str:
    nature, saint = str(rng.choice(C.NATURE_WORDS)), str(rng.choice(C.SAINTS))
    if ctype == "hospital":
        opts = [f"{town} Regional Medical Center", f"St. {saint} Hospital", f"{nature} Community Hospital",
                f"{nature} Medical Center"]
    elif ctype == "ambulatory_surgery_center":
        opts = [f"{town} Surgery Center", f"{nature} Ambulatory Surgical Center", f"{nature} Outpatient Surgery"]
    elif ctype == "extended_care":
        opts = [f"{nature} Rehabilitation & Nursing", f"{nature} Senior Living", f"{nature} Care Center",
                f"{nature} Manor", f"{nature} Gardens Health Campus", f"The {nature} at {town}"]
    elif ctype == "physician_office":
        opts = [f"{rng.choice(C.SURNAMES)} {rng.choice(C.SPECIALTIES)}", f"{town} {rng.choice(C.SPECIALTIES)}"]
    else:
        opts = [f"{nature} Home Health", f"{town} Home Care Services", f"{nature} Visiting Nurses",
                f"{rng.choice(C.SURNAMES)} Home Health Partners"]
    name = str(rng.choice(opts))
    return name


def _unique_customer_name(rng, ctype, town, used: set) -> str:
    for _ in range(25):
        name = _customer_name(rng, ctype, town)
        if name not in used:
            break
    else:
        for suffix in C.FACILITY_SUFFIXES:
            if f"{name} - {suffix}" not in used:
                name = f"{name} - {suffix}"
                break
        else:
            base, n = name, 2
            while name in used:
                name, n = f"{base} {n}", n + 1
    used.add(name)  # membership only; never iterated, so order is irrelevant
    return name


def build_customers(cfg: P2PConfig, dcs: pd.DataFrame, idns: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "customers")
    types = list(C.CUSTOMER_TYPES)
    shares = np.array([C.CUSTOMER_TYPES[t][0] for t in types])
    dc_cat = {d["dc_id"]: d for d in C.DCS}
    used: set[str] = set()
    rows, i = [], 0
    for dc_id, n in zip(dcs["dc_id"], allocate(cfg.p.n_customers, dcs["size_sqft"].tolist())):
        towns = dc_cat[dc_id]["towns"]
        tw = np.array([0.35] + [0.65 / (len(towns) - 1)] * (len(towns) - 1))
        local_idns = idns.loc[idns["home_dc_id"] == dc_id, "idn_id"].tolist() or idns["idn_id"].tolist()
        for _ in range(n):
            ctype = types[int(rng.choice(len(types), p=shares / shares.sum()))]
            town, state, tlat, tlon = towns[int(rng.choice(len(towns), p=tw))]
            _, idn_p = C.CUSTOMER_TYPES[ctype]
            beds = None
            if ctype == "hospital":
                beds = int(np.clip(rng.lognormal(np.log(220), 0.6), 25, 1200))
            elif ctype == "extended_care":
                beds = int(rng.integers(40, 221))
            base_vol = {"hospital": (beds or 100) / 50, "ambulatory_surgery_center": 1.5,
                        "extended_care": (beds or 60) / 60, "physician_office": 0.3, "home_health": 0.5}[ctype]
            rows.append({
                "customer_id": f"CUS-{i + 1:05d}",
                "name": _unique_customer_name(rng, ctype, town, used),
                "customer_type": ctype,
                "idn_id": str(rng.choice(local_idns)) if rng.random() < idn_p else None,
                "bed_count": beds,
                "city": town, "state": state,
                "lat": round(tlat + float(rng.normal(0, 0.06)), 5),
                "lon": round(tlon + float(rng.normal(0, 0.06)), 5),
                "dc_id": dc_id,
                "volume_index": round(base_vol * float(rng.lognormal(0, 0.4)), 3),
                "lum_program": bool(ctype == "hospital" and rng.random() < 0.30),
                "account_open_date": cfg.history_start.date() - timedelta(days=int(rng.integers(90, 365 * 20))),
            })
            i += 1
    df = pd.DataFrame(rows)
    df["bed_count"] = df["bed_count"].astype("Int64")
    df["contract_type"] = np.where(df["idn_id"].notna(), "IDN contract", "GPO")
    return df


def build_fleet(cfg: P2PConfig, dcs: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "fleet")
    per_dc = customers.groupby("dc_id").size().reindex(dcs["dc_id"]).fillna(0).tolist()
    counts = allocate(cfg.p.n_trucks - 2 * len(dcs), per_dc)
    counts = [c + 2 for c in counts]  # every DC gets at least two trucks
    vtypes = list(C.VEHICLE_TYPES)
    vshare = np.array([C.VEHICLE_TYPES[v]["share"] for v in vtypes])
    rows, i = [], 0
    for dc_id, n in zip(dcs["dc_id"], counts):
        for k in range(n):
            vt = vtypes[0] if k == 0 else vtypes[int(rng.choice(len(vtypes), p=vshare / vshare.sum()))]
            spec = C.VEHICLE_TYPES[vt]
            reefer = vt == "tractor_53ft" and (k == 0 or rng.random() < 0.15)
            vid = f"TRK-{i + 1:04d}"
            rows.append({"vehicle_id": vid, "home_dc_id": dc_id, "vehicle_type": vt,
                         "make_model": str(rng.choice(spec["makes"])), "model_year": int(rng.integers(2018, 2027)),
                         "capacity_pallets": spec["pallets"], "has_reefer": bool(reefer),
                         "reefer_setpoint_c": 15.0 if reefer else None,
                         "telematics_device_id": f"TLM-{vid}", "telematics_fw": "2.8.4"})
            i += 1
    return pd.DataFrame(rows)


DAY_PATTERNS = {1: [("MON-FRI", 5)], 2: [("MON-WED-FRI", 3), ("TUE-THU", 2)]}


def build_routes(cfg: P2PConfig, dcs: pd.DataFrame, customers: pd.DataFrame, fleet: pd.DataFrame):
    """Sweep-and-sequence routing, then a feasible truck schedule.

    Customers are grouped into town-clustered territories (never mixing
    distant towns -- a pure bearing sweep paired Cheyenne with Pueblo),
    sequenced by greedy nearest-neighbour, and timed at 55 km/h road speed
    with service time per stop. Each truck runs one route per day on a fixed
    day pattern, so no truck is ever in two places at once -- something the
    fleet telemetry in step 3 depends on.
    """
    def hhmm(m):
        m = int(m)
        return f"{(m // 60) % 24:02d}:{m % 60:02d}" + (" +1d" if m >= 1440 else "")

    routes, stops = [], []
    cust_route = {}
    customers = customers.copy()
    customers["route_id_tmp"] = None
    customers["is_hospital"] = (customers["customer_type"] == "hospital").astype(int)
    for _, dc in dcs.iterrows():
        trucks = fleet.loc[fleet["home_dc_id"] == dc["dc_id"], "vehicle_id"].tolist()
        cust = customers[customers["dc_id"] == dc["dc_id"]].copy()
        cust["town"] = cust["city"] + ", " + cust["state"]
        cust["bearing"] = np.arctan2(cust["lat"] - dc["lat"], cust["lon"] - dc["lon"])
        n_routes = max(1, min(len(cust), len(trucks) * cfg.p.routes_per_truck))

        # Town-clustered territories: a route never mixes distant towns.
        towns = (cust.groupby("town").agg(n=("customer_id", "size"), b=("bearing", "mean"))
                 .sort_values(["b"]).reset_index())
        chunks = []
        if n_routes >= len(towns):
            per_town = [1 + x for x in allocate(n_routes - len(towns), towns["n"].tolist())]
            for town, k in zip(towns["town"], per_town):
                tc = cust[cust["town"] == town].sort_values(["bearing", "customer_id"])
                chunks.extend(c for c in np.array_split(tc.index.to_numpy(), min(k, len(tc))) if len(c))
        else:  # tiny fleet: group neighbouring towns
            for grp in np.array_split(np.arange(len(towns)), n_routes):
                tc = cust[cust["town"].isin(towns.loc[grp, "town"])].sort_values(["bearing", "customer_id"])
                chunks.append(tc.index.to_numpy())

        dc_routes = []
        for k, chunk in enumerate(chunks):
            rid = f"R-{dc['dc_id']}-{k + 1:03d}"
            remaining = list(chunk)
            pos = (dc["lat"], dc["lon"])
            elapsed, total_km, seq, rstops = 0.0, 0.0, 0, []
            while remaining:
                nxt = min(remaining, key=lambda j: (haversine_km(*pos, customers.at[j, "lat"], customers.at[j, "lon"]), j))
                c = customers.loc[nxt]
                d = haversine_km(*pos, c["lat"], c["lon"]) * 1.25  # road-distance factor
                total_km += d
                elapsed += d / 55 * 60
                seq += 1
                rstops.append({"route_id": rid, "stop_seq": seq, "customer_id": c["customer_id"],
                               "leg_km": round(d, 1), "planned_arrival_offset_min": round(elapsed, 1)})
                elapsed += 35 if c["customer_type"] == "hospital" else 20
                cust_route[c["customer_id"]] = rid
                customers.at[nxt, "route_id_tmp"] = rid
                pos = (c["lat"], c["lon"])
                remaining.remove(nxt)
            return_km = haversine_km(*pos, dc["lat"], dc["lon"]) * 1.25
            total_km += return_km
            elapsed += return_km / 55 * 60
            stops.extend(rstops)
            dc_routes.append({"route_id": rid, "dc_id": dc["dc_id"], "stops": seq,
                              "planned_km": round(total_km, 1), "planned_duration_min": round(elapsed, 1)})

        # Day patterns: each truck runs one route per day, alternating between
        # its routes on fixed days (e.g. Mon/Wed/Fri and Tue/Thu). Routes with
        # the most hospital volume get the more frequent pattern.
        patterns = DAY_PATTERNS[cfg.p.routes_per_truck]
        hosp = customers.loc[customers["dc_id"] == dc["dc_id"]].groupby("route_id_tmp")["is_hospital"].sum()
        order = sorted(range(len(dc_routes)),
                       key=lambda i: (-hosp.get(dc_routes[i]["route_id"], 0), -dc_routes[i]["planned_duration_min"], i))
        nt = len(trucks)
        for pos_i, ridx in enumerate(order):
            p_idx, slot = divmod(pos_i, nt)
            r = dc_routes[ridx]
            dep = 5 * 60
            ret = dep + r["planned_duration_min"]
            r.update({"vehicle_id": trucks[slot], "service_days": patterns[p_idx][0],
                      "deliveries_per_week": patterns[p_idx][1],
                      "planned_departure_min": dep, "planned_return_min": int(round(ret)),
                      "planned_departure_local": hhmm(dep), "planned_return_local": hhmm(ret)})
        routes.extend(sorted(dc_routes, key=lambda r: r["route_id"]))
    cols = ["route_id", "dc_id", "vehicle_id", "service_days", "deliveries_per_week", "planned_departure_local",
            "planned_return_local", "planned_departure_min", "planned_return_min", "stops",
            "planned_km", "planned_duration_min"]
    return pd.DataFrame(routes)[cols], pd.DataFrame(stops), cust_route


_ASSET_ABBR = {"conveyor_segment": "CNV", "induction_station": "IND", "crossbelt_sorter": "SRT",
               "amr": "AMR", "shuttle_asrs": "ASR", "palletizer": "PAL", "stretch_wrapper": "WRP"}


def build_dc_assets(cfg: P2PConfig, dcs: pd.DataFrame) -> pd.DataFrame:
    rng = rng_for(cfg.seed, "world", "dc_assets")
    types = list(C.DC_ASSET_TYPES)
    weights = [r["size_sqft"] * (1.2 if r["automation_level"] == "high" else 0.8) for _, r in dcs.iterrows()]
    rows = []
    for (_, dc), n in zip(dcs.iterrows(), allocate(cfg.p.n_dc_assets, weights)):
        per_type = allocate(n, [C.DC_ASSET_TYPES[t][0] for t in types])
        si, ci = types.index("crossbelt_sorter"), types.index("conveyor_segment")
        if per_type[si] == 0 and per_type[ci] > 0:  # every DC needs a sorter for the demo
            per_type[si], per_type[ci] = 1, per_type[ci] - 1
        for t, cnt in zip(types, per_type):
            _, zone, crit, (kw_lo, kw_hi) = C.DC_ASSET_TYPES[t]
            for k in range(cnt):
                aid = f"{dc['dc_id']}-{_ASSET_ABBR[t]}-{k + 1:03d}"
                speed = (round(float(rng.uniform(1.0, 2.5)), 2) if t == "conveyor_segment"
                         else round(float(rng.uniform(2.0, 2.5)), 2) if t == "crossbelt_sorter" else None)
                tput = {"crossbelt_sorter": (8000, 15000), "induction_station": (1800, 3000),
                        "palletizer": (600, 1200), "shuttle_asrs": (400, 900)}.get(t)
                rows.append({
                    "asset_id": aid, "dc_id": dc["dc_id"], "asset_type": t, "zone": zone, "criticality": crit,
                    "install_year": int(rng.integers(max(dc["opened_year"], 2005), 2026)),
                    "rated_motor_kw": round(float(rng.uniform(kw_lo, kw_hi)), 2),
                    "rated_speed_mps": speed,
                    "rated_throughput_per_hr": int(rng.integers(*tput)) if tput else None,
                    "telemetry_device_id": f"GW-{aid}", "controller_fw": "3.4.0",
                })
    df = pd.DataFrame(rows)
    df["rated_throughput_per_hr"] = df["rated_throughput_per_hr"].astype("Int64")
    return df


TABLE_DESCRIPTIONS = {
    "plants": "Manufacturing plants and their capabilities (molding, textiles, kit assembly, wound care, EtO sterilisation).",
    "production_lines": "Lines per plant. Molding lines group injection-molding machines into cells.",
    "molding_machines": "Injection-molding machines. `telemetry_device_id` is the key the molding stream will carry.",
    "sterilizers": "EtO sterilisation chambers with their validated cycle windows (the hero-scenario asset).",
    "suppliers": "Raw-material, packaging and finished-goods suppliers. Every category has at least two.",
    "components": "Purchased raw materials and packaging, each with primary and alternate supplier.",
    "products": "Finished-goods SKUs with UDI-DI (GS1 example prefix), make/buy, sterility, cost, ABC class.",
    "molds": "One mold per in-house molded SKU, with the setpoints and tolerances the molding physics will use.",
    "bom": "Bills of material. `component_kind = sku` rows are kit contents (finished goods inside procedure packs).",
    "distribution_centers": "DCs. Exactly one is flagged as the outage-scenario DC.",
    "idns": "Integrated delivery networks (health systems) that group customers under one contract.",
    "customers": "Provider sites (hospitals, ASCs, extended care, physician offices, home health). FICTIONAL names.",
    "fleet_vehicles": "Owned delivery trucks; reefer units carry temperature-sensitive products.",
    "routes": "Town-clustered delivery routes, each run by one truck on a fixed day pattern.",
    "route_stops": "Stop sequence and planned arrival offsets per route.",
    "capacity_plan": "Required vs available capacity per plant capability, sized to the synthetic demand (~70% target utilisation).",
    "dc_assets": "DC automation assets (conveyors, sorters, AMRs, ASRS, palletisers) that will stream telemetry.",
}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def build_capacity_plan(plants, lines, machines, sterilizers, hours, pallets) -> pd.DataFrame:
    rows = []
    for _, p in plants.iterrows():
        pid = p["plant_id"]
        for cap in _caps(p):
            if cap == "eto_sterilization":
                ch = sterilizers[sterilizers["plant_id"] == pid]
                cap_pal = float((ch["pallet_capacity"] * STERILIZER_CYCLES_PER_DAY).sum())
                req = pallets.get(pid, 0.0)
                rows.append({"plant_id": pid, "capability": cap, "resources": len(ch),
                             "required_per_day": round(req, 1), "unit": "pallets",
                             "capacity_per_day": round(cap_pal, 1),
                             "forecast_utilisation": round(req / cap_pal, 3) if cap_pal else None})
                continue
            if cap == "molding":
                n = int((machines["plant_id"] == pid).sum())
            else:
                n = int(((lines["plant_id"] == pid) & (lines["line_type"] == cap)).sum())
            req = hours.get((pid, cap), 0.0)
            rows.append({"plant_id": pid, "capability": cap, "resources": n,
                         "required_per_day": round(req, 1), "unit": "running hours",
                         "capacity_per_day": 24.0 * n,
                         "forecast_utilisation": round(req / (24.0 * n), 3) if n else None})
    return pd.DataFrame(rows)


def build_world(cfg: P2PConfig) -> dict[str, pd.DataFrame]:
    plants = build_plants(cfg)
    suppliers = build_suppliers(cfg)
    components = build_components(cfg, suppliers)
    products = build_products(cfg, plants, suppliers)
    molds = build_molds(cfg, products)
    bom = build_bom(cfg, products)
    dcs = build_dcs(cfg)
    idns = build_idns(cfg, dcs)
    customers = build_customers(cfg, dcs, idns)
    fleet = build_fleet(cfg, dcs, customers)
    routes, route_stops, cust_route = build_routes(cfg, dcs, customers, fleet)
    customers["route_id"] = customers["customer_id"].map(cust_route)
    customers["deliveries_per_week"] = customers["route_id"].map(routes.set_index("route_id")["deliveries_per_week"])
    dc_assets = build_dc_assets(cfg, dcs)
    # Capacity is sized to demand: same demand model step 2 simulates.
    _, d_net = expected_demand(customer_models(cfg.seed, products, customers))
    hours, pallets = required_capacity(products, molds, d_net)
    lines, machines = build_lines_and_machines(cfg, plants, hours)
    sterilizers = build_sterilizers(cfg, plants, pallets)
    capacity_plan = build_capacity_plan(plants, lines, machines, sterilizers, hours, pallets)
    return {
        "plants": plants, "production_lines": lines, "molding_machines": machines,
        "sterilizers": sterilizers, "suppliers": suppliers, "components": components,
        "products": products, "molds": molds, "bom": bom, "distribution_centers": dcs,
        "idns": idns, "customers": customers, "fleet_vehicles": fleet, "routes": routes,
        "route_stops": route_stops, "dc_assets": dc_assets, "capacity_plan": capacity_plan,
    }
