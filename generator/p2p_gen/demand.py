"""
Demand model and capacity sizing, shared by steps 1 and 2.
==========================================================
Customers buy from a formulary (their contracted assortment). Expected
demand per (SKU, DC) follows analytically from the formularies, and plant
capacity is SIZED to that demand at a target utilisation.

Why size capacity instead of hard-coding it: a fixed "2 lines per plant"
either starves the DCs (lines at 100%, orders unfilled) or leaves plants
idle, and either way the scenario numbers stop meaning anything. Step 1
uses this module to size lines, machines and sterilisers; step 2 uses the
SAME functions (same seeds) to simulate, so they cannot disagree.

Say it on the slide: capacity is sized to the PoC's synthetic demand, not
taken from any real plant.
"""
from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
import pandas as pd

from . import catalogs as C
from .determinism import rng_for

AFFINITY = {
    "hospital":                  {"molded_disposables": 1.0, "textiles": 1.0, "procedure_kits": 1.2, "wound_care": 0.8, "exam_gloves": 1.2, "general_supplies": 1.0},
    "ambulatory_surgery_center": {"molded_disposables": 0.8, "textiles": 1.2, "procedure_kits": 1.5, "wound_care": 0.4, "exam_gloves": 1.0, "general_supplies": 0.6},
    "extended_care":             {"molded_disposables": 0.7, "textiles": 0.4, "procedure_kits": 0.1, "wound_care": 1.5, "exam_gloves": 1.2, "general_supplies": 1.5},
    "physician_office":          {"molded_disposables": 0.6, "textiles": 0.3, "procedure_kits": 0.2, "wound_care": 0.6, "exam_gloves": 1.3, "general_supplies": 1.2},
    "home_health":               {"molded_disposables": 0.3, "textiles": 0.1, "procedure_kits": 0.1, "wound_care": 1.6, "exam_gloves": 1.2, "general_supplies": 1.0},
}
ASSORTMENT_SHARE = {"hospital": 0.60, "ambulatory_surgery_center": 0.30, "extended_care": 0.20,
                    "physician_office": 0.12, "home_health": 0.10}
CASES_PER_PALLET = {"molded_disposables": 48, "textiles": 40, "procedure_kits": 30, "wound_care": 60,
                    "exam_gloves": 50, "general_supplies": 40}
KIT_RATE = {"surgical_L": 120, "surgical_M": 180, "surgical_S": 300, "bedside": 300, "dressing": 500}

TARGET_UTILISATION = {"line": 0.70, "molding": 0.70, "sterilizer": 0.60}
STERILIZER_CYCLES_PER_DAY = 2.4   # ~9.5 h chamber time + turnaround
FAMILY_CAPABILITY = {"textiles": "textiles", "procedure_kits": "kit_assembly", "wound_care": "wound_care",
                     "molded_disposables": "molding"}


def customer_models(seed: int, products: pd.DataFrame, customers: pd.DataFrame):
    """Per-customer formulary and ordering parameters. Deterministic in seed."""
    rng = rng_for(seed, "tx", "formulary")
    n = len(products)
    fam = products["family"].to_numpy()
    dw = products["demand_weight"].to_numpy()
    sku_ids = products["sku_id"].to_numpy()
    models = {}
    for _, c in customers.iterrows():
        w = np.array([AFFINITY[c["customer_type"]][f] for f in fam]) * dw
        size = int(np.clip(round(ASSORTMENT_SHARE[c["customer_type"]] * n), 10, n))
        idx = np.sort(rng.choice(n, size=size, replace=False, p=w / w.sum()))
        p = w[idx] / w[idx].sum()
        models[c["customer_id"]] = dict(
            skus=sku_ids[idx], p=p,
            lam=float(np.clip(4 + 9 * c["volume_index"], 3, 0.6 * size)),
            mu=0.4 + 0.5 * c["volume_index"],
            disc=float(rng.uniform(0.82, 0.90) if pd.notna(c["idn_id"]) else rng.uniform(0.88, 0.96)),
            dc=c["dc_id"], route=c["route_id"], dpw=int(c["deliveries_per_week"]))
    return models


def inclusion_probabilities(p: np.ndarray, k: float) -> np.ndarray:
    """P(item i appears in an order of k lines drawn WITHOUT replacement, prob ~ p).

    Naive k*p is wrong here: once the popular items are on the order, the
    remaining draws shift to the tail, so tail items appear far more often
    than k*p says. Using k*p under-forecast demand by ~28% on the first run,
    which under-sized every plant and buy target. Rosén's successive-
    sampling approximation: pi_i = 1 - exp(-p_i * t), with t solved so that
    sum(pi) = k.
    """
    k = min(float(k), len(p) - 1e-9)
    lo, hi = 0.0, 1.0
    while (1 - np.exp(-p * hi)).sum() < k:
        hi *= 2
    for _ in range(60):
        mid = (lo + hi) / 2
        if (1 - np.exp(-p * mid)).sum() < k:
            lo = mid
        else:
            hi = mid
    return 1 - np.exp(-p * hi)


def expected_demand(models) -> tuple[dict, dict]:
    """Expected cases per CALENDAR day by (sku, dc) and by sku."""
    d_dc = defaultdict(float)
    for m in models.values():
        n = len(m["skus"])
        # E[k] for k = clip(Poisson(lam), 1, n): lam is large enough that the
        # clip barely matters, but stay exact at the low end.
        k = max(1.0, min(m["lam"], n))
        incl = inclusion_probabilities(m["p"], k) * (1 + m["mu"]) * m["dpw"] / 7.0
        for s, v in zip(m["skus"], incl):
            d_dc[(s, m["dc"])] += float(v)
    d_net = defaultdict(float)
    for (s, _), v in d_dc.items():
        d_net[s] += v
    return d_dc, d_net


def production_rate(p, mold=None) -> float:
    """Good eaches per running hour for one SKU on one line / machine."""
    fam, base = p["family"], p["base_name"]
    if fam == "molded_disposables":
        return float(mold["cavities"]) * 3600.0 / float(mold["nominal_cycle_s"]) * 0.95
    if fam == "procedure_kits":
        attrs = next(a for b, _, a in C.SKU_TEMPLATES["procedure_kits"] if b == base)
        return KIT_RATE[attrs["contents"]]
    if fam == "textiles":
        if "Cap" in base or "Shoe" in base:
            return 4000
        if "Patient" in base or "Isolation" in base:
            return 900
        if "Gown" in base:
            return 300
        if "Cover" in base:
            return 600
        return 250
    return 1500  # wound care converting


def required_capacity(products: pd.DataFrame, molds: pd.DataFrame, d_net: dict):
    """Running hours/day by (plant, capability) and sterile pallets/day by plant."""
    hours = defaultdict(float)
    pallets = defaultdict(float)
    mold_by_sku = molds.set_index("sku_id") if len(molds) else None
    for _, p in products[products["make_or_buy"] == "make"].iterrows():
        d = d_net.get(p["sku_id"], 0.0)
        if d <= 0:
            continue
        mold = mold_by_sku.loc[p["sku_id"]] if p["family"] == "molded_disposables" else None
        hours[(p["producing_plant_id"], FAMILY_CAPABILITY[p["family"]])] += \
            d * p["each_per_case"] / production_rate(p, mold)
        if p["sterile"]:
            pallets[p["sterilization_plant_id"]] += d / CASES_PER_PALLET[p["family"]]
    return hours, pallets


def lines_needed(hours_per_day: float) -> int:
    return max(2, math.ceil(hours_per_day / (24 * TARGET_UTILISATION["line"])))
