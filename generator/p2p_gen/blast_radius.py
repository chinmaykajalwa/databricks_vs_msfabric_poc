"""
Hero scenario ground truth: the blast radius of a drifting RH probe.
====================================================================
Story: during the live demo a humidity excursion on the hero steriliser
triggers an investigation, which finds its RH probe has been drifting for
`hero_drift_days`. Every load in that window is suspect. The question the
agent has to answer:

    which lots, where are they now, and which open customer orders
    (and which customers / health systems) depend on them?

This module answers it with plain joins over the transaction tables -- the
same joins the agent must perform on each platform. The result is written to
output/ground_truth/ (NOT loaded into either platform), so each platform's
answer can be scored against it: precision, recall, time to answer.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .config import P2PConfig


def hero_blast_radius(cfg: P2PConfig, world: dict, tx: dict, hero_id: str) -> dict:
    live = pd.Timestamp(cfg.live_start)
    drift_from = live - pd.Timedelta(days=cfg.hero_drift_days)

    loads = tx["sterilization_loads"]
    sus_loads = loads[(loads["sterilizer_id"] == hero_id) & (loads["start_ts"] >= drift_from)]
    lc = tx["load_contents"]
    sus_lc = lc[lc["load_id"].isin(sus_loads["load_id"])]
    lots = tx["lots"].set_index("lot_id")
    sus_lot_ids = sorted(sus_lc["lot_id"].dropna().unique())

    onh = tx["inventory_on_hand"]
    at_dc = onh[onh["lot_id"].isin(sus_lot_ids)]
    tr = tx["stock_transfers"]
    in_transit = tr[tr["lot_id"].isin(sus_lot_ids) & (tr["status"] == "in_transit")]
    shipped = tx["shipment_lines"][tx["shipment_lines"]["lot_id"].isin(sus_lot_ids)]

    alloc = tx["order_allocations"]
    sus_alloc = alloc[alloc["lot_id"].isin(sus_lot_ids)]
    ol = tx["order_lines"].set_index("order_line_id")
    orders = tx["customer_orders"].set_index("order_id")
    sus_orders = sorted(ol.loc[sus_alloc["order_line_id"].unique(), "order_id"].unique()) if len(sus_alloc) else []
    custs = world["customers"].set_index("customer_id")
    sus_cust = sorted(orders.loc[sus_orders, "customer_id"].unique()) if sus_orders else []
    delivered_orders = sorted(tx["shipments"].set_index("shipment_id").loc[shipped["shipment_id"].unique(), "order_id"].unique()) if len(shipped) else []
    delivered_cust = sorted(orders.loc[delivered_orders, "customer_id"].unique()) if delivered_orders else []

    prod = world["products"].set_index("sku_id")
    unit = {s: float(prod.loc[s, "unit_cost_usd"]) * int(prod.loc[s, "each_per_case"]) for s in prod.index}

    def value(df, qty_col="qty_cases"):
        return round(float(sum(unit[s] * q for s, q in zip(df["sku_id"], df[qty_col]))), 2) if len(df) else 0.0

    at_dc_value = value(at_dc)
    return {
        "scenario": "hero_rh_probe_drift",
        "sterilizer_id": hero_id,
        "drift_window_start_utc": drift_from.isoformat(),
        "live_start_utc": live.isoformat(),
        "suspect_loads": sorted(sus_loads["load_id"].tolist()),
        "loads_in_chamber_at_live_start": sorted(sus_loads[sus_loads["status"] == "in_cycle"]["load_id"].tolist()),
        "suspect_lots": sus_lot_ids,
        "suspect_skus": sorted(lots.loc[sus_lot_ids, "sku_id"].unique().tolist()) if sus_lot_ids else [],
        "lots_by_status": lots.loc[sus_lot_ids, "status"].value_counts().to_dict() if sus_lot_ids else {},
        "on_hand_at_dcs": {
            "cases": int(at_dc["qty_cases"].sum()),
            "cases_allocated_to_open_orders": int(at_dc["qty_allocated_cases"].sum()),
            "inventory_value_usd": at_dc_value,
            "by_dc": at_dc.groupby("dc_id")["qty_cases"].sum().astype(int).to_dict(),
        },
        "in_transit": {"transfers": len(in_transit), "cases": int(in_transit["qty_cases"].sum()),
                       "to_dcs": sorted(in_transit["to_dc_id"].unique().tolist())},
        "already_delivered": {"cases": int(shipped["qty_cases"].sum()), "orders": len(delivered_orders),
                              "customers": delivered_cust},
        "open_orders_at_risk": {
            "orders": sus_orders, "order_lines": int(sus_alloc["order_line_id"].nunique()),
            "cases": int(sus_alloc["qty_cases"].sum()),
            "customers": sus_cust,
            "hospitals": [c for c in sus_cust if custs.loc[c, "customer_type"] == "hospital"],
            "idns": sorted(custs.loc[sus_cust, "idn_id"].dropna().unique().tolist()) if sus_cust else [],
            "by_requested_date": {str(k): int(v) for k, v in
                                  sus_alloc.groupby("requested_delivery_date")["qty_cases"].sum().items()},
        },
    }


def write_ground_truth(cfg: P2PConfig, br: dict) -> str:
    out = Path(cfg.out_root) / "ground_truth"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "hero_blast_radius.json"
    path.write_text(json.dumps(br, indent=2, default=str))
    return str(path)


def summary_text(br: dict) -> str:
    o, a, t, d = br["on_hand_at_dcs"], br["open_orders_at_risk"], br["in_transit"], br["already_delivered"]
    return "\n".join([
        f"  Hero steriliser {br['sterilizer_id']}, drift window from {br['drift_window_start_utc'][:16]}Z",
        f"  {len(br['suspect_loads'])} suspect loads ({len(br['loads_in_chamber_at_live_start'])} in chamber at live start)"
        f" -> {len(br['suspect_lots'])} lots across {len(br['suspect_skus'])} SKUs; lot status {br['lots_by_status']}",
        f"  On hand at DCs: {o['cases']:,} cases (${o['inventory_value_usd']:,.0f}) {o['by_dc']}",
        f"  In transit: {t['cases']:,} cases on {t['transfers']} transfers to {t['to_dcs']}",
        f"  Already delivered: {d['cases']:,} cases on {d['orders']} orders to {len(d['customers'])} customers",
        f"  OPEN ORDERS AT RISK: {len(a['orders'])} orders / {a['order_lines']} lines / {a['cases']:,} cases, "
        f"{len(a['customers'])} customers ({len(a['hospitals'])} hospitals) in {len(a['idns'])} IDNs",
    ])
