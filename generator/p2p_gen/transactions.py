"""
Step 2: transactions, lots and genealogy.
=========================================
A day-stepped discrete-event simulation of the whole plant-to-provider chain:

  component lots -> work orders (molding machines / assembly lines)
    -> EtO sterilisation loads (chamber-scheduled, aeration, BI hold, release)
    -> lots -> stock transfers -> DC inventory (lot level)
    -> customer orders -> FEFO allocation -> shipments (lot level)

Buy SKUs arrive from suppliers straight into DCs as supplier lots.

The simulation starts `prehistory_days` before the history window, so every
case on hand at day 1 was produced, sterilised and shipped like any other and
has full genealogy. Everything is cut at `live_start`: work still running,
loads still in the chamber, lots awaiting BI release, trucks in transit and
open orders are left exactly as they would be at that instant -- the state
the live demo starts from.

Design choice that matters for the hero scenario: a sterile finished lot is
one work order's product sterilised in ONE load (common EtO practice). A
load's blast radius is therefore an exact query, not an estimate.

All times are integer minutes from the simulation origin (UTC) until they
are written out.
"""
from __future__ import annotations

import heapq
import math
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from . import catalogs as C
from .config import P2PConfig
from .determinism import rng_for, stable_hash
from .demand import CASES_PER_PALLET, customer_models, expected_demand, production_rate
from .world import allocate, haversine_km

DAY = 1440
HOUR = 60

MAKE_CADENCE = {"A": 7, "B": 14, "C": 21}
KIT_CADENCE = {"A": 2, "B": 4, "C": 6}   # packs are built close to demand
BUY_CADENCE = {"A": 14, "B": 21, "C": 28}
COMPONENT_INTERVAL = {"resin": 10, "nonwoven": 14, "medical_film": 14, "packaging": 21,
                      "wound_materials": 21, "sterilant_gas": 7}
MAKE_LEAD_DAYS = 7
P_WO_DEVIATION = 0.03
P_LOAD_FAIL = 0.012


_WEEKDAYS = {"MON-FRI": (0, 1, 2, 3, 4), "MON-WED-FRI": (0, 2, 4), "TUE-THU": (1, 3)}


def _weekday_set(pattern: str) -> tuple[int, ...]:
    return _WEEKDAYS[pattern]


class Columns:
    """Append-only, column-oriented row store for the high-volume tables.

    A Python dict per row costs ~0.5-1 KB; baseline produces millions of
    rows, and the first baseline run was OOM-killed at 3.9 GB. Storing one
    list per column (with shared ID strings) cuts that by an order of
    magnitude and yields an identical DataFrame.
    """

    def __init__(self):
        self.cols: dict[str, list] = {}
        self.n = 0

    def append(self, rec: dict) -> None:
        if not self.cols:
            self.cols = {k: [] for k in rec}
        for k, v in rec.items():
            self.cols[k].append(v)
        self.n += 1

    def extend(self, recs) -> None:
        for r in recs:
            self.append(r)

    def __len__(self) -> int:
        return self.n

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.cols)


COLUMNAR = ("order_lines", "shipment_lines", "inventory_ledger", "inventory_snapshot_daily",
            "customer_formulary", "order_allocations", "lot_genealogy")


class Sim:
    """Holds the state of one simulation run."""

    def __init__(self, cfg: P2PConfig, w: dict[str, pd.DataFrame]):
        self.cfg, self.w = cfg, w
        self.origin = cfg.history_start - timedelta(days=cfg.prehistory_days)
        self.hs_day = cfg.prehistory_days
        self.he_day = cfg.prehistory_days + cfg.p.history_days
        self.he = self.he_day * DAY  # live start, in minutes
        self.rng = rng_for(cfg.seed, "tx", "sim")

        self.prod = w["products"].set_index("sku_id")
        self.plants = w["plants"].set_index("plant_id")
        self.dcs = w["distribution_centers"].set_index("dc_id")
        self.dc_ids = w["distribution_centers"]["dc_id"].tolist()
        self.molds = w["molds"].set_index("sku_id")
        self.bom = w["bom"]
        self.ster = w["sterilizers"]
        # Plain-dict indexes: pandas .loc row lookups were 72% of runtime.
        self.P = self.prod.to_dict("index")
        self.PL = self.plants.to_dict("index")
        self.DC = self.dcs.to_dict("index")
        self.MOLD = self.molds.to_dict("index")
        self.SUP = w["suppliers"].set_index("supplier_id").to_dict("index")
        self.ST = self.ster.set_index("sterilizer_id").to_dict("index")
        self.ROUTE = w["routes"].set_index("route_id").to_dict("index")
        self.STOP_OFF = dict(zip(w["route_stops"]["customer_id"], w["route_stops"]["planned_arrival_offset_min"]))
        self.STOP_SEQ = dict(zip(w["route_stops"]["customer_id"], w["route_stops"]["stop_seq"]))
        self._delay = {}
        self.BOM = defaultdict(list)
        for b in self.bom.to_dict("records"):
            self.BOM[b["parent_sku_id"]].append(b)
        self.MAKE = [s for s, p in self.P.items() if p["make_or_buy"] == "make"]
        self.BUY = [s for s, p in self.P.items() if p["make_or_buy"] == "buy"]
        self._res_cache = {}
        self.hero_ster = cfg.hero_sterilizer_id or self.ster.sort_values("sterilizer_id")["sterilizer_id"].iloc[0]

        # output rows
        self.rows = defaultdict(list)
        for name in COLUMNAR:
            self.rows[name] = Columns()
        self.seq = defaultdict(int)

        # state
        self.inv = {}                                # (dc, lot) -> [qty, avail_min]
        self.inv_by = defaultdict(list)              # (dc, sku) -> [lot]
        self.lot_meta = {}                           # lot -> dict
        self.lots_by_sku = defaultdict(list)         # sku -> [(avail_min, lot)]
        self.receipts = []                           # heap (avail_min, seq, dc, lot, qty, ref)
        self.pipe_make = defaultdict(float)          # sku -> cases not yet at a DC
        self.inbound = defaultdict(float)            # (sku, dc) -> cases in transit
        self.pipe_buy = defaultdict(float)           # (sku, dc) -> cases on open POs
        self.res_free = {}                           # resource -> free minute
        self.res_busy = defaultdict(float)           # resource -> busy minutes inside history
        self.ster_queue = defaultdict(list)          # plant -> [item]
        self.ster_free = {s: 0 for s in self.ster["sterilizer_id"]}
        self.comp_lots = defaultdict(list)           # (plant, component) -> [(recv_min, comp_lot)]
        self.nc_comp_lot = None
        self.devs = []

    # ------------------------------------------------------------------ utils
    def ts(self, m: float) -> datetime:
        return self.origin + timedelta(minutes=float(m))

    def next_id(self, prefix: str) -> int:
        self.seq[prefix] += 1
        return self.seq[prefix]

    def in_history(self, m: float) -> bool:
        return self.hs_day * DAY <= m < self.he

    # ------------------------------------------------------- demand model
    def build_formulary(self):
        self.cust_model = customer_models(self.cfg.seed, self.w["products"], self.w["customers"])
        for cid, m in self.cust_model.items():
            for s, pp in zip(m["skus"], m["p"]):
                self.rows["customer_formulary"].append({"customer_id": cid, "sku_id": s,
                                                        "share_of_lines": round(float(pp), 6),
                                                        "contract_discount": round(m["disc"], 4)})
        self.d_dc, self.d_net = expected_demand(self.cust_model)
        crng = rng_for(self.cfg.seed, "tx", "cover")
        self.cover = {}
        for s, fam in self.prod["family"].items():
            lo, hi = self.cfg.kit_cover_days if fam == "procedure_kits" else self.cfg.cover_days
            self.cover[s] = int(crng.integers(lo, hi + 1))

    # ------------------------------------------------------ component lots
    def build_component_lots(self):
        rng = rng_for(self.cfg.seed, "tx", "component_lots")
        comps = self.w["components"].set_index("component_id")
        used = defaultdict(list)
        make = self.w["products"][self.w["products"]["make_or_buy"] == "make"]
        bom_c = self.bom[self.bom["component_kind"] == "component"].merge(
            make[["sku_id", "producing_plant_id"]], left_on="parent_sku_id", right_on="sku_id")
        for plant, g in bom_c.groupby("producing_plant_id", sort=True):
            used[plant] = sorted(g["component_ref"].unique())
        for plant in sorted(self.ster["plant_id"].unique()):
            used[plant] = sorted(set(used[plant]) | {"STL-ETO"})
        # One nonconforming resin lot inside the history window (supplier deviation).
        nc_target = (self.hs_day + 8) * DAY
        best = None
        for plant in sorted(used):
            for comp in used[plant]:
                c = comps.loc[comp]
                every = COMPONENT_INTERVAL[c["category"]]
                t = -30 * DAY + int(stable_hash(plant, comp) % every) * DAY
                while t < self.he:
                    m = t + 8 * HOUR + int(rng.integers(0, 6 * HOUR))
                    sup = c["primary_supplier_id"] if (rng.random() < 0.8 or not c["alternate_supplier_id"]) else c["alternate_supplier_id"]
                    lot = f"CL-{comp}-{plant}-{self.ts(m):%y%j}"
                    self.comp_lots[(plant, comp)].append((m, lot))
                    self.rows["component_lots"].append({
                        "component_lot_id": lot, "component_id": comp, "supplier_id": sup, "plant_id": plant,
                        "received_ts": m, "coa_status": "accepted", "inspection_status": "released"})
                    if c["category"] == "resin" and plant == self.w["plants"]["plant_id"].iloc[0]:
                        if best is None or abs(m - nc_target) < abs(best[0] - nc_target):
                            best = (m, lot, len(self.rows["component_lots"]) - 1, sup)
                    t += every * DAY
        if best:
            self.nc_comp_lot = best[1]
            self.rows["component_lots"][best[2]]["coa_status"] = "nonconforming"
            self.rows["component_lots"][best[2]]["inspection_status"] = "released_after_mrb"
            self.nc_supplier = best[3]
            self.nc_dev = self.new_deviation("sup_resin_coa", best[0] + 3 * DAY, self.w["plants"]["plant_id"].iloc[0],
                                             "component_lot", best[1], rng, force_sev="major",
                                             extra={"supplier_id": best[3]})

    def comp_lot_at(self, plant, comp, m):
        lots = self.comp_lots.get((plant, comp))
        if not lots:
            return None
        best = None
        for rm, lot in lots:
            if rm <= m:
                best = lot
            else:
                break
        return best

    # ------------------------------------------------------------ deviations
    def new_deviation(self, key, opened, plant, asset_type, asset_id, rng, force_sev=None, extra=None,
                      hold_days=None):
        spec = next(d for d in C.DEVIATION_TYPES if d[0] == key)
        _, cat, _, sev_p, rc, title = spec
        sev = force_sev or str(rng.choice(["minor", "major", "critical"], p=sev_p))
        dev_id = f"DEV-{self.ts(opened):%Y}-{self.next_id('dev'):05d}"
        close = opened + int((hold_days or rng.uniform(5, 30)) * DAY)
        rec = {"deviation_id": dev_id, "opened_ts": opened, "type_key": key, "category": cat, "title": title,
               "severity": sev, "plant_id": plant, "asset_type": asset_type, "asset_id": asset_id,
               "root_cause_category": rc, "closed_ts": close if close <= self.he else None,
               "status": "closed" if close <= self.he else "open", "source": "simulation", "supplier_id": None}
        rec.update(extra or {})
        self.rows["deviations"].append(rec)
        if sev in ("major", "critical"):
            self.new_capa(dev_id, rc, opened, rng)
        return dev_id

    def new_capa(self, dev_id, rc, opened, rng):
        due = opened + int(rng.integers(30, 91)) * DAY
        closed = opened + int(rng.integers(20, 120)) * DAY
        self.rows["capas"].append({
            "capa_id": f"CAPA-{self.ts(opened):%Y}-{self.next_id('capa'):04d}", "deviation_id": dev_id,
            "action": str(rng.choice(C.CAPA_ACTIONS[rc])), "opened_ts": opened, "due_ts": due,
            "closed_ts": closed if closed <= self.he else None,
            "status": "closed" if closed <= self.he else ("overdue" if due < self.he else "open"),
            "effectiveness_verified": bool(closed <= self.he and rng.random() < 0.85)})

    def archive_deviations(self):
        """Closed deviations from the 18 months before the simulation: the
        precedent library an agent searches ("has this happened before?")."""
        rng = rng_for(self.cfg.seed, "tx", "archive")
        n = {"lite": 40, "baseline": 200}.get(self.cfg.profile, 40)
        assets = {
            "sterilizer": self.ster[["sterilizer_id", "plant_id"]].values.tolist(),
            "molding_machine": self.w["molding_machines"][["machine_id", "plant_id"]].values.tolist(),
            "line": self.w["production_lines"].query("line_type != 'molding'")[["line_id", "plant_id"]].values.tolist(),
            "dc_asset": self.w["dc_assets"].query("asset_type == 'crossbelt_sorter'")[["asset_id", "dc_id"]].values.tolist(),
            "vehicle": self.w["fleet_vehicles"].query("has_reefer")[["vehicle_id", "home_dc_id"]].values.tolist(),
            "component_lot": [[s, self.w["plants"]["plant_id"].iloc[0]] for s in
                              self.w["suppliers"].query("category == 'resin'")["supplier_id"]],
        }
        types = [d for d in C.DEVIATION_TYPES if assets.get(d[2])]
        start = -540 * DAY
        # Precedent for the hero: two earlier RH-probe events on the same chamber.
        hero_plant = self.ST[self.hero_ster]["plant_id"]
        for days_before in (300, 120):
            self.new_deviation("eto_rh_probe_drift", self.hs_day * DAY - days_before * DAY - 4 * DAY,
                               hero_plant, "sterilizer", self.hero_ster, rng, force_sev="major",
                               extra={"source": "archive"}, hold_days=21)
        for _ in range(n - 2):
            key, _, akind, *_ = types[int(rng.integers(len(types)))]
            aid, plant = assets[akind][int(rng.integers(len(assets[akind])))]
            opened = int(rng.uniform(start, -10 * DAY))
            extra = {"source": "archive"}
            if akind == "component_lot":  # archive supplier events cite the supplier, not a lot
                akind, extra["supplier_id"] = "supplier", aid
            self.new_deviation(key, opened, plant, akind, aid, rng, extra=extra)

    # ------------------------------------------------------- lots & stock
    def register_lot(self, lot_id, sku, cases, mfg, release, src, **kw):
        p = self.P[sku]
        shelf = p["shelf_life_months"]
        exp = None
        if pd.notna(shelf):
            exp = (self.ts(mfg) + timedelta(days=int(shelf) * 30)).date()
        status = kw.pop("status", None) or ("released" if release <= self.he else "quarantine")
        rec = {"lot_id": lot_id, "sku_id": sku, "source": src, "qty_cases": int(cases),
               "qty_each": int(cases) * int(p["each_per_case"]), "manufacture_ts": mfg, "release_ts": release,
               "expiry_date": exp, "status": status,
               "udi_pi": f"(01){p['udi_di']}" + (f"(17){exp:%y%m%d}" if exp else "") + f"(10){lot_id}"}
        rec.update(kw)
        self.rows["lots"].append(rec)
        self.lot_meta[lot_id] = {"sku": sku, "exp": exp, "release": release, "status": status}
        return rec

    def ship_to_dcs(self, lot_id, sku, cases, plant, release):
        """Split a released lot across DCs by need and create transfers."""
        need = []
        for dc in self.dc_ids:
            d = self.d_dc.get((sku, dc), 0.0)
            onh = sum(self.inv[(dc, l)][0] for l in self.inv_by[(dc, sku)])
            target = d * (self.cover[sku] + MAKE_LEAD_DAYS)
            need.append(max(0.0, target - onh - self.inbound[(sku, dc)]))
        if sum(need) <= 0:
            need = [self.d_dc.get((sku, dc), 0.0) for dc in self.dc_ids]
        if sum(need) <= 0:
            need = [1.0] * len(self.dc_ids)
        split = allocate(int(cases), need)
        p = self.PL[plant]
        for dc, q in zip(self.dc_ids, split):
            if q <= 0:
                continue
            ship = release + int(self.rng.integers(2 * HOUR, 10 * HOUR))
            if ship > self.he:
                continue  # still at the plant at live start
            d = self.DC[dc]
            transit = int((haversine_km(p["lat"], p["lon"], d["lat"], d["lon"]) * 1.2 / 60 + 4) * HOUR)
            recv = ship + transit
            tid = f"TR-{self.next_id('tr'):07d}"
            self.rows["stock_transfers"].append({
                "transfer_id": tid, "lot_id": lot_id, "sku_id": sku, "from_plant_id": plant, "to_dc_id": dc,
                "qty_cases": int(q), "ship_ts": ship, "receipt_ts": recv if recv <= self.he else None,
                "eta_ts": recv, "status": "received" if recv <= self.he else "in_transit"})
            self.inbound[(sku, dc)] += q
            heapq.heappush(self.receipts, (recv, self.next_id("rq"), dc, lot_id, int(q), tid, "transfer"))

    def receive_until(self, m):
        while self.receipts and self.receipts[0][0] <= m:
            avail, _, dc, lot, q, ref, kind = heapq.heappop(self.receipts)
            if avail > self.he:
                continue
            sku = self.lot_meta[lot]["sku"]
            key = (dc, lot)
            if key not in self.inv:
                self.inv[key] = [0, avail]
                self.inv_by[(dc, sku)].append(lot)
                self.inv_by[(dc, sku)].sort(key=lambda l: (str(self.lot_meta[l]["exp"] or "9999"), l))
            self.inv[key][0] += q
            if kind == "transfer":
                self.inbound[(sku, dc)] -= q
                self.pipe_make[sku] -= q
            else:
                self.pipe_buy[(sku, dc)] -= q
            self.rows["inventory_ledger"].append({"ts": avail, "dc_id": dc, "lot_id": lot, "sku_id": sku,
                                                  "qty_delta_cases": q, "txn_type": "receipt", "ref_id": ref})

    # --------------------------------------------------------- production
    def resource_for(self, sku):
        if sku in self._res_cache:
            return self._res_cache[sku]
        p = self.P[sku]
        plant = p["producing_plant_id"]
        if p["family"] == "molded_disposables":
            mold = self.MOLD[sku]
            m = self.w["molding_machines"]
            cands = m[(m["plant_id"] == plant) & (m["clamp_tonnage"] >= mold["min_clamp_tonnage"])]["machine_id"].tolist()
            out = (cands, production_rate(p, mold), f"MLD-{sku[4:]}")
        else:
            cap = {"textiles": "textiles", "procedure_kits": "kit_assembly", "wound_care": "wound_care"}[p["family"]]
            lines = self.w["production_lines"]
            cands = lines[(lines["plant_id"] == plant) & (lines["line_type"] == cap)]["line_id"].tolist()
            out = (cands, production_rate(p), None)
        self._res_cache[sku] = out
        return out

    def create_work_order(self, sku, cases, day, opening=False):
        p = self.P[sku]
        plant = p["producing_plant_id"]
        cands, rate, mold_id = self.resource_for(sku)
        res = min(cands, key=lambda r: (self.res_free.get(r, 0), r))
        start = max(self.res_free.get(res, 0), day * DAY + 6 * HOUR)
        each = int(cases) * int(p["each_per_case"])
        dur = int(each / rate * 60) + 90  # +90 min changeover
        end = start + dur
        self.res_free[res] = end
        lo, hi = max(start, self.hs_day * DAY), min(end, self.he)
        if hi > lo:
            self.res_busy[res] += hi - lo
        scrap = float(self.rng.uniform(0.005, 0.03))
        good_cases = max(1, int(cases * (1 - scrap)))
        wo = f"WO-{plant}-{self.next_id('wo'):06d}"
        status = "completed" if end <= self.he else ("in_progress" if start < self.he else "scheduled")
        self.rows["work_orders"].append({
            "work_order_id": wo, "sku_id": sku, "plant_id": plant, "resource_id": res, "mold_id": mold_id,
            "planned_qty_cases": int(cases), "good_qty_cases": good_cases if status == "completed" else None,
            "scrap_pct": round(scrap * 100, 2) if status == "completed" else None,
            "start_ts": start, "end_ts": end, "status": status, "is_opening_stock": opening})
        self.pipe_make[sku] += good_cases
        if status != "completed":
            return
        # genealogy: components and kit contents consumed
        for b in self.BOM[sku]:
            if b["component_kind"] == "component":
                cl = self.comp_lot_at(plant, b["component_ref"], start)
                if cl:
                    self.edge("component_lot", cl, "work_order", wo, "consumed_by", b["qty_per_each"] * each, start)
            else:
                cand = [l for a, l in self.lots_by_sku[b["component_ref"]] if a <= start]
                if cand:
                    self.edge("lot", cand[-1], "work_order", wo, "consumed_by", b["qty_per_each"] * each, start)
        # deviation (history only)
        hold_dev, reject = None, False
        if not opening and self.in_history(end):
            used = {self.comp_lot_at(plant, b["component_ref"], start)
                    for b in self.BOM[sku] if b["component_kind"] == "component"}
            if self.nc_comp_lot and self.nc_comp_lot in used:
                hold_dev = (self.nc_dev, 4)
            elif self.rng.random() < P_WO_DEVIATION:
                fam = p["family"]
                key = {"molded_disposables": str(self.rng.choice(["mold_short_shot", "mold_flash", "mold_black_specks", "mold_dimensional_oos"])),
                       "textiles": "txt_seam_defect", "procedure_kits": str(self.rng.choice(["kit_missing_component", "pkg_label_mixup"])),
                       "wound_care": "pkg_seal_integrity"}[fam]
                days = float(self.rng.uniform(2, 6))
                dev = self.new_deviation(key, end + 2 * HOUR, plant,
                                         "molding_machine" if fam == "molded_disposables" else "line", res, self.rng,
                                         hold_days=days)
                hold_dev, reject = (dev, days), bool(self.rng.random() < 0.10)
        wo_item = {"wo": wo, "sku": sku, "cases": good_cases, "plant": plant, "end": end,
                   "hold": hold_dev, "reject": reject, "frag": 0}
        if p["sterile"]:
            sp = p["sterilization_plant_id"]
            ready = end + (DAY if sp != plant else 30)
            self.ster_queue[sp].append(dict(wo_item, ready=ready, remaining=good_cases, sp=sp))
        else:
            release = end + int(self.rng.integers(8 * HOUR, 24 * HOUR))
            self.finish_lot(wo_item, f"L{self.ts(end):%y%j}-{plant}-{wo[-6:]}", good_cases, release, None)

    def finish_lot(self, item, lot_id, cases, release, load_id):
        hold, reject = item["hold"], item["reject"]
        status = None
        if hold:
            release += int(hold[1] * DAY)
            self.rows["deviation_lots"].append({"deviation_id": hold[0], "lot_id": lot_id,
                                                "work_order_id": item["wo"], "disposition": "rejected" if reject else "released"})
            if release > self.he:
                status = "on_hold"
        if reject:
            status = "rejected"
        self.register_lot(lot_id, item["sku"], cases, item["end"], release, "make", work_order_id=item["wo"],
                          sterilization_load_id=load_id, status=status)
        self.edge("work_order", item["wo"], "lot", lot_id, "produced", cases, item["end"])
        if load_id:
            self.edge("sterilization_load", load_id, "lot", lot_id, "sterilized", cases, release)
        if reject:
            self.pipe_make[item["sku"]] -= cases
            return
        self.lots_by_sku[item["sku"]].append((release, lot_id))
        if release <= self.he:
            self.ship_to_dcs(lot_id, item["sku"], cases, item["plant"], release)

    def edge(self, ft, fid, tt, tid, rel, qty, m):
        self.rows["lot_genealogy"].append({"from_type": ft, "from_id": fid, "to_type": tt, "to_id": tid,
                                           "relation": rel, "qty": round(float(qty), 4), "ts": m})

    # ------------------------------------------------------- sterilisation
    def run_sterilizers(self, horizon):
        for plant in sorted(self.ster_queue):
            q = self.ster_queue[plant]
            chambers = sorted(c for c, v in self.ST.items() if v["plant_id"] == plant)
            while q:
                ch = min(chambers, key=lambda c: (self.ster_free[c], c))
                earliest = min(i["ready"] for i in q)
                start = max(self.ster_free[ch], earliest)
                if start >= horizon:
                    break
                cap = int(self.ST[ch]["pallet_capacity"])
                window = [i for i in sorted(q, key=lambda i: (i["ready"], i["wo"])) if i["ready"] <= start + 4 * HOUR]
                load, pallets = [], 0
                for it in window:
                    if pallets >= cap:
                        break
                    cpp = CASES_PER_PALLET[self.P[it["sku"]]["family"]]
                    need_p = math.ceil(it["remaining"] / cpp)
                    take_p = min(need_p, cap - pallets)
                    take_c = min(it["remaining"], take_p * cpp)
                    load.append((it, take_c, take_p))
                    pallets += take_p
                start = max(start, max(it["ready"] for it, _, _ in load))
                if start >= horizon:
                    break
                exposure = int(self.ST[ch]["exposure_minutes"])
                precond_end = start + 120
                exp_end = precond_end + exposure
                cycle_end = exp_end + 150
                self.ster_free[ch] = cycle_end + 30
                lo, hi = max(start, self.hs_day * DAY), min(cycle_end, self.he)
                if hi > lo:
                    self.res_busy[ch] += hi - lo
                in_hist = self.in_history(cycle_end)
                failed = in_hist and self.rng.random() < P_LOAD_FAIL
                load_id = f"SL-{ch[4:]}-{self.ts(start):%y%j}-{self.next_id('sl' + ch):03d}"
                done = cycle_end <= self.he
                result = ("fail" if failed else "pass") if done else None
                aer_end = cycle_end + int(self.ST[ch]["aeration_hours"]) * HOUR
                rel = max(aer_end, cycle_end + 48 * HOUR) + int(self.rng.integers(2 * HOUR, 8 * HOUR))
                self.rows["sterilization_loads"].append({
                    "load_id": load_id, "sterilizer_id": ch, "plant_id": plant, "start_ts": start,
                    "precond_end_ts": precond_end, "exposure_end_ts": exp_end, "cycle_end_ts": cycle_end,
                    "aeration_end_ts": aer_end if result == "pass" else None,
                    "bi_result": ("negative" if rel <= self.he else "incubating") if result == "pass" else None,
                    "cycle_result": result, "status": ("completed" if done else "in_cycle"),
                    "pallets": pallets, "pallet_capacity": cap, "work_orders": len(load),
                    "release_ts": rel if (result == "pass" and rel <= self.he) else None})
                eto = self.comp_lot_at(plant, "STL-ETO", start)
                if eto:
                    self.edge("component_lot", eto, "sterilization_load", load_id, "consumed_by", 0.0, start)
                if failed:
                    self.new_deviation(str(self.rng.choice(["eto_low_gas_conc", "eto_vacuum_leak", "eto_temp_excursion"])),
                                       cycle_end, plant, "sterilizer", ch, self.rng, hold_days=3)
                for it, take_c, take_p in load:
                    self.rows["load_contents"].append({"load_id": load_id, "work_order_id": it["wo"],
                                                       "sku_id": it["sku"], "cases": int(take_c), "pallets": take_p,
                                                       "lot_id": None, "cycle_result": result})
                    if failed:
                        it["ready"] = cycle_end + 2 * HOUR  # re-sterilise the same product
                        continue
                    it["remaining"] -= take_c
                    if not done:
                        continue  # product is in the chamber at live start
                    it["frag"] += 1
                    lot = f"L{self.ts(it['end']):%y%j}-{it['plant']}-{it['wo'][-6:]}-S{it['frag']}"
                    self.rows["load_contents"][-1]["lot_id"] = lot
                    self.finish_lot(it, lot, take_c, rel, load_id)
                q[:] = [i for i in q if i["remaining"] > 0]

    # --------------------------------------------------------------- buying
    def create_po(self, sku, dc, cases, order_m, opening=False):
        p = self.P[sku]
        sup = self.SUP[p["primary_supplier_id"]]
        lead = int(sup["lead_time_days"])
        # receipt includes incoming inspection, so received == available
        recv = order_m + int((lead + 1 + self.rng.normal(0, max(1.0, lead * 0.08))) * DAY)
        po = f"PO-{self.next_id('po'):07d}"
        lot = f"V{p['primary_supplier_id'][-4:]}-{self.ts(recv):%y%j}-{self.next_id('vlot'):05d}"
        status = "received" if recv <= self.he else "open"
        self.rows["purchase_orders"].append({
            "po_id": po, "supplier_id": p["primary_supplier_id"], "sku_id": sku, "dc_id": dc,
            "qty_cases": int(cases), "order_ts": order_m, "promised_ts": order_m + lead * DAY,
            "receipt_ts": recv if status == "received" else None, "status": status,
            "lot_id": lot if status == "received" else None, "is_opening_stock": opening})
        self.pipe_buy[(sku, dc)] += cases
        if status == "received":
            avail = recv
            self.register_lot(lot, sku, cases, recv - int(self.rng.integers(20, 90)) * DAY, avail, "buy",
                              purchase_order_id=po, supplier_id=p["primary_supplier_id"])
            self.lots_by_sku[sku].append((avail, lot))
            heapq.heappush(self.receipts, (avail, self.next_id("rq"), dc, lot, int(cases), po, "po"))

    # ------------------------------------------------------------- planning
    def opening_stock(self):
        """Lots produced/bought before the window so day-1 stock has genealogy."""
        for sku, p in self.P.items():
            cad = self.cadence(p)
            k_n = math.ceil(self.cover[sku] / cad)
            if p["make_or_buy"] == "make":
                d = self.d_net.get(sku, 0.0)
                if d <= 0:
                    continue
                for k in range(k_n):
                    day = self.hs_day - MAKE_LEAD_DAYS - k * cad - int(stable_hash(sku, k) % 3)
                    self.opening.setdefault(day, []).append((sku, max(1, math.ceil(cad * d))))
            else:
                for dc in self.dc_ids:
                    d = self.d_dc.get((sku, dc), 0.0)
                    if d <= 0:
                        continue
                    lead = int(self.SUP[p["primary_supplier_id"]]["lead_time_days"])
                    for k in range(k_n):
                        recv_day = self.hs_day - 2 - k * cad
                        self.opening_po.setdefault(recv_day - lead, []).append((sku, dc, max(1, math.ceil(cad * d))))

    @staticmethod
    def cadence(p) -> int:
        if p["make_or_buy"] == "buy":
            return BUY_CADENCE[p["abc_class"]]
        return (KIT_CADENCE if p["family"] == "procedure_kits" else MAKE_CADENCE)[p["abc_class"]]

    def plan(self, day):
        for sku in self.MAKE:
            p = self.P[sku]
            d = self.d_net.get(sku, 0.0)
            cad = self.cadence(p)
            if d <= 0 or (day + stable_hash("cad", sku)) % cad:
                continue
            onh = sum(self.inv[(dc, l)][0] for dc in self.dc_ids for l in self.inv_by[(dc, sku)])
            pos = onh + self.pipe_make[sku]
            target = d * (self.cover[sku] + MAKE_LEAD_DAYS)
            if pos < target:
                self.create_work_order(sku, max(1, math.ceil(target + cad * d - pos)), day)
        for sku in self.BUY:
            p = self.P[sku]
            cad = BUY_CADENCE[p["abc_class"]]
            if (day + stable_hash("cad", sku)) % cad:
                continue
            lead = int(self.SUP[p["primary_supplier_id"]]["lead_time_days"])
            for dc in self.dc_ids:
                d = self.d_dc.get((sku, dc), 0.0)
                if d <= 0:
                    continue
                onh = sum(self.inv[(dc, l)][0] for l in self.inv_by[(dc, sku)])
                pos = onh + self.pipe_buy[(sku, dc)]
                target = d * (self.cover[sku] + lead)
                if pos < target:
                    self.create_po(sku, dc, max(1, math.ceil(target + cad * d - pos)), day * DAY + 9 * HOUR)

    # --------------------------------------------------------------- demand
    def orders_for_day(self, day, rng, open_orders=False):
        wd = self.ts(day * DAY).weekday()
        if wd >= 5:
            return
        for cid in sorted(self.cust_model):
            m = self.cust_model[cid]
            r = self.ROUTE[m["route"]]
            if wd not in _weekday_set(r["service_days"]):
                continue
            order_m = day * DAY - int(rng.integers(1, 6)) * DAY + int(rng.integers(8 * HOUR, 18 * HOUR))
            k = int(np.clip(rng.poisson(m["lam"]), 1, len(m["skus"])))
            pick = rng.choice(len(m["skus"]), size=k, replace=False, p=m["p"])
            qtys = 1 + rng.poisson(m["mu"], size=k)
            if open_orders and order_m > self.he:
                continue
            oid = f"SO-{self.next_id('so'):08d}"
            eta = day * DAY + int(r["planned_departure_min"]) + int(self.STOP_OFF[cid])
            lines, shipped_any = [], False
            for j, (i, q) in enumerate(sorted(zip(pick, qtys))):
                sku = m["skus"][i]
                p = self.P[sku]
                olid = f"{oid}-{j + 1:03d}"
                price = round(float(p["list_price_usd"]) * int(p["each_per_case"]) * m["disc"], 2)
                if open_orders:
                    got = self.allocate_soft(m["dc"], sku, int(q), olid, day)
                    status = "allocated" if got == q else ("partially_allocated" if got else "unallocated")
                    lines.append({"order_line_id": olid, "order_id": oid, "sku_id": sku, "qty_ordered_cases": int(q),
                                  "qty_allocated_cases": got, "qty_shipped_cases": 0, "qty_backordered_cases": 0,
                                  "unit_price_usd": price, "status": status})
                else:
                    got = self.allocate_ship(m["dc"], sku, int(q), olid, oid, day * DAY + 2 * HOUR)
                    shipped_any |= got > 0
                    lines.append({"order_line_id": olid, "order_id": oid, "sku_id": sku, "qty_ordered_cases": int(q),
                                  "qty_allocated_cases": got, "qty_shipped_cases": got,
                                  "qty_backordered_cases": int(q) - got, "unit_price_usd": price,
                                  "status": "shipped" if got == q else ("short_shipped" if got else "backordered")})
            self.rows["order_lines"].extend(lines)
            if open_orders:
                status = "open"
            else:
                status = "delivered" if all(l["status"] == "shipped" for l in lines) else "delivered_partial"
            self.rows["customer_orders"].append({
                "order_id": oid, "customer_id": cid, "dc_id": m["dc"], "route_id": m["route"],
                "order_ts": order_m, "requested_delivery_date": self.ts(day * DAY).date(),
                "lines": len(lines), "status": status})
            if not open_orders and shipped_any:
                self.rows["shipments"].append({
                    "shipment_id": f"SH-{oid[3:]}", "order_id": oid, "route_id": m["route"],
                    "vehicle_id": r["vehicle_id"], "ship_date": self.ts(day * DAY).date(),
                    "planned_arrival_ts": eta, "delivered_ts": eta + self.route_delay(m["route"], day, self.STOP_SEQ[cid]),
                    "status": "delivered"})

    def route_delay(self, route, day, seq) -> int:
        """Minutes late at stop `seq`: a per-route-day random walk (traffic and
        delays accumulate along a route), so recorded deliveries are drivable in
        order. Independent noise per stop produced out-of-order deliveries."""
        key = (route, day)
        if key not in self._delay:
            r = rng_for(self.cfg.seed, "tx", "delay", route, day)
            n = int(self.ROUTE[route]["stops"])
            self._delay[key] = np.round(r.normal(0, 8) + np.cumsum(r.normal(1.0, 4.0, n))).astype(int)
        return int(self._delay[key][seq - 1])

    def _fefo(self, dc, sku, at):
        for lot in self.inv_by[(dc, sku)]:
            qty, avail = self.inv[(dc, lot)]
            meta = self.lot_meta[lot]
            if qty <= 0 or avail > at or meta["status"] == "rejected":
                continue
            if meta["exp"] and meta["exp"] <= (self.ts(at) + timedelta(days=30)).date():
                continue
            yield lot

    def allocate_ship(self, dc, sku, q, olid, oid, at):
        got = 0
        for lot in self._fefo(dc, sku, at):
            take = min(q - got, self.inv[(dc, lot)][0])
            self.inv[(dc, lot)][0] -= take
            got += take
            self.rows["shipment_lines"].append({"shipment_id": f"SH-{oid[3:]}", "order_line_id": olid,
                                                "lot_id": lot, "qty_cases": take})
            self.rows["inventory_ledger"].append({"ts": at, "dc_id": dc, "lot_id": lot, "sku_id": sku,
                                                  "qty_delta_cases": -take, "txn_type": "ship", "ref_id": olid})
            if got >= q:
                break
        return got

    def allocate_soft(self, dc, sku, q, olid, day):
        got = 0
        for lot in self._fefo(dc, sku, self.he):
            free = self.inv[(dc, lot)][0] - self.soft[(dc, lot)]
            if free <= 0:
                continue
            take = min(q - got, free)
            self.soft[(dc, lot)] += take
            got += take
            self.rows["order_allocations"].append({"order_line_id": olid, "dc_id": dc, "lot_id": lot,
                                                   "qty_cases": take, "requested_delivery_date": self.ts(day * DAY).date()})
            if got >= q:
                break
        return got

    # ------------------------------------------------------------------ run
    def run(self):
        self.build_formulary()
        self.build_component_lots()
        self.archive_deviations()
        self.opening, self.opening_po = {}, {}
        self.opening_stock()
        drng = rng_for(self.cfg.seed, "tx", "demand")
        min_po_day = min(self.opening_po) if self.opening_po else 0
        for day in range(min(0, min_po_day), self.he_day):
            for sku, dc, q in self.opening_po.get(day, []):
                self.create_po(sku, dc, q, day * DAY + 9 * HOUR, opening=True)
            if day < 0:
                continue
            self.receive_until(day * DAY + 2 * HOUR)
            if day >= self.hs_day:
                self.orders_for_day(day, drng)
            for sku, q in self.opening.get(day, []):
                self.create_work_order(sku, q, day, opening=True)
            if day >= self.hs_day:
                self.plan(day)
            self.run_sterilizers((day + 1) * DAY)
            self.receive_until((day + 1) * DAY)
            if day >= self.hs_day:
                self.snapshot(day)
        # Open orders for the next N business days, soft-allocated at live start.
        self.soft = defaultdict(int)
        day, n = self.he_day, 0
        while n < self.cfg.open_order_business_days:
            if self.ts(day * DAY).weekday() < 5:
                self.orders_for_day(day, drng, open_orders=True)
                n += 1
            day += 1
        self.on_hand()
        return self

    def snapshot(self, day):
        d = self.ts(day * DAY).date()
        for (dc, lot), (q, _) in self.inv.items():
            if q > 0:
                self.rows["inventory_snapshot_daily"].append({"snapshot_date": d, "dc_id": dc, "lot_id": lot,
                                                              "sku_id": self.lot_meta[lot]["sku"], "qty_cases": q})

    def on_hand(self):
        for (dc, lot), (q, avail) in sorted(self.inv.items()):
            if q <= 0:
                continue
            meta = self.lot_meta[lot]
            alloc = self.soft[(dc, lot)]
            self.rows["inventory_on_hand"].append({
                "dc_id": dc, "lot_id": lot, "sku_id": meta["sku"], "qty_cases": q, "qty_allocated_cases": alloc,
                "qty_available_cases": q - alloc, "lot_status": meta["status"], "expiry_date": meta["exp"],
                "as_of_ts": self.he})


# ---------------------------------------------------------------------------
TS_COLS = {"received_ts", "opened_ts", "closed_ts", "due_ts", "manufacture_ts", "release_ts", "start_ts",
           "end_ts", "precond_end_ts", "exposure_end_ts", "cycle_end_ts", "aeration_end_ts", "ship_ts",
           "receipt_ts", "eta_ts", "order_ts", "promised_ts", "planned_arrival_ts", "delivered_ts", "ts",
           "as_of_ts"}

ID_COLS = {"dc_id", "lot_id", "sku_id", "customer_id", "txn_type", "from_type", "to_type", "relation",
           "status", "requested_delivery_date", "snapshot_date"}

TABLE_ORDER = ["customer_formulary", "component_lots", "work_orders", "sterilization_loads", "load_contents",
               "lots", "lot_genealogy", "stock_transfers", "purchase_orders", "customer_orders", "order_lines",
               "shipments", "shipment_lines", "order_allocations", "inventory_ledger", "inventory_snapshot_daily",
               "inventory_on_hand", "deviations", "deviation_lots", "capas"]


def build_transactions(cfg: P2PConfig, world: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], Sim]:
    sim = Sim(cfg, world).run()
    out = {}
    origin = pd.to_datetime(sim.origin, utc=True)
    for name in TABLE_ORDER:
        src = sim.rows.pop(name, [])          # release raw rows as we go (memory)
        df = src.frame() if isinstance(src, Columns) else pd.DataFrame(src)
        del src
        for col in df.columns:
            if col in TS_COLS:
                df[col] = origin + pd.to_timedelta(df[col].astype("float64"), unit="min")
            elif name in COLUMNAR and col in ID_COLS:
                # Highly repetitive IDs: dictionary-encode. Parquet stores them
                # dictionary-encoded anyway; Spark / Fabric read them as strings.
                df[col] = df[col].astype("category")
        out[name] = df
    # Stable row order regardless of event interleaving.
    keys = {"lots": ["lot_id"], "work_orders": ["work_order_id"], "deviations": ["deviation_id"],
            "capas": ["capa_id"], "sterilization_loads": ["load_id"], "inventory_ledger": ["ts", "dc_id", "lot_id", "ref_id"]}
    for name, k in keys.items():
        if len(out[name]):
            out[name] = out[name].sort_values(k, kind="mergesort").reset_index(drop=True)
    return out, sim
