"""
Step 3: telemetry physics.
==========================
One engine produces every sensor value, for any time window: the 30-day
history files now, and the live Event Hubs stream in step 5. History and
live are therefore continuous by construction.

Signals are driven by what actually happened in step 2:
  molding      -> the work order (and so the mold, setpoints, cycle time) running
                  on that machine at that moment; changeovers; idle standby
  sterilisers  -> each load's phase timestamps: vacuum, steam conditioning,
                  gas exposure, nitrogen washes, venting
  DC assets    -> night-picking / receiving shifts scaled by that DC's actual
                  shipments that day
  vehicles     -> each route day's real delivered_ts per stop; reefer
                  temperature with door-open bumps

Generation is chunked in 6-hour blocks with an RNG keyed by (asset, block),
and smooth components are functions of absolute time, so any window can be
regenerated identically -- in a batch job or by the live publisher.

Values are physically plausible, not calibrated to any real equipment.
"""
from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
import pandas as pd

from .config import P2PConfig
from .determinism import rng_for, stable_hash
from .world import haversine_km

SEC, MIN, HOUR, DAYMS = 1000, 60_000, 3_600_000, 86_400_000
CHUNK = 6 * HOUR
EVENT = 0
GOOD, UNCERTAIN, BAD = 0, 1, 2
QUALITY_NAMES = np.array(["GOOD", "UNCERTAIN", "BAD"])

TOPIC = {"molding_machine": "molding-telemetry", "sterilizer": "sterilizer-cycles",
         "dc_asset": "dc-asset-telemetry", "vehicle": "fleet-telemetry"}
AGG_MS = {"molding-telemetry": MIN, "sterilizer-cycles": MIN, "dc-asset-telemetry": 5 * MIN,
          "fleet-telemetry": MIN}

# signal, unit, interval_ms (0 = per event), min_valid, max_valid, max_rate_of_change/sample, criticality
SIGNALS = {
    "molding_machine": [
        ("melt_temp_c", "degC", 5000, 150, 300, 15, "critical"),
        ("mold_temp_c", "degC", 5000, 5, 95, 8, "important"),
        ("hyd_oil_temp_c", "degC", 10000, 15, 70, 4, "standard"),
        ("motor_power_kw", "kW", 5000, 0, 120, 80, "standard"),
        ("machine_state", "code", 60000, 0, 2, math.inf, "standard"),
        ("cycle_time_s", "s", EVENT, 3, 90, math.inf, "important"),
        ("inj_pressure_peak_bar", "bar", EVENT, 300, 2000, 300, "critical"),
        ("cavity_pressure_peak_bar", "bar", EVENT, 100, 1300, 300, "critical"),
        ("shot_count", "count", EVENT, 0, 1e8, math.inf, "standard"),
    ],
    "sterilizer": [
        ("chamber_pressure_kpa", "kPa", 2000, 0, 110, 25, "critical"),
        ("chamber_temp_c", "degC", 2000, 10, 70, 2, "critical"),
        ("chamber_rh_pct", "%RH", 2000, 0, 100, 10, "critical"),
        ("eto_conc_mg_l", "mg/L", 10000, 0, 900, 200, "critical"),
        ("steam_flow_kg_h", "kg/h", 5000, 0, 400, 300, "important"),
        ("jacket_temp_c", "degC", 10000, 10, 70, 4, "standard"),
        ("cycle_phase", "code", 60000, 0, 3, math.inf, "standard"),
    ],
    "vehicle": [
        ("gps_lat", "deg", 30000, 24, 50, 0.05, "standard"),
        ("gps_lon", "deg", 30000, -125, -66, 0.05, "standard"),
        ("speed_kmh", "km/h", 30000, 0, 130, 60, "standard"),
        ("engine_on", "bool", 60000, 0, 1, math.inf, "standard"),
    ],
}
REEFER_SIGNAL = ("reefer_temp_c", "degC", 60000, -5, 40, 4, "critical")
_MOTOR = ("motor_current_a", "A", 5000, 0, 250, 60, "standard")
_CYC = ("cycles_per_min", "1/min", 60000, 0, 200, math.inf, "standard")
DC_SIGNALS = {
    "conveyor_segment": [_MOTOR, ("vibration_mm_s", "mm/s", 10000, 0, 30, 5, "important"),
                         ("bearing_temp_c", "degC", 60000, 0, 120, 5, "standard"),
                         ("belt_speed_mps", "m/s", 10000, 0, 4, 3, "standard")],
    "crossbelt_sorter": [_MOTOR, ("vibration_mm_s", "mm/s", 10000, 0, 30, 5, "critical"),
                         ("bearing_temp_c", "degC", 60000, 0, 120, 5, "important"),
                         ("belt_speed_mps", "m/s", 10000, 0, 4, 3, "standard"),
                         ("sort_rate_per_min", "1/min", 60000, 0, 400, math.inf, "important"),
                         ("missort_pct", "%", 60000, 0, 10, math.inf, "important")],
    "induction_station": [_MOTOR, ("induct_rate_per_min", "1/min", 60000, 0, 80, math.inf, "standard")],
    "amr": [("battery_pct", "%", 30000, 0, 100, 5, "standard"), ("amr_state", "code", 60000, 0, 2, math.inf, "standard")],
    "shuttle_asrs": [_MOTOR, _CYC],
    "palletizer": [_MOTOR, _CYC],
    "stretch_wrapper": [_MOTOR, _CYC],
}
ISO_VIBRATION_ALARM = 7.1  # mm/s RMS, ISO 10816 zone C/D boundary for this machine class


def to_ms(s: pd.Series) -> np.ndarray:
    return ((s - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(milliseconds=1)).to_numpy(dtype="float64")


def ts_ms(t) -> int:
    return int((pd.Timestamp(t) - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(milliseconds=1))


def grid(t0: int, t1: int, interval: int) -> np.ndarray:
    start = -(-t0 // interval) * interval
    return np.arange(start, t1, interval, dtype=np.int64)


def wander(t, key, amp, period_min) -> np.ndarray:
    """Smooth, deterministic drift as a function of ABSOLUTE time (chunk-independent)."""
    ph = (stable_hash(key) % 10_000) / 10_000 * 2 * math.pi
    x = t / (period_min * MIN) * 2 * math.pi
    return amp * (np.sin(x + ph) + 0.5 * np.sin(x / 2.7 + 2 * ph))


# ---------------------------------------------------------------------------
# Reference dimensions
# ---------------------------------------------------------------------------
def build_dims(w: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    assets = []
    for _, m in w["molding_machines"].iterrows():
        assets.append((m["machine_id"], "molding_machine", "injection_molding_machine", m["plant_id"], "plant",
                       m["telemetry_device_id"], m["controller_fw"]))
    for _, s in w["sterilizers"].iterrows():
        assets.append((s["sterilizer_id"], "sterilizer", "eto_sterilizer", s["plant_id"], "plant",
                       s["telemetry_device_id"], s["controller_fw"]))
    for _, a in w["dc_assets"].iterrows():
        assets.append((a["asset_id"], "dc_asset", a["asset_type"], a["dc_id"], "dc", a["telemetry_device_id"],
                       a["controller_fw"]))
    reefer = set(w["fleet_vehicles"].loc[w["fleet_vehicles"]["has_reefer"], "vehicle_id"])
    for _, v in w["fleet_vehicles"].iterrows():
        assets.append((v["vehicle_id"], "vehicle", "reefer_tractor" if v["vehicle_id"] in reefer else v["vehicle_type"],
                       v["home_dc_id"], "dc", v["telematics_device_id"], v["telematics_fw"]))
    adim = pd.DataFrame(assets, columns=["asset_id", "asset_kind", "asset_type", "site_id", "site_type",
                                         "device_id", "firmware"])
    adim["topic"] = adim["asset_kind"].map(TOPIC)

    tags = []
    for _, a in adim.sort_values(["topic", "asset_id"]).iterrows():
        if a["asset_kind"] == "dc_asset":
            sigs = DC_SIGNALS[a["asset_type"]]
        else:
            sigs = list(SIGNALS[a["asset_kind"]])
            if a["asset_type"] == "reefer_tractor":
                sigs.append(REEFER_SIGNAL)
        for sig, unit, iv, lo, hi, roc, crit in sigs:
            tags.append({"tag_name": f"{a['site_id']}.{a['asset_id']}.{sig}", "asset_id": a["asset_id"],
                         "asset_kind": a["asset_kind"], "asset_type": a["asset_type"], "site_id": a["site_id"],
                         "signal": sig, "unit": unit, "interval_ms": iv,
                         "expected_hz": (1000.0 / iv) if iv else None, "sampling": "periodic" if iv else "per_event",
                         "min_valid": float(lo), "max_valid": float(hi), "max_rate_of_change": float(roc),
                         "frozen_threshold": 30 if iv and iv <= 10000 else 10, "criticality": crit,
                         "topic": a["topic"]})
    tdim = pd.DataFrame(tags)
    tdim.insert(0, "tag_id", np.arange(1, len(tdim) + 1, dtype=np.int32))
    return adim, tdim


# ---------------------------------------------------------------------------
# Context: schedules from step 2, scenarios, tag index
# ---------------------------------------------------------------------------
class Context:
    def __init__(self, cfg: P2PConfig, w: dict, tx: dict):
        self.cfg, self.w, self.tx = cfg, w, tx
        self.adim, self.tdim = build_dims(w)
        self.TAG = {(a, s): int(t) for a, s, t in self.tdim[["asset_id", "signal", "tag_id"]].values}
        self.live = ts_ms(cfg.live_start)
        self.hist0 = ts_ms(cfg.history_start)
        self.hero = cfg.hero_sterilizer_id or w["sterilizers"].sort_values("sterilizer_id")["sterilizer_id"].iloc[0]
        self.drift_start = self.live - cfg.hero_drift_days * DAYMS
        self._molding()
        self._sterilizers()
        self._dc()
        self._fleet()
        self.scenarios = build_scenarios(self)
        self.scen_by_asset = defaultdict(list)
        for s in self.scenarios:
            self.scen_by_asset[s["asset_id"]].append(s)

    def _molding(self):
        wo = self.tx["work_orders"]
        wo = wo[wo["resource_id"].str.startswith("IMM")].merge(self.w["molds"], on="mold_id", how="left")
        mach = self.w["molding_machines"].set_index("machine_id")
        self.MOLD = {}
        for mid, g in wo.sort_values("start_ts").groupby("resource_id"):
            s = to_ms(g["start_ts"])
            self.MOLD[mid] = dict(
                ws=s, ps=s + 90 * MIN, we=to_ms(g["end_ts"]), wo=g["work_order_id"].to_numpy(),
                melt=g["melt_temp_setpoint_c"].to_numpy(float), mold=g["mold_temp_setpoint_c"].to_numpy(float),
                inj=g["injection_pressure_setpoint_bar"].to_numpy(float), cyc=g["nominal_cycle_s"].to_numpy(float),
                ton=float(mach.loc[mid, "clamp_tonnage"]))

    def _sterilizers(self):
        ld = self.tx["sterilization_loads"].sort_values("start_ts")
        dev = self.tx["deviations"]
        fails = dev[(dev["category"] == "sterilization") & (dev["source"] == "simulation")]
        fail_at = {(a, ts_ms(t)): k for a, t, k in fails[["asset_id", "opened_ts", "type_key"]].values}
        st = self.w["sterilizers"].set_index("sterilizer_id")
        self.STER = {}
        for sid in st.index:
            g = ld[ld["sterilizer_id"] == sid]
            ce = to_ms(g["cycle_end_ts"])
            self.STER[sid] = dict(
                s=to_ms(g["start_ts"]), pe=to_ms(g["precond_end_ts"]), ee=to_ms(g["exposure_end_ts"]), ce=ce,
                load=g["load_id"].to_numpy(),
                fail=np.array([fail_at.get((sid, int(c)), "") for c in ce], dtype=object),
                vf=float(st.loc[sid, "chamber_volume_m3"]) / 112.0)

    def _dc(self):
        sh = self.tx["shipments"].merge(self.w["routes"][["route_id", "dc_id"]], on="route_id")
        daily = sh.groupby(["dc_id", "ship_date"]).size()
        mean = daily.groupby(level=0).mean()
        self.DC_DAY = {(dc, pd.Timestamp(d).date()): float(v / mean[dc]) for (dc, d), v in daily.items()}
        self.DCA = self.w["dc_assets"].set_index("asset_id").to_dict("index")

    def _fleet(self):
        cfg, w, tx = self.cfg, self.w, self.tx
        dcs = w["distribution_centers"].set_index("dc_id")
        cust = w["customers"].set_index("customer_id")
        stops = w["route_stops"].sort_values(["route_id", "stop_seq"])
        routes = w["routes"].set_index("route_id")
        sh = tx["shipments"].merge(tx["customer_orders"][["order_id", "customer_id"]], on="order_id")
        delivered = {(c, pd.Timestamp(d).date()): ts_ms(t) for c, d, t in sh[["customer_id", "ship_date", "delivered_ts"]].values}
        days = pd.date_range(cfg.history_start - pd.Timedelta(days=1), cfg.live_start + pd.Timedelta(days=1), freq="D")
        wd = {"MON-FRI": (0, 1, 2, 3, 4), "MON-WED-FRI": (0, 2, 4), "TUE-THU": (1, 3)}
        per_vehicle = defaultdict(list)
        for rid, r in routes.iterrows():
            seq = stops[stops["route_id"] == rid]
            for d in days:
                if d.weekday() not in wd[r["service_days"]]:
                    continue
                day0 = ts_ms(d.tz_localize("UTC") if d.tzinfo is None else d)
                dep = day0 + int(r["planned_departure_min"]) * MIN
                pts = [(dep, dcs.loc[r["dc_id"], "lat"], dcs.loc[r["dc_id"], "lon"], 0)]
                # Arrivals are the recorded delivered_ts (system of record); dwell is
                # trimmed if the next recorded arrival leaves less time.
                cs = [cust.loc[c_] for c_ in seq["customer_id"]]
                arrs = [delivered.get((c_, d.date()), dep + int(o * MIN))
                        for c_, o in zip(seq["customer_id"], seq["planned_arrival_offset_min"])]
                prev_t = dep
                for i, c in enumerate(cs):
                    arr = max(arrs[i], prev_t + MIN)
                    leave = arr + (35 if c["customer_type"] == "hospital" else 20) * MIN
                    if i + 1 < len(arrs):
                        leave = max(arr + 5 * MIN, min(leave, arrs[i + 1] - 3 * MIN))
                    pts.append((arr, c["lat"], c["lon"], 1))
                    pts.append((leave, c["lat"], c["lon"], 0))
                    prev_t = leave
                back = haversine_km(pts[-1][1], pts[-1][2], dcs.loc[r["dc_id"], "lat"], dcs.loc[r["dc_id"], "lon"]) * 1.25
                pts.append((prev_t + int(back / 55 * 60) * MIN, dcs.loc[r["dc_id"], "lat"], dcs.loc[r["dc_id"], "lon"], 2))
                per_vehicle[r["vehicle_id"]].append(pts)
        self.FLEET = {}
        for _, v in w["fleet_vehicles"].iterrows():
            home = dcs.loc[v["home_dc_id"]]
            trips = sorted(per_vehicle.get(v["vehicle_id"], []))
            t, la, lo, arr_t, ends = [0.0], [home["lat"]], [home["lon"]], [], []
            for trip in trips:
                for (tt, a, b, kind) in trip:
                    t.append(tt); la.append(a); lo.append(b)
                    if kind == 1:
                        arr_t.append(tt)
                ends.append((trip[0][0], trip[-1][0]))
            t.append(9e15); la.append(home["lat"]); lo.append(home["lon"])
            t, la, lo = np.array(t), np.array(la), np.array(lo)
            dist = np.array([haversine_km(la[i], lo[i], la[i + 1], lo[i + 1]) * 1.25 for i in range(len(t) - 1)])
            dur_h = np.maximum(np.diff(t) / HOUR, 1e-9)
            spd = np.where(dist > 0.01, dist / dur_h, 0.0)
            self.FLEET[v["vehicle_id"]] = dict(t=t, lat=la, lon=lo, spd=spd, arr=np.array(arr_t),
                                               trips=np.array(ends) if ends else np.zeros((0, 2)),
                                               reefer=bool(v["has_reefer"]), setpoint=15.0)

    # hero probe error (percentage points, reads high) as a function of time
    def hero_drift(self, t) -> np.ndarray:
        slope = self.cfg.hero_true_rh_drift_at_live / (self.live - self.drift_start)
        return np.where(t >= self.drift_start, (t - self.drift_start) * slope, 0.0)


# ---------------------------------------------------------------------------
# Scenarios (ground truth): process anomalies + data-quality defects
# ---------------------------------------------------------------------------
def build_scenarios(ctx: Context) -> list[dict]:
    cfg, w, tx = ctx.cfg, ctx.w, ctx.tx
    sc = []

    def add(**kw):
        kw.setdefault("end_ms", None)
        kw.setdefault("linked_deviation_id", None)
        kw.setdefault("linked_load_id", None)
        kw["anomaly_id"] = f"AN-{len(sc) + 1:05d}"
        sc.append(kw)

    # 1. Molding deviations from step 2 get a telemetry precursor.
    effect = {"mold_short_shot": ("melt_temp_c", "heater_band_failure"), "mold_flash": ("inj_pressure_peak_bar", "pressure_overshoot"),
              "mold_black_specks": ("melt_temp_c", "barrel_overheat"), "mold_dimensional_oos": ("mold_temp_c", "mold_cooling_loss")}
    dev = tx["deviations"]
    for _, d in dev[(dev["category"] == "molding") & (dev["source"] == "simulation")].iterrows():
        sig, kind = effect[d["type_key"]]
        o = ts_ms(d["opened_ts"])
        add(scenario=kind, category="process", window="history", asset_id=d["asset_id"], signal=sig,
            start_ms=o - 8 * HOUR, end_ms=o - 2 * HOUR, alarm_ms=o - 3 * HOUR, severity=d["severity"],
            linked_deviation_id=d["deviation_id"],
            description=f"Precursor to {d['type_key']} ({d['deviation_id']}): {sig} departs from setpoint over 6 h")
    # 2. Failed sterilisation cycles from step 2.
    fmap = {"eto_low_gas_conc": ("eto_conc_mg_l", "eto_low_gas"), "eto_vacuum_leak": ("chamber_pressure_kpa", "vacuum_leak"),
            "eto_temp_excursion": ("chamber_temp_c", "exposure_temp_low")}
    for sid, S in ctx.STER.items():
        for i, f in enumerate(S["fail"]):
            if f:
                sig, kind = fmap[f]
                add(scenario=kind, category="process", window="history", asset_id=sid, signal=sig,
                    start_ms=int(S["s"][i]), end_ms=int(S["ce"][i]), alarm_ms=int(S["pe"][i]), severity="major",
                    linked_load_id=S["load"][i], description=f"Failed EtO cycle {S['load'][i]}: {f}")
    # 3. HERO: RH probe drift (history -> live), invisible on RH, visible on steam.
    add(scenario="rh_probe_drift", category="process", window="history+live", asset_id=ctx.hero, signal="chamber_rh_pct",
        start_ms=ctx.drift_start, alarm_ms=None, severity="critical",
        description="RH probe reads high by a growing margin; controller under-humidifies. Reported RH stays at "
                    "setpoint; steam_flow_kg_h falls. True RH < 50% validated minimum once error > 15 points.")
    m = cfg.story_minutes
    add(scenario="rh_probe_failure", category="equipment", window="live", asset_id=ctx.hero, signal="chamber_rh_pct",
        start_ms=ctx.live + m["rh_probe_failure"] * MIN, end_ms=ctx.live + 120 * MIN,
        alarm_ms=ctx.live + m["rh_probe_failure"] * MIN, severity="critical",
        description="Drifted RH probe fails: saturates near 100% (UNCERTAIN, then BAD). The live alarm that starts the hero investigation.")
    # 4. Sorter bearing degradation (history trend -> live ISO alarm).
    outage_dc = w["distribution_centers"].loc[w["distribution_centers"]["is_outage_scenario_dc"], "dc_id"].iloc[0]
    da = w["dc_assets"]
    sorter = da[(da["asset_type"] == "crossbelt_sorter") & (da["dc_id"] != outage_dc)].sort_values("asset_id")["asset_id"].iloc[0]
    add(scenario="bearing_degradation", category="equipment", window="history+live", asset_id=sorter, signal="vibration_mm_s",
        start_ms=ctx.live - 5 * DAYMS, alarm_ms=ctx.live + m["sorter_vibration_alarm"] * MIN, severity="major",
        description=f"Sorter bearing wear: vibration grows exponentially, crossing ISO {ISO_VIBRATION_ALARM} mm/s at the alarm time; "
                    "early trend visible ~2 days ahead (predictive-maintenance target).")
    # 5. Reefer failure on a staged trailer (live).
    fl = w["fleet_vehicles"]
    truck = fl[fl["has_reefer"] & (fl["home_dc_id"] != outage_dc)].sort_values("vehicle_id")["vehicle_id"].iloc[0]
    add(scenario="reefer_failure", category="equipment", window="live", asset_id=truck, signal="reefer_temp_c",
        # SOP-LOG-601 alarm = above 20 C for MORE THAN 15 min. With a 40-min rise toward ambient and sensor
        # noise, readings stay above 20 C from ~17 min after failure, so failure starts 32 min before the alarm.
        start_ms=ctx.live + (m["reefer_failure"] - 32) * MIN, end_ms=ctx.live + 180 * MIN,
        alarm_ms=ctx.live + m["reefer_failure"] * MIN, severity="major",
        description="Reefer compressor failure on a trailer staged at the dock: temperature climbs toward ambient; "
                    "temperature-sensitive product at risk.")
    # 6. DC outage (live): every asset at the outage DC goes silent.
    add(scenario="dc_outage", category="site", window="live", asset_id=outage_dc, signal="*",
        start_ms=ctx.live + m["dc_outage"] * MIN, end_ms=ctx.live + (m["dc_outage"] + 90) * MIN,
        alarm_ms=ctx.live + m["dc_outage"] * MIN + 2 * MIN, severity="critical",
        description="Site outage at the outage-scenario DC: all telemetry stops for 90 minutes; network rebalancing needed.")
    # 7. Data-quality defects in history (silent ones are the interesting ones).
    rng = rng_for(cfg.seed, "tel", "defects")
    hot = {s["asset_id"] for s in sc}
    pool = ctx.tdim[(ctx.tdim["interval_ms"] > 0) & ~ctx.tdim["asset_id"].isin(hot)
                    & ~ctx.tdim["signal"].isin(["machine_state", "cycle_phase", "amr_state", "engine_on", "gps_lat", "gps_lon"])]
    span0, span1 = ctx.hist0 + DAYMS, ctx.live - DAYMS
    n = {"lite": (4, 25, 60), "baseline": (20, 120, 300)}.get(cfg.profile, (4, 25, 60))
    picks = pool.sample(n=min(len(pool), sum(n)), random_state=int(rng.integers(1 << 30)))
    rows = picks.to_dict("records")
    for i, r in enumerate(rows):
        start = int(rng.integers(span0, span1) // MIN * MIN)
        if i < n[0]:
            add(scenario="dq_stuck_sensor", category="data_quality", window="history", asset_id=r["asset_id"],
                signal=r["signal"], start_ms=start, end_ms=start + int(rng.integers(4, 9)) * HOUR, alarm_ms=None,
                severity="minor", description="Sensor frozen at last value, quality still GOOD (silent).")
        elif i < n[0] + n[1]:
            add(scenario="dq_dropout", category="data_quality", window="history", asset_id=r["asset_id"],
                signal=r["signal"], start_ms=start, end_ms=start + int(rng.integers(2, 21)) * MIN, alarm_ms=None,
                severity="minor", description="Sensor dropout: values missing, quality BAD.")
        else:
            add(scenario="dq_spike", category="data_quality", window="history", asset_id=r["asset_id"],
                signal=r["signal"], start_ms=start, end_ms=start + 1, alarm_ms=None, severity="minor",
                description="Single-sample spike (x5), quality GOOD (silent).")
    for s in sc:
        s["tag_id"] = ctx.TAG.get((s["asset_id"], s["signal"]))
    return sc


def ramp(t, s) -> np.ndarray:
    """0 before the window, linear 0->1 across it, 0 after (deviation resolved)."""
    return np.where((t >= s["start_ms"]) & (t < s["end_ms"]), (t - s["start_ms"]) / (s["end_ms"] - s["start_ms"]), 0.0)


# ---------------------------------------------------------------------------
# Generators. Each returns {tag_id: (ts_ms int64, value float64, quality int8)}
# ---------------------------------------------------------------------------
def _out(ctx, asset, sig, t, v, q=None):
    return ctx.TAG[(asset, sig)], (t.astype(np.int64), v.astype(np.float64),
                                    np.zeros(len(t), np.int8) if q is None else q.astype(np.int8))


def gen_molding(ctx: Context, a: str, t0: int, t1: int, rng) -> dict:
    M = ctx.MOLD.get(a, dict(ws=np.array([]), ps=np.array([]), we=np.array([]), wo=np.array([]), melt=np.array([]),
                             mold=np.array([]), inj=np.array([]), cyc=np.array([]), ton=250.0))
    scen = ctx.scen_by_asset.get(a, [])
    out = {}

    def state(t):
        i = np.searchsorted(M["ws"], t, "right") - 1
        ok = (i >= 0)
        ic = np.clip(i, 0, max(len(M["ws"]) - 1, 0))
        valid = ok & (t < (M["we"][ic] if len(M["we"]) else 0))
        run = valid & (t >= (M["ps"][ic] if len(M["ps"]) else 0))
        return ic, valid, run

    t = grid(t0, t1, 5000)
    ic, valid, run = state(t)
    melt_sp = np.where(valid, M["melt"][ic] if len(M["melt"]) else 190, 190.0)
    mold_sp = np.where(valid, M["mold"][ic] if len(M["mold"]) else 28, 28.0)
    ws = M["ws"][ic] if len(M["ws"]) else np.zeros_like(t, float)
    heat = melt_sp - (melt_sp - 190) * np.exp(-np.maximum(t - ws, 0) / (25 * MIN))
    melt = np.where(run, melt_sp + wander(t, (a, "melt"), 1.0, 37), np.where(valid, heat, 190 + wander(t, (a, "sb"), 0.8, 53)))
    melt += rng.normal(0, 0.5, len(t))
    mold = np.where(valid, mold_sp + wander(t, (a, "mold"), 0.6, 23), 28 + wander(t, (a, "mamb"), 1.5, 600)) + rng.normal(0, 0.3, len(t))
    power = np.where(run, M["ton"] * 0.12 + 2.5 * np.sin(t / 7000), np.where(valid, M["ton"] * 0.05, 1.5))
    power = np.maximum(0.0, power + rng.normal(0, 1, len(t)) * np.where(valid, 0.6, 0.15))  # standby: small noise, never < 0
    for s in scen:
        r = ramp(t, s) if s["end_ms"] else 0
        if s["scenario"] == "heater_band_failure":
            melt -= 14 * r
        elif s["scenario"] == "barrel_overheat":
            melt += 16 * r
        elif s["scenario"] == "mold_cooling_loss":
            mold += 8 * r
    out.update([_out(ctx, a, "melt_temp_c", t, melt), _out(ctx, a, "mold_temp_c", t, mold),
                _out(ctx, a, "motor_power_kw", t, power)])
    t10 = grid(t0, t1, 10000)
    _, v10, r10 = state(t10)
    oil = 40 + 6 * r10 + 2 * v10 + wander(t10, (a, "oil"), 1.5, 900) + rng.normal(0, 0.2, len(t10))
    out.update([_out(ctx, a, "hyd_oil_temp_c", t10, oil)])
    t60 = grid(t0, t1, 60000)
    _, v60, r60 = state(t60)
    out.update([_out(ctx, a, "machine_state", t60, np.where(r60, 1, np.where(v60, 2, 0)).astype(float))])

    # per-shot signals
    st, ct, ip, cp, sc_ = [], [], [], [], []
    for k in range(len(M["ws"])):
        lo, hi = max(t0, M["ps"][k]), min(t1, M["we"][k])
        if hi <= lo:
            continue
        cyc = M["cyc"][k] * 1000
        ks = np.arange(math.ceil((lo - M["ps"][k]) / cyc), math.ceil((hi - M["ps"][k]) / cyc))
        if not len(ks):
            continue
        tt = M["ps"][k] + ks * cyc + rng.normal(0, 0.01 * cyc, len(ks))
        tt = np.clip(tt, lo, hi - 1)
        inj = M["inj"][k] * (1 + rng.normal(0, 0.008, len(ks)))
        cav = 0.55 * M["inj"][k] * (1 + rng.normal(0, 0.015, len(ks)))
        for s in scen:
            r = ramp(tt, s)
            if s["scenario"] == "heater_band_failure":
                cav *= 1 - 0.20 * r  # cold melt -> short shots: cavity peak falls toward the 0.45 alarm
            elif s["scenario"] == "pressure_overshoot":
                inj *= 1 + 0.07 * r
                cav *= 1 + 0.10 * r
            elif s["scenario"] == "mold_cooling_loss":
                cav *= 1 - 0.03 * r
        st.append(tt); ct.append(M["cyc"][k] * (1 + rng.normal(0, 0.01, len(ks))))
        ip.append(inj); cp.append(cav); sc_.append(ks + 1.0)
    cat = (lambda x: np.concatenate(x) if x else np.array([]))
    tt = cat(st).astype(np.int64)
    for sig, vals in (("cycle_time_s", cat(ct)), ("inj_pressure_peak_bar", cat(ip)),
                      ("cavity_pressure_peak_bar", cat(cp)), ("shot_count", cat(sc_))):
        out.update([_out(ctx, a, sig, tt, vals)])
    return out


def steriliser_profile(ctx: Context, a: str, t: np.ndarray) -> dict:
    """True physical state of the chamber at times t (before probe error and noise)."""
    S = ctx.STER[a]
    n = len(t)
    i = np.searchsorted(S["s"], t, "right") - 1
    ic = np.clip(i, 0, max(len(S["s"]) - 1, 0))
    if len(S["s"]):
        inl = (i >= 0) & (t < S["ce"][ic])
        s, pe, ee, ce = S["s"][ic], S["pe"][ic], S["ee"][ic], S["ce"][ic]
        fail = S["fail"][ic]
    else:
        inl = np.zeros(n, bool)
        s = pe = ee = ce = np.zeros(n)
        fail = np.array([""] * n, dtype=object)
    pre, exp_ = inl & (t < pe), inl & (t >= pe) & (t < ee)
    post = inl & (t >= ee)
    tp, te, tq = (t - s) / MIN, (t - pe) / MIN, (t - ee) / MIN
    rem = (ce - t) / MIN
    leak, low_gas, cold = fail == "eto_vacuum_leak", fail == "eto_low_gas_conc", fail == "eto_temp_excursion"
    target = 65.0 - (ctx.hero_drift(t) if a == ctx.hero else 0.0)

    floor = np.where(leak, 21.0, 8.0)
    p_pre = np.where(tp < 20, floor + (101.3 - floor) * np.exp(-tp / 5), floor + 7 * (1 - np.exp(-(tp - 20) / 8)))
    p_exp = floor + 7 + 40 * (1 - np.exp(-te / 4)) - 0.002 * te
    w = np.mod(tq, 35)
    p_wash = np.where(w < 15, 8 + 52 * np.exp(-w / 3), 8 + 52 * (1 - np.exp(-(w - 15) / 3)))
    p_post = np.where(rem < 10, 8 + 93.3 * (1 - np.exp(-(10 - rem) / 2.5)), p_wash)
    pressure = np.select([pre, exp_, post], [p_pre, p_exp, p_post], 101.3)

    t_exp_target = np.where(cold, 46.5, 52.0)
    temp = np.select([pre, exp_, post],
                     [30 + 16 * (1 - np.exp(-tp / 20)), 46 + (t_exp_target - 46) * (1 - np.exp(-te / 15)),
                      52 - 12 * (1 - np.exp(-tq / 40))], 24 + wander(t, (a, "amb"), 1.5, 1440))
    rh_pre = np.where(tp < 20, 5 + 30 * np.exp(-tp / 5), target * (1 - np.exp(-(tp - 20) / 10)))
    rh = np.select([pre, exp_, post], [rh_pre, target - 3 * (1 - np.exp(-te / 60)), 8 + (target - 3) * np.exp(-tq / 20)],
                   45 + wander(t, (a, "rh"), 3, 720))
    gas = np.where(low_gas, 400.0, 550.0)
    conc = np.select([exp_, post], [gas * (1 - np.exp(-te / 4)) - 0.04 * te, 520 * np.exp(-tq / 12)], 0.0)
    vf = S["vf"]
    steam = np.select([pre & (tp >= 20) & (tp < 60), exp_], [140 * vf * target / 65, 6 * vf * target / 65], 0.0)
    jacket = np.where(inl, 54.0, 30.0)
    phase = np.select([pre, exp_, post], [1.0, 2.0, 3.0], 0.0)
    return dict(pressure=pressure, temp=temp, rh=rh, conc=conc, steam=steam, jacket=jacket, phase=phase)


def gen_sterilizer(ctx: Context, a: str, t0: int, t1: int, rng) -> dict:
    out = {}
    t = grid(t0, t1, 2000)
    P = steriliser_profile(ctx, a, t)
    rh_rep = P["rh"] + rng.normal(0, 0.4, len(t))
    q = np.zeros(len(t), np.int8)
    if a == ctx.hero:
        rh_rep = rh_rep + ctx.hero_drift(t)          # probe reads high
        for s in ctx.scen_by_asset[a]:
            if s["scenario"] == "rh_probe_failure":
                f = (t >= s["start_ms"]) & (t < s["end_ms"])
                rh_rep = np.where(f, 99.4 + rng.normal(0, 0.3, len(t)), rh_rep)
                q = np.where(f, np.where(t < s["start_ms"] + 10 * MIN, UNCERTAIN, BAD), q)
    out.update([_out(ctx, a, "chamber_pressure_kpa", t, P["pressure"] + rng.normal(0, 0.15, len(t))),
                _out(ctx, a, "chamber_temp_c", t, P["temp"] + rng.normal(0, 0.1, len(t))),
                _out(ctx, a, "chamber_rh_pct", t, np.clip(rh_rep, 0, 100), q)])
    t5 = grid(t0, t1, 5000)
    P5 = steriliser_profile(ctx, a, t5)
    out.update([_out(ctx, a, "steam_flow_kg_h", t5, np.maximum(0, P5["steam"] * (1 + rng.normal(0, 0.02, len(t5)))))])
    t10 = grid(t0, t1, 10000)
    P10 = steriliser_profile(ctx, a, t10)
    out.update([_out(ctx, a, "eto_conc_mg_l", t10, np.maximum(0, P10["conc"] + rng.normal(0, 3, len(t10)))),
                _out(ctx, a, "jacket_temp_c", t10, P10["jacket"] + rng.normal(0, 0.1, len(t10)))])
    t60 = grid(t0, t1, 60000)
    out.update([_out(ctx, a, "cycle_phase", t60, steriliser_profile(ctx, a, t60)["phase"])])
    return out


_SHIFT = np.array([0.9, 0.8, 0.6, 0.5, 0.4, 0.3, 0.45, 0.6, 0.6, 0.6, 0.55, 0.5,
                   0.5, 0.5, 0.45, 0.4, 0.5, 0.7, 1.0, 1.0, 1.0, 1.0, 1.0, 0.95])  # UTC hour; night picking peaks


def dc_activity(ctx: Context, dc: str, t: np.ndarray) -> np.ndarray:
    key = (dc, int(t[0]) if len(t) else 0, len(t))
    cache = ctx.__dict__.setdefault("_act_cache", {})
    if key in cache:
        return cache[key]
    if len(cache) > 20000:
        cache.clear()
    cache[key] = _dc_activity(ctx, dc, t)
    return cache[key]


def _dc_activity(ctx: Context, dc: str, t: np.ndarray) -> np.ndarray:
    hours = (t // HOUR) % 24
    dates = pd.to_datetime(t, unit="ms", utc=True).date
    wkd = pd.to_datetime(t, unit="ms", utc=True).weekday
    day_f = np.array([ctx.DC_DAY.get((dc, d), 1.0) for d in dates]) if len(t) < 50 else None
    if day_f is None:  # vectorise via unique days
        uniq = {d: ctx.DC_DAY.get((dc, d), 1.0) for d in set(dates)}
        day_f = np.array([uniq[d] for d in dates])
    return _SHIFT[hours] * np.clip(day_f, 0.3, 1.6) * np.where(wkd >= 5, 0.25, 1.0)


def gen_dc_asset(ctx: Context, a: str, t0: int, t1: int, rng) -> dict:
    A = ctx.DCA[a]
    typ, dc = A["asset_type"], A["dc_id"]
    out = {}
    thr = 0.15 + (stable_hash(a, "thr") % 20) / 100.0
    kw = A["rated_motor_kw"]
    degr = [s for s in ctx.scen_by_asset.get(a, []) if s["scenario"] == "bearing_degradation"]

    def excess(t):
        """Two-stage bearing wear: a slow multi-day rise (the predictive-maintenance
        signal, +1.5 mm/s by the alarm) and a fast final stage (spalling, tau 30 min)
        that crosses the ISO alarm exactly at alarm_ms. A single exponential cannot
        do both: gentle enough to trend for days means already alarming 20 minutes early."""
        if not degr:
            return np.zeros(len(t))
        s = degr[0]
        slow = 1.5 * np.clip((t - s["start_ms"]) / (s["alarm_ms"] - s["start_ms"]), 0, None) ** 1.5
        fast = (ISO_VIBRATION_ALARM - 2.25 - 1.5) * np.exp((t - s["alarm_ms"]) / (30 * MIN))
        return np.where(t >= s["start_ms"], slow + fast, 0.0)

    if typ != "amr":
        t = grid(t0, t1, 5000)
        act = dc_activity(ctx, dc, t)
        run = act > thr
        cur = np.where(run, kw * 1.8 * (0.3 + 0.7 * act), 0.0) + np.abs(rng.normal(0, 0.15, len(t)))
        out.update([_out(ctx, a, "motor_current_a", t, cur)])
    if typ in ("conveyor_segment", "crossbelt_sorter"):
        t = grid(t0, t1, 10000)
        act = dc_activity(ctx, dc, t)
        run = act > thr
        vib = np.where(run, 1.2 + 1.0 * act, 0.0) + 0.3 + np.abs(rng.normal(0, 0.08, len(t))) + excess(t) * run
        spd = np.where(run, A["rated_speed_mps"] or 2.0, 0.0) + rng.normal(0, 0.01, len(t))
        out.update([_out(ctx, a, "vibration_mm_s", t, vib), _out(ctx, a, "belt_speed_mps", t, np.maximum(spd, 0))])
        t60 = grid(t0, t1, 60000)
        act60 = dc_activity(ctx, dc, t60)
        btemp = 26 + 14 * act60 + 3 * excess(t60) + rng.normal(0, 0.2, len(t60))
        out.update([_out(ctx, a, "bearing_temp_c", t60, btemp)])
        if typ == "crossbelt_sorter":
            tput = (A["rated_throughput_per_hr"] or 10000) / 60
            out.update([_out(ctx, a, "sort_rate_per_min", t60, np.maximum(0, tput * act60 * (act60 > thr) * (1 + rng.normal(0, 0.03, len(t60))))),
                        _out(ctx, a, "missort_pct", t60, np.maximum(0, 0.04 + rng.normal(0, 0.01, len(t60)) + 0.08 * excess(t60)))])
    elif typ == "induction_station":
        t60 = grid(t0, t1, 60000)
        act60 = dc_activity(ctx, dc, t60)
        out.update([_out(ctx, a, "induct_rate_per_min", t60, np.maximum(0, (A["rated_throughput_per_hr"] or 2400) / 60 * act60 * (1 + rng.normal(0, 0.04, len(t60)))))])
    elif typ == "amr":
        t30 = grid(t0, t1, 30000)
        ph = np.mod(t30 / MIN + stable_hash(a, "amr") % 300, 300)
        batt = np.where(ph < 240, 95 - 75 * ph / 240, 20 + 75 * (ph - 240) / 60) + rng.normal(0, 0.3, len(t30))
        out.update([_out(ctx, a, "battery_pct", t30, np.clip(batt, 0, 100))])
        t60 = grid(t0, t1, 60000)
        ph60 = np.mod(t60 / MIN + stable_hash(a, "amr") % 300, 300)
        act60 = dc_activity(ctx, dc, t60)
        out.update([_out(ctx, a, "amr_state", t60, np.where(ph60 >= 240, 2.0, np.where(act60 > thr, 1.0, 0.0)))])
    else:
        t60 = grid(t0, t1, 60000)
        act60 = dc_activity(ctx, dc, t60)
        k = {"shuttle_asrs": (A["rated_throughput_per_hr"] or 600) / 60, "palletizer": 12, "stretch_wrapper": 2.5}[typ]
        out.update([_out(ctx, a, "cycles_per_min", t60, np.maximum(0, k * act60 * (act60 > thr) * (1 + rng.normal(0, 0.05, len(t60)))))])
    return out


def gen_vehicle(ctx: Context, a: str, t0: int, t1: int, rng) -> dict:
    F = ctx.FLEET[a]
    out = {}
    t = grid(t0, t1, 30000)
    seg = np.clip(np.searchsorted(F["t"], t, "right") - 1, 0, len(F["spd"]) - 1)
    moving = F["spd"][seg] > 0.5
    keep = moving | (t % (5 * MIN) == 0)           # parked: heartbeat every 5 min
    t = t[keep]
    seg, moving = seg[keep], moving[keep]
    lat = np.interp(t, F["t"], F["lat"]) + rng.normal(0, 0.00003, len(t))
    lon = np.interp(t, F["t"], F["lon"]) + rng.normal(0, 0.00003, len(t))
    spd = np.where(moving, F["spd"][seg] * (1 + rng.normal(0, 0.08, len(t))), 0.0)
    out.update([_out(ctx, a, "gps_lat", t, lat), _out(ctx, a, "gps_lon", t, lon),
                _out(ctx, a, "speed_kmh", t, np.clip(spd, 0, 120))])
    t60 = grid(t0, t1, 60000)
    on = np.zeros(len(t60), bool)
    for s_, e_ in F["trips"]:
        on |= (t60 >= s_ - 10 * MIN) & (t60 < e_)
    out.update([_out(ctx, a, "engine_on", t60, on.astype(float))])
    if F["reefer"]:
        temp = F["setpoint"] + rng.normal(0, 0.2, len(t60))
        arr = F["arr"][(F["arr"] > t0 - HOUR) & (F["arr"] < t1)]
        for ar in arr:
            temp += np.where(t60 >= ar, 3.0 * np.exp(-(t60 - ar) / (10 * MIN)), 0.0)
        for s in ctx.scen_by_asset.get(a, []):
            if s["scenario"] == "reefer_failure":
                temp += np.where(t60 >= s["start_ms"], (30 - F["setpoint"]) * (1 - np.exp(-np.maximum(t60 - s["start_ms"], 0) / (40 * MIN))), 0.0)
        out.update([_out(ctx, a, "reefer_temp_c", t60, temp)])
    return out


GENERATORS = {"molding_machine": gen_molding, "sterilizer": gen_sterilizer, "dc_asset": gen_dc_asset, "vehicle": gen_vehicle}


def generate(ctx: Context, asset_id: str, kind: str, t0: int, t1: int) -> dict:
    """All tags of one asset over [t0, t1). t0 must sit on a 6-hour chunk boundary
    so the RNG stream is the same whether called by the batch job or the live publisher."""
    assert t0 % CHUNK == 0 and t1 - t0 <= CHUNK, "generate() works on aligned 6-hour chunks"
    rng = rng_for(ctx.cfg.seed, "tel", asset_id, t0 // CHUNK)
    out = GENERATORS[kind](ctx, asset_id, t0, t1, rng)
    # site outage: no data at all from that DC
    for s in ctx.scenarios:
        if s["scenario"] == "dc_outage" and kind in ("dc_asset",) and ctx.DCA[asset_id]["dc_id"] == s["asset_id"]:
            for k, (tt, v, q) in out.items():
                m = ~((tt >= s["start_ms"]) & (tt < s["end_ms"]))
                out[k] = (tt[m], v[m], q[m])
    return apply_defects(ctx, asset_id, out)


def apply_defects(ctx: Context, a: str, out: dict) -> dict:
    for s in ctx.scen_by_asset.get(a, []):
        if not s["scenario"].startswith("dq_") or s["tag_id"] not in out:
            continue
        tt, v, q = out[s["tag_id"]]
        m = (tt >= s["start_ms"]) & (tt < s["end_ms"])
        if not m.any():
            if s["scenario"] == "dq_spike":
                j = np.searchsorted(tt, s["start_ms"])
                if j < len(tt) and tt[j] - s["start_ms"] < 2 * MIN:
                    v = v.copy(); v[j] *= 5
                    out[s["tag_id"]] = (tt, v, q)
            continue
        v, q = v.copy(), q.copy()
        if s["scenario"] == "dq_stuck_sensor":
            v[m] = v[np.argmax(m)]
        elif s["scenario"] == "dq_dropout":
            v[m], q[m] = np.nan, BAD
        out[s["tag_id"]] = (tt, v, q)
    return out


def aggregate(tt: np.ndarray, v: np.ndarray, q: np.ndarray, bucket: int):
    """Per-bucket n, mean, min, max, last, n_bad (BAD/NaN excluded from stats)."""
    if not len(tt):
        return None
    b = tt // bucket * bucket
    edges = np.flatnonzero(np.diff(b)) + 1
    starts = np.concatenate([[0], edges])
    good = (q != BAD) & ~np.isnan(v)
    vz = np.where(good, v, 0.0)
    n_good = np.add.reduceat(good.astype(np.int32), starts)
    s = np.add.reduceat(vz, starts)
    mn = np.minimum.reduceat(np.where(good, v, np.inf), starts)
    mx = np.maximum.reduceat(np.where(good, v, -np.inf), starts)
    cnt = np.diff(np.concatenate([starts, [len(tt)]]))
    last = v[np.concatenate([edges - 1, [len(tt) - 1]])]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(n_good > 0, s / np.maximum(n_good, 1), np.nan)
    mn = np.where(n_good > 0, mn, np.nan)
    mx = np.where(n_good > 0, mx, np.nan)
    return b[starts], cnt, mean, mn, mx, last, cnt - n_good
