"""
Step 5: live publisher to Azure Event Hubs (Kafka endpoint).
============================================================
Streams the same physics as the step-3 history, continuing from live start,
to four Event Hubs over ONE Kafka producer:

    molding-telemetry | sterilizer-cycles | dc-asset-telemetry | fleet-telemetry

Message = one JSON envelope per (topic, site) per flush interval, packed
against a byte budget just under the 64 KB Event Hubs billing unit:

    {"src":"p2p-controltower","topic":...,"site":"P01","sent_ms":...,"n":42,
     "records":[{"t":1790812800000,"g":417,"v":52.03,"q":"G"}, ...]}

Lean records carry only time (epoch ms), tag_id, value and quality; join
reference/tag_dim for everything else (the Novelis finding: the envelope,
not the shape, decides cost). Partition key = site, so per-device order is
preserved while messages still fill the billing unit.

Live-only defects, applied at publish time and recorded in the run's ground
truth: late / out-of-order records, duplicates, a clock-skewed DC gateway,
and a firmware 4.3 schema change on one plant's molding machines (new
melt-temperature tag IDs not yet in tag_dim, plus an extra "fw" field).

Event Hubs gotchas carried over from the Novelis POC (each cost hours there):
  * producer-side compression is rejected (Kafka error 43) -> compression None
  * a produce request > 1 MB fails as "Unable to bootstrap" -> max_request_size 1 MB
  * drain futures by count AND time, log to stderr, fail fast, preflight probe
  * read env files by parsing KEY=VALUE, never `set -a; . file` (';' truncates)
"""
from __future__ import annotations

import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

from .determinism import rng_for
from .telemetry import CHUNK, MIN, Context, generate

LOGGER = logging.getLogger("p2p.publisher")
BILLING_UNIT = 64 * 1024
MAX_EVENT = 1024 * 1024
MAX_REQUEST = 1_000_000
BUDGET = int(BILLING_UNIT * 0.97)
TOPICS = ("molding-telemetry", "sterilizer-cycles", "dc-asset-telemetry", "fleet-telemetry")
Q = np.array(["G", "U", "B"])
FW_NEW = "4.3.0"


# ---------------------------------------------------------------------------
# Stream: generate, then apply live defects, then order by SEND time
# ---------------------------------------------------------------------------
class LiveStream:
    def __init__(self, ctx: Context, t_start: int, t_end: int, topics=TOPICS,
                 late_frac=0.005, dup_frac=0.001, skew_ms=120_000, fw_minute=20):
        self.ctx, self.t_start, self.t_end = ctx, t_start, t_end
        cfg = ctx.cfg
        adim = ctx.adim[ctx.adim["topic"].isin(topics)]
        site_of = dict(zip(adim["asset_id"], adim["site_id"]))
        topic_of = dict(zip(adim["asset_id"], adim["topic"]))
        self.sites = sorted(adim["site_id"].unique())
        self.topics = list(topics)
        s_idx = {s: i for i, s in enumerate(self.sites)}
        t_idx = {t: i for i, t in enumerate(self.topics)}
        cols = defaultdict(list)
        c0 = (t_start // CHUNK) * CHUNK
        for aid, kind in adim[["asset_id", "asset_kind"]].values:
            for c in range(c0, t_end, CHUNK):
                for tag, (tt, v, q) in generate(ctx, aid, kind, c, c + CHUNK).items():
                    m = (tt >= t_start) & (tt < t_end)
                    if not m.any():
                        continue
                    n = int(m.sum())
                    cols["ts"].append(tt[m]); cols["tag"].append(np.full(n, tag, np.int32))
                    cols["v"].append(v[m]); cols["q"].append(q[m])
                    cols["site"].append(np.full(n, s_idx[site_of[aid]], np.int16))
                    cols["topic"].append(np.full(n, t_idx[topic_of[aid]], np.int8))
                    cols["asset"].append(np.full(n, aid, object))
        a = {k: np.concatenate(v) for k, v in cols.items()}
        rng = rng_for(cfg.seed, "live", "defects", t_start)
        n = len(a["ts"])
        a["true_ts"] = a["ts"].copy()
        a["fw"] = np.zeros(n, bool)
        self.truth = {"late_records": 0, "duplicate_records": 0}

        # 1. firmware 4.3 on the second molding plant (or the first if only one): melt temp gets NEW tag IDs
        plants = sorted(ctx.adim.query("asset_kind == 'molding_machine'")["site_id"].unique())
        fw_plant = plants[1] if len(plants) > 1 else plants[0]
        machines = sorted(ctx.adim.query("asset_kind == 'molding_machine' and site_id == @fw_plant")["asset_id"])
        base = int(ctx.tdim["tag_id"].max()) + 1
        self.fw_map = {ctx.TAG[(m, "melt_temp_c")]: base + i for i, m in enumerate(machines)}
        self.fw_t = ctx.live + fw_minute * MIN
        self.fw_plant = fw_plant
        old = np.isin(a["tag"], list(self.fw_map)) & (a["ts"] >= self.fw_t)
        a["tag"][old] = np.vectorize(self.fw_map.get)(a["tag"][old]) if old.any() else a["tag"][old]
        mach = np.isin(a["asset"], machines) & (a["ts"] >= self.fw_t)
        a["fw"][mach] = True

        # 2. clock-skewed gateway at one DC that is neither the outage DC nor the sorter scenario's DC
        dcs = ctx.w["distribution_centers"]
        busy = {s["asset_id"] for s in ctx.scenarios if s["scenario"] == "dc_outage"} | \
            {ctx.DCA[s["asset_id"]]["dc_id"] for s in ctx.scenarios if s["scenario"] == "bearing_degradation"}
        cand = sorted(set(dcs["dc_id"]) - busy) or sorted(dcs["dc_id"])
        self.skew_site, self.skew_ms = cand[-1], skew_ms
        sk = (a["site"] == s_idx.get(self.skew_site, -1)) & (a["topic"] == t_idx.get("dc-asset-telemetry", -1))
        a["ts"][sk] += skew_ms                       # reported time is ahead of true time

        # 3. send time: on time, except late records (30 s - 10 min) -> out of order
        send = a["true_ts"].copy()
        late = rng.random(n) < late_frac
        send[late] += rng.integers(30_000, 600_000, int(late.sum()))
        self.truth["late_records"] = int(late.sum())
        # 4. duplicates (at-least-once delivery): re-sent 0-5 s later
        dup = np.flatnonzero(rng.random(n) < dup_frac)
        self.truth["duplicate_records"] = len(dup)
        for k in a:
            a[k] = np.concatenate([a[k], a[k][dup]])
        send = np.concatenate([send, send[dup] + rng.integers(0, 5000, len(dup))])
        o = np.argsort(send, kind="stable")
        self.a = {k: v[o] for k, v in a.items()}
        self.send = send[o]
        self.n_generated = n

    def manifest(self) -> dict:
        ctx = self.ctx
        return {"firmware_change": {"plant": self.fw_plant, "at_sim_ms": self.fw_t, "new_version": FW_NEW,
                                    "tag_id_map_old_to_new": {str(k): v for k, v in self.fw_map.items()},
                                    "note": "New melt_temp_c tag IDs are NOT in reference/tag_dim; see reference/tag_dim_fw43.csv"},
                "clock_skew": {"site": self.skew_site, "topic": "dc-asset-telemetry", "reported_ahead_ms": self.skew_ms},
                **self.truth, "records_generated": self.n_generated}

    def tag_dim_fw43(self):
        td = self.ctx.tdim.set_index("tag_id")
        rows = []
        for old, new in self.fw_map.items():
            r = td.loc[old].to_dict()
            r.update(tag_id=new, tag_name=r["tag_name"] + ".fw43", replaces_tag_id=old, firmware=FW_NEW)
            rows.append(r)
        return rows


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
class DryRunSink:
    """Serialises and measures; sends nothing. Settles the cost question before provisioning."""

    def __init__(self, keep_samples=3):
        self.samples, self.keep = [], keep_samples
        self.messages = []          # (topic, key, body) kept for checks

    def send(self, topic, key, body):
        self.messages.append((topic, key, body))

    def close(self):
        pass


class KafkaSink:
    MAX_INFLIGHT = 25
    MAX_DRAIN_INTERVAL_S = 5.0

    def __init__(self, bootstrap: str, connection_string: str, metrics: dict, timeout_s=30.0, fail_fast=True):
        from kafka import KafkaProducer
        if "SharedAccessKey" not in connection_string:
            raise ValueError("connection string has no SharedAccessKey -- probably truncated. Read env files by "
                             "parsing KEY=VALUE (--env-file), not `set -a; . file`.")
        if "EntityPath=" in connection_string:
            raise ValueError("connection string is scoped to ONE event hub (EntityPath=...). The publisher writes "
                             "four hubs; use a namespace-level SAS policy connection string.")
        self.metrics, self.timeout_s, self.fail_fast = metrics, timeout_s, fail_fast
        self._inflight, self._last = [], time.monotonic()
        self._p = KafkaProducer(
            bootstrap_servers=[bootstrap], security_protocol="SASL_SSL", sasl_mechanism="PLAIN",
            sasl_plain_username="$ConnectionString", sasl_plain_password=connection_string,
            key_serializer=lambda k: k.encode(), value_serializer=lambda v: v,
            acks="all", retries=10, retry_backoff_ms=500, linger_ms=50,
            compression_type=None,               # Event Hubs rejects compressed batches (error 43)
            max_request_size=MAX_REQUEST)        # > 1 MB surfaces as "Unable to bootstrap"

    def send(self, topic, key, body):
        self._inflight.append(self._p.send(topic, key=key, value=body))
        if len(self._inflight) >= self.MAX_INFLIGHT or time.monotonic() - self._last >= self.MAX_DRAIN_INTERVAL_S:
            self._drain()

    def _drain(self):
        first = None
        for f in self._inflight:
            try:
                f.get(timeout=self.timeout_s)
            except Exception as exc:  # noqa: BLE001
                self.metrics["send_failures"] += 1
                first = first or exc
                if self.metrics["send_failures"] <= 10:
                    LOGGER.error("send failed (%d): %s", self.metrics["send_failures"], exc)
        self._inflight.clear()
        self._last = time.monotonic()
        if first is not None and self.fail_fast:
            raise RuntimeError(f"Kafka sends failing ({self.metrics['send_failures']}); first error {first!r}") from first

    def close(self):
        try:
            self._drain()
        finally:
            self._p.flush(timeout=30)
            self._p.close(timeout=10)


def preflight(sink: KafkaSink, topics):
    """One tiny probe per hub, resolved immediately: transport faults surface in seconds."""
    for t in topics:
        sink.send(t, "preflight", json.dumps({"src": "p2p-controltower", "preflight": True}).encode())
    sink._drain()


# ---------------------------------------------------------------------------
# Publish loop
# ---------------------------------------------------------------------------
def publish(stream: LiveStream, sink, time_scale=1.0, flush_ms=5000, offset_ms=0, progress_s=60, log=print) -> dict:
    """Send the stream in flush windows of plant time, paced to the wall clock.

    time_scale: plant seconds per wall second (1.0 real time; 0 = as fast as possible).
    offset_ms:  added to every record time (rebase simulated time onto the wall clock).
    """
    a, send = stream.a, stream.send
    m = defaultdict(int)
    per_topic = defaultdict(lambda: defaultdict(int))
    peak = defaultdict(lambda: [0, 0])          # wall-second bucket -> [events, bytes] (for TU sizing)
    lat = []
    t0_plant, wall0 = stream.t_start, time.monotonic()
    vals = np.round(a["v"].astype(float), 4)
    # windows run past t_end until the last late straggler has been sent
    last = max(stream.t_end, int(send.max()) + 1) if len(send) else stream.t_end
    edges = np.searchsorted(send, np.arange(stream.t_start, last + flush_ms, flush_ms))
    last_print = time.monotonic()
    for w in range(len(edges) - 1):
        i0, i1 = edges[w], edges[w + 1]
        flush_at = stream.t_start + (w + 1) * flush_ms
        if time_scale > 0:
            lag = (flush_at - t0_plant) / 1000 / time_scale - (time.monotonic() - wall0)
            if lag > 0:
                time.sleep(lag)
        if i1 <= i0:
            continue
        idx = np.arange(i0, i1)
        key = a["topic"][idx].astype(np.int32) * 10_000 + a["site"][idx]
        for k in np.unique(key):
            sel = idx[key == k]
            topic, site = stream.topics[k // 10_000], stream.sites[k % 10_000]
            recs = []
            for j in sel:
                r = {"t": int(a["ts"][j] + offset_ms), "g": int(a["tag"][j]),
                     "v": None if math.isnan(vals[j]) else float(vals[j]), "q": str(Q[a["q"][j]])}
                if a["fw"][j]:
                    r["fw"] = FW_NEW
                recs.append(r)
            for body, n in _pack(topic, site, recs, offset_ms + flush_at):
                sink.send(topic, site, body)
                units = max(1, math.ceil(len(body) / BILLING_UNIT))
                m["messages"] += 1; m["records"] += n; m["bytes"] += len(body); m["billable_units"] += units
                m["max_message_bytes"] = max(m["max_message_bytes"], len(body))
                pt = per_topic[topic]
                pt["messages"] += 1; pt["records"] += n; pt["bytes"] += len(body); pt["billable_units"] += units
                b = peak[flush_at // 1000]
                b[0] += units; b[1] += len(body)
        if flush_at <= stream.t_end:      # trailing windows hold only late stragglers
            lat.append(float(np.median(flush_at - a["true_ts"][idx])))
        if time.monotonic() - last_print >= progress_s:
            log(f"  plant +{(flush_at - stream.t_start) / 60000:5.1f} min: {m['records']:,} records, "
                f"{m['messages']:,} messages, {m['billable_units']:,} billable units")
            last_print = time.monotonic()
    sink.close()
    span_s = (stream.t_end - stream.t_start) / 1000
    ev_s = np.array([v[0] for v in peak.values()]) / (flush_ms / 1000)
    mb_s = np.array([v[1] for v in peak.values()]) / (flush_ms / 1000) / 1e6
    return {
        **dict(m), "per_topic": {k: dict(v) for k, v in per_topic.items()},
        "plant_seconds": span_s, "wall_seconds": round(time.monotonic() - wall0, 1),
        "records_per_plant_sec": round(m["records"] / span_s, 1),
        "billable_units_per_plant_sec": round(m["billable_units"] / span_s, 3),
        "billing_unit_utilisation_pct": round(100 * m["bytes"] / (m["billable_units"] * BILLING_UNIT), 1) if m["billable_units"] else 0,
        "projected_billable_events_per_month": int(m["billable_units"] / span_s * 86400 * 30),
        "peak_ingress_events_per_s": round(float(ev_s.max()), 2) if len(ev_s) else 0,
        "peak_ingress_mb_per_s": round(float(mb_s.max()), 4) if len(mb_s) else 0,
        "throughput_units_needed": max(1, math.ceil(max(float(ev_s.max()) / 1000 if len(ev_s) else 0,
                                                        float(mb_s.max()) if len(mb_s) else 0))),
        "median_plant_latency_ms": round(float(np.mean(lat)), 0) if lat else 0,
        "flush_ms": flush_ms, "time_scale": time_scale, "send_failures": m.get("send_failures", 0),
    }


def _pack(topic, site, recs, sent_ms):
    """Pack records into envelopes under the byte budget (measured, not estimated)."""
    head = f'{{"src":"p2p-controltower","topic":"{topic}","site":"{site}","sent_ms":{int(sent_ms)},"n":'
    parts, size, out = [], 0, []
    for r in recs:
        s = json.dumps(r, separators=(",", ":"))
        if parts and size + len(s) + 1 + len(head) + 24 > BUDGET:
            out.append(parts)
            parts, size = [], 0
        parts.append(s)
        size += len(s) + 1
    if parts:
        out.append(parts)
    return [((head + f'{len(p)},"records":[' + ",".join(p) + "]}").encode(), len(p)) for p in out]


def utc_now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
