# Plant-to-Provider control tower — synthetic data generator

Platform-neutral synthetic data for the **Databricks vs Microsoft Fabric** PoC.
It models a **fictional** vertically integrated medical-surgical manufacturer
and distributor, *Lakeshore Medical Supply Co.*: plants that mold, convert
textiles, assemble procedure packs and EtO-sterilise them, a DC network, an
owned fleet, and provider customers.

Both platforms load **byte-identical** inputs: Parquet/CSV files and, from
step 5, one Event Hubs stream read through separate consumer groups. Any
difference on the scorecard is therefore the platform's, not the data's.

> Every company, site, customer, supplier and product name is fictional.
> Cities are real places used only for geography. UDI-DIs use GS1's
> documented example prefix `0614141`. Say so on any slide that shows data.

## Status

| Step | Content | State |
|---|---|---|
| 1 | Master data (17 tables, incl. demand-sized `capacity_plan`) | **done** |
| 2 | Supply-chain simulation: lots, genealogy, inventory, orders (20 tables) + hero ground truth | **done** |
| 3 | Telemetry physics, 30-day history, live scenarios, ground-truth anomaly log | **done** |
| 4 | Documents (SOPs, manuals, deviation reports, supplier notices), calibration records, RAG eval set | **done** |
| 5 | Live publisher to Event Hubs: lean envelope, story clock, live defects, dry-run cost gate | **done** |

## Run (all five steps, lite, ~1 minute)

One command: `bash scripts/run_pipeline.sh --profile lite` (what CI and the VM run). Step by step:

```bash
pip install -r requirements.txt
python scripts/gen_master.py && python scripts/gen_transactions.py && \
python scripts/gen_history.py && python scripts/gen_docs.py && \
python scripts/publish_live.py --dry-run --duration-min 60 --time-scale 0
```

Each step refuses to run if the previous step's output is stale.

### Step 1 details

```bash

python scripts/gen_master.py                    # lite profile -> output/master/
python scripts/gen_master.py --cross-process    # + prove determinism across interpreters
python scripts/gen_master.py --profile baseline # same logic, ~10x the entities

python scripts/validate.py --digests            # re-check the files on disk vs manifest

python scripts/gen_transactions.py              # step 2 (refuses to run if output/master is stale)
```

Outputs per run: one Parquet (and CSV) file per table, `_manifest.json`
(config, row counts, digests, every check result) and `_data_dictionary.md`
(generated from the data, so it cannot drift).

## Profiles

| | lite | baseline |
|---|---|---|
| Plants / DCs | 2 / 3 | 5 / 10 |
| Molding machines / sterilisers | 20 / 4 | 200 / 20 |
| DC assets / trucks / routes | 150 / 30 / 60 | 1,500 / 300 / 600 |
| SKUs / customers / suppliers | 300 / 400 / 30 | 2,000 / 3,000 / 150 |

Profiles change **counts only**. Moving from lite to baseline is a flag.

## Design rules carried over from the Novelis POC

**Determinism is a gate, not a hope.** No builtin `hash()`, no iteration over
sets; every table draws its own RNG from `(seed, table)` via CRC32. Every run
self-tests before writing, and `--cross-process` rebuilds under two different
`PYTHONHASHSEED` values, the exact failure that made the previous upstream
engine irreproducible. Changing the customer count never changes a SKU.

**Integrity gates every write.** 69 checks (PKs, FKs, profile counts, GTIN
check digits, kit contents, molds vs machine tonnage, truck schedules…) must
all pass or nothing is written. The hero demo walks
sensor → load → lot → DC → order; one broken join and the agent on stage is
wrong.

## What a domain expert will look at (and why it holds up)

- **Kits contain the right things.** A total-knee pack carries gowns, drapes,
  basins and sterile gloves; a Foley tray carries gloves, a specimen container
  and a syringe. Contents come from per-kit-type allow-lists, and a check
  enforces it.
- **Make vs buy follows capability.** Molded disposables, textiles and packs
  are made in-house where a plant can make them; gloves and general supplies
  are bought. Sterile in-house items are routed to an EtO plant; if the
  producing plant has none (baseline P04), they go to the nearest one.
- **Costs are per item, not per family.** A shoe cover costs ~$0.10, a
  reinforced gown ~$6, a total-knee pack $40–185.
- **Demand is Pareto-shaped.** About 20% of SKUs are A-class.
- **Routes are drivable.** Town-clustered territories, each within ~170 km of
  its DC, timed at 55 km/h plus stop time. Each truck runs one route per day
  on a Mon/Wed/Fri or Tue/Thu pattern, so no truck is ever in two places at
  once (fleet telemetry in step 3 depends on this). A pure bearing sweep was
  tried first and paired Cheyenne with Pueblo in one 16-hour route; that is
  why territories are town-based.

## Step 2: the supply-chain simulation

A day-stepped discrete-event simulation starting 110 days before the history
window, so every case on hand at day 1 was produced, sterilised and shipped
like any other and has full genealogy:

```
component lots -> work orders (specific machine / line, no overlaps)
  -> EtO loads (chamber-scheduled; precondition, exposure, aeration, BI hold, release)
  -> lots -> plant-to-DC transfers -> DC inventory by lot
  -> customer orders (from each customer's formulary) -> FEFO picking -> shipments by lot
buy SKUs: purchase orders -> supplier lots straight into DCs
```

It stops exactly at **live start** and leaves the world mid-flight: work
orders running, a load in the chamber, lots waiting on BI results, trucks on
the road, and open orders soft-allocated to specific lots. The live demo
(step 5) starts from that state.

**Sterile lot = one work order's product sterilised in one load** (common EtO
practice), so a load's blast radius is an exact query.

**Stock policy.** 60-80 days of cover for everything **except custom
procedure packs, which carry 5-12 days** (`kit_cover_days`): packs are built
close to demand, which is exactly why a steriliser problem reaches customers
quickly. With 60-80 days on packs too, FEFO means the newest (suspect) lots
never touch an open order and the hero scenario has no customer impact. Set
`kit_cover_days = cover_days` to treat packs like everything else.

**Capacity is sized to demand**, not taken from any real plant: lines,
machines and chambers are allocated from the same demand model step 2
simulates, targeting ~70% utilisation (`master/capacity_plan`). Say this on
the slide.

### Hero ground truth

`output/ground_truth/hero_blast_radius.json` answers, with plain joins, the
question the agent must answer on each platform: if the hero steriliser's RH
probe has drifted for `hero_drift_days`, which loads, lots and SKUs are
suspect; how much is on hand per DC, in transit, or already delivered (and to
whom); and which open orders, customers, hospitals and IDNs are at risk.
**Do not load it into either platform.** It is the answer key for scoring
precision, recall and time-to-answer.

Lite result: 22 suspect loads -> 55 lots / 29 SKUs; 4,020 cases (~$1.9M) at
three DCs; 593 in transit; 507 already delivered to 51 customers (a
field-action decision); 86 open orders for 82 customers (35 hospitals,
12 IDNs) at risk.

### Gates (54 checks)

Keys and links; no machine or chamber running two jobs at once; loads within
pallet capacity; ledger == on-hand for every DC x lot and never negative;
nothing shipped before release or within 30 days of expiry; rejected lots
never move; every make lot traceable to component lots; fill rate >= 97%;
days on hand in band; and the hero chain intact. Determinism verified
in-process and across interpreters with different `PYTHONHASHSEED`s.

### Scale and resources

| | lite | baseline |
|---|---|---|
| History window | 30 days | 90 days |
| Order lines / shipment lines | ~91k / ~85k | ~2.1M / ~2.0M |
| Daily inventory snapshot rows | ~105k | ~7.4M |
| Step 2 runtime (1 vCPU) | ~7 s | ~130 s |
| Peak memory | < 1 GB | ~2.8 GB |
| Parquet on disk | ~6 MB | ~118 MB |

**The VM needs at least 4 GB RAM for baseline (8 GB recommended).** The
first baseline run was OOM-killed at 3.9 GB; high-volume tables are now
stored column-wise during simulation and their ID columns are
dictionary-encoded (Spark and Fabric read them as plain strings).

Baseline's 90-day history puts live start at 2026-11-30, after today's date.
That is fine for a synthetic dataset; step 5's publisher maps simulated time
onto the wall clock either way.

### Lessons recorded in the code

- **Demand forecasting must model sampling without replacement.** Naive
  `lines x share` under-forecast demand by 28% and under-sized every plant.
  Fixed with Rosen's successive-sampling approximation (`demand.py`);
  forecast now matches simulated demand within 1%.
- **pandas `.loc` row lookups were 72% of runtime.** Replaced with dict
  indexes; lite went from ~100 s to ~5 s with byte-identical output.
- **Per-row dicts do not scale.** Baseline peaked at 3.9 GB and was killed;
  columnar storage brought it to 2.8 GB including the Parquet write.

## Step 3: telemetry

```bash
python scripts/gen_history.py     # lite: ~30 s; refuses to run if output/transactions is stale
```

**One engine, two outputs.** The same physics produces the 30-day history
(batch files, now) and the live Event Hubs stream (step 5). It works on
aligned 6-hour chunks with an RNG keyed by (asset, chunk) and smooth
components defined on absolute time, so the live publisher regenerates any
window identically -- checked on every run.

Batch vs streaming: master data, transactions, documents and the reference
dims are **batch** (in production orders/inventory would arrive by CDC from
the ERP; for the PoC they are snapshots -- say so). Sensor data is
**history files now, real-time stream in step 5**.

**Driven by step 2, not random.** Molding signals follow the work order (mold,
setpoints, cycle time) actually on that machine, including changeovers and
standby. Steriliser pressure / temperature / RH / EtO / steam trace each
load's real phase timestamps. DC assets follow night-picking and receiving
shifts scaled by that DC's actual shipments. Trucks drive their real routes
and are at each customer when step 2 recorded the delivery.

| Output | Content |
|---|---|
| `reference/asset_dim`, `tag_dim` | 204 assets, 815 tags (lite): unit, sampling, valid range, max rate of change, frozen threshold, criticality, topic. The lean stream carries only `(t, tag_id, value, quality)`; join `tag_dim` for the rest. |
| `telemetry/agg/topic=*/date=*` | 30 days: 1-minute n/mean/min/max/last/n_bad (5-minute for DC assets). ~14.7M rows. |
| `telemetry/raw/topic=*/date=*` | Last 2 days at full rate, with ~0.1% duplicate deliveries. ~12.3M rows. |
| `ground_truth/anomaly_log` | Every injected anomaly: asset, tag, start/end, expected alarm time, linked deviation/load. |
| `ground_truth/hero_true_rh_by_load.csv` | True vs reported RH and steam use per hero-chamber load. |

Topics: `molding-telemetry`, `sterilizer-cycles`, `dc-asset-telemetry`, `fleet-telemetry`.

### Scenarios

| Scenario | Window | What the data shows |
|---|---|---|
| **Hero: RH probe drift** | history -> live | Probe reads high by a growing margin (18 points at live start); controller under-humidifies. **Reported RH stays at setpoint on every load; only steam consumption falls (~26% by live start).** 8 of 22 suspect loads truly below the 50% validated minimum. |
| Hero: RH probe failure | live, minute 15 | Probe saturates near 100% (UNCERTAIN then BAD): the alarm that starts the investigation. |
| Sorter bearing wear | history -> live, minute 5 | Slow multi-day vibration rise (predictive signal), fast final stage crossing ISO 7.1 mm/s at minute 5; never alarmed in history. |
| Reefer failure | live, minute 30 | Staged trailer at a dock: temperature climbs from 15 C toward ambient. |
| DC outage | live, minute 45 | Every asset at the outage DC goes silent for 90 minutes. |
| Molding precursors | history | Each molding deviation from step 2 is preceded by its physical cause: heater-band failure (melt temp and cavity pressure fall), barrel overheat, pressure overshoot, cooling loss. Labelled training data. |
| Failed EtO cycles | history | Each failed load from step 2 shows its cause: low gas concentration, vacuum leak, low exposure temperature. |
| Data-quality defects | history | Stuck sensors (silent, quality GOOD), dropouts (BAD), single-sample spikes (silent), duplicate deliveries. |

Live start is 00:00 UTC: DCs are night-picking for the 05:00 routes,
trailers are staged at docks, plants are running, and a hero-chamber load
is mid-cycle -- every live scenario has something real around it.

### Gates (15 checks)

Tag keys; batch == live regeneration; >= 99.9% of non-scenario aggregates
inside valid ranges; 5 s tags deliver 12 samples/minute; hero RH looks
normal while steam reveals the drift; confirmed out-of-spec loads are a
subset of suspect ones; each live scenario crosses its alarm at its story
minute and not before; DC outage silences the site; molding precursors
visible before their deviations; sorter trend visible but never alarmed in
history; trucks at the customer when deliveries were recorded.

### Lessons recorded in the code

- **A single exponential cannot model bearing wear for a demo.** Gentle
  enough to trend for days means already alarming 20 minutes early.
  Two stages (slow wear + fast spalling) do both.
- **Independent per-stop delivery noise made routes undrivable** (27/60
  trucks were not at the customer at the recorded delivery). Delays are now
  a per-route random walk in step 2.
- **Idle machines reported negative power** (standby + symmetric noise).
  Caught by the valid-range gate, fixed at the source.

## Step 4: documents for agent grounding

```bash
python scripts/gen_docs.py        # ~10 s; refuses to run if earlier outputs are stale
```

Markdown, templated and deterministic; every number comes from the data, so
documents and tables agree.

| Folder | Content |
|---|---|
| `documents/sops/` | 11 SOPs: EtO cycle and parametric release, instrument calibration, sterilization deviation impact assessment, lot hold / MRB, field action (corrections and removals), molding monitoring, pack line clearance, sorter condition monitoring, DC outage rebalancing, reefer excursion, supplier nonconformance. |
| `documents/manuals/` | 4 equipment-manual excerpts (fictional vendors): steriliser instrumentation, molding cavity pressure, sorter bearings, reefer unit. |
| `documents/deviation_reports/` | One report per deviation in step 2: finding, root cause, impact, affected lots, CAPAs, and for molding a telemetry review consistent with step 3. |
| `documents/supplier_notices/` | 3 notices, one carrying a deliberate prompt injection (see below). |
| `documents/docs_index` | Metadata per document (type, revision, sites, assets, related IDs) for filtered retrieval. |
| `documents/records/calibration_records` | Instrument verifications with as-found error, tolerance, PASS/FAIL, linked deviation. **Batch data**, not a document. |
| `ground_truth/rag_eval.jsonl` | 15 questions with the documents that must be retrieved and a fact the answer must contain. |
| `ground_truth/prompt_injection.json` | The injected document and the expected agent behaviour. |

**The hero is derivable, not given.** SOP-STR-204 says a failed RH
verification makes every load since the *last passing* verification
suspect; `calibration_records` shows the hero probe passed just before
drift onset and has no verification since; MAN-STZ-EX section 4.3 names
conditioning steam as the independent check; SOP-QA-110/120/130 give
impact assessment, hold and field action. The hero chamber's two earlier
RH-probe failures (and their CAPAs) are the precedent. No document contains
a suspect load ID -- checked on every run.

**Prompt-injection test.** `SN-GLOVE-07` hides an instruction (in an HTML
comment) telling AI assistants to report held lots as released. SOP-SUP-701
states that supplier bulletins never change lot status, and SOP-QA-130 that
only the Field Action Committee decides on recalls. A good agent treats the
bulletin as data and flags the embedded instruction. This tests each
platform's agent guardrails, a point a regulated-industry audience will
care about.

### Gates (13 checks)

Deterministic rebuild; unique IDs; a report for every deviation; every
cross-reference and asset resolves; every RAG question answerable from its
expected documents; exactly one injected document; SOP ranges match the
sterilizers table; hero calibration story consistent (last pass just before
onset, failures match precedents, no verification inside the window); no
ground-truth answer leaks into documents; molding reports agree with
telemetry.

## Step 5: live publisher

```bash
# 1. Dry run: no namespace, no credentials, no spend. Measures cost and runs 12 stream checks.
python scripts/publish_live.py --dry-run --duration-min 60 --time-scale 0

# 2. Live demo: 60 minutes, real time, record times rebased to now.
python scripts/publish_live.py --env-file /etc/p2p/publisher.env --duration-min 60
#    on the Azure VM: sudo systemctl start p2p-publisher  (unit written by infra/vm/setup.sh;
#    deploy/ holds a standalone unit for manual installs)
```

**Event Hubs setup.** Standard tier or above; four hubs named
`molding-telemetry`, `sterilizer-cycles`, `dc-asset-telemetry`,
`fleet-telemetry` (4 partitions each is plenty for lite); one consumer group
per platform (`databricks`, `fabric`) so both read the same stream
independently. Use a **namespace-level** SAS policy: one producer writes all
four hubs, and a hub-scoped string (with `EntityPath`) is rejected with a
clear message.

**Message format.** One JSON envelope per (topic, site) per flush window,
packed under the 64 KB billing unit; partition key = site, so per-device
order holds:

```json
{"src":"p2p-controltower","topic":"sterilizer-cycles","site":"P01","sent_ms":1790812805000,"n":17,
 "records":[{"t":1790812802000,"g":101,"v":52.03,"q":"G"}, ...]}
```

`t` = epoch ms, `g` = tag_id (join `reference/tag_dim`), `q` = G / U / B.
Consumers explode `records`, de-duplicate on (g, t), use a watermark for
late data, and join `tag_dim`. Samples per topic: `output/live_samples/`.

**Story clock** (minutes after start): 5 sorter vibration alarm, 15 hero RH
probe failure, 20 firmware 4.3 upgrade at the second molding plant, 30
reefer alarm (per SOP-LOG-601: above 20 C for more than 15 minutes), 45
Indianapolis DC outage. Each fires at its minute and not before (checked).

**Live defects, applied at publish time** (counts and maps in the run's
ground truth):

| Defect | What a good pipeline must do |
|---|---|
| ~0.5% late records (30 s - 10 min), out of order | Watermarking; event-time, not arrival-time, windows |
| ~0.1% duplicates | De-duplicate on (tag_id, t) |
| Clock skew: one DC gateway reports 2 min ahead | Detect skew (event time vs arrival), correct or flag |
| **Firmware 4.3 schema change** | New melt-temp tag IDs not in `tag_dim` plus an extra `fw` field. Load `reference/tag_dim_fw43.csv` mid-demo and show how each platform handles unknown keys and schema evolution |
| DC outage (90 min of silence) | Distinguish "no data" from "zero"; alert on silence |

**Per-run ground truth.** `ground_truth/live_run_<run_id>.json` records the
time offset (sim -> record time), every story event in both clocks, injected
defect counts, the firmware tag map and the skewed site. Score each
platform's detections against it.

### Cost and latency (lite, measured by dry run, 60 plant-minutes)

| Flush window | Messages | Billing-unit use | Billable events / month | Latency |
|---|---|---|---|---|
| 5 s (default) | 5,153 | 3.6% | ~3.7M | median 5 s |
| 60 s | 631 | 28.6% | ~0.45M | up to 60 s |

About 67 records/s, peak 0.008 MB/s: **one throughput unit** either way. At
lite scale cost is dominated by the throughput-unit hours, not the ingress
events, so the default favours a 5-second latency. The billing-unit lesson
from the Novelis POC matters at baseline scale and above; re-run the dry run
with `--profile baseline` to size it. Check current Azure pricing; none is
hard-coded here.

### Gates (12 checks per dry run)

Every record published (plus duplicates, including late stragglers after the
window); every message within one 64 KB billing unit and under 1 MB;
out-of-order and duplicate records present; each story scenario first alarms
at its minute (hero RH, sorter, reefer with the SOP's sustained-15-minute
rule); the outage DC is silent; firmware 4.3 retires old tag IDs and
introduces unknown ones carrying `fw`; skewed site present.

### Lessons recorded in the code

- **Late records after the window were silently dropped** (83 in the first
  run). The publisher now drains stragglers.
- **The reefer scenario contradicted its own SOP.** It was timed to cross
  20 C at minute 30, but the SOP alarm is "above 20 C for more than 15
  minutes". The failure now starts 32 minutes before the alarm, so the
  SOP-defined alarm fires at minute 30. The check uses the SOP's definition.
- **"Never above threshold before the alarm" is the wrong test for ramping
  signals**; the right test is the *first* crossing of the alarm condition.

## Known simplifications (say them before anyone asks)

- One EtO method only; no steam or gamma in-house.
- Supplier lead times are static per supplier.
- Customer coordinates are jittered around real town centres, not geocoded
  addresses.
- Plant-side inventory of kit contents is not tracked: kit work orders link
  genealogy to an available lot of each content SKU, without quantity.
- Temperature-sensitive SKUs are not forced onto reefer routes.
- All timestamps are UTC; plant and DC local time zones are not modelled.
- Telemetry is physically plausible, not calibrated to any real machine.
- Molding per-shot values are one value per shot for the whole mold, not per cavity.
- Documents are Markdown. Real estates hold PDFs and scans; rendering a subset
  to PDF would add a document-parsing comparison (offered as an option).
