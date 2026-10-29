#!/usr/bin/env python
"""
Render the agent-ready layer for both platforms from platforms/shared/agent_layer.py,
and (with --test) execute every table's SQL in DuckDB against generated data and check
the hero answers derivable from the curated tables against the ground truth.

    python platforms/shared/build_agent_notebooks.py
    python platforms/shared/build_agent_notebooks.py --test /path/to/generator/output/lite
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from agent_layer import SOURCES, TABLES  # noqa: E402

PLATFORMS = HERE.parent
DBX_OUT = PLATFORMS / "databricks" / "agents" / "10_agent_ready_layer.py"
FAB_OUT = PLATFORMS / "fabric" / "notebooks" / "03_agent_ready_layer.py"
DESC_OUT = PLATFORMS / "shared" / "agent_table_descriptions.md"
EX_OUT = PLATFORMS / "shared" / "agent_examples.md"

APPLY = '''
def sql_str(s):
    return "'" + s.replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"

for t in TABLES:
    target = target_name(t["name"])
    body = t["sql"].format(**SRC)
    spark.sql(f"CREATE OR REPLACE TABLE {target} COMMENT {sql_str(t['description'])} AS {body}")
    for col, comment in t["columns"].items():
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{col}` COMMENT {sql_str(comment)}")
    print(f"{target:<45} {spark.table(target).count():>10,} rows")
'''


def render_databricks() -> str:
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Agent-ready layer (Genie Agent data model)
# MAGIC GENERATED from `platforms/shared/agent_layer.py` -- edit that file, then run
# MAGIC `python platforms/shared/build_agent_notebooks.py`. Identical to Fabric's `03_agent_ready_layer`.
# MAGIC
# MAGIC Creates `<catalog>.agent.*`: nine curated tables with table and column comments (Unity Catalog), which
# MAGIC the Genie Agent uses as its semantic context. Run after `01_ingest_landing`.

# COMMAND ----------

# MAGIC %run ../notebooks/00_config

# COMMAND ----------

import json
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.agent COMMENT 'Curated, documented tables for AI agents'")
TABLES = json.loads(r\'\'\'{json.dumps(TABLES, indent=1)}\'\'\')
SRC = {{name: f"{{BRONZE}}.{{name}}" for name in {json.dumps(SOURCES)}}}

def target_name(name):
    return f"{{CATALOG}}.agent.{{name}}"
{APPLY}'''


def render_fabric() -> str:
    return f'''# Fabric notebook (PySpark). Attach the Lakehouse loaded by 01_load_landing_to_lakehouse.
# 03 · Agent-ready layer (Fabric data agent data model)
# GENERATED from platforms/shared/agent_layer.py -- edit that file, then run
# python platforms/shared/build_agent_notebooks.py. Identical to Databricks' agents/10_agent_ready_layer.
#
# Creates agent_* Delta tables (visible in the Lakehouse SQL analytics endpoint, which the data agent
# queries). Column comments are stored in Delta; paste platforms/shared/agent_table_descriptions.md into the
# data agent's data-source instructions as well, since the agent may not read Delta comments.

import json
TABLES = json.loads(r\'\'\'{json.dumps(TABLES, indent=1)}\'\'\')
SRC = {{name: name for name in {json.dumps(SOURCES)}}}

def target_name(name):
    return f"agent_{{name}}"
{APPLY}'''


def render_descriptions() -> str:
    out = ["# Agent-ready tables: descriptions",
           "",
           "Paste into the Fabric data agent's **data source instructions** for the Lakehouse (Databricks reads the",
           "same text from Unity Catalog comments). Table names: Databricks `<catalog>.agent.<name>`, Fabric `agent_<name>`.",
           ""]
    for t in TABLES:
        out += [f"## {t['name']}", "", t["description"], ""]
        out += [f"- `{c}`: {d}" for c, d in t["columns"].items()]
        out.append("")
    return "\n".join(out)


def render_examples() -> str:
    from example_queries import KQL_EXAMPLES, render
    out = ["# Example questions and queries for the agents", "",
           "GENERATED from `example_queries.py`. They use chamber STZ-P02-01 / DC01 on purpose: they teach the",
           "agent the patterns without giving away the hero answers it is scored on.", ""]
    for title, dialect, lang in (("Databricks Genie Agent (Spark SQL)", "spark", "sql"),
                                 ("Fabric data agent, Lakehouse source (T-SQL)", "tsql", "sql")):
        out += [f"## {title}", ""]
        for q, sql in render(dialect):
            out += [f"**{q}**", "", f"```{lang}", sql, "```", ""]
    out += ["## Fabric data agent, Eventhouse source (KQL)", ""]
    for q, kql in KQL_EXAMPLES:
        out += [f"**{q}**", "", "```kusto", kql, "```", ""]
    return "\n".join(out)


# ---------------------------------------------------------------------------
# DuckDB test harness
# ---------------------------------------------------------------------------
def duck_sql(sql: str) -> str:
    return re.sub(r"INTERVAL (\d+) (MINUTES|DAYS)", lambda m: f"INTERVAL '{m.group(1)} {m.group(2).lower()}'", sql)


def test(out_dir: Path) -> int:
    import duckdb
    con = duckdb.connect()
    paths = {s: out_dir / "transactions" / (s[4:] + ".parquet") for s in SOURCES if s.startswith("txn_")}
    paths.update({s: out_dir / "master" / (s[7:] + ".parquet") for s in SOURCES if s.startswith("master_")})
    paths.update({"calibration_records": out_dir / "documents/records/calibration_records.parquet",
                  "ref_tag_dim": out_dir / "reference/tag_dim.parquet"})
    for s, p in paths.items():
        con.execute(f"CREATE VIEW {s} AS SELECT * FROM read_parquet('{p}')")
    con.execute(f"CREATE VIEW telemetry_agg_history AS SELECT * FROM read_parquet('{out_dir}/telemetry/agg/**/*.parquet', hive_partitioning = true)")
    src = {s: s for s in SOURCES}
    for t in TABLES:
        con.execute(f"CREATE TABLE {t['name']} AS {duck_sql(t['sql'].format(**src))}")
        cols = [r[0] for r in con.execute(f"DESCRIBE {t['name']}").fetchall()]
        assert cols == list(t["columns"]), f"{t['name']}: columns {cols} != documented {list(t['columns'])}"
        n = con.execute(f"SELECT count(*) FROM {t['name']}").fetchone()[0]
        print(f"  {t['name']:<24} {n:>9,} rows, columns match docs")

    gt = json.loads((out_dir / "ground_truth/hero_blast_radius.json").read_text())
    hero = gt["sterilizer_id"]
    con.execute(f"""CREATE TABLE suspect AS
        SELECT l.load_id FROM sterilization_loads l
        WHERE l.sterilizer_id = '{hero}' AND l.cycle_start_ts >= (
          SELECT max(performed_ts) FROM calibration_history
          WHERE asset_id = '{hero}' AND instrument = 'chamber_rh_pct' AND result = 'PASS')""")
    con.execute("CREATE TABLE s_lots AS SELECT DISTINCT lot_id FROM load_lots JOIN suspect USING (load_id) WHERE lot_id IS NOT NULL")
    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    ex = "SELECT {} FROM open_order_exposure JOIN s_lots USING (lot_id)"
    got = {
        "suspect loads": q("SELECT count(*) FROM suspect"),
        "suspect lots": q("SELECT count(*) FROM s_lots"),
        "on-hand cases": q("SELECT sum(cases) FROM lot_locations JOIN s_lots USING (lot_id) WHERE location_type = 'dc_on_hand'"),
        "in-transit cases": q("SELECT coalesce(sum(cases),0) FROM lot_locations JOIN s_lots USING (lot_id) WHERE location_type = 'in_transit'"),
        "delivered cases": q("SELECT coalesce(sum(cases),0) FROM lot_locations JOIN s_lots USING (lot_id) WHERE location_type = 'delivered'"),
        "open orders": q(ex.format("count(DISTINCT order_id)")),
        "customers at risk": q(ex.format("count(DISTINCT customer_id)")),
        "hospitals at risk": q(ex.format("count(DISTINCT CASE WHEN customer_type = 'hospital' THEN customer_id END)")),
        "IDNs at risk": q(ex.format("count(DISTINCT idn_id)")),
        "RH probe precedents": q(f"SELECT count(DISTINCT deviation_id) FROM deviations_and_capas WHERE asset_id = '{hero}' AND type_key = 'eto_rh_probe_drift'"),
    }
    want = {
        "suspect loads": len(gt["suspect_loads"]), "suspect lots": len(gt["suspect_lots"]),
        "on-hand cases": gt["on_hand_at_dcs"]["cases"], "in-transit cases": gt["in_transit"]["cases"],
        "delivered cases": gt["already_delivered"]["cases"],
        "open orders": len(gt["open_orders_at_risk"]["orders"]), "customers at risk": len(gt["open_orders_at_risk"]["customers"]),
        "hospitals at risk": len(gt["open_orders_at_risk"]["hospitals"]), "IDNs at risk": len(gt["open_orders_at_risk"]["idns"]),
        "RH probe precedents": 3,
    }
    ok = True
    for k in want:
        good = got[k] == want[k]
        ok &= good
        print(f"  {'OK ' if good else 'BAD'} {k:<22} curated {got[k]:>6}   ground truth {want[k]:>6}")
    # Conservation: plant + in transit + DC + delivered == lot quantity for released/quarantined make lots
    bal = con.execute("""
        SELECT count(*) FROM (
          SELECT l.lot_id, l.qty_cases, sum(x.cases) AS located FROM txn_lots l JOIN lot_locations x USING (lot_id)
          WHERE l.source = 'make' AND l.status <> 'rejected' GROUP BY l.lot_id, l.qty_cases) WHERE located <> qty_cases""").fetchone()[0]
    print(f"  {'OK ' if bal == 0 else 'BAD'} lot_locations conserves every make lot's cases ({bal} mismatches)")
    ok &= bal == 0
    steam = con.execute(f"""SELECT min(reported_rh_exposure_pct), max(reported_rh_exposure_pct), min(steam_vs_baseline_pct)
        FROM steam_vs_rh_per_load WHERE load_id IN (SELECT load_id FROM suspect)""").fetchone()
    flat = steam[1] - steam[0] < 1.5 and steam[2] <= -15
    print(f"  {'OK ' if flat else 'BAD'} suspect loads: reported RH {steam[0]}-{steam[1]} (flat), steam down to {steam[2]}% vs baseline")
    ok &= flat
    print("TEST", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", type=Path, help="generator output dir (e.g. generator/output/lite) to test the SQL against")
    a = ap.parse_args()
    DBX_OUT.parent.mkdir(parents=True, exist_ok=True)
    DBX_OUT.write_text(render_databricks())
    FAB_OUT.write_text(render_fabric())
    DESC_OUT.write_text(render_descriptions())
    EX_OUT.write_text(render_examples())
    print(f"rendered {DBX_OUT.relative_to(PLATFORMS)}, {FAB_OUT.relative_to(PLATFORMS)}, {DESC_OUT.relative_to(PLATFORMS)}")
    return test(a.test) if a.test else 0


if __name__ == "__main__":
    raise SystemExit(main())
