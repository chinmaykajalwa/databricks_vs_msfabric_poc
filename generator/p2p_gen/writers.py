"""
Writers: Parquet (+ optional CSV) and a run manifest.
=====================================================
Parquet, not Delta, so the generator runs anywhere without Spark. Both
platforms load the same files: Databricks via a UC Volume, Fabric via a
Lakehouse Files folder or a OneLake shortcut.
"""
from __future__ import annotations

import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import P2PConfig
from .determinism import table_digest


def write_tables(cfg: P2PConfig, tables: dict[str, pd.DataFrame], domain: str) -> dict:
    out = Path(cfg.out_root) / domain
    out.mkdir(parents=True, exist_ok=True)
    info = {}
    for name, df in sorted(tables.items()):
        path = out / f"{name}.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="snappy")
        if cfg.write_csv:
            df.to_csv(out / f"{name}.csv", index=False)
        info[name] = {"rows": len(df), "columns": len(df.columns), "digest": table_digest(df),
                      "parquet_bytes": path.stat().st_size, "path": str(path)}
    return info


def write_manifest(cfg: P2PConfig, domain: str, payload: dict) -> str:
    path = Path(cfg.out_root) / domain / "_manifest.json"
    doc = {"domain": domain,
           "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
           "python": platform.python_version(), "pandas": pd.__version__, "pyarrow": pa.__version__,
           "config": cfg.manifest(), **payload}
    path.write_text(json.dumps(doc, indent=2, default=str))
    return str(path)


def write_data_dictionary(cfg: P2PConfig, tables: dict[str, pd.DataFrame], domain: str,
                          descriptions: dict[str, str] | None = None) -> str:
    """Markdown data dictionary: per column dtype, null %, distinct count, example.

    Generated from the data itself, so it can never drift from what was written.
    """
    descriptions = descriptions or {}
    lines = [f"# Data dictionary: `{domain}`", "",
             f"Profile `{cfg.profile}`, seed `{cfg.seed}`. All names and entities are FICTIONAL.", ""]
    for name, df in sorted(tables.items()):
        lines += [f"## `{name}` ({len(df):,} rows)", ""]
        if name in descriptions:
            lines += [descriptions[name], ""]
        lines += ["| column | dtype | null % | distinct | example |", "|---|---|---|---|---|"]
        for col in df.columns:
            s = df[col]
            ex = s.dropna().iloc[0] if s.notna().any() else ""
            ex = str(ex).replace("|", "/")[:48]
            lines.append(f"| `{col}` | {s.dtype} | {100 * s.isna().mean():.0f} | {s.nunique(dropna=True):,} | {ex} |")
        lines.append("")
    path = Path(cfg.out_root) / domain / "_data_dictionary.md"
    path.write_text("\n".join(lines))
    return str(path)
