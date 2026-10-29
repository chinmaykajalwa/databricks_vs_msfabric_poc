#!/usr/bin/env python
"""
Index the generated documents into Azure AI Search (for the Fabric data agent's
unstructured-data source; Databricks can use the same index or its own document support).

    # on the VM (managed identity, no keys):
    python scripts/index_documents.py --out /opt/p2p/output/lite --endpoint https://<svc>.search.windows.net
    # locally, inspect the chunks without touching Azure:
    python scripts/index_documents.py --out output --dry-run

One search document per section ("## ...") of each Markdown file, long sections split further,
each chunk prefixed with the document title so it stands on its own. The raw Markdown is indexed
unchanged -- including the HTML comment in SN-GLOVE-07 that carries the deliberate prompt
injection, so the guardrail test reaches the agent exactly as designed.

The index is recreated on every run (idempotent; documents are regenerated deterministically).
Ground truth is never indexed: only output/<profile>/documents/ is read.
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import pandas as pd

MAX_CHARS = 2000


def chunks(meta: dict, text: str) -> list[dict]:
    body = text
    parts = re.split(r"(?m)^(## .+)$", body)
    sections = [("Overview", parts[0])] + [(parts[i][3:].strip(), parts[i + 1]) for i in range(1, len(parts) - 1, 2)]
    out = []
    for title, sec in sections:
        sec = sec.strip()
        if not sec:
            continue
        pieces = [sec[i:i + MAX_CHARS] for i in range(0, len(sec), MAX_CHARS)] or [sec]
        for k, piece in enumerate(pieces):
            out.append({
                "id": f"{meta['doc_id']}--{len(out):03d}",
                "doc_id": meta["doc_id"], "doc_type": meta["doc_type"], "title": meta["title"],
                "section": title + (f" (part {k + 1})" if len(pieces) > 1 else ""),
                "content": f"{meta['title']} | {title}\n\n{piece}",
                "revision": str(meta["revision"]), "effective_date": str(meta["effective_date"]),
                "asset_ids": [x for x in str(meta["asset_ids"] or "").split(",") if x],
                "related_ids": [x for x in str(meta["related_ids"] or "").split(",") if x],
                "path": meta["path"],
            })
    return out


def build_chunks(out_dir: Path) -> list[dict]:
    idx = pd.read_parquet(out_dir / "documents" / "docs_index.parquet").fillna("")
    docs = []
    for meta in idx.to_dict("records"):
        text = (out_dir / meta["path"]).read_text()
        docs.extend(chunks(meta, text))
    return docs


def index_definition(name: str):
    from azure.search.documents.indexes.models import (SearchableField, SearchFieldDataType, SearchIndex,
                                                       SemanticConfiguration, SemanticField,
                                                       SemanticPrioritizedFields, SemanticSearch, SimpleField)
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SimpleField(name="doc_id", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SimpleField(name="doc_type", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SearchableField(name="title", type=SearchFieldDataType.String),
        SearchableField(name="section", type=SearchFieldDataType.String),
        SearchableField(name="content", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
        SimpleField(name="revision", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="effective_date", type=SearchFieldDataType.String, filterable=True, sortable=True),
        # collection=True, not type=Collection(...): SearchableField ignores `type` and would create a single
        # Edm.String, which then rejects the lists we upload ("unexpected 'StartArray' node").
        SearchableField(name="asset_ids", collection=True, filterable=True, facetable=True),
        SearchableField(name="related_ids", collection=True, filterable=True),
        SimpleField(name="path", type=SearchFieldDataType.String),
    ]
    semantic = SemanticSearch(configurations=[SemanticConfiguration(
        name="default", prioritized_fields=SemanticPrioritizedFields(
            title_field=SemanticField(field_name="title"),
            content_fields=[SemanticField(field_name="content")],
            keywords_fields=[SemanticField(field_name="section"), SemanticField(field_name="doc_id")]))],
        default_configuration_name="default")
    return SearchIndex(name=name, fields=fields, semantic_search=semantic)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=Path("output"))
    ap.add_argument("--endpoint", help="https://<service>.search.windows.net")
    ap.add_argument("--index", default="p2p-documents")
    ap.add_argument("--api-key", default=None, help="Admin key (default: managed identity / Entra ID)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--retries", type=int, default=10, help="RBAC can take minutes to propagate on a new VM")
    a = ap.parse_args(argv)
    docs = build_chunks(a.out)
    n_docs = len({d["doc_id"] for d in docs})
    inj = [d["id"] for d in docs if "NOTE TO AI ASSISTANTS" in d["content"]]
    print(f"[index] {len(docs)} chunks from {n_docs} documents; injection test present in {inj}")
    if a.dry_run:
        for d in docs[:3]:
            print(f"  {d['id']}: {d['section']} ({len(d['content'])} chars)")
        return 0
    if not a.endpoint:
        print("[index] --endpoint required unless --dry-run", file=sys.stderr)
        return 2
    from azure.core.credentials import AzureKeyCredential
    from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
    from azure.search.documents import SearchClient
    from azure.search.documents.indexes import SearchIndexClient
    cred = AzureKeyCredential(a.api_key) if a.api_key else (
        ManagedIdentityCredential() if Path("/etc/p2p/setup.conf").exists() else DefaultAzureCredential())
    for attempt in range(1, a.retries + 1):
        try:
            ic = SearchIndexClient(a.endpoint, cred)
            if a.index in list(ic.list_index_names()):
                ic.delete_index(a.index)
            ic.create_index(index_definition(a.index))
            sc = SearchClient(a.endpoint, a.index, cred)
            ok = 0
            for i in range(0, len(docs), 500):
                ok += sum(r.succeeded for r in sc.upload_documents(docs[i:i + 500]))
            print(f"[index] uploaded {ok}/{len(docs)} chunks to {a.endpoint}/indexes/{a.index}")
            return 0 if ok == len(docs) else 1
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status_code", None)
            print(f"[index] attempt {attempt} failed (HTTP {status}): {type(exc).__name__}: {str(exc)[:200]}", file=sys.stderr)
            # Only waiting can fix 401/403 (RBAC still propagating on a new VM) and 429/5xx. Anything else,
            # e.g. 400 for a schema mismatch, will fail identically every time: stop now.
            if status is not None and status not in (401, 403, 429) and status < 500:
                print("[index] not retrying: this error will not heal by waiting", file=sys.stderr)
                return 1
            if attempt < a.retries:
                time.sleep(60)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())