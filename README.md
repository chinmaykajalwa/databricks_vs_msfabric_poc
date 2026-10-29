# P2P Control Tower: Databricks vs Microsoft Fabric PoC

Synthetic, deterministic data for a **plant-to-provider control tower** at a fictional
vertically integrated medical-surgical manufacturer and distributor, plus the Azure
infrastructure that generates it and streams it live to both platforms.

> Everything here is **fictional**: company, sites, customers, suppliers, products,
> documents. Say so on every slide that shows it.

```
GitHub repo ──clone──> Azure VM (cloud-init -> infra/vm/setup.sh)
                         │ 1. generate batch data (steps 1-4) + gates + dry run
                         │ 2. upload ──> ADLS Gen2  landing/      ──> Databricks (external location)
                         │                          groundtruth/  (scoring only)  └─> Fabric (OneLake shortcut)
                         │ 3. demo: systemctl start p2p-publisher
                         └──(Kafka)──> Event Hubs: 4 topics ──> Databricks (consumer group "databricks")
                                                             └─> Fabric     (consumer group "fabric")
```

Both platforms read **the same files and the same stream**: any difference on the
scorecard is the platform's, not the data's.

## Repository layout

| Path | What |
|---|---|
| `generator/` | Python package `p2p_gen` + scripts: master data, supply-chain simulation, telemetry physics, documents, live publisher. Full detail in [`generator/README.md`](generator/README.md). |
| `generator/scripts/run_pipeline.sh` | Runs steps 1-4, on-disk validation and the step-5 dry-run gate; fails fast. |
| `infra/terraform/` | Event Hubs (4 topics, 2 consumer groups), ADLS Gen2 landing zone, generator VM. Runbook in [`infra/terraform/README.md`](infra/terraform/README.md). |
| `infra/vm/` | `setup.sh` (install, pipeline, upload, publisher unit) and `upload.sh` (managed-identity upload). Versioned with the code; the VM runs them from its checkout. |
| `platforms/databricks/` | Notebooks: batch ingest + reconciliation, Event Hubs streaming (bronze/silver), alert views, hero analysis. |
| `platforms/fabric/` | Lakehouse notebooks (batch + hero) and Eventhouse KQL (update policy, materialized views, alerts). |
| `platforms/shared/` | Agentic AI on both platforms: curated agent-ready tables (one definition, tested), identical instructions, example queries, scoring against ground truth. |
| `.github/workflows/ci.yml` | On every push: full lite pipeline with all gates + dry run; `terraform fmt` / `validate`. |

## Quick start

**Locally (no Azure):**
```bash
pip install -r generator/requirements.txt
bash generator/scripts/run_pipeline.sh --profile lite      # ~1 minute, all gates
```

**On Azure:** see [`infra/terraform/README.md`](infra/terraform/README.md). In short:
`export TF_VAR_git_pat=...`, `terraform apply`, wait ~10 minutes, check the setup
summary, then `sudo systemctl start p2p-publisher` when the audience is watching.

## What the data contains

| Layer | Content (lite) |
|---|---|
| Master data | 2 plants, 3 DCs, 300 SKUs with UDI-DIs and BOMs, 400 customers in 12 health systems, 30 trucks, capacity sized to demand |
| Transactions | 140-day simulation: work orders, EtO loads, lots and genealogy, transfers, POs, orders, shipments by lot, inventory, deviations, CAPAs |
| Telemetry | 815 tags on 204 assets: 30 days of 1-minute history + 2 days raw; live stream from the same physics |
| Documents | 11 SOPs, 4 manuals, a report per deviation, supplier notices, calibration records |
| Ground truth | Hero blast radius, anomaly log, true-vs-reported humidity, RAG eval set, prompt-injection test, per-live-run scoring files |

**The hero story:** a sterilizer humidity probe has drifted for 10 days. Reported
humidity looks perfect; only steam consumption reveals it. At minute 15 of the live
run the probe fails, and the agent must derive the suspect window from calibration
records and SOPs, trace loads -> lots -> DCs -> open orders -> hospitals, and leave the
recall decision to humans.

## Security notes

- No secrets in the repo. The Event Hubs connection string and the GitHub PAT are
  injected by Terraform into root-only files on the VM; storage uploads use the VM's
  managed identity (no keys).
- Terraform state contains the SAS strings and the VM's SSH key: keep it local or in
  a locked-down backend; it is gitignored.
- Ground truth lives in its own container. Grant the platforms access to `landing`
  only.
