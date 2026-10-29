# Infrastructure runbook (Terraform)

Creates, in one resource group:

| Resource | Purpose |
|---|---|
| Event Hubs namespace (Standard, Kafka endpoint) | 4 hubs: `molding-telemetry`, `sterilizer-cycles`, `dc-asset-telemetry`, `fleet-telemetry` |
| Consumer groups `databricks`, `fabric` on every hub | Each platform reads the same stream with its own offsets |
| SAS rules | `publisher-send` (namespace-level send: one producer writes all 4 hubs); `databricks-listen`, `fabric-listen` |
| ADLS Gen2 account, containers `landing` + `groundtruth` | Batch data for both platforms; answer keys kept separate |
| Linux VM (Ubuntu 24.04, B2ms) with managed identity | Clones the repo, generates and uploads data, runs the live publisher |
| Role assignment | VM identity -> Storage Blob Data Contributor on the account |
| Azure AI Search (basic, semantic ranker free plan) | Index `p2p-documents` of SOPs, manuals, deviation reports, supplier notices for the agents |
| Role assignments | VM identity -> Search Service Contributor + Search Index Data Contributor (builds the index without keys) |

## Prerequisites

- Terraform >= 1.5, Azure CLI logged in (`az login`), subscription selected.
- Your identity needs **Owner** or **User Access Administrator** on the subscription or
  resource group (for the VM's role assignment).
- A GitHub **fine-grained PAT**, Contents: Read on this repo only (if private).

## Deploy

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # set repo_url, ssh_source_cidr, location
export TF_VAR_git_pat="github_pat_xxxxxxxx"       # PowerShell: $env:TF_VAR_git_pat = "..."
terraform init
terraform plan                                    # preconditions fail the plan if the PAT is missing or repo_url is a placeholder
terraform apply
terraform output -raw ssh_private_key > ./generator_id_rsa && chmod 600 ./generator_id_rsa
```

First boot takes ~5-10 minutes (packages, venv, pipeline ~1 min for lite, upload).

## Verify

From your machine, without SSH (Run Command):
```bash
az vm run-command invoke -g <rg> -n <vm> --command-id RunShellScript \
  --scripts "grep -E 'SETUP SUMMARY|status:|FAILED|PIPELINE OK' /var/log/p2p-setup.log" \
  --query "value[0].message" -o tsv
```
Expect `SETUP SUMMARY install=ok pipeline=ok upload=ok search=ok unit=ok`. If `upload=FAILED` or `search=FAILED`,
RBAC was still propagating (both stages retry for ~10 minutes): re-run `sudo /opt/p2p/bootstrap.sh` in the
background (`nohup ... &`), or just the failed stage: `upload.sh all`, or
`cd /opt/p2p/repo/generator && /opt/p2p/venv/bin/python scripts/index_documents.py --out /opt/p2p/output/lite --endpoint <search endpoint>`.
Never run `bootstrap.sh` synchronously through Run Command (it takes minutes and wedges
the extension); use SSH or `nohup ... &` and poll.

Then check the landing zone (`terraform output landing_abfss_path`) in the Portal.

## Demo day

```bash
ssh -i ./generator_id_rsa azureuser@<ip>
sudo systemctl start p2p-publisher        # 60-minute story, record times = now
journalctl -u p2p-publisher -f
```
Story minutes: 5 sorter vibration, 15 hero RH probe failure, 20 firmware 4.3,
30 reefer alarm, 45 DC outage. When the run ends, the live-run ground truth is uploaded
to `groundtruth/` automatically (ExecStopPost). The unit does **not** restart on
failure: a restart would replay the story with new timestamps, which looks like a
second incident.

**Rehearsal** (whole story in 10 minutes, without changing the unit):
```bash
cd /opt/p2p/repo/generator && sudo /opt/p2p/venv/bin/python scripts/publish_live.py \
  --out /opt/p2p/output/lite --env-file /etc/p2p/publisher.env --time-scale 6
```

## Deploy new code without rebuilding the VM

Push to the repo (CI must be green), then on the VM:
```bash
sudo /opt/p2p/bootstrap.sh     # re-clones repo_ref, re-runs setup (pipeline, upload, unit)
cat /opt/p2p/COMMIT            # what is deployed
```
Changing Terraform variables that feed cloud-init rebuilds the VM (custom_data is
ForceNew), which is the correct behaviour since cloud-init only runs on first boot.
Pin `repo_ref` to a tag or SHA for the demo, so the VM runs exactly what CI tested.
## Documents in Azure AI Search
Adding search to an existing deployment changes cloud-init, so `terraform apply` **rebuilds the VM** (Event Hubs,
storage and your platform setups are untouched; the deterministic generator recreates identical data). Afterwards:
```bash
terraform output search_endpoint
terraform output -raw search_query_key      # read-only key for the Fabric data agent
```
Portal -> the search service -> Indexes -> `p2p-documents` should show ~350 documents (one per document section).
Search for `steam consumption` to see MAN-STZ-EX section 4.3 come back first.

## Connecting the platforms

- **Kafka bootstrap:** `terraform output kafka_bootstrap_server`.
- **Listen secrets:** `terraform output -json platform_listen_connection_strings`.
- **Databricks:** Spark Kafka source, `kafka.security.protocol=SASL_SSL`,
  `kafka.sasl.mechanism=PLAIN`, JAAS username `$ConnectionString`, password = the
  `databricks` listen string; `kafka.group.id`/consumer group `databricks`. Batch:
  Unity Catalog external location on `landing_abfss_path` (via an access connector).
- **Fabric:** Eventstream with an Azure Event Hubs source per hub, consumer group
  `fabric`, the `fabric-listen` key. Batch: OneLake shortcut to `landing_abfss_path`.
- Load `reference/tag_dim.*` into both; load `reference/tag_dim_fw43.csv` mid-demo to
  show schema evolution.

## Teardown

```bash
terraform destroy
```
Then revoke the GitHub PAT. Deleting the resource group also removes everything.

## Changes from the Novelis two-arm Terraform

| Area | Before | Now |
|---|---|---|
| Code on the VM | 2 branches, 2 venvs, 2 services, 24x7 loops | 1 repo/ref, 1 venv; batch generation once, live run on demand |
| Setup logic | Inline in cloud-init | In the repo (`infra/vm/setup.sh`); cloud-init only clones and calls it |
| Hubs | 2 hubs, hub-scoped SAS | 4 hubs (for_each), namespace-level send + per-platform listen |
| Consumer groups | `databricks` | `databricks` and `fabric` on every hub |
| Storage | none | ADLS Gen2 `landing` + `groundtruth`, VM managed identity, retrying upload |
| VM | B2s, no identity | B2ms (baseline headroom), system-assigned identity, 64 GB StandardSSD |
| Ref | branch only (`--branch`) | branch, tag **or** commit SHA |
| CI | none | GitHub Actions: full pipeline + gates + dry run; terraform fmt/validate |

Kept from Novelis: plan-time PAT check; clone to temp then swap; `GIT_TERMINAL_PROMPT=0`;
token scrubbed from `.git/config`; secrets in 0600 root files, never shell-sourced;
`PYTHONUNBUFFERED=1`; boot diagnostics; per-stage status with a single SUMMARY line;
non-zero exit so `cloud-init status` is truthful; SSH scoped to your IP.
