# ---------------------------------------------------------------------------
# Core variables: naming, Event Hubs, storage
# ---------------------------------------------------------------------------

variable "name_prefix" {
  description = "Short prefix for all resource names. A random suffix is appended to globally unique names (Event Hubs namespace, storage account)."
  type        = string
  default     = "p2ppoc"

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.name_prefix))
    error_message = "name_prefix must be 3-12 lowercase alphanumeric characters (storage account naming rules)."
  }
}

variable "location" {
  description = "Azure region. Put it in the same region as the Databricks workspace and Fabric capacity used for the PoC, so neither platform pays cross-region egress or latency."
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Resource group created for this PoC. Deleting it tears everything down."
  type        = string
  default     = "p2p-controltower-poc-rg"
}

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default = {
    project     = "p2p-controltower-poc"
    environment = "poc"
    managed_by  = "terraform"
  }
}

# ---- Event Hubs ------------------------------------------------------------

variable "eventhub_capacity" {
  description = "Throughput units (1 TU = 1 MB/s or 1,000 events/s ingress). The lite dry run measures ~0.008 MB/s peak: 1 TU is ample."
  type        = number
  default     = 1
}

variable "eventhub_auto_inflate_enabled" {
  description = "Auto-scale TUs on bursts instead of throttling."
  type        = bool
  default     = true
}

variable "eventhub_maximum_throughput_units" {
  description = "Auto-inflate ceiling; caps worst-case TU spend."
  type        = number
  default     = 2
}

variable "eventhub_topics" {
  description = "Event Hubs (Kafka topics). MUST match the publisher's TOPICS (generator/p2p_gen/publisher.py); the publisher writes all four."
  type        = list(string)
  default     = ["molding-telemetry", "sterilizer-cycles", "dc-asset-telemetry", "fleet-telemetry"]

  validation {
    condition     = length(setsubtract(["molding-telemetry", "sterilizer-cycles", "dc-asset-telemetry", "fleet-telemetry"], var.eventhub_topics)) == 0
    error_message = "eventhub_topics must include all four publisher topics: molding-telemetry, sterilizer-cycles, dc-asset-telemetry, fleet-telemetry."
  }
}

variable "eventhub_partition_count" {
  description = "Partitions per hub. The publisher keys by site (a handful of plants/DCs), so 4 is plenty. Cannot be changed after creation on Standard."
  type        = number
  default     = 4
}

variable "eventhub_message_retention_days" {
  description = "Retention (Standard: 1-7 days). 7 lets either platform re-read a demo run during the week."
  type        = number
  default     = 7

  validation {
    condition     = var.eventhub_message_retention_days >= 1 && var.eventhub_message_retention_days <= 7
    error_message = "Standard tier supports 1-7 days."
  }
}

variable "consumer_groups" {
  description = "One consumer group per reading platform, on every hub, so Databricks and Fabric read the same stream independently (separate offsets) -- the fairness condition of the comparison."
  type        = list(string)
  default     = ["databricks", "fabric"]
}

variable "public_network_access_enabled" {
  description = "Public endpoints for Event Hubs and storage. True for PoC simplicity (access still gated by SAS / Entra ID). Set false only with private endpoints for both platforms."
  type        = bool
  default     = true
}

# ---- Storage (ADLS Gen2 landing zone for the batch data) -------------------

variable "create_storage" {
  description = "Create the ADLS Gen2 account the VM uploads batch data to (master, transactions, telemetry history, documents). Both Databricks (external location) and Fabric (OneLake shortcut) read it from there."
  type        = bool
  default     = true
}

variable "landing_container" {
  description = "Container for data the platforms load. Grant Databricks and Fabric access to THIS container only."
  type        = string
  default     = "landing"
}

variable "groundtruth_container" {
  description = "Container for ground truth (answer keys, live-run scoring files). Never grant the platforms access -- agents must not be able to read the answers."
  type        = string
  default     = "groundtruth"
}

variable "storage_replication" {
  description = "Replication for the landing account. LRS is fine: everything in it can be regenerated deterministically."
  type        = string
  default     = "LRS"
}
