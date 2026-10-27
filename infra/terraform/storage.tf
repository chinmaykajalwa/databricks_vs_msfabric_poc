# ---------------------------------------------------------------------------
# ADLS Gen2 landing zone for the batch data
# ---------------------------------------------------------------------------
# The VM generates master data, transactions, telemetry history, documents and
# reference dims, then uploads them here with its managed identity (no keys on
# the VM). Databricks reads via a Unity Catalog external location; Fabric via a
# OneLake shortcut. Both read the SAME files -- the fairness condition.
#
#   landing/p2p/<profile>/{master,transactions,telemetry,documents,reference,live_samples}
#   groundtruth/p2p/<profile>/ground_truth     <- never give the platforms access
# ---------------------------------------------------------------------------

resource "azurerm_storage_account" "landing" {
  count                    = var.create_storage ? 1 : 0
  name                     = "${var.name_prefix}st${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication
  is_hns_enabled           = true # ADLS Gen2: abfss:// paths for both platforms

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = var.public_network_access_enabled

  tags = local.common_tags
}

resource "azurerm_storage_container" "landing" {
  count                 = var.create_storage ? 1 : 0
  name                  = var.landing_container
  storage_account_id    = azurerm_storage_account.landing[0].id
  container_access_type = "private"
}

resource "azurerm_storage_container" "groundtruth" {
  count                 = var.create_storage ? 1 : 0
  name                  = var.groundtruth_container
  storage_account_id    = azurerm_storage_account.landing[0].id
  container_access_type = "private"
}

# The VM's managed identity may write both containers. Requires the deploying
# identity to hold Owner or User Access Administrator on the subscription/RG.
# RBAC takes minutes to propagate; upload.sh retries for up to 10 minutes.
resource "azurerm_role_assignment" "vm_blob_writer" {
  count                = var.create_storage && var.create_vm ? 1 : 0
  scope                = azurerm_storage_account.landing[0].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_virtual_machine.generator[0].identity[0].principal_id
  principal_type       = "ServicePrincipal"
}
