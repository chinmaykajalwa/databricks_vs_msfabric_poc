# ---------------------------------------------------------------------------
# Azure AI Search: document index for the agents
# ---------------------------------------------------------------------------
# The Fabric data agent reaches unstructured content (SOPs, manuals, deviation
# reports, supplier notices) through an Azure AI Search index. The VM builds the
# index right after uploading the batch data (infra/vm/setup.sh -> index_documents.py),
# authenticating with its managed identity, so no admin key ever sits on the VM.
#
# The Fabric data agent queries the index with the END USER's Entra identity (not a key):
# grant Search Index Data Reader to agent users via var.search_reader_principal_ids.
# Keys AND Entra ID are both accepted (authentication_failure_mode enables RBAC alongside keys).
# ---------------------------------------------------------------------------

resource "azurerm_search_service" "docs" {
  count               = var.create_search ? 1 : 0
  name                = "${var.name_prefix}-srch-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.search_sku
  replica_count       = 1
  partition_count     = 1

  local_authentication_enabled  = true
  authentication_failure_mode   = "http401WithBearerChallenge"
  semantic_search_sku           = var.search_semantic_sku == "" ? null : var.search_semantic_sku
  public_network_access_enabled = var.public_network_access_enabled

  tags = local.common_tags
}

# VM identity: create/replace the index (service contributor) and upload documents (index data contributor).
resource "azurerm_role_assignment" "vm_search_service" {
  count                = var.create_search && var.create_vm ? 1 : 0
  scope                = azurerm_search_service.docs[0].id
  role_definition_name = "Search Service Contributor"
  principal_id         = azurerm_linux_virtual_machine.generator[0].identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "vm_search_data" {
  count                = var.create_search && var.create_vm ? 1 : 0
  scope                = azurerm_search_service.docs[0].id
  role_definition_name = "Search Index Data Contributor"
  principal_id         = azurerm_linux_virtual_machine.generator[0].identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

# End users (or groups) of the agents: data-plane read on the index.
resource "azurerm_role_assignment" "search_readers" {
  for_each             = var.create_search ? var.search_reader_principal_ids : {}
  scope                = azurerm_search_service.docs[0].id
  role_definition_name = "Search Index Data Reader"
  principal_id         = each.value.object_id
  principal_type       = each.value.type
}

resource "azurerm_role_assignment" "search_readers_contributor" {
  for_each             = var.create_search && var.search_readers_also_service_contributor ? var.search_reader_principal_ids : {}
  scope                = azurerm_search_service.docs[0].id
  role_definition_name = "Search Service Contributor"
  principal_id         = each.value.object_id
  principal_type       = each.value.type
}
