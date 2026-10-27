# ---------------------------------------------------------------------------
# P2P control tower PoC -- Event Hubs (streaming path)
# ---------------------------------------------------------------------------
#   VM publisher --(Kafka, 4 topics)--> Event Hubs --> Databricks (cg: databricks)
#                                                  \--> Fabric     (cg: fabric)
# Standard tier exposes the Kafka endpoint by default.
# ---------------------------------------------------------------------------

locals {
  common_tags = var.tags
  hub_cg_pairs = {
    for p in setproduct(var.eventhub_topics, var.consumer_groups) : "${p[0]}/${p[1]}" => { hub = p[0], cg = p[1] }
  }
}

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.name_prefix}-ehns-${random_string.suffix.result}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name

  # Standard is the minimum tier with the Kafka surface. Do not downgrade to Basic.
  sku      = "Standard"
  capacity = var.eventhub_capacity

  auto_inflate_enabled     = var.eventhub_auto_inflate_enabled
  maximum_throughput_units = var.eventhub_auto_inflate_enabled ? var.eventhub_maximum_throughput_units : null

  public_network_access_enabled = var.public_network_access_enabled

  # kafka_enabled intentionally not set: on by default for Standard, and the explicit
  # flag has caused spurious "replace namespace" diffs in the provider.
  tags = local.common_tags
}

# One hub per signal family. Separate hubs (not one shared topic) so each platform's
# per-topic ingest, lag and cost can be measured and compared.
resource "azurerm_eventhub" "topic" {
  for_each          = toset(var.eventhub_topics)
  name              = each.value
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.eventhub_partition_count
  message_retention = var.eventhub_message_retention_days
}

resource "azurerm_eventhub_consumer_group" "platform" {
  for_each            = local.hub_cg_pairs
  name                = each.value.cg
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.topic[each.value.hub].name
  resource_group_name = azurerm_resource_group.this.name
}

# ---------------------------------------------------------------------------
# SAS rules. Namespace-scoped, least privilege per ROLE:
#   publisher-send   send only. Namespace scope because ONE Kafka producer writes all
#                    four hubs; a hub-scoped string (EntityPath=...) is rejected by the
#                    publisher with a clear error.
#   <platform>-listen  listen only, one per platform, so each can be revoked independently.
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_namespace_authorization_rule" "publisher_send" {
  name                = "publisher-send"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_eventhub_namespace_authorization_rule" "platform_listen" {
  for_each            = toset(var.consumer_groups)
  name                = "${each.value}-listen"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  listen              = true
  send                = false
  manage              = false
}
