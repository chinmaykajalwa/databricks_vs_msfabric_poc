# ---------------------------------------------------------------------------
# Outputs. Secrets are sensitive: fetch with `terraform output -raw <name>`.
# ---------------------------------------------------------------------------

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.this.name
}

output "kafka_bootstrap_server" {
  description = "bootstrap.servers for Databricks (Spark Kafka source) and for Fabric Eventstream / custom endpoints."
  value       = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
}

output "topics" {
  value = [for t in var.eventhub_topics : azurerm_eventhub.topic[t].name]
}

output "consumer_groups" {
  description = "Each platform reads every topic with its own consumer group."
  value       = var.consumer_groups
}

output "publisher_send_connection_string" {
  description = "Namespace-level SEND-only (already injected into the VM). For running the publisher elsewhere."
  value       = azurerm_eventhub_namespace_authorization_rule.publisher_send.primary_connection_string
  sensitive   = true
}

output "platform_listen_connection_strings" {
  description = "LISTEN-only per platform, e.g. terraform output -json platform_listen_connection_strings | jq -r .databricks"
  value       = { for k, r in azurerm_eventhub_namespace_authorization_rule.platform_listen : k => r.primary_connection_string }
  sensitive   = true
}

output "storage_account" {
  value = var.create_storage ? azurerm_storage_account.landing[0].name : null
}

output "landing_abfss_path" {
  description = "Point the Databricks external location / Fabric OneLake shortcut here."
  value       = var.create_storage ? "abfss://${var.landing_container}@${azurerm_storage_account.landing[0].name}.dfs.core.windows.net/p2p/${var.data_profile}/" : null
}

output "groundtruth_abfss_path" {
  description = "Answer keys for scoring. Do NOT grant either platform access."
  value       = var.create_storage ? "abfss://${var.groundtruth_container}@${azurerm_storage_account.landing[0].name}.dfs.core.windows.net/p2p/${var.data_profile}/ground_truth/" : null
}

output "search_endpoint" {
  description = "Azure AI Search endpoint for the Fabric data agent's document source."
  value       = var.create_search ? "https://${azurerm_search_service.docs[0].name}.search.windows.net" : null
}
output "search_index_name" {
  value = var.create_search ? var.search_index_name : null
}
output "search_query_key" {
  description = "READ-ONLY query key for the Fabric data agent connection: terraform output -raw search_query_key"
  value       = var.create_search ? azurerm_search_service.docs[0].query_keys[0].key : null
  sensitive   = true
}
output "vm_public_ip" {
  value = var.create_vm ? azurerm_public_ip.generator[0].ip_address : null
}

output "ssh_command" {
  value = var.create_vm ? "ssh -i ./generator_id_rsa ${var.admin_username}@${azurerm_public_ip.generator[0].ip_address}" : null
}

output "ssh_private_key" {
  description = "terraform output -raw ssh_private_key > ./generator_id_rsa && chmod 600 ./generator_id_rsa"
  value       = var.create_vm ? tls_private_key.generator[0].private_key_pem : null
  sensitive   = true
}

output "vm_hints" {
  value = var.create_vm ? {
    setup_summary   = "grep -E 'SETUP SUMMARY|status:|FAILED' /var/log/p2p-setup.log"
    deployed_commit = "cat /opt/p2p/COMMIT"
    start_demo      = "sudo systemctl start p2p-publisher"
    follow_demo     = "journalctl -u p2p-publisher -f"
    redeploy_code   = "sudo /opt/p2p/bootstrap.sh   # pulls repo_ref again, re-runs setup"
    boot_log        = "az vm boot-diagnostics get-boot-log -g ${azurerm_resource_group.this.name} -n ${azurerm_linux_virtual_machine.generator[0].name}"
  } : null
}
