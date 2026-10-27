# ---------------------------------------------------------------------------
# Generator VM
# ---------------------------------------------------------------------------
# cloud-init writes config + secrets, clones the repo, and hands over to the
# repo's own infra/vm/setup.sh (venv, pipeline, upload, publisher unit). Setup
# logic is versioned with the code: to update a running VM, re-run
#   sudo /opt/p2p/bootstrap.sh
# which pulls repo_ref again and re-runs setup -- no rebuild needed.
# ---------------------------------------------------------------------------

locals {
  vm_count = var.create_vm ? 1 : 0
}

resource "tls_private_key" "generator" {
  count     = local.vm_count
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "azurerm_virtual_network" "generator" {
  count               = local.vm_count
  name                = "${var.name_prefix}-vnet"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  address_space       = ["10.30.0.0/16"]
  tags                = local.common_tags
}

resource "azurerm_subnet" "generator" {
  count                = local.vm_count
  name                 = "generator"
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.generator[0].name
  address_prefixes     = ["10.30.1.0/24"]
}

resource "azurerm_network_security_group" "generator" {
  count               = local.vm_count
  name                = "${var.name_prefix}-nsg"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.common_tags

  # Inbound SSH from the admin's IP only; outbound (Event Hubs 9093, storage 443,
  # GitHub, PyPI) uses default egress.
  security_rule {
    name                       = "allow-ssh-from-admin"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.ssh_source_cidr
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "generator" {
  count                     = local.vm_count
  subnet_id                 = azurerm_subnet.generator[0].id
  network_security_group_id = azurerm_network_security_group.generator[0].id
}

resource "azurerm_public_ip" "generator" {
  count               = local.vm_count
  name                = "${var.name_prefix}-pip"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.common_tags
}

resource "azurerm_network_interface" "generator" {
  count               = local.vm_count
  name                = "${var.name_prefix}-nic"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.common_tags

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.generator[0].id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.generator[0].id
  }
}

resource "azurerm_linux_virtual_machine" "generator" {
  count                 = local.vm_count
  name                  = "${var.name_prefix}-gen-vm"
  location              = azurerm_resource_group.this.location
  resource_group_name   = azurerm_resource_group.this.name
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.generator[0].id]
  tags                  = local.common_tags

  # Managed identity: uploads to ADLS Gen2 without any storage key on the VM.
  identity {
    type = "SystemAssigned"
  }

  admin_ssh_key {
    username   = var.admin_username
    public_key = tls_private_key.generator[0].public_key_openssh
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = var.os_disk_size_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  # Serial console log without SSH or Run Command (its absence cost several
  # debugging rounds on the Novelis VM):  az vm boot-diagnostics get-boot-log -g <rg> -n <vm>
  boot_diagnostics {
    storage_account_uri = null
  }

  # custom_data is ForceNew: changing it rebuilds the VM, which is correct because
  # cloud-init only runs on first boot. For code-only changes, use bootstrap.sh instead.
  custom_data = base64encode(templatefile("${path.module}/cloud-init.yaml.tftpl", {
    repo_url         = var.repo_url
    repo_ref         = var.repo_ref
    git_pat          = var.git_pat
    generator_subdir = var.generator_subdir

    eventhub_bootstrap         = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
    eventhub_connection_string = azurerm_eventhub_namespace_authorization_rule.publisher_send.primary_connection_string

    data_profile          = var.data_profile
    run_pipeline          = var.run_pipeline_on_boot ? "true" : "false"
    upload_to_storage     = var.create_storage && var.upload_to_storage ? "true" : "false"
    storage_account       = var.create_storage ? azurerm_storage_account.landing[0].name : ""
    landing_container     = var.landing_container
    groundtruth_container = var.groundtruth_container

    publisher_autostart    = var.publisher_autostart ? "true" : "false"
    publisher_duration_min = var.publisher_duration_min
    publisher_time_scale   = var.publisher_time_scale
    publisher_flush_ms     = var.publisher_flush_ms
  }))

  # Preconditions FAIL the plan (a `check` block only warns, which let a missing
  # PAT and a placeholder repo_url through to a VM that could not clone).
  lifecycle {
    precondition {
      condition     = !var.repo_is_private || length(var.git_pat) > 0
      error_message = "repo_is_private = true but git_pat is empty. PowerShell: $env:TF_VAR_git_pat = \"github_pat_...\"  (a plain $TF_VAR_git_pat is NOT an environment variable). bash: export TF_VAR_git_pat=..."
    }
    precondition {
      condition     = !strcontains(var.repo_url, "<org>") && !strcontains(var.repo_url, "<repo>")
      error_message = "repo_url is still the placeholder from terraform.tfvars.example. Set it to your GitHub repo's HTTPS URL."
    }
  }

}
