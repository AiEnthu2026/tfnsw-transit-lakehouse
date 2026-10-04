resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "random_string" "storage_suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_storage_account" "main" {
  name                = "st${var.project_name}${var.environment}${random_string.storage_suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  min_tls_version                  = "TLS1_2"
  allow_nested_items_to_be_public  = false
  shared_access_key_enabled        = false

  network_rules {
    default_action = "Deny"
    ip_rules       = var.allowed_ip_ranges
    bypass         = ["AzureServices"]
    private_link_access {
      endpoint_resource_id = var.adf_salesforce_resource_id
      endpoint_tenant_id   = data.azurerm_client_config.current.tenant_id
    }
  }

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_databricks_access_connector" "main" {
  name                = "dbac-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_role_assignment" "access_connector_storage" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${var.project_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${var.project_name}-${var.environment}-managed"

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${var.project_name}-${var.environment}-${random_string.storage_suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  tenant_id            = data.azurerm_client_config.current.tenant_id
  sku_name             = "standard"

  rbac_authorization_enabled  = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ip_ranges
  }

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_role_assignment" "self_keyvault_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  sku                  = "PerGB2018"
  retention_in_days    = 30

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_monitor_diagnostic_setting" "keyvault" {
  name                       = "diag-kv-${var.environment}"
  target_resource_id         = azurerm_key_vault.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "AuditEvent"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

resource "azurerm_monitor_diagnostic_setting" "storage_blob" {
  name                       = "diag-storage-blob-${var.environment}"
  target_resource_id         = "${azurerm_storage_account.main.id}/blobServices/default/"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "StorageRead"
  }

  enabled_log {
    category = "StorageWrite"
  }

  enabled_log {
    category = "StorageDelete"
  }

  enabled_metric {
    category = "Transaction"
  }
}

resource "azurerm_monitor_diagnostic_setting" "subscription_activity_log" {
  name                       = "diag-subscription-activity-${var.environment}"
  target_resource_id         = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "Administrative"
  }

  enabled_log {
    category = "Security"
  }

  enabled_log {
    category = "Alert"
  }

  enabled_log {
    category = "Policy"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "checkpoints" {
  name               = "checkpoints"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_network_security_perimeter" "main" {
  name                = "nsp-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_network_security_perimeter_profile" "databricks" {
  name                           = "databricks-profile"
  network_security_perimeter_id = azurerm_network_security_perimeter.main.id
}

resource "azurerm_network_security_perimeter_access_rule" "allow_databricks_serverless" {
  name                                   = "allow-databricks-serverless"
  direction                              = "Inbound"
  network_security_perimeter_profile_id = azurerm_network_security_perimeter_profile.databricks.id

  service_tags = [
    "AzureDatabricksServerless.AustraliaEast",
  ]
}

resource "azurerm_network_security_perimeter_association" "storage" {
  name                                   = "storage-assoc"
  resource_id                            = azurerm_storage_account.main.id
  access_mode                            = "Learning"
  network_security_perimeter_profile_id = azurerm_network_security_perimeter_profile.databricks.id
}

resource "azurerm_network_security_perimeter_association" "keyvault" {
  name                                   = "keyvault-assoc"
  resource_id                            = azurerm_key_vault.main.id
  access_mode                            = "Enforced"
  network_security_perimeter_profile_id = azurerm_network_security_perimeter_profile.databricks.id
}

resource "azurerm_network_security_perimeter_access_rule" "allow_own_ip" {
  name                                   = "allow-own-ip"
  direction                              = "Inbound"
  network_security_perimeter_profile_id = azurerm_network_security_perimeter_profile.databricks.id

  address_prefixes = [for ip in var.allowed_ip_ranges : "${ip}/32"]
}

resource "azurerm_network_security_perimeter_access_rule" "allow_databricks_control_plane" {
  name                                   = "allow-databricks-control-plane"
  direction                              = "Inbound"
  network_security_perimeter_profile_id = azurerm_network_security_perimeter_profile.databricks.id
  address_prefixes                       = var.databricks_control_plane_nat_ips
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "access_connector_id" {
  value       = azurerm_databricks_access_connector.main.id
  description = "The Azure Resource Manager ID of the Databricks Access Connector."
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "key_vault_id" {
  value = azurerm_key_vault.main.id
}
