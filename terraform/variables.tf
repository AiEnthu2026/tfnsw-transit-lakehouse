variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "australiaeast"
}

variable "project_name" {
  description = "Short name used as a prefix for all resource names"
  type        = string
  default     = "tfnsw"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "allowed_ip_ranges" {
  description = "Public IP addresses allowed to reach storage and Key Vault data-plane"
  type        = list(string)
  sensitive   = true
}

variable "databricks_control_plane_nat_ips" {
  description = "Azure Databricks control-plane NAT IP ranges for australiaeast, needed for Key Vault-backed secret scope access (no service tag exists for this path). Source: https://learn.microsoft.com/en-us/azure/databricks/resources/ip-domain-region"
  type        = list(string)
  default = [
    "4.198.162.56/29",
    "40.79.169.48/29",
    "4.237.24.16/29",
    "13.70.105.50/32",
    "20.211.147.64/29",
    "20.28.138.72/29",
    "20.11.26.96/29",
    "20.40.72.88/29",
    "20.5.1.136/29",
    "20.5.170.240/29",
    "20.248.253.185/32"
  ]
}

variable "adf_salesforce_resource_id" {
  description = "Salesforce resource id"
  type        = string
  sensitive   = true
}