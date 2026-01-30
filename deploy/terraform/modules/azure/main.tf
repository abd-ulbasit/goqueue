# =============================================================================
# GOQUEUE TERRAFORM - AZURE AKS MODULE
# =============================================================================
#
# This module provisions a complete Azure infrastructure for running GoQueue.
#
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
  }
}

# =============================================================================
# VARIABLES
# =============================================================================

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "goqueue-rg"
}

variable "cluster_name" {
  description = "Name of the AKS cluster"
  type        = string
  default     = "goqueue"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "node_vm_size" {
  description = "VM size for AKS nodes"
  type        = string
  default     = "Standard_D2s_v3"
}

variable "node_count" {
  description = "Number of nodes"
  type        = number
  default     = 3
}

variable "goqueue_replicas" {
  description = "Number of GoQueue broker replicas"
  type        = number
  default     = 3
}

variable "goqueue_storage_size" {
  description = "Storage size per GoQueue broker"
  type        = string
  default     = "10Gi"
}

variable "install_prometheus" {
  description = "Install Prometheus stack"
  type        = bool
  default     = true
}

variable "install_traefik" {
  description = "Install Traefik ingress controller"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags for all resources"
  type        = map(string)
  default = {
    Project   = "goqueue"
    ManagedBy = "terraform"
  }
}

# =============================================================================
# PROVIDER CONFIGURATION
# =============================================================================

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# =============================================================================
# RESOURCE GROUP
# =============================================================================

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# =============================================================================
# VIRTUAL NETWORK
# =============================================================================

resource "azurerm_virtual_network" "main" {
  name                = "${var.cluster_name}-vnet"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = ["10.0.0.0/8"]
  tags                = var.tags
}

resource "azurerm_subnet" "aks" {
  name                 = "aks-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.240.0.0/16"]
}

# =============================================================================
# AKS CLUSTER
# =============================================================================

resource "azurerm_kubernetes_cluster" "main" {
  name                = var.cluster_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = "1.29"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ DEFAULT NODE POOL                                                       │
  # │                                                                         │
  # │ AKS requires at least one "system" node pool for core components.       │
  # │ We use this pool for GoQueue workloads as well.                         │
  # └─────────────────────────────────────────────────────────────────────────┘
  default_node_pool {
    name                = "default"
    node_count          = var.node_count
    vm_size             = var.node_vm_size
    vnet_subnet_id      = azurerm_subnet.aks.id
    os_disk_size_gb     = 50
    os_disk_type        = "Managed"
    type                = "VirtualMachineScaleSets"
    enable_auto_scaling = false

    node_labels = {
      role = "goqueue"
    }
  }

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ MANAGED IDENTITY                                                        │
  # │                                                                         │
  # │ AKS can use managed identity instead of service principal.              │
  # │ Azure manages the identity lifecycle automatically.                     │
  # └─────────────────────────────────────────────────────────────────────────┘
  identity {
    type = "SystemAssigned"
  }

  # Network configuration
  network_profile {
    network_plugin    = "azure"
    network_policy    = "azure"
    load_balancer_sku = "standard"
    service_cidr      = "10.0.0.0/16"
    dns_service_ip    = "10.0.0.10"
  }

  # Enable RBAC
  role_based_access_control_enabled = true

  tags = var.tags
}

# =============================================================================
# KUBERNETES AND HELM PROVIDERS
# =============================================================================

provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.main.kube_config[0].host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = azurerm_kubernetes_cluster.main.kube_config[0].host
    client_certificate     = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_certificate)
    client_key             = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.main.kube_config[0].cluster_ca_certificate)
  }
}

# =============================================================================
# STORAGE CLASS
# =============================================================================

resource "kubernetes_storage_class" "premium" {
  metadata {
    name = "managed-premium"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner = "disk.csi.azure.com"
  reclaim_policy      = "Delete"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    skuName = "Premium_LRS"
  }

  depends_on = [azurerm_kubernetes_cluster.main]
}

# =============================================================================
# GOQUEUE DEPLOYMENT
# =============================================================================

resource "kubernetes_namespace" "goqueue" {
  metadata {
    name = "goqueue"
    labels = {
      name = "goqueue"
    }
  }

  depends_on = [azurerm_kubernetes_cluster.main]
}

resource "helm_release" "goqueue" {
  name       = "goqueue"
  namespace  = kubernetes_namespace.goqueue.metadata[0].name
  chart      = "${path.module}/../../../../kubernetes/helm/goqueue"

  values = [
    yamlencode({
      replicaCount = var.goqueue_replicas

      persistence = {
        enabled      = true
        storageClass = "managed-premium"
        size         = var.goqueue_storage_size
      }

      resources = {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
        requests = {
          cpu    = "500m"
          memory = "512Mi"
        }
      }

      prometheus = {
        enabled = var.install_prometheus
      }

      traefik = {
        enabled = var.install_traefik
      }
    })
  ]

  depends_on = [
    kubernetes_storage_class.premium,
    azurerm_kubernetes_cluster.main
  ]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "cluster_name" {
  description = "AKS cluster name"
  value       = azurerm_kubernetes_cluster.main.name
}

output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.main.name
}

output "kube_config" {
  description = "Kubernetes config"
  value       = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive   = true
}

output "configure_kubectl" {
  description = "Configure kubectl command"
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.main.name} --name ${azurerm_kubernetes_cluster.main.name}"
}

output "goqueue_namespace" {
  description = "Kubernetes namespace for GoQueue"
  value       = kubernetes_namespace.goqueue.metadata[0].name
}
