# =============================================================================
# GOQUEUE TERRAFORM - GCP GKE MODULE
# =============================================================================
#
# This module provisions a complete GCP infrastructure for running GoQueue:
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │                        GCP ARCHITECTURE                                     │
# │                                                                             │
# │  ┌──────────────────────────────────────────────────────────────────────┐   │
# │  │                              VPC                                     │   │
# │  │  ┌─────────────────────────────────────────────────────────────┐    │   │
# │  │  │                     GKE Cluster                              │    │   │
# │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │    │   │
# │  │  │  │  goqueue-0  │ │  goqueue-1  │ │  goqueue-2  │            │    │   │
# │  │  │  │  (zone-a)   │ │  (zone-b)   │ │  (zone-c)   │            │    │   │
# │  │  │  └─────────────┘ └─────────────┘ └─────────────┘            │    │   │
# │  │  │           │              │              │                    │    │   │
# │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │    │   │
# │  │  │  │  PD SSD     │ │  PD SSD     │ │  PD SSD     │            │    │   │
# │  │  │  └─────────────┘ └─────────────┘ └─────────────┘            │    │   │
# │  │  └─────────────────────────────────────────────────────────────┘    │   │
# │  └──────────────────────────────────────────────────────────────────────┘   │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
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

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "cluster_name" {
  description = "Name of the GKE cluster"
  type        = string
  default     = "goqueue"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "node_machine_type" {
  description = "Machine type for GKE nodes"
  type        = string
  default     = "e2-medium"
}

variable "node_count" {
  description = "Number of nodes per zone"
  type        = number
  default     = 1
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

# =============================================================================
# DATA SOURCES
# =============================================================================

data "google_compute_zones" "available" {
  project = var.project_id
  region  = var.region
}

# =============================================================================
# VPC NETWORK
# =============================================================================

resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  project                 = var.project_id
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.cluster_name}-subnet"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.0.0.0/16"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ SECONDARY RANGES FOR GKE                                                │
  # │                                                                         │
  # │ GKE uses VPC-native networking (alias IP ranges):                       │
  # │   - Pods get IPs from the pods range                                    │
  # │   - Services get IPs from the services range                            │
  # │   - More efficient routing, no IP masquerading                          │
  # └─────────────────────────────────────────────────────────────────────────┘
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/16"
  }
}

# =============================================================================
# GKE CLUSTER
# =============================================================================

resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  project  = var.project_id
  location = var.region

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ AUTOPILOT vs STANDARD                                                   │
  # │                                                                         │
  # │ Autopilot: Google manages nodes, pay per pod                            │
  # │   - Simpler, less control                                               │
  # │   - Good for: Most workloads                                            │
  # │                                                                         │
  # │ Standard: You manage node pools                                         │
  # │   - More control, more responsibility                                   │
  # │   - Good for: Specific node requirements, GPUs, etc.                    │
  # │                                                                         │
  # │ We use Standard for control over node specs for GoQueue.                │
  # └─────────────────────────────────────────────────────────────────────────┘

  # Remove default node pool, we'll create our own
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ WORKLOAD IDENTITY                                                       │
  # │                                                                         │
  # │ GCP's equivalent of AWS IRSA. Allows pods to impersonate Google         │
  # │ service accounts without storing credentials.                           │
  # └─────────────────────────────────────────────────────────────────────────┘
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Release channel for automatic upgrades
  release_channel {
    channel = var.environment == "prod" ? "STABLE" : "REGULAR"
  }

  # Network policy for pod-to-pod traffic control
  network_policy {
    enabled = true
  }

  # Maintenance window (UTC)
  maintenance_policy {
    recurring_window {
      start_time = "2024-01-01T04:00:00Z"
      end_time   = "2024-01-01T08:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA,SU"
    }
  }
}

# =============================================================================
# NODE POOL
# =============================================================================

resource "google_container_node_pool" "primary_nodes" {
  name       = "${var.cluster_name}-node-pool"
  project    = var.project_id
  location   = var.region
  cluster    = google_container_cluster.primary.name
  node_count = var.node_count

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ AUTOSCALING                                                             │
  # │                                                                         │
  # │ GKE can automatically add/remove nodes based on pending pods.           │
  # │ For GoQueue (StatefulSet), scaling is usually manual, but we enable     │
  # │ autoscaling for handling monitoring and other workloads.                │
  # └─────────────────────────────────────────────────────────────────────────┘
  autoscaling {
    min_node_count = var.node_count
    max_node_count = var.node_count * 2
  }

  node_config {
    machine_type = var.node_machine_type

    # Google recommends custom service accounts
    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      env  = var.environment
      role = "goqueue"
    }

    # Enable workload identity on nodes
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Shielded instance options for security
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}


# =============================================================================
# SERVICE ACCOUNT
# =============================================================================

resource "google_service_account" "gke_sa" {
  account_id   = "${var.cluster_name}-gke-sa"
  display_name = "GKE Service Account for ${var.cluster_name}"
  project      = var.project_id
}

# Minimum required roles for GKE nodes
resource "google_project_iam_member" "gke_sa_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_sa.email}"
}

# =============================================================================
# STORAGE CLASS
# =============================================================================

data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.primary.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${google_container_cluster.primary.endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
  }
}

resource "kubernetes_storage_class" "ssd" {
  metadata {
    name = "premium-rwo"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner = "pd.csi.storage.gke.io"
  reclaim_policy      = "Delete"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    type = "pd-ssd"
  }

  depends_on = [google_container_node_pool.primary_nodes]
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

  depends_on = [google_container_node_pool.primary_nodes]
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
        storageClass = "premium-rwo"
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
    kubernetes_storage_class.ssd,
    google_container_node_pool.primary_nodes
  ]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.primary.name
}

output "cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = google_container_cluster.primary.endpoint
  sensitive   = true
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "configure_kubectl" {
  description = "Configure kubectl command"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.primary.name} --region ${var.region} --project ${var.project_id}"
}

output "goqueue_namespace" {
  description = "Kubernetes namespace for GoQueue"
  value       = kubernetes_namespace.goqueue.metadata[0].name
}
