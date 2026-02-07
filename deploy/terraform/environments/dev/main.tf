# =============================================================================
# GOQUEUE TERRAFORM - DEV ENVIRONMENT (AWS)
# =============================================================================
#
# OPTIMIZED ARCHITECTURE:
#
#   ┌─────────────────────────────────────────────────────────────────────────┐
#   │                      EKS Cluster (ap-south-1)                           │
#   │                                                                         │
#   │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐           │
#   │  │   c5.xlarge     │ │   c5.xlarge     │ │   c5.xlarge     │           │
#   │  │   Node 1 (AZ-a) │ │   Node 2 (AZ-b) │ │   Node 3 (AZ-a) │           │
#   │  │                 │ │                 │ │                 │           │
#   │  │  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │           │
#   │  │  │ goqueue-0 │  │ │  │ goqueue-1 │  │ │  │ goqueue-2 │  │           │
#   │  │  │  (leader) │  │ │  │ (follower)│  │ │  │ (follower)│  │           │
#   │  │  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │           │
#   │  │       │         │ │       │         │ │       │         │           │
#   │  │  ┌─────────┐    │ │  ┌─────────┐    │ │  ┌─────────┐    │           │
#   │  │  │ EBS PV  │    │ │  │ EBS PV  │    │ │  │ EBS PV  │    │           │
#   │  │  │ (gp3)   │    │ │  │ (gp3)   │    │ │  │ (gp3)   │    │           │
#   │  │  └─────────┘    │ │  └─────────┘    │ │  └─────────┘    │           │
#   │  └─────────────────┘ └─────────────────┘ └─────────────────┘           │
#   │                                                                         │
#   │  Pod Anti-Affinity: Each goqueue pod on different node                  │
#   └─────────────────────────────────────────────────────────────────────────┘
#
# WHY C5.XLARGE?
#   - 4 vCPU, 8 GB RAM - sufficient for high-throughput queue
#   - Compute-optimized (not burstable like t3)
#   - Consistent CPU performance for sustained workloads
#   - ~$0.17/hour in ap-south-1 (vs $0.04 for t3.medium)
#   - Alternative: c6i.xlarge (newer gen, similar price)
#
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ S3 REMOTE BACKEND WITH STATE LOCKING                                    │
  # │                                                                         │
  # │ WHY REMOTE STATE?                                                       │
  # │   - Team collaboration (shared state)                                   │
  # │   - State locking prevents concurrent modifications                     │
  # │   - Encryption at rest                                                  │
  # │   - Versioning for rollback                                             │
  # └─────────────────────────────────────────────────────────────────────────┘
  backend "s3" {
    bucket         = "goqueue-terraform-state-558162184856"
    key            = "dev/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "goqueue-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = "ap-south-1"
}

# ============================================================================
# PROVIDER CONFIGURATION FOR KUBERNETES/HELM
# ============================================================================
#
# NOTE: These providers require the EKS cluster to exist first.
# On initial deploy, run: terraform apply -target=module.goqueue_aws.module.eks
# Then run: terraform apply (for the full deployment)
#
# This is a common terraform chicken-and-egg problem with EKS.
# The Helm/K8s providers need cluster endpoint, but the cluster doesn't
# exist yet on first run.
#
# WORKAROUND: The tf.sh script handles this by:
# 1. First apply: Create EKS cluster only
# 2. Second apply: Deploy Helm charts
#
# OR use a separate terraform configuration for the cluster and workloads.
#
# For now, we reference module outputs which will fail on first run.
# The tf.sh script handles this gracefully.
# ============================================================================

# These data sources will fail on first run - this is expected behavior
# The tf.sh script handles the two-phase deployment
data "aws_eks_cluster" "cluster" {
  name       = "goqueue-dev"
  depends_on = [module.goqueue_aws]
}

data "aws_eks_cluster_auth" "cluster" {
  name       = "goqueue-dev"
  depends_on = [module.goqueue_aws]
}

provider "kubernetes" {
  host                   = try(data.aws_eks_cluster.cluster.endpoint, "")
  cluster_ca_certificate = try(base64decode(data.aws_eks_cluster.cluster.certificate_authority[0].data), "")
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    args        = ["eks", "get-token", "--cluster-name", "goqueue-dev"]
    command     = "aws"
  }
}

provider "helm" {
  kubernetes {
    host                   = try(data.aws_eks_cluster.cluster.endpoint, "")
    cluster_ca_certificate = try(base64decode(data.aws_eks_cluster.cluster.certificate_authority[0].data), "")
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      args        = ["eks", "get-token", "--cluster-name", "goqueue-dev"]
      command     = "aws"
    }
  }
}

# Use the AWS module
module "goqueue_aws" {
  source = "../../modules/aws"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ CLUSTER CONFIGURATION                                                   │
  # │                                                                         │
  # │ Region: ap-south-1 (Mumbai) - low latency for India-based testing      │
  # │                                                                         │
  # │ K8S VERSION: 1.31 (Standard Support)                                    │
  # │   - 1.29 is in Extended Support (6x cost!)                              │
  # │   - 1.31 is current stable with standard support pricing                │
  # └─────────────────────────────────────────────────────────────────────────┘
  region          = "ap-south-1"
  cluster_name    = "goqueue-dev"
  cluster_version = "1.31"
  environment     = "dev"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ NODE CONFIGURATION - COMPUTE OPTIMIZED                                  │
  # │                                                                         │
  # │ c5.xlarge: 4 vCPU, 8 GB RAM                                             │
  # │   - Consistent CPU (no burst credits)                                   │
  # │   - High network bandwidth (up to 10 Gbps)                              │
  # │   - EBS-optimized by default                                            │
  # │                                                                         │
  # │ 3 nodes = 1 goqueue pod per node with anti-affinity                     │
  # └─────────────────────────────────────────────────────────────────────────┘
  node_instance_types = ["c5.xlarge"]
  node_desired_size   = 3
  node_min_size       = 3
  node_max_size       = 3  # Fixed size for benchmark consistency

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ GOQUEUE CONFIGURATION                                                   │
  # │                                                                         │
  # │ 3 replicas with pod anti-affinity = one per node                        │
  # │ 50Gi storage per broker for production-like testing                     │
  # └─────────────────────────────────────────────────────────────────────────┘
  goqueue_replicas     = 3
  goqueue_storage_size = "50Gi"
  
  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ GOQUEUE IMAGE CONFIGURATION                                             │
  # │                                                                         │
  # │ Using v0.7.0-security-schemas tag which includes:                       │
  # │   - Cluster mode with distributed consensus                             │
  # │   - Security features (TLS, mTLS, API Key Auth, RBAC)                   │
  # │   - Schema Registry for message validation                              │
  # │   - Transaction support for exactly-once semantics                      │
  # └─────────────────────────────────────────────────────────────────────────┘
  goqueue_image_repository = "ghcr.io/abd-ulbasit/goqueue"
  goqueue_image_tag        = "v0.7.0-security-schemas"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ SECURITY CONFIGURATION (M21)                                            │
  # │                                                                         │
  # │ Enables:                                                                 │
  # │   - TLS for client connections (HTTPS)                                  │
  # │   - mTLS for inter-node cluster communication                           │
  # │   - API Key authentication with RBAC                                    │
  # │   - Self-signed certificates (dev environment)                          │
  # └─────────────────────────────────────────────────────────────────────────┘
  security_enabled         = true
  security_tls_self_signed = true
  security_root_api_key    = "3c78bc92f544273a7772dbcf5bbd8bfc872b103acbf5750ea2b4daf96330a1e2"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ BACKUP CONFIGURATION (M23)                                              │
  # │                                                                         │
  # │ Enables:                                                                 │
  # │   - Scheduled metadata backups (topics, offsets, schemas)               │
  # │   - VolumeSnapshot-based EBS backups                                    │
  # │   - StorageClass with Delete policy (prevents orphaned volumes)         │
  # │                                                                         │
  # │ COSTS:                                                                   │
  # │   - EBS Snapshots: ~$0.05/GB-month (incremental)                        │
  # │   - For 3 x 50GB PVCs = ~$7.50/month in snapshots                       │
  # └─────────────────────────────────────────────────────────────────────────┘
  backup_enabled                  = true
  backup_schedule                 = "0 2 * * *"   # Daily at 2 AM
  backup_volume_snapshot_enabled  = true
  backup_volume_snapshot_schedule = "0 3 * * *"   # Daily at 3 AM
  backup_retention_days           = 7

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ MONITORING - PROMETHEUS OPERATOR                                        │
  # │                                                                         │
  # │ The previous agent INCORRECTLY disabled Prometheus to fix the error.    │
  # │ The ROOT CAUSE was: ServiceMonitor/PrometheusRule CRDs were missing.    │
  # │                                                                         │
  # │ PROPER FIX: Install kube-prometheus-stack BEFORE GoQueue.               │
  # │ This installs Prometheus Operator which provides the CRDs.              │
  # └─────────────────────────────────────────────────────────────────────────┘
  install_prometheus = true
  install_traefik    = false  # Use LoadBalancer directly for benchmarks

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ TRACING CONFIGURATION (M25)                                             │
  # │                                                                         │
  # │ Enables OTLP trace export to Grafana Tempo.                             │
  # │ Sampling at 1.0 in dev (capture all traces for debugging).              │
  # │ In production, reduce to 0.01-0.1 for cost efficiency.                  │
  # └─────────────────────────────────────────────────────────────────────────┘
  tracing_enabled       = true
  tracing_otlp_endpoint = "tempo.goqueue:4317"
  tracing_sampling_rate = 1.0

  # Tags
  tags = {
    Project     = "goqueue"
    Environment = "dev"
    ManagedBy   = "terraform"
    Owner       = "basit"
  }
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.goqueue_aws.cluster_name
}

output "configure_kubectl" {
  description = "Command to configure kubectl"
  value       = module.goqueue_aws.configure_kubectl
}

output "goqueue_namespace" {
  description = "Kubernetes namespace for GoQueue"
  value       = module.goqueue_aws.goqueue_namespace
}
