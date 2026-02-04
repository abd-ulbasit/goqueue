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

# Use the AWS module
module "goqueue_aws" {
  source = "../../modules/aws"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ CLUSTER CONFIGURATION                                                   │
  # │                                                                         │
  # │ Region: ap-south-1 (Mumbai) - low latency for India-based testing      │
  # └─────────────────────────────────────────────────────────────────────────┘
  region          = "ap-south-1"
  cluster_name    = "goqueue-dev"
  cluster_version = "1.29"
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
  # │ Using 'latest' tag which includes cluster support (v0.4.0+):            │
  # │   - Cluster mode is now the default behavior                            │
  # │   - HTTP server starts before cluster coordinator joins                 │
  # │   - Bootstrap mode allows joins without controller                      │
  # │   - Quorum retry mechanism for peer discovery                           │
  # └─────────────────────────────────────────────────────────────────────────┘
  goqueue_image_repository = "ghcr.io/abd-ulbasit/goqueue"
  goqueue_image_tag        = "latest"

  # Monitoring
  install_prometheus = true
  install_traefik    = false  # Use LoadBalancer directly for benchmarks

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
