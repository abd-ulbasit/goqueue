# =============================================================================
# GOQUEUE TERRAFORM - DEV ENVIRONMENT (AWS)
# =============================================================================
#
# Development environment configuration for AWS EKS.
#
# USAGE:
#   cd deploy/terraform/environments/dev
#   terraform init
#   terraform plan
#   terraform apply
#
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ BACKEND CONFIGURATION                                                   │
  # │                                                                         │
  # │ For production, use remote state (S3, GCS, Azure Blob, etc.)            │
  # │ This enables team collaboration and state locking.                      │
  # │                                                                         │
  # │ Uncomment and configure for remote state:                               │
  # │                                                                         │
  # │ backend "s3" {                                                          │
  # │   bucket         = "goqueue-terraform-state"                            │
  # │   key            = "dev/terraform.tfstate"                              │
  # │   region         = "us-east-1"                                          │
  # │   dynamodb_table = "goqueue-terraform-locks"                            │
  # │   encrypt        = true                                                 │
  # │ }                                                                       │
  # └─────────────────────────────────────────────────────────────────────────┘
}

# Use the AWS module
module "goqueue_aws" {
  source = "../../modules/aws"

  # Cluster configuration
  region          = "us-east-1"
  cluster_name    = "goqueue-dev"
  cluster_version = "1.29"
  environment     = "dev"

  # Node configuration (cost-optimized for dev)
  node_instance_types = ["t3.medium"]
  node_desired_size   = 3
  node_min_size       = 3
  node_max_size       = 5

  # GoQueue configuration
  goqueue_replicas     = 3
  goqueue_storage_size = "10Gi"

  # Monitoring (enabled for dev to test dashboards)
  install_prometheus = true
  install_traefik    = true

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
