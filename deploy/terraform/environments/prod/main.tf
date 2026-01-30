# =============================================================================
# GOQUEUE TERRAFORM - PROD ENVIRONMENT (AWS)
# =============================================================================
#
# Production environment configuration for AWS EKS.
#
# DIFFERENCES FROM DEV:
#   - Larger instance types
#   - More nodes
#   - Larger storage
#   - Multi-AZ NAT gateways (higher availability)
#   - Stricter security settings
#
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  # IMPORTANT: Use remote state for production!
  # backend "s3" {
  #   bucket         = "goqueue-terraform-state"
  #   key            = "prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "goqueue-terraform-locks"
  #   encrypt        = true
  # }
}

module "goqueue_aws" {
  source = "../../modules/aws"

  # Cluster configuration
  region          = "us-east-1"
  cluster_name    = "goqueue-prod"
  cluster_version = "1.29"
  environment     = "prod"

  # Node configuration (production-grade)
  node_instance_types = ["m5.large"]
  node_desired_size   = 5
  node_min_size       = 5
  node_max_size       = 10

  # GoQueue configuration (production)
  goqueue_replicas     = 5
  goqueue_storage_size = "100Gi"

  # Monitoring
  install_prometheus = true
  install_traefik    = true

  # Tags
  tags = {
    Project     = "goqueue"
    Environment = "prod"
    ManagedBy   = "terraform"
    CostCenter  = "engineering"
  }
}

output "cluster_name" {
  value = module.goqueue_aws.cluster_name
}

output "configure_kubectl" {
  value = module.goqueue_aws.configure_kubectl
}

output "goqueue_namespace" {
  value = module.goqueue_aws.goqueue_namespace
}
