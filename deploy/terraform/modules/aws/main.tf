# =============================================================================
# GOQUEUE TERRAFORM - AWS EKS MODULE
# =============================================================================
#
# This module provisions a complete AWS infrastructure for running GoQueue:
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │                        AWS ARCHITECTURE                                     │
# │                                                                             │
# │  ┌──────────────────────────────────────────────────────────────────────┐   │
# │  │                              VPC                                     │   │
# │  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
# │  │  │   Public Subnet 1   │  │   Public Subnet 2   │  (NAT, LB)        │   │
# │  │  │   AZ: us-east-1a    │  │   AZ: us-east-1b    │                   │   │
# │  │  └─────────────────────┘  └─────────────────────┘                   │   │
# │  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
# │  │  │  Private Subnet 1   │  │  Private Subnet 2   │  (EKS Nodes)      │   │
# │  │  │   AZ: us-east-1a    │  │   AZ: us-east-1b    │                   │   │
# │  │  └─────────────────────┘  └─────────────────────┘                   │   │
# │  │                                                                     │   │
# │  │  ┌─────────────────────────────────────────────────────────────┐   │   │
# │  │  │                     EKS Cluster                              │   │   │
# │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │   │   │
# │  │  │  │  goqueue-0  │ │  goqueue-1  │ │  goqueue-2  │            │   │   │
# │  │  │  │   (node-1)  │ │   (node-2)  │ │   (node-3)  │            │   │   │
# │  │  │  └─────────────┘ └─────────────┘ └─────────────┘            │   │   │
# │  │  │           │              │              │                    │   │   │
# │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │   │   │
# │  │  │  │   EBS PV    │ │   EBS PV    │ │   EBS PV    │  (gp3)     │   │   │
# │  │  │  └─────────────┘ └─────────────┘ └─────────────┘            │   │   │
# │  │  └─────────────────────────────────────────────────────────────┘   │   │
# │  └──────────────────────────────────────────────────────────────────────┘   │
# └─────────────────────────────────────────────────────────────────────────────┘
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

  required_providers {
    aws = {
      source  = "hashicorp/aws"
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
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# =============================================================================
# VARIABLES
# =============================================================================

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "goqueue"
}

variable "cluster_version" {
  description = "Kubernetes version for EKS"
  type        = string
  default     = "1.29"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "node_instance_types" {
  description = "EC2 instance types for EKS nodes"
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 5
}

variable "goqueue_replicas" {
  description = "Number of GoQueue broker replicas"
  type        = number
  default     = 3
}

variable "goqueue_storage_size" {
  description = "Storage size per GoQueue broker (e.g., 10Gi)"
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
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "goqueue"
    ManagedBy   = "terraform"
  }
}

# =============================================================================
# DATA SOURCES
# =============================================================================

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# =============================================================================
# VPC
# =============================================================================

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
  
  common_tags = merge(var.tags, {
    Environment = var.environment
    Cluster     = var.cluster_name
  })
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k)]
  public_subnets  = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k + 4)]

  enable_nat_gateway     = true
  single_nat_gateway     = var.environment != "prod"  # Cost optimization for non-prod
  enable_dns_hostnames   = true
  enable_dns_support     = true

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ SUBNET TAGS FOR EKS                                                     │
  # │                                                                         │
  # │ These tags tell the AWS Load Balancer Controller where to place ALBs:   │
  # │   - kubernetes.io/role/elb: Public subnets for internet-facing LBs      │
  # │   - kubernetes.io/role/internal-elb: Private subnets for internal LBs   │
  # └─────────────────────────────────────────────────────────────────────────┘
  public_subnet_tags = {
    "kubernetes.io/role/elb"                      = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"             = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "owned"
  }

  tags = local.common_tags
}

# =============================================================================
# EKS CLUSTER
# =============================================================================

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  # Cluster access
  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  # VPC configuration
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ CLUSTER ADD-ONS                                                         │
  # │                                                                         │
  # │ These are AWS-managed add-ons that EKS installs and maintains:          │
  # │   - coredns: DNS resolution within the cluster                          │
  # │   - kube-proxy: Network proxy on each node                              │
  # │   - vpc-cni: AWS networking for pods (uses ENIs)                        │
  # │   - aws-ebs-csi-driver: Dynamic EBS volume provisioning                 │
  # └─────────────────────────────────────────────────────────────────────────┘
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  # ┌─────────────────────────────────────────────────────────────────────────┐
  # │ MANAGED NODE GROUPS                                                     │
  # │                                                                         │
  # │ Managed node groups let AWS handle:                                     │
  # │   - AMI updates and security patches                                    │
  # │   - Graceful node draining during updates                               │
  # │   - Auto-scaling group management                                       │
  # │                                                                         │
  # │ Alternative: Self-managed node groups (more control, more work)         │
  # └─────────────────────────────────────────────────────────────────────────┘
  eks_managed_node_groups = {
    goqueue = {
      name           = "goqueue-nodes"
      instance_types = var.node_instance_types

      min_size     = var.node_min_size
      max_size     = var.node_max_size
      desired_size = var.node_desired_size

      # ┌───────────────────────────────────────────────────────────────────────┐
      # │ EBS ENCRYPTION                                                        │
      # │                                                                       │
      # │ All EBS volumes are encrypted at rest using AES-256.                  │
      # │ AWS manages the keys by default (or use custom KMS key).              │
      # └───────────────────────────────────────────────────────────────────────┘
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 50
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 125
            encrypted             = true
            delete_on_termination = true
          }
        }
      }

      labels = {
        role = "goqueue"
      }

      tags = local.common_tags
    }
  }

  # Allow access from anywhere (restrict in production)
  cluster_security_group_additional_rules = {
    ingress_all = {
      description = "Allow all ingress"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  tags = local.common_tags
}

# =============================================================================
# IAM FOR EBS CSI DRIVER (IRSA)
# =============================================================================

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ IAM ROLES FOR SERVICE ACCOUNTS (IRSA)                                       │
# │                                                                             │
# │ IRSA lets Kubernetes pods assume IAM roles without hardcoding credentials.  │
# │                                                                             │
# │ HOW IT WORKS:                                                               │
# │   1. Create IAM role with trust policy for OIDC provider                    │
# │   2. Annotate K8s ServiceAccount with role ARN                              │
# │   3. Pods using that ServiceAccount can assume the role                     │
# │                                                                             │
# │ SECURITY BENEFIT:                                                           │
# │   - No long-lived credentials                                               │
# │   - Least privilege per service                                             │
# │   - Auditable via CloudTrail                                                │
# └─────────────────────────────────────────────────────────────────────────────┘

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${var.cluster_name}-ebs-csi-driver"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }

  tags = local.common_tags
}

# =============================================================================
# STORAGE CLASS
# =============================================================================

resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner = "ebs.csi.aws.com"
  reclaim_policy      = "Delete"
  volume_binding_mode = "WaitForFirstConsumer"

  parameters = {
    type       = "gp3"
    iops       = "3000"
    throughput = "125"
    encrypted  = "true"
  }
}

# =============================================================================
# HELM RELEASES
# =============================================================================

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", var.cluster_name]
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", var.cluster_name]
  }
}

# Create namespace for GoQueue
resource "kubernetes_namespace" "goqueue" {
  metadata {
    name = "goqueue"
    labels = {
      name = "goqueue"
    }
  }

  depends_on = [module.eks]
}

# GoQueue Helm release
resource "helm_release" "goqueue" {
  name       = "goqueue"
  namespace  = kubernetes_namespace.goqueue.metadata[0].name
  chart      = "${path.module}/../../../../kubernetes/helm/goqueue"
  
  values = [
    yamlencode({
      replicaCount = var.goqueue_replicas
      
      persistence = {
        enabled      = true
        storageClass = "gp3"
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
    kubernetes_storage_class.gp3,
    module.eks
  ]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = module.eks.cluster_security_group_id
}

output "region" {
  description = "AWS region"
  value       = var.region
}

output "configure_kubectl" {
  description = "Configure kubectl command"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "goqueue_namespace" {
  description = "Kubernetes namespace for GoQueue"
  value       = kubernetes_namespace.goqueue.metadata[0].name
}
